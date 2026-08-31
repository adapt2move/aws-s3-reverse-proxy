package blobcache

// Recovery. Everything here runs once, before the store starts serving, and
// its whole job is to decide what on disk can still be believed.
//
// The rule it works to: what comes back must be a subset of what went in.
// Losing an entry costs a fetch from upstream; inventing one is the failure
// this package exists to make impossible.

import (
	"io"
	"os"
	"path/filepath"
	"sort"
	"strings"
)

// recover rebuilds the index from what is on disk.
func (s *Store) recover() error {
	if err := s.clearTemp(); err != nil {
		return err
	}
	segments, blobFiles, err := s.scanDir()
	if err != nil {
		return err
	}

	// Blob files are opened first so that the log records referring to them
	// can be resolved as they are replayed. Any that no record claims is an
	// upload that was interrupted between its rename and its log entry.
	blobs := make(map[uint64]*container, len(blobFiles))
	for _, path := range blobFiles {
		c, _, err := openBlobFile(path)
		if err != nil {
			s.opts.logf("blobcache: discarding unusable blob file %s: %v", filepath.Base(path), err)
			s.counters.corruptRecords.Add(1)
			_ = os.Remove(path)
			continue
		}
		blobs[c.id] = c
	}
	referenced := make(map[uint64]bool, len(blobs))

	var last *container
	var lastEnd int64
	var lastSealed bool
	for _, path := range segments {
		c, id, size, err := openSegment(path)
		if err != nil {
			s.opts.logf("blobcache: discarding unusable segment %s: %v", filepath.Base(path), err)
			s.counters.corruptRecords.Add(1)
			_ = os.Remove(path)
			continue
		}
		if id >= s.nextID.Load() {
			s.nextID.Store(id + 1)
		}
		end, sealed, err := replaySegment(c, size, func(off int64, h recordHeader, key string) error {
			blob := (*container)(nil)
			if h.Kind == kindBlobRef {
				blob = blobs[h.BlobID]
				if blob == nil {
					// The payload is gone, but the record still says this
					// key was overwritten at this point in the log. Honour
					// the overwrite and drop the value: the alternative is
					// resurrecting whatever this record replaced.
					h.Kind = kindTombstone
				} else {
					referenced[h.BlobID] = true
				}
			}
			s.apply(c, off, h, key, blob)
			return nil
		})
		if err != nil {
			return err
		}
		if end < size && !sealed {
			// Everything past the last intact record is not data. It is
			// what a crash left in a file nobody fsynced, and writing over
			// it is the only way a later replay cannot mistake it for a
			// record.
			s.opts.logf("blobcache: %s: discarding %d bytes after the last intact record", filepath.Base(path), size-end)
			s.counters.corruptRecords.Add(1)
		}
		c.size.Store(size)
		s.containers[id] = c
		s.segments.Add(1)
		s.disk.Add(size)
		last, lastEnd, lastSealed = c, end, sealed
	}

	for id, c := range blobs {
		if referenced[id] {
			s.containers[id] = c
			s.blobs.Add(1)
			s.disk.Add(c.size.Load())
			if id >= s.nextID.Load() {
				s.nextID.Store(id + 1)
			}
			continue
		}
		s.opts.logf("blobcache: removing orphaned blob file %s", filepath.Base(c.path))
		c.retire()
	}

	// Reopen the last segment for appending if it was never sealed and has
	// room left; otherwise start a fresh one.
	if last != nil && !lastSealed && lastEnd < s.opts.SegmentSize {
		active, err := reopenSegment(last, lastEnd)
		if err == nil {
			s.active = active
			s.disk.Add(lastEnd - last.size.Load())
			last.size.Store(lastEnd)
			return nil
		}
		s.opts.logf("blobcache: cannot reopen %s for appending: %v", filepath.Base(last.path), err)
	}
	active, err := createSegment(s.opts.Dir, s.nextID.Add(1), s.opts.SegmentSize)
	if err != nil {
		return err
	}
	s.containers[active.c.id] = active.c
	s.segments.Add(1)
	s.active = active
	return nil
}

func (s *Store) clearTemp() error {
	entries, err := os.ReadDir(tempDir(s.opts.Dir))
	if err != nil {
		return err
	}
	for _, e := range entries {
		// A file here is an upload that never reached its commit. There is
		// nothing to salvage: it has no trailer, so nothing refers to it.
		_ = os.Remove(filepath.Join(tempDir(s.opts.Dir), e.Name()))
	}
	return nil
}

// scanDir returns the store's files in log order and removes anything else.
func (s *Store) scanDir() (segments, blobs []string, err error) {
	entries, err := os.ReadDir(s.opts.Dir)
	if err != nil {
		return nil, nil, err
	}
	for _, e := range entries {
		name := e.Name()
		switch {
		case e.IsDir() && name == "tmp":
		case !e.IsDir() && strings.HasPrefix(name, "seg-") && strings.HasSuffix(name, ".log"):
			segments = append(segments, filepath.Join(s.opts.Dir, name))
		case !e.IsDir() && strings.HasPrefix(name, "blob-") && strings.HasSuffix(name, ".dat"):
			blobs = append(blobs, filepath.Join(s.opts.Dir, name))
		default:
			// The store owns this directory outright. Leaving a stranger's
			// file in it would make the size budget a guess.
			s.opts.logf("blobcache: removing unrecognised file %s", name)
			_ = os.RemoveAll(filepath.Join(s.opts.Dir, name))
		}
	}
	// The names are zero-padded hex of a monotonic id, so sorting them
	// sorts the log.
	sort.Strings(segments)
	return segments, blobs, nil
}

// reopenSegment turns a recovered segment back into the one being appended
// to, truncating whatever followed its last intact record.
func reopenSegment(c *container, end int64) (*activeSegment, error) {
	wf, err := os.OpenFile(c.path, os.O_WRONLY, 0o600)
	if err != nil {
		return nil, err
	}
	if err := wf.Truncate(end); err != nil {
		_ = wf.Close()
		return nil, err
	}
	if _, err := wf.Seek(end, io.SeekStart); err != nil {
		_ = wf.Close()
		return nil, err
	}
	return &activeSegment{
		c:             c,
		wf:            wf,
		bw:            newSegmentBuffer(wf),
		end:           end,
		lastWriteback: end,
	}, nil
}
