package blobcache

import (
	"bufio"
	"errors"
	"hash/crc32"
	"io"
	"os"
)

// activeSegment is the one segment records are appended to. It is owned
// outright by the store's writer goroutine, so nothing in here locks: the
// serialization that makes the log ordered is the same serialization that
// makes this struct single-threaded.
type activeSegment struct {
	c  *container
	wf *os.File
	bw *bufio.Writer

	// end is the offset the next record starts at, which is also the
	// segment's current length once bw is flushed.
	end int64

	lastWriteback int64
	dirty         bool
}

// newSegmentBuffer sizes the append buffer. It is what turns a batch of
// commits into one write(), so it wants to be comfortably larger than any
// inline payload.
func newSegmentBuffer(wf *os.File) *bufio.Writer {
	return bufio.NewWriterSize(wf, 1<<20)
}

func createSegment(dir string, id uint64, size int64) (*activeSegment, error) {
	path := segmentPath(dir, id)
	wf, err := os.OpenFile(path, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0o600)
	if err != nil {
		return nil, err
	}
	// Reserving the whole segment up front keeps the filesystem from
	// growing an extent tree one append at a time, and turns "out of disk"
	// into an error here rather than halfway through somebody's upload. It
	// is advisory: a filesystem that cannot do it just says so.
	preallocate(wf, 0, size)

	header := encodeSegmentHeader(id)
	if _, err := wf.Write(header); err != nil {
		_ = wf.Close()
		_ = os.Remove(path)
		return nil, err
	}
	rf, err := os.Open(path)
	if err != nil {
		_ = wf.Close()
		_ = os.Remove(path)
		return nil, err
	}

	c := &container{id: id, path: path, rf: rf}
	c.refs.Store(1)
	s := &activeSegment{c: c, wf: wf, bw: newSegmentBuffer(wf), end: int64(len(header))}
	return s, nil
}

// append copies one encoded record into the segment's buffer and reports
// where it landed. Nothing reaches a syscall until flush, which is what lets
// a burst of writes become a single write().
func (s *activeSegment) append(record []byte) (offset int64, err error) {
	offset = s.end
	if _, err := s.bw.Write(record); err != nil {
		return 0, err
	}
	s.end += int64(len(record))
	s.dirty = true
	return offset, nil
}

// flush pushes the buffer into the page cache. It does not fsync, and that is
// the point: the data is now visible to every reader through the page cache,
// and its durability is the kernel's problem rather than this request's.
func (s *activeSegment) flush(writebackEvery int64) error {
	if !s.dirty {
		return nil
	}
	if err := s.bw.Flush(); err != nil {
		return err
	}
	s.dirty = false
	if writebackEvery > 0 && s.end-s.lastWriteback >= writebackEvery {
		// Ask for writeback to start, without waiting for it. Left alone, a
		// sustained burst piles up dirty pages until the kernel forces a
		// synchronous flush on whichever request is unlucky enough to be in
		// flight at the time.
		startWriteback(s.wf, s.lastWriteback, s.end-s.lastWriteback)
		s.lastWriteback = s.end
	}
	return nil
}

func (s *activeSegment) sync() error {
	if err := s.flush(-1); err != nil {
		return err
	}
	return s.wf.Sync()
}

// seal finishes a segment: it appends an index of everything in it, fsyncs
// once, and stops writing to it forever.
//
// The index is built by reading the segment back rather than by remembering
// what went into it. The bytes are in the page cache — we wrote them seconds
// ago — so the read is memory-speed, and the alternative would mean holding
// every key of a full segment on the heap until it filled up. It also means
// the footer is produced by the same code that replays a segment without one,
// so the two can never disagree.
func (s *activeSegment) seal(verify bool) error {
	if err := s.flush(-1); err != nil {
		return err
	}
	var entries []footerEntry
	_, err := replayRecords(s.c.rf, s.end, verify, func(off int64, h recordHeader, key string) error {
		entries = append(entries, footerEntry{Offset: off, Header: h, Key: key})
		return nil
	})
	if err != nil {
		return err
	}
	if _, err := s.wf.Write(encodeFooter(entries)); err != nil {
		return err
	}
	// The one fsync a segment ever gets, amortised over its whole size. It
	// makes the footer trustworthy, which is what lets the next Open skip
	// reading the segment at all.
	if err := s.wf.Sync(); err != nil {
		return err
	}
	return s.wf.Close()
}

// close abandons an active segment without sealing it, for the error paths
// and for a store that is shutting down in a hurry.
func (s *activeSegment) close() error {
	err := s.flush(-1)
	if cerr := s.wf.Close(); err == nil {
		err = cerr
	}
	return err
}

// openSegment opens an existing segment for reading and returns its id.
func openSegment(path string) (*container, uint64, int64, error) {
	rf, err := os.Open(path)
	if err != nil {
		return nil, 0, 0, err
	}
	info, err := rf.Stat()
	if err != nil {
		_ = rf.Close()
		return nil, 0, 0, err
	}
	header := make([]byte, segmentHeaderSize)
	if _, err := io.ReadFull(io.NewSectionReader(rf, 0, segmentHeaderSize), header); err != nil {
		_ = rf.Close()
		return nil, 0, 0, errShortSegment
	}
	id, err := decodeSegmentHeader(header)
	if err != nil {
		_ = rf.Close()
		return nil, 0, 0, err
	}
	c := &container{id: id, path: path, rf: rf}
	c.refs.Store(1)
	return c, id, info.Size(), nil
}

// replaySegment reconstructs what a segment contains, preferring its footer
// and falling back to reading the records themselves.
//
// It returns the offset at which the records end — for a sealed segment that
// is where the footer starts, and for one that was still being written when
// the process died, it is the end of the last record that checks out. Whatever
// follows is not data, it is the shape a crash left behind.
func replaySegment(c *container, fileSize int64, fn func(off int64, h recordHeader, key string) error) (recordsEnd int64, sealed bool, err error) {
	if entries, end, ok := readFooter(c.rf, fileSize); ok {
		for _, e := range entries {
			if err := fn(e.Offset, e.Header, e.Key); err != nil {
				return 0, false, err
			}
		}
		return end, true, nil
	}
	end, err := replayRecords(c.rf, fileSize, true, fn)
	return end, false, err
}

// readFooter returns the index a sealed segment ends with. Anything wrong
// with it — missing, truncated, a checksum that does not match — is reported
// as "no footer" rather than as an error: replaying the records is always
// available, and always correct.
func readFooter(rf *os.File, fileSize int64) ([]footerEntry, int64, bool) {
	if fileSize < segmentHeaderSize+footerTrailerSize {
		return nil, 0, false
	}
	trailer := make([]byte, footerTrailerSize)
	if _, err := rf.ReadAt(trailer, fileSize-footerTrailerSize); err != nil {
		return nil, 0, false
	}
	count, length, sum, err := decodeFooterTrailer(trailer)
	if err != nil {
		return nil, 0, false
	}
	start := fileSize - footerTrailerSize - length
	if start < segmentHeaderSize {
		return nil, 0, false
	}
	index := make([]byte, length)
	if _, err := rf.ReadAt(index, start); err != nil && !errors.Is(err, io.EOF) {
		return nil, 0, false
	}
	entries, err := decodeFooter(index, count, sum)
	if err != nil {
		return nil, 0, false
	}
	return entries, start, true
}

// replayRecords walks a segment's records from the front, stopping at the
// first one that is not intact.
//
// With verify set it also checks every payload against its checksum, which is
// the guarantee the whole no-fsync design rests on: a record the filesystem
// zero-filled or tore in half does not decode, and one whose bytes were
// silently altered does not check out. Either way it ends the replay, and
// everything after it is discarded rather than trusted.
func replayRecords(rf *os.File, limit int64, verify bool, fn func(off int64, h recordHeader, key string) error) (int64, error) {
	if limit < segmentHeaderSize {
		return segmentHeaderSize, nil
	}
	section := io.NewSectionReader(rf, segmentHeaderSize, limit-segmentHeaderSize)
	r := bufio.NewReaderSize(section, 1<<20)

	offset := int64(segmentHeaderSize)
	header := make([]byte, recordHeaderSize)
	for {
		if _, err := io.ReadFull(r, header); err != nil {
			// A short read here is the ordinary end of a segment, not a
			// problem: the tail of an unsealed file is whatever the
			// preallocation left behind.
			return offset, nil
		}
		h, err := decodeRecordHeader(header)
		if err != nil {
			return offset, nil
		}
		if offset+h.size() > limit {
			return offset, nil
		}
		key := make([]byte, h.KeyLen)
		if _, err := io.ReadFull(r, key); err != nil {
			return offset, nil
		}
		meta := make([]byte, h.MetaLen)
		if _, err := io.ReadFull(r, meta); err != nil {
			return offset, nil
		}

		// The checksum covers the variable part in the order it appears on
		// disk, so it can be computed while streaming past the payload
		// rather than by holding it.
		digest := crc32.New(crc32c)
		_, _ = digest.Write(key)
		_, _ = digest.Write(meta)
		if h.Kind == kindInline {
			if verify {
				if _, err := io.CopyN(digest, r, int64(h.DataLen)); err != nil {
					return offset, nil
				}
			} else if _, err := r.Discard(int(h.DataLen)); err != nil {
				return offset, nil
			}
		}
		if verify && digest.Sum32() != h.CRC {
			return offset, nil
		}

		if err := fn(offset, h, string(key)); err != nil {
			return 0, err
		}
		offset += h.size()
	}
}
