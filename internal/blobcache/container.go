package blobcache

import (
	"fmt"
	"os"
	"path/filepath"
	"sync/atomic"
)

// A container is one file the store reads payloads out of: a segment holding
// many records, or a blob file holding exactly one.
//
// Its lifetime is reference counted rather than locked. Eviction has to be
// able to unlink a file while readers are still streaming out of it, and
// making a read hold a lock that a write path also wants would put disk
// latency on the wrong side of the mutex. So: the store holds one reference,
// a Reader holds one for as long as it is open, and the file is closed and
// unlinked by whoever drops the last one.
type container struct {
	id   uint64
	path string
	blob bool // single-payload file rather than a segment

	// rf is opened once and only ever read with pread, so every reader can
	// share it without coordinating a file offset.
	rf *os.File

	refs  atomic.Int64
	size  atomic.Int64 // bytes this file occupies on disk
	live  atomic.Int64 // bytes still reachable from the index
	atime atomic.Int64 // unix nanos of the last read, for eviction
	dead  atomic.Bool  // retired: no longer reachable from the index

	// members are the entries ever written into this container, in log
	// order. Eviction needs to find them without sweeping the whole index,
	// and a superseded entry is recognised by no longer being the one the
	// index holds — so nothing has to be removed from here.
	//
	// Only the store's writer goroutine touches this.
	members []*entry
}

func (c *container) acquire() bool {
	for {
		n := c.refs.Load()
		if n <= 0 {
			return false
		}
		if c.refs.CompareAndSwap(n, n+1) {
			return true
		}
	}
}

func (c *container) release() {
	if c.refs.Add(-1) != 0 {
		return
	}
	// Last one out. Closing before unlinking is not required on Unix — an
	// open descriptor keeps the inode alive — but it is the order that also
	// behaves on platforms where it is.
	if c.rf != nil {
		_ = c.rf.Close()
	}
	if c.dead.Load() {
		_ = os.Remove(c.path)
	}
}

// retire drops the store's own reference. The file goes away once the last
// reader is done with it, which may be now or may be several seconds from
// now; either way the caller does not have to wait.
func (c *container) retire() {
	if c.dead.Swap(true) {
		return
	}
	c.release()
}

func (c *container) readAt(p []byte, off int64) error {
	n, err := c.rf.ReadAt(p, off)
	if err != nil {
		return err
	}
	if n != len(p) {
		return fmt.Errorf("blobcache: short read of %d/%d bytes at %d in %s", n, len(p), off, c.path)
	}
	return nil
}

// File names. The id is zero-padded hex so that a directory listing sorts in
// creation order, which is the order the log has to be replayed in.
func segmentPath(dir string, id uint64) string {
	return filepath.Join(dir, fmt.Sprintf("seg-%016x.log", id))
}

func blobPath(dir string, id uint64) string {
	return filepath.Join(dir, fmt.Sprintf("blob-%016x.dat", id))
}

func tempDir(dir string) string { return filepath.Join(dir, "tmp") }
