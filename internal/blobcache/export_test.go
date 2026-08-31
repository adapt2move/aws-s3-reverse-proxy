package blobcache

// Crash stops the store the way a killed process does: the writer goroutine
// goes away, the segment being appended to is never sealed, and nothing is
// fsynced on the way out.
//
// Recovering from a store that was closed cleanly proves almost nothing —
// every interesting case in this package is about what a process that did not
// get to run its shutdown left behind.
func (s *Store) Crash() {
	s.sendMu.Lock()
	if s.closing {
		s.sendMu.Unlock()
		return
	}
	s.closing = true
	s.crashed.Store(true)
	s.sendMu.Unlock()
	close(s.quit)
	<-s.stopped
}

// ActiveSegmentPath is where the store is currently appending, for a test
// that wants to damage it.
func (s *Store) ActiveSegmentPath() string { return s.active.c.path }

// DiskBytes is what the store believes it is using.
func (s *Store) DiskBytes() int64 { return s.disk.Load() }

// RecordSize is how many bytes an inline record of these sizes occupies.
func RecordSize(key string, meta, data []byte) int64 {
	return int64(recordHeaderSize + len(key) + len(meta) + len(data))
}

// SegmentHeaderSize is where the first record of a segment starts.
const SegmentHeaderSize = segmentHeaderSize
