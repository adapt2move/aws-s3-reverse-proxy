package filestore

import (
	"errors"
	"io"
	"os"
	"sync"
)

// Writer accumulates one payload in its own file and, on Commit, makes it the
// entry for its key.
//
// The file is named by a fresh sequence number and nothing points at it until
// Commit, so a reader can never see it half-written, and there is no rename
// to make: publishing is an update of the index.
//
// A Writer is not safe for concurrent use.
type Writer struct {
	s   *Store
	key string
	id  uint64 // zero once the file is no longer this writer's to remove
	f   *os.File

	written int64
	err     error
	done    bool

	// reserved is how much disk this write has claimed against the store's
	// allowance for writes in flight. It is given back when the write ends,
	// whichever way it ends.
	reserved int64
}

func (w *Writer) Write(p []byte) (int, error) {
	if w.done {
		return 0, errors.New("filestore: write after commit")
	}
	if w.err != nil {
		return 0, w.err
	}
	next := w.written + int64(len(p))
	if next > w.s.opts.MaxObjectSize {
		// Announced small, turned out large. Fail the write rather than let
		// one object push everything else out of the cache; the caller is
		// expected to keep going without us.
		w.fail(ErrTooLarge)
		return 0, w.err
	}
	if next > w.reserved {
		// A write whose length was announced claimed its disk up front. One
		// whose length is not known claims it as it arrives, because that is
		// the only point at which anybody knows how much there is.
		if !w.s.reserve(next - w.reserved) {
			w.fail(ErrNoRoom)
			return 0, w.err
		}
		w.reserved = next
	}
	n, err := w.f.Write(p)
	w.written += int64(n)
	if err != nil {
		w.fail(err)
	}
	return n, err
}

// Size is how many payload bytes have been written so far.
func (w *Writer) Size() int64 { return w.written }

// Commit publishes the payload under its key, described by meta. Once it
// returns nil, Get finds the entry.
//
// The metadata is supplied here rather than at Put because it is often not
// known any earlier: what an object store says about an upload — its ETag —
// arrives once the upload has landed. So it goes into the file after the
// payload, followed by the key, which leaves the payload at offset 0.
//
// Nothing is synced. The file only has to be readable by this process, and
// the page cache already makes it so.
func (w *Writer) Commit(meta []byte) error {
	if w.done {
		return errors.New("filestore: commit called twice")
	}
	if w.err != nil {
		return w.err
	}
	w.done = true
	if _, err := w.f.Write(meta); err != nil {
		return w.discard(err)
	}
	if _, err := io.WriteString(w.f, w.key); err != nil {
		return w.discard(err)
	}
	err := w.f.Close()
	w.f = nil
	if err != nil {
		return w.discard(err)
	}
	length := w.written + int64(len(meta)) + int64(len(w.key))
	e := &entry{key: w.key, id: w.id, size: w.written, meta: int64(len(meta)), disk: blocks(length)}
	reserved := w.reserved
	// From here the file belongs to the store, which removes it when the
	// entry goes or, if it is closed already, right away.
	w.id, w.reserved = 0, 0
	return w.s.commit(e, reserved)
}

// Abort discards the write. It is safe to call after Commit and safe to call
// twice, so `defer w.Abort()` is the right way to use a Writer.
func (w *Writer) Abort() {
	if w.done {
		return
	}
	w.done = true
	w.release()
}

func (w *Writer) fail(err error) {
	if w.err == nil {
		w.err = err
	}
	w.release()
}

func (w *Writer) discard(err error) error {
	w.err = err
	w.release()
	return err
}

// release closes and removes the file and gives back the reservation. The
// file was never an entry, so nothing else can be reading it.
func (w *Writer) release() {
	if w.f != nil {
		_ = w.f.Close()
		w.f = nil
	}
	if w.id != 0 {
		_ = os.Remove(w.s.path(w.id))
		w.id = 0
	}
	w.s.release(w.reserved)
	w.reserved = 0
}

// Reader streams one cached payload.
//
// It holds the file open, so neither eviction nor a newer version of the key
// can pull the ground out from under a download in progress: the entry leaves
// the index immediately, and the bytes stay readable until the reader closes.
// Closing it is therefore not optional.
type Reader struct {
	f      *os.File
	body   *io.SectionReader
	meta   []byte
	size   int64
	closed bool
}

// Meta returns the opaque metadata stored with the payload. The slice belongs
// to the caller.
func (r *Reader) Meta() []byte { return r.meta }

// Size is the payload length in bytes.
func (r *Reader) Size() int64 { return r.size }

func (r *Reader) Read(p []byte) (int, error) {
	if r.closed {
		return 0, os.ErrClosed
	}
	return r.body.Read(p)
}

func (r *Reader) ReadAt(p []byte, off int64) (int, error) {
	if r.closed {
		return 0, os.ErrClosed
	}
	return r.body.ReadAt(p, off)
}

func (r *Reader) Seek(offset int64, whence int) (int64, error) {
	if r.closed {
		return 0, os.ErrClosed
	}
	return r.body.Seek(offset, whence)
}

// Close releases the file. Calling it twice is harmless.
func (r *Reader) Close() error {
	if r.closed {
		return nil
	}
	r.closed = true
	return r.f.Close()
}

// reaper removes files in the background.
//
// The moments a file stops being needed — a commit that evicted something, a
// delete on an upload's path — are moments somebody is waiting, and unlinking
// a large file is not free. The index is updated synchronously, which is what
// correctness rests on; the disk catches up here.
type reaper struct {
	mu      sync.Mutex
	idle    *sync.Cond // broadcast whenever pending drops to zero
	queue   []string
	pending int
	stopped bool

	wake chan struct{}
	quit chan struct{}
	done chan struct{}
}

func (r *reaper) start() {
	r.idle = sync.NewCond(&r.mu)
	r.wake = make(chan struct{}, 1)
	r.quit = make(chan struct{})
	r.done = make(chan struct{})
	go r.run()
}

// remove queues paths for removal. Once the reaper has stopped it removes them
// itself, so nothing queued late is left behind.
func (r *reaper) remove(paths ...string) {
	if len(paths) == 0 {
		return
	}
	r.mu.Lock()
	if r.stopped {
		r.mu.Unlock()
		for _, p := range paths {
			_ = os.RemoveAll(p)
		}
		return
	}
	r.queue = append(r.queue, paths...)
	r.pending += len(paths)
	r.mu.Unlock()
	select {
	case r.wake <- struct{}{}:
	default:
	}
}

func (r *reaper) run() {
	defer close(r.done)
	for {
		select {
		case <-r.wake:
			r.drain()
		case <-r.quit:
			r.drain()
			return
		}
	}
}

func (r *reaper) drain() {
	for {
		r.mu.Lock()
		batch := r.queue
		r.queue = nil
		r.mu.Unlock()
		if len(batch) == 0 {
			return
		}
		for _, p := range batch {
			_ = os.RemoveAll(p)
		}
		r.mu.Lock()
		r.pending -= len(batch)
		if r.pending == 0 {
			r.idle.Broadcast()
		}
		r.mu.Unlock()
	}
}

// stop removes what is still queued and ends the goroutine.
func (r *reaper) stop() {
	r.mu.Lock()
	r.stopped = true
	r.mu.Unlock()
	close(r.quit)
	<-r.done
}
