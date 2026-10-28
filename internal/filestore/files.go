package filestore

import (
	"errors"
	"io"
	"os"
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

	// claimed is the disk this write holds against MaxBytes. Nothing is
	// written that has not been claimed first, and the claim is given back
	// when the write ends, whichever way it ends.
	claimed int64
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
	if err := w.grow(next); err != nil {
		w.fail(err)
		return 0, w.err
	}
	n, err := w.f.Write(p)
	w.written += int64(n)
	if err != nil {
		w.fail(err)
	}
	return n, err
}

// grow claims what a file of length n occupies, beyond what this write has
// claimed already. A write whose length was announced claimed it all in Put;
// one whose length is not known claims it as it arrives, because that is the
// only point at which anybody knows how much there is.
func (w *Writer) grow(n int64) error {
	need := blocks(n) - w.claimed
	if need <= 0 {
		return nil
	}
	if err := w.s.claim(need); err != nil {
		return err
	}
	w.claimed += need
	return nil
}

// Size is how many payload bytes have been written so far.
func (w *Writer) Size() int64 { return w.written }

// Commit publishes the payload under its key, described by meta. Once it
// returns nil, Get finds the entry. It is CommitUnless with nothing to check.
func (w *Writer) Commit(meta []byte) error { return w.CommitUnless(meta, nil) }

// CommitUnless publishes the payload under its key, described by meta, unless
// superseded reports true. Once it returns nil, Get finds the entry.
//
// The metadata is supplied here rather than at Put because it is often not
// known any earlier: what an object store says about an upload — its ETag —
// arrives once the upload has landed. So it goes into the file after the
// payload, followed by the key, which leaves the payload at offset 0. Both
// are claimed before they are written, like the payload was.
//
// superseded is asked with the store's lock held, at the moment the entry
// would become visible, which is the only moment its answer is final: a
// check made any earlier leaves a gap in which the object can be invalidated
// and the out-of-date payload published anyway. It must not call back into
// the store. When it reports true, the file is discarded and CommitUnless
// returns ErrSuperseded.
//
// Nothing is synced. The file only has to be readable by this process, and
// the page cache already makes it so.
func (w *Writer) CommitUnless(meta []byte, superseded func() bool) error {
	if w.done {
		return errors.New("filestore: commit called twice")
	}
	if w.err != nil {
		return w.err
	}
	w.done = true
	length := w.written + int64(len(meta)) + int64(len(w.key))
	if err := w.grow(length); err != nil {
		return w.discard(err)
	}
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
	e := &entry{key: w.key, id: w.id, size: w.written, meta: int64(len(meta)), disk: blocks(length)}
	claimed := w.claimed
	// From here the file belongs to the store, which removes it when the
	// entry goes or, if it is closed already, right away.
	w.id, w.claimed = 0, 0
	return w.s.commit(e, claimed, superseded)
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

// release closes and removes the file, and only then gives back the claim:
// the other way round, the space would be counted free while the file still
// held it. The file was never an entry, so nothing else can be reading it.
func (w *Writer) release() {
	if w.f != nil {
		_ = w.f.Close()
		w.f = nil
	}
	if w.id != 0 {
		_ = os.Remove(w.s.path(w.id))
		w.id = 0
	}
	w.s.unclaim(w.claimed)
	w.claimed = 0
}

// Reader streams one cached payload.
//
// It holds the file, so neither eviction nor a newer version of the key can
// pull the ground out from under a download in progress: the entry leaves the
// index immediately, and the file stays on disk, still counted against
// MaxBytes, until the reader closes. Closing it is therefore not optional.
type Reader struct {
	s      *Store
	e      *entry
	f      *os.File
	body   *io.SectionReader
	meta   []byte
	closed bool
}

// Meta returns the opaque metadata stored with the payload. The slice belongs
// to the caller.
func (r *Reader) Meta() []byte { return r.meta }

// Size is the payload length in bytes.
func (r *Reader) Size() int64 { return r.e.size }

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

// Close releases the file, and removes it if it has left the index and this
// was the last reader. Calling it twice is harmless.
func (r *Reader) Close() error {
	if r.closed {
		return nil
	}
	r.closed = true
	err := r.f.Close()
	r.s.unref(r.e)
	return err
}
