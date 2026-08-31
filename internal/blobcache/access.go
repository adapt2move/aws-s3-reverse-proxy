package blobcache

import (
	"errors"
	"io"
	"os"
	"sync"
)

// encodeRecord lays out one log record: a fixed header, then the key, then —
// only for an inline payload — the payload, then the metadata.
//
// The checksum is taken over the variable part in exactly the order it is
// written, which is what lets recovery verify it while streaming past a
// payload it has no reason to hold.
func encodeRecord(kind recordKind, key string, meta, data []byte, dataLen int64, blobID uint64) []byte {
	size := recordHeaderSize + len(key) + len(meta)
	if kind == kindInline {
		size += len(data)
	}
	buf := make([]byte, size)
	n := recordHeaderSize
	n += copy(buf[n:], key)
	if kind == kindInline {
		n += copy(buf[n:], data)
	}
	copy(buf[n:], meta)
	h := recordHeader{
		Kind:    kind,
		KeyLen:  uint32(len(key)),
		MetaLen: uint32(len(meta)),
		DataLen: uint64(dataLen),
		BlobID:  blobID,
		CRC:     checksum(buf[recordHeaderSize:]),
	}
	h.encode(buf[:recordHeaderSize])
	return buf
}

// zeroHeader reserves room for a record header that is filled in once the
// payload it describes is complete.
var zeroHeader [recordHeaderSize]byte

// stagingPool recycles the buffers records are assembled in. A cache under
// load allocates one of these per write, and they are as large as the
// payloads themselves, so leaving them to the garbage collector shows up in
// the profile long before anything else does.
var stagingPool sync.Pool

func takeStaging() []byte {
	if b, _ := stagingPool.Get().(*[]byte); b != nil {
		return (*b)[:0]
	}
	return nil
}

func returnStaging(b []byte) {
	if cap(b) == 0 {
		return
	}
	b = b[:0]
	stagingPool.Put(&b)
}

// Writer accumulates one payload and, on Commit, publishes it under its key.
//
// A small payload is staged in memory and appended to the log as a single
// record. One that outgrows Options.InlineMaxSize — whether because the size
// hint said so or because it turned out longer than announced — spills into a
// file of its own without the caller noticing.
//
// A Writer is not safe for concurrent use, and nothing it has been given is
// visible to Get until Commit returns.
type Writer struct {
	s   *Store
	key string

	// buf is the record itself, assembled in place: header, key, metadata,
	// then the payload as it arrives. Building it here rather than copying
	// a staged payload into a record at commit time is one full copy of
	// every cached object that does not happen.
	buf          []byte
	payloadStart int

	blob    *blobWriter
	written int64
	err     error
	done    bool
}

// beginInline lays out the fixed part of the record so the payload can be
// appended straight onto it. The metadata goes on the end at commit time.
func (w *Writer) beginInline() {
	w.buf = append(takeStaging(), zeroHeader[:]...)
	w.buf = append(w.buf, w.key...)
	w.payloadStart = len(w.buf)
}

func (w *Writer) Write(p []byte) (int, error) {
	if w.done {
		return 0, errors.New("blobcache: write after commit")
	}
	if w.err != nil {
		return 0, w.err
	}
	if w.written+int64(len(p)) > w.s.opts.MaxObjectSize {
		// Announced small, turned out large. Fail the write rather than
		// let one object push everything else out of the cache; the caller
		// is expected to keep going without us.
		w.fail(ErrTooLarge)
		return 0, w.err
	}
	if w.blob == nil && w.written+int64(len(p)) > w.s.opts.InlineMaxSize {
		if err := w.spill(); err != nil {
			w.fail(err)
			return 0, w.err
		}
	}
	if w.blob != nil {
		n, err := w.blob.Write(p)
		w.written += int64(n)
		if err != nil {
			w.fail(err)
		}
		return n, err
	}
	if w.buf == nil {
		w.beginInline()
	}
	w.buf = append(w.buf, p...)
	w.written += int64(len(p))
	return len(p), nil
}

// ReadFrom lets io.Copy hand the payload over in one call, which is how a
// caller teeing a stream into the cache avoids a second copy.
func (w *Writer) ReadFrom(r io.Reader) (int64, error) {
	buf := make([]byte, 128<<10)
	var total int64
	for {
		n, err := r.Read(buf)
		if n > 0 {
			written, werr := w.Write(buf[:n])
			total += int64(written)
			if werr != nil {
				return total, werr
			}
		}
		if err == io.EOF {
			return total, nil
		}
		if err != nil {
			return total, err
		}
	}
}

// spill moves a payload that outgrew the staging buffer into its own file,
// carrying over what has been written so far.
func (w *Writer) spill() error {
	blob, err := newBlobWriter(w.s.opts.Dir, w.s.nextID.Add(1), 0)
	if err != nil {
		return err
	}
	if len(w.buf) > w.payloadStart {
		if _, err := blob.Write(w.buf[w.payloadStart:]); err != nil {
			blob.abort()
			return err
		}
	}
	returnStaging(w.buf)
	w.buf = nil
	w.blob = blob
	return nil
}

func (w *Writer) fail(err error) {
	if w.err == nil {
		w.err = err
	}
	w.release()
}

// Size is how many payload bytes have been written so far.
func (w *Writer) Size() int64 { return w.written }

// Commit publishes the payload under its key, described by meta. Once it
// returns nil, Get finds the entry.
//
// The metadata is supplied here rather than at Put because it is often not
// known any earlier: what an object store says about an upload — its ETag,
// its stored length — arrives once the upload has landed, and buffering the
// whole payload just to record that alongside it would defeat the point.
//
// ErrBusy means the store was too far behind to take the write; the entry is
// simply not cached, which is never a reason for the caller to fail.
func (w *Writer) Commit(meta []byte) error {
	if w.done {
		return errors.New("blobcache: commit called twice")
	}
	if w.err != nil {
		return w.err
	}
	if int64(len(meta)) > int64(^uint32(0)) {
		w.fail(errors.New("blobcache: metadata is absurdly long"))
		return w.err
	}
	w.done = true

	if w.blob != nil {
		c, err := w.blob.commit()
		if err != nil {
			w.blob.abort()
			w.blob = nil
			return err
		}
		w.blob = nil
		h := recordHeader{
			Kind:    kindBlobRef,
			KeyLen:  uint32(len(w.key)),
			MetaLen: uint32(len(meta)),
			DataLen: uint64(w.written),
			BlobID:  c.id,
		}
		op := takeOp()
		op.key, op.header, op.blob = w.key, h, c
		op.record = encodeRecord(kindBlobRef, w.key, meta, nil, w.written, c.id)
		if err := w.s.submit(op); err != nil {
			// The record never reached the log, so nothing will ever refer
			// to this file. Take it back out now rather than leave it for
			// the next startup to notice.
			c.retire()
			return err
		}
		return nil
	}

	if w.buf == nil {
		w.beginInline()
	}
	w.buf = append(w.buf, meta...)
	defer func() {
		returnStaging(w.buf)
		w.buf = nil
	}()

	// The record is already laid out; all that is left is to describe it.
	h := recordHeader{
		Kind:    kindInline,
		KeyLen:  uint32(len(w.key)),
		MetaLen: uint32(len(meta)),
		DataLen: uint64(w.written),
		CRC:     checksum(w.buf[recordHeaderSize:]),
	}
	h.encode(w.buf[:recordHeaderSize])

	op := takeOp()
	op.key, op.header, op.record = w.key, h, w.buf
	return w.s.submit(op)
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

func (w *Writer) release() {
	if w.blob != nil {
		w.blob.abort()
		w.blob = nil
	}
	returnStaging(w.buf)
	w.buf = nil
}

// Reader streams one cached payload.
//
// It holds the file it reads from open, so eviction cannot pull the ground
// out from under a download in progress: the entry disappears from the index
// immediately, and the bytes stay readable until the last reader closes.
// Closing it is therefore not optional.
type Reader struct {
	c      *container
	body   *io.SectionReader
	meta   []byte
	offset int64
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

// Payload exposes the backing file and the byte range the payload occupies in
// it.
//
// It is here for the one caller that wants the kernel to do the copy —
// sendfile, or a copy_file_range — which is worth reaching for on a large
// object and pointless on a small one. Everyone else should use Read.
// The file is valid until Close.
func (r *Reader) Payload() (f *os.File, offset, length int64) {
	return r.c.rf, r.offset, r.size
}

// Close releases the underlying file. Calling it twice is harmless.
func (r *Reader) Close() error {
	if r.closed {
		return nil
	}
	r.closed = true
	r.c.release()
	return nil
}
