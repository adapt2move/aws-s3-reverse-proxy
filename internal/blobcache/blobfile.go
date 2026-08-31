package blobcache

import (
	"hash"
	"hash/crc32"
	"io"
	"os"
	"path/filepath"
)

// A blob file holds exactly one payload, for the objects that are too big to
// belong in a shared segment. Above a certain size the reasons for the log
// invert: a single payload would monopolise the append point for the length
// of an upload, and a segment holding one huge object cannot be reclaimed
// without throwing that object away.
//
// These files are the one place the store does fsync on the write path, and
// it is affordable precisely because they are large: one round trip next to
// megabytes of payload is noise, and it buys a file whose trailer means what
// it says.
type blobWriter struct {
	dir     string
	id      uint64
	tmpPath string
	f       *os.File
	digest  hash.Hash32
	written int64
	done    bool
}

func newBlobWriter(dir string, id uint64, sizeHint int64) (*blobWriter, error) {
	tmp := filepath.Join(tempDir(dir), blobTempName(id))
	f, err := os.OpenFile(tmp, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0o600)
	if err != nil {
		return nil, err
	}
	if sizeHint > 0 {
		preallocate(f, 0, sizeHint+blobTrailerSize)
	}
	return &blobWriter{dir: dir, id: id, tmpPath: tmp, f: f, digest: crc32.New(crc32c)}, nil
}

func (w *blobWriter) Write(p []byte) (int, error) {
	n, err := w.f.Write(p)
	if n > 0 {
		_, _ = w.digest.Write(p[:n])
		w.written += int64(n)
	}
	return n, err
}

// commit writes the trailer, makes the file durable, and only then gives it
// its real name.
//
// The order is what matters. The payload is on stable storage before anything
// refers to the file by a name a reader would look for, so the rename can be
// lost by a crash — costing a cache entry — but can never expose a file whose
// contents did not make it.
func (w *blobWriter) commit() (*container, error) {
	trailer := blobTrailer{ID: w.id, DataLen: w.written, CRC: w.digest.Sum32()}
	if _, err := w.f.Write(trailer.encode()); err != nil {
		return nil, err
	}
	if err := w.f.Sync(); err != nil {
		return nil, err
	}
	if err := w.f.Close(); err != nil {
		return nil, err
	}
	w.done = true
	final := blobPath(w.dir, w.id)
	if err := os.Rename(w.tmpPath, final); err != nil {
		_ = os.Remove(w.tmpPath)
		return nil, err
	}
	rf, err := os.Open(final)
	if err != nil {
		_ = os.Remove(final)
		return nil, err
	}
	c := &container{id: w.id, path: final, blob: true, rf: rf}
	c.refs.Store(1)
	c.live.Store(w.written)
	c.size.Store(w.written + blobTrailerSize)
	return c, nil
}

func (w *blobWriter) abort() {
	if w.done {
		return
	}
	w.done = true
	_ = w.f.Close()
	_ = os.Remove(w.tmpPath)
}

// openBlobFile opens a blob file and checks that it finished being written.
//
// Only the trailer is verified, not the payload: validating a gigabyte on
// every startup would cost more than re-fetching the rare entry that is
// wrong, and the file was fsynced before it was given this name, so a
// half-written one does not have this name at all.
func openBlobFile(path string) (*container, blobTrailer, error) {
	var t blobTrailer
	rf, err := os.Open(path)
	if err != nil {
		return nil, t, err
	}
	info, err := rf.Stat()
	if err != nil {
		_ = rf.Close()
		return nil, t, err
	}
	if info.Size() < blobTrailerSize {
		_ = rf.Close()
		return nil, t, errBadBlobFile
	}
	buf := make([]byte, blobTrailerSize)
	if _, err := rf.ReadAt(buf, info.Size()-blobTrailerSize); err != nil {
		_ = rf.Close()
		return nil, t, errBadBlobFile
	}
	t, err = decodeBlobTrailer(buf)
	if err != nil {
		_ = rf.Close()
		return nil, t, err
	}
	if t.DataLen+blobTrailerSize != info.Size() {
		_ = rf.Close()
		return nil, t, errBadBlobFile
	}
	c := &container{id: t.ID, path: path, blob: true, rf: rf}
	c.refs.Store(1)
	c.live.Store(t.DataLen)
	c.size.Store(info.Size())
	return c, t, nil
}

// verifyBlobPayload re-reads a blob file and checks it against the checksum
// in its trailer. Startup does not do this — see openBlobFile — but a test,
// or an operator chasing a suspected bad volume, wants a way to ask.
func verifyBlobPayload(c *container, t blobTrailer) error {
	digest := crc32.New(crc32c)
	if _, err := io.Copy(digest, io.NewSectionReader(c.rf, 0, t.DataLen)); err != nil {
		return err
	}
	if digest.Sum32() != t.CRC {
		return errBadBlobFile
	}
	return nil
}

func blobTempName(id uint64) string {
	return "blob-" + hex16(id) + ".tmp"
}

func hex16(v uint64) string {
	const digits = "0123456789abcdef"
	var out [16]byte
	for i := 15; i >= 0; i-- {
		out[i] = digits[v&0xf]
		v >>= 4
	}
	return string(out[:])
}
