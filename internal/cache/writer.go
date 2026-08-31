package cache

import (
	"encoding/json"
	"fmt"
	"net/http"

	"github.com/Kriechi/aws-s3-reverse-proxy/internal/blobcache"
)

// Writer captures one upstream response on its way to a client.
//
// It is written to as a side effect of serving somebody, which sets the rule
// everything here follows: a Writer never slows the response down and never
// fails it. Every error it can produce ends the caching of that one object
// and nothing else, so a caller can ignore what Commit returns and still be
// correct.
type Writer struct {
	c   *Cache
	key string
	w   *blobcache.Writer

	// expect is the payload length the response announced, or negative if
	// it announced none. It is what makes a truncated download detectable:
	// a body that stops early has to not become a cache entry.
	expect  int64
	written int64
	done    bool
	failed  bool

	// exempt marks the writer that belongs to the mutation itself, which is
	// the one write the mutation's poison is not aimed at.
	exempt bool
}

func (w *Writer) Write(p []byte) (int, error) {
	if w.done || w.failed {
		return len(p), nil
	}
	n, err := w.w.Write(p)
	w.written += int64(n)
	if err != nil {
		// Out of disk, over the object-size limit, whatever it was: stop
		// caching this object and let the response carry on. The caller is
		// mid-transfer to a client and must not learn about this.
		w.failed = true
		w.w.Abort()
	}
	return len(p), nil
}

// Commit stores the response under its key, described by header.
//
// It refuses in three cases, and all three are ordinary: the body was shorter
// than it announced, the object was invalidated while this response was in
// flight, or the store was too busy to take it. None of them is a reason for
// the caller to do anything.
func (w *Writer) Commit(header http.Header) error {
	if w.done {
		return nil
	}
	w.done = true
	defer w.finish()

	if w.failed {
		return nil
	}
	if w.expect >= 0 && w.written != w.expect {
		// A client that hung up halfway leaves a body that never finished.
		// Caching it would answer somebody's next read with a truncated
		// object, which is worse than not caching at all.
		w.w.Abort()
		w.c.dropped.Add(1)
		return fmt.Errorf("%w: %d of %d bytes", errShortWrite, w.written, w.expect)
	}
	if !w.exempt && w.poisoned() {
		w.w.Abort()
		w.c.dropped.Add(1)
		return nil
	}

	meta, err := json.Marshal(storedMeta{
		Header:   canonical(header),
		Size:     w.written,
		StoredAt: w.c.now().UnixNano(),
	})
	if err != nil {
		w.w.Abort()
		w.c.dropped.Add(1)
		return err
	}
	if err := w.w.Commit(meta); err != nil {
		w.c.dropped.Add(1)
		return err
	}
	w.c.stored.Add(1)
	return nil
}

// Abort throws the capture away. It is safe after Commit and safe twice.
func (w *Writer) Abort() {
	if w.done {
		return
	}
	w.done = true
	w.w.Abort()
	w.finish()
}

// Size is how many payload bytes have been captured so far.
func (w *Writer) Size() int64 { return w.written }

// canonical normalises header names once, on the way in, so that a hit does
// not have to. What comes off the disk is JSON, which preserves whatever
// spelling it was given — and http.Header.Get only finds the canonical one,
// so an entry stored with "ETag" would come back without an ETag.
func canonical(header http.Header) http.Header {
	out := make(http.Header, len(header))
	for name, values := range header {
		key := http.CanonicalHeaderKey(name)
		if _, taken := out[key]; taken && key != name {
			// The same header under two spellings. The one already written
			// the canonical way wins, so which value survives does not
			// depend on the order a map happened to be walked in.
			continue
		}
		out[key] = values
	}
	return out
}

// poisoned reports whether the object was invalidated while this response was
// being written, and clears the flag if this was the last writer for the key.
func (w *Writer) poisoned() bool {
	w.c.mu.Lock()
	defer w.c.mu.Unlock()
	p := w.c.pending[w.key]
	return p != nil && p.poisoned
}

func (w *Writer) finish() {
	w.c.mu.Lock()
	defer w.c.mu.Unlock()
	p := w.c.pending[w.key]
	if p == nil {
		return
	}
	p.writers--
	if p.writers == 0 {
		// The poison was aimed at the writes that were in flight when the
		// invalidation happened. With none left, it has done its job.
		p.poisoned = false
	}
	w.c.forgetLocked(w.key, p)
}
