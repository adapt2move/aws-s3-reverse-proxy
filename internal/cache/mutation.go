package cache

import "net/http"

// Mutation is a write to the object store, seen from the cache.
//
// It exists because invalidating after a write is not enough. Consider a read
// of an object that starts, an upload that replaces the object, and the read
// finishing afterwards: it carries the version that was current when it
// began, and storing that would leave the cache holding a superseded object —
// with no MaxAge, indefinitely. Invalidating at the end of the upload does
// not help, because the read finishes later still.
//
// So the key is claimed for the whole length of the write instead. While a
// Mutation is open, reads of that key neither come from the cache nor go into
// it, and the window closes completely. The write's own body is exempt, which
// is what lets an upload leave the cache warm rather than empty.
//
// End must always be called.
type Mutation struct {
	c    *Cache
	keys []string

	capture *Writer
	stored  bool
}

// BeginMutation claims these keys for a write that is about to be sent
// upstream. Whatever was cached for them is dropped immediately, and any
// response for them still being written is marked to be thrown away.
func (c *Cache) BeginMutation(keys ...string) *Mutation {
	if len(keys) == 0 {
		return nil
	}
	c.mu.Lock()
	for _, key := range keys {
		p, ok := c.pending[key]
		if !ok {
			p = &pending{}
			c.pending[key] = p
		}
		if p.mutating > 0 {
			// A second write to a key that is already being written. From
			// here there is no way to tell which one the object store will
			// keep, so neither body may be cached — see Capture.
			p.contested = true
		}
		p.mutating++
		p.poisoned = true
	}
	c.mu.Unlock()
	for _, key := range keys {
		_ = c.store.Delete(key)
	}
	c.forgetRevalidation(keys...)
	c.invalidations.Add(uint64(len(keys)))
	return &Mutation{c: c, keys: keys}
}

// Capture asks for a writer to tee the uploaded body into, so a successful
// upload leaves the object cached instead of merely un-cached. It returns nil
// when the store will not take it — too large, too busy, no room — and a nil
// writer is not an error the caller has to handle.
//
// It also returns nil when another write to the same key is already in
// flight. The exemption below is what makes capturing a body safe at all, and
// it holds only while this mutation is the only one: with two of them, both
// bodies would be exempt and both would commit, and the one left in the cache
// would be whichever finished writing here last — which has nothing to do
// with which one the object store kept. Declining leaves plain invalidation,
// which is the right answer when nobody can say what the object now is.
//
// Only a single-key mutation can capture a body; a batch delete has no body
// that describes any one object.
func (m *Mutation) Capture(size int64) *Writer {
	if m == nil || len(m.keys) != 1 || m.capture != nil {
		return nil
	}
	if m.c.contested(m.keys[0]) {
		return nil
	}
	w, err := m.c.store.Put(m.keys[0], size)
	if err != nil {
		return nil
	}
	m.c.mu.Lock()
	if p := m.c.pending[m.keys[0]]; p != nil {
		p.writers++
	}
	m.c.mu.Unlock()
	// exempt: this writer *is* the write everything else is being kept away
	// from, so the poison the mutation set does not apply to it. It is still
	// checked against contested when it commits, because a rival write can
	// start after this point.
	m.capture = &Writer{c: m.c, key: m.keys[0], w: w, expect: size, exempt: true}
	return m.capture
}

// Store records that the upstream accepted the write, and commits the
// captured body under the key described by header.
func (m *Mutation) Store(header http.Header) {
	if m == nil || m.capture == nil {
		return
	}
	if err := m.capture.Commit(header); err == nil && !m.capture.failed {
		m.stored = true
	}
}

// End releases the keys.
//
// If no body was stored — the write failed, there was nothing to capture, or
// capturing it did not work out — the keys are invalidated once more. They
// were already invalidated at the start; doing it again costs a lookup and
// closes the case where something slipped into the cache in between.
func (m *Mutation) End() {
	if m == nil {
		return
	}
	if m.capture != nil {
		m.capture.Abort() // no-op after a successful Commit
	}
	if !m.stored {
		for _, key := range m.keys {
			_ = m.c.store.Delete(key)
		}
	}
	// Always, stored or not. A revalidation that was in flight while this
	// write happened can land after it, and its "still current" was about
	// the version this write replaced. Freshness has to run from the write.
	m.c.forgetRevalidation(m.keys...)
	m.c.mu.Lock()
	for _, key := range m.keys {
		p := m.c.pending[key]
		if p == nil {
			continue
		}
		p.mutating--
		if p.mutating == 0 && p.writers == 0 {
			p.poisoned = false
			// The key is quiet again, so a later write starts uncontested.
			p.contested = false
		}
		m.c.forgetLocked(key, p)
	}
	m.c.mu.Unlock()
}
