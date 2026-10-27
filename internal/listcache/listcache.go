// Package listcache remembers the last answer the object store gave to each
// listing, so that one can still be given while the object store is not
// answering.
//
// It is a fallback and never a cache in the ordinary sense: a listing that the
// object store answers is always answered by the object store, and what is
// kept here is only consulted when it does not. That is what makes it safe to
// have at all. A listing is a summary of every key under a prefix, so any
// write anywhere under it changes the answer — and serving remembered
// listings on the hot path would turn every such write into a window in which
// clients are told something that is no longer true.
//
// Like internal/cache, it knows nothing about S3, tenants or policy. A
// listing is stored under an opaque key, together with its scope: the prefix
// every object key it summarises starts with. A write names object keys, and
// it invalidates every listing whose scope is a prefix of one of them. The
// caller builds both kinds of key the same way, which is the whole contract.
//
// Consistency is the point of the package, and it rests on three rules:
//
//   - A write that may have changed an object drops every listing that covers
//     it, before the client that made the write is told it succeeded. A
//     listing remembered from before a write can therefore never be served
//     after it.
//   - A listing that was in flight while a covering write was — in either
//     order, overlapping at all — is not remembered. Which side of the write
//     the object store answered it from is not something this process can
//     know.
//   - Nothing is remembered for longer than MaxAge, and nothing survives a
//     restart. A remembered listing can only miss what changed without
//     passing through this process, and both bounds limit how much that can
//     be.
package listcache

import (
	"container/list"
	"net/http"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

// Options configures a Cache.
type Options struct {
	// MaxBytes bounds the memory the remembered listings may occupy. The
	// least recently used ones are dropped to stay within it.
	MaxBytes int64

	// MaxAge is how old a remembered listing may be and still be served.
	MaxAge time.Duration

	// Now is the clock, for tests.
	Now func() time.Time
}

// Cache holds the last successful answer to each listing, in memory. It is
// safe for concurrent use.
type Cache struct {
	maxBytes int64
	maxAge   time.Duration
	now      func() time.Time

	mu      sync.Mutex
	entries map[string]*entry
	// byScope indexes entries by the prefix they summarise, so that a write
	// walks the distinct scopes rather than every page of every listing.
	byScope map[string]map[string]*entry
	// order is least recently used at the back.
	order *list.List
	bytes int64

	// writes counts, per object key, the writes to it in flight upstream.
	writes map[string]int
	// captures are the listings in flight upstream that may be remembered.
	captures map[*Capture]struct{}

	stored        atomic.Uint64
	dropped       atomic.Uint64
	invalidations atomic.Uint64
}

type entry struct {
	key      string
	scope    string
	header   http.Header
	body     []byte
	storedAt time.Time
	cost     int64
	elem     *list.Element
}

// New creates an empty cache.
func New(opts Options) *Cache {
	if opts.Now == nil {
		opts.Now = time.Now
	}
	return &Cache{
		maxBytes: opts.MaxBytes,
		maxAge:   opts.MaxAge,
		now:      opts.Now,
		entries:  make(map[string]*entry),
		byScope:  make(map[string]map[string]*entry),
		order:    list.New(),
		writes:   make(map[string]int),
		captures: make(map[*Capture]struct{}),
	}
}

// Listing is a remembered answer.
type Listing struct {
	// Header is what was kept of the response headers.
	Header http.Header
	// Body is the response body exactly as the object store sent it. It is
	// shared between everyone the listing is served to: read it, never
	// modify it.
	Body []byte
	// Age is how long ago the object store gave this answer.
	Age time.Duration
}

// Get returns the remembered answer to a listing, or nil when there is none
// young enough to serve.
func (c *Cache) Get(key string) *Listing {
	c.mu.Lock()
	defer c.mu.Unlock()
	e := c.entries[key]
	if e == nil {
		return nil
	}
	age := c.now().Sub(e.storedAt)
	if age > c.maxAge {
		c.removeLocked(e)
		return nil
	}
	c.order.MoveToFront(e.elem)
	return &Listing{Header: e.header.Clone(), Body: e.body, Age: age}
}

// Capture is one listing on its way from the object store, which may become
// the remembered answer once it has arrived. Exactly one of Commit, Discard
// or Abort ends it; the later ones do nothing.
type Capture struct {
	c        *Cache
	key      string
	scope    string
	poisoned bool
	done     bool
}

// Begin registers a listing that is about to be sent upstream. It has to be
// called before the request is sent, not when the answer arrives: a write
// that starts in between would otherwise go unnoticed.
func (c *Cache) Begin(key, scope string) *Capture {
	c.mu.Lock()
	defer c.mu.Unlock()
	cp := &Capture{c: c, key: key, scope: scope}
	for object := range c.writes {
		if strings.HasPrefix(object, scope) {
			// A write under this scope is in flight, so this answer may be
			// from either side of it.
			cp.poisoned = true
			break
		}
	}
	c.captures[cp] = struct{}{}
	return cp
}

// Commit remembers the object store's answer, unless a write under its scope
// overlapped it. It reports whether the answer was kept.
func (cp *Capture) Commit(header http.Header, body []byte) bool {
	c := cp.c
	c.mu.Lock()
	defer c.mu.Unlock()
	if !cp.endLocked() {
		return false
	}
	if cp.poisoned {
		c.dropped.Add(1)
		return false
	}
	e := &entry{
		key:      cp.key,
		scope:    cp.scope,
		header:   header.Clone(),
		body:     body,
		storedAt: c.now(),
	}
	e.cost = costOf(e)
	if e.cost > c.maxBytes {
		c.dropped.Add(1)
		if old := c.entries[cp.key]; old != nil {
			// The new answer is too large to keep, and the old one is no
			// longer the answer.
			c.removeLocked(old)
		}
		return false
	}
	if old := c.entries[cp.key]; old != nil {
		c.removeLocked(old)
	}
	c.entries[e.key] = e
	if c.byScope[e.scope] == nil {
		c.byScope[e.scope] = make(map[string]*entry)
	}
	c.byScope[e.scope][e.key] = e
	e.elem = c.order.PushFront(e)
	c.bytes += e.cost
	for c.bytes > c.maxBytes {
		c.removeLocked(c.order.Back().Value.(*entry))
	}
	c.stored.Add(1)
	return true
}

// Discard forgets what was remembered for this listing: the object store
// answered it, and not with a listing. Whatever made it refuse — the bucket
// is gone, the credentials changed — the remembered answer describes a state
// the object store no longer agrees with.
func (cp *Capture) Discard() {
	c := cp.c
	c.mu.Lock()
	defer c.mu.Unlock()
	if !cp.endLocked() {
		return
	}
	if old := c.entries[cp.key]; old != nil {
		c.removeLocked(old)
	}
}

// Abort ends the capture without touching what is remembered: the object
// store gave no answer worth acting on.
func (cp *Capture) Abort() {
	c := cp.c
	c.mu.Lock()
	defer c.mu.Unlock()
	cp.endLocked()
}

// endLocked marks the capture finished and reports whether it still was not.
func (cp *Capture) endLocked() bool {
	if cp.done {
		return false
	}
	cp.done = true
	delete(cp.c.captures, cp)
	return true
}

// Write is a write to the object store, seen from the listings it may
// change. End must always be called.
type Write struct {
	c     *Cache
	keys  []string
	ended bool
}

// BeginWrite claims these object keys for a write that is about to be sent
// upstream.
//
// What is remembered is kept for as long as the write is in flight: until
// the object store has answered, the write has not happened as far as any
// client can tell, and an outage is exactly when writes fail. Listings in
// flight under these keys, and those that start before the write ends, are
// not remembered.
func (c *Cache) BeginWrite(keys ...string) *Write {
	if len(keys) == 0 {
		return nil
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	for _, key := range keys {
		c.writes[key]++
		c.poisonCapturesLocked(key)
	}
	return &Write{c: c, keys: keys}
}

// End releases the keys. Calling it again does nothing, so a caller can end a
// write as soon as it knows the outcome and still defer End as a backstop.
//
// refused reports that the object store is known not to have applied the
// write: it refused it, or it never received it. Only then are the listings
// covering the keys kept. Any other outcome — success, a server error, a
// connection that broke after the request was sent — may have changed the
// objects, and they are dropped.
func (w *Write) End(refused bool) {
	if w == nil || w.ended {
		return
	}
	w.ended = true
	c := w.c
	c.mu.Lock()
	defer c.mu.Unlock()
	for _, key := range w.keys {
		if c.writes[key]--; c.writes[key] <= 0 {
			delete(c.writes, key)
		}
		if !refused {
			c.invalidateLocked(key)
		}
	}
}

// Invalidate drops every listing covering these object keys, and keeps any
// listing in flight under them from being remembered. It is for a change
// this process learned about without having made it.
func (c *Cache) Invalidate(keys ...string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	for _, key := range keys {
		c.poisonCapturesLocked(key)
		c.invalidateLocked(key)
	}
}

// Purge forgets everything, including the listings still in flight.
func (c *Cache) Purge() error {
	c.mu.Lock()
	defer c.mu.Unlock()
	for cp := range c.captures {
		cp.poisoned = true
	}
	c.invalidations.Add(uint64(len(c.entries)))
	clear(c.entries)
	clear(c.byScope)
	c.order.Init()
	c.bytes = 0
	return nil
}

func (c *Cache) poisonCapturesLocked(object string) {
	for cp := range c.captures {
		if strings.HasPrefix(object, cp.scope) {
			cp.poisoned = true
		}
	}
}

func (c *Cache) invalidateLocked(object string) {
	for scope, entries := range c.byScope {
		if !strings.HasPrefix(object, scope) {
			continue
		}
		for _, e := range entries {
			c.removeLocked(e)
			c.invalidations.Add(1)
		}
	}
}

func (c *Cache) removeLocked(e *entry) {
	delete(c.entries, e.key)
	if scoped := c.byScope[e.scope]; scoped != nil {
		delete(scoped, e.key)
		if len(scoped) == 0 {
			delete(c.byScope, e.scope)
		}
	}
	c.order.Remove(e.elem)
	c.bytes -= e.cost
}

// entryOverhead approximates what an entry costs beyond its strings and
// body: the structs, the map slots, the list element.
const entryOverhead = 256

func costOf(e *entry) int64 {
	cost := int64(entryOverhead + 2*len(e.key) + len(e.scope) + len(e.body))
	for name, values := range e.header {
		cost += int64(len(name))
		for _, v := range values {
			cost += int64(len(v))
		}
	}
	return cost
}

// Size reports how many listings are remembered and the memory they occupy,
// in the shape the purge endpoint asks for.
func (c *Cache) Size() (entries int, bytes int64) {
	s := c.Stats()
	return s.Entries, s.Bytes
}

// Stats reports what the cache holds.
type Stats struct {
	Entries       int
	Bytes         int64
	Capacity      int64
	Stored        uint64
	Dropped       uint64
	Invalidations uint64
}

func (c *Cache) Stats() Stats {
	c.mu.Lock()
	entries, bytes := len(c.entries), c.bytes
	c.mu.Unlock()
	return Stats{
		Entries:       entries,
		Bytes:         bytes,
		Capacity:      c.maxBytes,
		Stored:        c.stored.Load(),
		Dropped:       c.dropped.Load(),
		Invalidations: c.invalidations.Load(),
	}
}
