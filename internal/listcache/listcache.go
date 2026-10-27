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
//
// Dropping a listing on every write would leave a prefix that is written to
// often without a fallback most of the time. So a write also reports the
// listings it dropped, as Stale, and the caller may ask the object store for
// them again in the background. The answer goes through the same capture as
// any other — a write overlapping it keeps it from being remembered — so a
// refreshed listing is the object store's answer and never one assembled
// here.
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
	refreshed     atomic.Uint64
	dropped       atomic.Uint64
	invalidations atomic.Uint64
}

type entry struct {
	key      string
	scope    string
	origin   string
	header   http.Header
	body     []byte
	storedAt time.Time
	// lastUsed is when a client last asked for this listing. A refresh does
	// not count: it keeps an answer current for clients, and one no client
	// has asked for in MaxAge is not worth keeping current.
	lastUsed time.Time
	cost     int64
	elem     *list.Element
}

// Stale is a remembered listing that a write just dropped, with what it takes
// to ask the object store for it again.
type Stale struct {
	Key   string
	Scope string
	// Origin is what the caller passed to Begin to say where the listing
	// came from — for the proxy, the upstream URL.
	Origin string

	lastUsed time.Time
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
	e.lastUsed = c.now()
	return &Listing{Header: e.header.Clone(), Body: e.body, Age: age}
}

// Capture is one listing on its way from the object store, which may become
// the remembered answer once it has arrived. Exactly one of Commit, Discard
// or Abort ends it; the later ones do nothing.
type Capture struct {
	c        *Cache
	key      string
	scope    string
	origin   string
	poisoned bool
	done     bool

	// refresh marks a capture nobody asked for; lastUsed is carried over
	// from the listing it refreshes rather than set to now.
	refresh  bool
	lastUsed time.Time
}

// Begin registers a client's listing that is about to be sent upstream. It
// has to be called before the request is sent, not when the answer arrives:
// a write that starts in between would otherwise go unnoticed.
//
// origin is kept with the answer and handed back in Stale, so the listing can
// be asked for again without the client.
func (c *Cache) Begin(key, scope, origin string) *Capture {
	c.mu.Lock()
	defer c.mu.Unlock()
	cp := &Capture{c: c, key: key, scope: scope, origin: origin, lastUsed: c.now()}
	cp.poisoned = c.writingUnderLocked(scope)
	c.captures[cp] = struct{}{}
	return cp
}

// BeginRefresh registers a background listing of what a write dropped. It
// reports false, and registers nothing, while a write under the listing's
// scope is in flight: that answer could not be remembered anyway, and asking
// for it would only cost a request.
func (c *Cache) BeginRefresh(s Stale) (*Capture, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.writingUnderLocked(s.Scope) {
		return nil, false
	}
	cp := &Capture{c: c, key: s.Key, scope: s.Scope, origin: s.Origin, refresh: true, lastUsed: s.lastUsed}
	c.captures[cp] = struct{}{}
	return cp, true
}

// writingUnderLocked reports whether a write under scope is in flight, so an
// answer for it now may be from either side of that write.
func (c *Cache) writingUnderLocked(scope string) bool {
	for object := range c.writes {
		if strings.HasPrefix(object, scope) {
			return true
		}
	}
	return false
}

// Overlapped reports whether a write under the capture's scope overlapped it,
// which is the reason an answer the object store gave was not remembered.
func (cp *Capture) Overlapped() bool {
	cp.c.mu.Lock()
	defer cp.c.mu.Unlock()
	return cp.poisoned
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
		origin:   cp.origin,
		header:   header.Clone(),
		body:     body,
		storedAt: c.now(),
		lastUsed: cp.lastUsed,
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
	if cp.refresh {
		c.refreshed.Add(1)
	}
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
//
// It returns the dropped listings worth asking for again: those a client has
// asked for within MaxAge.
func (w *Write) End(refused bool) []Stale {
	if w == nil || w.ended {
		return nil
	}
	w.ended = true
	c := w.c
	c.mu.Lock()
	defer c.mu.Unlock()
	var stale []Stale
	for _, key := range w.keys {
		if c.writes[key]--; c.writes[key] <= 0 {
			delete(c.writes, key)
		}
		if !refused {
			stale = c.invalidateLocked(key, stale)
		}
	}
	return stale
}

// Invalidate drops every listing covering these object keys, and keeps any
// listing in flight under them from being remembered. It is for a change
// this process learned about without having made it, and returns what End
// would.
func (c *Cache) Invalidate(keys ...string) []Stale {
	c.mu.Lock()
	defer c.mu.Unlock()
	var stale []Stale
	for _, key := range keys {
		c.poisonCapturesLocked(key)
		stale = c.invalidateLocked(key, stale)
	}
	return stale
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

func (c *Cache) invalidateLocked(object string, stale []Stale) []Stale {
	now := c.now()
	for scope, entries := range c.byScope {
		if !strings.HasPrefix(object, scope) {
			continue
		}
		for _, e := range entries {
			c.removeLocked(e)
			c.invalidations.Add(1)
			if e.origin != "" && now.Sub(e.lastUsed) <= c.maxAge {
				stale = append(stale, Stale{Key: e.key, Scope: e.scope, Origin: e.origin, lastUsed: e.lastUsed})
			}
		}
	}
	return stale
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
	Refreshed     uint64
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
		Refreshed:     c.refreshed.Load(),
		Dropped:       c.dropped.Load(),
		Invalidations: c.invalidations.Load(),
	}
}
