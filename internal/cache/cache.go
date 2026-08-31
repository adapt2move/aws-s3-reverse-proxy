// Package cache holds upstream responses on local disk so that reading the
// same object twice costs the object store once.
//
// It is an HTTP response cache and nothing more. It has no idea what S3 is,
// which tenant asked, or whether anyone was allowed to: it takes an opaque
// key, a set of response headers and a payload, and hands them back. The
// decisions that matter for safety — that a lookup happens only after
// authorization, and that the key names the *upstream* object rather than
// the one the client asked for — are made by the caller, because they are
// the caller's to make.
//
// Underneath is internal/blobcache, which owns the disk. What this package
// adds is the three things a response cache needs and a blob store should not
// know about:
//
//   - what a cached response *is*: the headers to replay, the payload, and
//     when it was stored.
//   - when it stops being usable: an entry older than MaxAge is reported
//     stale rather than served.
//   - what happens when a write and an invalidation race: a response being
//     streamed into the cache while the object behind it is replaced must
//     not be committed, or the cache would end up holding a version that was
//     already superseded when it landed.
package cache

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/Kriechi/aws-s3-reverse-proxy/internal/blobcache"
)

// Options configures a Cache. Anything left at zero takes the default named
// in blobcache.Options, which is where the disk-shaped knobs are documented.
type Options struct {
	Dir           string
	MaxBytes      int64
	MaxEntries    int
	MaxObjectSize int64
	SegmentSize   int64
	InlineMaxSize int64

	// MaxAge is how long an entry may be served without being fetched
	// again. Zero means never: the cache is then only ever wrong if
	// something outside this process changes an object.
	MaxAge time.Duration

	Logf func(format string, args ...any)

	// Now is the clock, for tests.
	Now func() time.Time
}

// Result is what a lookup found. It is a string because it goes straight into
// a log field and a metric label.
type Result string

const (
	// Hit means the entry was returned and is within MaxAge.
	Hit Result = "hit"
	// Miss means there is no entry for this key.
	Miss Result = "miss"
	// Stale means an entry exists but has outlived MaxAge. The entry is
	// still returned, because there is something useful to do with it: ask
	// the upstream whether it is still current, which costs a round trip
	// rather than a transfer.
	Stale Result = "stale"
	// Revalidated means a stale entry was checked against the upstream and
	// found unchanged. It is reported separately from a hit because the two
	// cost different things, and a deployment tuning MaxAge wants to see
	// which one it is getting.
	Revalidated Result = "revalidated"
	// Bypass means the cache declined to answer: an upload for this key is
	// in flight, so neither what is cached nor what the upstream would
	// return right now is settled.
	Bypass Result = "bypass"
)

// Cache is a set of cached upstream responses on local disk. It is safe for
// concurrent use.
type Cache struct {
	store  *blobcache.Store
	maxAge time.Duration
	maxObj int64
	now    func() time.Time
	logf   func(string, ...any)

	// mu guards pending, which only ever holds the keys with a write or a
	// fill in flight — never the cache's contents.
	mu      sync.Mutex
	pending map[string]*pending

	// revalMu guards revalidated: when each key was last confirmed current
	// against the upstream. See Refresh.
	revalMu     sync.Mutex
	revalidated map[string]revalidation

	stored        atomic.Uint64
	dropped       atomic.Uint64
	invalidations atomic.Uint64
}

// pending is what is happening to one key right now, and is the whole of the
// coordination between writers, background fills and invalidation.
type pending struct {
	writers int
	filling bool

	// mutating counts the writes to this key that are in flight upstream.
	// While there are any, the key is neither read from nor written to the
	// cache — see Mutation.
	mutating int

	// contested records that more than one write to this key has been in
	// flight at once since the last time the key was quiet. Which of them
	// the object store keeps is the object store's business and is not
	// visible from here, so none of their bodies may be cached.
	contested bool

	// poisoned records that the object behind this key changed while a
	// response for it was being written. Committing that response would put
	// a version into the cache that was already out of date when it
	// arrived, so the write is thrown away instead.
	poisoned bool
}

// New opens or creates a cache in opts.Dir.
func New(opts Options) (*Cache, error) {
	if opts.Now == nil {
		opts.Now = time.Now
	}
	store, err := blobcache.Open(blobcache.Options{
		Dir:           opts.Dir,
		MaxBytes:      opts.MaxBytes,
		MaxEntries:    opts.MaxEntries,
		MaxObjectSize: opts.MaxObjectSize,
		SegmentSize:   opts.SegmentSize,
		InlineMaxSize: opts.InlineMaxSize,
		SyncDeletes:   true,
		Logf:          opts.Logf,
	})
	if err != nil {
		return nil, fmt.Errorf("cache: %w", err)
	}
	logf := opts.Logf
	if logf == nil {
		logf = func(string, ...any) {}
	}
	return &Cache{
		store:       store,
		maxAge:      opts.MaxAge,
		maxObj:      store.MaxObjectSize(),
		now:         opts.Now,
		logf:        logf,
		pending:     make(map[string]*pending),
		revalidated: make(map[string]revalidation),
	}, nil
}

// Close stops the cache. Entries already handed out keep working until they
// are closed.
func (c *Cache) Close() error { return c.store.Close() }

// Entry is a cached response, open for reading.
//
// The payload is a ReadSeeker so that http.ServeContent can answer a Range
// request out of it — which is not a nicety for this proxy's workload but the
// whole of it, since a client like DuckDB's httpfs reads objects almost
// exclusively in ranges.
//
// Close it.
type Entry struct {
	// Header is the response headers to replay, exactly as the upstream
	// sent them.
	Header http.Header

	// key and storedAt identify which version of the object this is, so
	// that a revalidation can be tied to the entry it actually checked.
	key string

	// Size is the payload length in bytes.
	Size int64

	// StoredAt is when the response was written into the cache, and Age is
	// how long ago that was.
	StoredAt time.Time
	Age      time.Duration

	body *blobcache.Reader
}

// revalidation is one entry having been confirmed current: when that was, and
// which version of the object it was about.
//
// The version matters. A revalidation that starts before a write and finishes
// after it confirmed the version the write replaced, and without pinning it to
// a version it would make the new entry look fresher than it is.
type revalidation struct {
	storedAt int64
	at       int64
}

// Body is the payload.
func (e *Entry) Body() io.ReadSeeker { return e.body }

// ETag is the entity tag the upstream gave this response, quotes included,
// or empty if it gave none.
func (e *Entry) ETag() string { return e.Header.Get("ETag") }

// Close releases the file the payload is read from.
func (e *Entry) Close() error { return e.body.Close() }

// Get looks up a cached response.
//
// The entry is returned for both Hit and Stale. A stale one is not useless:
// its ETag is what lets the caller ask the upstream whether the object has
// changed, which costs a round trip instead of a transfer. Only Miss and
// Bypass come back without one.
//
// Close the entry.
func (c *Cache) Get(key string) (*Entry, Result) {
	if c.busy(key) {
		return nil, Bypass
	}
	reader, err := c.store.Get(key)
	if err != nil {
		return nil, Miss
	}
	meta, err := decodeMeta(reader.Meta())
	if err != nil || meta.Size != reader.Size() {
		// The payload and what is recorded about it disagree. That should
		// be impossible — they are committed as one record — so treat it as
		// corruption rather than as a cache entry, and get rid of it.
		_ = reader.Close()
		c.logf("cache: discarding an entry whose metadata does not describe it: %s", key)
		_ = c.store.Delete(key)
		return nil, Miss
	}

	// Freshness runs from whenever the object was last known to be current,
	// which is the later of when it was fetched and when it was last
	// confirmed unchanged.
	knownCurrent := meta.StoredAt
	if at := c.revalidatedAt(key, meta.StoredAt); at > knownCurrent {
		knownCurrent = at
	}
	entry := &Entry{
		Header:   meta.Header,
		key:      key,
		Size:     meta.Size,
		StoredAt: time.Unix(0, meta.StoredAt),
		Age:      c.now().Sub(time.Unix(0, knownCurrent)),
		body:     reader,
	}
	if c.maxAge > 0 && entry.Age > c.maxAge {
		return entry, Stale
	}
	return entry, Hit
}

// Refresh records that this entry was checked against the upstream just now
// and found unchanged, so it is current again without having been fetched
// again.
//
// It takes the entry rather than a key so that the record is tied to the
// version it was about: a revalidation of the object a write is in the middle
// of replacing must not make the replacement look fresher than it is, and
// there is no ordering between the two that the caller could arrange instead.
//
// It is kept in memory rather than written back into the entry, because the
// payload and its metadata are one record on disk and restamping it would
// mean rewriting the object. Losing these timestamps on restart costs one
// revalidation per object, which is what a restart costs anyway.
func (c *Cache) Refresh(entry *Entry) {
	if c.maxAge <= 0 || entry == nil || entry.key == "" {
		return
	}
	now := c.now().UnixNano()
	c.revalMu.Lock()
	defer c.revalMu.Unlock()
	if len(c.revalidated) >= maxRevalidationRecords {
		c.pruneRevalidatedLocked(now)
	}
	c.revalidated[entry.key] = revalidation{storedAt: entry.StoredAt.UnixNano(), at: now}
}

// maxRevalidationRecords bounds the table above. It holds one small entry per
// object that has outlived MaxAge and been confirmed current since, which is
// far fewer than the cache holds — but it is memory the store does not
// account for, so it gets a ceiling.
const maxRevalidationRecords = 1 << 16

func (c *Cache) pruneRevalidatedLocked(now int64) {
	cutoff := now - int64(c.maxAge)
	for key, record := range c.revalidated {
		if record.at < cutoff {
			delete(c.revalidated, key)
		}
	}
	if len(c.revalidated) >= maxRevalidationRecords {
		// All of it is recent, so there is nothing stale to drop. Throwing
		// the table away costs revalidations and never correctness.
		clear(c.revalidated)
	}
}

// revalidatedAt reports when this exact version of the object was last
// confirmed current, and zero when the record on file is about another one.
func (c *Cache) revalidatedAt(key string, storedAt int64) int64 {
	if c.maxAge <= 0 {
		return 0
	}
	c.revalMu.Lock()
	defer c.revalMu.Unlock()
	record, ok := c.revalidated[key]
	if !ok || record.storedAt != storedAt {
		return 0
	}
	return record.at
}

func (c *Cache) forgetRevalidation(keys ...string) {
	if c.maxAge <= 0 {
		return
	}
	c.revalMu.Lock()
	defer c.revalMu.Unlock()
	for _, key := range keys {
		delete(c.revalidated, key)
	}
}

// Has reports whether a fresh entry exists, without opening it.
func (c *Cache) Has(key string) bool {
	entry, result := c.Get(key)
	if entry != nil {
		_ = entry.Close()
	}
	return result == Hit
}

// Put starts capturing a response under key. size is the payload length when
// it is known and negative when it is not.
//
// The caller writes the payload to the Writer and calls Commit with the
// response headers, or Abort. Abort after Commit does nothing, so it is safe
// to defer.
func (c *Cache) Put(key string, size int64) (*Writer, error) {
	if err := c.claim(key); err != nil {
		return nil, err
	}
	w, err := c.store.Put(key, size)
	if err != nil {
		c.unclaim(key)
		return nil, err
	}
	return &Writer{c: c, key: key, w: w, expect: size}, nil
}

// claim reserves the right to capture a response for this key.
//
// One capture per key at a time. Concurrent readers of the same cold object
// each get their own response and each stream it to their own client — none
// of them is made to wait, which is the rule — but only the first of them
// writes it down. The others would append a second copy that the index would
// immediately supersede, so the store would carry the cost of writing bytes
// it was always going to reclaim, and each of them would hold its own
// preallocated file while doing it.
func (c *Cache) claim(key string) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	p, ok := c.pending[key]
	if !ok {
		p = &pending{}
		c.pending[key] = p
	}
	if p.mutating > 0 {
		return ErrBusyKey
	}
	if p.writers > 0 {
		c.forgetLocked(key, p)
		return ErrAlreadyCapturing
	}
	p.writers++
	return nil
}

func (c *Cache) unclaim(key string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if p := c.pending[key]; p != nil {
		p.writers--
		c.forgetLocked(key, p)
	}
}

// contested reports whether more than one write to this key has been in
// flight at once since the key was last quiet.
func (c *Cache) contested(key string) bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	p := c.pending[key]
	return p == nil || p.contested
}

// busy reports whether a write to this key is in flight upstream.
func (c *Cache) busy(key string) bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	p := c.pending[key]
	return p != nil && p.mutating > 0
}

// Invalidate drops the cached responses for these keys, and marks any
// response currently being written for them so that it is thrown away rather
// than committed.
//
// That second half is the part that is easy to miss. A read that started
// before an object was replaced can finish after it was, and committing what
// it fetched would leave the cache holding a version that was already
// superseded when it landed — the one way a cache in front of a single writer
// can go stale on its own.
func (c *Cache) Invalidate(keys ...string) {
	if len(keys) == 0 {
		return
	}
	c.mu.Lock()
	for _, key := range keys {
		if p := c.pending[key]; p != nil {
			p.poisoned = true
		}
	}
	c.mu.Unlock()
	for _, key := range keys {
		_ = c.store.Delete(key)
	}
	c.forgetRevalidation(keys...)
	c.invalidations.Add(uint64(len(keys)))
}

// Purge empties the cache. It is the escape hatch for the case the cache
// cannot detect on its own: something outside this process changed the data,
// and there is no list of which keys.
func (c *Cache) Purge() error {
	c.mu.Lock()
	for _, p := range c.pending {
		p.poisoned = true
	}
	c.mu.Unlock()
	c.revalMu.Lock()
	clear(c.revalidated)
	c.revalMu.Unlock()
	return c.store.Purge()
}

// ClaimFill reserves the right to populate key in the background, and reports
// false when somebody else already has it.
//
// Without it, a burst of ranged reads of the same cold object — which is
// exactly what a query engine does when it opens a file — would each start
// their own full fetch of it.
func (c *Cache) ClaimFill(key string) bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	p, ok := c.pending[key]
	if !ok {
		p = &pending{}
		c.pending[key] = p
	}
	if p.filling {
		return false
	}
	p.filling = true
	return true
}

// ReleaseFill gives back what ClaimFill reserved.
func (c *Cache) ReleaseFill(key string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if p := c.pending[key]; p != nil {
		p.filling = false
		c.forgetLocked(key, p)
	}
}

// forgetLocked drops a key from the pending set once nothing is happening to
// it. The map holds work in flight, never the cache's contents, so it stays
// as small as the concurrency.
func (c *Cache) forgetLocked(key string, p *pending) {
	if p.writers == 0 && p.mutating == 0 && !p.filling {
		delete(c.pending, key)
	}
}

// Stats reports what the cache holds. The hit and miss counters live with the
// caller, which is where the request that produced them is.
type Stats struct {
	Entries       int
	Bytes         int64
	Capacity      int64
	MaxObjectSize int64
	Stored        uint64
	Dropped       uint64
	Invalidations uint64
	Evictions     uint64
	EvictedBytes  uint64
	WritesDropped uint64
}

func (c *Cache) Stats() Stats {
	s := c.store.Stats()
	return Stats{
		Entries:       s.Entries,
		Bytes:         s.Bytes,
		Capacity:      s.Capacity,
		MaxObjectSize: c.maxObj,
		Stored:        c.stored.Load(),
		Dropped:       c.dropped.Load(),
		Invalidations: c.invalidations.Load(),
		Evictions:     s.Evictions,
		EvictedBytes:  s.EvictedBytes,
		WritesDropped: s.DroppedWrites,
	}
}

// MaxObjectSize is the largest payload the cache will accept, so a caller can
// decide not to start something the cache would refuse.
func (c *Cache) MaxObjectSize() int64 { return c.maxObj }

// Storable reports whether a response may be cached at all.
//
// Only `no-store` and `private` are honoured, and both are honoured because
// they are statements about the object rather than about the transfer: an
// operator who marked an object that way meant it, and this proxy is a shared
// cache. Freshness directives (`max-age`, `no-cache`) are deliberately
// ignored — how long an entry may be served is the deployment's decision,
// made once in the configuration, not one an object's own metadata gets to
// make on its behalf.
func Storable(header http.Header) bool {
	for _, value := range header.Values("Cache-Control") {
		for _, directive := range strings.Split(value, ",") {
			switch strings.ToLower(strings.TrimSpace(directive)) {
			case "no-store", "private":
				return false
			}
		}
	}
	return true
}

// storedMeta is what travels beside a payload. It is JSON because it is read
// once per hit, is a few hundred bytes, and being able to read a cache entry
// with `strings` when something is wrong is worth more than the microsecond.
type storedMeta struct {
	Header   http.Header `json:"header"`
	Size     int64       `json:"size"`
	StoredAt int64       `json:"stored_at"`
}

func decodeMeta(raw []byte) (storedMeta, error) {
	var meta storedMeta
	if err := json.Unmarshal(raw, &meta); err != nil {
		return meta, err
	}
	if meta.Header == nil {
		meta.Header = http.Header{}
	}
	return meta, nil
}

var (
	errShortWrite = errors.New("cache: the response was shorter than it announced")

	// ErrBusyKey is returned by Put while an upload for the same key is in
	// flight. It is not a failure: the response is simply not cached, and
	// the caller carries on serving it.
	ErrBusyKey = errors.New("cache: a write to this key is in flight")

	// ErrAlreadyCapturing is returned by Put when another response for the
	// same key is already being captured. Also not a failure: one of the
	// concurrent readers stores the object and the rest simply serve it.
	ErrAlreadyCapturing = errors.New("cache: this key is already being captured")
)
