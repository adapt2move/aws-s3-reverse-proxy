package listcache

import (
	"net/http"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type clock struct {
	mu  sync.Mutex
	now time.Time
}

func (c *clock) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.now
}

func (c *clock) advance(d time.Duration) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.now = c.now.Add(d)
}

func newTestCache(t *testing.T) (*Cache, *clock) {
	t.Helper()
	clk := &clock{now: time.Unix(1_700_000_000, 0)}
	return New(Options{MaxBytes: 1 << 20, MaxAge: time.Hour, Now: clk.Now}), clk
}

var xmlHeader = http.Header{"Content-Type": {"application/xml"}}

// remember runs a listing through the whole capture, the way the proxy does.
func remember(t *testing.T, c *Cache, key, scope, body string) {
	t.Helper()
	require.True(t, c.Begin(key, scope).Commit(xmlHeader, []byte(body)))
}

func TestARememberedListingIsServed(t *testing.T) {
	c, clk := newTestCache(t)
	remember(t, c, "bucket?prefix=t/data/", "bucket/t/data/", "<a/>")
	clk.advance(5 * time.Minute)

	got := c.Get("bucket?prefix=t/data/")
	require.NotNil(t, got)
	assert.Equal(t, "<a/>", string(got.Body))
	assert.Equal(t, "application/xml", got.Header.Get("Content-Type"))
	assert.Equal(t, 5*time.Minute, got.Age)
	assert.Nil(t, c.Get("bucket?prefix=t/other/"))
}

func TestNothingOlderThanMaxAgeIsServed(t *testing.T) {
	c, clk := newTestCache(t)
	remember(t, c, "k", "bucket/t/", "<a/>")
	clk.advance(time.Hour + time.Second)
	assert.Nil(t, c.Get("k"))
	assert.Equal(t, 0, c.Stats().Entries, "an expired listing is let go, not kept around")
}

// The rule the package exists for: a write that may have changed an object
// takes every listing that would show it with it.
func TestAWriteDropsTheListingsThatCoverIt(t *testing.T) {
	c, _ := newTestCache(t)
	remember(t, c, "covering", "bucket/t/data/", "<a/>")
	remember(t, c, "covering-root", "bucket/t/", "<a/>")
	remember(t, c, "sibling", "bucket/t/logs/", "<a/>")
	remember(t, c, "other-bucket", "other/t/data/", "<a/>")

	w := c.BeginWrite("bucket/t/data/2026/a.csv")
	assert.NotNil(t, c.Get("covering"), "until the object store has answered, the write has not happened")
	w.End(false)

	assert.Nil(t, c.Get("covering"))
	assert.Nil(t, c.Get("covering-root"))
	assert.NotNil(t, c.Get("sibling"), "a listing of another prefix does not show the object")
	assert.NotNil(t, c.Get("other-bucket"))
}

// An outage is exactly when writes fail, and a write that failed changed
// nothing. Dropping the listings anyway would take the fallback away when it
// is needed.
func TestARefusedWriteKeepsTheListings(t *testing.T) {
	c, _ := newTestCache(t)
	remember(t, c, "k", "bucket/t/data/", "<a/>")
	c.BeginWrite("bucket/t/data/a.csv").End(true)
	assert.NotNil(t, c.Get("k"))
}

// A listing that was in flight while a write was may have been answered from
// either side of it. Remembering it could serve the pre-write state after
// the write was acknowledged.
func TestAListingThatOverlapsAWriteIsNotRemembered(t *testing.T) {
	t.Run("the write starts while the listing is in flight", func(t *testing.T) {
		c, _ := newTestCache(t)
		listing := c.Begin("k", "bucket/t/data/")
		w := c.BeginWrite("bucket/t/data/a.csv")
		w.End(false)
		assert.False(t, listing.Commit(xmlHeader, []byte("<old/>")))
		assert.Nil(t, c.Get("k"))
	})
	t.Run("the listing starts while the write is in flight", func(t *testing.T) {
		c, _ := newTestCache(t)
		w := c.BeginWrite("bucket/t/data/a.csv")
		listing := c.Begin("k", "bucket/t/data/")
		w.End(false)
		assert.False(t, listing.Commit(xmlHeader, []byte("<old/>")))
	})
	t.Run("even when the write is refused in the end", func(t *testing.T) {
		// Whether it was refused is only known at the end; by then the
		// listing may already have been answered with it half-applied.
		c, _ := newTestCache(t)
		listing := c.Begin("k", "bucket/t/data/")
		c.BeginWrite("bucket/t/data/a.csv").End(true)
		assert.False(t, listing.Commit(xmlHeader, []byte("<old/>")))
	})
	t.Run("a write elsewhere does not get in the way", func(t *testing.T) {
		c, _ := newTestCache(t)
		listing := c.Begin("k", "bucket/t/data/")
		c.BeginWrite("bucket/t/logs/a.log").End(false)
		assert.True(t, listing.Commit(xmlHeader, []byte("<new/>")))
	})
	t.Run("a listing after the write has ended is remembered", func(t *testing.T) {
		c, _ := newTestCache(t)
		c.BeginWrite("bucket/t/data/a.csv").End(false)
		assert.True(t, c.Begin("k", "bucket/t/data/").Commit(xmlHeader, []byte("<new/>")))
	})
}

func TestDiscardForgetsTheRememberedAnswer(t *testing.T) {
	c, _ := newTestCache(t)
	remember(t, c, "k", "bucket/t/", "<a/>")
	c.Begin("k", "bucket/t/").Discard()
	assert.Nil(t, c.Get("k"))
}

func TestAbortKeepsTheRememberedAnswer(t *testing.T) {
	c, _ := newTestCache(t)
	remember(t, c, "k", "bucket/t/", "<a/>")
	c.Begin("k", "bucket/t/").Abort()
	assert.NotNil(t, c.Get("k"))
}

func TestInvalidateDropsAndPoisons(t *testing.T) {
	c, _ := newTestCache(t)
	remember(t, c, "k", "bucket/t/", "<a/>")
	listing := c.Begin("k2", "bucket/t/data/")
	c.Invalidate("bucket/t/data/a.csv")
	assert.Nil(t, c.Get("k"))
	assert.False(t, listing.Commit(xmlHeader, []byte("<old/>")))
}

func TestPurgeForgetsEverything(t *testing.T) {
	c, _ := newTestCache(t)
	remember(t, c, "k", "bucket/t/", "<a/>")
	listing := c.Begin("k2", "bucket/u/")
	require.NoError(t, c.Purge())
	assert.Nil(t, c.Get("k"))
	assert.False(t, listing.Commit(xmlHeader, []byte("<old/>")), "a purge also covers what was in flight")
	entries, bytes := c.Size()
	assert.Zero(t, entries)
	assert.Zero(t, bytes)
}

func TestMemoryStaysWithinItsLimit(t *testing.T) {
	clk := &clock{now: time.Unix(1_700_000_000, 0)}
	c := New(Options{MaxBytes: 4096, MaxAge: time.Hour, Now: clk.Now})
	body := string(make([]byte, 1000))
	for _, key := range []string{"a", "b", "c", "d", "e"} {
		remember(t, c, key, "bucket/"+key+"/", body)
		assert.LessOrEqual(t, c.Stats().Bytes, int64(4096))
	}
	assert.Nil(t, c.Get("a"), "the least recently used listing goes first")
	assert.NotNil(t, c.Get("e"))

	assert.False(t, c.Begin("e", "bucket/e/").Commit(xmlHeader, make([]byte, 8192)))
	assert.Nil(t, c.Get("e"), "an answer too large to keep still replaces the one before it")
}

func TestEndIsIdempotentAndNilSafe(t *testing.T) {
	c, _ := newTestCache(t)
	w := c.BeginWrite("bucket/t/a")
	w.End(false)
	w.End(false)
	var none *Write
	none.End(false)
	assert.Nil(t, c.BeginWrite())

	c.mu.Lock()
	defer c.mu.Unlock()
	assert.Empty(t, c.writes, "a write that ended holds nothing")
	assert.Empty(t, c.captures)
}
