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
	require.True(t, c.Begin(key, scope, "origin-of-"+key).Commit(xmlHeader, []byte(body)))
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
	w.End(Answered)

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
	c.BeginWrite("bucket/t/data/a.csv").End(Refused)
	assert.NotNil(t, c.Get("k"))
}

// A listing that was in flight while a write was may have been answered from
// either side of it. Remembering it could serve the pre-write state after
// the write was acknowledged.
func TestAListingThatOverlapsAWriteIsNotRemembered(t *testing.T) {
	t.Run("the write starts while the listing is in flight", func(t *testing.T) {
		c, _ := newTestCache(t)
		listing := c.Begin("k", "bucket/t/data/", "")
		w := c.BeginWrite("bucket/t/data/a.csv")
		w.End(Answered)
		assert.False(t, listing.Commit(xmlHeader, []byte("<old/>")))
		assert.Nil(t, c.Get("k"))
	})
	t.Run("the listing starts while the write is in flight", func(t *testing.T) {
		c, _ := newTestCache(t)
		w := c.BeginWrite("bucket/t/data/a.csv")
		listing := c.Begin("k", "bucket/t/data/", "")
		w.End(Answered)
		assert.False(t, listing.Commit(xmlHeader, []byte("<old/>")))
	})
	t.Run("even when the write is refused in the end", func(t *testing.T) {
		// Whether it was refused is only known at the end; by then the
		// listing may already have been answered with it half-applied.
		c, _ := newTestCache(t)
		listing := c.Begin("k", "bucket/t/data/", "")
		c.BeginWrite("bucket/t/data/a.csv").End(Refused)
		assert.False(t, listing.Commit(xmlHeader, []byte("<old/>")))
	})
	t.Run("a write elsewhere does not get in the way", func(t *testing.T) {
		c, _ := newTestCache(t)
		listing := c.Begin("k", "bucket/t/data/", "")
		c.BeginWrite("bucket/t/logs/a.log").End(Answered)
		assert.True(t, listing.Commit(xmlHeader, []byte("<new/>")))
	})
	t.Run("a listing after the write has ended is remembered", func(t *testing.T) {
		c, _ := newTestCache(t)
		c.BeginWrite("bucket/t/data/a.csv").End(Answered)
		assert.True(t, c.Begin("k", "bucket/t/data/", "").Commit(xmlHeader, []byte("<new/>")))
	})
}

func TestDiscardForgetsTheRememberedAnswer(t *testing.T) {
	c, _ := newTestCache(t)
	remember(t, c, "k", "bucket/t/", "<a/>")
	c.Begin("k", "bucket/t/", "").Discard()
	assert.Nil(t, c.Get("k"))
}

func TestAbortKeepsTheRememberedAnswer(t *testing.T) {
	c, _ := newTestCache(t)
	remember(t, c, "k", "bucket/t/", "<a/>")
	c.Begin("k", "bucket/t/", "").Abort()
	assert.NotNil(t, c.Get("k"))
}

func TestInvalidateDropsAndPoisons(t *testing.T) {
	c, _ := newTestCache(t)
	remember(t, c, "k", "bucket/t/", "<a/>")
	listing := c.Begin("k2", "bucket/t/data/", "")
	c.Invalidate("bucket/t/data/a.csv")
	assert.Nil(t, c.Get("k"))
	assert.False(t, listing.Commit(xmlHeader, []byte("<old/>")))
}

func TestPurgeForgetsEverything(t *testing.T) {
	c, _ := newTestCache(t)
	remember(t, c, "k", "bucket/t/", "<a/>")
	listing := c.Begin("k2", "bucket/u/", "")
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

	assert.False(t, c.Begin("e", "bucket/e/", "").Commit(xmlHeader, make([]byte, 8192)))
	assert.Nil(t, c.Get("e"), "an answer too large to keep still replaces the one before it")
}

func TestEndIsIdempotentAndNilSafe(t *testing.T) {
	c, _ := newTestCache(t)
	w := c.BeginWrite("bucket/t/a")
	w.End(Answered)
	w.End(Answered)
	var none *Write
	none.End(Answered)
	assert.Nil(t, c.BeginWrite())

	c.mu.Lock()
	defer c.mu.Unlock()
	assert.Empty(t, c.writes, "a write that ended holds nothing")
	assert.Empty(t, c.captures)
}

// A write hands back what it dropped, so the caller can ask the object store
// for it again instead of leaving the prefix without a fallback.
func TestAWriteReportsTheListingsItDropped(t *testing.T) {
	c, _ := newTestCache(t)
	remember(t, c, "covering", "bucket/t/data/", "<a/>")
	remember(t, c, "sibling", "bucket/t/logs/", "<a/>")

	stale := c.BeginWrite("bucket/t/data/a.csv").End(Answered)
	require.Len(t, stale, 1)
	assert.Equal(t, "covering", stale[0].Key)
	assert.Equal(t, "bucket/t/data/", stale[0].Scope)
	assert.Equal(t, "origin-of-covering", stale[0].Origin)

	assert.Empty(t, c.BeginWrite("bucket/t/data/b.csv").End(Answered), "nothing is left to drop")
	remember(t, c, "covering", "bucket/t/data/", "<a/>")
	assert.Empty(t, c.BeginWrite("bucket/t/data/c.csv").End(Refused), "a refused write drops nothing")
	assert.Len(t, c.Invalidate("bucket/t/data/c.csv"), 1)
}

// Refreshing is for listings clients use. One nobody asked for in MaxAge is
// not kept current forever just because the prefix is written to.
func TestOnlyListingsInUseAreWorthRefreshing(t *testing.T) {
	c, clk := newTestCache(t)
	remember(t, c, "k", "bucket/t/", "<a/>")
	stale := c.BeginWrite("bucket/t/a").End(Answered)
	require.Len(t, stale, 1)

	// Refreshed, then left alone by clients for longer than MaxAge.
	refresh, ok := c.BeginRefresh(stale[0])
	require.True(t, ok)
	require.True(t, refresh.Commit(xmlHeader, []byte("<b/>")))
	assert.Equal(t, uint64(1), c.Stats().Refreshed)
	clk.advance(30 * time.Minute)
	stale = c.BeginWrite("bucket/t/b").End(Answered)
	require.Len(t, stale, 1, "still within MaxAge of the last client")
	refresh, _ = c.BeginRefresh(stale[0])
	require.True(t, refresh.Commit(xmlHeader, []byte("<c/>")))

	clk.advance(31 * time.Minute)
	assert.Empty(t, c.BeginWrite("bucket/t/c").End(Answered),
		"a refresh does not count as use: an hour after the last client, it stops")

	// A client asking again makes it worth keeping current again.
	remember(t, c, "k", "bucket/t/", "<d/>")
	assert.Len(t, c.BeginWrite("bucket/t/d").End(Answered), 1)
}

func TestARefreshWaitsForWritesInFlight(t *testing.T) {
	c, _ := newTestCache(t)
	remember(t, c, "k", "bucket/t/", "<a/>")
	stale := c.BeginWrite("bucket/t/a").End(Answered)
	require.Len(t, stale, 1)

	w := c.BeginWrite("bucket/t/b")
	_, ok := c.BeginRefresh(stale[0])
	assert.False(t, ok, "while a write is in flight the answer could not be remembered anyway")
	w.End(Answered)

	refresh, ok := c.BeginRefresh(stale[0])
	require.True(t, ok)
	c.BeginWrite("bucket/t/c").End(Refused)
	assert.False(t, refresh.Commit(xmlHeader, []byte("<b/>")))
	assert.True(t, refresh.Overlapped(), "and says why, so the caller can try again later")
}

// A write that was sent and never answered may still land, minutes later.
// Until the hold has passed, nothing under it is remembered, and nothing is
// handed back to be refreshed: a refresh would remember the listing from
// before it landed.
func TestAnUnresolvedWriteHoldsItsKeys(t *testing.T) {
	clk := &clock{now: time.Unix(1_700_000_000, 0)}
	c := New(Options{MaxBytes: 1 << 20, MaxAge: time.Hour, UnresolvedHold: 50 * time.Millisecond, Now: clk.Now})
	remember(t, c, "k", "bucket/t/", "<a/>")

	assert.Empty(t, c.BeginWrite("bucket/t/a").End(Unresolved), "nothing to refresh")
	assert.Nil(t, c.Get("k"), "but dropped all the same")
	assert.False(t, c.Begin("k", "bucket/t/", "").Commit(xmlHeader, []byte("<before/>")),
		"a listing now may be from before the write lands")
	_, ok := c.BeginRefresh(Stale{Key: "k", Scope: "bucket/t/"})
	assert.False(t, ok)

	require.Eventually(t, func() bool {
		return c.Begin("k", "bucket/t/", "").Commit(xmlHeader, []byte("<after/>"))
	}, time.Second, 10*time.Millisecond, "after the hold the claim is given back")
}

func TestTheHoldDefaultsToMaxAge(t *testing.T) {
	c := New(Options{MaxBytes: 1 << 20, MaxAge: time.Hour})
	assert.Equal(t, time.Hour, c.hold)
}

// What an entry costs is what it holds on to: the origin it was listed from,
// and the whole buffer the body was read into.
func TestAnEntryCostsWhatItHolds(t *testing.T) {
	c, _ := newTestCache(t)
	body := make([]byte, 100, 4096)
	origin := "http://store/bucket?list-type=2&prefix=" + string(make([]byte, 500))
	require.True(t, c.Begin("k", "bucket/t/", origin).Commit(xmlHeader, body))
	assert.GreaterOrEqual(t, c.Stats().Bytes, int64(4096+len(origin)))
}
