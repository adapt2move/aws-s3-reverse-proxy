package cache

import (
	"io"
	"net/http"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// clock is a hand-wound time source, so that a test about expiry does not
// have to wait for it.
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

func newCache(t *testing.T, tune ...func(*Options)) (*Cache, *clock) {
	t.Helper()
	tick := &clock{now: time.Date(2026, 3, 1, 12, 0, 0, 0, time.UTC)}
	opts := Options{
		Dir:           t.TempDir(),
		MaxBytes:      4 << 20,
		SegmentSize:   64 << 10,
		InlineMaxSize: 8 << 10,
		MaxObjectSize: 1 << 20,
		Now:           tick.Now,
		Logf:          func(format string, args ...any) { t.Logf("cache: "+format, args...) },
	}
	for _, f := range tune {
		f(&opts)
	}
	c, err := New(opts)
	require.NoError(t, err)
	t.Cleanup(func() { _ = c.Close() })
	return c, tick
}

func store(t *testing.T, c *Cache, key string, header http.Header, body []byte) {
	t.Helper()
	w, err := c.Put(key, int64(len(body)))
	require.NoError(t, err)
	defer w.Abort()
	_, err = w.Write(body)
	require.NoError(t, err)
	require.NoError(t, w.Commit(header))
}

// objectHeader is deliberately spelled the way a hand-built header is rather
// than the way net/http canonicalises one: "ETag" is what an operator writes
// and "Etag" is what Header.Get looks for, and an entry stored under the
// first would come back without an ETag at all.
func objectHeader() http.Header {
	return http.Header{
		"Content-Type":  {"application/vnd.apache.parquet"},
		"ETag":          {`"d41d8cd98f00b204e9800998ecf8427e"`},
		"Last-Modified": {"Sun, 01 Mar 2026 11:00:00 GMT"},
		"x-amz-meta-ow": {"team-a"},
	}
}

func TestStoreAndServe(t *testing.T) {
	c, _ := newCache(t)
	body := []byte("PAR1....data....PAR1")
	store(t, c, "bucket/tenant/a.parquet", objectHeader(), body)

	entry, result := c.Get("bucket/tenant/a.parquet")
	require.Equal(t, Hit, result)
	defer entry.Close()

	// The headers come back exactly as the upstream sent them: replaying a
	// response means replaying what it said about itself.
	assert.Equal(t, "application/vnd.apache.parquet", entry.Header.Get("Content-Type"))
	assert.Equal(t, `"d41d8cd98f00b204e9800998ecf8427e"`, entry.ETag())
	assert.Equal(t, "team-a", entry.Header.Get("X-Amz-Meta-Ow"))
	assert.Equal(t, int64(len(body)), entry.Size)

	got, err := io.ReadAll(entry.Body())
	require.NoError(t, err)
	assert.Equal(t, body, got)
}

func TestMissForAnUnknownKey(t *testing.T) {
	c, _ := newCache(t)
	entry, result := c.Get("bucket/tenant/nothing")
	assert.Nil(t, entry)
	assert.Equal(t, Miss, result)
}

func TestEntryGoesStale(t *testing.T) {
	c, tick := newCache(t, func(o *Options) { o.MaxAge = 5 * time.Minute })
	store(t, c, "k", objectHeader(), []byte("v"))

	entry, result := c.Get("k")
	require.Equal(t, Hit, result)
	assert.Equal(t, time.Duration(0), entry.Age)
	require.NoError(t, entry.Close())

	tick.advance(4 * time.Minute)
	entry, result = c.Get("k")
	require.Equal(t, Hit, result, "inside MaxAge it is still a hit")
	assert.Equal(t, 4*time.Minute, entry.Age)
	require.NoError(t, entry.Close())

	tick.advance(2 * time.Minute)
	entry, result = c.Get("k")
	assert.Equal(t, Stale, result)
	assert.Nil(t, entry, "a stale entry is not handed back, because there is nothing safe to do with one")
}

func TestNoMaxAgeMeansNeverStale(t *testing.T) {
	c, tick := newCache(t)
	store(t, c, "k", objectHeader(), []byte("v"))
	tick.advance(365 * 24 * time.Hour)
	_, result := c.Get("k")
	assert.Equal(t, Hit, result, "with no MaxAge the only thing that can invalidate an entry is a write")
}

func TestTruncatedResponseIsNotStored(t *testing.T) {
	c, _ := newCache(t)

	// The client hung up halfway. Caching what arrived would answer the next
	// read with a truncated object, which is worse than not caching at all.
	w, err := c.Put("k", 100)
	require.NoError(t, err)
	defer w.Abort()
	_, err = w.Write([]byte("only twenty bytes..."))
	require.NoError(t, err)
	err = w.Commit(objectHeader())
	assert.ErrorIs(t, err, errShortWrite)

	_, result := c.Get("k")
	assert.Equal(t, Miss, result)
	assert.Equal(t, uint64(1), c.Stats().Dropped)
}

func TestUnknownLengthIsStoredAsWritten(t *testing.T) {
	c, _ := newCache(t)
	w, err := c.Put("k", -1)
	require.NoError(t, err)
	defer w.Abort()
	_, err = w.Write([]byte("however long it turns out to be"))
	require.NoError(t, err)
	require.NoError(t, w.Commit(objectHeader()))

	entry, result := c.Get("k")
	require.Equal(t, Hit, result)
	defer entry.Close()
	assert.Equal(t, int64(31), entry.Size)
}

func TestInvalidateRemovesTheEntry(t *testing.T) {
	c, _ := newCache(t)
	store(t, c, "a", objectHeader(), []byte("1"))
	store(t, c, "b", objectHeader(), []byte("2"))

	c.Invalidate("a", "b")
	_, ra := c.Get("a")
	_, rb := c.Get("b")
	assert.Equal(t, Miss, ra)
	assert.Equal(t, Miss, rb)
	assert.Equal(t, uint64(2), c.Stats().Invalidations)
}

func TestInvalidationDuringAWriteDiscardsIt(t *testing.T) {
	c, _ := newCache(t)

	// This is the race the whole pending map exists for: a read of an object
	// starts, the object is replaced, and the read finishes afterwards
	// carrying the version that was current when it began. Committing it
	// would leave the cache holding a version that was already superseded
	// when it landed — and, with no MaxAge, holding it indefinitely.
	w, err := c.Put("k", 5)
	require.NoError(t, err)
	defer w.Abort()
	_, err = w.Write([]byte("older"))
	require.NoError(t, err)

	c.Invalidate("k") // the object behind it was just replaced

	require.NoError(t, w.Commit(objectHeader()), "a discarded capture is not an error for the caller")
	_, result := c.Get("k")
	assert.Equal(t, Miss, result, "the superseded response must not have been stored")
	assert.Equal(t, uint64(1), c.Stats().Dropped)

	// And the poison does not outlive the writes it was aimed at.
	store(t, c, "k", objectHeader(), []byte("newer"))
	entry, result := c.Get("k")
	require.Equal(t, Hit, result)
	defer entry.Close()
	body, err := io.ReadAll(entry.Body())
	require.NoError(t, err)
	assert.Equal(t, "newer", string(body))
}

func TestPurgeEmptiesEverything(t *testing.T) {
	c, _ := newCache(t)
	store(t, c, "a", objectHeader(), []byte("1"))
	store(t, c, "b", objectHeader(), []byte("2"))

	require.NoError(t, c.Purge())
	_, ra := c.Get("a")
	_, rb := c.Get("b")
	assert.Equal(t, Miss, ra)
	assert.Equal(t, Miss, rb)
	assert.Equal(t, 0, c.Stats().Entries)

	store(t, c, "c", objectHeader(), []byte("3"))
	assert.True(t, c.Has("c"), "the cache is usable again straight away")
}

func TestFillIsClaimedOnce(t *testing.T) {
	c, _ := newCache(t)

	// A query engine opening a file issues a burst of ranged reads of the
	// same cold object. Exactly one of them should go and fetch it.
	assert.True(t, c.ClaimFill("k"))
	assert.False(t, c.ClaimFill("k"))
	assert.True(t, c.ClaimFill("other"))

	c.ReleaseFill("k")
	assert.True(t, c.ClaimFill("k"), "once the fill is done another may start")
	c.ReleaseFill("k")
	c.ReleaseFill("other")

	// The map tracks work in flight, not contents, so it empties out.
	c.mu.Lock()
	defer c.mu.Unlock()
	assert.Empty(t, c.pending)
}

func TestASpellingIsNotDecidedByMapOrder(t *testing.T) {
	c, _ := newCache(t)

	// The same header given under two spellings has to resolve the same way
	// every time, and to the one that was already written canonically.
	for i := 0; i < 20; i++ {
		key := "k"
		w, err := c.Put(key, 1)
		require.NoError(t, err)
		_, err = w.Write([]byte("x"))
		require.NoError(t, err)
		require.NoError(t, w.Commit(http.Header{
			"Etag": {`"canonical"`},
			"ETag": {`"as written"`},
		}))
		entry, result := c.Get(key)
		require.Equal(t, Hit, result)
		assert.Equal(t, `"canonical"`, entry.ETag())
		require.NoError(t, entry.Close())
		c.Invalidate(key)
	}
}

func TestStorableHonoursNoStoreAndPrivate(t *testing.T) {
	for _, tc := range []struct {
		cacheControl string
		want         bool
	}{
		{"", true},
		{"max-age=60", true},
		{"public", true},
		{"no-cache", true}, // a freshness directive; this deployment decides freshness
		{"no-store", false},
		{"private", false},
		{"max-age=60, no-store", false},
		{"  NO-STORE  ", false},
	} {
		header := http.Header{}
		if tc.cacheControl != "" {
			header.Set("Cache-Control", tc.cacheControl)
		}
		assert.Equal(t, tc.want, Storable(header), "Cache-Control: %q", tc.cacheControl)
	}
}

func TestCorruptMetadataIsDiscarded(t *testing.T) {
	c, _ := newCache(t)

	// The payload and what is recorded about it are committed as one record,
	// so they cannot disagree — but if they ever did, the entry has to go
	// rather than be served.
	w, err := c.store.Put("k", 4)
	require.NoError(t, err)
	_, err = w.Write([]byte("data"))
	require.NoError(t, err)
	require.NoError(t, w.Commit([]byte("this is not json")))

	_, result := c.Get("k")
	assert.Equal(t, Miss, result)
	_, result = c.Get("k")
	assert.Equal(t, Miss, result, "and it is gone, not merely refused")
}

func TestSurvivesAReopen(t *testing.T) {
	dir := t.TempDir()
	opts := Options{Dir: dir, MaxBytes: 4 << 20, SegmentSize: 64 << 10, InlineMaxSize: 8 << 10}
	c, err := New(opts)
	require.NoError(t, err)
	store(t, c, "k", objectHeader(), []byte("payload"))
	require.NoError(t, c.Close())

	c, err = New(opts)
	require.NoError(t, err)
	defer c.Close()
	entry, result := c.Get("k")
	require.Equal(t, Hit, result)
	defer entry.Close()
	assert.Equal(t, `"d41d8cd98f00b204e9800998ecf8427e"`, entry.ETag())
	body, err := io.ReadAll(entry.Body())
	require.NoError(t, err)
	assert.Equal(t, "payload", string(body))
}

func TestReadsAreBypassedWhileAWriteIsInFlight(t *testing.T) {
	c, _ := newCache(t)
	store(t, c, "k", objectHeader(), []byte("old"))

	// The window a plain invalidate-afterwards cannot close: a read that
	// starts after the upload began still sees the old object upstream, and
	// finishes after the upload landed. While the write is in flight the
	// cache refuses to answer for that key and refuses to be filled for it,
	// so there is nothing for such a read to poison.
	m := c.BeginMutation("k")
	entry, result := c.Get("k")
	assert.Nil(t, entry)
	assert.Equal(t, Bypass, result)

	_, err := c.Put("k", 3)
	assert.ErrorIs(t, err, ErrBusyKey, "a read-through must not fill a key that is being written")

	// Other keys are untouched.
	store(t, c, "other", objectHeader(), []byte("fine"))
	assert.True(t, c.Has("other"))

	m.End()
	_, result = c.Get("k")
	assert.Equal(t, Miss, result, "the old value is gone even though nothing replaced it")
}

func TestAnUploadCanLeaveTheCacheWarm(t *testing.T) {
	c, _ := newCache(t)
	store(t, c, "k", objectHeader(), []byte("old"))

	m := c.BeginMutation("k")
	w := m.Capture(3)
	require.NotNil(t, w, "the body of an upload is the one write a mutation lets through")
	_, err := w.Write([]byte("new"))
	require.NoError(t, err)

	header := http.Header{"Content-Type": {"application/vnd.apache.parquet"}}
	header.Set("ETag", `"fresh"`)
	m.Store(header)
	m.End()

	entry, result := c.Get("k")
	require.Equal(t, Hit, result, "a successful upload should leave the object cached, not merely uncached")
	defer entry.Close()
	assert.Equal(t, `"fresh"`, entry.ETag())
	body, err := io.ReadAll(entry.Body())
	require.NoError(t, err)
	assert.Equal(t, "new", string(body))
}

func TestAFailedUploadLeavesNothingCached(t *testing.T) {
	c, _ := newCache(t)
	store(t, c, "k", objectHeader(), []byte("old"))

	// The upload was captured but the upstream refused it, so Store is never
	// called. What must not survive is the old value.
	m := c.BeginMutation("k")
	w := m.Capture(3)
	require.NotNil(t, w)
	_, err := w.Write([]byte("new"))
	require.NoError(t, err)
	m.End()

	_, result := c.Get("k")
	assert.Equal(t, Miss, result)
}

func TestAPartlyUploadedBodyIsNotStored(t *testing.T) {
	c, _ := newCache(t)

	m := c.BeginMutation("k")
	w := m.Capture(100)
	require.NotNil(t, w)
	_, err := w.Write([]byte("client hung up"))
	require.NoError(t, err)
	m.Store(objectHeader()) // the upstream would not have accepted this either
	m.End()

	_, result := c.Get("k")
	assert.Equal(t, Miss, result)
}

func TestABatchMutationHasNoBodyToCapture(t *testing.T) {
	c, _ := newCache(t)
	store(t, c, "a", objectHeader(), []byte("1"))
	store(t, c, "b", objectHeader(), []byte("2"))

	m := c.BeginMutation("a", "b")
	assert.Nil(t, m.Capture(10), "a batch delete has no body describing any one object")
	m.End()

	assert.False(t, c.Has("a"))
	assert.False(t, c.Has("b"))
}

func TestMutationsOnTheSameKeyNest(t *testing.T) {
	c, _ := newCache(t)
	first := c.BeginMutation("k")
	second := c.BeginMutation("k")

	first.End()
	_, result := c.Get("k")
	assert.Equal(t, Bypass, result, "the key is still claimed by the second write")

	second.End()
	_, result = c.Get("k")
	assert.Equal(t, Miss, result)

	c.mu.Lock()
	defer c.mu.Unlock()
	assert.Empty(t, c.pending, "a finished write leaves nothing behind")
}
