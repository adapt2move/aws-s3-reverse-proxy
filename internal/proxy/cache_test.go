package proxy

import (
	"fmt"
	"io"
	"net/http"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/Kriechi/aws-s3-reverse-proxy/internal/cache"
)

// newCachingProxy is the deployment with a cache in it: everything else about
// the fixture is unchanged, so a difference in these tests is a difference the
// cache made.
func newCachingProxy(t *testing.T, tweaks ...func(*Config)) (*Handler, *fakeUpstream, *cache.Cache) {
	t.Helper()
	objectCache, err := cache.New(cache.Options{
		Dir:           t.TempDir(),
		MaxBytes:      8 << 20,
		SegmentSize:   256 << 10,
		InlineMaxSize: 32 << 10,
		MaxObjectSize: 1 << 20,
	})
	require.NoError(t, err)
	t.Cleanup(func() { _ = objectCache.Close() })

	all := append([]func(*Config){func(cfg *Config) {
		cfg.Cache = objectCache
		cfg.CacheWrites = true
		cfg.CacheRangeFills = true
	}}, tweaks...)
	h, upstream := newTestProxy(t, all...)
	return h, upstream, objectCache
}

// servesObject makes the fake upstream answer every GET with this body.
func servesObject(u *fakeUpstream, body string, header http.Header) {
	u.mu.Lock()
	defer u.mu.Unlock()
	u.respond = func(w http.ResponseWriter, r *http.Request) {
		for name, values := range header {
			w.Header()[http.CanonicalHeaderKey(name)] = values
		}
		if r.Method == http.MethodGet || r.Method == http.MethodHead {
			w.Header().Set("Content-Type", "application/octet-stream")
			w.Header().Set("Etag", `"upstream-etag"`)
			http.ServeContent(w, r, "", time.Time{}, strings.NewReader(body))
			return
		}
		w.Header().Set("Etag", `"upstream-etag"`)
		w.WriteHeader(http.StatusOK)
	}
}

func getObject(t *testing.T, h *Handler, tenant, level, target string, header http.Header) *http.Response {
	t.Helper()
	return do(t, h, clientRequest{
		method: http.MethodGet, target: target, tenant: tenant, level: level, headers: header,
	}).Result()
}

// waitFor gives a background fill a moment to land. Everything else in this
// suite is synchronous; this is the one thing that is not.
func waitFor(t *testing.T, what string, condition func() bool) {
	t.Helper()
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		if condition() {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatalf("timed out waiting for %s", what)
}

func TestSecondReadDoesNotReachTheObjectStore(t *testing.T) {
	h, upstream, _ := newCachingProxy(t)
	servesObject(upstream, "column,value\n1,2\n", nil)

	first := getObject(t, h, tenantA, "rw", "/bucket/datasets/2026/a.csv", nil)
	require.Equal(t, http.StatusOK, first.StatusCode)
	assert.Equal(t, "MISS", first.Header.Get("X-Cache"))
	assert.Equal(t, 1, upstream.count())

	second := getObject(t, h, tenantA, "rw", "/bucket/datasets/2026/a.csv", nil)
	require.Equal(t, http.StatusOK, second.StatusCode)
	assert.Equal(t, "HIT", second.Header.Get("X-Cache"))
	assert.Equal(t, 1, upstream.count(), "the second read should not have reached the object store")

	body := readBody(t, second)
	assert.Equal(t, "column,value\n1,2\n", body)
	assert.Equal(t, `"upstream-etag"`, second.Header.Get("Etag"), "a replayed response keeps what the upstream said about the object")
	assert.Equal(t, "application/octet-stream", second.Header.Get("Content-Type"))
}

// The guarantee the whole design hangs on: a cache entry is an answer to a
// request that was already going to be allowed. It can never make one that
// would have been refused.
func TestACachedObjectStillGoesThroughPolicy(t *testing.T) {
	h, upstream, _ := newCachingProxy(t)
	servesObject(upstream, "secret", nil)

	// rws may read the private carve-out, so this warms the cache.
	warm := getObject(t, h, tenantA, "rws", "/bucket/datasets/2026/private/x.csv", nil)
	require.Equal(t, http.StatusOK, warm.StatusCode)
	require.Equal(t, 1, upstream.count())

	// ro is denied that path by the same fixture. The object is sitting in
	// the cache; the answer must still be 403.
	denied := getObject(t, h, tenantA, "ro", "/bucket/datasets/2026/private/x.csv", nil)
	assert.Equal(t, http.StatusForbidden, denied.StatusCode)
	assert.NotEqual(t, "HIT", denied.Header.Get("X-Cache"))
	assert.NotContains(t, readBody(t, denied), "secret")
	assert.Equal(t, 1, upstream.count(), "and it must not have been fetched again either")
}

// Isolation is not something the cache has to remember to check: the key it
// stores under is the upstream object, tenant prefix included, so two tenants
// naming the same object are two keys.
func TestOneTenantCannotBeServedAnothersCachedObject(t *testing.T) {
	h, upstream, _ := newCachingProxy(t)
	upstream.mu.Lock()
	upstream.respond = func(w http.ResponseWriter, r *http.Request) {
		// Each tenant's object says which prefix it came from.
		w.Header().Set("Content-Type", "text/plain")
		http.ServeContent(w, r, "", time.Time{}, strings.NewReader("stored at "+r.URL.Path))
	}
	upstream.mu.Unlock()

	a := getObject(t, h, tenantA, "rw", "/bucket/datasets/shared.csv", nil)
	require.Equal(t, http.StatusOK, a.StatusCode)
	assert.Contains(t, readBody(t, a), tenantA)

	b := getObject(t, h, tenantB, "rw", "/bucket/datasets/shared.csv", nil)
	require.Equal(t, http.StatusOK, b.StatusCode)
	assert.Equal(t, "MISS", b.Header.Get("X-Cache"), "the other tenant's entry is a different key entirely")
	body := readBody(t, b)
	assert.Contains(t, body, tenantB)
	assert.NotContains(t, body, tenantA)
	assert.Equal(t, 2, upstream.count())
}

func TestAnUploadLeavesTheObjectCached(t *testing.T) {
	h, upstream, _ := newCachingProxy(t)
	servesObject(upstream, "old contents", nil)

	require.Equal(t, http.StatusOK, getObject(t, h, tenantA, "rw", "/bucket/datasets/a.csv", nil).StatusCode)
	require.Equal(t, 1, upstream.count())

	put := do(t, h, clientRequest{
		method: http.MethodPut, target: "/bucket/datasets/a.csv",
		body: []byte("new contents"), tenant: tenantA, level: "rw",
		headers: http.Header{"Content-Type": {"text/csv"}},
	}).Result()
	require.Equal(t, http.StatusOK, put.StatusCode)
	require.Equal(t, 2, upstream.count())

	// The bytes were already passing through, so the upload is the cheapest
	// cache fill there is — and the next read is served from it.
	after := getObject(t, h, tenantA, "rw", "/bucket/datasets/a.csv", nil)
	require.Equal(t, http.StatusOK, after.StatusCode)
	assert.Equal(t, "HIT", after.Header.Get("X-Cache"))
	assert.Equal(t, "new contents", readBody(t, after))
	assert.Equal(t, "text/csv", after.Header.Get("Content-Type"), "what the client said about the object is what a read reports back")
	assert.Equal(t, 2, upstream.count())
}

func TestAnUploadInvalidatesEvenWithoutWriteThrough(t *testing.T) {
	h, upstream, _ := newCachingProxy(t, func(cfg *Config) { cfg.CacheWrites = false })
	servesObject(upstream, "old contents", nil)

	require.Equal(t, http.StatusOK, getObject(t, h, tenantA, "rw", "/bucket/datasets/a.csv", nil).StatusCode)
	put := do(t, h, clientRequest{
		method: http.MethodPut, target: "/bucket/datasets/a.csv",
		body: []byte("new contents"), tenant: tenantA, level: "rw",
	}).Result()
	require.Equal(t, http.StatusOK, put.StatusCode)

	servesObject(upstream, "new contents", nil)
	after := getObject(t, h, tenantA, "rw", "/bucket/datasets/a.csv", nil)
	assert.Equal(t, "MISS", after.Header.Get("X-Cache"), "the stale entry must be gone even when the upload was not captured")
	assert.Equal(t, "new contents", readBody(t, after))
}

func TestADeleteInvalidatesTheObject(t *testing.T) {
	h, upstream, objectCache := newCachingProxy(t)
	servesObject(upstream, "contents", nil)

	require.Equal(t, http.StatusOK, getObject(t, h, tenantA, "rw", "/bucket/datasets/a.csv", nil).StatusCode)
	require.True(t, objectCache.Has("bucket/"+tenantA+"/datasets/a.csv"))

	del := do(t, h, clientRequest{
		method: http.MethodDelete, target: "/bucket/datasets/a.csv", tenant: tenantA, level: "rw",
	}).Result()
	require.Equal(t, http.StatusOK, del.StatusCode)
	assert.False(t, objectCache.Has("bucket/"+tenantA+"/datasets/a.csv"))
}

func TestABatchDeleteInvalidatesEveryKeyItRemoves(t *testing.T) {
	h, upstream, objectCache := newCachingProxy(t)
	servesObject(upstream, "contents", nil)

	for _, key := range []string{"datasets/a.csv", "datasets/b.csv"} {
		require.Equal(t, http.StatusOK, getObject(t, h, tenantA, "rw", "/bucket/"+key, nil).StatusCode)
	}
	require.True(t, objectCache.Has("bucket/"+tenantA+"/datasets/a.csv"))
	require.True(t, objectCache.Has("bucket/"+tenantA+"/datasets/b.csv"))

	upstream.mu.Lock()
	upstream.respond = func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/xml")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`<?xml version="1.0"?><DeleteResult></DeleteResult>`))
	}
	upstream.mu.Unlock()

	body := `<?xml version="1.0"?><Delete><Object><Key>datasets/a.csv</Key></Object><Object><Key>datasets/b.csv</Key></Object></Delete>`
	resp := do(t, h, clientRequest{
		method: http.MethodPost, target: "/bucket?delete", body: []byte(body),
		tenant: tenantA, level: "rw",
	}).Result()
	require.Equal(t, http.StatusOK, resp.StatusCode)

	assert.False(t, objectCache.Has("bucket/"+tenantA+"/datasets/a.csv"))
	assert.False(t, objectCache.Has("bucket/"+tenantA+"/datasets/b.csv"))
}

func TestRangeIsServedFromTheCache(t *testing.T) {
	h, upstream, _ := newCachingProxy(t)
	servesObject(upstream, "0123456789abcdef", nil)

	require.Equal(t, http.StatusOK, getObject(t, h, tenantA, "rw", "/bucket/datasets/a.bin", nil).StatusCode)
	require.Equal(t, 1, upstream.count())

	ranged := getObject(t, h, tenantA, "rw", "/bucket/datasets/a.bin", http.Header{"Range": {"bytes=4-7"}})
	assert.Equal(t, http.StatusPartialContent, ranged.StatusCode)
	assert.Equal(t, "HIT", ranged.Header.Get("X-Cache"))
	assert.Equal(t, "bytes 4-7/16", ranged.Header.Get("Content-Range"))
	assert.Equal(t, "4567", readBody(t, ranged))
	assert.Equal(t, 1, upstream.count(), "a range out of a cached object is not an upstream read")
}

// The workload this exists for: a client that only ever reads ranges would
// otherwise never fill the cache, and its hit rate would be exactly zero.
func TestARangedMissFetchesTheWholeObject(t *testing.T) {
	h, upstream, objectCache := newCachingProxy(t)
	servesObject(upstream, "0123456789abcdef", nil)

	ranged := getObject(t, h, tenantA, "rw", "/bucket/datasets/a.parquet", http.Header{"Range": {"bytes=-4"}})
	require.Equal(t, http.StatusPartialContent, ranged.StatusCode)
	assert.Equal(t, "cdef", readBody(t, ranged), "the client still gets its range straight from the upstream")

	key := "bucket/" + tenantA + "/datasets/a.parquet"
	waitFor(t, "the background fill to land", func() bool { return objectCache.Has(key) })

	before := upstream.count()
	next := getObject(t, h, tenantA, "rw", "/bucket/datasets/a.parquet", http.Header{"Range": {"bytes=0-3"}})
	assert.Equal(t, http.StatusPartialContent, next.StatusCode)
	assert.Equal(t, "HIT", next.Header.Get("X-Cache"))
	assert.Equal(t, "0123", readBody(t, next))
	assert.Equal(t, before, upstream.count())
}

func TestRangeFillsCanBeTurnedOff(t *testing.T) {
	h, upstream, objectCache := newCachingProxy(t, func(cfg *Config) { cfg.CacheRangeFills = false })
	servesObject(upstream, "0123456789abcdef", nil)

	require.Equal(t, http.StatusPartialContent,
		getObject(t, h, tenantA, "rw", "/bucket/datasets/a.bin", http.Header{"Range": {"bytes=0-3"}}).StatusCode)

	time.Sleep(50 * time.Millisecond)
	assert.False(t, objectCache.Has("bucket/"+tenantA+"/datasets/a.bin"))
}

func TestHeadIsAnsweredFromACachedRead(t *testing.T) {
	h, upstream, _ := newCachingProxy(t)
	servesObject(upstream, "sixteen bytes!!!", nil)

	require.Equal(t, http.StatusOK, getObject(t, h, tenantA, "rw", "/bucket/datasets/a.bin", nil).StatusCode)
	require.Equal(t, 1, upstream.count())

	head := do(t, h, clientRequest{
		method: http.MethodHead, target: "/bucket/datasets/a.bin", tenant: tenantA, level: "rw",
	}).Result()
	assert.Equal(t, http.StatusOK, head.StatusCode)
	assert.Equal(t, "HIT", head.Header.Get("X-Cache"))
	assert.Equal(t, "16", head.Header.Get("Content-Length"))
	assert.Equal(t, `"upstream-etag"`, head.Header.Get("Etag"))
	assert.Equal(t, 1, upstream.count())
}

// A listing's body is filtered per access level on the way out, so a cached
// one would be an answer to one caller rather than a copy of anything.
func TestListingsAreNeverCached(t *testing.T) {
	h, upstream, _ := newCachingProxy(t)
	upstream.mu.Lock()
	upstream.respond = func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/xml")
		_, _ = w.Write([]byte(`<?xml version="1.0"?><ListBucketResult></ListBucketResult>`))
	}
	upstream.mu.Unlock()

	for i := 0; i < 2; i++ {
		resp := do(t, h, clientRequest{
			method: http.MethodGet, target: "/bucket?list-type=2&prefix=datasets/", tenant: tenantA, level: "rw",
		}).Result()
		require.Equal(t, http.StatusOK, resp.StatusCode)
		assert.Empty(t, resp.Header.Get("X-Cache"), "a listing is not something the cache has an opinion about")
	}
	assert.Equal(t, 2, upstream.count())
}

func TestNoStoreIsHonoured(t *testing.T) {
	h, upstream, objectCache := newCachingProxy(t)
	servesObject(upstream, "contents", http.Header{"Cache-Control": {"no-store"}})

	require.Equal(t, http.StatusOK, getObject(t, h, tenantA, "rw", "/bucket/datasets/a.csv", nil).StatusCode)
	assert.False(t, objectCache.Has("bucket/"+tenantA+"/datasets/a.csv"),
		"an object marked no-store meant it, and this is a shared cache")
}

func TestIfMatchGoesToTheObjectStore(t *testing.T) {
	h, upstream, _ := newCachingProxy(t)
	servesObject(upstream, "contents", nil)

	require.Equal(t, http.StatusOK, getObject(t, h, tenantA, "rw", "/bucket/datasets/a.csv", nil).StatusCode)
	require.Equal(t, 1, upstream.count())

	// If-Match is a concurrency primitive, not a freshness question: the
	// caller is asking the object store to decide, and a cache answering on
	// its behalf would break what the caller is relying on.
	conditional := getObject(t, h, tenantA, "rw", "/bucket/datasets/a.csv",
		http.Header{"If-Match": {`"upstream-etag"`}})
	assert.Equal(t, "BYPASS", conditional.Header.Get("X-Cache"))
	assert.Equal(t, 2, upstream.count())
}

func TestATruncatedUpstreamResponseIsNotCached(t *testing.T) {
	h, upstream, objectCache := newCachingProxy(t)
	upstream.mu.Lock()
	upstream.respond = func(w http.ResponseWriter, r *http.Request) {
		// Announces a hundred bytes and delivers twenty. A cache that kept
		// this would answer somebody's next read with a truncated object.
		w.Header().Set("Content-Length", "100")
		w.Header().Set("Content-Type", "application/octet-stream")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("only twenty bytes..."))
	}
	upstream.mu.Unlock()

	_ = getObject(t, h, tenantA, "rw", "/bucket/datasets/a.csv", nil)
	assert.False(t, objectCache.Has("bucket/"+tenantA+"/datasets/a.csv"))
}

func TestAnObjectTooLargeForTheCacheIsStillServed(t *testing.T) {
	h, upstream, objectCache := newCachingProxy(t, func(cfg *Config) {})
	big := strings.Repeat("x", 2<<20) // over MaxObjectSize
	servesObject(upstream, big, nil)

	resp := getObject(t, h, tenantA, "rw", "/bucket/datasets/big.bin", nil)
	require.Equal(t, http.StatusOK, resp.StatusCode)
	assert.Len(t, readBody(t, resp), len(big), "the read is unaffected by the cache declining it")
	assert.False(t, objectCache.Has("bucket/"+tenantA+"/datasets/big.bin"))
}

// expiringProxy is a caching deployment with a hand-wound clock, so a test
// about expiry does not have to wait for it.
func expiringProxy(t *testing.T, maxAge time.Duration) (*Handler, *fakeUpstream, *cache.Cache, func(time.Duration)) {
	t.Helper()
	var mu sync.Mutex
	now := time.Date(2026, 3, 1, 12, 0, 0, 0, time.UTC)
	objectCache, err := cache.New(cache.Options{
		Dir: t.TempDir(), MaxBytes: 8 << 20, SegmentSize: 256 << 10, InlineMaxSize: 32 << 10,
		MaxObjectSize: 1 << 20,
		MaxAge:        maxAge,
		Now: func() time.Time {
			mu.Lock()
			defer mu.Unlock()
			return now
		},
	})
	require.NoError(t, err)
	t.Cleanup(func() { _ = objectCache.Close() })

	h, upstream := newTestProxy(t, func(cfg *Config) {
		cfg.Cache = objectCache
		cfg.CacheWrites = true
		cfg.CacheRangeFills = true
	})
	return h, upstream, objectCache, func(d time.Duration) {
		mu.Lock()
		defer mu.Unlock()
		now = now.Add(d)
	}
}

// An expired entry is asked about, not thrown away. When the object has not
// changed — the common case, since a write through this proxy invalidates
// rather than expires — the answer costs a round trip instead of transferring
// the object again.
func TestAnExpiredEntryIsRevalidatedRatherThanRefetched(t *testing.T) {
	h, upstream, _, advance := expiringProxy(t, time.Minute)
	servesObject(upstream, "contents", nil)

	require.Equal(t, http.StatusOK, getObject(t, h, tenantA, "rw", "/bucket/datasets/a.csv", nil).StatusCode)
	require.Equal(t, 1, upstream.count())

	advance(2 * time.Minute)
	revalidated := getObject(t, h, tenantA, "rw", "/bucket/datasets/a.csv", nil)
	require.Equal(t, http.StatusOK, revalidated.StatusCode)
	assert.Equal(t, "REVALIDATED", revalidated.Header.Get("X-Cache"))
	assert.Equal(t, "contents", readBody(t, revalidated), "the body came off the disk, not the wire")

	// One extra upstream request, and it was a HEAD: no body crossed the
	// network, which is the whole point of revalidating.
	require.Equal(t, 2, upstream.count())
	assert.Equal(t, http.MethodHead, upstream.last(t).method)

	// And it is fresh again, so the next read does not ask a second time.
	assert.Equal(t, "HIT", getObject(t, h, tenantA, "rw", "/bucket/datasets/a.csv", nil).Header.Get("X-Cache"))
	assert.Equal(t, 2, upstream.count())
}

func TestAChangedObjectIsFetchedAgain(t *testing.T) {
	h, upstream, _, advance := expiringProxy(t, time.Minute)
	servesObject(upstream, "old contents", nil)

	require.Equal(t, http.StatusOK, getObject(t, h, tenantA, "rw", "/bucket/datasets/a.csv", nil).StatusCode)

	// Somebody else replaced the object, so the ETag no longer matches what
	// was cached and the revalidation has to come back negative.
	advance(2 * time.Minute)
	upstream.mu.Lock()
	upstream.respond = func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/octet-stream")
		w.Header().Set("Etag", `"a-different-etag"`)
		http.ServeContent(w, r, "", time.Time{}, strings.NewReader("new contents"))
	}
	upstream.mu.Unlock()

	fresh := getObject(t, h, tenantA, "rw", "/bucket/datasets/a.csv", nil)
	require.Equal(t, http.StatusOK, fresh.StatusCode)
	assert.Equal(t, "STALE", fresh.Header.Get("X-Cache"))
	assert.Equal(t, "new contents", readBody(t, fresh))

	// And the new version replaced the old one.
	advance(time.Second)
	next := getObject(t, h, tenantA, "rw", "/bucket/datasets/a.csv", nil)
	assert.Equal(t, "HIT", next.Header.Get("X-Cache"))
	assert.Equal(t, "new contents", readBody(t, next))
}

func TestARevalidatedRangeComesOffDisk(t *testing.T) {
	h, upstream, _, advance := expiringProxy(t, time.Minute)
	servesObject(upstream, "0123456789abcdef", nil)

	require.Equal(t, http.StatusOK, getObject(t, h, tenantA, "rw", "/bucket/datasets/a.parquet", nil).StatusCode)
	advance(2 * time.Minute)

	// The reason revalidation is answered here rather than by rewriting a
	// 304 further down: only here is there a ResponseWriter to hand to
	// ServeContent, and a ranged read of a revalidated object still has to
	// come out as a proper 206.
	ranged := getObject(t, h, tenantA, "rw", "/bucket/datasets/a.parquet", http.Header{"Range": {"bytes=4-7"}})
	assert.Equal(t, http.StatusPartialContent, ranged.StatusCode)
	assert.Equal(t, "REVALIDATED", ranged.Header.Get("X-Cache"))
	assert.Equal(t, "bytes 4-7/16", ranged.Header.Get("Content-Range"))
	assert.Equal(t, "4567", readBody(t, ranged))
}

func TestRevalidationFallsBackToFetchingWhenItCannotTell(t *testing.T) {
	h, upstream, _, advance := expiringProxy(t, time.Minute)

	// An object store that returns no ETag leaves nothing to compare, so
	// the entry has to be fetched rather than assumed current. Being wrong
	// this way costs a transfer; being wrong the other way serves a stale
	// object.
	upstream.mu.Lock()
	upstream.respond = func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/octet-stream")
		http.ServeContent(w, r, "", time.Time{}, strings.NewReader("contents"))
	}
	upstream.mu.Unlock()

	require.Equal(t, http.StatusOK, getObject(t, h, tenantA, "rw", "/bucket/datasets/a.csv", nil).StatusCode)
	advance(2 * time.Minute)

	resp := getObject(t, h, tenantA, "rw", "/bucket/datasets/a.csv", nil)
	assert.Equal(t, "STALE", resp.Header.Get("X-Cache"))
	assert.Equal(t, "contents", readBody(t, resp))
	assert.Equal(t, http.MethodGet, upstream.last(t).method, "no HEAD is worth issuing without an ETag to compare")
}

func TestExpiryCanBeTurnedOff(t *testing.T) {
	h, upstream, _, advance := expiringProxy(t, 0)
	servesObject(upstream, "contents", nil)

	require.Equal(t, http.StatusOK, getObject(t, h, tenantA, "rw", "/bucket/datasets/a.csv", nil).StatusCode)
	advance(365 * 24 * time.Hour)
	assert.Equal(t, "HIT", getObject(t, h, tenantA, "rw", "/bucket/datasets/a.csv", nil).Header.Get("X-Cache"))
	assert.Equal(t, 1, upstream.count(), "with no expiry only a write through this proxy invalidates")
}

func TestWithoutACacheNothingChanges(t *testing.T) {
	h, upstream := newTestProxy(t)
	servesObject(upstream, "contents", nil)

	for i := 0; i < 2; i++ {
		resp := getObject(t, h, tenantA, "rw", "/bucket/datasets/a.csv", nil)
		require.Equal(t, http.StatusOK, resp.StatusCode)
		assert.Empty(t, resp.Header.Get("X-Cache"))
	}
	assert.Equal(t, 2, upstream.count(), "every read reaches the object store, as it always did")
}

// The point of this one is what happens *after* the concurrent reads, not
// during them. Asserting only status and body would pass with no cache at
// all: both come from the upstream stream that every reader gets either way.
func TestConcurrentReadsOfAColdObjectLeaveItCached(t *testing.T) {
	h, upstream, objectCache := newCachingProxy(t)
	body := strings.Repeat("payload", 100)
	servesObject(upstream, body, nil)

	const keys, readers = 4, 16
	var wg sync.WaitGroup
	for i := 0; i < readers; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			target := fmt.Sprintf("/bucket/datasets/%d.bin", i%keys)
			resp := getObject(t, h, tenantA, "rw", target, nil)
			assert.Equal(t, http.StatusOK, resp.StatusCode)
			assert.Equal(t, body, readBody(t, resp))
		}(i)
	}
	wg.Wait()

	// Every one of them should have ended up cached, and a read afterwards
	// should not reach the object store.
	before := upstream.count()
	for i := 0; i < keys; i++ {
		target := fmt.Sprintf("/bucket/datasets/%d.bin", i)
		require.True(t, objectCache.Has("bucket/"+tenantA+"/datasets/"+fmt.Sprint(i)+".bin"),
			"%s was read %d times and never stored", target, readers/keys)
		resp := getObject(t, h, tenantA, "rw", target, nil)
		assert.Equal(t, "HIT", resp.Header.Get("X-Cache"))
		assert.Equal(t, body, readBody(t, resp))
	}
	assert.Equal(t, before, upstream.count(), "the reads afterwards should all have been hits")
}

// Completing a multipart upload replaces the object without a PutObject ever
// being seen, so it is the one write whose invalidation nothing else would
// cover. A test that uploaded to a fresh key would pass on a broken one, too,
// because a cold miss and a dropped entry look the same from outside.
func TestCompletingAMultipartUploadInvalidatesTheObject(t *testing.T) {
	h, upstream, objectCache := newCachingProxy(t)
	key := "datasets/big.parquet"
	servesObject(upstream, "the version before the upload", nil)

	require.Equal(t, http.StatusOK, getObject(t, h, tenantA, "rw", "/bucket/"+key, nil).StatusCode)
	require.Equal(t, "HIT", getObject(t, h, tenantA, "rw", "/bucket/"+key, nil).Header.Get("X-Cache"))
	require.True(t, objectCache.Has("bucket/"+tenantA+"/"+key))

	upstream.mu.Lock()
	upstream.respond = func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/xml")
		_, _ = w.Write([]byte(`<?xml version="1.0"?><CompleteMultipartUploadResult>` +
			`<Key>` + tenantA + `/` + key + `</Key>` +
			`</CompleteMultipartUploadResult>`))
	}
	upstream.mu.Unlock()

	complete := do(t, h, clientRequest{
		method: http.MethodPost, target: "/bucket/" + key + "?uploadId=abc",
		body: []byte("<CompleteMultipartUpload/>"), tenant: tenantA, level: "rw",
	}).Result()
	require.Equal(t, http.StatusOK, complete.StatusCode)

	assert.False(t, objectCache.Has("bucket/"+tenantA+"/"+key),
		"the object changed without a PutObject; the cache has to have noticed")

	servesObject(upstream, "the version the multipart upload assembled", nil)
	after := getObject(t, h, tenantA, "rw", "/bucket/"+key, nil)
	assert.Equal(t, "MISS", after.Header.Get("X-Cache"))
	assert.Equal(t, "the version the multipart upload assembled", readBody(t, after))
}

// Uploading a part changes nothing a reader can see — the object does not
// exist until the upload is completed — so it must not throw away what is
// cached under that key.
func TestUploadingAPartLeavesTheCachedObjectAlone(t *testing.T) {
	h, upstream, objectCache := newCachingProxy(t)
	key := "datasets/big.parquet"
	servesObject(upstream, "the object as it stands", nil)

	require.Equal(t, http.StatusOK, getObject(t, h, tenantA, "rw", "/bucket/"+key, nil).StatusCode)
	require.True(t, objectCache.Has("bucket/"+tenantA+"/"+key))

	part := do(t, h, clientRequest{
		method: http.MethodPut, target: "/bucket/" + key + "?partNumber=1&uploadId=abc",
		body: []byte("part one"), tenant: tenantA, level: "rw",
	}).Result()
	require.Equal(t, http.StatusOK, part.StatusCode)

	resp := getObject(t, h, tenantA, "rw", "/bucket/"+key, nil)
	assert.Equal(t, "HIT", resp.Header.Get("X-Cache"))
	assert.Equal(t, "the object as it stands", readBody(t, resp))
}

// readBody drains a response and returns it as a string.
func readBody(t *testing.T, resp *http.Response) string {
	t.Helper()
	defer resp.Body.Close()
	body, err := io.ReadAll(resp.Body)
	require.NoError(t, err)
	return string(body)
}
