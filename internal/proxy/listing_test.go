package proxy

import (
	"net/http"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/Kriechi/aws-s3-reverse-proxy/internal/listcache"
)

// newFallbackProxy is the deployment with the listing fallback turned on and
// nothing else changed. Background refreshes are off unless a tweak turns
// them on, so that what a write drops stays dropped where a test is about
// the dropping.
func newFallbackProxy(t *testing.T, timeout time.Duration, tweaks ...func(*Config)) (*Handler, *fakeUpstream, *outage) {
	t.Helper()
	h, upstream := newTestProxy(t, append([]func(*Config){func(cfg *Config) {
		cfg.ListFallback = listcache.New(listcache.Options{MaxBytes: 1 << 20, MaxAge: time.Hour})
		cfg.ListFallbackTimeout = timeout
	}}, tweaks...)...)
	t.Cleanup(h.Close)
	o := &outage{}
	upstream.mu.Lock()
	upstream.respond = o.serve
	upstream.mu.Unlock()
	return h, upstream, o
}

// outage is an object store whose health the test decides: it lists two
// objects of tenant A's — one of them in the carve-out only rws may read —
// and accepts writes, until it is told to do otherwise.
type outage struct {
	mu         sync.Mutex
	listStatus int
	listDelay  time.Duration
	hangUp     bool
	putStatus  int
	putDelay   time.Duration
	// written are the upstream keys accepted since, listed after the two
	// that are always there.
	written []string
}

func (o *outage) set(change func(*outage)) {
	o.mu.Lock()
	defer o.mu.Unlock()
	change(o)
}

func (o *outage) serve(w http.ResponseWriter, r *http.Request) {
	o.mu.Lock()
	listStatus, delay, hangUp, putStatus := o.listStatus, o.listDelay, o.hangUp, o.putStatus
	putDelay := o.putDelay
	written := append([]string(nil), o.written...)
	o.mu.Unlock()

	if r.Method == http.MethodPut {
		time.Sleep(putDelay)
		if putStatus != 0 {
			w.WriteHeader(putStatus)
			return
		}
		o.set(func(o *outage) { o.written = append(o.written, strings.TrimPrefix(r.URL.Path, "/bucket/")) })
		w.Header().Set("Etag", `"new"`)
		w.WriteHeader(http.StatusOK)
		return
	}
	if delay > 0 {
		select {
		case <-time.After(delay):
		case <-r.Context().Done():
			return
		}
	}
	if hangUp {
		conn, _, err := w.(http.Hijacker).Hijack()
		if err == nil {
			_ = conn.Close()
		}
		return
	}
	if listStatus != 0 {
		w.Header().Set("Content-Type", "application/xml")
		w.WriteHeader(listStatus)
		_, _ = w.Write([]byte(`<Error><Code>InternalError</Code><Message>We encountered an internal error.</Message></Error>`))
		return
	}
	prefix := r.URL.Query().Get("prefix")
	var extra strings.Builder
	for _, key := range written {
		if strings.HasPrefix(key, prefix) {
			extra.WriteString(`<Contents><Key>` + key + `</Key><Size>1</Size></Contents>`)
		}
	}
	w.Header().Set("Content-Type", "application/xml")
	w.Header().Set("X-Amz-Request-Id", "live-request")
	_, _ = w.Write([]byte(`<?xml version="1.0" encoding="UTF-8"?>` +
		`<ListBucketResult><Name>bucket</Name><Prefix>` + prefix + `</Prefix>` +
		`<Contents><Key>` + prefix + `2026/a.csv</Key><Size>10</Size></Contents>` +
		`<Contents><Key>` + prefix + `2026/private/secret.csv</Key><Size>20</Size></Contents>` +
		extra.String() +
		`</ListBucketResult>`))
}

// listings counts the listing requests that reached the object store.
func listings(u *fakeUpstream) int {
	u.mu.Lock()
	defer u.mu.Unlock()
	n := 0
	for _, r := range u.requests {
		if r.method == http.MethodGet && r.query.Has("list-type") {
			n++
		}
	}
	return n
}

func withRefreshDelay(delay time.Duration) func(*Config) {
	return func(cfg *Config) { cfg.ListFallbackRefreshDelay = delay }
}

func list(t *testing.T, h *Handler, tenant, level string) *http.Response {
	t.Helper()
	return do(t, h, clientRequest{
		method: http.MethodGet, target: "/bucket?list-type=2&prefix=datasets/", tenant: tenant, level: level,
	}).Result()
}

func put(t *testing.T, h *Handler, target string) *http.Response {
	t.Helper()
	return do(t, h, clientRequest{
		method: http.MethodPut, target: target, body: []byte("x"), tenant: tenantA, level: "rw",
	}).Result()
}

func TestListingsHaveNoFallbackUnlessOneIsConfigured(t *testing.T) {
	h, upstream := newTestProxy(t)
	o := &outage{}
	upstream.mu.Lock()
	upstream.respond = o.serve
	upstream.mu.Unlock()

	require.Equal(t, http.StatusOK, list(t, h, tenantA, "rws").StatusCode)
	o.set(func(o *outage) { o.listStatus = http.StatusInternalServerError })
	failed := list(t, h, tenantA, "rws")
	assert.Equal(t, http.StatusInternalServerError, failed.StatusCode)
	assert.Empty(t, failed.Header.Get("X-Cache"))
}

func TestAListingTheObjectStoreFailsIsAnsweredFromMemory(t *testing.T) {
	h, upstream, o := newFallbackProxy(t, time.Second)

	live := list(t, h, tenantA, "rws")
	require.Equal(t, http.StatusOK, live.StatusCode)
	assert.Empty(t, live.Header.Get("X-Cache"), "a listing the object store answered is its answer, not the cache's")
	liveBody := readBody(t, live)

	for _, status := range []int{http.StatusInternalServerError, http.StatusServiceUnavailable} {
		o.set(func(o *outage) { o.listStatus = status })
		remembered := list(t, h, tenantA, "rws")
		require.Equal(t, http.StatusOK, remembered.StatusCode, "upstream answered %d", status)
		assert.Equal(t, "FALLBACK", remembered.Header.Get("X-Cache"))
		assert.NotEmpty(t, remembered.Header.Get("Age"))
		assert.Empty(t, remembered.Header.Get("X-Amz-Request-Id"), "a remembered answer does not claim to be one particular exchange")
		body := readBody(t, remembered)
		assert.Equal(t, liveBody, body, "a remembered listing reads exactly like the live one did")
		assert.NotContains(t, body, tenantA, "and goes through the same rewrite: no injected prefix reaches the client")
		assert.Equal(t, int64(len(body)), remembered.ContentLength)
	}
	assert.Equal(t, 3, upstream.count(), "every listing still went to the object store first")
}

func TestAListingTheObjectStoreDoesNotAnswerInTimeIsAnsweredFromMemory(t *testing.T) {
	h, _, o := newFallbackProxy(t, 50*time.Millisecond)
	require.Equal(t, http.StatusOK, list(t, h, tenantA, "rws").StatusCode)

	o.set(func(o *outage) { o.listDelay = 2 * time.Second })
	start := time.Now()
	remembered := list(t, h, tenantA, "rws")
	elapsed := time.Since(start)

	require.Equal(t, http.StatusOK, remembered.StatusCode)
	assert.Equal(t, "FALLBACK", remembered.Header.Get("X-Cache"))
	assert.Less(t, elapsed, time.Second, "the answer came from memory at the deadline, not from the object store at its leisure")
	assert.Contains(t, readBody(t, remembered), "<Key>datasets/2026/a.csv</Key>")
}

func TestAConnectionThatBreaksIsAnsweredFromMemory(t *testing.T) {
	h, _, o := newFallbackProxy(t, time.Second)
	require.Equal(t, http.StatusOK, list(t, h, tenantA, "rws").StatusCode)

	o.set(func(o *outage) { o.hangUp = true })
	remembered := list(t, h, tenantA, "rws")
	require.Equal(t, http.StatusOK, remembered.StatusCode)
	assert.Equal(t, "FALLBACK", remembered.Header.Get("X-Cache"))
}

// With nothing to fall back on, giving up at the deadline would only turn a
// slow listing into a failed one.
func TestWithoutARememberedListingTheObjectStoreIsWaitedFor(t *testing.T) {
	h, _, o := newFallbackProxy(t, 50*time.Millisecond)
	o.set(func(o *outage) { o.listDelay = 200 * time.Millisecond })

	start := time.Now()
	live := list(t, h, tenantA, "rws")
	require.Equal(t, http.StatusOK, live.StatusCode)
	assert.Empty(t, live.Header.Get("X-Cache"))
	assert.GreaterOrEqual(t, time.Since(start), 200*time.Millisecond)

	o.set(func(o *outage) { o.listDelay, o.listStatus = 0, http.StatusInternalServerError })
	assert.Equal(t, "FALLBACK", list(t, h, tenantA, "rws").Header.Get("X-Cache"),
		"and the slow answer is the one remembered")
}

func TestWithoutARememberedListingAFailureIsPassedOn(t *testing.T) {
	h, _, o := newFallbackProxy(t, time.Second)
	o.set(func(o *outage) { o.listStatus = http.StatusInternalServerError })
	failed := list(t, h, tenantA, "rws")
	assert.Equal(t, http.StatusInternalServerError, failed.StatusCode)
	assert.Empty(t, failed.Header.Get("X-Cache"))
}

// What is remembered is the object store's answer, not the one a particular
// caller was given — so the filtering happens again, for whoever it is
// served to, with the policy in force at that moment.
func TestARememberedListingIsFilteredForWhoeverItIsServedTo(t *testing.T) {
	h, _, o := newFallbackProxy(t, time.Second)
	warm := list(t, h, tenantA, "rws")
	require.Equal(t, http.StatusOK, warm.StatusCode)
	require.Contains(t, readBody(t, warm), "private/secret.csv", "rws may see the carve-out")

	o.set(func(o *outage) { o.listStatus = http.StatusInternalServerError })
	remembered := list(t, h, tenantA, "ro")
	require.Equal(t, http.StatusOK, remembered.StatusCode)
	assert.Equal(t, "FALLBACK", remembered.Header.Get("X-Cache"))
	body := readBody(t, remembered)
	assert.Contains(t, body, "<Key>datasets/2026/a.csv</Key>")
	assert.NotContains(t, body, "private", "ro may not, whoever's listing it was remembered from")
}

func TestTenantsDoNotShareRememberedListings(t *testing.T) {
	h, _, o := newFallbackProxy(t, time.Second)
	require.Equal(t, http.StatusOK, list(t, h, tenantA, "rws").StatusCode)

	o.set(func(o *outage) { o.listStatus = http.StatusInternalServerError })
	other := list(t, h, tenantB, "rws")
	assert.Equal(t, http.StatusInternalServerError, other.StatusCode,
		"tenant B's listing of the same prefix is another listing upstream")
	assert.NotContains(t, readBody(t, other), tenantA)
}

func TestAWriteThroughTheProxyDropsTheRememberedListing(t *testing.T) {
	h, _, o := newFallbackProxy(t, time.Second)
	require.Equal(t, http.StatusOK, list(t, h, tenantA, "rws").StatusCode)

	require.Equal(t, http.StatusOK, put(t, h, "/bucket/datasets/2026/b.csv").StatusCode)
	o.set(func(o *outage) { o.listStatus = http.StatusInternalServerError })
	failed := list(t, h, tenantA, "rws")
	assert.Equal(t, http.StatusInternalServerError, failed.StatusCode,
		"the remembered listing does not show b.csv, so it is no longer an answer")
}

func TestAWriteElsewhereKeepsTheRememberedListing(t *testing.T) {
	h, _, o := newFallbackProxy(t, time.Second)
	require.Equal(t, http.StatusOK, list(t, h, tenantA, "rws").StatusCode)

	require.Equal(t, http.StatusOK, put(t, h, "/bucket/workspaces/x/notes.txt").StatusCode)
	o.set(func(o *outage) { o.listStatus = http.StatusInternalServerError })
	assert.Equal(t, "FALLBACK", list(t, h, tenantA, "rws").Header.Get("X-Cache"))
}

// During an outage clients keep trying to write, and a write the object
// store refused or never received changed nothing.
func TestAWriteThatDidNotHappenKeepsTheRememberedListing(t *testing.T) {
	t.Run("refused", func(t *testing.T) {
		h, _, o := newFallbackProxy(t, time.Second)
		require.Equal(t, http.StatusOK, list(t, h, tenantA, "rws").StatusCode)

		o.set(func(o *outage) { o.putStatus = http.StatusForbidden })
		require.Equal(t, http.StatusForbidden, put(t, h, "/bucket/datasets/2026/b.csv").StatusCode)
		o.set(func(o *outage) { o.listStatus = http.StatusInternalServerError })
		assert.Equal(t, "FALLBACK", list(t, h, tenantA, "rws").Header.Get("X-Cache"))
	})
	t.Run("never received", func(t *testing.T) {
		h, upstream, _ := newFallbackProxy(t, time.Second)
		require.Equal(t, http.StatusOK, list(t, h, tenantA, "rws").StatusCode)

		upstream.server.Close()
		// The object store is gone and refuses new connections. A pooled
		// one it closed a moment ago is another case: a write on it may
		// have been sent, so it invalidates, and the test must not depend
		// on which of the two the transport happens to pick.
		h.transport.CloseIdleConnections()
		require.Equal(t, http.StatusBadGateway, put(t, h, "/bucket/datasets/2026/b.csv").StatusCode)
		remembered := list(t, h, tenantA, "rws")
		require.Equal(t, http.StatusOK, remembered.StatusCode)
		assert.Equal(t, "FALLBACK", remembered.Header.Get("X-Cache"))
	})
	t.Run("but a server error may have come after the write landed", func(t *testing.T) {
		h, _, o := newFallbackProxy(t, time.Second)
		require.Equal(t, http.StatusOK, list(t, h, tenantA, "rws").StatusCode)

		o.set(func(o *outage) { o.putStatus = http.StatusInternalServerError })
		require.Equal(t, http.StatusInternalServerError, put(t, h, "/bucket/datasets/2026/b.csv").StatusCode)
		o.set(func(o *outage) { o.listStatus = http.StatusInternalServerError })
		assert.Equal(t, http.StatusInternalServerError, list(t, h, tenantA, "rws").StatusCode)
	})
}

func TestABatchDeleteDropsTheRememberedListing(t *testing.T) {
	h, _, o := newFallbackProxy(t, time.Second)
	require.Equal(t, http.StatusOK, list(t, h, tenantA, "rws").StatusCode)

	deleted := do(t, h, clientRequest{
		method: http.MethodPost, target: "/bucket?delete", body: deleteBatchBody("datasets/2026/a.csv"),
		tenant: tenantA, level: "rw",
	}).Result()
	require.Equal(t, http.StatusOK, deleted.StatusCode)

	o.set(func(o *outage) { o.listStatus = http.StatusInternalServerError })
	assert.Equal(t, http.StatusInternalServerError, list(t, h, tenantA, "rws").StatusCode)
}

// A refusal is the object store answering, and what it answered is not the
// listing that was remembered: the bucket may be gone, the credentials may
// have changed.
func TestAnAnswerThatIsNotAListingForgetsTheRememberedOne(t *testing.T) {
	h, _, o := newFallbackProxy(t, time.Second)
	require.Equal(t, http.StatusOK, list(t, h, tenantA, "rws").StatusCode)

	o.set(func(o *outage) { o.listStatus = http.StatusNotFound })
	assert.Equal(t, http.StatusNotFound, list(t, h, tenantA, "rws").StatusCode)
	o.set(func(o *outage) { o.listStatus = http.StatusInternalServerError })
	assert.Equal(t, http.StatusInternalServerError, list(t, h, tenantA, "rws").StatusCode)
}

// A completion's answer is in its body, which reaches the client before the
// request is over. The listings that would not show the new object have to
// be gone by then.
func TestACompletedMultipartUploadDropsTheRememberedListing(t *testing.T) {
	h, upstream, o := newFallbackProxy(t, time.Second)
	require.Equal(t, http.StatusOK, list(t, h, tenantA, "rws").StatusCode)

	var listedDuringCompletion *http.Response
	upstream.mu.Lock()
	upstream.respond = func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			o.serve(w, r)
			return
		}
		w.Header().Set("Content-Type", "application/xml")
		w.WriteHeader(http.StatusOK)
		w.(http.Flusher).Flush()
		// The proxy has the headers and is waiting for the result. By the
		// time it could hand that to the client, the listing is gone.
		waitFor(t, "the remembered listing to be dropped", func() bool {
			entries, _ := h.ListFallback().Size()
			return entries == 0
		})
		// The object store goes away before the completion is over.
		o.set(func(o *outage) { o.listStatus = http.StatusInternalServerError })
		listedDuringCompletion = list(t, h, tenantA, "rws")
		_, _ = w.Write([]byte(`<CompleteMultipartUploadResult><Key>` + tenantA + `/datasets/2026/big.bin</Key></CompleteMultipartUploadResult>`))
	}
	upstream.mu.Unlock()

	body := []byte(`<CompleteMultipartUpload><Part><PartNumber>1</PartNumber><ETag>"p1"</ETag></Part></CompleteMultipartUpload>`)
	completed := do(t, h, clientRequest{
		method: http.MethodPost, target: "/bucket/datasets/2026/big.bin?uploadId=u1", body: body,
		tenant: tenantA, level: "rw",
	}).Result()
	require.Equal(t, http.StatusOK, completed.StatusCode)
	require.NotNil(t, listedDuringCompletion)
	assert.Equal(t, http.StatusInternalServerError, listedDuringCompletion.StatusCode,
		"once the completion has been answered, the listing from before it is not an answer")
	assert.Equal(t, http.StatusInternalServerError, list(t, h, tenantA, "rws").StatusCode)
}

// The point of refreshing: a write drops the listing, and once the writes
// have settled the object store is asked again — so the fallback there is
// is the object store's answer from after the write.
func TestADroppedListingIsListedAgainOnceWritesSettle(t *testing.T) {
	h, upstream, o := newFallbackProxy(t, time.Second, withRefreshDelay(20*time.Millisecond))
	require.Equal(t, http.StatusOK, list(t, h, tenantA, "rws").StatusCode)

	require.Equal(t, http.StatusOK, put(t, h, "/bucket/datasets/2026/b.csv").StatusCode)
	waitFor(t, "the background refresh", func() bool { return h.ListFallback().Stats().Refreshed == 1 })
	assert.Equal(t, 2, listings(upstream))

	o.set(func(o *outage) { o.listStatus = http.StatusInternalServerError })
	remembered := list(t, h, tenantA, "rws")
	require.Equal(t, http.StatusOK, remembered.StatusCode)
	assert.Equal(t, "FALLBACK", remembered.Header.Get("X-Cache"))
	body := readBody(t, remembered)
	assert.Contains(t, body, "<Key>datasets/2026/b.csv</Key>", "the object written is in the answer remembered after it")
	assert.NotContains(t, body, tenantA)
}

func TestABurstOfWritesCostsOneRefresh(t *testing.T) {
	h, upstream, _ := newFallbackProxy(t, time.Second, withRefreshDelay(100*time.Millisecond))
	require.Equal(t, http.StatusOK, list(t, h, tenantA, "rws").StatusCode)

	for _, name := range []string{"b", "c", "d", "e", "f"} {
		require.Equal(t, http.StatusOK, put(t, h, "/bucket/datasets/2026/"+name+".csv").StatusCode)
	}
	waitFor(t, "the background refresh", func() bool { return h.ListFallback().Stats().Refreshed == 1 })
	time.Sleep(250 * time.Millisecond)
	assert.Equal(t, 2, listings(upstream), "one listing by the client, one after the burst")
}

// An object store that fails a refresh is in the outage the fallback is for;
// asking it again and again would only add to the load.
func TestARefreshThatFailsIsNotRetried(t *testing.T) {
	h, upstream, o := newFallbackProxy(t, time.Second, withRefreshDelay(20*time.Millisecond))
	require.Equal(t, http.StatusOK, list(t, h, tenantA, "rws").StatusCode)

	o.set(func(o *outage) { o.listStatus = http.StatusInternalServerError })
	require.Equal(t, http.StatusOK, put(t, h, "/bucket/datasets/2026/b.csv").StatusCode)
	waitFor(t, "the background refresh", func() bool { return listings(upstream) == 2 })
	time.Sleep(200 * time.Millisecond)
	assert.Equal(t, 2, listings(upstream))
	assert.Equal(t, http.StatusInternalServerError, list(t, h, tenantA, "rws").StatusCode,
		"and without a refreshed answer there is no fallback: the one from before the write is gone")
}

func TestContinuationPagesAreNotRefreshed(t *testing.T) {
	h, upstream, _ := newFallbackProxy(t, time.Second, withRefreshDelay(20*time.Millisecond))
	page := do(t, h, clientRequest{
		method: http.MethodGet, target: "/bucket?list-type=2&prefix=datasets/&continuation-token=opaque",
		tenant: tenantA, level: "rws",
	}).Result()
	require.Equal(t, http.StatusOK, page.StatusCode)

	require.Equal(t, http.StatusOK, put(t, h, "/bucket/datasets/2026/b.csv").StatusCode)
	time.Sleep(150 * time.Millisecond)
	assert.Equal(t, 1, listings(upstream), "the token belongs to the listing as it was")
}

func TestAMultipartCompletionIsRefreshedAfterItsClaimEnds(t *testing.T) {
	h, upstream, o := newFallbackProxy(t, time.Second, withRefreshDelay(20*time.Millisecond))
	require.Equal(t, http.StatusOK, list(t, h, tenantA, "rws").StatusCode)
	upstream.mu.Lock()
	upstream.respond = func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodPost {
			w.Header().Set("Content-Type", "application/xml")
			_, _ = w.Write([]byte(`<CompleteMultipartUploadResult/>`))
			return
		}
		o.serve(w, r)
	}
	upstream.mu.Unlock()

	body := []byte(`<CompleteMultipartUpload><Part><PartNumber>1</PartNumber><ETag>"p1"</ETag></Part></CompleteMultipartUpload>`)
	require.Equal(t, http.StatusOK, do(t, h, clientRequest{
		method: http.MethodPost, target: "/bucket/datasets/2026/big.bin?uploadId=u1", body: body,
		tenant: tenantA, level: "rw",
	}).Code)
	waitFor(t, "the background refresh", func() bool { return h.ListFallback().Stats().Refreshed == 1 })
}

func TestClosingTheHandlerStopsPendingRefreshes(t *testing.T) {
	h, upstream, _ := newFallbackProxy(t, time.Second, withRefreshDelay(100*time.Millisecond))
	require.Equal(t, http.StatusOK, list(t, h, tenantA, "rws").StatusCode)
	require.Equal(t, http.StatusOK, put(t, h, "/bucket/datasets/2026/b.csv").StatusCode)

	h.Close()
	time.Sleep(250 * time.Millisecond)
	assert.Equal(t, 1, listings(upstream))
}

// A refresh that would overlap a write is put off rather than sent: its
// answer could not be remembered. It is not forgotten either.
func TestARefreshWaitsOutAWriteInFlight(t *testing.T) {
	h, upstream, o := newFallbackProxy(t, time.Second, withRefreshDelay(20*time.Millisecond))
	require.Equal(t, http.StatusOK, list(t, h, tenantA, "rws").StatusCode)
	require.Equal(t, http.StatusOK, put(t, h, "/bucket/datasets/2026/b.csv").StatusCode)

	o.set(func(o *outage) { o.putDelay = 200 * time.Millisecond })
	slow := make(chan int)
	go func() { slow <- put(t, h, "/bucket/datasets/2026/c.csv").StatusCode }()
	time.Sleep(100 * time.Millisecond)
	assert.Equal(t, 1, listings(upstream), "nothing is listed while the write is in flight")
	require.Equal(t, http.StatusOK, <-slow)

	waitFor(t, "the background refresh", func() bool { return h.ListFallback().Stats().Refreshed == 1 })
	o.set(func(o *outage) { o.listStatus = http.StatusInternalServerError })
	body := readBody(t, list(t, h, tenantA, "rws"))
	assert.Contains(t, body, "datasets/2026/b.csv")
	assert.Contains(t, body, "datasets/2026/c.csv", "the answer is from after both writes")
}
