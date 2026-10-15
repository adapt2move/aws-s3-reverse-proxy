package proxy

import (
	"net/http"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	log "github.com/sirupsen/logrus"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/Kriechi/aws-s3-reverse-proxy/internal/cache"
)

// An object store's error can quote the request it refused — the proxy's
// re-signed request, customer key included. What reaches the client is the
// error, not the request.
func TestAnUpstreamErrorNeverCarriesTheKey(t *testing.T) {
	store := newSSEStore(t)
	store.echoSignatureErrors = true
	h := newEncryptingProxy(t, store)

	for _, req := range []clientRequest{
		{method: http.MethodGet, target: "/bucket/datasets/a.csv"},
		{method: http.MethodPut, target: "/bucket/datasets/a.csv", body: []byte("x")},
		{method: http.MethodPut, target: "/bucket/datasets/a.csv", body: []byte("x"), headers: http.Header{"If-None-Match": {"*"}}},
		{method: http.MethodPost, target: "/bucket/datasets/a.csv?uploads"},
	} {
		req.tenant, req.level = tenantA, "rw"
		resp := do(t, h, req).Result()
		body := readBody(t, resp)
		require.Equal(t, http.StatusForbidden, resp.StatusCode, "%s %s", req.method, req.target)
		assert.Contains(t, body, "<Code>SignatureDoesNotMatch</Code>", "the error itself survives")
		assert.Contains(t, body, "<RequestId>R1</RequestId>")
		for _, secret := range []string{testKeyEncoded, digestOf(testKeyEncoded), "UPSTREAMKEYID", "CanonicalRequest", "x-amz-server-side-encryption-customer-key"} {
			assert.NotContains(t, body, secret, "%s %s", req.method, req.target)
		}
		assert.NotContains(t, strings.ToLower(body), strings.ToLower(hexOf(testKeyEncoded)))
	}
}

func hexOf(s string) string {
	const digits = "0123456789abcdef"
	out := make([]byte, 0, 2*len(s))
	for i := 0; i < len(s); i++ {
		out = append(out, digits[s[i]>>4], digits[s[i]&0x0f])
	}
	return string(out)
}

// Without UPSTREAM_SSE_C_REPAIR a plaintext object is served and left as it
// is: nothing is rewritten, and the operator is told why.
func TestRepairIsOffUnlessAskedFor(t *testing.T) {
	hook := captureLogs(t)
	store := newSSEStore(t)
	h := newEncryptingProxy(t, store, func(cfg *Config) { cfg.RepairPlaintext = false })
	key := upstreamKey("datasets/legacy.csv")
	store.put(key, []byte("old"), "", nil)

	get := clientGet(t, h, "datasets/legacy.csv", nil)
	require.Equal(t, http.StatusOK, get.StatusCode)
	assert.Equal(t, "old", readBody(t, get))
	waitFor(t, "the skip", func() bool { return logged(hook, log.InfoLevel, "repair is off") })
	h.Close()
	assert.Empty(t, store.requestsWhere(func(r recordedRequest) bool { return r.method == http.MethodPost }))
	assert.Empty(t, store.object(key).keyMD5)
}

// Tags added while the parts were uploading are visible to the final
// observation, and a replacement would drop them: the repair stops.
func TestTagsAddedDuringARepairStopIt(t *testing.T) {
	hook := captureLogs(t)
	store := newSSEStore(t)
	h := newEncryptingProxy(t, store)
	key := upstreamKey("datasets/legacy.csv")
	obj := store.put(key, []byte("old"), "", nil)
	store.beforePart = func() {
		store.mu.Lock()
		store.objects[key].tags = 2
		store.mu.Unlock()
	}
	_ = obj

	require.Equal(t, http.StatusOK, clientGet(t, h, "datasets/legacy.csv", nil).StatusCode)
	waitFor(t, "the skip", func() bool { return logged(hook, log.WarnLevel, "the object has tags") })
	h.Close()
	after := store.object(key)
	assert.Equal(t, 2, after.tags, "the tags are still there")
	assert.Empty(t, after.keyMD5)
	assert.Empty(t, store.requestsWhere(isCompletion))
	assert.Zero(t, store.openUploads())
}

// A rewritten object gets the default ACL. An object with any other grant,
// one whose ACL cannot be read, one that would change owner, and one whose
// ACL changes during the repair are all left as they are.
func TestARepairKeepsAccessGrantsOrSkips(t *testing.T) {
	for _, tc := range []struct {
		name   string
		setup  func(store *sseStore, key string)
		reason string
	}{
		{"an extra grant", func(store *sseStore, key string) {
			store.objects[key].grants = `<Grant><Grantee><URI>http://acs.amazonaws.com/groups/global/AllUsers</URI></Grantee><Permission>READ</Permission></Grant>`
		}, "access grants beyond its owner's"},
		{"an unreadable ACL", func(store *sseStore, _ string) { store.aclStatus = http.StatusForbidden }, "could not be read"},
		{"another owner", func(store *sseStore, _ string) { store.uploadOwner = "owner-2" }, "would belong to another owner"},
		{"a grant added during the repair", func(store *sseStore, key string) {
			store.beforePart = func() {
				store.mu.Lock()
				store.objects[key].grants = `<Grant><Grantee><ID>owner-3</ID></Grantee><Permission>READ</Permission></Grant>`
				store.mu.Unlock()
			}
		}, "access grants beyond its owner's"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			hook := captureLogs(t)
			store := newSSEStore(t)
			h := newEncryptingProxy(t, store)
			key := upstreamKey("datasets/legacy.csv")
			store.put(key, []byte("old"), "", nil)
			store.mu.Lock()
			tc.setup(store, key)
			store.mu.Unlock()

			require.Equal(t, http.StatusOK, clientGet(t, h, "datasets/legacy.csv", nil).StatusCode)
			waitFor(t, "the skip", func() bool { return logged(hook, log.WarnLevel, tc.reason) })
			h.Close()
			assert.Empty(t, store.object(key).keyMD5, "still as it was")
			assert.Empty(t, store.requestsWhere(isCompletion))
			assert.Zero(t, store.openUploads())
		})
	}
}

// An upload the store accepted without confirming the key is cached with
// that question still open. When the store does encrypt again, a later cache
// hit has the object checked and repaired, instead of serving the plaintext
// object's entry for as long as it lives.
func TestAnUnconfirmedUploadStaysEligibleThroughTheCache(t *testing.T) {
	store := newSSEStore(t)
	store.ignoreSSEC = true
	var cached *cache.Cache
	h := newEncryptingProxy(t, store, withTestCache(t, &cached, 0))
	newTestClock(h)
	key := upstreamKey("datasets/a.csv")

	require.Equal(t, http.StatusOK, clientPut(t, h, "datasets/a.csv", []byte("hello")).StatusCode)
	assert.Empty(t, store.object(key).keyMD5, "the store ignored the key")

	store.mu.Lock()
	store.ignoreSSEC = false
	store.mu.Unlock()
	hit := clientGet(t, h, "datasets/a.csv", nil)
	require.Equal(t, "HIT", hit.Header.Get("X-Cache"))
	assert.Empty(t, hit.Header.Get(unconfirmedMarker))
	waitForRepair(t, store, key)
	h.Close()
	assert.Equal(t, "MISS", clientGet(t, h, "datasets/a.csv", nil).Header.Get("X-Cache"), "the repair cleared the entry")
}

// The same for a read the store answered without confirming the key: what
// the cache keeps of it stays eligible once the store encrypts again, past
// the suspension that store's earlier behaviour earned.
func TestAnUnconfirmedReadStaysEligibleThroughTheCache(t *testing.T) {
	store := newSSEStore(t)
	store.ignoreSSEC = true
	var cached *cache.Cache
	h := newEncryptingProxy(t, store, withTestCache(t, &cached, 0))
	clock := newTestClock(h)
	key := upstreamKey("datasets/a.csv")
	store.put(key, []byte("hello"), "", nil)

	require.Equal(t, "MISS", clientGet(t, h, "datasets/a.csv", nil).Header.Get("X-Cache"))
	waitFor(t, "the first attempt to give up", func() bool { return store.openUploads() == 0 && len(store.requestsWhere(isPartUpload)) == 1 })

	store.mu.Lock()
	store.ignoreSSEC = false
	store.mu.Unlock()
	clock.advance(repairSuspension + time.Second)
	require.Equal(t, "HIT", clientGet(t, h, "datasets/a.csv", nil).Header.Get("X-Cache"))
	waitForRepair(t, store, key)
}

// testClock is the repairer's clock, moved by hand and safe to move while a
// repair is reading it.
type testClock struct{ offset atomic.Int64 }

func newTestClock(h *Handler) *testClock {
	c := &testClock{}
	start := time.Now()
	h.repairs.now = func() time.Time { return start.Add(time.Duration(c.offset.Load())) }
	return c
}

func (c *testClock) advance(d time.Duration) { c.offset.Add(int64(d)) }
