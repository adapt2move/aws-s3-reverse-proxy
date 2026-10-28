package proxy

import (
	"bytes"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	log "github.com/sirupsen/logrus"
	"github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/Kriechi/aws-s3-reverse-proxy/internal/cache"
	"github.com/Kriechi/aws-s3-reverse-proxy/internal/ssec"
)

// newEncryptingProxy is the deployment with a customer key, in front of a
// store that does SSE-C over TLS.
func newEncryptingProxy(t *testing.T, store *sseStore, tweaks ...func(*Config)) *Handler {
	t.Helper()
	cfg := testConfig(t, "")
	cfg.UpstreamScheme = "https"
	cfg.UpstreamEndpoint = store.host()
	cfg.rootCAs = store.rootCAs()
	cfg.UpstreamSSECKey = mustKey(t, testKeyEncoded)
	cfg.RepairPlaintext = true
	for _, tweak := range tweaks {
		tweak(&cfg)
	}
	h, err := New(cfg)
	require.NoError(t, err)
	// Repairs run in the background; a test must not end with one still
	// writing into the next test's store.
	t.Cleanup(h.Close)
	return h
}

// withTestCache gives the deployment a cache, and returns it through cached.
func withTestCache(t *testing.T, cached **cache.Cache, maxAge time.Duration) func(*Config) {
	return withTestCacheIn(t, t.TempDir(), cached, maxAge)
}

func withTestCacheIn(t *testing.T, dir string, cached **cache.Cache, maxAge time.Duration) func(*Config) {
	return func(cfg *Config) {
		c, err := cache.New(cache.Options{
			Dir:           dir,
			MaxBytes:      8 << 20,
			MaxObjectSize: 1 << 20,
			MaxAge:        maxAge,
		})
		require.NoError(t, err)
		t.Cleanup(func() { _ = c.Close() })
		cfg.Cache, cfg.CacheWrites, cfg.CacheRangeFills = c, true, true
		*cached = c
	}
}

// captureLogs records every log entry for the length of a test. The suite
// otherwise runs with logging silenced, which is right for asserting on
// responses and wrong for asserting that an operator was told something.
var logMu sync.Mutex

func captureLogs(t *testing.T) *test.Hook {
	t.Helper()
	logMu.Lock()
	hook := test.NewGlobal()
	level, out := log.GetLevel(), log.StandardLogger().Out
	log.SetLevel(log.DebugLevel)
	log.SetOutput(io.Discard)
	t.Cleanup(func() {
		log.SetLevel(level)
		log.SetOutput(out)
		log.StandardLogger().ReplaceHooks(make(log.LevelHooks))
		logMu.Unlock()
	})
	return hook
}

// logged reports whether an entry at this level mentions the fragment.
func logged(hook *test.Hook, level log.Level, fragment string) bool {
	for _, entry := range hook.AllEntries() {
		if entry.Level == level && strings.Contains(entry.Message, fragment) {
			return true
		}
	}
	return false
}

func upstreamKey(key string) string { return tenantA + "/" + key }

func clientGet(t *testing.T, h *Handler, key string, header http.Header) *http.Response {
	t.Helper()
	return do(t, h, clientRequest{method: http.MethodGet, target: "/bucket/" + key, tenant: tenantA, level: "rw", headers: header}).Result()
}

func clientPut(t *testing.T, h *Handler, key string, body []byte) *http.Response {
	t.Helper()
	return do(t, h, clientRequest{method: http.MethodPut, target: "/bucket/" + key, tenant: tenantA, level: "rw", body: body}).Result()
}

func hasKey(r recordedRequest) bool { return r.header.Get(ssec.HeaderKey) != "" }

func waitForRepair(t *testing.T, store *sseStore, key string) *sseObject {
	t.Helper()
	var obj *sseObject
	waitFor(t, "the object to be encrypted", func() bool {
		obj = store.object(key)
		return obj != nil && obj.keyMD5 != ""
	})
	return obj
}

func TestAKeyRequiresAnHTTPSUpstream(t *testing.T) {
	cfg := testConfig(t, "http://minio:9000")
	cfg.UpstreamSSECKey = mustKey(t, testKeyEncoded)
	_, err := New(cfg)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "https")
}

// The everyday path: a client that knows nothing about encryption writes and
// reads, and the object is encrypted at rest under the operator's key.
func TestObjectsAreWrittenAndReadWithTheKey(t *testing.T) {
	store := newSSEStore(t)
	h := newEncryptingProxy(t, store)

	put := clientPut(t, h, "datasets/a.csv", []byte("column,value\n1,2\n"))
	require.Equal(t, http.StatusOK, put.StatusCode)
	for name := range put.Header {
		assert.False(t, ssec.IsCustomerHeader(name), "a client response carries no customer-key header: %s", name)
	}

	stored := store.object(upstreamKey("datasets/a.csv"))
	require.NotNil(t, stored)
	assert.Equal(t, digestOf(testKeyEncoded), stored.keyMD5, "stored under the configured key")

	write := store.requestsWhere(func(r recordedRequest) bool { return r.method == http.MethodPut })[0]
	assert.True(t, hasKey(write))
	signed := signedHeaders(t, write.header.Get("Authorization"))
	assert.Contains(t, signed, "x-amz-server-side-encryption-customer-key", "the key is covered by the upstream signature")

	get := clientGet(t, h, "datasets/a.csv", nil)
	require.Equal(t, http.StatusOK, get.StatusCode)
	assert.Equal(t, "column,value\n1,2\n", readBody(t, get))
	assert.Empty(t, get.Header.Get(ssec.HeaderAlgorithm))
	assert.Empty(t, get.Header.Get(ssec.HeaderKeyMD5))

	ranged := clientGet(t, h, "datasets/a.csv", http.Header{"Range": {"bytes=0-5"}})
	require.Equal(t, http.StatusPartialContent, ranged.StatusCode)
	assert.Equal(t, "column", readBody(t, ranged))

	head := do(t, h, clientRequest{method: http.MethodHead, target: "/bucket/datasets/a.csv", tenant: tenantA, level: "ro"}).Result()
	assert.Equal(t, http.StatusOK, head.StatusCode)

	for _, r := range store.recorded() {
		assert.True(t, hasKey(r), "%s %s went upstream without the key", r.method, r.path)
	}
}

// The key goes where S3 takes it and nowhere else: listing, deleting and
// abandoning an upload never carry it.
func TestOnlyObjectOperationsCarryTheKey(t *testing.T) {
	store := newSSEStore(t)
	h := newEncryptingProxy(t, store)

	create := do(t, h, clientRequest{method: http.MethodPost, target: "/bucket/datasets/big.bin?uploads", tenant: tenantA, level: "rw"}).Result()
	require.Equal(t, http.StatusOK, create.StatusCode)
	uploadID := between(readBody(t, create), "<UploadId>", "</UploadId>")
	require.NotEmpty(t, uploadID)

	part := do(t, h, clientRequest{method: http.MethodPut, target: "/bucket/datasets/big.bin?partNumber=1&uploadId=" + uploadID, tenant: tenantA, level: "rw", body: []byte("part one")}).Result()
	require.Equal(t, http.StatusOK, part.StatusCode)
	partETag := part.Header.Get("Etag")

	list := do(t, h, clientRequest{method: http.MethodGet, target: "/bucket/datasets/big.bin?uploadId=" + uploadID, tenant: tenantA, level: "rw"}).Result()
	require.Equal(t, http.StatusOK, list.StatusCode)

	complete := do(t, h, clientRequest{method: http.MethodPost, target: "/bucket/datasets/big.bin?uploadId=" + uploadID, tenant: tenantA, level: "rw",
		body: []byte(fmt.Sprintf(`<CompleteMultipartUpload><Part><PartNumber>1</PartNumber><ETag>%s</ETag></Part></CompleteMultipartUpload>`, partETag))}).Result()
	require.Equal(t, http.StatusOK, complete.StatusCode, readBody(t, complete))
	assert.Equal(t, digestOf(testKeyEncoded), store.object(upstreamKey("datasets/big.bin")).keyMD5)

	abandoned := do(t, h, clientRequest{method: http.MethodPost, target: "/bucket/datasets/other.bin?uploads", tenant: tenantA, level: "rw"}).Result()
	otherID := between(readBody(t, abandoned), "<UploadId>", "</UploadId>")
	abort := do(t, h, clientRequest{method: http.MethodDelete, target: "/bucket/datasets/other.bin?uploadId=" + otherID, tenant: tenantA, level: "rw"}).Result()
	require.Equal(t, http.StatusNoContent, abort.StatusCode)

	del := do(t, h, clientRequest{method: http.MethodDelete, target: "/bucket/datasets/big.bin", tenant: tenantA, level: "rw"}).Result()
	require.Equal(t, http.StatusNoContent, del.StatusCode)
	listing := do(t, h, clientRequest{method: http.MethodGet, target: "/bucket?list-type=2&prefix=datasets/", tenant: tenantA, level: "rw"}).Result()
	require.Equal(t, http.StatusOK, listing.StatusCode)

	for _, r := range store.recorded() {
		_, isUpload := r.query["uploadId"]
		_, isCreate := r.query["uploads"]
		switch {
		case r.method == http.MethodDelete, r.path == "/bucket":
			assert.False(t, hasKey(r), "%s %s?%s must not carry the key", r.method, r.path, r.query.Encode())
		case isCreate, isUpload, r.method == http.MethodGet:
			assert.True(t, hasKey(r), "%s %s?%s must carry the key", r.method, r.path, r.query.Encode())
		}
	}
}

// A client that sends its own key is told, plainly, that this proxy does not
// do that — before anything reaches the object store or the cache, and with
// or without a key of the proxy's own.
func TestClientKeysAreRefusedWithAnExplanation(t *testing.T) {
	for _, configured := range []bool{true, false} {
		store := newSSEStore(t)
		var cached *cache.Cache
		h := newEncryptingProxy(t, store, withTestCache(t, &cached, 0), func(cfg *Config) {
			if !configured {
				cfg.UpstreamSSECKey = nil
			}
		})
		secret := otherKeyEncoded
		for _, header := range []http.Header{
			{ssec.HeaderAlgorithm: {"AES256"}, ssec.HeaderKey: {secret}, ssec.HeaderKeyMD5: {digestOf(secret)}},
			{ssec.HeaderKey: {secret}},
			{ssec.HeaderAlgorithm: {""}},
			{"X-Amz-Copy-Source-Server-Side-Encryption-Customer-Key": {secret}},
		} {
			for _, method := range []string{http.MethodGet, http.MethodPut, http.MethodHead} {
				resp := do(t, h, clientRequest{method: method, target: "/bucket/datasets/a.csv", tenant: tenantA, level: "rw", headers: header, body: []byte("x")}).Result()
				assert.Equal(t, http.StatusBadRequest, resp.StatusCode)
				body := readBody(t, resp)
				if method != http.MethodHead {
					assert.Contains(t, body, "<Code>InvalidRequest</Code>")
					assert.Contains(t, body, "client-provided encryption keys are not passed through")
				}
				assert.NotContains(t, body, secret)
				assert.NotContains(t, body, digestOf(secret))
			}
		}
		assert.Empty(t, store.recorded(), "nothing reached the object store")
		assert.Zero(t, cached.Stats().Entries, "nothing reached the cache")
	}
}

// A store that accepts a write without confirming encryption gets the write
// accepted: the client sees its success, the cache treats it like any other,
// and the operator gets a warning. Nothing is retried, and nothing is retried
// without a key.
func TestAnUnconfirmedWriteWarnsAndSucceeds(t *testing.T) {
	hook := captureLogs(t)
	store := newSSEStore(t)
	store.ignoreSSEC = true
	var cached *cache.Cache
	h := newEncryptingProxy(t, store, withTestCache(t, &cached, 0))

	put := clientPut(t, h, "datasets/a.csv", []byte("hello"))
	require.Equal(t, http.StatusOK, put.StatusCode)
	assert.True(t, logged(hook, log.WarnLevel, "without confirming SSE-C"))
	writes := store.requestsWhere(func(r recordedRequest) bool { return r.method == http.MethodPut })
	require.Len(t, writes, 1, "the write is never retried")
	assert.True(t, hasKey(writes[0]))

	get := clientGet(t, h, "datasets/a.csv", nil)
	assert.Equal(t, "HIT", get.Header.Get("X-Cache"), "an unconfirmed write is still cached as usual")
	assert.Equal(t, "hello", readBody(t, get))
}

func TestAWriteTheStoreRefusesStaysRefused(t *testing.T) {
	store := newSSEStore(t)
	store.fail = http.MethodPut
	h := newEncryptingProxy(t, store)

	put := clientPut(t, h, "datasets/a.csv", []byte("hello"))
	assert.Equal(t, http.StatusInternalServerError, put.StatusCode)
	writes := store.requestsWhere(func(r recordedRequest) bool { return r.method == http.MethodPut })
	require.Len(t, writes, 1, "a refused write is not retried, with or without a key")
	assert.True(t, hasKey(writes[0]))
}

// The legacy path, end to end: a plaintext object is read through, and then
// encrypted in place with its bytes and metadata intact.
func TestAPlaintextObjectIsServedAndThenEncrypted(t *testing.T) {
	hook := captureLogs(t)
	store := newSSEStore(t)
	h := newEncryptingProxy(t, store)
	key := upstreamKey("datasets/legacy.csv")
	before := store.put(key, []byte("legacy,bytes\n"), "", http.Header{
		"Content-Type":   {"text/csv"},
		"X-Amz-Meta-Who": {"somebody"},
	})

	get := clientGet(t, h, "datasets/legacy.csv", nil)
	require.Equal(t, http.StatusOK, get.StatusCode)
	assert.Equal(t, "legacy,bytes\n", readBody(t, get))
	assert.Equal(t, "text/csv", get.Header.Get("Content-Type"))
	assert.Empty(t, get.Header.Get(unconfirmedMarker), "the marker is never shown to a client")

	after := waitForRepair(t, store, key)
	assert.Equal(t, digestOf(testKeyEncoded), after.keyMD5)
	assert.Equal(t, before.data, after.data, "the bytes are the same bytes")
	assert.Equal(t, "text/csv", after.header.Get("Content-Type"), "content headers survive")
	assert.Equal(t, "somebody", after.header.Get("X-Amz-Meta-Who"), "metadata survives")
	h.Close()
	assert.Zero(t, store.openUploads(), "nothing is left behind")
	assert.True(t, logged(hook, log.InfoLevel, "encrypted a plaintext object"),
		"a completion the store accepted is reported as one, even when shutdown starts while it is answered")

	// The replacement was conditional on the version that was read, enforced
	// by the store; and the completion carried no key, so the store could
	// evaluate that condition against a plaintext object.
	complete := store.requestsWhere(func(r recordedRequest) bool {
		_, ok := r.query["uploadId"]
		return r.method == http.MethodPost && ok
	})
	require.Len(t, complete, 1)
	assert.Equal(t, `"`+before.etag+`"`, complete[0].header.Get("If-Match"))
	assert.False(t, hasKey(complete[0]))

	// And the next read is an ordinary encrypted one.
	again := clientGet(t, h, "datasets/legacy.csv", nil)
	assert.Equal(t, "legacy,bytes\n", readBody(t, again))
}

// The AWS SDKs v2 and v3 mark a read with `?x-id=GetObject`, which travels
// upstream as the client signed it. It must not hide the read from the
// legacy path.
func TestAPlaintextReadWithTheSDKMarkerIsServed(t *testing.T) {
	store := newSSEStore(t)
	h := newEncryptingProxy(t, store)
	key := upstreamKey("datasets/legacy.csv")
	store.put(key, []byte("legacy"), "", nil)

	get := do(t, h, clientRequest{method: http.MethodGet, target: "/bucket/datasets/legacy.csv?x-id=GetObject", tenant: tenantA, level: "rw"}).Result()
	require.Equal(t, http.StatusOK, get.StatusCode)
	assert.Equal(t, "legacy", readBody(t, get))
	waitForRepair(t, store, key)
}

// A range is what the client asked for; it is never what the object is
// replaced with.
func TestARangedReadOfAPlaintextObjectRepairsFromTheWholeObject(t *testing.T) {
	store := newSSEStore(t)
	h := newEncryptingProxy(t, store)
	key := upstreamKey("datasets/legacy.bin")
	store.put(key, []byte("0123456789"), "", nil)

	ranged := clientGet(t, h, "datasets/legacy.bin", http.Header{"Range": {"bytes=2-4"}})
	require.Equal(t, http.StatusPartialContent, ranged.StatusCode)
	assert.Equal(t, "234", readBody(t, ranged))

	after := waitForRepair(t, store, key)
	assert.Equal(t, "0123456789", string(after.data))

	head := do(t, h, clientRequest{method: http.MethodHead, target: "/bucket/datasets/legacy.bin", tenant: tenantA, level: "ro"}).Result()
	assert.Equal(t, http.StatusOK, head.StatusCode)
	assert.Equal(t, "10", head.Header.Get("Content-Length"))
}

func TestAHeadOfAPlaintextObjectIsServed(t *testing.T) {
	store := newSSEStore(t)
	h := newEncryptingProxy(t, store)
	key := upstreamKey("datasets/legacy.bin")
	store.put(key, []byte("0123456789"), "", http.Header{"Content-Type": {"application/x-test"}})

	head := do(t, h, clientRequest{method: http.MethodHead, target: "/bucket/datasets/legacy.bin", tenant: tenantA, level: "ro"}).Result()
	require.Equal(t, http.StatusOK, head.StatusCode)
	assert.Equal(t, "application/x-test", head.Header.Get("Content-Type"))
	assert.Equal(t, "10", head.Header.Get("Content-Length"))
	waitForRepair(t, store, key)
}

// An object encrypted under a different key is an error the operator has to
// hear about — and nothing else: no read without the key, no rewrite.
func TestAnObjectUnderAnotherKeyIsAnError(t *testing.T) {
	hook := captureLogs(t)
	store := newSSEStore(t)
	h := newEncryptingProxy(t, store)
	key := upstreamKey("datasets/foreign.csv")
	store.put(key, []byte("secret"), digestOf(otherKeyEncoded), nil)

	for _, method := range []string{http.MethodGet, http.MethodHead} {
		resp := do(t, h, clientRequest{method: method, target: "/bucket/datasets/foreign.csv", tenant: tenantA, level: "rw"}).Result()
		assert.Equal(t, http.StatusForbidden, resp.StatusCode, method)
		assert.NotContains(t, readBody(t, resp), "secret")
	}
	assert.True(t, logged(hook, log.ErrorLevel, "encrypted with another key"))
	h.Close()
	for _, r := range store.recorded() {
		assert.True(t, hasKey(r), "no request without the key: %s %s", r.method, r.path)
		assert.NotEqual(t, http.MethodPost, r.method, "no rewrite")
	}
	assert.Equal(t, digestOf(otherKeyEncoded), store.object(key).keyMD5)
}

// Without a configured key, an encrypted object cannot be read, and the
// operator is told why.
func TestWithoutAKeyAnEncryptedObjectIsAnError(t *testing.T) {
	hook := captureLogs(t)
	store := newSSEStore(t)
	h := newEncryptingProxy(t, store, func(cfg *Config) { cfg.UpstreamSSECKey = nil })
	store.put(upstreamKey("datasets/enc.csv"), []byte("secret"), digestOf(testKeyEncoded), nil)

	get := clientGet(t, h, "datasets/enc.csv", nil)
	assert.Equal(t, http.StatusBadRequest, get.StatusCode)
	assert.Contains(t, readBody(t, get), "Server Side Encryption")
	assert.True(t, logged(hook, log.ErrorLevel, "has none configured"))
}

// A 400 is only ever a reason to ask. One that is not the "not applicable"
// refusal is passed on as it is; a HEAD's bodyless 400 is asked about, and
// when the keyless HEAD does not show a plaintext object, it stands too.
func TestAnAmbiguousRefusalIsNotTakenForPlaintext(t *testing.T) {
	hook := captureLogs(t)
	store := newSSEStore(t)
	store.refuseReads = "Some other reason entirely."
	h := newEncryptingProxy(t, store)
	store.put(upstreamKey("datasets/a.csv"), []byte("x"), "", nil)

	get := clientGet(t, h, "datasets/a.csv", nil)
	assert.Equal(t, http.StatusBadRequest, get.StatusCode)
	assert.Contains(t, readBody(t, get), "Some other reason entirely.")
	assert.True(t, logged(hook, log.ErrorLevel, "refused a read with the configured SSE-C key"))
	assert.Empty(t, store.requestsWhere(func(r recordedRequest) bool { return !hasKey(r) }), "a GET's other 400 is not even asked about")

	head := do(t, h, clientRequest{method: http.MethodHead, target: "/bucket/datasets/a.csv", tenant: tenantA, level: "rw"}).Result()
	assert.Equal(t, http.StatusBadRequest, head.StatusCode)
	assert.True(t, logged(hook, log.ErrorLevel, "could not be shown to be stored without one"))
	h.Close()

	assert.Empty(t, store.requestsWhere(func(r recordedRequest) bool { return r.method == http.MethodGet && !hasKey(r) }),
		"nothing was read without the key")
	assert.Empty(t, store.requestsWhere(func(r recordedRequest) bool { return r.method == http.MethodPost }), "and nothing rewritten")
}

// The store answers a plaintext refusal for a GET, but the keyless HEAD that
// is meant to confirm it fails: the state is unknown, so the original refusal
// stands and nothing is rewritten.
func TestAnUnconfirmedPlaintextStateKeepsTheRefusal(t *testing.T) {
	hook := captureLogs(t)
	store := newSSEStore(t)
	h := newEncryptingProxy(t, store)
	store.put(upstreamKey("datasets/legacy.csv"), []byte("x"), "", nil)
	store.mu.Lock()
	store.fail = http.MethodHead
	store.mu.Unlock()

	get := clientGet(t, h, "datasets/legacy.csv", nil)
	assert.Equal(t, http.StatusBadRequest, get.StatusCode)
	assert.Contains(t, readBody(t, get), "not applicable")
	assert.True(t, logged(hook, log.ErrorLevel, "could not be shown to be stored without one"))
	h.Close()
	assert.Empty(t, store.requestsWhere(func(r recordedRequest) bool { return r.method == http.MethodGet && !hasKey(r) }))
	assert.Equal(t, "", store.object(upstreamKey("datasets/legacy.csv")).keyMD5)
}

// The two ways another writer can get in while a repair runs. Either way the
// store refuses the conditional completion, the other writer's outcome
// stands, and the upload is abandoned.
func TestARepairNeverOverwritesAConcurrentReplacement(t *testing.T) {
	store := newSSEStore(t)
	h := newEncryptingProxy(t, store)
	key := upstreamKey("datasets/legacy.csv")
	store.put(key, []byte("old"), "", nil)
	var once sync.Once
	store.beforeComplete = func() {
		once.Do(func() { store.put(key, []byte("new, written by somebody else"), "", nil) })
	}

	require.Equal(t, http.StatusOK, clientGet(t, h, "datasets/legacy.csv", nil).StatusCode)
	waitFor(t, "the completion", func() bool { return len(store.requestsWhere(isCompletion)) > 0 })
	h.Close()

	assert.Equal(t, "new, written by somebody else", string(store.object(key).data))
	assert.Zero(t, store.openUploads(), "the abandoned upload was aborted")
	assert.Len(t, store.requestsWhere(isCompletion), 1, "and not retried")
}

func TestARepairNeverRecreatesADeletedObject(t *testing.T) {
	store := newSSEStore(t)
	h := newEncryptingProxy(t, store)
	key := upstreamKey("datasets/legacy.csv")
	store.put(key, []byte("old"), "", nil)
	var once sync.Once
	store.beforeComplete = func() {
		once.Do(func() {
			store.mu.Lock()
			delete(store.objects, key)
			store.mu.Unlock()
		})
	}

	require.Equal(t, http.StatusOK, clientGet(t, h, "datasets/legacy.csv", nil).StatusCode)
	waitFor(t, "the completion", func() bool { return len(store.requestsWhere(isCompletion)) > 0 })
	h.Close()

	assert.Nil(t, store.object(key), "the delete stands")
	assert.Zero(t, store.openUploads())
}

func isCompletion(r recordedRequest) bool {
	_, ok := r.query["uploadId"]
	return r.method == http.MethodPost && ok
}

// A store that cannot replace an object conditionally still serves it, and
// keeps it as it is: the repair does not weaken its condition to get through.
func TestWithoutConditionalReplacementTheRepairIsSkipped(t *testing.T) {
	hook := captureLogs(t)
	store := newSSEStore(t)
	store.noConditionalComplete = true
	h := newEncryptingProxy(t, store)
	key := upstreamKey("datasets/legacy.csv")
	store.put(key, []byte("old"), "", nil)

	get := clientGet(t, h, "datasets/legacy.csv", nil)
	require.Equal(t, http.StatusOK, get.StatusCode)
	assert.Equal(t, "old", readBody(t, get))
	waitFor(t, "the attempt", func() bool { return logged(hook, log.WarnLevel, "does not support conditional replacement") })
	store.put(upstreamKey("datasets/other.csv"), []byte("other"), "", nil)
	require.Equal(t, http.StatusOK, clientGet(t, h, "datasets/other.csv", nil).StatusCode)
	h.Close()

	assert.Equal(t, "", store.object(key).keyMD5, "still plaintext")
	assert.Zero(t, store.openUploads())
	assert.Len(t, store.requestsWhere(isCompletion), 1, "and the next object is not even tried while repairs are suspended")
	for _, r := range store.requestsWhere(isCompletion) {
		assert.NotEmpty(t, r.header.Get("If-Match"), "never an unconditional completion")
	}
}

func TestNoRepairInReadOnlyMode(t *testing.T) {
	store := newSSEStore(t)
	h := newEncryptingProxy(t, store, func(cfg *Config) { cfg.ReadOnly = true })
	key := upstreamKey("datasets/legacy.csv")
	store.put(key, []byte("old"), "", nil)

	get := clientGet(t, h, "datasets/legacy.csv", nil)
	require.Equal(t, http.StatusOK, get.StatusCode)
	assert.Equal(t, "old", readBody(t, get))
	h.Close()
	assert.Empty(t, store.requestsWhere(func(r recordedRequest) bool { return r.method == http.MethodPost || r.method == http.MethodPut }))
}

// A path the policy lets no level write is one the operator made read-only
// for everybody; the repair is held to that.
func TestNoRepairUnderAPathNobodyMayWrite(t *testing.T) {
	hook := captureLogs(t)
	store := newSSEStore(t)
	h := newEncryptingProxy(t, store)
	key := upstreamKey("workspaces/w1/inbox/legacy.csv")
	store.put(key, []byte("old"), "", nil)

	get := do(t, h, clientRequest{method: http.MethodGet, target: "/bucket/workspaces/w1/inbox/legacy.csv", tenant: tenantA, level: "rw"}).Result()
	require.Equal(t, http.StatusOK, get.StatusCode)
	waitFor(t, "the skip", func() bool {
		return logged(hook, log.WarnLevel, "no access level of the current policy may write this path")
	})
	h.Close()
	assert.Equal(t, "", store.object(key).keyMD5)
	assert.Empty(t, store.requestsWhere(func(r recordedRequest) bool { return r.method == http.MethodPost }))
}

func TestNoRepairOfATaggedObject(t *testing.T) {
	hook := captureLogs(t)
	store := newSSEStore(t)
	h := newEncryptingProxy(t, store)
	key := upstreamKey("datasets/tagged.csv")
	obj := store.put(key, []byte("old"), "", nil)
	store.mu.Lock()
	obj.tags = 2
	store.mu.Unlock()

	require.Equal(t, http.StatusOK, clientGet(t, h, "datasets/tagged.csv", nil).StatusCode)
	waitFor(t, "the skip", func() bool { return logged(hook, log.WarnLevel, "the object has tags") })
	h.Close()
	assert.Equal(t, "", store.object(key).keyMD5)
}

// A failed repair costs nothing but the attempt: the read succeeded, the
// upload is cleaned up, and a later read — after the retry interval — tries
// again.
func TestAFailedRepairIsRetriedByALaterRead(t *testing.T) {
	hook := captureLogs(t)
	store := newSSEStore(t)
	store.failPart = 1
	h := newEncryptingProxy(t, store)
	clock := time.Now()
	h.repairs.now = func() time.Time { return clock }
	key := upstreamKey("datasets/legacy.csv")
	store.put(key, []byte("old"), "", nil)

	require.Equal(t, http.StatusOK, clientGet(t, h, "datasets/legacy.csv", nil).StatusCode)
	waitFor(t, "the failure", func() bool { return logged(hook, log.WarnLevel, "a later read will retry it") })
	waitFor(t, "the abort", func() bool { return store.openUploads() == 0 })

	// Too soon: no second attempt.
	creates := func() int {
		return len(store.requestsWhere(func(r recordedRequest) bool { _, ok := r.query["uploads"]; return ok }))
	}
	require.Equal(t, http.StatusOK, clientGet(t, h, "datasets/legacy.csv", nil).StatusCode)
	assert.Equal(t, 1, creates())

	store.mu.Lock()
	store.failPart = 0
	store.mu.Unlock()
	clock = clock.Add(repairRetryAfter + time.Second)
	require.Equal(t, http.StatusOK, clientGet(t, h, "datasets/legacy.csv", nil).StatusCode)
	waitForRepair(t, store, key)
	assert.Equal(t, 2, creates())
}

// A store that ignores SSE-C entirely answers every read without confirming
// it. The repair notices on its first part, abandons the upload before it
// replaces anything, and stops every other repair with it: rewriting the
// bucket object by object to no effect is the loop this has to avoid.
func TestAStoreThatIgnoresTheKeySuspendsRepairs(t *testing.T) {
	hook := captureLogs(t)
	store := newSSEStore(t)
	store.ignoreSSEC = true
	h := newEncryptingProxy(t, store)
	store.put(upstreamKey("datasets/a.csv"), []byte("a"), "", nil)
	store.put(upstreamKey("datasets/b.csv"), []byte("b"), "", nil)

	require.Equal(t, http.StatusOK, clientGet(t, h, "datasets/a.csv", nil).StatusCode)
	waitFor(t, "the suspension", func() bool { return logged(hook, log.WarnLevel, "suspending repairs") })
	require.Equal(t, http.StatusOK, clientGet(t, h, "datasets/b.csv", nil).StatusCode)
	h.Close()

	creates := store.requestsWhere(func(r recordedRequest) bool { _, ok := r.query["uploads"]; return ok })
	assert.Len(t, creates, 1, "one attempt, then nothing")
	assert.Len(t, store.requestsWhere(isPartUpload), 1, "that one stopped at its first part")
	assert.Empty(t, store.requestsWhere(isCompletion), "and replaced nothing")
	assert.Zero(t, store.openUploads(), "and cleaned up")
}

// MinIO confirms SSE-C on the parts of a multipart upload, not on its
// initiation. A multipart upload through the proxy is therefore not a
// warning — demanding a confirmation an operation does not give would make
// every one of them look like a failure.
func TestAMultipartUploadDoesNotWarnAboutItsInitiation(t *testing.T) {
	hook := captureLogs(t)
	store := newSSEStore(t)
	h := newEncryptingProxy(t, store)

	create := do(t, h, clientRequest{method: http.MethodPost, target: "/bucket/datasets/big.bin?uploads", tenant: tenantA, level: "rw"}).Result()
	uploadID := between(readBody(t, create), "<UploadId>", "</UploadId>")
	part := do(t, h, clientRequest{method: http.MethodPut, target: "/bucket/datasets/big.bin?partNumber=1&uploadId=" + uploadID, tenant: tenantA, level: "rw", body: []byte("x")}).Result()
	require.Equal(t, http.StatusOK, part.StatusCode)
	for _, entry := range hook.AllEntries() {
		assert.GreaterOrEqual(t, entry.Level, log.InfoLevel, "unexpected %s: %s", entry.Level, entry.Message)
	}
}

// An object larger than the cache takes, and larger than one part, streams
// through the repair part by part.
func TestALargeObjectIsRepairedInParts(t *testing.T) {
	store := newSSEStore(t)
	var cached *cache.Cache
	h := newEncryptingProxy(t, store, withTestCache(t, &cached, 0))
	key := upstreamKey("datasets/large.bin")
	data := bytes.Repeat([]byte("0123456789abcdef"), (repairPartSize*2+12345)/16)
	store.put(key, data, "", nil)

	get := clientGet(t, h, "datasets/large.bin", nil)
	require.Equal(t, http.StatusOK, get.StatusCode)
	assert.Equal(t, len(data), len(readBody(t, get)))

	after := waitForRepair(t, store, key)
	assert.True(t, bytes.Equal(data, after.data))
	parts := store.requestsWhere(func(r recordedRequest) bool { _, ok := r.query["partNumber"]; return ok })
	assert.Len(t, parts, 3)
	for _, part := range parts {
		assert.Equal(t, "UNSIGNED-PAYLOAD", part.header.Get("X-Amz-Content-Sha256"), "parts stream; nothing hashes them first")
	}
	assert.Zero(t, cached.Stats().Entries, "too large for the cache, repaired anyway")
}

// The cache and the repair: a plaintext read is cached, the cached entry keeps
// the repair from being forgotten, and the repair's replacement invalidates
// it — including a read that was still being captured when it landed.
func TestARepairInvalidatesWhatTheCacheHeld(t *testing.T) {
	store := newSSEStore(t)
	store.failPart = 1
	var cached *cache.Cache
	h := newEncryptingProxy(t, store, withTestCache(t, &cached, 0))
	clock := time.Now()
	h.repairs.now = func() time.Time { return clock }
	key := upstreamKey("datasets/legacy.csv")
	store.put(key, []byte("old"), "", nil)

	first := clientGet(t, h, "datasets/legacy.csv", nil)
	require.Equal(t, "old", readBody(t, first))
	waitFor(t, "the failed attempt", func() bool { return store.openUploads() == 0 && len(store.requestsWhere(isPartUpload)) > 0 })

	hit := clientGet(t, h, "datasets/legacy.csv", nil)
	require.Equal(t, "HIT", hit.Header.Get("X-Cache"))
	assert.Empty(t, hit.Header.Get(unconfirmedMarker))
	staleETag := hit.Header.Get("Etag")

	store.mu.Lock()
	store.failPart = 0
	store.mu.Unlock()
	clock = clock.Add(repairRetryAfter + time.Second)

	// A hit on the cached plaintext entry is enough to schedule the repair.
	require.Equal(t, "HIT", clientGet(t, h, "datasets/legacy.csv", nil).Header.Get("X-Cache"))
	waitForRepair(t, store, key)
	h.Close()

	after := clientGet(t, h, "datasets/legacy.csv", nil)
	assert.Equal(t, "MISS", after.Header.Get("X-Cache"), "the repair invalidated the plaintext entry")
	assert.Equal(t, "old", readBody(t, after))
	assert.NotEqual(t, staleETag, after.Header.Get("Etag"), "and the client sees the new version's ETag")
}

// An old read that is still streaming when the replacement lands must not
// commit what it read.
func TestAReadInFlightDuringARepairIsNotCached(t *testing.T) {
	store := newSSEStore(t)
	var cached *cache.Cache
	h := newEncryptingProxy(t, store, withTestCache(t, &cached, 0))
	key := upstreamKey("datasets/legacy.csv")
	cacheKey := "bucket/" + key
	store.put(key, []byte("old"), "", nil)

	// The capture starts while the repair is uploading, before its
	// completion…
	var inflight *cache.Writer
	store.beforePart = func() {
		if inflight == nil {
			w, err := cached.Put(cacheKey, 3)
			if err == nil {
				_, _ = w.Write([]byte("old"))
				inflight = w
			}
		}
	}
	// …from a read that does not itself capture (a HEAD).
	head := do(t, h, clientRequest{method: http.MethodHead, target: "/bucket/datasets/legacy.csv", tenant: tenantA, level: "rw"}).Result()
	require.Equal(t, http.StatusOK, head.StatusCode)
	waitForRepair(t, store, key)
	h.Close()

	require.NotNil(t, inflight)
	_ = inflight.Commit(http.Header{"Etag": {`"stale"`}})
	entry, result := cached.Get(cacheKey)
	if entry != nil {
		entry.Close()
	}
	assert.Equal(t, cache.Miss, result, "the stale capture was thrown away")
}

func isPartUpload(r recordedRequest) bool { _, ok := r.query["partNumber"]; return ok }

// An expired entry of a plaintext object revalidates like any other — and its
// revalidation is a read that schedules the repair.
func TestAnExpiredPlaintextEntryRevalidates(t *testing.T) {
	store := newSSEStore(t)
	store.failPart = 1 // keep it plaintext for the revalidation
	var cached *cache.Cache
	h := newEncryptingProxy(t, store, withTestCache(t, &cached, 50*time.Millisecond))
	store.put(upstreamKey("datasets/legacy.csv"), []byte("old"), "", nil)

	require.Equal(t, "MISS", clientGet(t, h, "datasets/legacy.csv", nil).Header.Get("X-Cache"))
	time.Sleep(80 * time.Millisecond)
	again := clientGet(t, h, "datasets/legacy.csv", nil)
	assert.Equal(t, "REVALIDATED", again.Header.Get("X-Cache"))
	assert.Equal(t, "old", readBody(t, again))
	revalidation := store.requestsWhere(func(r recordedRequest) bool { return r.method == http.MethodHead && hasKey(r) })
	assert.NotEmpty(t, revalidation, "the revalidation asked with the key")
}

// A ranged miss fills the cache in the background, with the key, for an
// encrypted object and for a plaintext one alike.
//
// The plaintext object's repair is kept from completing: a repair that lands
// while the fill is still being written discards the fill — correctly, since
// it describes the version the repair replaced — and the point here is the
// fill.
func TestARangedMissFillsWithTheKey(t *testing.T) {
	store := newSSEStore(t)
	store.failPart = 1
	var cached *cache.Cache
	h := newEncryptingProxy(t, store, withTestCache(t, &cached, 0))
	store.put(upstreamKey("datasets/enc.bin"), []byte("0123456789"), digestOf(testKeyEncoded), nil)
	store.put(upstreamKey("datasets/plain.bin"), []byte("abcdefghij"), "", nil)

	for _, name := range []string{"enc.bin", "plain.bin"} {
		ranged := clientGet(t, h, "datasets/"+name, http.Header{"Range": {"bytes=1-2"}})
		require.Equal(t, http.StatusPartialContent, ranged.StatusCode, name)
		waitFor(t, "the fill of "+name, func() bool { return cached.Has("bucket/" + upstreamKey("datasets/"+name)) })
	}
	h.Close()
	hit := clientGet(t, h, "datasets/enc.bin", nil)
	assert.Equal(t, "HIT", hit.Header.Get("X-Cache"))
	assert.Equal(t, "0123456789", readBody(t, hit))
}

// The key goes to the object store and nowhere else: not to a client, not to
// a log line, not into the cache's files.
func TestTheKeyDoesNotLeak(t *testing.T) {
	hook := captureLogs(t)
	store := newSSEStore(t)
	var cached *cache.Cache
	dir := t.TempDir()
	h := newEncryptingProxy(t, store, withTestCacheIn(t, dir, &cached, 0))
	store.put(upstreamKey("datasets/legacy.csv"), []byte("old"), "", nil)
	store.put(upstreamKey("datasets/foreign.csv"), []byte("x"), digestOf(otherKeyEncoded), nil)

	var seen bytes.Buffer
	record := func(resp *http.Response) {
		_ = resp.Header.Write(&seen)
		seen.WriteString(readBody(t, resp))
	}
	record(clientPut(t, h, "datasets/a.csv", []byte("hello")))
	record(clientGet(t, h, "datasets/a.csv", nil))
	record(clientGet(t, h, "datasets/legacy.csv", nil))
	record(clientGet(t, h, "datasets/foreign.csv", nil))
	record(clientGet(t, h, "datasets/missing.csv", nil))
	waitForRepair(t, store, upstreamKey("datasets/legacy.csv"))
	h.Close()

	for _, entry := range hook.AllEntries() {
		line, _ := entry.String()
		seen.WriteString(line)
	}
	_ = filepath.Walk(dir, func(path string, info os.FileInfo, err error) error {
		if err == nil && !info.IsDir() {
			raw, _ := os.ReadFile(path)
			seen.Write(raw)
		}
		return nil
	})
	for _, secret := range []string{testKeyEncoded, digestOf(testKeyEncoded), otherKeyEncoded, digestOf(otherKeyEncoded)} {
		assert.NotContains(t, seen.String(), secret)
	}
}

func between(s, open, close string) string {
	_, rest, ok := strings.Cut(s, open)
	if !ok {
		return ""
	}
	value, _, _ := strings.Cut(rest, close)
	return value
}
