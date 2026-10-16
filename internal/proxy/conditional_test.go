package proxy

import (
	"fmt"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/Kriechi/aws-s3-reverse-proxy/internal/cache"
)

// Conditional writes onto an object that is still plaintext. The store the
// fixture models — the pinned MinIO — refuses them with 400 when they carry
// the key, so these tests fail outright without conditional.go.

func conditionalPut(t *testing.T, h *Handler, key, body string, header http.Header) *http.Response {
	t.Helper()
	header = header.Clone()
	header.Set("Content-Type", "text/plain")
	return do(t, h, clientRequest{method: http.MethodPut, target: "/bucket/" + key, tenant: tenantA, level: "rw",
		body: []byte(body), headers: header}).Result()
}

func TestAConditionalPutOntoAPlaintextObject(t *testing.T) {
	cases := []struct {
		name      string
		condition func(etag string) http.Header
		status    int
	}{
		{"If-Match the current ETag", func(etag string) http.Header { return http.Header{"If-Match": {`"` + etag + `"`}} }, http.StatusOK},
		{"If-Match another ETag", func(string) http.Header { return http.Header{"If-Match": {`"not-it"`}} }, http.StatusPreconditionFailed},
		{"If-None-Match *", func(string) http.Header { return http.Header{"If-None-Match": {"*"}} }, http.StatusPreconditionFailed},
		{"If-None-Match another ETag", func(string) http.Header { return http.Header{"If-None-Match": {`"not-it"`}} }, http.StatusOK},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			store := newSSEStore(t)
			h := newEncryptingProxy(t, store)
			key := upstreamKey("datasets/legacy.csv")
			before := store.put(key, []byte("old"), "", nil)

			resp := conditionalPut(t, h, "datasets/legacy.csv", "new", tc.condition(before.etag))
			body := readBody(t, resp)
			require.Equal(t, tc.status, resp.StatusCode, body)
			assert.NotContains(t, body, tenantA, "an error document names the key as the client knows it")
			for name := range resp.Header {
				assert.NotContains(t, name, "Customer", "no customer-key header reaches the client")
			}

			after := store.object(key)
			if tc.status == http.StatusOK {
				assert.Equal(t, "new", string(after.data))
				assert.Equal(t, digestOf(testKeyEncoded), after.keyMD5, "the write is encrypted")
				assert.Equal(t, "text/plain", after.header.Get("Content-Type"), "and keeps what the client said about it")
				assert.Equal(t, `"`+after.etag+`"`, resp.Header.Get("Etag"))
			} else {
				assert.Equal(t, "old", string(after.data), "a failed condition changes nothing")
				assert.Empty(t, after.keyMD5)
			}
			h.Close()
			assert.Zero(t, store.openUploads(), "no upload is left behind")
			for _, r := range store.requestsWhere(isCompletion) {
				assert.False(t, hasKey(r), "the store evaluates the condition against the stored object")
				assert.True(t, hasWriteCondition(r.header), "with the client's own condition")
			}
		})
	}
}

// Onto an encrypted object, or a key that does not exist yet, a conditional
// PUT goes upstream the ordinary way: the store evaluates it with the key.
func TestAConditionalPutOntoAnythingElseIsForwarded(t *testing.T) {
	store := newSSEStore(t)
	h := newEncryptingProxy(t, store)
	encrypted := store.put(upstreamKey("datasets/enc.csv"), []byte("old"), digestOf(testKeyEncoded), nil)

	resp := conditionalPut(t, h, "datasets/enc.csv", "new", http.Header{"If-Match": {`"` + encrypted.etag + `"`}})
	require.Equal(t, http.StatusOK, resp.StatusCode, readBody(t, resp))
	resp = conditionalPut(t, h, "datasets/fresh.csv", "new", http.Header{"If-None-Match": {"*"}})
	require.Equal(t, http.StatusOK, resp.StatusCode, readBody(t, resp))

	assert.Empty(t, store.requestsWhere(func(r recordedRequest) bool { _, ok := r.query["uploads"]; return ok }), "no upload was needed")
	assert.Equal(t, digestOf(testKeyEncoded), store.object(upstreamKey("datasets/fresh.csv")).keyMD5)
}

// The cache treats the rewritten PUT like any other: the uploaded body is what
// a read of the object returns next.
func TestAConditionalPutOntoAPlaintextObjectIsCached(t *testing.T) {
	store := newSSEStore(t)
	var cached *cache.Cache
	h := newEncryptingProxy(t, store, withTestCache(t, &cached, 0))
	before := store.put(upstreamKey("datasets/legacy.csv"), []byte("old"), "", nil)

	resp := conditionalPut(t, h, "datasets/legacy.csv", "new", http.Header{"If-Match": {`"` + before.etag + `"`}})
	require.Equal(t, http.StatusOK, resp.StatusCode)
	get := clientGet(t, h, "datasets/legacy.csv", nil)
	assert.Equal(t, "HIT", get.Header.Get("X-Cache"))
	assert.Equal(t, "new", readBody(t, get))
	assert.Equal(t, resp.Header.Get("Etag"), get.Header.Get("Etag"))
}

// A client's conditional completion over a plaintext object is refused by the
// store while it carries the key, and sent once more without it. The parts
// carried the key, so the assembled object is encrypted all the same.
func TestAConditionalCompletionOntoAPlaintextObject(t *testing.T) {
	for _, tc := range []struct {
		name   string
		match  func(etag string) string
		status int
	}{
		{"matching", func(etag string) string { return `"` + etag + `"` }, http.StatusOK},
		{"not matching", func(string) string { return `"not-it"` }, http.StatusPreconditionFailed},
	} {
		t.Run(tc.name, func(t *testing.T) {
			store := newSSEStore(t)
			h := newEncryptingProxy(t, store)
			key := upstreamKey("datasets/big.bin")
			before := store.put(key, []byte("old"), "", nil)

			create := do(t, h, clientRequest{method: http.MethodPost, target: "/bucket/datasets/big.bin?uploads", tenant: tenantA, level: "rw"}).Result()
			uploadID := between(readBody(t, create), "<UploadId>", "</UploadId>")
			part := do(t, h, clientRequest{method: http.MethodPut, target: "/bucket/datasets/big.bin?partNumber=1&uploadId=" + uploadID,
				tenant: tenantA, level: "rw", body: []byte("new")}).Result()
			require.Equal(t, http.StatusOK, part.StatusCode)

			complete := do(t, h, clientRequest{method: http.MethodPost, target: "/bucket/datasets/big.bin?uploadId=" + uploadID, tenant: tenantA, level: "rw",
				headers: http.Header{"If-Match": {tc.match(before.etag)}},
				body:    []byte(fmt.Sprintf(`<CompleteMultipartUpload><Part><PartNumber>1</PartNumber><ETag>%s</ETag></Part></CompleteMultipartUpload>`, part.Header.Get("Etag")))}).Result()
			require.Equal(t, tc.status, complete.StatusCode, readBody(t, complete))

			after := store.object(key)
			if tc.status == http.StatusOK {
				assert.Equal(t, "new", string(after.data))
				assert.Equal(t, digestOf(testKeyEncoded), after.keyMD5)
			} else {
				assert.Equal(t, "old", string(after.data))
			}
			attempts := store.requestsWhere(isCompletion)
			require.Len(t, attempts, 2, "first with the key, then once without")
			assert.True(t, hasKey(attempts[0]))
			assert.False(t, hasKey(attempts[1]))
			assert.Equal(t, attempts[0].body, attempts[1].body)
		})
	}
}

// An unconditional completion keeps the key, which AWS S3 needs there for an
// upload with checksums; and a conditional one over an encrypted object is
// sent once.
func TestOtherCompletionsKeepTheKey(t *testing.T) {
	store := newSSEStore(t)
	h := newEncryptingProxy(t, store)
	store.put(upstreamKey("datasets/enc.bin"), []byte("old"), digestOf(testKeyEncoded), nil)

	for _, condition := range []http.Header{nil, {"If-None-Match": {`"not-it"`}}} {
		create := do(t, h, clientRequest{method: http.MethodPost, target: "/bucket/datasets/enc.bin?uploads", tenant: tenantA, level: "rw"}).Result()
		uploadID := between(readBody(t, create), "<UploadId>", "</UploadId>")
		part := do(t, h, clientRequest{method: http.MethodPut, target: "/bucket/datasets/enc.bin?partNumber=1&uploadId=" + uploadID,
			tenant: tenantA, level: "rw", body: []byte("new")}).Result()
		complete := do(t, h, clientRequest{method: http.MethodPost, target: "/bucket/datasets/enc.bin?uploadId=" + uploadID, tenant: tenantA, level: "rw",
			headers: condition,
			body:    []byte(fmt.Sprintf(`<CompleteMultipartUpload><Part><PartNumber>1</PartNumber><ETag>%s</ETag></Part></CompleteMultipartUpload>`, part.Header.Get("Etag")))}).Result()
		require.Equal(t, http.StatusOK, complete.StatusCode, readBody(t, complete))
	}
	for _, r := range store.requestsWhere(isCompletion) {
		assert.True(t, hasKey(r))
	}
	assert.Len(t, store.requestsWhere(isCompletion), 2)
}
