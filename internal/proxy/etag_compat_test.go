package proxy

import (
	"fmt"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/Kriechi/aws-s3-reverse-proxy/internal/cache"
)

// An object store seen in the wild answers `ETag: "'abcef123'"` — a
// single-quoted value inside the standard double quotes — and then refuses
// that same value, single quotes included, as a precondition or in a part
// list. The fixture does both (see sseStore.quirkyETags), so every test here
// fails loudly the moment a single quote travels back upstream.
//
// The assertions are on the requests the store actually received, not on a
// helper's output: the point is what goes over the wire.

func noSingleQuotes(t *testing.T, store *sseStore) {
	t.Helper()
	for _, r := range store.recorded() {
		for _, name := range []string{"If-Match", "If-None-Match", "If-Range"} {
			assert.NotContains(t, r.header.Get(name), "'", "%s %s sent %s with a single quote", r.method, r.path, name)
		}
		if isCompletion(r) {
			assert.NotContains(t, r.body, "'", "a part list with a single quote")
		}
	}
}

func TestQuirkyETagsThroughTheCache(t *testing.T) {
	store := newSSEStore(t)
	store.quirkyETags = true
	var cached *cache.Cache
	h := newEncryptingProxy(t, store, withTestCache(t, &cached, 100*time.Millisecond))

	put := clientPut(t, h, "datasets/a.csv", []byte("hello"))
	require.Equal(t, http.StatusOK, put.StatusCode)
	etag := put.Header.Get("Etag")
	assert.NotContains(t, etag, "'", "the client gets a standard validator")
	value := strings.Trim(etag, `"`)
	assert.Equal(t, value, store.object(upstreamKey("datasets/a.csv")).etag)

	hit := clientGet(t, h, "datasets/a.csv", nil)
	require.Equal(t, "HIT", hit.Header.Get("X-Cache"))
	assert.Equal(t, etag, hit.Header.Get("Etag"), "the cache stores and replays the normalized ETag")

	// A replayed validator matches the cached entry, in either spelling.
	for _, validator := range []string{etag, `"'` + value + `'"`} {
		notModified := clientGet(t, h, "datasets/a.csv", http.Header{"If-None-Match": {validator}})
		assert.Equal(t, http.StatusNotModified, notModified.StatusCode, validator)
	}

	// A freshness check compares canonical values: the store's `"'x'"` is
	// the cached `"x"`, so expiry costs a HEAD rather than a transfer.
	time.Sleep(150 * time.Millisecond)
	revalidated := clientGet(t, h, "datasets/a.csv", nil)
	assert.Equal(t, "REVALIDATED", revalidated.Header.Get("X-Cache"))
	assert.Equal(t, "hello", readBody(t, revalidated))

	// A precondition only the store can evaluate goes upstream — in standard
	// syntax, whatever spelling the client copied.
	conditional := clientGet(t, h, "datasets/a.csv", http.Header{"If-Match": {`"'` + value + `'"`}})
	require.Equal(t, http.StatusOK, conditional.StatusCode, readBody(t, conditional))
	forwarded := store.requestsWhere(func(r recordedRequest) bool { return r.header.Get("If-Match") != "" })
	require.NotEmpty(t, forwarded)
	assert.Equal(t, `"`+value+`"`, forwarded[len(forwarded)-1].header.Get("If-Match"))

	noSingleQuotes(t, store)
}

func TestQuirkyETagsThroughAMultipartUpload(t *testing.T) {
	store := newSSEStore(t)
	store.quirkyETags = true
	h := newEncryptingProxy(t, store)

	create := do(t, h, clientRequest{method: http.MethodPost, target: "/bucket/datasets/big.bin?uploads", tenant: tenantA, level: "rw"}).Result()
	uploadID := between(readBody(t, create), "<UploadId>", "</UploadId>")

	var partETags []string
	for n := 1; n <= 2; n++ {
		part := do(t, h, clientRequest{method: http.MethodPut, target: fmt.Sprintf("/bucket/datasets/big.bin?partNumber=%d&uploadId=%s", n, uploadID),
			tenant: tenantA, level: "rw", body: []byte(fmt.Sprintf("part %d", n))}).Result()
		require.Equal(t, http.StatusOK, part.StatusCode)
		assert.NotContains(t, part.Header.Get("Etag"), "'")
		partETags = append(partETags, part.Header.Get("Etag"))
	}

	list := do(t, h, clientRequest{method: http.MethodGet, target: "/bucket/datasets/big.bin?uploadId=" + uploadID, tenant: tenantA, level: "rw"}).Result()
	listing := readBody(t, list)
	assert.NotContains(t, listing, "&#39;", "a part listing carries normalized ETags too")
	assert.Contains(t, listing, "<ETag>&#34;"+strings.Trim(partETags[0], `"`)+"&#34;</ETag>")

	// One part as the client was given it, one as a client that copied the
	// store's own spelling from somewhere would send it.
	body := fmt.Sprintf(`<CompleteMultipartUpload><Part><PartNumber>1</PartNumber><ETag>%s</ETag></Part>`+
		`<Part><PartNumber>2</PartNumber><ETag>&quot;&apos;%s&apos;&quot;</ETag></Part></CompleteMultipartUpload>`,
		partETags[0], strings.Trim(partETags[1], `"`))
	complete := do(t, h, clientRequest{method: http.MethodPost, target: "/bucket/datasets/big.bin?uploadId=" + uploadID,
		tenant: tenantA, level: "rw", body: []byte(body), headers: http.Header{"Content-Md5": {"c3RhbGUgZGlnZXN0IG9mIHRoZSBjbGllbnQgYm9keQ=="}}}).Result()
	result := readBody(t, complete)
	require.Equal(t, http.StatusOK, complete.StatusCode, result)
	assert.NotContains(t, complete.Header.Get("Etag"), "'")
	assert.NotContains(t, result, "&#39;")
	assert.Equal(t, "part 1part 2", string(store.object(upstreamKey("datasets/big.bin")).data))

	sent := store.requestsWhere(isCompletion)
	require.Len(t, sent, 1)
	assert.Contains(t, sent[0].body, "<ETag>"+strings.Trim(partETags[0], `"`)+"</ETag>", "part values are bare: no double quotes either")
	assert.Empty(t, sent[0].header.Get("Content-Md5"), "the client's digest described the body it sent, not this one")
	noSingleQuotes(t, store)
}

func TestQuirkyETagsThroughARepair(t *testing.T) {
	store := newSSEStore(t)
	store.quirkyETags = true
	h := newEncryptingProxy(t, store)
	key := upstreamKey("datasets/legacy.csv")
	before := store.put(key, []byte("old"), "", nil)

	get := clientGet(t, h, "datasets/legacy.csv", nil)
	require.Equal(t, http.StatusOK, get.StatusCode)
	assert.Equal(t, `"`+before.etag+`"`, get.Header.Get("Etag"))
	waitForRepair(t, store, key)
	h.Close()

	source := store.requestsWhere(func(r recordedRequest) bool { return r.method == http.MethodGet && !hasKey(r) })
	require.NotEmpty(t, source)
	assert.Equal(t, `"`+before.etag+`"`, source[len(source)-1].header.Get("If-Match"), "the source read is pinned with a standard validator")
	complete := store.requestsWhere(isCompletion)
	require.Len(t, complete, 1)
	assert.Equal(t, `"`+before.etag+`"`, complete[0].header.Get("If-Match"), "and so is the replacement")
	noSingleQuotes(t, store)
}
