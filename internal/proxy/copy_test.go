package proxy

import (
	"bytes"
	"encoding/xml"
	"io"
	"net/http"
	"net/http/httptest"
	"strconv"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/Kriechi/aws-s3-reverse-proxy/internal/cache"
	"github.com/Kriechi/aws-s3-reverse-proxy/internal/policy"
	"github.com/Kriechi/aws-s3-reverse-proxy/internal/ssec"
)

// copyRequest is the CopyObject the fleet-insights WebUI sends: a PUT of the
// destination naming the source, with no body and nothing else.
func copyRequest(tenant, level, source, destination string) clientRequest {
	req := clientRequest{method: http.MethodPut, target: "/bucket/" + destination, tenant: tenant, level: level}
	return req.withSource("/bucket/" + source)
}

// withSource replaces the request's headers with x-amz-copy-source as given.
func (c clientRequest) withSource(source string) clientRequest {
	c.headers = http.Header{"X-Amz-Copy-Source": {source}}
	return c
}

// newPlainStoreProxy is the deployment without a customer key, in front of
// the same store.
func newPlainStoreProxy(t *testing.T, store *sseStore, tweaks ...func(*Config)) *Handler {
	return newEncryptingProxy(t, store, append([]func(*Config){func(cfg *Config) {
		cfg.UpstreamSSECKey, cfg.RepairPlaintext = nil, false
	}}, tweaks...)...)
}

type copyResult struct {
	LastModified string `xml:"LastModified"`
	ETag         string `xml:"ETag"`
}

func parseCopyResult(t *testing.T, rec *httptest.ResponseRecorder) copyResult {
	t.Helper()
	require.Equal(t, http.StatusOK, rec.Code, rec.Body.String())
	assert.Equal(t, "application/xml", rec.Header().Get("Content-Type"))
	var result copyResult
	require.NoError(t, xml.Unmarshal(rec.Body.Bytes(), &result))
	return result
}

var sourceHeaders = http.Header{
	"Content-Type":        {"text/csv"},
	"Content-Encoding":    {"br"},
	"Content-Disposition": {`attachment; filename="report.csv"`},
	"Content-Language":    {"de"},
	"Cache-Control":       {"max-age=60"},
	"Expires":             {"Thu, 01 Jan 2032 00:00:00 GMT"},
	"X-Amz-Meta-Author":   {"fleet"},
}

// Without a key: the copy is a read and a write of the store, both under the
// tenant prefix, and the new object has the source's bytes and metadata.
func TestCopyObjectWithoutAKey(t *testing.T) {
	store := newSSEStore(t)
	h := newPlainStoreProxy(t, store)
	store.put(upstreamKey("datasets/src.csv"), []byte("a,b\n1,2\n"), "", sourceHeaders.Clone())

	rec := do(t, h, copyRequest(tenantA, "rw", "datasets/src.csv", "datasets/dst.csv"))
	result := parseCopyResult(t, rec)

	copied := store.object(upstreamKey("datasets/dst.csv"))
	require.NotNil(t, copied)
	assert.Equal(t, "a,b\n1,2\n", string(copied.data))
	assert.Empty(t, copied.keyMD5)
	for name := range sourceHeaders {
		assert.Equal(t, sourceHeaders.Get(name), copied.header.Get(name), name)
	}
	assert.Equal(t, `"`+copied.etag+`"`, result.ETag)
	_, err := time.Parse("2006-01-02T15:04:05.000Z", result.LastModified)
	assert.NoError(t, err)

	requests := store.recorded()
	require.Len(t, requests, 2)
	assert.Equal(t, http.MethodGet, requests[0].method)
	assert.Equal(t, "/bucket/"+upstreamKey("datasets/src.csv"), requests[0].path)
	assert.Equal(t, http.MethodPut, requests[1].method)
	assert.Equal(t, "/bucket/"+upstreamKey("datasets/dst.csv"), requests[1].path)
	for _, r := range requests {
		assert.Empty(t, r.header.Get("X-Amz-Copy-Source"), "no native copy reaches the store")
	}
}

// With the key: the source is read decrypted and the copy is stored under the
// key, whether the source was encrypted or is a legacy plaintext object.
func TestCopyObjectReencryptsUnderTheKey(t *testing.T) {
	for _, tc := range []struct {
		name   string
		keyMD5 string
	}{
		{"an encrypted source", digestOf(testKeyEncoded)},
		{"a plaintext source", ""},
	} {
		t.Run(tc.name, func(t *testing.T) {
			store := newSSEStore(t)
			h := newEncryptingProxy(t, store)
			store.put(upstreamKey("datasets/src.csv"), []byte("secret,bytes\n"), tc.keyMD5, sourceHeaders.Clone())

			rec := do(t, h, copyRequest(tenantA, "rw", "datasets/src.csv", "datasets/dst.csv"))
			parseCopyResult(t, rec)
			for name := range rec.Header() {
				assert.False(t, ssec.IsCustomerHeader(name), "a client response carries no customer-key header: %s", name)
			}

			copied := store.object(upstreamKey("datasets/dst.csv"))
			require.NotNil(t, copied)
			assert.Equal(t, digestOf(testKeyEncoded), copied.keyMD5, "the copy is stored under the key")
			assert.Equal(t, "secret,bytes\n", string(copied.data))
			assert.Equal(t, "fleet", copied.header.Get("X-Amz-Meta-Author"))

			put := store.requestsWhere(func(r recordedRequest) bool {
				return r.method == http.MethodPut && r.path == "/bucket/"+upstreamKey("datasets/dst.csv")
			})
			require.Len(t, put, 1)
			assert.True(t, hasKey(put[0]))

			get := clientGet(t, h, "datasets/dst.csv", nil)
			require.Equal(t, http.StatusOK, get.StatusCode)
			assert.Equal(t, "secret,bytes\n", readBody(t, get))

			if tc.keyMD5 == "" {
				// Reading a plaintext source is a read like any other: the
				// source itself is scheduled for repair.
				waitForRepair(t, store, upstreamKey("datasets/src.csv"))
			}
		})
	}
}

// A missing source is S3's NoSuchKey, and nothing is written.
func TestCopyObjectOfAMissingSource(t *testing.T) {
	store := newSSEStore(t)
	h := newEncryptingProxy(t, store)

	rec := do(t, h, copyRequest(tenantA, "rw", "datasets/nothing.csv", "datasets/dst.csv"))
	assert.Equal(t, http.StatusNotFound, rec.Code)
	assert.Contains(t, rec.Body.String(), "<Code>NoSuchKey</Code>")
	assert.Nil(t, store.object(upstreamKey("datasets/dst.csv")))
	assert.Empty(t, store.requestsWhere(func(r recordedRequest) bool { return r.method == http.MethodPut }))
}

// A source under another key cannot be read, and the store's refusal is
// passed on with the key redacted and the tenant prefix taken out.
func TestCopyObjectOfAnUnreadableSource(t *testing.T) {
	store := newSSEStore(t)
	h := newEncryptingProxy(t, store)
	store.put(upstreamKey("datasets/src.csv"), []byte("x"), digestOf(otherKeyEncoded), nil)

	rec := do(t, h, copyRequest(tenantA, "rw", "datasets/src.csv", "datasets/dst.csv"))
	assert.Equal(t, http.StatusForbidden, rec.Code)
	assert.NotContains(t, rec.Body.String(), tenantA)
	assert.Nil(t, store.object(upstreamKey("datasets/dst.csv")))
}

// A refused write is passed on like a refused PutObject.
func TestCopyObjectWhoseWriteIsRefused(t *testing.T) {
	store := newSSEStore(t)
	store.fail = http.MethodPut
	h := newEncryptingProxy(t, store)
	store.put(upstreamKey("datasets/src.csv"), []byte("x"), digestOf(testKeyEncoded), nil)

	rec := do(t, h, copyRequest(tenantA, "rw", "datasets/src.csv", "datasets/dst.csv"))
	assert.Equal(t, http.StatusInternalServerError, rec.Code)
	assert.Contains(t, rec.Body.String(), "<Code>InternalError</Code>")
}

// Two keys, two decisions, and neither one is enough on its own. A refused
// copy never reaches the object store.
func TestCopyObjectIsAuthorizedOnBothKeys(t *testing.T) {
	for _, tc := range []struct {
		name, level, source, destination string
		allowed                          bool
	}{
		{"read the source, write the destination", "rw", "datasets/2026/a.csv", "workspaces/w1/a.csv", true},
		{"read-only level", "ro", "datasets/2026/a.csv", "workspaces/w1/inbox/a.csv", false},
		{"source the level may not read", "rw", "datasets/2026/private/a.csv", "datasets/2026/a.csv", false},
		{"source no rule covers", "rw", "nowhere/a.csv", "datasets/2026/a.csv", false},
		{"destination the level may only read", "rw", "datasets/2026/a.csv", "workspaces/w1/inbox/a.csv", false},
		{"destination the level may not touch", "rw", "datasets/2026/a.csv", "datasets/2026/private/a.csv", false},
		{"a level that may read the private carve-out", "rws", "datasets/2026/private/a.csv", "datasets/2026/private/b.csv", true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			h, upstream := newTestProxy(t)
			rec := do(t, h, copyRequest(tenantA, tc.level, tc.source, tc.destination))
			if !tc.allowed {
				assert.Equal(t, http.StatusForbidden, rec.Code)
				assert.Zero(t, upstream.count())
				return
			}
			assert.Equal(t, http.StatusOK, rec.Code)
			require.Equal(t, 2, upstream.count())
			assert.Equal(t, "/bucket/"+tenantA+"/"+tc.source, upstream.requests[0].path)
			assert.Equal(t, "/bucket/"+tenantA+"/"+tc.destination, upstream.requests[1].path)
		})
	}
}

// The source is prefixed like the destination: a tenant naming another
// tenant's key, or a key outside its prefix, only ever reads inside its own.
func TestCopyObjectSourceStaysInsideTheTenant(t *testing.T) {
	for _, source := range []string{
		tenantB + "/datasets/2026/a.csv",
		"..%2F" + tenantB + "%2Fdatasets/2026/a.csv",
		"datasets/2026/..%2F..%2F" + tenantB + "/a.csv",
		"%2F" + tenantB + "/datasets/a.csv",
	} {
		t.Run(source, func(t *testing.T) {
			h, upstream := newTestProxy(t)
			rec := do(t, h, copyRequest(tenantA, "rws", "unused", "datasets/2026/dst.csv").withSource("/bucket/"+source))
			assert.NotEqual(t, http.StatusOK, rec.Code)
			assert.Zero(t, upstream.count())
		})
	}

	t.Run("another tenant's key name is a key of one's own", func(t *testing.T) {
		h, upstream := newTestProxy(t)
		rec := do(t, h, copyRequest(tenantA, "rw", "datasets/"+tenantB+"/a.csv", "datasets/dst.csv"))
		require.Equal(t, http.StatusOK, rec.Code)
		assert.Equal(t, "/bucket/"+tenantA+"/datasets/"+tenantB+"/a.csv", upstream.requests[0].path)
	})

	t.Run("another bucket", func(t *testing.T) {
		h, upstream := newTestProxy(t)
		rec := do(t, h, copyRequest(tenantA, "rw", "unused", "datasets/dst.csv").withSource("/other-bucket/datasets/a.csv"))
		assert.Equal(t, http.StatusForbidden, rec.Code)
		assert.Zero(t, upstream.count())
	})
}

// The global credential copies anywhere in the bucket, as keys are upstream,
// when its permission is full — and not at all when it is read.
func TestCopyObjectWithGlobalAccess(t *testing.T) {
	source, destination := tenantA+"/datasets/a.csv", tenantB+"/datasets/a.csv"
	request := func() clientRequest {
		return globalRequest(http.MethodPut, "/bucket/"+destination, nil).withSource("bucket/" + source)
	}

	t.Run("full", func(t *testing.T) {
		h, upstream := newTestProxy(t, withGlobalAccess(policy.PermissionFull))
		require.Equal(t, http.StatusOK, do(t, h, request()).Code)
		require.Equal(t, 2, upstream.count())
		assert.Equal(t, "/bucket/"+source, upstream.requests[0].path)
		assert.Equal(t, "/bucket/"+destination, upstream.requests[1].path)
	})

	t.Run("read", func(t *testing.T) {
		h, upstream := newTestProxy(t, withGlobalAccess(policy.PermissionRead))
		assert.Equal(t, http.StatusForbidden, do(t, h, request()).Code)
		assert.Zero(t, upstream.count())
	})

	t.Run("read-only mode", func(t *testing.T) {
		h, upstream := newTestProxy(t, withGlobalAccess(policy.PermissionFull), func(cfg *Config) { cfg.ReadOnly = true })
		assert.Equal(t, http.StatusForbidden, do(t, h, request()).Code)
		assert.Zero(t, upstream.count())
	})
}

func TestCopyObjectInReadOnlyMode(t *testing.T) {
	h, upstream := newTestProxy(t, func(cfg *Config) { cfg.ReadOnly = true })
	rec := do(t, h, copyRequest(tenantA, "rws", "datasets/a.csv", "datasets/b.csv"))
	assert.Equal(t, http.StatusForbidden, rec.Code)
	assert.Zero(t, upstream.count())
}

// The destination is a write like a PutObject: what the cache held for it is
// replaced, and a remembered listing that cannot show it is dropped.
func TestCopyObjectInvalidatesTheDestination(t *testing.T) {
	t.Run("cache", func(t *testing.T) {
		var cached *cache.Cache
		store := newSSEStore(t)
		h := newEncryptingProxy(t, store, withTestCache(t, &cached, time.Hour))
		store.put(upstreamKey("datasets/src.csv"), []byte("new"), digestOf(testKeyEncoded), nil)
		store.put(upstreamKey("datasets/dst.csv"), []byte("old"), digestOf(testKeyEncoded), nil)

		require.Equal(t, "old", readBody(t, clientGet(t, h, "datasets/dst.csv", nil)))
		require.True(t, cached.Has("bucket/"+upstreamKey("datasets/dst.csv")))

		parseCopyResult(t, do(t, h, copyRequest(tenantA, "rw", "datasets/src.csv", "datasets/dst.csv")))

		get := clientGet(t, h, "datasets/dst.csv", nil)
		assert.Equal(t, "HIT", get.Header.Get("X-Cache"), "the copy filled the cache like an upload does")
		assert.Equal(t, "new", readBody(t, get))
	})

	t.Run("listing fallback", func(t *testing.T) {
		h, upstream, o := newFallbackProxy(t, time.Second)
		upstream.mu.Lock()
		upstream.respond = func(w http.ResponseWriter, r *http.Request) {
			if r.Method == http.MethodGet && !r.URL.Query().Has("list-type") {
				w.Header().Set("Content-Length", "1")
				_, _ = w.Write([]byte("x"))
				return
			}
			o.serve(w, r)
		}
		upstream.mu.Unlock()
		require.Equal(t, http.StatusOK, list(t, h, tenantA, "rws").StatusCode)

		rec := do(t, h, copyRequest(tenantA, "rw", "datasets/2026/a.csv", "datasets/2026/b.csv"))
		require.Equal(t, http.StatusOK, rec.Code)
		o.set(func(o *outage) { o.listStatus = http.StatusInternalServerError })
		assert.Equal(t, http.StatusInternalServerError, list(t, h, tenantA, "rws").StatusCode,
			"the remembered listing does not show b.csv, so it is no longer an answer")
	})

	t.Run("a copy policy refuses keeps the listing", func(t *testing.T) {
		h, _, o := newFallbackProxy(t, time.Second)
		require.Equal(t, http.StatusOK, list(t, h, tenantA, "rws").StatusCode)
		rec := do(t, h, copyRequest(tenantA, "ro", "datasets/2026/a.csv", "datasets/2026/b.csv"))
		require.Equal(t, http.StatusForbidden, rec.Code)
		o.set(func(o *outage) { o.listStatus = http.StatusInternalServerError })
		assert.Equal(t, http.StatusOK, list(t, h, tenantA, "rws").StatusCode)
	})
}

// The source streams into the destination: the write starts before the read
// has finished, so memory is bounded by the copy buffer, not the object.
func TestCopyObjectStreams(t *testing.T) {
	const half = 1 << 20
	putStarted := make(chan struct{})
	var received int
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.Method {
		case http.MethodGet:
			w.Header().Set("Content-Length", strconv.Itoa(2*half))
			_, _ = w.Write(bytes.Repeat([]byte("a"), half))
			w.(http.Flusher).Flush()
			select {
			case <-putStarted:
			case <-time.After(5 * time.Second):
				return // the proxy is waiting for the whole source
			}
			_, _ = w.Write(bytes.Repeat([]byte("b"), half))
		case http.MethodPut:
			assert.Equal(t, int64(2*half), r.ContentLength)
			_, err := io.ReadFull(r.Body, make([]byte, half))
			require.NoError(t, err)
			close(putStarted)
			rest, _ := io.ReadAll(r.Body)
			received = half + len(rest)
			w.Header().Set("Etag", `"streamed"`)
		}
	}))
	t.Cleanup(server.Close)
	h, err := New(testConfig(t, server.URL))
	require.NoError(t, err)

	result := parseCopyResult(t, do(t, h, copyRequest(tenantA, "rw", "datasets/big.bin", "datasets/copy.bin")))
	assert.Equal(t, `"streamed"`, result.ETag)
	assert.Equal(t, 2*half, received)
}

// The access log names the operation and both keys.
func TestCopyObjectIsLogged(t *testing.T) {
	hook := captureLogs(t)
	h, _ := newTestProxy(t)
	require.Equal(t, http.StatusOK, do(t, h, copyRequest(tenantA, "rw", "datasets/a.csv", "datasets/b.csv")).Code)

	var found bool
	for _, entry := range hook.AllEntries() {
		if entry.Message == "request served" && entry.Data["operation"] == "CopyObject" {
			found = true
			assert.Equal(t, "datasets/b.csv", entry.Data["key"])
			assert.Equal(t, "datasets/a.csv", entry.Data["source_key"])
			assert.Equal(t, "datasets/**", entry.Data["rule"])
		}
	}
	assert.True(t, found, "no access log line for the copy")
}
