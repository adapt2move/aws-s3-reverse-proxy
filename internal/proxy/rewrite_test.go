package proxy

import (
	"bytes"
	"compress/gzip"
	"fmt"
	"net/http"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func respondXML(u *fakeUpstream, body string) {
	u.respond = func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/xml")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(body))
	}
}

// The client never sees the prefix the proxy injected — otherwise a key it
// read back out of a listing would be double-prefixed on the next GET.
func TestListingResponseIsStripped(t *testing.T) {
	h, upstream := newTestProxy(t)
	respondXML(upstream, `<?xml version="1.0" encoding="UTF-8"?>
<ListBucketResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/">
  <Name>bucket</Name>
  <Prefix>`+tenantA+`/datasets/</Prefix>
  <NextMarker>`+tenantA+`/datasets/z.csv</NextMarker>
  <NextContinuationToken>1ueGcxLPRx1Tr/XYExHnhbYLgveDs2J/</NextContinuationToken>
  <Contents><Key>`+tenantA+`/datasets/a.csv</Key><Size>10</Size></Contents>
  <Contents><Key>`+tenantA+`/datasets/b.csv</Key><Size>20</Size></Contents>
  <CommonPrefixes><Prefix>`+tenantA+`/datasets/2026/</Prefix></CommonPrefixes>
</ListBucketResult>`)

	rec := do(t, h, clientRequest{
		method: http.MethodGet, target: "/bucket/?list-type=2&prefix=datasets/",
		tenant: tenantA, level: "ro",
	})
	require.Equal(t, http.StatusOK, rec.Code)

	out := rec.Body.String()
	assert.NotContains(t, out, tenantA, "no upstream-shaped key may reach the client")
	assert.Contains(t, out, "<Key>datasets/a.csv</Key>")
	assert.Contains(t, out, "<Prefix>datasets/</Prefix>")
	assert.Contains(t, out, "<NextMarker>datasets/z.csv</NextMarker>")
	// The opaque pagination token is left exactly as the upstream minted
	// it — it is not a key, and touching it would break the next page.
	assert.Contains(t, out, "<NextContinuationToken>1ueGcxLPRx1Tr/XYExHnhbYLgveDs2J/</NextContinuationToken>")
	assert.Equal(t, int64(rec.Body.Len()), rec.Result().ContentLength,
		"the rewritten body has to carry its own length")
}

// boto3 and DuckDB's httpfs list with EncodingType=url by default, so the
// prefix arrives percent-encoded and has to be matched in that form too.
func TestListingResponseIsStrippedWhenURLEncoded(t *testing.T) {
	h, upstream := newTestProxy(t)
	respondXML(upstream, `<ListBucketResult>`+
		`<Contents><Key>`+tenantA+`%2Fdatasets%2Fa.csv</Key></Contents>`+
		`</ListBucketResult>`)

	rec := do(t, h, clientRequest{
		method: http.MethodGet, target: "/bucket/?list-type=2&prefix=datasets/&encoding-type=url",
		tenant: tenantA, level: "ro",
	})
	require.Equal(t, http.StatusOK, rec.Code)
	assert.Contains(t, rec.Body.String(), "<Key>datasets%2Fa.csv</Key>")
	assert.NotContains(t, rec.Body.String(), tenantA)
}

// "Not matched by a rule is denied" has to hold for the listing that would
// reveal a key just as much as for a GET of it.
func TestListingHidesUnreadableEntries(t *testing.T) {
	h, upstream := newTestProxy(t)
	respondXML(upstream, `<ListBucketResult>`+
		`<Contents><Key>`+tenantA+`/workspaces/w1/out/visible.csv</Key></Contents>`+
		`<Contents><Key>`+tenantA+`/nowhere/secret.csv</Key></Contents>`+
		`<CommonPrefixes><Prefix>`+tenantA+`/workspaces/w2/</Prefix></CommonPrefixes>`+
		`<CommonPrefixes><Prefix>`+tenantA+`/nowhere/</Prefix></CommonPrefixes>`+
		`</ListBucketResult>`)

	rec := do(t, h, clientRequest{
		method: http.MethodGet, target: "/bucket/?list-type=2&prefix=workspaces/",
		tenant: tenantA, level: "ro",
	})
	require.Equal(t, http.StatusOK, rec.Code)

	out := rec.Body.String()
	assert.Contains(t, out, "workspaces/w1/out/visible.csv")
	assert.Contains(t, out, "<Prefix>workspaces/w2/</Prefix>")
	assert.NotContains(t, out, "secret.csv")
	assert.NotContains(t, out, "nowhere/")
}

// The same holds for a path a rule denies outright: it sits under a prefix
// the caller may list, so nothing but the per-entry check keeps it out of
// the answer.
func TestListingHidesDeniedEntries(t *testing.T) {
	h, upstream := newTestProxy(t)
	body := `<ListBucketResult>` +
		`<Contents><Key>` + tenantA + `/datasets/2026/a.csv</Key></Contents>` +
		`<Contents><Key>` + tenantA + `/datasets/2026/private/salaries.csv</Key></Contents>` +
		`<CommonPrefixes><Prefix>` + tenantA + `/datasets/2026/private/</Prefix></CommonPrefixes>` +
		`</ListBucketResult>`

	t.Run("a denied level does not see them", func(t *testing.T) {
		respondXML(upstream, body)
		rec := do(t, h, clientRequest{
			method: http.MethodGet, target: "/bucket/?list-type=2&prefix=datasets/",
			tenant: tenantA, level: "rw",
		})
		require.Equal(t, http.StatusOK, rec.Code)

		out := rec.Body.String()
		assert.Contains(t, out, "<Key>datasets/2026/a.csv</Key>")
		assert.NotContains(t, out, "salaries.csv")
		assert.NotContains(t, out, "private/")
	})

	t.Run("the level the rule grants still sees them", func(t *testing.T) {
		respondXML(upstream, body)
		rec := do(t, h, clientRequest{
			method: http.MethodGet, target: "/bucket/?list-type=2&prefix=datasets/",
			tenant: tenantA, level: "rws",
		})
		require.Equal(t, http.StatusOK, rec.Code)
		assert.Contains(t, rec.Body.String(), "<Key>datasets/2026/private/salaries.csv</Key>")
	})
}

// A CompleteMultipartUpload result names the key twice — once bare, once
// inside a URL.
func TestMultipartResultIsStripped(t *testing.T) {
	h, upstream := newTestProxy(t)
	respondXML(upstream, `<CompleteMultipartUploadResult>`+
		`<Location>http://s3.example.com/bucket/`+tenantA+`/datasets/big.parquet</Location>`+
		`<Key>`+tenantA+`/datasets/big.parquet</Key>`+
		`</CompleteMultipartUploadResult>`)

	rec := do(t, h, clientRequest{
		method: http.MethodPost, target: "/bucket/datasets/big.parquet?uploadId=abc",
		body: []byte("<CompleteMultipartUpload/>"), tenant: tenantA, level: "rw",
	})
	require.Equal(t, http.StatusOK, rec.Code)

	out := rec.Body.String()
	assert.Contains(t, out, "<Key>datasets/big.parquet</Key>")
	assert.Contains(t, out, "<Location>http://s3.example.com/bucket/datasets/big.parquet</Location>")
	assert.NotContains(t, out, tenantA)
}

// An upstream error names the path it was acting on, which carries the
// prefix.
func TestErrorResponseIsStripped(t *testing.T) {
	h, upstream := newTestProxy(t)
	upstream.respond = func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/xml")
		w.WriteHeader(http.StatusNotFound)
		_, _ = w.Write([]byte(`<Error><Code>NoSuchKey</Code><Resource>/bucket/` + tenantA + `/datasets/gone.csv</Resource></Error>`))
	}

	rec := do(t, h, clientRequest{
		method: http.MethodGet, target: "/bucket/datasets/gone.csv",
		tenant: tenantA, level: "ro",
	})
	require.Equal(t, http.StatusNotFound, rec.Code)
	assert.Contains(t, rec.Body.String(), "<Resource>/bucket/datasets/gone.csv</Resource>")
	assert.NotContains(t, rec.Body.String(), tenantA)
}

// An object that happens to be XML is a payload, not a listing: it must
// stream through untouched, however large it is.
func TestObjectPayloadIsNeverRewritten(t *testing.T) {
	h, upstream := newTestProxy(t, func(c *Config) { c.Limits.RewriteBody = 16 })
	payload := `<Key>` + tenantA + `/this-is-user-data</Key>` + strings.Repeat("x", 4096)
	respondXML(upstream, payload)

	rec := do(t, h, clientRequest{
		method: http.MethodGet, target: "/bucket/datasets/document.xml",
		tenant: tenantA, level: "ro",
	})
	require.Equal(t, http.StatusOK, rec.Code)
	assert.Equal(t, payload, rec.Body.String())
}

// An upstream that compresses a listing must not turn the rewrite into a
// silent pass-through: the body would reach the client still carrying the
// tenant prefix, and a batch delete would lose its per-key denials. MinIO
// compresses these responses whenever the client's Accept-Encoding reaches
// it, which is exactly what happens through a proxy.
func TestCompressedListingIsStillStripped(t *testing.T) {
	h, upstream := newTestProxy(t)
	body := `<ListBucketResult><Contents><Key>` + tenantA + `/datasets/a.csv</Key></Contents></ListBucketResult>`
	upstream.respond = func(w http.ResponseWriter, r *http.Request) {
		// The client's header must NOT have been forwarded on this path: Go's
		// transport adds its own `gzip` and undoes it transparently, which is
		// what lets the body be rewritten. The client's value carries a token
		// the transport never emits, so seeing it here would mean the strip
		// did not happen — unconditionally, since a guard that can skip
		// itself is a guard that reads as more than it is.
		assert.NotEqual(t, "gzip, deflate, custom-from-client", r.Header.Get("Accept-Encoding"),
			"the client's Accept-Encoding must not reach the upstream on a rewriting operation")
		// Answer compressed anyway, as a stubborn store would.
		var buf bytes.Buffer
		zw := gzip.NewWriter(&buf)
		_, _ = zw.Write([]byte(body))
		require.NoError(t, zw.Close())

		w.Header().Set("Content-Type", "application/xml")
		w.Header().Set("Content-Encoding", "gzip")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write(buf.Bytes())
	}

	rec := do(t, h, clientRequest{
		method: http.MethodGet, target: "/bucket/?list-type=2&prefix=datasets/",
		tenant: tenantA, level: "ro",
		headers: http.Header{"Accept-Encoding": {"gzip, deflate, custom-from-client"}},
	})
	require.Equal(t, http.StatusOK, rec.Code)
	assert.Contains(t, rec.Body.String(), "<Key>datasets/a.csv</Key>")
	assert.NotContains(t, rec.Body.String(), tenantA)
	assert.Empty(t, rec.Header().Get("Content-Encoding"), "the body was decoded, so it must not still claim to be encoded")
}

// A coding we cannot undo must fail closed rather than forward a body the
// tenant prefix was never stripped from.
func TestUndecodableListingFailsClosed(t *testing.T) {
	h, upstream := newTestProxy(t)
	upstream.respond = func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/xml")
		w.Header().Set("Content-Encoding", "br")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("\x00\x01not-brotli-either"))
	}

	rec := do(t, h, clientRequest{
		method: http.MethodGet, target: "/bucket/?list-type=2&prefix=datasets/",
		tenant: tenantA, level: "ro",
	})
	assert.Equal(t, http.StatusBadGateway, rec.Code)
	assert.NotContains(t, rec.Body.String(), tenantA)
}

// A listing is buffered to have the tenant prefix stripped out of it, and
// that buffer is capped. Past the cap there is no safe answer: forwarding the
// body unrewritten would hand the client upstream-shaped keys, so the request
// fails closed instead. An object payload is a different case — it is never
// rewritten and never buffered, which TestObjectPayloadIsNeverRewritten
// covers.
func TestOversizedListingFailsClosed(t *testing.T) {
	h, upstream := newTestProxy(t, func(c *Config) { c.Limits.RewriteBody = 256 })

	var body strings.Builder
	body.WriteString("<ListBucketResult>")
	for i := 0; i < 200; i++ {
		fmt.Fprintf(&body, "<Contents><Key>%s/datasets/%d.csv</Key></Contents>", tenantA, i)
	}
	body.WriteString("</ListBucketResult>")
	respondXML(upstream, body.String())

	rec := do(t, h, clientRequest{
		method: http.MethodGet, target: "/bucket/?list-type=2&prefix=datasets/",
		tenant: tenantA, level: "ro",
	})
	assert.Equal(t, http.StatusBadGateway, rec.Code)
	assert.NotContains(t, rec.Body.String(), tenantA,
		"a listing that could not be rewritten must not reach the client at all")
}

// The same cap has to bound the *decompressed* size, or a small gzip body
// would expand past it unchecked — the compressed read being under the cap
// says nothing about what it expands to.
//
// Reaching that branch takes some care, and the obvious test does not. On the
// RewritesXML path the proxy deletes the client's Accept-Encoding, so Go's
// transport supplies its own and decompresses the response transparently:
// ModifyResponse then sees an already-expanded body with no Content-Encoding
// at all, and the compressed-read cap next door is what fires. The gzip
// branch is only entered when the *client* sent Accept-Encoding — which the
// proxy forwards — and that, on the rewrite path, means a non-RewritesXML
// operation answering >= 300. An error body, in other words, which is exactly
// the response that names the upstream-prefixed key.
func TestGzipErrorBodyExpandingPastTheCapFailsClosed(t *testing.T) {
	h, upstream := newTestProxy(t, func(c *Config) { c.Limits.RewriteBody = 4096 })

	// Highly repetitive, so it compresses to a few hundred bytes and expands
	// to far more than the cap.
	var plain strings.Builder
	plain.WriteString("<Error><Code>NoSuchKey</Code>")
	for i := 0; i < 2000; i++ {
		fmt.Fprintf(&plain, "<Resource>/bucket/%s/datasets/gone.csv</Resource>", tenantA)
	}
	plain.WriteString("</Error>")

	var compressed bytes.Buffer
	zw := gzip.NewWriter(&compressed)
	_, _ = zw.Write([]byte(plain.String()))
	require.NoError(t, zw.Close())
	require.Less(t, compressed.Len(), 4096, "the compressed body must be under the cap")
	require.Greater(t, plain.Len(), 4096, "and the decompressed body must be over it")

	upstream.respond = func(w http.ResponseWriter, r *http.Request) {
		// The client's own Accept-Encoding has to have reached the upstream,
		// or Go's transport would have decompressed the answer for us and
		// this would be testing the branch next door.
		//
		// The value matters as much as the assertion. When the proxy strips
		// this header the transport substitutes a bare `gzip`, so a guard
		// expecting `gzip` is satisfied by the very failure it is watching
		// for. `gzip, deflate` is a value the transport never emits, so the
		// two cases are distinguishable.
		require.Equal(t, "gzip, deflate", r.Header.Get("Accept-Encoding"),
			"the client's own Accept-Encoding must have been forwarded, not the transport's substitute")
		w.Header().Set("Content-Type", "application/xml")
		w.Header().Set("Content-Encoding", "gzip")
		w.WriteHeader(http.StatusNotFound)
		_, _ = w.Write(compressed.Bytes())
	}

	rec := do(t, h, clientRequest{
		method: http.MethodGet, target: "/bucket/datasets/gone.csv",
		tenant: tenantA, level: "ro",
		headers: http.Header{"Accept-Encoding": {"gzip, deflate"}},
	})
	// What the cap buys is a bound on the amplification: this body is ~270x
	// larger decompressed, and a hostile one is limited only by what gzip can
	// do. Without it the whole expansion is held in memory before anything
	// looks at its size. (The prefix strip itself still works on an
	// over-large body — the risk here is the allocation, not a leak — but a
	// refused response must carry nothing either way.)
	assert.Equal(t, http.StatusBadGateway, rec.Code)
	assert.NotContains(t, rec.Body.String(), tenantA)
}
