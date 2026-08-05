package main

import (
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
	h, upstream := newTestProxy(t)
	h.MaxRewriteBodySize = 16
	payload := `<Key>` + tenantA + `/this-is-user-data</Key>` + strings.Repeat("x", 4096)
	respondXML(upstream, payload)

	rec := do(t, h, clientRequest{
		method: http.MethodGet, target: "/bucket/datasets/document.xml",
		tenant: tenantA, level: "ro",
	})
	require.Equal(t, http.StatusOK, rec.Code)
	assert.Equal(t, payload, rec.Body.String())
}

func TestStripPrefixFromValue(t *testing.T) {
	prefix := []byte("acme/")
	cases := []struct{ in, want string }{
		{"acme/a.csv", "a.csv"},
		{"acme%2Fa.csv", "a.csv"},
		{"/bucket/acme/a.csv", "/bucket/a.csv"},
		{"https://host/bucket/acme/a.csv", "https://host/bucket/a.csv"},
		// No occurrence: never remove "the wrong" bytes.
		{"other/a.csv", "other/a.csv"},
		{"", ""},
	}
	for _, tc := range cases {
		assert.Equal(t, tc.want, string(stripPrefixFromValue([]byte(tc.in), prefix)), tc.in)
	}
}
