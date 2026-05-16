package main

import (
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strconv"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// Pure-function tests of stripKeyPrefixFromListBody — fast, no network.

func TestStripKeyPrefixFromListBody_DisabledWhenEmpty(t *testing.T) {
	body := []byte(`<Contents><Key>x</Key></Contents>`)
	assert.Equal(t, body, stripKeyPrefixFromListBody(body, ""))
}

func TestStripKeyPrefixFromListBody_StripsContentsKey(t *testing.T) {
	body := []byte(`<Contents><Key>tenants/acme/uploads/demo.csv</Key></Contents>`)
	got := stripKeyPrefixFromListBody(body, "tenants/acme/")
	assert.Equal(t, `<Contents><Key>uploads/demo.csv</Key></Contents>`, string(got))
}

func TestStripKeyPrefixFromListBody_StripsTopLevelPrefix(t *testing.T) {
	body := []byte(`<Prefix>tenants/acme/uploads/</Prefix>`)
	got := stripKeyPrefixFromListBody(body, "tenants/acme/")
	assert.Equal(t, `<Prefix>uploads/</Prefix>`, string(got))
}

func TestStripKeyPrefixFromListBody_StripsCommonPrefixes(t *testing.T) {
	body := []byte(`<CommonPrefixes><Prefix>tenants/acme/uploads/sub/</Prefix></CommonPrefixes>`)
	got := stripKeyPrefixFromListBody(body, "tenants/acme/")
	assert.Equal(t,
		`<CommonPrefixes><Prefix>uploads/sub/</Prefix></CommonPrefixes>`,
		string(got))
}

func TestStripKeyPrefixFromListBody_StripsMarkers(t *testing.T) {
	body := []byte(
		`<Marker>tenants/acme/a</Marker>` +
			`<NextMarker>tenants/acme/z</NextMarker>` +
			`<StartAfter>tenants/acme/m</StartAfter>` +
			`<KeyMarker>tenants/acme/p</KeyMarker>` +
			`<NextKeyMarker>tenants/acme/q</NextKeyMarker>`,
	)
	got := stripKeyPrefixFromListBody(body, "tenants/acme/")
	assert.Equal(t,
		`<Marker>a</Marker><NextMarker>z</NextMarker><StartAfter>m</StartAfter><KeyMarker>p</KeyMarker><NextKeyMarker>q</NextKeyMarker>`,
		string(got))
}

func TestStripKeyPrefixFromListBody_LeavesOpaqueTokensUntouched(t *testing.T) {
	body := []byte(
		`<ContinuationToken>tenants/acme/abc==</ContinuationToken>` +
			`<NextContinuationToken>tenants/acme/def==</NextContinuationToken>` +
			`<UploadIdMarker>tenants/acme/u1</UploadIdMarker>`,
	)
	// These elements happen to start with the prefix string by
	// coincidence — they are opaque tokens, not key paths, and must
	// pass through unchanged.
	assert.Equal(t, body, stripKeyPrefixFromListBody(body, "tenants/acme/"))
}

func TestStripKeyPrefixFromListBody_LeavesNonMatchingValuesUntouched(t *testing.T) {
	body := []byte(`<Key>other-tenant/file.csv</Key>`)
	// A Key that does NOT start with the configured prefix means the
	// upstream returned a key from another scope (shouldn't happen but
	// be defensive). Pass through verbatim — stripping nothing is
	// safer than stripping the wrong characters.
	assert.Equal(t, body, stripKeyPrefixFromListBody(body, "tenants/acme/"))
}

// CompleteMultipartUploadResult — <Location> embeds the prefixed key
// inside a fully-qualified URL ("https://bucket.s3.../tenants/acme/x.csv").
// Bare HasPrefix wouldn't strip; the URL-embedded path branch in
// stripPrefixFromValue handles it.

func TestStripKeyPrefixFromListBody_CompleteMultipartUploadResult(t *testing.T) {
	body := []byte(`<?xml version="1.0" encoding="UTF-8"?>
<CompleteMultipartUploadResult>
  <Location>https://bucket.s3.eu-central-1.amazonaws.com/tenants/acme/uploads/file.csv</Location>
  <Bucket>bucket</Bucket>
  <Key>tenants/acme/uploads/file.csv</Key>
  <ETag>"abc-1"</ETag>
</CompleteMultipartUploadResult>`)
	got := string(stripKeyPrefixFromListBody(body, "tenants/acme/"))
	// URL-embedded key in <Location> stripped, bare key in <Key>
	// stripped, <Bucket>/<ETag> untouched.
	assert.Contains(t, got,
		`<Location>https://bucket.s3.eu-central-1.amazonaws.com/uploads/file.csv</Location>`)
	assert.Contains(t, got, `<Key>uploads/file.csv</Key>`)
	assert.Contains(t, got, `<Bucket>bucket</Bucket>`)
	assert.Contains(t, got, `<ETag>"abc-1"</ETag>`)
	assert.NotContains(t, got, "tenants/acme/")
}

// Error responses — <Resource> embeds the prefixed key after the
// bucket path (`/<bucket>/<prefixed-key>`). Path-form stripping.

func TestStripKeyPrefixFromListBody_ErrorResponseResource(t *testing.T) {
	body := []byte(`<?xml version="1.0" encoding="UTF-8"?>
<Error>
  <Code>NoSuchKey</Code>
  <Message>The specified key does not exist.</Message>
  <Key>tenants/acme/uploads/missing.csv</Key>
  <Resource>/bucket/tenants/acme/uploads/missing.csv</Resource>
  <RequestId>req-1</RequestId>
</Error>`)
	got := string(stripKeyPrefixFromListBody(body, "tenants/acme/"))
	assert.Contains(t, got, `<Key>uploads/missing.csv</Key>`)
	assert.Contains(t, got, `<Resource>/bucket/uploads/missing.csv</Resource>`)
	// Non-key elements untouched.
	assert.Contains(t, got, "<Code>NoSuchKey</Code>")
	assert.Contains(t, got, "<RequestId>req-1</RequestId>")
	assert.NotContains(t, got, "tenants/acme/")
}

func TestStripKeyPrefixFromListBody_ResourceBareKeyForm(t *testing.T) {
	// Some S3-compatible stores emit <Resource> as a bare key (no
	// `/bucket/` prefix). Bare-key branch handles it.
	body := []byte(`<Error><Resource>tenants/acme/uploads/x.csv</Resource></Error>`)
	got := string(stripKeyPrefixFromListBody(body, "tenants/acme/"))
	assert.Contains(t, got, `<Resource>uploads/x.csv</Resource>`)
}

// stripPrefixFromValue unit tests — the helper that handles both
// bare-key and URL-embedded forms.

func TestStripPrefixFromValue_BareKey(t *testing.T) {
	out := stripPrefixFromValue([]byte("tenants/acme/uploads/x.csv"), []byte("tenants/acme/"))
	assert.Equal(t, "uploads/x.csv", string(out))
}

func TestStripPrefixFromValue_UrlEmbeddedKey(t *testing.T) {
	out := stripPrefixFromValue(
		[]byte("https://bucket.s3.region.amazonaws.com/tenants/acme/uploads/x.csv"),
		[]byte("tenants/acme/"),
	)
	assert.Equal(t,
		"https://bucket.s3.region.amazonaws.com/uploads/x.csv",
		string(out))
}

func TestStripPrefixFromValue_AbsolutePathEmbeddedKey(t *testing.T) {
	out := stripPrefixFromValue(
		[]byte("/bucket/tenants/acme/uploads/x.csv"),
		[]byte("tenants/acme/"),
	)
	assert.Equal(t, "/bucket/uploads/x.csv", string(out))
}

func TestStripPrefixFromValue_NoMatch(t *testing.T) {
	// A value that neither starts with the prefix nor contains
	// `/<prefix>` is returned unchanged.
	out := stripPrefixFromValue([]byte("other-tenant/x.csv"), []byte("tenants/acme/"))
	assert.Equal(t, "other-tenant/x.csv", string(out))
}

func TestStripPrefixFromValue_EmptyInputs(t *testing.T) {
	assert.Equal(t, "", string(stripPrefixFromValue(nil, []byte("tenants/acme/"))))
	assert.Equal(t, "x", string(stripPrefixFromValue([]byte("x"), nil)))
	assert.Equal(t, "x", string(stripPrefixFromValue([]byte("x"), []byte(""))))
}

func TestStripKeyPrefixFromListBody_FullListBucketResultV2(t *testing.T) {
	body := []byte(`<?xml version="1.0" encoding="UTF-8"?>
<ListBucketResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/">
  <Name>example-bucket</Name>
  <Prefix>tenants/acme/uploads/</Prefix>
  <KeyCount>2</KeyCount>
  <MaxKeys>1000</MaxKeys>
  <IsTruncated>false</IsTruncated>
  <Contents>
    <Key>tenants/acme/uploads/a.csv</Key>
    <LastModified>2026-05-16T08:00:00.000Z</LastModified>
    <ETag>"abc"</ETag>
    <Size>281</Size>
    <StorageClass>STANDARD</StorageClass>
  </Contents>
  <Contents>
    <Key>tenants/acme/uploads/b.csv</Key>
    <LastModified>2026-05-16T08:00:00.000Z</LastModified>
    <ETag>"def"</ETag>
    <Size>123</Size>
    <StorageClass>STANDARD</StorageClass>
  </Contents>
  <ContinuationToken>tenants/acme/opaque==</ContinuationToken>
</ListBucketResult>`)
	got := string(stripKeyPrefixFromListBody(body, "tenants/acme/"))
	// Keys + top-level Prefix stripped.
	assert.Contains(t, got, "<Prefix>uploads/</Prefix>")
	assert.Contains(t, got, "<Key>uploads/a.csv</Key>")
	assert.Contains(t, got, "<Key>uploads/b.csv</Key>")
	// Pagination token left as-is — opaque to the proxy.
	assert.Contains(t, got, "<ContinuationToken>tenants/acme/opaque==</ContinuationToken>")
	// Other elements untouched.
	assert.Contains(t, got, "<Name>example-bucket</Name>")
	assert.Contains(t, got, "<Size>281</Size>")
}

func TestStripKeyPrefixFromListBody_CommonPrefixesInsideListBucketResult(t *testing.T) {
	body := []byte(`<ListBucketResult>
  <Prefix>tenants/acme/</Prefix>
  <CommonPrefixes>
    <Prefix>tenants/acme/folder-a/</Prefix>
  </CommonPrefixes>
  <CommonPrefixes>
    <Prefix>tenants/acme/folder-b/</Prefix>
  </CommonPrefixes>
</ListBucketResult>`)
	got := string(stripKeyPrefixFromListBody(body, "tenants/acme/"))
	assert.Contains(t, got, "<Prefix></Prefix>")
	assert.Contains(t, got, "<Prefix>folder-a/</Prefix>")
	assert.Contains(t, got, "<Prefix>folder-b/</Prefix>")
}

// Integration test: stand up a tiny upstream that mimics the prefixed
// LIST response, point a Handler at it, fire a LIST through the proxy,
// assert the client sees stripped keys + a matching Content-Length.

func TestModifyListResponse_EndToEnd(t *testing.T) {
	const upstreamPayload = `<?xml version="1.0" encoding="UTF-8"?>
<ListBucketResult>
  <Name>bucket</Name>
  <Prefix>tenants/acme/uploads/</Prefix>
  <Contents><Key>tenants/acme/uploads/x.csv</Key><Size>10</Size></Contents>
</ListBucketResult>`

	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/xml")
		w.Header().Set("Content-Length", strconv.Itoa(len(upstreamPayload)))
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(upstreamPayload))
	}))
	defer upstream.Close()

	h := &Handler{KeyPrefix: "tenants/acme/"}

	// Hit stripKeyPrefixFromResponse directly with a synthesised
	// *http.Response (the wiring inside ServeHTTP is the standard
	// ReverseProxy pattern and is exercised by the existing
	// handler_test.go suite).
	upstreamResp, err := http.Get(upstream.URL + "/bucket/?list-type=2&prefix=uploads/")
	require.NoError(t, err)

	err = h.stripKeyPrefixFromResponse(upstreamResp)
	require.NoError(t, err)

	body, err := io.ReadAll(upstreamResp.Body)
	require.NoError(t, err)
	upstreamResp.Body.Close()

	got := string(body)
	assert.Contains(t, got, "<Prefix>uploads/</Prefix>",
		"top-level Prefix should be stripped")
	assert.Contains(t, got, "<Key>uploads/x.csv</Key>",
		"Contents.Key should be stripped")
	assert.NotContains(t, got, "tenants/acme/",
		"no occurrence of the org prefix should remain in keys")
	// Content-Length must reflect the (smaller) rewritten body.
	assert.Equal(t,
		strconv.Itoa(len(body)),
		upstreamResp.Header.Get("Content-Length"),
		"Content-Length must match rewritten body")
	assert.Equal(t,
		int64(len(body)),
		upstreamResp.ContentLength,
		"resp.ContentLength must match rewritten body")
}

func TestStripKeyPrefixFromResponse_SkipsNonXmlObjectPayloads(t *testing.T) {
	// GET / HEAD / PUT on an object — response body is the object payload
	// and is identified as non-XML by Content-Type. Must NEVER be rewritten
	// even when it contains the prefix as text.
	h := &Handler{KeyPrefix: "tenants/acme/"}
	resp := &http.Response{
		Header:  http.Header{"Content-Type": []string{"text/csv"}},
		Body:    io.NopCloser(strings.NewReader("tenants/acme/uploads/x,1\n")),
		Request: &http.Request{URL: &url.URL{Path: "/bucket/uploads/x.csv"}},
	}
	require.NoError(t, h.stripKeyPrefixFromResponse(resp))
	body, _ := io.ReadAll(resp.Body)
	assert.Equal(t, "tenants/acme/uploads/x,1\n", string(body))
}

func TestStripKeyPrefixFromResponse_RunsOnObjectLevelXmlResponses(t *testing.T) {
	// CompleteMultipartUpload is POST /bucket/key?uploadId=... — object
	// path, but the response is XML with prefixed <Location> / <Key>.
	// The previous bucket-level-only gating skipped these. Now: XML
	// content-type alone qualifies.
	h := &Handler{KeyPrefix: "tenants/acme/"}
	body := `<CompleteMultipartUploadResult>
  <Location>https://bucket.s3.region.amazonaws.com/tenants/acme/uploads/x.csv</Location>
  <Key>tenants/acme/uploads/x.csv</Key>
</CompleteMultipartUploadResult>`
	resp := &http.Response{
		Header:  http.Header{"Content-Type": []string{"application/xml"}},
		Body:    io.NopCloser(strings.NewReader(body)),
		Request: &http.Request{URL: &url.URL{Path: "/bucket/uploads/x.csv"}},
	}
	require.NoError(t, h.stripKeyPrefixFromResponse(resp))
	got, _ := io.ReadAll(resp.Body)
	assert.Contains(t, string(got),
		`<Location>https://bucket.s3.region.amazonaws.com/uploads/x.csv</Location>`)
	assert.Contains(t, string(got), `<Key>uploads/x.csv</Key>`)
	assert.NotContains(t, string(got), "tenants/acme/")
}

func TestStripKeyPrefixFromResponse_RunsOnErrorResponses(t *testing.T) {
	// 404 NoSuchKey on a GetObject — object path, XML body with
	// <Resource> embedding the prefixed path.
	h := &Handler{KeyPrefix: "tenants/acme/"}
	body := `<Error>
  <Code>NoSuchKey</Code>
  <Resource>/bucket/tenants/acme/uploads/missing.csv</Resource>
</Error>`
	resp := &http.Response{
		StatusCode: 404,
		Header:     http.Header{"Content-Type": []string{"application/xml"}},
		Body:       io.NopCloser(strings.NewReader(body)),
		Request:    &http.Request{URL: &url.URL{Path: "/bucket/uploads/missing.csv"}},
	}
	require.NoError(t, h.stripKeyPrefixFromResponse(resp))
	got, _ := io.ReadAll(resp.Body)
	assert.Contains(t, string(got), `<Resource>/bucket/uploads/missing.csv</Resource>`)
	assert.NotContains(t, string(got), "tenants/acme/")
}

func TestModifyListResponse_SkipsWhenKeyPrefixEmpty(t *testing.T) {
	h := &Handler{KeyPrefix: ""}
	body := `<Contents><Key>uploads/x.csv</Key></Contents>`
	resp := &http.Response{
		Header:  http.Header{"Content-Type": []string{"application/xml"}},
		Body:    io.NopCloser(strings.NewReader(body)),
		Request: &http.Request{URL: &url.URL{Path: "/bucket/"}},
	}
	require.NoError(t, h.stripKeyPrefixFromResponse(resp))
	got, _ := io.ReadAll(resp.Body)
	assert.Equal(t, body, string(got))
}

func TestModifyListResponse_SkipsContentEncoded(t *testing.T) {
	// Upstream that compresses the LIST response — we don't try to
	// decode/re-encode, just pass through (rare in practice; S3
	// doesn't compress LIST responses).
	h := &Handler{KeyPrefix: "tenants/acme/"}
	body := "gzipped-bytes-that-look-arbitrary"
	resp := &http.Response{
		Header: http.Header{
			"Content-Type":     []string{"application/xml"},
			"Content-Encoding": []string{"gzip"},
		},
		Body:    io.NopCloser(strings.NewReader(body)),
		Request: &http.Request{URL: &url.URL{Path: "/bucket/"}},
	}
	require.NoError(t, h.stripKeyPrefixFromResponse(resp))
	got, _ := io.ReadAll(resp.Body)
	assert.Equal(t, body, string(got))
}
