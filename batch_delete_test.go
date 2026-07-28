package main

import (
	"crypto/md5"
	"encoding/base64"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// A DeleteObjects (batch delete) request keeps its object keys in the XML body,
// so none of the path-based rules see them. These tests pin down that the body
// is rewritten and checked just like a single-object path would be.

func deleteBody(keys ...string) string {
	var b strings.Builder
	b.WriteString(`<?xml version="1.0" encoding="UTF-8"?>` +
		`<Delete xmlns="http://s3.amazonaws.com/doc/2006-03-01/">`)
	for _, k := range keys {
		fmt.Fprintf(&b, `<Object><Key>%s</Key></Object>`, k)
	}
	b.WriteString(`<Quiet>false</Quiet></Delete>`)
	return b.String()
}

func TestIsDeleteObjectsRequest(t *testing.T) {
	mk := func(method, rawurl string) *http.Request {
		return httptest.NewRequest(method, rawurl, nil)
	}
	assert.True(t, isDeleteObjectsRequest(mk(http.MethodPost, "http://h/bucket?delete")))
	// The AWS SDK with forcePathStyle:true addresses the bucket with a
	// trailing slash — still a batch delete.
	assert.True(t, isDeleteObjectsRequest(mk(http.MethodPost, "http://h/bucket/?delete=")))
	assert.False(t, isDeleteObjectsRequest(mk(http.MethodPost, "http://h/bucket?uploads")))
	assert.False(t, isDeleteObjectsRequest(mk(http.MethodGet, "http://h/bucket?delete")))
	// `?delete` on an object path is not the batch operation — that key IS in
	// the path and the ordinary path rules already cover it.
	assert.False(t, isDeleteObjectsRequest(mk(http.MethodPost, "http://h/bucket/key?delete")))
}

func TestDeleteObjectsKeys(t *testing.T) {
	keys, err := deleteObjectsKeys([]byte(deleteBody("a.csv", "nested/b.parquet")))
	require.NoError(t, err)
	assert.Equal(t, []string{"a.csv", "nested/b.parquet"}, keys)

	// Keys are XML-escaped on the wire; matching must happen on the real key.
	keys, err = deleteObjectsKeys([]byte(deleteBody("a&amp;b/secret.csv")))
	require.NoError(t, err)
	assert.Equal(t, []string{"a&b/secret.csv"}, keys)
}

func TestDeleteObjectsKeysRejectsBodyWeCannotFullyRewrite(t *testing.T) {
	// Garbage in, error out — never a silently forwarded body.
	_, err := deleteObjectsKeys([]byte("not xml at all"))
	assert.Error(t, err)

	// An <Object> without a <Key> makes the parsed view and the regexp view
	// disagree: refuse rather than forward a body we did not fully understand.
	_, err = deleteObjectsKeys([]byte(`<Delete><Object><VersionId>1</VersionId></Object></Delete>`))
	assert.Error(t, err)
}

func TestPrefixDeleteObjectsKeys(t *testing.T) {
	got := string(prefixDeleteObjectsKeys([]byte(deleteBody("a.csv", "nested/b.parquet")), "tenants/acme/"))
	assert.Contains(t, got, `<Key>tenants/acme/a.csv</Key>`)
	assert.Contains(t, got, `<Key>tenants/acme/nested/b.parquet</Key>`)
	// Everything that is not a key is forwarded verbatim.
	assert.Contains(t, got, `<Quiet>false</Quiet>`)
	assert.Contains(t, got, `xmlns="http://s3.amazonaws.com/doc/2006-03-01/"`)

	// A prefix with XML-special characters is escaped, not spliced in raw.
	got = string(prefixDeleteObjectsKeys([]byte(`<Delete><Object><Key>x</Key></Object></Delete>`), "a&b/"))
	assert.Contains(t, got, `<Key>a&amp;b/x</Key>`)
}

func TestRewriteDeleteObjectsBodyTransparentWithoutFeatures(t *testing.T) {
	// A plain re-signing proxy must stay byte-for-byte transparent — including
	// for bodies this proxy would otherwise refuse to parse.
	h := &Handler{}
	for _, body := range []string{deleteBody("a.csv"), "not xml at all"} {
		got, err := h.rewriteDeleteObjectsBody([]byte(body))
		require.NoError(t, err)
		assert.Equal(t, body, string(got))
	}
}

func TestRewriteDeleteObjectsBodyInjectsKeyPrefix(t *testing.T) {
	h := &Handler{KeyPrefix: "tenants/acme/"}
	got, err := h.rewriteDeleteObjectsBody([]byte(deleteBody("a.csv", "b.csv")))
	require.NoError(t, err)
	assert.Contains(t, string(got), `<Key>tenants/acme/a.csv</Key>`)
	assert.Contains(t, string(got), `<Key>tenants/acme/b.csv</Key>`)
}

func TestRewriteDeleteObjectsBodyEnforcesPrefixes(t *testing.T) {
	h := &Handler{
		KeyPrefix:           "tenants/acme/",
		DenyKeyPrefixes:     []string{"hidden/"},
		ReadOnlyKeyPrefixes: []string{"protected/"},
	}

	// One denied key rejects the WHOLE batch: a partial delete would report
	// success for keys that were never forwarded.
	_, err := h.rewriteDeleteObjectsBody([]byte(deleteBody("public/a.csv", "hidden/secret.csv")))
	require.Error(t, err)
	assert.True(t, errors.Is(err, errDeniedKeyPrefix), "want a deny violation, got %v", err)

	_, err = h.rewriteDeleteObjectsBody([]byte(deleteBody("public/a.csv", "protected/x.csv")))
	require.Error(t, err)
	assert.True(t, errors.Is(err, errReadOnlyKeyPrefix), "want a read-only violation, got %v", err)

	// Keys that share the bytes but not the prefix boundary are unaffected.
	got, err := h.rewriteDeleteObjectsBody([]byte(deleteBody("hiddenfile.csv")))
	require.NoError(t, err)
	assert.Contains(t, string(got), `<Key>tenants/acme/hiddenfile.csv</Key>`)

	// A body we cannot fully parse is refused once a key-scoped feature is on:
	// forwarding it would bypass both the prefix injection and the checks.
	_, err = h.rewriteDeleteObjectsBody([]byte("not xml at all"))
	assert.Error(t, err)
}

// captureUpstream returns a Handler whose upstream records the request it
// receives, plus a pointer to that record.
func captureUpstream(t *testing.T) (*Handler, *http.Request, *[]byte) {
	var seen http.Request
	var seenBody []byte
	thf := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		seenBody, _ = io.ReadAll(r.Body)
		seen = *r
		fmt.Fprintln(w, "Hello, client")
	})
	return newTestProxyWithHandler(t, &thf), &seen, &seenBody
}

func signedDeleteRequest(body string) *http.Request {
	req := httptest.NewRequest(http.MethodPost, "http://foobar.example.com/bucket?delete", strings.NewReader(body))
	sum := md5.Sum([]byte(body))
	req.Header.Set("Content-Md5", base64.StdEncoding.EncodeToString(sum[:]))
	signRequest(req)
	// signRequest signs with an empty body, and the AWS signer attaches the
	// body it signed to the request — put the real one back.
	req.Body = io.NopCloser(strings.NewReader(body))
	req.ContentLength = int64(len(body))
	return req
}

// The regression this whole feature is about: with --key-prefix set, a batch
// delete used to travel upstream with the raw client keys. S3 answers a delete
// of a non-existent key with success, so the client saw every key "deleted"
// while the prefixed objects were still there.
func TestHandlerBatchDeleteInjectsKeyPrefixIntoBody(t *testing.T) {
	h, upstream, upstreamBody := captureUpstream(t)
	h.KeyPrefix = "tenants/acme/"

	resp := httptest.NewRecorder()
	h.ServeHTTP(resp, signedDeleteRequest(deleteBody("a.csv", "nested/b.parquet")))
	require.Equal(t, 200, resp.Code)

	got := string(*upstreamBody)
	assert.Contains(t, got, `<Key>tenants/acme/a.csv</Key>`)
	assert.Contains(t, got, `<Key>tenants/acme/nested/b.parquet</Key>`)
	assert.NotContains(t, got, `<Key>a.csv</Key>`)

	// The keys are in the body, so the request path stays bucket-level and no
	// `prefix=` query parameter is invented for it.
	assert.Equal(t, "/bucket", upstream.URL.Path)
	assert.Empty(t, upstream.URL.Query().Get("prefix"))
	require.Contains(t, upstream.URL.Query(), "delete")

	// The body grew: Content-Length and Content-MD5 must describe what we
	// actually send, or the upstream answers 400.
	assert.Equal(t, int64(len(got)), upstream.ContentLength)
	sum := md5.Sum(*upstreamBody)
	assert.Equal(t, base64.StdEncoding.EncodeToString(sum[:]), upstream.Header.Get("Content-Md5"))
	// …and the recomputed digest is covered by the upstream signature, not
	// smuggled past the signer afterwards.
	assert.Contains(t, upstream.Header.Get("Authorization"), "content-md5")
}

func TestHandlerBatchDeleteDropsStaleClientChecksums(t *testing.T) {
	h, upstream, _ := captureUpstream(t)
	h.KeyPrefix = "tenants/acme/"

	req := signedDeleteRequest(deleteBody("a.csv"))
	// Modern AWS SDKs send a flexible checksum instead of / next to the MD5.
	// It was computed over the client's body and is wrong for ours.
	req.Header.Set("X-Amz-Checksum-Crc32", "AAAAAA==")
	req.Header.Set("X-Amz-Sdk-Checksum-Algorithm", "CRC32")
	resp := httptest.NewRecorder()
	h.ServeHTTP(resp, req)
	require.Equal(t, 200, resp.Code)

	assert.Empty(t, upstream.Header.Get("X-Amz-Checksum-Crc32"), "stale checksum must not be forwarded")
	assert.Empty(t, upstream.Header.Get("X-Amz-Sdk-Checksum-Algorithm"))
	// The payload hash must be the signer's, computed over the rewritten body.
	assert.NotEmpty(t, upstream.Header.Get("X-Amz-Content-Sha256"))
}

func TestHandlerBatchDeleteIsTransparentWithoutKeyPrefix(t *testing.T) {
	h, upstream, upstreamBody := captureUpstream(t)

	body := deleteBody("a.csv")
	req := signedDeleteRequest(body)
	req.Header.Set("X-Amz-Checksum-Crc32", "AAAAAA==")
	resp := httptest.NewRecorder()
	h.ServeHTTP(resp, req)
	require.Equal(t, 200, resp.Code)
	assert.Equal(t, body, string(*upstreamBody))
	assert.Equal(t, int64(len(body)), upstream.ContentLength)
	// Nothing was rewritten, so the client's own digests still describe the
	// body and are forwarded as they always were.
	assert.Equal(t, "AAAAAA==", upstream.Header.Get("X-Amz-Checksum-Crc32"))
}

func TestHandlerBatchDeleteRejectedInReadOnlyMode(t *testing.T) {
	h, _, upstreamBody := captureUpstream(t)
	h.ReadOnly = true

	resp := httptest.NewRecorder()
	h.ServeHTTP(resp, signedDeleteRequest(deleteBody("a.csv")))
	assert.Equal(t, http.StatusForbidden, resp.Code)
	assert.Nil(t, *upstreamBody, "a batch delete must never reach upstream in read-only mode")
}

func TestHandlerBatchDeleteRejectsDeniedKeys(t *testing.T) {
	h, _, upstreamBody := captureUpstream(t)
	h.DenyKeyPrefixes = []string{"hidden/"}

	resp := httptest.NewRecorder()
	h.ServeHTTP(resp, signedDeleteRequest(deleteBody("public/a.csv", "hidden/secret.csv")))
	assert.Equal(t, http.StatusForbidden, resp.Code)
	assert.Nil(t, *upstreamBody, "a denied batch delete must never reach upstream")
}

func TestHandlerBatchDeleteRejectsReadOnlyKeys(t *testing.T) {
	h, _, upstreamBody := captureUpstream(t)
	h.ReadOnlyKeyPrefixes = []string{"protected/"}

	resp := httptest.NewRecorder()
	h.ServeHTTP(resp, signedDeleteRequest(deleteBody("protected/x.csv")))
	assert.Equal(t, http.StatusForbidden, resp.Code)
	assert.Nil(t, *upstreamBody, "a protected batch delete must never reach upstream")
}

func TestHandlerBatchDeleteAllowsOtherKeys(t *testing.T) {
	h, _, upstreamBody := captureUpstream(t)
	h.DenyKeyPrefixes = []string{"hidden/"}
	h.ReadOnlyKeyPrefixes = []string{"protected/"}

	resp := httptest.NewRecorder()
	h.ServeHTTP(resp, signedDeleteRequest(deleteBody("public/a.csv", "hiddenfile.csv")))
	assert.Equal(t, 200, resp.Code)
	assert.Contains(t, string(*upstreamBody), `<Key>public/a.csv</Key>`)
}

// The response side of a batch delete: DeleteResult echoes every key back, in
// upstream (prefixed) form. modifyResponse already strips <Key>, so the client
// sees the keys it sent — a client comparing them would otherwise conclude that
// nothing it asked for was deleted.
func TestModifyResponseStripsPrefixFromDeleteResult(t *testing.T) {
	h := &Handler{KeyPrefix: "tenants/acme/"}
	body := `<?xml version="1.0" encoding="UTF-8"?>
<DeleteResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/">
  <Deleted><Key>tenants/acme/a.csv</Key></Deleted>
  <Error><Key>tenants/acme/b.csv</Key><Code>AccessDenied</Code></Error>
</DeleteResult>`
	resp := &http.Response{
		Header:  http.Header{"Content-Type": []string{"application/xml"}},
		Body:    io.NopCloser(strings.NewReader(body)),
		Request: &http.Request{URL: &url.URL{Path: "/bucket"}},
	}
	require.NoError(t, h.modifyResponse(resp))
	got, _ := io.ReadAll(resp.Body)
	assert.Contains(t, string(got), `<Key>a.csv</Key>`)
	assert.Contains(t, string(got), `<Key>b.csv</Key>`)
	assert.NotContains(t, string(got), "tenants/acme/")
	assert.Contains(t, string(got), `<Code>AccessDenied</Code>`)
}
