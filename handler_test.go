package main

import (
	"bytes"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go/aws/credentials"
	v4 "github.com/aws/aws-sdk-go/aws/signer/v4"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// func TestMain(m *testing.M) {
// 	log.SetOutput(ioutil.Discard)
// }

func newTestProxy(t *testing.T) *Handler {
	thf := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		fmt.Fprintln(w, "Hello, client")
	})
	return newTestProxyWithHandler(t, &thf)
}

func newTestProxyWithHandler(t *testing.T, thf *http.HandlerFunc) *Handler {
	ts := httptest.NewServer(thf)
	tsURL, _ := url.Parse(ts.URL)

	h, err := NewAwsS3ReverseProxy(Options{
		Debug:                 true,
		AllowedSourceEndpoint: "foobar.example.com",
		AllowedSourceSubnet:   []string{"0.0.0.0/0"},
		AwsCredentials:        []string{"fooooooooooooooo,bar"},
		Region:                "eu-test-1",
		UpstreamInsecure:      true,
		UpstreamEndpoint:      tsURL.Host,
	})
	assert.Nil(t, err)
	return h
}

func signRequest(r *http.Request) {
	// delete headers to get clean signature
	r.Header.Del("accept-encoding")
	r.Header.Del("authorization")
	r.Header.Set("X-Amz-Date", "20060102T150405Z")
	r.URL.RawPath = r.URL.Path

	// compute the expected signature with valid credentials
	body := bytes.NewReader([]byte{})
	signTime, _ := time.Parse("20060102T150405Z", r.Header["X-Amz-Date"][0])
	signer := v4.NewSigner(credentials.NewStaticCredentialsFromCreds(credentials.Value{
		AccessKeyID:     "fooooooooooooooo",
		SecretAccessKey: "bar",
	}))
	signer.Sign(r, body, "s3", "eu-test-1", signTime)
}

func verifySignature(w http.ResponseWriter, r *http.Request) {
	// save copy of the received signature
	receivedAuthorization := r.Header["Authorization"][0]

	// delete headers to get clean signature
	r.Header.Del("accept-encoding")
	r.Header.Del("authorization")

	// compute the expected signature with valid credentials
	body := bytes.NewReader([]byte{})
	signTime, _ := time.Parse("20060102T150405Z", r.Header["X-Amz-Date"][0])
	signer := v4.NewSigner(credentials.NewStaticCredentialsFromCreds(credentials.Value{
		AccessKeyID:     "fooooooooooooooo",
		SecretAccessKey: "bar",
	}))
	signer.Sign(r, body, "s3", "eu-test-1", signTime)
	expectedAuthorization := r.Header["Authorization"][0]

	// WORKAROUND S3CMD who dont use white space before the comma in the authorization header
	// Sanitize fakeReq to remove white spaces before the comma signature
	receivedAuthorization = strings.Replace(receivedAuthorization, ",Signature", ", Signature", 1)
	// Sanitize fakeReq to remove white spaces before the comma signheaders
	receivedAuthorization = strings.Replace(receivedAuthorization, ",SignedHeaders", ", SignedHeaders", 1)

	// verify signature
	fmt.Fprintln(w, receivedAuthorization, expectedAuthorization)
	if receivedAuthorization == expectedAuthorization {
		fmt.Fprintln(w, "ok")
	} else {
		fmt.Fprintln(w, "failed signature check")
	}
}

func TestHandlerMissingAmzDate(t *testing.T) {
	h := newTestProxy(t)

	req := httptest.NewRequest(http.MethodGet, "http://foobar.example.com", nil)
	resp := httptest.NewRecorder()
	h.ServeHTTP(resp, req)
	assert.Equal(t, 400, resp.Code)
	assert.Contains(t, resp.Body.String(), "X-Amz-Date header missing or set multiple times")
}

func TestHandlerMissingAuthorization(t *testing.T) {
	h := newTestProxy(t)

	req := httptest.NewRequest(http.MethodGet, "http://foobar.example.com", nil)
	req.Header.Set("X-Amz-Date", "20060102T150405Z")
	resp := httptest.NewRecorder()
	h.ServeHTTP(resp, req)
	assert.Equal(t, 400, resp.Code)
	assert.Contains(t, resp.Body.String(), "Authorization header missing or set multiple times")
}

func TestHandlerMissingCredential(t *testing.T) {
	h := newTestProxy(t)

	req := httptest.NewRequest(http.MethodGet, "http://foobar.example.com", nil)
	req.Header.Set("X-Amz-Date", "20060102T150405Z")
	req.Header.Set("Authorization", "foobar")
	resp := httptest.NewRecorder()
	h.ServeHTTP(resp, req)
	assert.Equal(t, 400, resp.Code)
	assert.Contains(t, resp.Body.String(), "invalid Authorization header: Credential not found")
}

func TestHandlerInvalidSignature(t *testing.T) {
	h := newTestProxy(t)

	req := httptest.NewRequest(http.MethodGet, "http://foobar.example.com", nil)
	req.Header.Set("X-Amz-Date", "20060102T150405Z")
	req.Header.Set("Authorization", "AWS4-HMAC-SHA256 Credential=fooooooooooooooo/20190101/eu-test-1/s3/aws4_request, SignedHeaders=host;x-amz-content-sha256;x-amz-date, Signature=some-signature")
	resp := httptest.NewRecorder()
	h.ServeHTTP(resp, req)
	assert.Equal(t, 400, resp.Code)
	assert.Contains(t, resp.Body.String(), "invalid signature in Authorization header")
}

func TestHandlerValidSignature(t *testing.T) {
	h := newTestProxy(t)

	req := httptest.NewRequest(http.MethodGet, "http://foobar.example.com", nil)
	signRequest(req)
	resp := httptest.NewRecorder()
	h.ServeHTTP(resp, req)
	assert.Equal(t, 200, resp.Code)
	assert.Contains(t, resp.Body.String(), "Hello, client")
}
func TestDecodeAwsChunkedUnsigned(t *testing.T) {
	// STREAMING-UNSIGNED-PAYLOAD-TRAILER: plain `<hex>\r\n<data>\r\n` per chunk,
	// terminated by `0\r\n\r\n` — exactly the framing seen baked into the corrupt
	// parquet objects (`165D\r\nPAR1…\r\n\r\n`).
	payload := bytes.Repeat([]byte("PAR1-payload-bytes-"), 500) // ~9.5 KB, multi-read
	var body bytes.Buffer
	fmt.Fprintf(&body, "%x\r\n", len(payload))
	body.Write(payload)
	body.WriteString("\r\n0\r\n\r\n")

	got, err := decodeAwsChunked(bytes.NewReader(body.Bytes()))
	require.NoError(t, err)
	require.Equal(t, payload, got)
}

func TestDecodeAwsChunkedSignedMultiChunk(t *testing.T) {
	// STREAMING-AWS4-HMAC-SHA256-PAYLOAD: each size line carries a
	// `;chunk-signature=…` extension we must ignore. Two data chunks.
	c1 := bytes.Repeat([]byte("A"), 17)
	c2 := bytes.Repeat([]byte("B"), 9)
	var body bytes.Buffer
	fmt.Fprintf(&body, "%x;chunk-signature=%064x\r\n", len(c1), 1)
	body.Write(c1)
	body.WriteString("\r\n")
	fmt.Fprintf(&body, "%x;chunk-signature=%064x\r\n", len(c2), 2)
	body.Write(c2)
	body.WriteString("\r\n")
	body.WriteString("0;chunk-signature=" + strings.Repeat("0", 64) + "\r\n\r\n")

	got, err := decodeAwsChunked(bytes.NewReader(body.Bytes()))
	require.NoError(t, err)
	require.Equal(t, append(append([]byte{}, c1...), c2...), got)
}

// The wiring: assembleUpstreamReq must hand the UPSTREAM request a DECODED body
// (and drop the streaming markers) when the incoming PUT is aws-chunked — so the
// object stored upstream is the real content, not the framed bytes.
func TestAssembleUpstreamReqDecodesChunkedBody(t *testing.T) {
	h := newTestProxy(t)
	payload := []byte("PAR1-the-real-object-content-PAR1")
	var framed bytes.Buffer
	fmt.Fprintf(&framed, "%x\r\n", len(payload))
	framed.Write(payload)
	framed.WriteString("\r\n0\r\n\r\n")

	req := httptest.NewRequest(http.MethodPut, "http://foobar.example.com/bucket/key", bytes.NewReader(framed.Bytes()))
	req.Header.Set("X-Amz-Content-Sha256", "STREAMING-UNSIGNED-PAYLOAD-TRAILER")
	req.Header.Set("Content-Encoding", "aws-chunked")
	req.Header.Set("X-Amz-Decoded-Content-Length", "33")

	var signer *v4.Signer
	for _, s := range h.Signers {
		signer = s
	}
	up, err := h.assembleUpstreamReq(signer, req, "eu-test-1")
	require.NoError(t, err)

	gotBody, _ := io.ReadAll(up.Body)
	require.Equal(t, payload, gotBody, "upstream must receive the decoded object, not the chunk-framed body")
	require.Equal(t, int64(len(payload)), up.ContentLength)
	require.Empty(t, up.Header.Get("Content-Encoding"), "aws-chunked marker must not be forwarded")
	require.Empty(t, up.Header.Get("X-Amz-Decoded-Content-Length"))
}

func TestIsAwsChunkedUpload(t *testing.T) {
	mk := func(sha, enc string) *http.Request {
		r := httptest.NewRequest(http.MethodPut, "http://h/bucket/key", nil)
		if sha != "" {
			r.Header.Set("X-Amz-Content-Sha256", sha)
		}
		if enc != "" {
			r.Header.Set("Content-Encoding", enc)
		}
		return r
	}
	require.True(t, isAwsChunkedUpload(mk("STREAMING-UNSIGNED-PAYLOAD-TRAILER", "")))
	require.True(t, isAwsChunkedUpload(mk("STREAMING-AWS4-HMAC-SHA256-PAYLOAD", "")))
	require.True(t, isAwsChunkedUpload(mk("", "aws-chunked")))
	require.False(t, isAwsChunkedUpload(mk("e3b0c442…", "")))
	require.False(t, isAwsChunkedUpload(mk("", "gzip")))
	require.False(t, isAwsChunkedUpload(mk("", "")))
}

func TestHandlerReadOnlyRejectsWrites(t *testing.T) {
	h := newTestProxy(t)
	h.ReadOnly = true

	for _, method := range []string{http.MethodPut, http.MethodPost, http.MethodDelete, http.MethodPatch} {
		req := httptest.NewRequest(method, "http://foobar.example.com/bucket/key", nil)
		signRequest(req) // a fully valid, signed write request
		resp := httptest.NewRecorder()
		h.ServeHTTP(resp, req)
		assert.Equal(t, http.StatusForbidden, resp.Code, "method %s must be rejected", method)
	}
}

func TestHandlerReadOnlyAllowsReads(t *testing.T) {
	h := newTestProxy(t)
	h.ReadOnly = true

	for _, method := range []string{http.MethodGet, http.MethodHead} {
		req := httptest.NewRequest(method, "http://foobar.example.com/bucket/key", nil)
		signRequest(req)
		resp := httptest.NewRecorder()
		h.ServeHTTP(resp, req)
		assert.Equal(t, 200, resp.Code, "method %s must be allowed", method)
	}
}

func TestHandlerWritesAllowedWhenNotReadOnly(t *testing.T) {
	h := newTestProxy(t) // ReadOnly defaults to false
	req := httptest.NewRequest(http.MethodPut, "http://foobar.example.com/bucket/key", nil)
	signRequest(req)
	resp := httptest.NewRecorder()
	h.ServeHTTP(resp, req)
	// Not a 403 — the write is proxied (the test upstream answers 200).
	assert.NotEqual(t, http.StatusForbidden, resp.Code)
}

func TestHandlerReadOnlyKeyPrefixRejectsProtectedWrites(t *testing.T) {
	h := newTestProxy(t)
	h.ReadOnlyKeyPrefixes = []string{"protected/", "locked/"}

	paths := []string{
		"http://foobar.example.com/bucket/protected/file.txt",
		"http://foobar.example.com/bucket/protected/nested/deep.txt",
		"http://foobar.example.com/bucket/locked/file.txt",
	}
	for _, path := range paths {
		for _, method := range []string{http.MethodPut, http.MethodPost, http.MethodDelete, http.MethodPatch} {
			req := httptest.NewRequest(method, path, nil)
			signRequest(req) // a fully valid, signed write request
			resp := httptest.NewRecorder()
			h.ServeHTTP(resp, req)
			assert.Equal(t, http.StatusForbidden, resp.Code, "%s %s must be rejected", method, path)
		}
	}
}

func TestHandlerReadOnlyKeyPrefixAllowsUnprotectedWrites(t *testing.T) {
	h := newTestProxy(t)
	h.ReadOnlyKeyPrefixes = []string{"protected/"}

	// Object keys outside the protected prefix, and bucket-level paths, are
	// proxied (the test upstream answers 200) — never a 403.
	paths := []string{
		"http://foobar.example.com/bucket/public/file.txt",
		"http://foobar.example.com/bucket/unprotectedfile.txt",
		"http://foobar.example.com/bucket", // bucket-level: no object key
	}
	for _, path := range paths {
		req := httptest.NewRequest(http.MethodPut, path, nil)
		signRequest(req)
		resp := httptest.NewRecorder()
		h.ServeHTTP(resp, req)
		assert.NotEqual(t, http.StatusForbidden, resp.Code, "PUT %s must be allowed", path)
	}
}

func TestHandlerReadOnlyKeyPrefixAllowsReadsOnProtectedKeys(t *testing.T) {
	h := newTestProxy(t)
	h.ReadOnlyKeyPrefixes = []string{"protected/"}

	for _, method := range []string{http.MethodGet, http.MethodHead} {
		req := httptest.NewRequest(method, "http://foobar.example.com/bucket/protected/file.txt", nil)
		signRequest(req)
		resp := httptest.NewRecorder()
		h.ServeHTTP(resp, req)
		assert.Equal(t, 200, resp.Code, "%s on a protected key must be allowed", method)
	}
}

// TestHandlerKeyPrefixAndReadOnlyKeyPrefixCombined pins down how the two flags
// interact: --read-only-key-prefix is evaluated against the CLIENT-FACING key
// (before --key-prefix is prepended), and only afterwards does --key-prefix
// rewrite the path sent upstream. This ordering is what lets operators write
// read-only prefixes in client terms while still confining writes to a fixed
// upstream prefix.
func TestHandlerKeyPrefixAndReadOnlyKeyPrefixCombined(t *testing.T) {
	var upstreamPath string
	thf := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		upstreamPath = r.URL.Path
		fmt.Fprintln(w, "Hello, client")
	})
	h := newTestProxyWithHandler(t, &thf)
	h.KeyPrefix = "tenants/acme/"
	h.ReadOnlyKeyPrefixes = []string{"protected/"}

	// A write under the protected prefix is rejected — the check matches the
	// client key "protected/secret.txt", NOT the upstream-rewritten
	// "tenants/acme/protected/secret.txt" — and never reaches upstream.
	req := httptest.NewRequest(http.MethodPut, "http://foobar.example.com/bucket/protected/secret.txt", nil)
	signRequest(req)
	resp := httptest.NewRecorder()
	upstreamPath = ""
	h.ServeHTTP(resp, req)
	assert.Equal(t, http.StatusForbidden, resp.Code, "protected write must be rejected")
	assert.Empty(t, upstreamPath, "rejected write must never reach upstream")

	// A write outside the protected prefix is allowed AND arrives upstream with
	// --key-prefix prepended: read-only check passed on the client key, then
	// the prefix was injected.
	req = httptest.NewRequest(http.MethodPut, "http://foobar.example.com/bucket/data/report.csv", nil)
	signRequest(req)
	resp = httptest.NewRecorder()
	upstreamPath = ""
	h.ServeHTTP(resp, req)
	assert.NotEqual(t, http.StatusForbidden, resp.Code, "unprotected write must be allowed")
	assert.Equal(t, "/bucket/tenants/acme/data/report.csv", upstreamPath,
		"allowed write must reach upstream with the key prefix injected")

	// Read-only prefixes are written in CLIENT terms: a client key that already
	// starts with the --key-prefix path does not match "protected/", so it is
	// NOT blocked. (Specifying "tenants/acme/protected/" would be the wrong
	// mental model.)
	req = httptest.NewRequest(http.MethodPut, "http://foobar.example.com/bucket/tenants/acme/protected/x.txt", nil)
	signRequest(req)
	resp = httptest.NewRecorder()
	h.ServeHTTP(resp, req)
	assert.NotEqual(t, http.StatusForbidden, resp.Code,
		"protection is matched against the client-facing key, not the upstream path")
}

func TestHandlerDenyKeyPrefixRejectsAllMethods(t *testing.T) {
	h := newTestProxy(t)
	h.DenyKeyPrefixes = []string{"hidden/", "secret/"}

	paths := []string{
		"http://foobar.example.com/bucket/hidden/file.txt",
		"http://foobar.example.com/bucket/hidden/nested/deep.txt",
		"http://foobar.example.com/bucket/secret/file.txt",
	}
	// Deny blocks reads AND writes — every method is rejected with 403.
	for _, path := range paths {
		for _, method := range []string{http.MethodGet, http.MethodHead, http.MethodPut, http.MethodPost, http.MethodDelete, http.MethodPatch} {
			req := httptest.NewRequest(method, path, nil)
			signRequest(req) // a fully valid, signed request
			resp := httptest.NewRecorder()
			h.ServeHTTP(resp, req)
			assert.Equal(t, http.StatusForbidden, resp.Code, "%s %s must be denied", method, path)
		}
	}
}

func TestHandlerDenyKeyPrefixAllowsOtherKeys(t *testing.T) {
	h := newTestProxy(t)
	h.DenyKeyPrefixes = []string{"hidden/"}

	// Object keys outside the denied prefix, and bucket-level paths, are
	// proxied (the test upstream answers 200) — never a 403.
	paths := []string{
		"http://foobar.example.com/bucket/public/file.txt",
		"http://foobar.example.com/bucket/hiddenfile.txt", // shares the bytes but not the "hidden/" boundary
		"http://foobar.example.com/bucket",                // bucket-level: no object key
	}
	for _, path := range paths {
		for _, method := range []string{http.MethodGet, http.MethodPut} {
			req := httptest.NewRequest(method, path, nil)
			signRequest(req)
			resp := httptest.NewRecorder()
			h.ServeHTTP(resp, req)
			assert.NotEqual(t, http.StatusForbidden, resp.Code, "%s %s must be allowed", method, path)
		}
	}
}

// TestHandlerKeyPrefixAndDenyKeyPrefixCombined pins down that --deny-key-prefix,
// like --read-only-key-prefix, is evaluated against the CLIENT-FACING key — the
// key exactly as the client sends it, before --key-prefix is prepended. The
// client never types the --key-prefix itself (the proxy adds it), so deny
// prefixes are written in client terms. A denied request never reaches
// upstream; an allowed request still gets --key-prefix injected on the way out.
func TestHandlerKeyPrefixAndDenyKeyPrefixCombined(t *testing.T) {
	var upstreamPath string
	thf := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		upstreamPath = r.URL.Path
		fmt.Fprintln(w, "Hello, client")
	})
	h := newTestProxyWithHandler(t, &thf)
	h.KeyPrefix = "tenants/acme/"
	h.DenyKeyPrefixes = []string{"hidden/"}

	// The client sends the client-facing key "hidden/secret.txt" (NOT
	// "tenants/acme/hidden/secret.txt" — the proxy would add that). It matches
	// the "hidden/" deny prefix, so it is rejected and never reaches upstream.
	req := httptest.NewRequest(http.MethodGet, "http://foobar.example.com/bucket/hidden/secret.txt", nil)
	signRequest(req)
	resp := httptest.NewRecorder()
	upstreamPath = ""
	h.ServeHTTP(resp, req)
	assert.Equal(t, http.StatusForbidden, resp.Code, "denied read must be rejected")
	assert.Empty(t, upstreamPath, "denied read must never reach upstream")

	// A request outside the denied prefix is allowed AND arrives upstream with
	// --key-prefix prepended: the deny check passed on the client key, then the
	// prefix was injected.
	req = httptest.NewRequest(http.MethodGet, "http://foobar.example.com/bucket/data/report.csv", nil)
	signRequest(req)
	resp = httptest.NewRecorder()
	upstreamPath = ""
	h.ServeHTTP(resp, req)
	assert.NotEqual(t, http.StatusForbidden, resp.Code, "unprotected read must be allowed")
	assert.Equal(t, "/bucket/tenants/acme/data/report.csv", upstreamPath,
		"allowed read must reach upstream with the key prefix injected")
}

func TestHandlerValidSignatureS3cmd(t *testing.T) {
	h := newTestProxy(t)

	req := httptest.NewRequest(http.MethodGet, "http://foobar.example.com", nil)
	signRequest(req)
	// get the generated signed authorization header in order to simulate the s3cmd syntax
	authorizationReq := req.Header.Get("Authorization")
	// simulating s3cmd syntax and remove the whites space after the comma of the Signature part
	authorizationReq = strings.Replace(authorizationReq, ", Signature", ",Signature", 1)
	// simulating s3cmd syntax and remove the whites space before the comma of the SignedHeaders part
	authorizationReq = strings.Replace(authorizationReq, ", SignedHeaders", ",SignedHeaders", 1)
	// push the edited authorization header
	req.Header.Set("Authorization", authorizationReq)
	resp := httptest.NewRecorder()
	h.ServeHTTP(resp, req)
	assert.Equal(t, 200, resp.Code)
	assert.Contains(t, resp.Body.String(), "Hello, client")
}

func TestHandlerInvalidCredential(t *testing.T) {
	h := newTestProxy(t)

	req := httptest.NewRequest(http.MethodGet, "http://foobar.example.com", nil)
	req.Header.Set("X-Amz-Date", "20060102T150405Z")
	req.Header.Set("Authorization", "AWS4-HMAC-SHA256 Credential=XXXooooooooooooo/20060102/eu-test-1/s3/aws4_request, SignedHeaders=host;x-amz-content-sha256;x-amz-date, Signature=a0d5e0c0924c1f9298c5f2a3925e202657bf1e239a1d6856235cbe0702855334") // signature computed manually for this test case
	resp := httptest.NewRecorder()
	h.ServeHTTP(resp, req)
	assert.Equal(t, 400, resp.Code)
	assert.Contains(t, resp.Body.String(), "invalid AccessKeyID in Credential")
}

func TestHandlerInvalidSourceSubnet(t *testing.T) {
	h := newTestProxy(t)
	_, newNet, _ := net.ParseCIDR("172.27.42.0/24")
	h.AllowedSourceSubnet = []*net.IPNet{newNet}

	req := httptest.NewRequest(http.MethodGet, "http://foobar.example.com", nil)
	req.Header.Set("X-Amz-Date", "20060102T150405Z")
	req.Header.Set("Authorization", "AWS4-HMAC-SHA256 Credential=XXXooooooooooooo/20060102/eu-test-1/s3/aws4_request, SignedHeaders=host;x-amz-content-sha256;x-amz-date, Signature=a0d5e0c0924c1f9298c5f2a3925e202657bf1e239a1d6856235cbe0702855334") // signature computed manually for this test case
	resp := httptest.NewRecorder()
	h.ServeHTTP(resp, req)
	assert.Equal(t, 400, resp.Code)
	assert.Contains(t, resp.Body.String(), "source IP not allowed")
}

func TestHandlerInvalidAmzDate(t *testing.T) {
	h := newTestProxy(t)

	req := httptest.NewRequest(http.MethodGet, "http://foobar.example.com", nil)
	req.Header.Set("X-Amz-Date", "foobar")
	req.Header.Set("Authorization", "AWS4-HMAC-SHA256 Credential=fooooooooooooooo/20060102/eu-test-1/s3/aws4_request, SignedHeaders=host;x-amz-content-sha256;x-amz-date, Signature=a0d5e0c0924c1f9298c5f2a3925e202657bf1e239a1d6856235cbe0702855334") // signature computed manually for this test case
	resp := httptest.NewRecorder()
	h.ServeHTTP(resp, req)
	assert.Equal(t, 400, resp.Code)
	assert.Contains(t, resp.Body.String(), "error parsing X-Amz-Date foobar")
}

func TestHandlerRawPathEncodingMatchingSignature(t *testing.T) {
	thf := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		verifySignature(w, r)
	})
	h := newTestProxyWithHandler(t, &thf)

	urls := []string{
		"http://foobar.example.com/foo%3Dbar/test.txt",
		"http://foobar.example.com/foo=bar/test.txt",
		"http://foobar.example.com/foo%3Dbar/test.txt?marker=1000",
		"http://foobar.example.com/foo=bar/test.txt?marker=1000",
	}

	for _, url := range urls {
		req := httptest.NewRequest(http.MethodGet, url, nil)
		signRequest(req)
		resp := httptest.NewRecorder()
		h.ServeHTTP(resp, req)
		assert.Equal(t, 200, resp.Code)
		assert.Contains(t, strings.TrimSpace(resp.Body.String()), "ok")
	}
}

func TestHandlerWithQueryArgs(t *testing.T) {
	thf := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		verifySignature(w, r)
		if r.URL.Query().Get("marker") == "1000" {
			fmt.Fprintln(w, "marker-ok")
		} else {
			fmt.Fprintln(w, "marker missing")
		}
	})
	h := newTestProxyWithHandler(t, &thf)

	urls := []string{
		"http://foobar.example.com/foo%3Dbar/test.txt?marker=1000",
		"http://foobar.example.com/foo=bar/test.txt?marker=1000",
	}

	for _, url := range urls {
		req := httptest.NewRequest(http.MethodGet, url, nil)
		signRequest(req)
		resp := httptest.NewRecorder()
		h.ServeHTTP(resp, req)
		assert.Equal(t, 200, resp.Code)
		assert.Contains(t, strings.TrimSpace(resp.Body.String()), "marker-ok")
	}
}

func TestHandlerPassCustomHeaders(t *testing.T) {
	thf := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("x-aws-s3-reverse-proxy") == "testing" {
			fmt.Fprintln(w, "ok")
		} else {
			fmt.Fprintln(w, "header missing")
		}
	})
	h := newTestProxyWithHandler(t, &thf)

	req := httptest.NewRequest(http.MethodGet, "http://foobar.example.com", nil)
	signRequest(req)
	req.Header.Set("x-aws-s3-reverse-proxy", "testing")
	resp := httptest.NewRecorder()
	h.ServeHTTP(resp, req)
	assert.Equal(t, 200, resp.Code)
	assert.Contains(t, strings.TrimSpace(resp.Body.String()), "ok")
}
