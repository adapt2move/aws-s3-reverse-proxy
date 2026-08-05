package main

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go/aws/credentials"
	v4 "github.com/aws/aws-sdk-go/aws/signer/v4"
	log "github.com/sirupsen/logrus"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// testPolicyYAML is the policy from the design, verbatim: a writable
// dataset area, a read-only carve-out nested *inside* a writable workspace
// tree, and the broader workspace rule after it. The ordering is the point
// — see TestPolicyNestedCarveOut.
const testPolicyYAML = `
identity:
  accessKeyIdPattern: '^(?P<tenant>[0-9a-f]{32})(?P<level>ro|rw|rws)$'
  secretTemplate: '{tenant}:{level}'
  keyPrefixTemplate: '{tenant}/'

levels: [ro, rw, rws]

rules:
  - pathPattern: 'datasets/**'
    grant: { ro: read, rw: full, rws: full }

  - pathPattern: 'workspaces/*/inbox/**'
    grant: { ro: read, rw: read, rws: read }

  - pathPattern: 'workspaces/**'
    grant: { ro: full, rw: full, rws: full }
`

const (
	tenantA = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
	tenantB = "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"

	testEndpoint = "s3.proxy.example.com:8099"
	testRegion   = "eu-central-1"
)

var testPepper = []byte("a-deployment-wide-pepper-value")

func init() {
	// The access log is noise in test output; failures are asserted on, not
	// read.
	log.SetLevel(log.PanicLevel)
}

func mustPolicy(t testing.TB) *Policy {
	t.Helper()
	policy, err := ParsePolicy([]byte(testPolicyYAML))
	require.NoError(t, err)
	return policy
}

// fakeUpstream stands in for the object store. It records what the proxy
// actually forwarded — the assertions about prefix injection and re-signing
// are all made against these recordings.
type fakeUpstream struct {
	mu       sync.Mutex
	requests []recordedRequest
	respond  func(w http.ResponseWriter, r *http.Request)
	server   *httptest.Server
}

type recordedRequest struct {
	method string
	path   string
	query  url.Values
	header http.Header
	body   string
}

func newFakeUpstream(t *testing.T) *fakeUpstream {
	u := &fakeUpstream{}
	u.server = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		u.mu.Lock()
		u.requests = append(u.requests, recordedRequest{
			method: r.Method,
			path:   r.URL.Path,
			query:  r.URL.Query(),
			header: r.Header.Clone(),
			body:   string(body),
		})
		respond := u.respond
		u.mu.Unlock()
		if respond != nil {
			respond(w, r)
			return
		}
		w.WriteHeader(http.StatusOK)
	}))
	t.Cleanup(u.server.Close)
	return u
}

func (u *fakeUpstream) last(t *testing.T) recordedRequest {
	t.Helper()
	u.mu.Lock()
	defer u.mu.Unlock()
	require.NotEmpty(t, u.requests, "the proxy did not forward anything upstream")
	return u.requests[len(u.requests)-1]
}

func (u *fakeUpstream) count() int {
	u.mu.Lock()
	defer u.mu.Unlock()
	return len(u.requests)
}

// newTestProxy builds a handler wired to a fake upstream, with the same
// defaults main() applies.
func newTestProxy(t *testing.T) (*Handler, *fakeUpstream) {
	t.Helper()
	upstream := newFakeUpstream(t)
	h := NewHandler(&Handler{
		UpstreamScheme:        "http",
		UpstreamEndpoint:      strings.TrimPrefix(upstream.server.URL, "http://"),
		AllowedSourceEndpoint: testEndpoint,
		AllowedSourceSubnet:   testSubnets(t, "0.0.0.0/0"),
		Policy:                NewStaticPolicyStore(mustPolicy(t)),
		Pepper:                testPepper,
		UpstreamSigner: v4.NewSigner(credentials.NewStaticCredentialsFromCreds(credentials.Value{
			AccessKeyID:     "UPSTREAMKEYID",
			SecretAccessKey: "upstream-secret",
		})),
		UpstreamRegion:     testRegion,
		MaxClockSkew:       15 * time.Minute,
		MaxChunkedBodySize: 1 << 20,
		MaxDeleteBodySize:  1 << 20,
		MaxRewriteBodySize: 1 << 20,
	})
	return h, upstream
}

func accessKeyFor(tenant, level string) string { return tenant + level }

func secretFor(tenant, level string) string {
	return deriveSecret(testPepper, tenant+":"+level)
}

// clientRequest is a request as a real S3 client would send it: signed with
// SigV4 over the derived secret, addressed to the proxy's endpoint.
type clientRequest struct {
	method   string
	target   string
	body     []byte
	tenant   string
	level    string
	headers  http.Header
	signTime time.Time

	// accessKeyID overrides the id derived from tenant+level, for the
	// forged-credential cases.
	accessKeyID string
	// secret overrides the derived secret, for the forged-signature cases.
	secret string
	// unsigned skips signing entirely (anonymous request).
	unsigned bool
}

func (c clientRequest) build(t *testing.T) *http.Request {
	t.Helper()
	var body io.Reader
	if c.body != nil {
		body = bytes.NewReader(c.body)
	}
	req := httptest.NewRequest(c.method, c.target, body)
	req.Host = testEndpoint
	req.RemoteAddr = "10.1.2.3:54321"
	for name, values := range c.headers {
		req.Header[http.CanonicalHeaderKey(name)] = values
	}
	if c.unsigned {
		return req
	}

	if req.Header.Get("X-Amz-Content-Sha256") == "" {
		sum := sha256.Sum256(c.body)
		req.Header.Set("X-Amz-Content-Sha256", hex.EncodeToString(sum[:]))
	}
	accessKeyID := c.accessKeyID
	if accessKeyID == "" {
		accessKeyID = accessKeyFor(c.tenant, c.level)
	}
	secret := c.secret
	if secret == "" {
		secret = secretFor(c.tenant, c.level)
	}
	signTime := c.signTime
	if signTime.IsZero() {
		signTime = time.Now()
	}
	signer := v4.NewSigner(credentials.NewStaticCredentialsFromCreds(credentials.Value{
		AccessKeyID:     accessKeyID,
		SecretAccessKey: secret,
	}))
	// The AWS SDKs sign S3 requests without escaping the canonical URI a
	// second time; a test client that did otherwise would not exercise what
	// real clients send.
	signer.DisableURIPathEscaping = true
	_, err := signer.Sign(req, bytes.NewReader(c.body), "s3", testRegion, signTime)
	require.NoError(t, err)
	return req
}

func do(t *testing.T, h *Handler, c clientRequest) *httptest.ResponseRecorder {
	t.Helper()
	return doRequest(t, h, c.build(t))
}

func doRequest(t *testing.T, h *Handler, req *http.Request) *httptest.ResponseRecorder {
	t.Helper()
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	return rec
}

func TestReadWriteDeleteRoundTrip(t *testing.T) {
	h, upstream := newTestProxy(t)

	t.Run("get injects the tenant prefix", func(t *testing.T) {
		rec := do(t, h, clientRequest{
			method: http.MethodGet, target: "/bucket/datasets/2026/a.parquet",
			tenant: tenantA, level: "ro",
		})
		assert.Equal(t, http.StatusOK, rec.Code)
		assert.Equal(t, "/bucket/"+tenantA+"/datasets/2026/a.parquet", upstream.last(t).path)
	})

	t.Run("put with a real payload hash is forwarded verbatim", func(t *testing.T) {
		payload := []byte("column,value\n1,2\n")
		rec := do(t, h, clientRequest{
			method: http.MethodPut, target: "/bucket/datasets/2026/b.csv",
			body: payload, tenant: tenantA, level: "rw",
		})
		assert.Equal(t, http.StatusOK, rec.Code)

		got := upstream.last(t)
		assert.Equal(t, "/bucket/"+tenantA+"/datasets/2026/b.csv", got.path)
		assert.Equal(t, string(payload), got.body)

		// The client's own digest describes the bytes we forwarded, so it
		// travels on untouched — no buffering, integrity preserved.
		sum := sha256.Sum256(payload)
		assert.Equal(t, hex.EncodeToString(sum[:]), got.header.Get("X-Amz-Content-Sha256"))
		// ... and the upstream signature is the proxy's, not the client's.
		assert.Contains(t, got.header.Get("Authorization"), "Credential=UPSTREAMKEYID/")
	})

	t.Run("delete", func(t *testing.T) {
		rec := do(t, h, clientRequest{
			method: http.MethodDelete, target: "/bucket/datasets/2026/b.csv",
			tenant: tenantA, level: "rw",
		})
		assert.Equal(t, http.StatusOK, rec.Code)
		assert.Equal(t, http.MethodDelete, upstream.last(t).method)
	})

	t.Run("a read-only level cannot write", func(t *testing.T) {
		before := upstream.count()
		rec := do(t, h, clientRequest{
			method: http.MethodPut, target: "/bucket/datasets/2026/c.csv",
			body: []byte("x"), tenant: tenantA, level: "ro",
		})
		assert.Equal(t, http.StatusForbidden, rec.Code)
		assert.Contains(t, rec.Body.String(), "AccessDenied")
		assert.Equal(t, before, upstream.count(), "the request must not reach the upstream")
	})
}

func TestMultipartRequiresFullPermission(t *testing.T) {
	h, upstream := newTestProxy(t)

	steps := []struct {
		name   string
		method string
		target string
		body   []byte
	}{
		{"initiate", http.MethodPost, "/bucket/datasets/big.parquet?uploads", nil},
		{"upload part", http.MethodPut, "/bucket/datasets/big.parquet?partNumber=1&uploadId=abc", []byte("part")},
		{"complete", http.MethodPost, "/bucket/datasets/big.parquet?uploadId=abc", []byte("<CompleteMultipartUpload/>")},
		{"abort", http.MethodDelete, "/bucket/datasets/big.parquet?uploadId=abc", nil},
		{"list parts", http.MethodGet, "/bucket/datasets/big.parquet?uploadId=abc", nil},
	}

	for _, step := range steps {
		t.Run(step.name+" is allowed for a full level", func(t *testing.T) {
			rec := do(t, h, clientRequest{
				method: step.method, target: step.target, body: step.body,
				tenant: tenantA, level: "rw",
			})
			assert.Equal(t, http.StatusOK, rec.Code)
			assert.Equal(t, "/bucket/"+tenantA+"/datasets/big.parquet", upstream.last(t).path)
		})
	}

	for _, step := range steps {
		if step.method == http.MethodGet {
			continue // ListParts is a read, and `read` grants it
		}
		t.Run(step.name+" is refused for a read-only level", func(t *testing.T) {
			before := upstream.count()
			rec := do(t, h, clientRequest{
				method: step.method, target: step.target, body: step.body,
				tenant: tenantA, level: "ro",
			})
			assert.Equal(t, http.StatusForbidden, rec.Code)
			assert.Equal(t, before, upstream.count())
		})
	}
}

// Every shape the proxy does not implement has to fail closed, whether or
// not the caller holds a perfectly valid credential.
func TestUnsupportedOperationsAreRefused(t *testing.T) {
	h, upstream := newTestProxy(t)

	cases := []struct {
		name    string
		method  string
		target  string
		headers http.Header
	}{
		{"copy source", http.MethodPut, "/bucket/workspaces/a/copy.txt",
			http.Header{"X-Amz-Copy-Source": {"/bucket/workspaces/a/orig.txt"}}},
		{"bucket acl", http.MethodGet, "/bucket?acl", nil},
		{"bucket policy", http.MethodPut, "/bucket?policy", nil},
		{"bucket versioning", http.MethodGet, "/bucket?versioning", nil},
		{"bucket lifecycle", http.MethodGet, "/bucket?lifecycle", nil},
		{"bucket tagging", http.MethodGet, "/bucket?tagging", nil},
		{"bucket create", http.MethodPut, "/bucket", nil},
		{"bucket delete", http.MethodDelete, "/bucket", nil},
		{"head bucket", http.MethodHead, "/bucket", nil},
		{"bucket location", http.MethodGet, "/bucket?location", nil},
		{"list multipart uploads", http.MethodGet, "/bucket?uploads", nil},
		{"list object versions", http.MethodGet, "/bucket?versions", nil},
		{"object acl", http.MethodGet, "/bucket/workspaces/a/x.txt?acl", nil},
		{"object tagging", http.MethodPut, "/bucket/workspaces/a/x.txt?tagging", nil},
		{"object restore", http.MethodPost, "/bucket/workspaces/a/x.txt?restore", nil},
		{"select object content", http.MethodPost, "/bucket/workspaces/a/x.txt?select&select-type=2", nil},
		{"unsupported method", http.MethodPatch, "/bucket/workspaces/a/x.txt", nil},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			before := upstream.count()
			rec := do(t, h, clientRequest{
				method: tc.method, target: tc.target, headers: tc.headers,
				tenant: tenantA, level: "rws",
			})
			assert.Equal(t, http.StatusForbidden, rec.Code)
			assert.Equal(t, before, upstream.count(), "the request must not reach the upstream")
		})
	}
}

func TestAuthenticationFailures(t *testing.T) {
	h, upstream := newTestProxy(t)
	target := "/bucket/datasets/a.csv"

	t.Run("anonymous", func(t *testing.T) {
		before := upstream.count()
		rec := do(t, h, clientRequest{method: http.MethodGet, target: target, unsigned: true})
		assert.Equal(t, http.StatusForbidden, rec.Code)
		assert.Equal(t, before, upstream.count())
	})

	t.Run("unknown access key id", func(t *testing.T) {
		before := upstream.count()
		rec := do(t, h, clientRequest{
			method: http.MethodGet, target: target,
			accessKeyID: "NOTATENANTATALL0", secret: "whatever", tenant: tenantA, level: "ro",
		})
		assert.Equal(t, http.StatusForbidden, rec.Code)
		assert.Equal(t, before, upstream.count())
	})

	t.Run("wrong secret", func(t *testing.T) {
		before := upstream.count()
		rec := do(t, h, clientRequest{
			method: http.MethodGet, target: target,
			tenant: tenantA, level: "ro", secret: "guessed-secret",
		})
		assert.Equal(t, http.StatusForbidden, rec.Code)
		assert.Equal(t, before, upstream.count())
	})

	t.Run("presigned query-string authentication", func(t *testing.T) {
		before := upstream.count()
		rec := do(t, h, clientRequest{
			method: http.MethodGet,
			target: target + "?X-Amz-Algorithm=AWS4-HMAC-SHA256&X-Amz-Credential=" +
				accessKeyFor(tenantA, "ro") + "%2F20260101%2Feu-central-1%2Fs3%2Faws4_request&X-Amz-Signature=deadbeef",
			tenant: tenantA, level: "ro",
		})
		assert.Equal(t, http.StatusForbidden, rec.Code)
		assert.Equal(t, before, upstream.count())
	})

	t.Run("outside the clock skew window", func(t *testing.T) {
		before := upstream.count()
		rec := do(t, h, clientRequest{
			method: http.MethodGet, target: target,
			tenant: tenantA, level: "ro",
			signTime: time.Now().Add(-30 * time.Minute),
		})
		assert.Equal(t, http.StatusForbidden, rec.Code)
		assert.Equal(t, before, upstream.count())
	})

	t.Run("inside the clock skew window", func(t *testing.T) {
		rec := do(t, h, clientRequest{
			method: http.MethodGet, target: target,
			tenant: tenantA, level: "ro",
			signTime: time.Now().Add(-5 * time.Minute),
		})
		assert.Equal(t, http.StatusOK, rec.Code)
	})

	t.Run("source IP outside the allowed subnet", func(t *testing.T) {
		strict, upstream := newTestProxy(t)
		strict.AllowedSourceSubnet = testSubnets(t, "127.0.0.1/32")

		req := clientRequest{method: http.MethodGet, target: target, tenant: tenantA, level: "ro"}.build(t)
		rec := httptest.NewRecorder()
		strict.ServeHTTP(rec, req)
		assert.Equal(t, http.StatusForbidden, rec.Code)
		assert.Equal(t, 0, upstream.count())
	})
}

func TestReadOnlyKillSwitch(t *testing.T) {
	h, upstream := newTestProxy(t)
	h.ReadOnly = true

	rec := do(t, h, clientRequest{
		method: http.MethodPut, target: "/bucket/workspaces/a/x.txt",
		body: []byte("x"), tenant: tenantA, level: "rws",
	})
	assert.Equal(t, http.StatusForbidden, rec.Code)
	assert.Equal(t, 0, upstream.count())

	rec = do(t, h, clientRequest{
		method: http.MethodGet, target: "/bucket/workspaces/a/x.txt",
		tenant: tenantA, level: "rws",
	})
	assert.Equal(t, http.StatusOK, rec.Code)
}

// A client that sends an aws-chunked body (the AWS JS SDK v3 does by
// default) must work unchanged: the framing is decoded so the upstream
// stores the real object bytes, and a checksum sent as a trailer is carried
// over as a header.
func TestAwsChunkedUpload(t *testing.T) {
	payload := "PAR1this-is-the-real-object-content"

	t.Run("with a checksum trailer", func(t *testing.T) {
		h, upstream := newTestProxy(t)
		framed := fmt.Sprintf("%x\r\n%s\r\n0\r\nx-amz-checksum-crc32:abcd1234\r\n\r\n", len(payload), payload)
		rec := do(t, h, clientRequest{
			method: http.MethodPut, target: "/bucket/datasets/big.parquet",
			body: []byte(framed), tenant: tenantA, level: "rw",
			headers: http.Header{
				"X-Amz-Content-Sha256":         {"STREAMING-UNSIGNED-PAYLOAD-TRAILER"},
				"X-Amz-Decoded-Content-Length": {fmt.Sprint(len(payload))},
				"X-Amz-Trailer":                {"x-amz-checksum-crc32"},
				"Content-Encoding":             {"aws-chunked"},
			},
		})
		require.Equal(t, http.StatusOK, rec.Code)

		got := upstream.last(t)
		assert.Equal(t, payload, got.body, "the upstream must receive the de-chunked object")
		assert.Equal(t, "abcd1234", got.header.Get("X-Amz-Checksum-Crc32"), "the trailer checksum must survive as a header")
		assert.Empty(t, got.header.Get("X-Amz-Trailer"), "announcing a trailer that is no longer there breaks the upload")
		assert.NotContains(t, got.header.Get("Content-Encoding"), "aws-chunked")
	})

	t.Run("without a trailer the body streams", func(t *testing.T) {
		h, upstream := newTestProxy(t)
		framed := fmt.Sprintf("%x;chunk-signature=deadbeef\r\n%s\r\n0;chunk-signature=cafe\r\n\r\n", len(payload), payload)
		rec := do(t, h, clientRequest{
			method: http.MethodPut, target: "/bucket/datasets/big.parquet",
			body: []byte(framed), tenant: tenantA, level: "rw",
			headers: http.Header{
				"X-Amz-Content-Sha256":         {"STREAMING-AWS4-HMAC-SHA256-PAYLOAD"},
				"X-Amz-Decoded-Content-Length": {fmt.Sprint(len(payload))},
				"Content-Encoding":             {"aws-chunked"},
			},
		})
		require.Equal(t, http.StatusOK, rec.Code)

		got := upstream.last(t)
		assert.Equal(t, payload, got.body)
		// Nothing was buffered, so the payload hash cannot be stated.
		assert.Equal(t, unsignedPayload, got.header.Get("X-Amz-Content-Sha256"))
	})

	t.Run("a buffered body larger than the cap is refused", func(t *testing.T) {
		h, _ := newTestProxy(t)
		h.MaxChunkedBodySize = 16
		framed := fmt.Sprintf("%x\r\n%s\r\n0\r\nx-amz-checksum-crc32:abcd\r\n\r\n", len(payload), payload)
		rec := do(t, h, clientRequest{
			method: http.MethodPut, target: "/bucket/datasets/big.parquet",
			body: []byte(framed), tenant: tenantA, level: "rw",
			headers: http.Header{
				"X-Amz-Content-Sha256":         {"STREAMING-UNSIGNED-PAYLOAD-TRAILER"},
				"X-Amz-Decoded-Content-Length": {fmt.Sprint(len(payload))},
				"X-Amz-Trailer":                {"x-amz-checksum-crc32"},
			},
		})
		assert.Equal(t, http.StatusBadRequest, rec.Code)
	})
}

// A large object must not be buffered anywhere: the proxy's memory use has
// to be independent of the object's size.
func TestLargeUploadIsNotBuffered(t *testing.T) {
	h, upstream := newTestProxy(t)
	h.MaxChunkedBodySize = 1024 // far smaller than the object below
	h.MaxDeleteBodySize = 1024
	h.MaxRewriteBodySize = 1024

	payload := bytes.Repeat([]byte("0123456789abcdef"), 64*1024) // 1 MiB
	rec := do(t, h, clientRequest{
		method: http.MethodPut, target: "/bucket/datasets/large.bin",
		body: payload, tenant: tenantA, level: "rw",
	})
	require.Equal(t, http.StatusOK, rec.Code)
	assert.Len(t, upstream.last(t).body, len(payload))
}

// testSubnets builds the parsed subnet slice the handler expects.
func testSubnets(t *testing.T, cidrs ...string) []*net.IPNet {
	t.Helper()
	var out []*net.IPNet
	for _, cidr := range cidrs {
		_, subnet, err := net.ParseCIDR(cidr)
		require.NoError(t, err)
		out = append(out, subnet)
	}
	return out
}

// The access-key-id layout is the policy file's business. A deployment that
// spells its ids with a separator — or with anything else outside
// `[a-zA-Z0-9]` — must be able to authenticate, which it could not while the
// Authorization header's Credential field was parsed with a narrower
// character class than the one identity.accessKeyIdPattern allows.
func TestAccessKeyIDLayoutIsNotHardCoded(t *testing.T) {
	policy, err := ParsePolicy([]byte(`
identity:
  accessKeyIdPattern: '^(?P<tenant>[a-z0-9]+)-(?P<level>reader|writer)$'
  secretTemplate: 'v1/{level}/{tenant}'
  keyPrefixTemplate: 'tenants/{tenant}/data/'
levels: [reader, writer]
rules:
  - pathPattern: 'datasets/**'
    grant: { reader: read, writer: full }
`))
	require.NoError(t, err)

	h, upstream := newTestProxy(t)
	h.Policy = NewStaticPolicyStore(policy)

	rec := do(t, h, clientRequest{
		method: http.MethodPut, target: "/bucket/datasets/a.csv", body: []byte("x"),
		accessKeyID: "acmecorp-writer",
		secret:      deriveSecret(testPepper, "v1/writer/acmecorp"),
	})
	require.Equal(t, http.StatusOK, rec.Code)
	assert.Equal(t, "/bucket/tenants/acmecorp/data/datasets/a.csv", upstream.last(t).path)

	// And the level still separates the two credentials.
	before := upstream.count()
	rec = do(t, h, clientRequest{
		method: http.MethodPut, target: "/bucket/datasets/a.csv", body: []byte("x"),
		accessKeyID: "acmecorp-reader",
		secret:      deriveSecret(testPepper, "v1/reader/acmecorp"),
	})
	assert.Equal(t, http.StatusForbidden, rec.Code)
	assert.Equal(t, before, upstream.count())
}
