package proxy

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/Kriechi/aws-s3-reverse-proxy/internal/policy"
	"github.com/Kriechi/aws-s3-reverse-proxy/internal/policytest"
	"github.com/Kriechi/aws-s3-reverse-proxy/internal/sigv4"
)

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
		strict, upstream := newTestProxy(t, func(c *Config) {
			c.AllowedSourceSubnet = testSubnets(t, "127.0.0.1/32")
		})

		req := clientRequest{method: http.MethodGet, target: target, tenant: tenantA, level: "ro"}.build(t)
		rec := httptest.NewRecorder()
		strict.ServeHTTP(rec, req)
		assert.Equal(t, http.StatusForbidden, rec.Code)
		assert.Equal(t, 0, upstream.count())
	})
}

func TestReadOnlyKillSwitch(t *testing.T) {
	h, upstream := newTestProxy(t, func(c *Config) { c.ReadOnly = true })

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
		assert.Equal(t, sigv4.UnsignedPayload, got.header.Get("X-Amz-Content-Sha256"))
	})

	t.Run("a buffered body larger than the cap is refused", func(t *testing.T) {
		h, _ := newTestProxy(t, func(c *Config) { c.Limits.ChunkedBody = 16 })
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
	// Every buffering cap is set far below the object pushed through, so a
	// proxy that buffered anywhere would refuse the upload instead of
	// streaming it.
	h, upstream := newTestProxy(t, func(c *Config) {
		c.Limits = Limits{ChunkedBody: 1024, DeleteBody: 1024, RewriteBody: 1024}
	})

	payload := bytes.Repeat([]byte("0123456789abcdef"), 64*1024) // 1 MiB
	rec := do(t, h, clientRequest{
		method: http.MethodPut, target: "/bucket/datasets/large.bin",
		body: payload, tenant: tenantA, level: "rw",
	})
	require.Equal(t, http.StatusOK, rec.Code)
	assert.Len(t, upstream.last(t).body, len(payload))
}

// The access-key-id layout is the policy file's business. A deployment that
// spells its ids with a separator — or with anything else outside
// `[a-zA-Z0-9]` — must be able to authenticate, which it could not while the
// Authorization header's Credential field was parsed with a narrower
// character class than the one identity.accessKeyIdPattern allows.
func TestAccessKeyIDLayoutIsNotHardCoded(t *testing.T) {
	compiled, err := policy.Parse([]byte(`
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

	h, upstream := newTestProxy(t, func(c *Config) { c.Policy = policy.NewStatic(compiled) })

	rec := do(t, h, clientRequest{
		method: http.MethodPut, target: "/bucket/datasets/a.csv", body: []byte("x"),
		accessKeyID: "acmecorp-writer",
		secret:      policy.DeriveSecret(testPepper, "v1/writer/acmecorp"),
	})
	require.Equal(t, http.StatusOK, rec.Code)
	assert.Equal(t, "/bucket/tenants/acmecorp/data/datasets/a.csv", upstream.last(t).path)

	// And the level still separates the two credentials.
	before := upstream.count()
	rec = do(t, h, clientRequest{
		method: http.MethodPut, target: "/bucket/datasets/a.csv", body: []byte("x"),
		accessKeyID: "acmecorp-reader",
		secret:      policy.DeriveSecret(testPepper, "v1/reader/acmecorp"),
	})
	assert.Equal(t, http.StatusForbidden, rec.Code)
	assert.Equal(t, before, upstream.count())
}

// The policy in force is read once per request, so a reload can never decide
// half of one.
func TestHandlerUsesReloadedPolicy(t *testing.T) {
	// A PolicySource the test can swap under a running handler — which is
	// what the interface is for: nothing here needs a file on disk or a
	// reload timer to prove the handler reads the policy per request.
	source := &swappablePolicy{current: policytest.Policy()}
	h, upstream := newTestProxy(t, func(c *Config) { c.Policy = source })

	rec := do(t, h, clientRequest{
		method: http.MethodPut, target: "/bucket/datasets/a.csv",
		body: []byte("x"), tenant: tenantA, level: "rw",
	})
	require.Equal(t, http.StatusOK, rec.Code)

	tightened, err := policy.Parse([]byte(`
identity:
  accessKeyIdPattern: '^(?P<tenant>[0-9a-f]{32})(?P<level>ro|rw|rws)$'
  secretTemplate: '{tenant}:{level}'
  keyPrefixTemplate: '{tenant}/'
levels: [ro, rw, rws]
rules:
  - pathPattern: 'datasets/**'
    grant: { ro: read, rw: read, rws: full }
`))
	require.NoError(t, err)
	source.current = tightened

	before := upstream.count()
	rec = do(t, h, clientRequest{
		method: http.MethodPut, target: "/bucket/datasets/a.csv",
		body: []byte("x"), tenant: tenantA, level: "rw",
	})
	assert.Equal(t, http.StatusForbidden, rec.Code)
	assert.Equal(t, before, upstream.count())
}

// swappablePolicy is the smallest possible PolicySource.
type swappablePolicy struct{ current *policy.Policy }

func (s *swappablePolicy) Current() *policy.Policy { return s.current }

// Whatever the proxy forwards, it also signs. Copying client headers onto the
// upstream request after signing would leave SignedHeaders at
// `host;x-amz-content-sha256;x-amz-date` — which AWS S3 refuses outright, and
// which on a store that does not refuse it means an `x-amz-` header took
// effect while covered by no signature, no classification and no policy rule.
func TestForwardedHeadersAreSigned(t *testing.T) {
	h, upstream := newTestProxy(t)

	rec := do(t, h, clientRequest{
		method: http.MethodPut, target: "/bucket/datasets/x.csv",
		body: []byte("hello"), tenant: tenantA, level: "rw",
		headers: http.Header{
			"Content-Type":        {"text/csv"},
			"Cache-Control":       {"max-age=60"},
			"X-Amz-Meta-Owner":    {"team-a"},
			"Content-Disposition": {`attachment; filename="x.csv"`},
		},
	})
	require.Equal(t, http.StatusOK, rec.Code)

	got := upstream.last(t)
	signed := signedHeaders(t, got.header.Get("Authorization"))
	for _, name := range []string{"content-type", "cache-control", "x-amz-meta-owner", "content-disposition"} {
		assert.Contains(t, signed, name, "a forwarded header must be covered by the upstream signature")
	}
	assert.Equal(t, "team-a", got.header.Get("X-Amz-Meta-Owner"))
	assert.Equal(t, "text/csv", got.header.Get("Content-Type"))
}

// The inverse of the refusal below, and the more fragile half: a whitelist
// that grows teeth against the next client is a whitelist nobody notices
// until that client 403s on every request. These are headers real SDKs send
// that carry no instruction to the object store — `x-amz-user-agent` above
// all, which is `User-Agent` wearing an `x-amz-` prefix because the Fetch
// spec forbids a browser from setting its own.
func TestClientTelemetryHeadersAreAccepted(t *testing.T) {
	for _, header := range []string{
		"X-Amz-User-Agent",             // AWS JS SDK v3 in a browser
		"X-Amz-Te",                     // legacy Java SDK v1
		"X-Amz-Sdk-Request",            // SDK retry counters
		"X-Amz-Sdk-Invocation-Id",      // SDK retry correlation
		"X-Amz-Sdk-Checksum-Algorithm", // the algorithm the SDK chose
	} {
		t.Run(header, func(t *testing.T) {
			h, upstream := newTestProxy(t)
			rec := do(t, h, clientRequest{
				method: http.MethodPut, target: "/bucket/datasets/x.csv",
				body: []byte("hello"), tenant: tenantA, level: "rw",
				headers: http.Header{header: {"anything"}},
			})
			require.Equal(t, http.StatusOK, rec.Code, "a client sending %s must not be refused", header)
			// Accepted, but it describes the caller rather than the object,
			// and to the object store the caller is this proxy.
			assert.Empty(t, upstream.last(t).header.Get(header),
				"%s describes the client, so it must not travel upstream", header)
		})
	}
}

// An `x-amz-` header this proxy does not understand is one it cannot
// authorize, so it is refused rather than passed through. x-amz-object-lock-*
// is the case that makes failing closed worth it: it can pin an object beyond
// any deletion, the operator's own included.
func TestUnknownAmzHeadersAreRefused(t *testing.T) {
	for _, header := range []string{
		"X-Amz-Acl", "X-Amz-Object-Lock-Mode", "X-Amz-Object-Lock-Retain-Until-Date",
		"X-Amz-Storage-Class", "X-Amz-Tagging", "X-Amz-Server-Side-Encryption",
		"X-Amz-Grant-Read", "X-Amz-Security-Token", "X-Amz-Expected-Bucket-Owner",
		"X-Amz-Website-Redirect-Location",
	} {
		t.Run(header, func(t *testing.T) {
			h, upstream := newTestProxy(t)
			rec := do(t, h, clientRequest{
				method: http.MethodPut, target: "/bucket/datasets/x.csv",
				body: []byte("hello"), tenant: tenantA, level: "rw",
				headers: http.Header{header: {"anything"}},
			})
			assert.Equal(t, http.StatusForbidden, rec.Code)
			assert.Equal(t, 0, upstream.count(), "the request must not reach the object store")
		})
	}
}

// The headers the proxy consumes itself describe the request to the proxy,
// not to the object store: they are accepted on the way in and replaced on
// the way out.
func TestConsumedHeadersDoNotTravelUpstream(t *testing.T) {
	h, upstream := newTestProxy(t)
	payload := "PAR1this-is-the-real-object-content"
	framed := fmt.Sprintf("%x\r\n%s\r\n0\r\n\r\n", len(payload), payload)

	rec := do(t, h, clientRequest{
		method: http.MethodPut, target: "/bucket/datasets/big.parquet",
		body: []byte(framed), tenant: tenantA, level: "rw",
		headers: http.Header{
			"X-Amz-Content-Sha256":         {"STREAMING-AWS4-HMAC-SHA256-PAYLOAD"},
			"X-Amz-Decoded-Content-Length": {fmt.Sprint(len(payload))},
			"X-Amz-Sdk-Checksum-Algorithm": {"CRC32"},
			"Content-Encoding":             {"aws-chunked"},
		},
	})
	require.Equal(t, http.StatusOK, rec.Code)

	got := upstream.last(t)
	assert.Equal(t, payload, got.body)
	assert.Empty(t, got.header.Get("X-Amz-Decoded-Content-Length"))
	assert.Empty(t, got.header.Get("X-Amz-Sdk-Checksum-Algorithm"))
	assert.NotContains(t, got.header.Get("Content-Encoding"), "aws-chunked")
	// The client's own credential never travels upstream either.
	assert.Contains(t, got.header.Get("Authorization"), "Credential=UPSTREAMKEYID/")
}

// A body with no stated length can only be forwarded as another chunked
// transfer encoding, which S3 refuses on PUT — so it is refused here, where
// the reason can be given.
func TestBodyWithoutContentLengthIsRefused(t *testing.T) {
	h, upstream := newTestProxy(t)

	req := clientRequest{
		method: http.MethodPut, target: "/bucket/datasets/x.csv",
		body: []byte("hello"), tenant: tenantA, level: "rw",
	}.build(t)
	req.ContentLength = -1

	rec := doRequest(t, h, req)
	assert.Equal(t, http.StatusBadRequest, rec.Code)
	assert.Equal(t, 0, upstream.count())
}

// signedHeaders pulls the SignedHeaders list out of an Authorization header.
func signedHeaders(t *testing.T, authorization string) []string {
	t.Helper()
	for _, part := range strings.Split(authorization, ",") {
		part = strings.TrimSpace(part)
		if rest, ok := strings.CutPrefix(part, "SignedHeaders="); ok {
			return strings.Split(rest, ";")
		}
	}
	t.Fatalf("no SignedHeaders in %q", authorization)
	return nil
}
