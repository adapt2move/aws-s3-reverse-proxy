package proxy

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
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
	"github.com/stretchr/testify/require"

	"github.com/Kriechi/aws-s3-reverse-proxy/internal/policy"
	"github.com/Kriechi/aws-s3-reverse-proxy/internal/policytest"
	"github.com/Kriechi/aws-s3-reverse-proxy/internal/sigv4"
)

// The suite runs against the shared fixture in internal/policytest: a
// writable dataset area, a read-only carve-out nested inside a writable
// workspace tree, and two tenants that must never reach each other.
const (
	tenantA = policytest.TenantA
	tenantB = policytest.TenantB

	testEndpoint = "s3.proxy.example.com:8099"
	testRegion   = "eu-central-1"
)

var testPepper = policytest.Pepper

func init() {
	// The access log is noise in test output; failures are asserted on, not
	// read.
	log.SetLevel(log.PanicLevel)
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

// latest returns the most recent recorded request without the testing.T
// plumbing, for use inside the upstream's own handler.
func (u *fakeUpstream) latest() recordedRequest {
	u.mu.Lock()
	defer u.mu.Unlock()
	if len(u.requests) == 0 {
		return recordedRequest{}
	}
	return u.requests[len(u.requests)-1]
}

func (u *fakeUpstream) count() int {
	u.mu.Lock()
	defer u.mu.Unlock()
	return len(u.requests)
}

// testConfig is the configuration main() would assemble, pointed at an
// upstream of the caller's choosing.
func testConfig(t *testing.T, upstreamURL string) Config {
	t.Helper()
	return Config{
		UpstreamScheme:        "http",
		UpstreamEndpoint:      strings.TrimPrefix(upstreamURL, "http://"),
		UpstreamRegion:        testRegion,
		AllowedSourceEndpoint: testEndpoint,
		AllowedSourceSubnet:   testSubnets(t, "0.0.0.0/0"),
		Policy:                policy.NewStatic(policytest.Policy()),
		Pepper:                testPepper,
		UpstreamSigner:        sigv4.NewSigner("UPSTREAMKEYID", "upstream-secret"),
		MaxClockSkew:          15 * time.Minute,
		Limits: Limits{
			ChunkedBody: 1 << 20,
			DeleteBody:  1 << 20,
			RewriteBody: 1 << 20,
		},
	}
}

// newTestProxy builds a handler wired to a fake upstream. A Config is
// immutable once New has seen it — which is the point of taking it whole —
// so a test that needs a different deployment says so up front through
// `tweaks` rather than reaching into the handler afterwards.
func newTestProxy(t *testing.T, tweaks ...func(*Config)) (*Handler, *fakeUpstream) {
	t.Helper()
	upstream := newFakeUpstream(t)
	cfg := testConfig(t, upstream.server.URL)
	for _, tweak := range tweaks {
		tweak(&cfg)
	}
	h, err := New(cfg)
	require.NoError(t, err)
	return h, upstream
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

func accessKeyFor(tenant, level string) string { return policytest.AccessKeyID(tenant, level) }

func secretFor(tenant, level string) string { return policytest.Secret(tenant, level) }

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
