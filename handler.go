package main

import (
	"context"
	"crypto/subtle"
	"errors"
	"fmt"
	"net"
	"net/http"
	"net/http/httputil"
	"net/url"
	"regexp"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go/aws/credentials"
	v4 "github.com/aws/aws-sdk-go/aws/signer/v4"
	log "github.com/sirupsen/logrus"
)

//   - new less strict regexp in order to allow different region naming (compatibility with other providers)
//   - east-eu-1 => pass (aws style)
//   - gra => pass (ceph style)
//   - "" => pass (some S3 clients, e.g. DuckDB's httpfs, leave the region
//     segment empty when no region is configured; the signature is still
//     valid, it just has an empty region scope)
var awsAuthorizationCredentialRegexp = regexp.MustCompile("Credential=([a-zA-Z0-9]+)/[0-9]+/([a-zA-Z-0-9]*)/s3/aws4_request")
var awsAuthorizationSignedHeadersRegexp = regexp.MustCompile("SignedHeaders=([a-zA-Z0-9;-]+)")

// emptyPayloadSHA256 is the SigV4 payload hash of a zero-length body.
const emptyPayloadSHA256 = "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"

// unsignedPayload tells the upstream that the payload hash is not part of
// the signature. We use it for the bodies we forward as a stream and whose
// digest we therefore never see (see prepareBody).
const unsignedPayload = "UNSIGNED-PAYLOAD"

// hexSHA256Regexp matches a literal payload hash as a client sends it in
// x-amz-content-sha256.
var hexSHA256Regexp = regexp.MustCompile(`^[0-9a-f]{64}$`)

// Handler is a multi-tenant S3 reverse proxy. Every request carries its own
// identity: the access-key id names the tenant and the access level, the
// secret is derived from a deployment-wide pepper, and the tenant's key
// prefix is injected by the proxy rather than sent by the client. Nothing
// tenant-specific is configured anywhere, so onboarding a tenant is a no-op
// here.
type Handler struct {
	// Print debug information, and include the rejection reason in error
	// responses. Never includes credentials or signatures.
	Debug bool

	// When true, every mutating method is rejected regardless of policy —
	// a deployment-wide kill switch for maintenance windows, independent
	// of the file on disk.
	ReadOnly bool

	// http or https, derived from the configured upstream endpoint.
	UpstreamScheme string

	// Upstream S3 endpoint (host[:port]); empty auto-detects AWS S3 from
	// the request's region.
	UpstreamEndpoint string

	// Allowed endpoint, i.e., Host header to accept incoming requests from
	AllowedSourceEndpoint string

	// Allowed source IPs and subnets for incoming requests
	AllowedSourceSubnet []*net.IPNet

	// Policy in force. Swapped atomically by a hot reload; a request reads
	// it exactly once so it is decided by one consistent policy throughout.
	Policy *PolicyStore

	// Pepper keys the HMAC that derives a client's secret. Comes from the
	// environment or a mounted secret, never from the policy file.
	Pepper []byte

	// UpstreamSigner re-signs requests with the credentials only this
	// process holds. Clients never see them.
	UpstreamSigner *v4.Signer

	// Optional: sign upstream requests for this region instead of the
	// region from the client's request. Useful when the client signs with
	// a placeholder (or empty) region but the backend expects a real one.
	UpstreamRegion string

	// MaxClockSkew bounds how far an X-Amz-Date may be from our clock in
	// either direction. It is what stops a captured signature from being
	// replayed indefinitely.
	MaxClockSkew time.Duration

	// MaxChunkedBodySize caps the aws-chunked body we buffer in order to
	// recover a checksum trailer (see prepareBody).
	MaxChunkedBodySize int64

	// MaxDeleteBodySize caps the DeleteObjects body we buffer, parse and
	// rebuild.
	MaxDeleteBodySize int64

	// MaxRewriteBodySize caps the XML response we buffer in order to strip
	// the tenant prefix back out.
	MaxRewriteBodySize int64

	// TenantMetricLabel adds the tenant id as a Prometheus label. Off by
	// default: tenant count is unbounded and each one would mint a new
	// time series.
	TenantMetricLabel bool

	// Proxy is built once and shared: it carries the connection pool, so
	// re-creating it per request would throw away every keep-alive.
	Proxy *httputil.ReverseProxy
}

// requestState is everything the response side needs to know about a
// request it is answering. It travels on the upstream request's context so
// that one shared ReverseProxy can serve every tenant.
type requestState struct {
	policy    *Policy
	identity  *Identity
	operation *operation

	// deniedDeletes are the batch-delete keys (client-facing) policy
	// refused. They are merged back into the upstream response as per-key
	// AccessDenied entries, which is what S3 itself does for a key the
	// caller may not delete.
	deniedDeletes []string

	// rule is the policy rule a body-level authorization matched, for the
	// access log — the URL-level path records it directly on the entry.
	rule string

	// clientRegion is the region scope the client signed with. It is only
	// used when no upstream region is configured, and it may legitimately
	// be empty (DuckDB's httpfs signs that way).
	clientRegion string

	// maxRewriteSize mirrors Handler.MaxRewriteBodySize so the response
	// rewrite needs nothing but this struct.
	maxRewriteSize int64
}

type requestStateKey struct{}

func withRequestState(ctx context.Context, st *requestState) context.Context {
	return context.WithValue(ctx, requestStateKey{}, st)
}

func requestStateFrom(ctx context.Context) *requestState {
	st, _ := ctx.Value(requestStateKey{}).(*requestState)
	return st
}

// NewHandler wires up the shared reverse proxy. The transport keeps a
// generous idle-connection pool: Go's default of two per host would force a
// fresh TCP (and TLS) handshake on most requests, which alone would blow
// the added-latency budget.
func NewHandler(h *Handler) *Handler {
	transport := http.DefaultTransport.(*http.Transport).Clone()
	transport.MaxIdleConns = 512
	transport.MaxIdleConnsPerHost = 128
	transport.IdleConnTimeout = 90 * time.Second

	if h.UpstreamSigner != nil {
		// S3 does not escape the canonical URI a second time. Without this
		// the signature would cover a path that differs from the one on the
		// wire for every key containing a character that needs escaping — a
		// space, a `+`, a non-ASCII byte.
		h.UpstreamSigner.DisableURIPathEscaping = true
	}

	h.Proxy = &httputil.ReverseProxy{
		// The request handed to ServeHTTP already carries the absolute
		// upstream URL, so there is nothing left to direct.
		Director:      func(*http.Request) {},
		Transport:     transport,
		FlushInterval: -1, // flush every write: object downloads stream
		ModifyResponse: func(resp *http.Response) error {
			return rewriteUpstreamResponse(resp)
		},
		ErrorHandler: func(w http.ResponseWriter, r *http.Request, err error) {
			log.WithError(err).Warn("upstream request failed")
			writeS3Error(w, http.StatusBadGateway, "InternalError", "The proxy could not reach the object store.")
		},
	}
	return h
}

func (h *Handler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	start := time.Now()
	rec := &statusRecorder{ResponseWriter: w, status: http.StatusOK}
	entry := &accessLogEntry{method: r.Method, path: r.URL.Path}

	proxyReq, st, err := h.prepare(rec, r, entry)
	if err != nil || proxyReq == nil {
		h.finishAccessLog(entry, rec, start)
		return
	}

	h.Proxy.ServeHTTP(rec, proxyReq.WithContext(withRequestState(r.Context(), st)))
	h.finishAccessLog(entry, rec, start)
}

// prepare runs the whole per-request lifecycle up to the point where the
// request is ready to be forwarded: verify, resolve, inject, authorize,
// re-sign. It returns (nil, nil, nil) when it has already answered the
// request itself — a rejection, or a batch delete in which policy refused
// every key.
func (h *Handler) prepare(w http.ResponseWriter, r *http.Request, entry *accessLogEntry) (*http.Request, *requestState, error) {
	if err := h.validateIncomingSourceIP(r); err != nil {
		h.reject(w, entry, http.StatusForbidden, "AccessDenied", err)
		return nil, nil, err
	}

	// Classify before authenticating: an operation this proxy does not
	// implement is refused whether or not the caller holds a valid
	// credential, so an unsupported call can never fall through to the
	// upstream by accident.
	op, err := classifyRequest(r)
	if err != nil {
		status := http.StatusForbidden
		if errors.Is(err, errInvalidKey) {
			status = http.StatusBadRequest
		}
		h.reject(w, entry, status, "AccessDenied", err)
		return nil, nil, err
	}
	entry.operation = string(op.kind)
	entry.key = op.key
	if op.kind == opListObjects {
		entry.key = op.listPrefix
	}

	if h.ReadOnly && !isReadMethod(r.Method) {
		err := fmt.Errorf("proxy is in read-only mode")
		h.reject(w, entry, http.StatusForbidden, "AccessDenied", err)
		return nil, nil, err
	}

	policy := h.Policy.Current()
	identity, region, err := h.authenticate(policy, r)
	if err != nil {
		h.reject(w, entry, http.StatusForbidden, "AccessDenied", err)
		return nil, nil, err
	}
	entry.tenant, entry.level = identity.Tenant, identity.Level

	st := &requestState{
		policy:         policy,
		identity:       identity,
		operation:      op,
		clientRegion:   region,
		maxRewriteSize: h.MaxRewriteBodySize,
	}

	// A batch delete is authorized key by key inside prepareBody, because
	// its keys live in the request body. Everything else has exactly one
	// subject: the object key, or the prefix of a listing.
	if op.kind != opDeleteObjects {
		subject := op.key
		if op.kind == opListObjects {
			subject = op.listPrefix
		}
		decision := policy.Authorize(identity.Level, subject, r.Method)
		entry.rule, entry.allowed = decision.Rule, decision.Allowed
		if !decision.Allowed {
			err := fmt.Errorf("policy denies %s on %q for level %q", r.Method, subject, identity.Level)
			h.reject(w, entry, http.StatusForbidden, "AccessDenied", err)
			return nil, nil, err
		}
	}

	proxyReq, err := h.buildUpstreamRequest(r, st)
	if op.kind == opDeleteObjects {
		// A batch delete is authorized inside the body preparation, so its
		// outcome only becomes known here.
		entry.rule = st.rule
		entry.deniedKeys = len(st.deniedDeletes)
		entry.allowed = err == nil
	}
	if err != nil {
		if errors.Is(err, errAllKeysDenied) {
			// Nothing survived authorization, so there is no upstream call
			// to make — answer with the per-key AccessDenied entries S3
			// would have returned.
			entry.reason = err.Error()
			writeDeleteResult(w, st.deniedDeletes)
			return nil, nil, nil
		}
		status := http.StatusBadRequest
		code := "InvalidRequest"
		if errors.Is(err, errUnsupportedOperation) {
			status, code = http.StatusForbidden, "AccessDenied"
		}
		h.reject(w, entry, status, code, err)
		return nil, nil, err
	}
	return proxyReq, st, nil
}

// reject answers a refused request. The body is an S3-shaped error so SDK
// clients surface something meaningful; the internal reason is logged, and
// only echoed to the client in debug mode.
func (h *Handler) reject(w http.ResponseWriter, entry *accessLogEntry, status int, code string, err error) {
	entry.allowed = false
	entry.reason = err.Error()
	message := "Access Denied."
	if h.Debug {
		message = err.Error()
	}
	writeS3Error(w, status, code, message)
}

// authenticate verifies the inbound SigV4 header signature and resolves the
// identity behind it.
//
// The secret is never stored: it is recomputed from the pepper for this one
// access-key id, the client's canonical request is reconstructed with it,
// and the two Authorization headers are compared in constant time. An
// anonymous request has no Authorization header and fails at the first
// step; an unknown access-key id fails at resolution; a forged signature
// fails at the comparison. All three are the same 403 to the client.
func (h *Handler) authenticate(policy *Policy, req *http.Request) (*Identity, string, error) {
	accessKeyID, region, err := h.validateIncomingHeaders(req)
	if err != nil {
		return nil, "", err
	}

	signTime, err := time.Parse("20060102T150405Z", req.Header["X-Amz-Date"][0])
	if err != nil {
		return nil, "", fmt.Errorf("malformed X-Amz-Date")
	}
	// A signature stays valid forever unless its timestamp is bounded, so a
	// captured Authorization header could be replayed at will.
	if skew := time.Since(signTime); skew > h.MaxClockSkew || skew < -h.MaxClockSkew {
		return nil, "", fmt.Errorf("X-Amz-Date is outside the accepted clock skew of %s", h.MaxClockSkew)
	}

	identity, err := policy.ResolveIdentity(accessKeyID, h.Pepper)
	if err != nil {
		return nil, "", err
	}

	signer := v4.NewSigner(credentials.NewStaticCredentialsFromCreds(credentials.Value{
		AccessKeyID:     accessKeyID,
		SecretAccessKey: identity.SecretAccessKey,
	}))
	// Match how an S3 client signs: the canonical URI is the request path
	// exactly as written on the wire, not that path escaped again.
	signer.DisableURIPathEscaping = true
	fakeReq, err := h.generateFakeIncomingRequest(signer, req, region, signTime)
	if err != nil {
		return nil, "", err
	}

	// WORKAROUND S3CMD which dont use white space before the some commas in
	// the authorization header.
	authorizationStr := strings.Replace(req.Header["Authorization"][0], ",Signature", ", Signature", 1)
	authorizationStr = strings.Replace(authorizationStr, ",SignedHeaders", ", SignedHeaders", 1)

	if subtle.ConstantTimeCompare([]byte(fakeReq.Header.Get("Authorization")), []byte(authorizationStr)) == 0 {
		// Deliberately no request dump here: it would carry the client's
		// Authorization header — and therefore its signature — into the log
		// at debug level.
		return nil, "", fmt.Errorf("invalid signature in Authorization header")
	}
	return identity, region, nil
}

func (h *Handler) validateIncomingSourceIP(req *http.Request) error {
	ip, _, _ := net.SplitHostPort(req.RemoteAddr)
	userIP := net.ParseIP(ip)
	for _, subnet := range h.AllowedSourceSubnet {
		if subnet.Contains(userIP) {
			return nil
		}
	}
	return fmt.Errorf("source IP %s not allowed", ip)
}

func (h *Handler) validateIncomingHeaders(req *http.Request) (string, string, error) {
	if len(req.Header["X-Amz-Date"]) != 1 {
		return "", "", fmt.Errorf("X-Amz-Date header missing or set multiple times")
	}
	if len(req.Header["Authorization"]) != 1 {
		return "", "", fmt.Errorf("Authorization header missing or set multiple times")
	}
	match := awsAuthorizationCredentialRegexp.FindStringSubmatch(req.Header["Authorization"][0])
	if len(match) != 3 {
		return "", "", fmt.Errorf("invalid Authorization header: Credential not found")
	}
	return match[1], match[2], nil
}

// generateFakeIncomingRequest rebuilds the canonical request the client
// signed and signs it with the derived secret, so the two Authorization
// headers can be compared.
func (h *Handler) generateFakeIncomingRequest(signer *v4.Signer, req *http.Request, region string, signTime time.Time) (*http.Request, error) {
	// req.URL.String() round-trips the path in exactly the form the client
	// wrote it, which is the form it signed.
	fakeReq, err := http.NewRequest(req.Method, req.URL.String(), nil)
	if err != nil {
		return nil, err
	}

	// We already validated that there is exactly one Authorization header.
	match := awsAuthorizationSignedHeadersRegexp.FindStringSubmatch(req.Header.Get("authorization"))
	if len(match) == 2 {
		for _, header := range strings.Split(match[1], ";") {
			fakeReq.Header.Set(header, req.Header.Get(header))
		}
	}

	// Delete a potentially double-added header
	fakeReq.Header.Del("host")
	fakeReq.Host = h.AllowedSourceEndpoint

	// The body is never read here: whatever payload hash the client signed
	// is among the signed headers we just copied, and the signer uses that
	// header verbatim when it is present. Verification therefore costs
	// nothing in memory, no matter how large the upload is.
	if _, err := signer.Sign(fakeReq, nil, "s3", region, signTime); err != nil {
		return nil, err
	}
	return fakeReq, nil
}

// isReadMethod reports whether an HTTP method is a non-mutating S3 read.
func isReadMethod(method string) bool {
	return method == http.MethodGet || method == http.MethodHead
}

// buildUpstreamRequest injects the tenant prefix, prepares the body and
// re-signs the request with the upstream credentials.
func (h *Handler) buildUpstreamRequest(req *http.Request, st *requestState) (*http.Request, error) {
	region := h.UpstreamRegion
	if region == "" {
		region = st.clientRegion
	}
	upstreamEndpoint := h.UpstreamEndpoint
	if upstreamEndpoint == "" {
		// No configured endpoint: fall back to AWS S3 for the region the
		// request is signed for, as the single-tenant proxy always did.
		upstreamEndpoint = fmt.Sprintf("s3.%s.amazonaws.com", region)
	}

	proxyURL := *req.URL
	proxyURL.Scheme = h.UpstreamScheme
	proxyURL.Host = upstreamEndpoint
	scopeToTenant(&proxyURL, st.identity.KeyPrefix, st.operation)

	body, payloadHash, err := h.prepareBody(req, st)
	if err != nil {
		return nil, err
	}

	proxyReq, err := http.NewRequest(req.Method, proxyURL.String(), body.reader)
	if err != nil {
		return nil, err
	}
	proxyReq.ContentLength = body.contentLength
	if val, ok := req.Header["Content-Type"]; ok {
		proxyReq.Header["Content-Type"] = val
	}
	if val, ok := req.Header["Content-Md5"]; ok {
		proxyReq.Header["Content-Md5"] = val
	}
	for name, values := range body.extraHeaders {
		proxyReq.Header[name] = values
	}
	// Set before signing so the signer adopts this digest instead of
	// reading the body to compute one — that read is what would otherwise
	// pull an entire upload into memory.
	proxyReq.Header.Set("X-Amz-Content-Sha256", payloadHash)

	// The signer attaches whatever body it was handed to the request, and
	// we hand it none on purpose — so that it adopts the payload hash set
	// above instead of reading the upload to compute one. Put the real body
	// back afterwards.
	signedBody, signedLength := proxyReq.Body, proxyReq.ContentLength
	if _, err := h.UpstreamSigner.Sign(proxyReq, nil, "s3", region, time.Now()); err != nil {
		return nil, err
	}
	proxyReq.Body, proxyReq.ContentLength = signedBody, signedLength

	// Add origin headers after the request is signed (no overwrite).
	copyHeaderWithoutOverwrite(proxyReq.Header, req.Header)
	// The client's own credential must never travel upstream — the
	// upstream signature replaced it.
	proxyReq.Header.Del("X-Amz-Decoded-Content-Length")

	return proxyReq, nil
}

// scopeToTenant is the whole isolation mechanism: the tenant's key prefix
// is *injected* here, never validated against something the client sent. A
// client cannot express a path outside its own scope, because the scope is
// not part of the request it makes.
//
// It covers the object key and, for a listing, the parameters that name
// keys: `prefix`, `marker` (ListObjects v1) and `start-after`
// (ListObjectsV2).
//
// `continuation-token` is deliberately forwarded verbatim. It is an opaque
// value the upstream minted for an already-scoped listing — prefixing it
// would corrupt it (and with it, pagination), while forwarding it is safe
// precisely because the client could not have obtained a token for any
// other scope.
func scopeToTenant(u *url.URL, keyPrefix string, op *operation) {
	if op.isBucketLevel() {
		if op.kind != opListObjects {
			return
		}
		q := u.Query()
		q.Set("prefix", keyPrefix+op.listPrefix)
		for _, param := range []string{"marker", "start-after"} {
			if v := q.Get(param); v != "" {
				q.Set(param, keyPrefix+v)
			}
		}
		u.RawQuery = q.Encode()
		return
	}
	// The key prefix is restricted to characters that need no escaping
	// (see validateRenderedKeyPrefix), so the decoded and the as-written
	// path stay in agreement.
	u.Path = "/" + op.bucket + "/" + keyPrefix + op.key
	u.RawPath = "/" + op.bucket + "/" + keyPrefix + op.escapedKey
}

func copyHeaderWithoutOverwrite(dst http.Header, src http.Header) {
	for k, v := range src {
		if _, ok := dst[k]; !ok {
			for _, vv := range v {
				dst.Add(k, vv)
			}
		}
	}
}
