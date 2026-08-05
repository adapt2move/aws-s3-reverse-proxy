// Package proxy is the request lifecycle, and only the lifecycle.
//
// One request travels through it in a fixed order — classify, authenticate,
// authorize, scope, re-sign, forward, rewrite — and every step of that order
// is delegated: the S3 protocol to internal/s3, the signatures to
// internal/sigv4, the decisions to internal/policy, the reporting to
// internal/observability. What is left here is the sequence itself, which is
// the part worth reading in one piece.
package proxy

import (
	"context"
	"errors"
	"fmt"
	"net"
	"net/http"
	"net/http/httputil"
	"time"

	log "github.com/sirupsen/logrus"

	"github.com/Kriechi/aws-s3-reverse-proxy/internal/observability"
	"github.com/Kriechi/aws-s3-reverse-proxy/internal/policy"
	"github.com/Kriechi/aws-s3-reverse-proxy/internal/s3"
	"github.com/Kriechi/aws-s3-reverse-proxy/internal/sigv4"
)

// PolicySource hands out the policy in force at the moment it is asked.
//
// A request reads it exactly once, so a hot reload can never split one
// request across two policies. *policy.Store is the implementation; the
// interface is what keeps this package from depending on there being a file
// on disk at all.
type PolicySource interface {
	Current() *policy.Policy
}

// Limits cap the three places where a body cannot be streamed and has to be
// held in memory instead. Each one is a bound on what a single request may
// make this process allocate.
type Limits struct {
	// ChunkedBody caps an aws-chunked body buffered to recover a checksum
	// trailer.
	ChunkedBody int64
	// DeleteBody caps a DeleteObjects request body, which is parsed and
	// rebuilt key by key.
	DeleteBody int64
	// RewriteBody caps an XML response buffered to strip the tenant prefix
	// back out of it.
	RewriteBody int64
}

// Config is everything a Handler needs. It is passed once to New and never
// mutated afterwards — the only thing that changes at runtime is the policy,
// and that arrives through PolicySource.
type Config struct {
	// Debug includes the rejection reason in error responses. It never
	// includes credentials or signatures.
	Debug bool

	// ReadOnly rejects every mutating method regardless of policy — a
	// deployment-wide kill switch for maintenance windows, independent of
	// the file on disk.
	ReadOnly bool

	// UpstreamScheme is http or https, derived from the configured endpoint.
	UpstreamScheme string

	// UpstreamEndpoint is the object store's host[:port]; empty auto-detects
	// AWS S3 from the region the request was signed for.
	UpstreamEndpoint string

	// UpstreamRegion signs upstream requests for this region instead of the
	// one from the client's request. Useful when the client signs with a
	// placeholder (or empty) region but the backend expects a real one.
	UpstreamRegion string

	// AllowedSourceEndpoint is the Host header incoming requests must carry,
	// and therefore the host their signature has to cover.
	AllowedSourceEndpoint string

	// AllowedSourceSubnet lists the networks requests may come from.
	AllowedSourceSubnet []*net.IPNet

	// Policy is the authorization model in force.
	Policy PolicySource

	// Pepper keys the HMAC that derives a client's secret. It comes from the
	// environment or a mounted secret, never from the policy file.
	Pepper []byte

	// UpstreamSigner re-signs requests with the credentials only this
	// process holds. Clients never see them.
	UpstreamSigner *sigv4.Signer

	// MaxClockSkew bounds how far an X-Amz-Date may be from this clock in
	// either direction.
	MaxClockSkew time.Duration

	// Limits bound the bodies that cannot stream.
	Limits Limits

	// TenantMetricLabel adds the tenant id as a Prometheus label. Off by
	// default: tenant count is unbounded and each one would mint a new time
	// series.
	TenantMetricLabel bool
}

// Handler is a multi-tenant S3 reverse proxy. Every request carries its own
// identity: the access-key id names the tenant and the access level, the
// secret is derived from a deployment-wide pepper, and the tenant's key
// prefix is injected by the proxy rather than sent by the client. Nothing
// tenant-specific is configured anywhere, so onboarding a tenant is a no-op
// here.
type Handler struct {
	cfg      Config
	verifier *sigv4.Verifier
	access   *observability.Recorder

	// proxy is built once and shared: it carries the connection pool, so
	// re-creating it per request would throw away every keep-alive.
	proxy *httputil.ReverseProxy
}

// New validates the configuration and wires up the shared reverse proxy.
//
// The transport keeps a generous idle-connection pool: Go's default of two
// per host would force a fresh TCP (and TLS) handshake on most requests,
// which alone would blow the added-latency budget.
func New(cfg Config) (*Handler, error) {
	if cfg.Policy == nil {
		return nil, errors.New("proxy: no policy source configured")
	}
	if len(cfg.Pepper) == 0 {
		return nil, errors.New("proxy: no credential pepper configured")
	}
	if cfg.UpstreamSigner == nil {
		return nil, errors.New("proxy: no upstream signer configured")
	}

	transport := http.DefaultTransport.(*http.Transport).Clone()
	transport.MaxIdleConns = 512
	transport.MaxIdleConnsPerHost = 128
	transport.IdleConnTimeout = 90 * time.Second

	h := &Handler{
		cfg: cfg,
		verifier: &sigv4.Verifier{
			Host:         cfg.AllowedSourceEndpoint,
			MaxClockSkew: cfg.MaxClockSkew,
		},
		access: &observability.Recorder{TenantLabel: cfg.TenantMetricLabel},
	}
	h.proxy = &httputil.ReverseProxy{
		// The request handed to ServeHTTP already carries the absolute
		// upstream URL, so there is nothing left to direct.
		Director:       func(*http.Request) {},
		Transport:      transport,
		FlushInterval:  -1, // flush every write: object downloads stream
		ModifyResponse: rewriteUpstreamResponse,
		ErrorHandler: func(w http.ResponseWriter, r *http.Request, err error) {
			log.WithError(err).Warn("upstream request failed")
			s3.WriteError(w, http.StatusBadGateway, "InternalError", "The proxy could not reach the object store.")
		},
	}
	return h, nil
}

// UpstreamAddr is the endpoint this handler forwards to, for a caller that
// has to report it at startup. Empty means "auto-detect from the region".
func (h *Handler) UpstreamAddr() (scheme, endpoint string) {
	return h.cfg.UpstreamScheme, h.cfg.UpstreamEndpoint
}

func (h *Handler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	start := time.Now()
	rec := observability.NewStatusRecorder(w)
	entry := &observability.AccessLog{Method: r.Method, Path: r.URL.Path}

	proxyReq, st, err := h.prepare(rec, r, entry)
	if err != nil || proxyReq == nil {
		h.access.Finish(entry, rec.Status(), time.Since(start))
		return
	}

	h.proxy.ServeHTTP(rec, proxyReq.WithContext(withRequestState(r.Context(), st)))
	h.access.Finish(entry, rec.Status(), time.Since(start))
}

// prepare runs the whole per-request lifecycle up to the point where the
// request is ready to be forwarded: verify, resolve, inject, authorize,
// re-sign. It returns (nil, nil, nil) when it has already answered the
// request itself — a rejection, or a batch delete in which policy refused
// every key.
func (h *Handler) prepare(w http.ResponseWriter, r *http.Request, entry *observability.AccessLog) (*http.Request, *requestState, error) {
	if err := h.validateSourceIP(r); err != nil {
		h.reject(w, entry, http.StatusForbidden, "AccessDenied", err)
		return nil, nil, err
	}

	// Classify before authenticating: an operation this proxy does not
	// implement is refused whether or not the caller holds a valid
	// credential, so an unsupported call can never fall through to the
	// upstream by accident.
	op, err := s3.Classify(r)
	if err != nil {
		status := http.StatusForbidden
		if errors.Is(err, s3.ErrInvalidKey) {
			status = http.StatusBadRequest
		}
		h.reject(w, entry, status, "AccessDenied", err)
		return nil, nil, err
	}
	entry.Operation = string(op.Kind)
	entry.Key = op.Key
	if op.Kind == s3.ListObjects {
		entry.Key = op.ListPrefix
	}

	if h.cfg.ReadOnly && !isReadMethod(r.Method) {
		err := fmt.Errorf("proxy is in read-only mode")
		h.reject(w, entry, http.StatusForbidden, "AccessDenied", err)
		return nil, nil, err
	}

	current := h.cfg.Policy.Current()
	identity, region, err := h.authenticate(current, r)
	if err != nil {
		h.reject(w, entry, http.StatusForbidden, "AccessDenied", err)
		return nil, nil, err
	}
	entry.Tenant, entry.Level = identity.Tenant, identity.Level

	st := &requestState{
		policy:         current,
		identity:       identity,
		operation:      op,
		clientRegion:   region,
		maxRewriteSize: h.cfg.Limits.RewriteBody,
	}

	// A batch delete is authorized key by key inside prepareBody, because
	// its keys live in the request body. Everything else has exactly one
	// subject: the object key, or the prefix of a listing.
	if op.Kind != s3.DeleteObjects {
		subject := op.Key
		if op.Kind == s3.ListObjects {
			subject = op.ListPrefix
		}
		decision := current.Authorize(identity.Level, subject, r.Method)
		entry.Rule, entry.Allowed = decision.Rule, decision.Allowed
		if !decision.Allowed {
			err := fmt.Errorf("policy denies %s on %q for level %q", r.Method, subject, identity.Level)
			h.reject(w, entry, http.StatusForbidden, "AccessDenied", err)
			return nil, nil, err
		}
	}

	proxyReq, err := h.buildUpstreamRequest(r, st)
	if op.Kind == s3.DeleteObjects {
		// A batch delete is authorized inside the body preparation, so its
		// outcome only becomes known here.
		entry.Rule = st.rule
		entry.DeniedKeys = len(st.deniedDeletes)
		entry.Allowed = err == nil
	}
	if err != nil {
		if errors.Is(err, errAllKeysDenied) {
			// Nothing survived authorization, so there is no upstream call
			// to make — answer with the per-key AccessDenied entries S3
			// would have returned.
			entry.Reason = err.Error()
			s3.WriteDeleteResult(w, st.deniedDeletes)
			return nil, nil, nil
		}
		status := http.StatusBadRequest
		code := "InvalidRequest"
		if errors.Is(err, s3.ErrUnsupportedOperation) {
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
func (h *Handler) reject(w http.ResponseWriter, entry *observability.AccessLog, status int, code string, err error) {
	entry.Allowed = false
	entry.Reason = err.Error()
	message := "Access Denied."
	if h.cfg.Debug {
		message = err.Error()
	}
	s3.WriteError(w, status, code, message)
}

// authenticate verifies the inbound SigV4 header signature and resolves the
// identity behind it.
//
// The secret is never stored: it is recomputed from the pepper for this one
// access-key id, the client's canonical request is reconstructed with it, and
// the two Authorization headers are compared in constant time. An anonymous
// request has no Authorization header and fails at the first step; an unknown
// access-key id fails at resolution; a forged signature fails at the
// comparison. All three are the same 403 to the client.
func (h *Handler) authenticate(current *policy.Policy, r *http.Request) (*policy.Identity, string, error) {
	cred, err := h.verifier.ReadCredential(r)
	if err != nil {
		return nil, "", err
	}
	identity, err := current.ResolveIdentity(cred.AccessKeyID, h.cfg.Pepper)
	if err != nil {
		return nil, "", err
	}
	if err := h.verifier.Verify(r, cred, identity.SecretAccessKey); err != nil {
		return nil, "", err
	}
	return identity, cred.Region, nil
}

func (h *Handler) validateSourceIP(req *http.Request) error {
	ip, _, _ := net.SplitHostPort(req.RemoteAddr)
	userIP := net.ParseIP(ip)
	for _, subnet := range h.cfg.AllowedSourceSubnet {
		if subnet.Contains(userIP) {
			return nil
		}
	}
	return fmt.Errorf("source IP %s not allowed", ip)
}

// isReadMethod reports whether an HTTP method is a non-mutating S3 read.
func isReadMethod(method string) bool {
	return method == http.MethodGet || method == http.MethodHead
}

// requestState is everything the response side needs to know about a request
// it is answering. It travels on the upstream request's context so that one
// shared ReverseProxy can serve every tenant.
type requestState struct {
	policy    *policy.Policy
	identity  *policy.Identity
	operation *s3.Operation

	// deniedDeletes are the batch-delete keys (client-facing) policy
	// refused. They are merged back into the upstream response as per-key
	// AccessDenied entries, which is what S3 itself does for a key the
	// caller may not delete.
	deniedDeletes []string

	// rule is the policy rule a body-level authorization matched, for the
	// access log — the URL-level path records it directly on the entry.
	rule string

	// clientRegion is the region scope the client signed with. It is only
	// used when no upstream region is configured, and it may legitimately be
	// empty (DuckDB's httpfs signs that way).
	clientRegion string

	// maxRewriteSize mirrors Limits.RewriteBody so the response rewrite
	// needs nothing but this struct.
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
