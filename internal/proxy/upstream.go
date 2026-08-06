package proxy

import (
	"fmt"
	"net/http"
	"net/url"
	"time"

	"github.com/Kriechi/aws-s3-reverse-proxy/internal/s3"
)

// buildUpstreamRequest injects the tenant prefix, prepares the body and
// re-signs the request with the upstream credentials.
func (h *Handler) buildUpstreamRequest(req *http.Request, st *requestState) (*http.Request, error) {
	region := h.cfg.UpstreamRegion
	endpoint := h.cfg.UpstreamEndpoint
	if endpoint == "" {
		// No configured endpoint: address AWS S3 for the region this
		// deployment signs for, as the single-tenant proxy always did.
		endpoint = fmt.Sprintf("s3.%s.amazonaws.com", region)
	}

	proxyURL := *req.URL
	proxyURL.Scheme = h.cfg.UpstreamScheme
	proxyURL.Host = endpoint
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

	// Client headers are copied BEFORE signing, so everything forwarded is
	// also covered by the upstream signature. Copying them afterwards would
	// leave `SignedHeaders` at `host;x-amz-content-sha256;x-amz-date` while
	// `x-amz-` headers travelled alongside it: AWS S3 refuses that outright,
	// and a store that does not (MinIO, Ceph, R2) would act on a header no
	// signature, no classification and no policy rule ever covered.
	//
	// Which headers those are is s3.ForwardableHeader's business — the
	// whitelist lives next to the operation whitelist rather than here.
	for name, values := range req.Header {
		if s3.ForwardableHeader(name) {
			proxyReq.Header[http.CanonicalHeaderKey(name)] = values
		}
	}
	if st.operation.RewritesXML {
		// This response has to be read to strip the tenant prefix back out of
		// it. Forwarding the client's Accept-Encoding would let the upstream
		// compress it *and* stop Go's transport from undoing that
		// automatically — leaving a body we would have to pass through
		// unrewritten. Dropping the header hands the negotiation to the
		// transport, which decompresses transparently.
		proxyReq.Header.Del("Accept-Encoding")
	}
	// The body preparation has the last word: it rebuilt or de-framed the
	// body, so its digests describe what actually goes upstream.
	for name, values := range body.extraHeaders {
		proxyReq.Header[name] = values
	}
	// Set before signing so the signer adopts this digest instead of reading
	// the body to compute one — that read is what would otherwise pull an
	// entire upload into memory.
	proxyReq.Header.Set("X-Amz-Content-Sha256", payloadHash)

	if err := h.cfg.UpstreamSigner.Sign(proxyReq, region, time.Now()); err != nil {
		return nil, err
	}

	return proxyReq, nil
}

// scopeToTenant is the whole isolation mechanism: the tenant's key prefix is
// *injected* here, never validated against something the client sent. A
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
// precisely because the client could not have obtained a token for any other
// scope.
func scopeToTenant(u *url.URL, keyPrefix string, op *s3.Operation) {
	if op.IsBucketLevel() {
		if op.Kind != s3.ListObjects {
			return
		}
		q := u.Query()
		q.Set("prefix", keyPrefix+op.ListPrefix)
		for _, param := range []string{"marker", "start-after"} {
			if v := q.Get(param); v != "" {
				q.Set(param, keyPrefix+v)
			}
		}
		u.RawQuery = q.Encode()
		return
	}
	// The key prefix is restricted to characters that need no escaping (see
	// policy's validateRenderedKeyPrefix), so the decoded and the as-written
	// path stay in agreement.
	u.Path = "/" + op.Bucket + "/" + keyPrefix + op.Key
	u.RawPath = "/" + op.Bucket + "/" + keyPrefix + op.EscapedKey
}
