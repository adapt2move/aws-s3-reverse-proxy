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
	if region == "" {
		region = st.clientRegion
	}
	endpoint := h.cfg.UpstreamEndpoint
	if endpoint == "" {
		// No configured endpoint: fall back to AWS S3 for the region the
		// request is signed for, as the single-tenant proxy always did.
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
	if val, ok := req.Header["Content-Type"]; ok {
		proxyReq.Header["Content-Type"] = val
	}
	if val, ok := req.Header["Content-Md5"]; ok {
		proxyReq.Header["Content-Md5"] = val
	}
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

	// Add origin headers after the request is signed (no overwrite).
	copyHeaderWithoutOverwrite(proxyReq.Header, req.Header)
	if st.operation.RewritesXML {
		// This response has to be read to strip the tenant prefix back out
		// of it. Forwarding the client's Accept-Encoding would let the
		// upstream compress it *and* stop Go's transport from undoing that
		// automatically — leaving a body we would have to pass through
		// unrewritten. Dropping the header hands the negotiation to the
		// transport, which decompresses transparently.
		proxyReq.Header.Del("Accept-Encoding")
	}
	// The client's own credential must never travel upstream — the upstream
	// signature replaced it.
	proxyReq.Header.Del("X-Amz-Decoded-Content-Length")

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

func copyHeaderWithoutOverwrite(dst http.Header, src http.Header) {
	for k, v := range src {
		if _, ok := dst[k]; !ok {
			for _, vv := range v {
				dst.Add(k, vv)
			}
		}
	}
}
