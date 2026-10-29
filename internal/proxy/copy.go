package proxy

// CopyObject, carried out by the proxy as a streamed GET of the source and a
// PUT of the destination. A native copy is never forwarded: under the
// proxy's SSE-C key it would need the copy-source key headers, which not
// every store supports (Hetzner does not), and one path then serves
// deployments with and without a key alike. The GET is an ordinary object
// read — decrypted under the key, legacy plaintext served and scheduled for
// repair like any other read — and the PUT stores the bytes under the key.

import (
	"io"
	"net/http"
	"net/url"

	"github.com/Kriechi/aws-s3-reverse-proxy/internal/listcache"
	"github.com/Kriechi/aws-s3-reverse-proxy/internal/s3"
	"github.com/Kriechi/aws-s3-reverse-proxy/internal/sigv4"
)

// maxCopySize is S3's limit for both a single PutObject and a CopyObject
// source; a larger object would need a multipart copy.
const maxCopySize = 5 << 30

// serveCopy answers a CopyObject. proxyReq is the request prepare built for
// the destination, carrying the client's context; only its scoped URL is used.
func (h *Handler) serveCopy(w http.ResponseWriter, proxyReq *http.Request, st *requestState) {
	ctx := proxyReq.Context()
	target := *proxyReq.URL
	target.RawQuery = ""
	logger := h.objectLog(st)

	// Nothing can have changed the destination until its PUT is sent.
	st.settleWrite(listcache.Refused)

	source, err := h.readCopySource(proxyReq, &target, st)
	if err != nil {
		logger.WithError(err).Warn("reading the source of a copy failed")
		s3.WriteError(w, http.StatusBadGateway, "InternalError", "The proxy could not read the copy source.")
		return
	}
	defer drain(source)
	switch {
	case source.StatusCode == http.StatusNotFound:
		s3.WriteError(w, http.StatusNotFound, "NoSuchKey", "The specified key does not exist.")
		return
	case source.StatusCode != http.StatusOK:
		h.relayError(w, source, st)
		return
	case source.ContentLength < 0:
		logger.Warn("the object store answered the source of a copy without a length")
		s3.WriteError(w, http.StatusBadGateway, "InternalError", "The proxy could not read the copy source.")
		return
	case source.ContentLength > maxCopySize:
		s3.WriteError(w, http.StatusBadRequest, "InvalidRequest",
			"The specified copy source is larger than the maximum allowable size for a copy source: 5368709120")
		return
	}

	header := objectHeaders(source.Header)
	st.uploadHeader = s3.CacheableResponseHeaders(header)
	h.applyCustomerKey(header, s3.PutObject)
	var body io.Reader
	if source.ContentLength > 0 {
		body = source.Body
		if capture := h.captureUpload(st, source.ContentLength); capture != nil {
			body = io.TeeReader(body, capture)
		}
	}
	st.writeSettled = false
	// UNSIGNED-PAYLOAD, as for a de-chunked upload: hashing the body first
	// would mean holding all of it.
	resp, err := h.send(ctx, http.MethodPut, &target, "", body, source.ContentLength, sigv4.UnsignedPayload, header)
	if err != nil {
		if neverSent(err) {
			st.settleWrite(listcache.Refused)
		}
		logger.WithError(err).Warn("writing the destination of a copy failed")
		s3.WriteError(w, http.StatusBadGateway, "InternalError", "The proxy could not write the copy.")
		return
	}
	st.settleWrite(writeOutcome(resp.StatusCode))
	if resp.StatusCode != http.StatusOK {
		h.relayError(w, resp, st)
		return
	}
	drain(resp)
	h.checkWriteConfirmation(resp, st)
	h.finishMutation(resp, st)
	st.mutation.End()
	h.endListingWrite(st.listingWrite, listcache.Answered)
	s3.WriteCopyObjectResult(w, s3.NormalizeETag(resp.Header.Get("Etag")), resp.Header.Get("Date"))
}

// readCopySource sends the GET of a copy's source through the same transport
// as a client's read, as a read of that key: the tenant prefix injected by
// scopeToTenant, the customer key applied, and the plaintext handling and
// repair bound to the source rather than the destination.
func (h *Handler) readCopySource(proxyReq *http.Request, target *url.URL, st *requestState) (*http.Response, error) {
	op := st.operation
	srcSt := &requestState{
		policy:   st.policy,
		identity: st.identity,
		operation: &s3.Operation{
			Kind:       s3.GetObject,
			Bucket:     op.Bucket,
			Key:        op.SourceKey,
			EscapedKey: (&url.URL{Path: op.SourceKey}).EscapedPath(),
		},
		maxRewriteSize: st.maxRewriteSize,
	}
	if h.cfg.Cache != nil {
		srcSt.cacheKey = cacheKeyFor(srcSt)
	}
	source := *target
	scopeToTenant(&source, st.identity.KeyPrefix, srcSt.operation)

	// Identity, so that Go's transport does not ask for gzip and then
	// decompress an object stored gzip-encoded: the copy is of its bytes.
	header := http.Header{"Accept-Encoding": {"identity"}}
	h.applyCustomerKey(header, s3.GetObject)
	req, err := h.upstreamRequest(withRequestState(proxyReq.Context(), srcSt), http.MethodGet, &source, "", nil, 0, sigv4.EmptyPayloadSHA256, header)
	if err != nil {
		return nil, err
	}
	return h.roundTrip(req)
}
