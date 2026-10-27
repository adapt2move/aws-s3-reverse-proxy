package proxy

import (
	"bytes"
	"io"
	"net/http"
	"strconv"
	"strings"

	"github.com/Kriechi/aws-s3-reverse-proxy/internal/s3"
)

// modifyResponse is everything that happens to an upstream response before it
// reaches the client: the tenant rewriting below, and — when a cache is
// configured — recording what the response says about the object.
//
// The order is deliberate. A write's outcome is recorded first, because
// whether the upload was accepted decides what the cache should hold. The
// read capture comes next, so that what is stored is the upstream's response
// and not a rewritten one: a listing filtered for one access level, or a body
// with the tenant prefix stripped out, is an answer to one request rather
// than a copy of an object. Rewriting comes last, because it is what the
// client is owed and nothing after it needs the original.
//
// Capturing before rewriting rather than after is what makes that a property
// of this function instead of a coincidence. It costs nothing — a capture
// only ever acts on a 200 or a 206, which are exactly the responses the
// rewrite passes through untouched — and without it, anyone who later teaches
// the rewrite to touch object reads would start caching rewritten bodies with
// no sign that anything had changed.
func (h *Handler) modifyResponse(resp *http.Response) error {
	if resp == nil || resp.Request == nil {
		return nil
	}
	st := requestStateFrom(resp.Request.Context())
	if st == nil {
		normalizeResponseETag(resp.Header)
		hideEncryption(resp.Header)
		if err := h.sanitizeErrorResponse(resp); err != nil {
			return err
		}
		return rewriteUpstreamResponse(resp)
	}
	h.checkWriteConfirmation(resp, st)
	h.finishMutation(resp, st)
	// For most writes the outcome is known and recorded once the headers are
	// in, so the claim on the key can go now, before the client sees a byte
	// of the answer. Releasing it any later leaves a window: the reverse
	// proxy flushes headers immediately, so a client can read the object it
	// just wrote while the claim is still held — and be told the cache had to
	// step aside.
	//
	// A multipart completion is the exception. AWS S3 answers it 200 at
	// once and keeps the connection open, streaming whitespace, until the
	// object is assembled; the result (or an error) is in the body. Its
	// headers say nothing about whether the new object exists yet, so its
	// claim is held until the whole answer is through — ServeHTTP's deferred
	// End, which also covers every request that never gets a response.
	if st.operation.Kind != s3.CompleteMultipartUpload {
		st.mutation.End()
		st.listingWrite.End(writeRefused(resp.StatusCode))
	} else if h.cfg.ListFallback != nil && !writeRefused(resp.StatusCode) {
		// The completion's body tells the client the object exists before
		// ServeHTTP gets to end the claim, so the listings that would not
		// show it go now. The claim itself stays: a listing answered while
		// the object is still being assembled must not be remembered.
		h.cfg.ListFallback.Invalidate(cacheKeyFor(st))
	}
	h.captureRead(resp, st)
	// After the capture, which keeps the plaintext marker in the cache, and
	// before anything reaches the client, which must see neither the marker
	// nor which customer key the store confirmed.
	normalizeResponseETag(resp.Header)
	hideEncryption(resp.Header)
	if err := h.sanitizeErrorResponse(resp); err != nil {
		return err
	}
	if err := rewriteUpstreamResponse(resp); err != nil {
		return err
	}
	if st.cacheResult != "" {
		// The client asked for something the cache could have answered and
		// did not. Saying so is what makes a hit rate debuggable from one
		// request rather than only from a dashboard.
		resp.Header.Set("X-Cache", strings.ToUpper(st.cacheResult))
	}
	return nil
}

// normalizeResponseETag puts a response's ETag into standard entity-tag
// syntax, so a client is never handed a validator — `"'abc'"` — that the
// object store would refuse to take back.
func normalizeResponseETag(header http.Header) {
	if etag := header.Get("Etag"); etag != "" {
		header.Set("Etag", s3.NormalizeETag(etag))
	}
}

// sanitizeErrorResponse rebuilds an upstream error body from the fields a
// client may see, and redacts the customer key from whatever is left.
//
// An object store's error describes the request *it* received, which is the
// proxy's re-signed request: SignatureDoesNotMatch quotes it whole, headers
// and all, and an argument error can repeat a header's value. With a customer
// key on that request, passing the document on would hand the key to any
// client that can provoke the error. Rebuilding from Code, Message, Key,
// BucketName, Resource and the request ids keeps everything an SDK reads;
// redacting afterwards covers a body that is not an S3 error document, and a
// message that quotes a value. Error bodies are small, and every one of them
// is read here regardless of its content type.
func (h *Handler) sanitizeErrorResponse(resp *http.Response) error {
	if resp.StatusCode < 300 || resp.StatusCode == http.StatusNotModified || resp.Body == nil || resp.Body == http.NoBody {
		return nil
	}
	body, err := s3.ReadCapped(resp.Body, h.cfg.Limits.RewriteBody, "upstream error response")
	resp.Body.Close()
	if err != nil {
		return err
	}
	body, err = s3.DecodeContentEncoding(body, resp.Header.Get("Content-Encoding"), h.cfg.Limits.RewriteBody)
	if err != nil {
		return err
	}
	resp.Header.Del("Content-Encoding")
	body = h.sanitizeErrorBody(body)
	resp.Body = io.NopCloser(bytes.NewReader(body))
	resp.ContentLength = int64(len(body))
	resp.Header.Set("Content-Length", strconv.Itoa(len(body)))
	return nil
}

// sanitizeErrorBody is sanitizeErrorResponse for a body already in hand.
func (h *Handler) sanitizeErrorBody(body []byte) []byte {
	body = s3.SanitizeErrorDocument(body)
	if h.cfg.UpstreamSSECKey != nil {
		body = h.cfg.UpstreamSSECKey.Redact(body)
	}
	return body
}

// rewriteUpstreamResponse is the response half of the tenant scoping: it
// strips the injected key prefix back out, hides listing entries no rule
// grants the caller read on, and merges the per-key denials of a batch delete
// into the upstream's result.
//
// Only responses that are known to embed object keys are buffered — listings,
// batch deletes, multipart results — plus XML error bodies, which name the
// request path. An object payload is never touched, so a GetObject of a 10
// GiB file still streams even when it happens to be XML.
func rewriteUpstreamResponse(resp *http.Response) error {
	if resp == nil || resp.Request == nil {
		return nil
	}
	st := requestStateFrom(resp.Request.Context())
	if st == nil {
		return nil
	}
	if !st.operation.RewritesXML && resp.StatusCode < 300 {
		return nil
	}
	if resp.Body == nil {
		return nil
	}
	if !strings.Contains(resp.Header.Get("Content-Type"), "xml") {
		return nil
	}

	body, err := s3.ReadCapped(resp.Body, st.maxRewriteSize, "upstream response")
	resp.Body.Close()
	if err != nil {
		return err
	}
	// The upstream is asked not to compress the responses we have to read
	// (see buildUpstreamRequest), but an object store that compresses
	// regardless must not turn into a silent pass-through: the body would
	// reach the client still carrying the tenant prefix, and a batch delete
	// would lose its per-key denials.
	body, err = s3.DecodeContentEncoding(body, resp.Header.Get("Content-Encoding"), st.maxRewriteSize)
	if err != nil {
		return err
	}
	resp.Header.Del("Content-Encoding")

	body = s3.StripKeyPrefix(body, st.identity.KeyPrefix)
	if st.operation.Kind == s3.ListObjects {
		// A listing is already authorized on its own prefix, so this only
		// ever removes what a denial *below* that prefix covers — the paths
		// inside the tenant's scope that no rule mentions at all, and the
		// ones a rule denies this level outright. Without it, "denied" would
		// hold for reads but not for the listing that reveals them.
		body = s3.FilterListEntries(body, func(key string) bool {
			return st.policy.Authorize(st.identity.Level, key, http.MethodGet).Allowed
		})
	}
	if st.operation.Kind == s3.DeleteObjects {
		body = s3.MergeDeniedDeletes(body, st.deniedDeletes)
	}
	// A listing, a part list and a multipart result all carry ETags, and a
	// client may well send them back — as a precondition, or in a part list.
	// They get the same normalization the ETag header does.
	body = s3.NormalizeXMLETags(body)

	resp.Body = io.NopCloser(bytes.NewReader(body))
	resp.ContentLength = int64(len(body))
	resp.Header.Set("Content-Length", strconv.Itoa(len(body)))
	return nil
}
