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
		return rewriteUpstreamResponse(resp)
	}
	h.finishMutation(resp, st)
	h.captureRead(resp, st)
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

	resp.Body = io.NopCloser(bytes.NewReader(body))
	resp.ContentLength = int64(len(body))
	resp.Header.Set("Content-Length", strconv.Itoa(len(body)))
	return nil
}
