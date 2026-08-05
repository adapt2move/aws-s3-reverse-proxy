package proxy

import (
	"bytes"
	"io"
	"net/http"
	"strconv"
	"strings"

	"github.com/Kriechi/aws-s3-reverse-proxy/internal/s3"
)

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
		// ever removes what the implicit deny at the end of the rule list
		// covers — the paths inside the tenant's scope that no rule mentions
		// at all. Without it, "not matched by a rule is denied" would hold
		// for reads but not for the listing that reveals them.
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
