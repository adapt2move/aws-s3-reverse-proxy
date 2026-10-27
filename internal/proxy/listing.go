package proxy

// The listing fallback, seen from the request lifecycle.
//
// A listing always goes to the object store. What changes with a fallback
// configured is what happens when the object store does not answer it: within
// ListFallbackTimeout, or with anything but a server error. Then the last
// answer it gave to the same listing is served instead, if there is one young
// enough — and if there is none, the request goes on exactly as it would have
// without the fallback, waiting as long as the object store takes.
//
// The rules of the object cache hold here too, and two of them are what make
// a remembered listing safe to serve at all:
//
//   - It is consulted after authentication and authorization, like every
//     other answer, so it can only answer a listing that was already allowed.
//   - What is remembered is the object store's own response, before the
//     tenant prefix is stripped out of it and before the entries the caller's
//     level may not read are filtered out. A remembered listing goes through
//     the same rewrite a live one does, with the policy in force when it is
//     served. One level's answer is never served to another.
//
// The key is the upstream request — bucket plus the query after the tenant
// prefix was injected into it — so two tenants listing the same prefix are
// two keys by construction. What invalidates it is any write under the
// upstream prefix it was listed with, see internal/listcache.

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"strconv"
	"time"

	"github.com/Kriechi/aws-s3-reverse-proxy/internal/s3"
)

// listingFallback is the X-Cache value and access-log result of a listing
// answered from memory because the object store did not answer it.
const listingFallback = "fallback"

// forward is the reverse proxy's transport: listings take the fallback path
// when there is one, everything else goes straight to roundTrip.
func (h *Handler) forward(req *http.Request) (*http.Response, error) {
	if h.cfg.ListFallback != nil {
		if st := requestStateFrom(req.Context()); st != nil && st.operation.Kind == s3.ListObjects {
			return h.forwardListing(req, st)
		}
	}
	return h.roundTrip(req)
}

// listingAnswer is what came back for a listing, body already read.
type listingAnswer struct {
	resp *http.Response
	body []byte
	err  error
}

// forwardListing sends a listing upstream, remembers a successful answer, and
// falls back to the remembered one when the object store does not answer.
//
// The deadline covers the whole answer, body included, not just its headers:
// the body is read here anyway — the rewrite has to have all of it — and a
// listing whose headers arrive on time and whose body never does has not been
// answered either.
func (h *Handler) forwardListing(req *http.Request, st *requestState) (*http.Response, error) {
	key, scope := listingKeyFor(req.URL, st), listingScopeFor(st)
	remembered := h.cfg.ListFallback.Get(key)
	capture := h.cfg.ListFallback.Begin(key, scope)

	ctx, cancel := context.WithCancel(req.Context())
	defer cancel()
	answers := make(chan listingAnswer, 1)
	go func() { answers <- h.fetchListing(req.WithContext(ctx), st.maxRewriteSize) }()

	// With nothing to fall back on, there is nothing to gain from giving up
	// early: the request waits for the object store like any other, and a nil
	// channel never fires.
	var deadline <-chan time.Time
	if remembered != nil {
		timer := time.NewTimer(h.cfg.ListFallbackTimeout)
		defer timer.Stop()
		deadline = timer.C
	}

	select {
	case answer := <-answers:
		switch {
		case answer.err == nil && answer.resp.StatusCode < 300:
			capture.Commit(listingHeaders(answer.resp.Header), answer.body)
			return answer.resp, nil
		case answer.err == nil && answer.resp.StatusCode < 500:
			capture.Discard()
			return answer.resp, nil
		}
		capture.Abort()
		// A client that went away is not an outage, and neither is a listing
		// too large to rewrite: the object store did answer that one.
		if remembered == nil || req.Context().Err() != nil || errors.Is(answer.err, errListingTooLarge) {
			return answer.resp, answer.err
		}
		logger := h.objectLog(st)
		reason := "failed"
		if answer.err != nil {
			logger = logger.WithError(answer.err)
		} else {
			reason = fmt.Sprintf("answered %d", answer.resp.StatusCode)
		}
		logger.Warnf("the object store %s a listing; serving the one it gave %s ago",
			reason, remembered.Age.Truncate(time.Second))
	case <-deadline:
		capture.Abort()
		h.objectLog(st).Warnf("the object store did not answer a listing within %s; serving the one it gave %s ago",
			h.cfg.ListFallbackTimeout, remembered.Age.Truncate(time.Second))
	}

	st.cacheResult = listingFallback
	header := remembered.Header
	// How old the answer is, in the standard header for it. Date is left to
	// the server, so it says when this response was sent.
	header.Set("Age", strconv.FormatInt(int64(remembered.Age/time.Second), 10))
	return &http.Response{
		Status:        "200 OK",
		StatusCode:    http.StatusOK,
		Proto:         "HTTP/1.1",
		ProtoMajor:    1,
		ProtoMinor:    1,
		Header:        header,
		Body:          io.NopCloser(bytes.NewReader(remembered.Body)),
		ContentLength: int64(len(remembered.Body)),
		Request:       req,
	}, nil
}

// errListingTooLarge is a listing larger than the rewrite may buffer. It is
// the object store answering, so it is not a reason to fall back.
var errListingTooLarge = errors.New("upstream listing exceeds the rewrite limit")

// fetchListing sends a listing upstream and reads its whole answer.
func (h *Handler) fetchListing(req *http.Request, limit int64) listingAnswer {
	resp, err := h.roundTrip(req)
	if err != nil {
		return listingAnswer{err: err}
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(io.LimitReader(resp.Body, limit+1))
	if err != nil {
		return listingAnswer{err: err}
	}
	if int64(len(body)) > limit {
		return listingAnswer{err: fmt.Errorf("%w: more than %d bytes", errListingTooLarge, limit)}
	}
	resp.Body = io.NopCloser(bytes.NewReader(body))
	resp.ContentLength = int64(len(body))
	resp.Header.Set("Content-Length", strconv.Itoa(len(body)))
	return listingAnswer{resp: resp, body: body}
}

// listingKeyFor names the upstream listing a request is: the bucket, and the
// query after the tenant prefix was injected into it, in canonical order.
//
// The SDKs' `?x-id=ListObjectsV2` marker names nothing and is left out. The
// separator cannot be confused with the query, which is encoded and so never
// contains a literal `?`.
func listingKeyFor(u *url.URL, st *requestState) string {
	query := u.Query()
	query.Del("x-id")
	return st.operation.Bucket + "?" + query.Encode()
}

// listingScopeFor is the prefix every upstream object key a listing
// summarises starts with — spelled like cacheKeyFor spells an object, so that
// a write to any key under it is recognized as one.
func listingScopeFor(st *requestState) string {
	return st.operation.Bucket + "/" + st.identity.KeyPrefix + st.operation.ListPrefix
}

// listingHeaders is the part of a listing response worth remembering: what
// the rewrite needs to read the body. Everything else describes one exchange
// with the object store, which a remembered answer is not.
func listingHeaders(header http.Header) http.Header {
	out := http.Header{}
	for _, name := range []string{"Content-Type", "Content-Encoding"} {
		if v := header.Get(name); v != "" {
			out.Set(name, v)
		}
	}
	return out
}

// beginListingWrite claims object keys in the listing fallback for a write
// about to be sent upstream.
func (h *Handler) beginListingWrite(st *requestState, keys ...string) {
	if h.cfg.ListFallback == nil || len(keys) == 0 {
		return
	}
	st.listingWrite = h.cfg.ListFallback.BeginWrite(keys...)
}

// writeRefused reports whether an upstream answer to a write says the write
// did not happen. A 4xx is the object store refusing it; a 5xx may have come
// after the object changed, and anything else means it did.
func writeRefused(status int) bool {
	return status >= 400 && status < 500
}

// neverSent reports whether an upstream request failed before the object
// store could have received it: the connection was never made.
func neverSent(err error) bool {
	var opErr *net.OpError
	return errors.As(err, &opErr) && opErr.Op == "dial"
}
