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
	"sync/atomic"
	"time"

	"github.com/Kriechi/aws-s3-reverse-proxy/internal/listcache"
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
	capture := h.cfg.ListFallback.Begin(key, scope, req.URL.String())

	if remembered == nil {
		// With nothing to fall back on, there is nothing to gain from giving
		// up early: the request waits for the object store like any other.
		answer := h.fetchListing(req, st.maxRewriteSize)
		answer.settle(capture)
		return answer.resp, answer.err
	}

	// The listing may outlive this request — see finishInBackground — so it
	// runs on the background work's context rather than the client's.
	ctx, cancel := context.WithTimeout(h.listingWork.ctx, listingWorkTimeout)
	answers := make(chan listingAnswer, 1)
	go func() { answers <- h.fetchListing(req.WithContext(ctx), st.maxRewriteSize) }()
	timer := time.NewTimer(h.cfg.ListFallbackTimeout)
	defer timer.Stop()

	select {
	case answer := <-answers:
		cancel()
		answer.settle(capture)
		// A listing too large to rewrite is not an outage: the object store
		// did answer that one.
		if !answer.failed() || errors.Is(answer.err, errListingTooLarge) {
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
	case <-req.Context().Done():
		// A client that went away is not an outage. The object store may
		// still be about to answer, and that answer is worth keeping.
		h.finishInBackground(capture, answers, cancel)
		return nil, req.Context().Err()
	case <-timer.C:
		h.finishInBackground(capture, answers, cancel)
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

// finishInBackground lets a listing the client stopped waiting for run to the
// end, and remembers its answer.
//
// Without it, an object store that is healthy but slower than the deadline
// would never update what is remembered: every listing would be cut off and
// answered from memory, until the remembered answer was too old to serve. With
// it, each listing leaves the object store's current answer for the next one.
// When there is no slot for it — a hang leaves many listings waiting at once —
// it is cancelled, which is what happened to all of them before.
func (h *Handler) finishInBackground(capture *listcache.Capture, answers <-chan listingAnswer, cancel context.CancelFunc) {
	if !h.listingWork.tryStart() {
		cancel()
		capture.Abort()
		return
	}
	go func() {
		defer h.listingWork.done()
		defer cancel()
		answer := <-answers
		answer.settle(capture)
	}()
}

// failed reports whether the object store gave no answer worth passing on: an
// error, a server error, or a refusal that only says it is overloaded or slow
// right now. Those are what the fallback answers in its place, and none of
// them says anything about the listing remembered.
func (a listingAnswer) failed() bool {
	if a.err != nil {
		return true
	}
	switch status := a.resp.StatusCode; {
	case status >= 500, status == http.StatusTooManyRequests, status == http.StatusRequestTimeout:
		return true
	}
	return false
}

// isListing reports whether the object store answered with a listing.
func (a listingAnswer) isListing() bool {
	return a.err == nil && a.resp.StatusCode < 300
}

// settle records an answer in the fallback: a listing is remembered, a
// refusal forgets what was remembered, and a failure leaves it alone.
func (a listingAnswer) settle(capture *listcache.Capture) {
	switch {
	case a.isListing():
		capture.Commit(listingHeaders(a.resp.Header), a.body)
	case a.failed():
		capture.Abort()
	default:
		capture.Discard()
	}
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

// endListingWrite releases a write's claim on the listing fallback, and has
// what it dropped — now, or earlier — asked for again once writes have
// settled. An unresolved write has nothing asked for again: the object store
// may not have applied it yet.
func (h *Handler) endListingWrite(w *listcache.Write, outcome listcache.Outcome, dropped ...listcache.Stale) {
	stale := w.End(outcome)
	if outcome == listcache.Answered {
		stale = append(stale, dropped...)
	}
	h.listingRefresh.schedule(stale)
}

// writeOutcome is what an upstream answer to a write says about it. A 4xx is
// the object store refusing it; a 5xx may have come after the object changed,
// and anything else means it did.
func writeOutcome(status int) listcache.Outcome {
	if status >= 400 && status < 500 {
		return listcache.Refused
	}
	return listcache.Answered
}

// settleWrite records what became of this request's write, for the claim
// ServeHTTP releases. Until it is called the write counts as unresolved.
func (st *requestState) settleWrite(outcome listcache.Outcome) {
	st.writeOutcome, st.writeSettled = outcome, true
}

// listingWriteOutcome is what became of this request's write, as far as it
// is known when the request is over.
//
// A write whose body never reached its end was not applied, whatever else
// went wrong: an object store cannot store an object, or complete an upload,
// from part of the request.
func (st *requestState) listingWriteOutcome() listcache.Outcome {
	if st.writeSettled {
		return st.writeOutcome
	}
	if st.bodyTracked && !st.bodySent.Load() {
		return listcache.Refused
	}
	return listcache.Unresolved
}

// trackBodySent records on st when the upstream request's body has been read
// to its end — the earliest point at which the object store can have all of
// it. A body that is empty is not tracked: there is nothing it could be
// missing.
//
// GetBody is wrapped the same way, so a retry on a fresh connection is
// tracked as well, and stays possible at all.
func trackBodySent(req *http.Request, st *requestState) {
	if req.Body == nil || req.Body == http.NoBody || req.ContentLength <= 0 {
		return
	}
	st.bodyTracked = true
	req.Body = &sentTracker{ReadCloser: req.Body, sent: &st.bodySent}
	if getBody := req.GetBody; getBody != nil {
		req.GetBody = func() (io.ReadCloser, error) {
			body, err := getBody()
			if err != nil {
				return nil, err
			}
			return &sentTracker{ReadCloser: body, sent: &st.bodySent}, nil
		}
	}
}

// sentTracker marks a request body as sent once it has been read to EOF.
type sentTracker struct {
	io.ReadCloser
	sent *atomic.Bool
}

func (t *sentTracker) Read(p []byte) (int, error) {
	n, err := t.ReadCloser.Read(p)
	if err == io.EOF {
		t.sent.Store(true)
	}
	return n, err
}

// neverSent reports whether an upstream request failed before the object
// store could have received it: the connection was never made.
func neverSent(err error) bool {
	var opErr *net.OpError
	return errors.As(err, &opErr) && opErr.Op == "dial"
}
