package proxy

// Refreshing what a write dropped from the listing fallback.
//
// A write under a prefix drops every remembered listing of it, which is what
// keeps the fallback consistent — and what leaves a prefix that is written to
// often without one. So once the writes have settled, the dropped listings
// are asked for again in the background, and the object store's answer
// becomes the remembered one.
//
// It is the object store's answer and nothing else. A refresh is an ordinary
// listing request, captured like a client's: a write that overlaps it keeps
// it from being remembered, exactly as it would a client's listing. Nothing
// here edits a remembered listing to match a write, because nothing here
// could know what the object store's answer would be.
//
// Three bounds keep it from costing more than it is worth:
//
//   - It waits for writes to settle. A burst of uploads under one prefix
//     costs one listing after the last of them, not one per upload — up to
//     listingRefreshMaxWait after the first, so a prefix written to without
//     pause is still refreshed now and then.
//   - Only listings a client asked for within the fallback's maximum age are
//     refreshed. A refresh does not count as use, so one nobody needs stops
//     being kept current.
//   - At most listingWorkSlots run at a time, together with the listings
//     finished after their client was answered from memory, and one that
//     fails is not retried: an object store that cannot answer a listing now
//     is the outage the fallback is for, and the next client listing that
//     succeeds remembers the answer anyway.

import (
	"context"
	"net/http"
	"net/url"
	"sync"
	"time"

	log "github.com/sirupsen/logrus"

	"github.com/Kriechi/aws-s3-reverse-proxy/internal/listcache"
	"github.com/Kriechi/aws-s3-reverse-proxy/internal/sigv4"
)

const (
	// listingRefreshMaxWait is how long writes that keep arriving can put
	// off a refresh, as a multiple of the settle delay.
	listingRefreshMaxWait = 10

	// listingWorkSlots caps the background listings in flight: refreshes,
	// and listings finished after their client was answered from memory.
	// They are work nobody is waiting for, like the cache's background
	// fills.
	listingWorkSlots = 4

	// listingWorkTimeout bounds one background listing. Nobody is waiting
	// for it, so it can be generous; it is bounded so that a hung object
	// store does not hold a slot forever.
	listingWorkTimeout = 30 * time.Second

	// listingRefreshMaxBackoff caps how far a refresh that keeps being
	// overlapped by writes is pushed back.
	listingRefreshMaxBackoff = time.Minute
)

// listingWork is the background listing the fallback does on its own. Its
// context is what lets Close stop it: during an object store hang, which is
// when there is most of it, waiting it out would keep shutdown waiting too.
type listingWork struct {
	ctx    context.Context
	cancel context.CancelFunc
	slots  chan struct{}

	mu      sync.Mutex
	closed  bool
	running sync.WaitGroup
}

func newListingWork() *listingWork {
	ctx, cancel := context.WithCancel(context.Background())
	return &listingWork{ctx: ctx, cancel: cancel, slots: make(chan struct{}, listingWorkSlots)}
}

// tryStart takes a slot for one piece of background work, and reports false
// when there is none free or the work is shutting down. done gives it back.
func (w *listingWork) tryStart() bool {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.closed {
		return false
	}
	select {
	case w.slots <- struct{}{}:
		w.running.Add(1)
		return true
	default:
		return false
	}
}

func (w *listingWork) done() {
	<-w.slots
	w.running.Done()
}

// close cancels what is running and waits for it to let go.
func (w *listingWork) close() {
	if w == nil {
		return
	}
	w.mu.Lock()
	w.closed = true
	w.mu.Unlock()
	w.cancel()
	w.running.Wait()
}

// listingRefresher re-lists, in the background, what writes dropped from the
// listing fallback.
type listingRefresher struct {
	h     *Handler
	delay time.Duration

	mu      sync.Mutex
	pending map[string]*pendingRefresh
	closed  bool
}

// pendingRefresh is one listing waiting for its writes to settle.
type pendingRefresh struct {
	stale listcache.Stale
	first time.Time
	timer *time.Timer
	// backoff is the next wait after a refresh a write got in the way of.
	backoff time.Duration
}

func newListingRefresher(h *Handler, delay time.Duration) *listingRefresher {
	return &listingRefresher{
		h:       h,
		delay:   delay,
		pending: make(map[string]*pendingRefresh),
	}
}

// schedule asks for these listings to be refreshed once writes under them
// have settled. A nil refresher — refreshing is off — ignores it.
func (r *listingRefresher) schedule(stale []listcache.Stale) {
	if r == nil || len(stale) == 0 {
		return
	}
	now := time.Now()
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.closed {
		return
	}
	for _, s := range stale {
		if !refreshable(s.Origin) {
			continue
		}
		if p := r.pending[s.Key]; p != nil {
			p.stale = s
			p.backoff = 0
			if now.Sub(p.first) < listingRefreshMaxWait*r.delay {
				p.timer.Reset(r.delay)
			}
			continue
		}
		key := s.Key
		r.pending[key] = &pendingRefresh{
			stale: s,
			first: now,
			timer: time.AfterFunc(r.delay, func() { r.fire(key) }),
		}
	}
}

// refreshable reports whether a listing is worth asking for again. A page
// reached through a continuation token is not: the token was minted for the
// listing as it was, and the refreshed first page hands out a new one, so
// the old page would be remembered under a token no client is given again.
func refreshable(origin string) bool {
	u, err := url.Parse(origin)
	if err != nil {
		return false
	}
	return !u.Query().Has("continuation-token")
}

// fire runs one refresh whose writes have settled.
func (r *listingRefresher) fire(key string) {
	r.mu.Lock()
	p := r.pending[key]
	if r.closed || p == nil {
		r.mu.Unlock()
		return
	}
	if !r.h.listingWork.tryStart() {
		// As much background listing as this proxy is willing to do is
		// running. This one waits its turn rather than being forgotten.
		p.timer.Reset(r.delay)
		r.mu.Unlock()
		return
	}
	delete(r.pending, key)
	r.mu.Unlock()

	defer r.h.listingWork.done()
	if r.refresh(p.stale) {
		return
	}
	// A write under the prefix got in the way, so its answer could not be
	// remembered. Try again later, further apart each time, unless a write
	// has scheduled it afresh in the meantime.
	backoff := p.backoff * 2
	if backoff == 0 {
		backoff = r.delay
	}
	backoff = min(backoff, listingRefreshMaxBackoff)
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.closed || r.pending[key] != nil {
		return
	}
	r.pending[key] = &pendingRefresh{
		stale:   p.stale,
		first:   time.Now(),
		backoff: backoff,
		timer:   time.AfterFunc(backoff, func() { r.fire(key) }),
	}
}

// refresh lists one dropped listing again, and reports false only when it
// should be tried again: a write overlapped it. Any other outcome is final.
func (r *listingRefresher) refresh(s listcache.Stale) bool {
	h := r.h
	capture, ok := h.cfg.ListFallback.BeginRefresh(s)
	if !ok {
		return false
	}
	ctx, cancel := context.WithTimeout(h.listingWork.ctx, listingWorkTimeout)
	defer cancel()
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, s.Origin, nil)
	if err != nil {
		capture.Abort()
		return true
	}
	req.Header.Set("X-Amz-Content-Sha256", sigv4.EmptyPayloadSHA256)
	if err := h.cfg.UpstreamSigner.Sign(req, h.cfg.UpstreamRegion, time.Now()); err != nil {
		capture.Abort()
		return true
	}
	answer := h.fetchListing(req, h.cfg.Limits.RewriteBody)
	if !answer.isListing() {
		capture.Abort()
		if answer.err != nil {
			log.WithError(answer.err).Debug("a background listing refresh failed")
		} else {
			log.Debugf("a background listing refresh was answered %d", answer.resp.StatusCode)
		}
		return true
	}
	if capture.Commit(listingHeaders(answer.resp.Header), answer.body) {
		return true
	}
	return !capture.Overlapped()
}

// close stops what is pending. What is running belongs to listingWork,
// which Close stops after this.
func (r *listingRefresher) close() {
	if r == nil {
		return
	}
	r.mu.Lock()
	r.closed = true
	for key, p := range r.pending {
		p.timer.Stop()
		delete(r.pending, key)
	}
	r.mu.Unlock()
}
