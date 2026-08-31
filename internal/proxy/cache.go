package proxy

// The cache, seen from the request lifecycle.
//
// Everything here hangs off five rules, and they are worth stating before the
// code because every function below is one of them:
//
//  1. The cache is consulted only after the request has been authenticated
//     and authorized. A hit is an answer to a request that was already going
//     to be allowed; it can never make one that would have been refused.
//  2. The key names the *upstream* object — bucket plus the injected tenant
//     prefix — never what the client asked for. Two tenants that both call
//     something `data/a.parquet` are two different keys by construction, and
//     nothing about that depends on remembering to check.
//  3. What is stored is what the upstream sent, before any tenant rewriting.
//     Listings are not cached at all, precisely because their bodies are
//     filtered per access level and a cached one would be a filtered one.
//  4. Nothing is committed until the upstream confirmed it and the whole
//     announced length arrived.
//  5. The cache never delays or fails a request. Every error path here ends
//     in "then don't cache it", because a cache that can break a read is
//     worse than no cache.

import (
	"context"
	"io"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"

	"github.com/Kriechi/aws-s3-reverse-proxy/internal/cache"
	"github.com/Kriechi/aws-s3-reverse-proxy/internal/s3"
	"github.com/Kriechi/aws-s3-reverse-proxy/internal/sigv4"
)

const (
	// cacheFillTimeout bounds a background fetch. It is generous because the
	// object may be large and nobody is waiting for it, and bounded because
	// a fill that never finishes holds its key claimed forever.
	cacheFillTimeout = 5 * time.Minute

	// maxConcurrentFills caps the background fetches in flight. They are
	// work nobody asked for, so they must never crowd out the requests that
	// somebody did.
	maxConcurrentFills = 4
)

// cacheKeyFor names the upstream object a request addresses.
//
// Bucket names cannot contain a slash, so `bucket/prefix/key` cannot be
// spelled two ways. The tenant prefix is in it because the prefix is what
// isolation is made of: leaving it out would make one tenant's cache entry
// answer another tenant's read.
func cacheKeyFor(st *requestState) string {
	if st.operation.IsBucketLevel() || st.operation.Key == "" {
		return ""
	}
	return st.operation.Bucket + "/" + st.identity.KeyPrefix + st.operation.Key
}

// upstreamKeyFor is the same thing for a key that has already been prefixed,
// which is how a batch delete's keys arrive.
func upstreamKeyFor(bucket, prefixedKey string) string {
	return bucket + "/" + prefixedKey
}

// mutatesObject reports whether an operation changes what a read of that key
// would return. Uploading a part does not — the object does not exist until
// the upload is completed — and neither does abandoning one.
func mutatesObject(kind s3.Kind) bool {
	switch kind {
	case s3.PutObject, s3.DeleteObject, s3.CompleteMultipartUpload:
		return true
	}
	return false
}

// readsAnObject reports whether an operation is one the cache could answer.
// Anything else is not a cache lookup at all, and reporting it as one would
// put every upload in the lookup metric as a bypass.
func readsAnObject(kind s3.Kind) bool {
	return kind == s3.GetObject || kind == s3.HeadObject
}

// asksTheStoreToDecide reports whether a read carries a condition only the
// object store can evaluate.
//
// `If-Match` and `If-Unmodified-Since` are not freshness questions, they are
// concurrency primitives: the caller is asking about the object's current
// state, and a cache answering on its behalf would break the guarantee the
// caller is relying on. `If-None-Match`, `If-Modified-Since` and `If-Range`
// are a different matter — those are about what the caller already has, and
// http.ServeContent evaluates them against the cached entry correctly.
func asksTheStoreToDecide(r *http.Request) bool {
	return r.Header.Get("If-Match") != "" || r.Header.Get("If-Unmodified-Since") != ""
}

// beginCaching decides what this request means for the cache, before a byte
// of it is sent upstream.
//
// A write claims its key here rather than invalidating afterwards. Doing it
// afterwards leaves a window: a read that starts while the upload is in
// flight sees the object the upload is about to replace, and can finish —
// and be cached — after the upload landed. Claiming the key up front closes
// it, at the cost of that key not being cached for the length of the write.
func (h *Handler) beginCaching(st *requestState) {
	if h.cfg.Cache == nil {
		return
	}
	st.cacheKey = cacheKeyFor(st)
	if st.cacheKey != "" && mutatesObject(st.operation.Kind) {
		st.mutation = h.cfg.Cache.BeginMutation(st.cacheKey)
	}
}

// serveCachedRead answers the request from disk when it can, and reports
// whether it did.
func (h *Handler) serveCachedRead(w http.ResponseWriter, r *http.Request, st *requestState) bool {
	if h.cfg.Cache == nil || st.cacheKey == "" || !readsAnObject(st.operation.Kind) {
		return false
	}
	if asksTheStoreToDecide(r) {
		st.cacheResult = string(cache.Bypass)
		return false
	}
	entry, result := h.cfg.Cache.Get(st.cacheKey)
	st.cacheResult = string(result)
	if result != cache.Hit {
		return false
	}
	defer entry.Close()

	header := w.Header()
	for name, values := range entry.Header {
		header[name] = values
	}
	header.Set("X-Cache", "HIT")
	// http.ServeContent does the rest: Content-Length, the Range arithmetic
	// and its 206, `If-None-Match` against the ETag above, `If-Range`, and
	// the 416 for a range past the end. A HEAD gets the headers and no body,
	// because net/http already knows not to write one.
	http.ServeContent(w, r, "", cachedModTime(entry), entry.Body())
	return true
}

// cachedModTime is the object's Last-Modified, or the zero time if the
// upstream did not give one — which tells ServeContent to leave modification
// dates out of its answer rather than invent one.
func cachedModTime(entry *cache.Entry) time.Time {
	if t, err := http.ParseTime(entry.Header.Get("Last-Modified")); err == nil {
		return t
	}
	return time.Time{}
}

// captureRead stores a response on its way to the client.
//
// The body is not read here and not buffered: it is wrapped, so the bytes
// reach the client at the same speed they always did and land in the cache on
// the way past.
func (h *Handler) captureRead(resp *http.Response, st *requestState) {
	if h.cfg.Cache == nil || st.cacheKey == "" || st.operation.Kind != s3.GetObject {
		return
	}
	switch resp.StatusCode {
	case http.StatusOK:
	case http.StatusPartialContent:
		// A ranged read cannot fill the cache from what it received — it
		// only has part of the object — so the object is fetched separately.
		h.fillAfterRangedRead(resp, st)
		return
	default:
		return
	}
	if resp.Body == nil || !cache.Storable(resp.Header) {
		return
	}
	if resp.ContentLength < 0 || resp.ContentLength > h.cfg.Cache.MaxObjectSize() {
		return
	}
	writer, err := h.cfg.Cache.Put(st.cacheKey, resp.ContentLength)
	if err != nil {
		return
	}
	resp.Body = &cacheTee{
		source: resp.Body,
		sink:   writer,
		header: s3.CacheableResponseHeaders(resp.Header),
	}
}

// fillAfterRangedRead fetches the whole object in the background after a
// ranged read missed.
//
// Without it the cache would stay empty under the workload it exists for. A
// client like DuckDB's httpfs reads an object almost entirely in ranges — a
// footer, then row groups — so a cache that only ever filled from whole-object
// reads would never see one.
func (h *Handler) fillAfterRangedRead(resp *http.Response, st *requestState) {
	if !h.cfg.CacheRangeFills || !cache.Storable(resp.Header) {
		return
	}
	size, ok := totalSizeFromContentRange(resp.Header.Get("Content-Range"))
	if !ok || size <= 0 || size > h.cfg.Cache.MaxObjectSize() {
		return
	}
	if !h.cfg.Cache.ClaimFill(st.cacheKey) {
		// Somebody is already fetching it. A burst of ranged reads of one
		// cold object is the normal case, not the exception.
		return
	}
	target := *resp.Request.URL
	go h.fillObject(st.cacheKey, &target)
}

// fillObject fetches one object and stores it. Nobody is waiting for it, so
// every failure is silent and simply leaves the object uncached.
func (h *Handler) fillObject(key string, target *url.URL) {
	defer h.cfg.Cache.ReleaseFill(key)

	select {
	case h.fillSlots <- struct{}{}:
		defer func() { <-h.fillSlots }()
	default:
		// Already as many background fetches as this proxy is willing to
		// run. Work nobody asked for must not crowd out work somebody did.
		return
	}

	ctx, cancel := context.WithTimeout(context.Background(), cacheFillTimeout)
	defer cancel()
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, target.String(), nil)
	if err != nil {
		return
	}
	// A plain, unconditional read of the object. None of the client's
	// headers come along: this request is not on anyone's behalf, and the
	// point is to store what the object store holds rather than what some
	// particular caller negotiated.
	req.Header.Set("X-Amz-Content-Sha256", sigv4.EmptyPayloadSHA256)
	if err := h.cfg.UpstreamSigner.Sign(req, h.cfg.UpstreamRegion, time.Now()); err != nil {
		return
	}
	resp, err := h.transport.RoundTrip(req)
	if err != nil {
		return
	}
	defer func() {
		_, _ = io.Copy(io.Discard, resp.Body)
		_ = resp.Body.Close()
	}()
	if resp.StatusCode != http.StatusOK || resp.ContentLength < 0 ||
		resp.ContentLength > h.cfg.Cache.MaxObjectSize() || !cache.Storable(resp.Header) {
		return
	}
	writer, err := h.cfg.Cache.Put(key, resp.ContentLength)
	if err != nil {
		return
	}
	defer writer.Abort()
	if _, err := io.Copy(writer, resp.Body); err != nil {
		return
	}
	_ = writer.Commit(s3.CacheableResponseHeaders(resp.Header))
}

// totalSizeFromContentRange reads the object's full length out of a
// `Content-Range: bytes 0-99/12345` header. An unsatisfied range or an
// unknown total (`/*`) reports false.
func totalSizeFromContentRange(value string) (int64, bool) {
	slash := strings.LastIndexByte(value, '/')
	if slash < 0 {
		return 0, false
	}
	size, err := strconv.ParseInt(strings.TrimSpace(value[slash+1:]), 10, 64)
	if err != nil {
		return 0, false
	}
	return size, true
}

// finishMutation records what became of a write.
//
// A successful upload whose body was captured leaves the object cached, which
// is the difference between a cache that warms up as data arrives and one
// that only warms up when somebody reads. Everything else — a refused upload,
// a delete, a completed multipart — leaves the key invalidated, which
// Mutation.End does on its own.
func (h *Handler) finishMutation(resp *http.Response, st *requestState) {
	if st.mutation == nil || resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return
	}
	header := http.Header{}
	for name, values := range st.uploadHeader {
		header[name] = values
	}
	if etag := resp.Header.Get("Etag"); etag != "" {
		header.Set("Etag", etag)
	}
	// A PUT response has no Last-Modified. Its Date is the object store's
	// clock at the moment it accepted the object, which is what a later read
	// will report to within a second.
	if date := resp.Header.Get("Date"); date != "" {
		header.Set("Last-Modified", date)
	}
	st.mutation.Store(header)
}

// captureUpload tees the body of an upload into the cache, so that writing an
// object leaves it cached rather than merely uncached.
//
// It declines an upload whose length is not known: the store needs to be able
// to tell a complete body from one whose client hung up, and an announced
// length is the only thing that distinguishes them.
func (h *Handler) captureUpload(st *requestState, contentLength int64) *cache.Writer {
	if h.cfg.Cache == nil || !h.cfg.CacheWrites || st.mutation == nil {
		return nil
	}
	if st.operation.Kind != s3.PutObject {
		return nil
	}
	if contentLength <= 0 || contentLength > h.cfg.Cache.MaxObjectSize() {
		return nil
	}
	return st.mutation.Capture(contentLength)
}

// cacheTee copies a response body into the cache as it is read out to the
// client.
//
// The commit hangs off reaching EOF rather than off Close, because that is the
// only signal that distinguishes a body that finished from one whose reader
// gave up. A client that disconnects halfway closes without EOF, and what it
// received does not become a cache entry.
type cacheTee struct {
	source io.ReadCloser
	sink   *cache.Writer
	header http.Header
	closed bool
}

func (t *cacheTee) Read(p []byte) (int, error) {
	n, err := t.source.Read(p)
	if n > 0 {
		_, _ = t.sink.Write(p[:n])
	}
	if err == io.EOF {
		_ = t.sink.Commit(t.header)
	}
	return n, err
}

func (t *cacheTee) Close() error {
	if !t.closed {
		t.closed = true
		t.sink.Abort() // a no-op once Commit has run
	}
	return t.source.Close()
}
