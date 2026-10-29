package proxy

// Encrypting legacy objects, one read at a time.
//
// When a read finds an object stored before there was a customer key, the
// object is rewritten in place under the key — in the background, after the
// read has already been answered. It is opportunistic: objects nobody reads
// stay as they are, a failed attempt is retried by a later read rather than by
// a job queue, and nothing here is a migration guarantee.
//
// What it does guarantee is that a repair never makes things worse:
//
//   - The replacement is conditional, and the condition is enforced by the
//     object store, not by this process. A rewrite is a multipart upload
//     whose completion carries `If-Match` with the ETag the source was read
//     at: if anything replaced the object meanwhile the store answers 412,
//     and if anything deleted it, 404 — the upload is abandoned either way
//     and nothing is retried unconditionally. A lock in this process would
//     only ever have protected against this process.
//   - The source is the whole object, read at that same ETag. A ranged
//     response a client happened to ask for is never a replacement body.
//   - Bytes stream from the source read into the parts, so memory is bounded
//     by the copy buffer whatever the object's size, and no plaintext copy is
//     ever staged in the bucket.
//   - Metadata the rewrite would silently drop — tags, object-lock retention
//     — is a reason to skip the object, not to rewrite it without them.
//   - It respects the deployment's write restrictions: nothing is rewritten
//     in read-only mode, or under a path the policy lets no level write.
//
// Why multipart even for a small object: a conditional PutObject with SSE-C
// headers over a plaintext object is refused by MinIO, which evaluates the
// precondition by reading the existing object with the request's key. A
// multipart completion carries no key (S3 does not need one there unless the
// upload uses checksums, and this one does not), so its precondition is
// evaluated against the object as it is stored. It works the same on AWS S3,
// which supports `If-Match` on CompleteMultipartUpload.

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/xml"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"reflect"
	"strconv"
	"strings"
	"sync"
	"time"

	log "github.com/sirupsen/logrus"

	"github.com/Kriechi/aws-s3-reverse-proxy/internal/cache"
	"github.com/Kriechi/aws-s3-reverse-proxy/internal/listcache"
	"github.com/Kriechi/aws-s3-reverse-proxy/internal/s3"
	"github.com/Kriechi/aws-s3-reverse-proxy/internal/sigv4"
	"github.com/Kriechi/aws-s3-reverse-proxy/internal/ssec"
)

const (
	// maxConcurrentRepairs bounds the rewrites in flight. Each one streams an
	// object out of the store and back in, so this is a bound on upstream
	// bandwidth spent on work no client is waiting for.
	maxConcurrentRepairs = 2

	// repairRetryAfter is how long an object that was tried — whatever the
	// outcome — is left alone before a read may try it again. It is what
	// keeps a store that silently ignores SSE-C, or an object that cannot be
	// repaired, from being rewritten on every read.
	repairRetryAfter = 10 * time.Minute

	// repairSuspension is how long every repair stops after the store
	// accepted a repair's encrypted write without confirming it. That is a
	// statement about the store, not about one object.
	repairSuspension = time.Hour

	// repairTimeout bounds one repair end to end.
	repairTimeout = time.Hour

	// abortTimeout bounds abandoning a failed upload, which runs on its own
	// context so that a shutdown still cleans up after itself.
	abortTimeout = 30 * time.Second

	// completeTimeout bounds the completion of an upload. It is not cut
	// short by a shutdown: once the store may have started replacing the
	// object, the only way to know whether it did is to read its answer.
	completeTimeout = 2 * time.Minute

	// repairPartSize is the size of each part of a rewrite. Large enough that
	// most objects take one part; raised for objects so big that 10 000
	// parts of this size would not hold them.
	repairPartSize = 16 << 20
	maxParts       = 10000

	// maxRepairRecords bounds the table of recent attempts.
	maxRepairRecords = 1 << 14
)

// repairer runs the rewrites. It is safe for concurrent use; a nil repairer
// — no key configured — does nothing.
type repairer struct {
	h *Handler

	ctx    context.Context
	cancel context.CancelFunc
	wg     sync.WaitGroup
	slots  chan struct{}

	mu        sync.Mutex
	running   map[string]bool
	attempted map[string]time.Time
	suspended time.Time

	// now is the clock, for tests.
	now func() time.Time
}

func newRepairer(h *Handler) *repairer {
	ctx, cancel := context.WithCancel(context.Background())
	return &repairer{
		h:         h,
		ctx:       ctx,
		cancel:    cancel,
		slots:     make(chan struct{}, maxConcurrentRepairs),
		running:   make(map[string]bool),
		attempted: make(map[string]time.Time),
		now:       time.Now,
	}
}

// close cancels the repairs in flight and waits for them to abandon their
// uploads.
func (r *repairer) close() {
	if r == nil {
		return
	}
	// Under the lock start holds, so no repair can be added between the
	// cancellation and the wait.
	r.mu.Lock()
	r.cancel()
	r.mu.Unlock()
	r.wg.Wait()
}

// repairJob is one object to rewrite: where it is upstream, and what the
// request that found it knew about it.
type repairJob struct {
	target url.URL
	st     *requestState
}

// schedule asks for an object to be repaired after a read found it in a state
// that may be plaintext. The job establishes the state itself before writing.
func (r *repairer) schedule(st *requestState, target *url.URL, etag, why string) {
	if r == nil || st == nil {
		return
	}
	if r.start(repairJob{target: *target, st: st}, etag) {
		r.h.objectLog(st).Info("scheduling encryption: " + why)
	}
}

// scheduleObserved is schedule for a read that already established the
// object is plaintext. The job observes it again before writing anyway: the
// observation it acts on has to be its own, taken immediately before.
func (r *repairer) scheduleObserved(st *requestState, target *url.URL, observed *objectState) {
	if r == nil || st == nil || observed == nil {
		return
	}
	r.start(repairJob{target: *target, st: st}, observed.etag)
}

// start runs a job unless one for the same object is already running, the
// same object at the same ETag was tried recently, repairs are suspended, or
// every slot is busy. Declining is never an error: the next read of the
// object will ask again.
//
// What the deployment forbids — read-only mode, a path nobody may write — is
// checked by the job rather than here, so that the skip is logged, and logged
// once per object and retry interval rather than on every read.
func (r *repairer) start(job repairJob, etag string) bool {
	if job.st.operation == nil || job.st.identity == nil {
		return false
	}
	job.target.RawQuery = ""
	object := job.target.EscapedPath()
	value, _ := s3.CanonicalETag(etag)
	attempt := object + "\x00" + value

	r.mu.Lock()
	defer r.mu.Unlock()
	now := r.now()
	if r.ctx.Err() != nil || now.Before(r.suspended) || r.running[object] {
		return false
	}
	if at, ok := r.attempted[attempt]; ok && now.Sub(at) < repairRetryAfter {
		return false
	}
	select {
	case r.slots <- struct{}{}:
	default:
		return false
	}
	if len(r.attempted) >= maxRepairRecords {
		for key, at := range r.attempted {
			if now.Sub(at) >= repairRetryAfter {
				delete(r.attempted, key)
			}
		}
		if len(r.attempted) >= maxRepairRecords {
			clear(r.attempted)
		}
	}
	r.attempted[attempt] = now
	r.running[object] = true
	r.wg.Add(1)
	go func() {
		defer func() {
			r.mu.Lock()
			delete(r.running, object)
			r.mu.Unlock()
			<-r.slots
			r.wg.Done()
		}()
		r.run(job)
	}()
	return true
}

// suspend stops every repair for a while.
func (r *repairer) suspend() {
	r.mu.Lock()
	r.suspended = r.now().Add(repairSuspension)
	r.mu.Unlock()
}

// errNotPlaintext and its siblings are outcomes, not failures: they say why a
// repair stopped, and each is logged at the level it deserves.
var (
	errNotPlaintext = errors.New("the object is not stored in plaintext")
	errGone         = errors.New("the object no longer exists")
	errChanged      = errors.New("the object changed while it was being rewritten")
	errUnconfirmed  = errors.New("the object store accepted an encrypted part without confirming SSE-C")

	// errNoConditionalWrites is a store that refuses If-Match on a
	// multipart completion. Without it the replacement cannot be made safe,
	// and is not made at all.
	errNoConditionalWrites = errors.New("the object store does not support conditional replacement (If-Match on CompleteMultipartUpload)")
)

// skipReason is a repair that stopped because the object is one it must not
// rewrite, found out partway through.
type skipReason string

func (s skipReason) Error() string { return string(s) }

// run is one repair, start to finish.
func (r *repairer) run(job repairJob) {
	h := r.h
	logger := h.objectLog(job.st).WithField("operation", "EncryptLegacyObject")
	ctx, cancel := context.WithTimeout(r.ctx, repairTimeout)
	defer cancel()

	// Classification comes first and runs whether or not this deployment
	// rewrites anything: an object that turns out to be encrypted after all
	// clears a cached entry that was still asking.
	state, err := r.observe(ctx, &job.target)
	switch {
	case errors.Is(err, errNotPlaintext):
		// Somebody encrypted it already — another proxy, an earlier repair
		// whose cache invalidation did not land. Whatever this process
		// cached from its plaintext days describes a version that is gone.
		h.invalidate(job.st)
		logger.Debug("not encrypting: the object is no longer stored in plaintext")
		return
	case errors.Is(err, errGone):
		logger.Debug("not encrypting: the object no longer exists")
		return
	case err != nil:
		logger.WithError(err).Warn("not encrypting: the object's encryption state could not be established")
		return
	}
	if !h.cfg.RepairPlaintext {
		logger.Info("not encrypting a plaintext object: repair is off; set UPSTREAM_SSE_C_REPAIR=true " +
			"once nothing writes the bucket without the key")
		return
	}
	if reason := r.prohibited(job.st); reason != "" {
		logger.Warn("not encrypting a plaintext object: " + reason)
		return
	}
	if reason := state.unrepairable(); reason != "" {
		logger.Warn("not encrypting a plaintext object: " + reason)
		return
	}
	acl, reason := r.readACL(ctx, &job.target)
	if reason != "" {
		logger.Warn("not encrypting a plaintext object: " + reason)
		return
	}

	err = r.rewrite(ctx, &job.target, job.st, state, acl)
	var skipped skipReason
	switch {
	case err == nil:
		logger.WithField("size", state.size).Info("encrypted a plaintext object with the configured SSE-C key")
	case errors.As(err, &skipped):
		logger.Warn("not encrypting a plaintext object: " + string(skipped))
	case errors.Is(err, errChanged), errors.Is(err, errGone):
		logger.WithError(err).Info("abandoned encrypting a plaintext object")
	case errors.Is(err, errUnconfirmed):
		r.suspend()
		logger.WithError(err).Warnf("abandoned encrypting a plaintext object; suspending repairs for %s", repairSuspension)
	case errors.Is(err, errNoConditionalWrites):
		r.suspend()
		logger.Warnf("not encrypting plaintext objects: the object store does not support conditional replacement, "+
			"so an object cannot be rewritten without risking a concurrent write; suspending repairs for %s", repairSuspension)
	case r.ctx.Err() != nil:
		logger.Info("abandoned encrypting a plaintext object: shutting down")
	default:
		logger.WithError(err).Warn("encrypting a plaintext object failed; a later read will retry it")
	}
}

// prohibited reports why the deployment forbids rewriting this object, or ""
// when it does not.
//
// The rewrite is the operator's, made with the operator's credentials, so it
// is not held to the access level of whoever happened to read the object. It
// is held to what the deployment forbids for everybody: read-only mode, and a
// path no level of the current policy may write — a path an operator made
// read-only for every tenant is one they did not mean to have rewritten.
//
// A read through the global credential names the key as it is upstream,
// which no rule is written against. There the operator's own word stands in
// for the policy's: the object is rewritten when that credential may write.
func (r *repairer) prohibited(st *requestState) string {
	if r.h.cfg.ReadOnly {
		return "the proxy is in read-only mode"
	}
	if st.identity.Global() {
		if st.policy.AuthorizeIdentity(st.identity, st.operation.Key, http.MethodPut).Allowed {
			return ""
		}
		return "the global access credential may not write"
	}
	current := r.h.cfg.Policy.Current()
	for _, level := range current.Levels() {
		if current.Authorize(level, st.operation.Key, http.MethodPut).Allowed {
			return ""
		}
	}
	return "no access level of the current policy may write this path"
}

// objectState is what the object store said about an object at one moment:
// enough to make a later write conditional on it, and to tell whether it
// changed in a way its ETag would not show.
type objectState struct {
	etag     string
	weak     bool
	size     int64
	modified string
	version  string

	// preserved are the headers a rewrite carries over to the new object.
	preserved http.Header

	// guarded are the headers that decide whether the object may be
	// rewritten at all — tags, retention, archival — beside the preserved
	// ones. Any change to them between two observations is a change.
	guarded http.Header

	// header is the whole response, for the checks that decide whether the
	// object can be rewritten at all.
	header http.Header
}

func observeState(resp *http.Response) *objectState {
	etag, weak := s3.CanonicalETag(resp.Header.Get("Etag"))
	return &objectState{
		etag:      etag,
		weak:      weak,
		size:      resp.ContentLength,
		modified:  resp.Header.Get("Last-Modified"),
		version:   resp.Header.Get("X-Amz-Version-Id"),
		preserved: preservedHeaders(resp.Header),
		guarded:   guardedHeaders(resp.Header),
		header:    resp.Header.Clone(),
	}
}

// preservedHeaders are what a rewrite has to carry over for the new object to
// be the same object: its content headers and the tenant's own metadata, plus
// the storage class and redirect it was stored with.
func preservedHeaders(header http.Header) http.Header {
	out := http.Header{}
	for name, values := range header {
		canonical := http.CanonicalHeaderKey(name)
		switch {
		case canonical == "Content-Type", canonical == "Content-Encoding", canonical == "Content-Disposition",
			canonical == "Content-Language", canonical == "Cache-Control", canonical == "Expires",
			canonical == "X-Amz-Website-Redirect-Location", strings.HasPrefix(canonical, "X-Amz-Meta-"):
			out[canonical] = values
		case canonical == "X-Amz-Storage-Class" && !strings.EqualFold(header.Get(name), "STANDARD"):
			out[canonical] = values
		}
	}
	return out
}

// guardedHeaders are the headers unrepairable decides on.
func guardedHeaders(header http.Header) http.Header {
	out := http.Header{}
	for _, name := range []string{"X-Amz-Tagging-Count", "X-Amz-Object-Lock-Mode", "X-Amz-Object-Lock-Retain-Until-Date",
		"X-Amz-Object-Lock-Legal-Hold", "X-Amz-Storage-Class", "X-Amz-Restore"} {
		if values := header.Values(name); len(values) > 0 {
			out[name] = values
		}
	}
	return out
}

// unrepairable reports why an object cannot be rewritten without losing
// something, or "" when it can.
func (s *objectState) unrepairable() string {
	switch {
	case s.etag == "":
		return "the object store gave no ETag to make the replacement conditional on"
	case s.weak:
		return "the object store gave only a weak ETag, which cannot make a replacement conditional"
	case s.size < 0:
		return "the object store did not state the object's size"
	case s.size > maxParts*int64(5<<30):
		return "the object is larger than a multipart upload can hold"
	}
	if count := s.header.Get("X-Amz-Tagging-Count"); count != "" && count != "0" {
		return "the object has tags, which a rewrite cannot carry over"
	}
	for _, name := range []string{"X-Amz-Object-Lock-Mode", "X-Amz-Object-Lock-Retain-Until-Date"} {
		if s.header.Get(name) != "" {
			return "the object is under object-lock retention"
		}
	}
	if strings.EqualFold(s.header.Get("X-Amz-Object-Lock-Legal-Hold"), "ON") {
		return "the object is under an object-lock legal hold"
	}
	switch strings.ToUpper(s.header.Get("X-Amz-Storage-Class")) {
	case "GLACIER", "DEEP_ARCHIVE":
		return "the object is archived"
	}
	return ""
}

// sameAs reports whether two observations describe the same version of an
// object. The ETag alone is not enough: replacing an object's metadata in
// place can leave its ETag as it was.
func (s *objectState) sameAs(other *objectState) bool {
	return s.etag == other.etag && s.weak == other.weak && s.size == other.size &&
		s.modified == other.modified && s.version == other.version &&
		reflect.DeepEqual(s.preserved, other.preserved) && reflect.DeepEqual(s.guarded, other.guarded)
}

// observe establishes the object's state with a HEAD without the key.
func (r *repairer) observe(ctx context.Context, target *url.URL) (*objectState, error) {
	resp, err := r.h.plainRequest(ctx, http.MethodHead, target, nil)
	if err != nil {
		return nil, err
	}
	drain(resp)
	switch {
	case resp.StatusCode == http.StatusNotFound:
		return nil, errGone
	case resp.StatusCode == http.StatusOK && resp.Header.Get(ssec.HeaderAlgorithm) == "":
		return observeState(resp), nil
	case resp.StatusCode == http.StatusOK, resp.StatusCode == http.StatusBadRequest:
		// A 400 to a keyless HEAD is what an SSE-C object answers; a 200
		// naming a customer algorithm is the same thing said differently.
		return nil, errNotPlaintext
	default:
		return nil, fmt.Errorf("HEAD without the key answered %d", resp.StatusCode)
	}
}

// rewrite replaces the object with an encrypted copy of itself, conditional
// on it still being the version observed.
func (r *repairer) rewrite(ctx context.Context, target *url.URL, st *requestState, state *objectState, acl *objectACL) (err error) {
	uploadID, err := r.createUpload(ctx, target, state.preserved)
	if err != nil {
		return err
	}
	completed := false
	defer func() {
		if !completed {
			r.abortUpload(target, uploadID)
		}
	}()

	// The rewritten object belongs to whoever the store makes the owner of
	// this upload. If that is not the object's owner now, the rewrite would
	// hand the object to somebody else.
	if owner, err := r.uploadOwner(ctx, target, uploadID); err != nil {
		return skipReason("the owner the rewritten object would have could not be read: " + err.Error())
	} else if acl.owner != "" && owner != "" && owner != acl.owner {
		return skipReason("the rewritten object would belong to another owner")
	}

	source, err := r.openSource(ctx, target, state)
	if err != nil {
		return err
	}
	defer drain(source)

	parts, err := r.uploadParts(ctx, target, uploadID, source.Body, state.size)
	if err != nil {
		return err
	}

	// The precondition below is the guarantee; this is the part of it an
	// ETag cannot express. Metadata replaced in place while the parts were
	// uploading would otherwise be overwritten by the copy taken before.
	now, err := r.observe(ctx, target)
	if err != nil {
		return err
	}
	// Exclusions are decided again on what the store says now: tags or a
	// hold added while the parts were uploading would be dropped by the
	// replacement as surely as ones that were there from the start.
	if reason := now.unrepairable(); reason != "" {
		return skipReason(reason)
	}
	if !now.sameAs(state) {
		return errChanged
	}
	if again, reason := r.readACL(ctx, target); reason != "" {
		return skipReason(reason)
	} else if !again.equal(acl) {
		return errChanged
	}

	// The key is claimed for the cache while the replacement lands: a read
	// that is still streaming the plaintext version must not commit it, and
	// what was cached from that version has to go. Ending the mutation
	// without a captured body is exactly that.
	var mutation *cache.Mutation
	if r.h.cfg.Cache != nil && st.cacheKey != "" {
		mutation = r.h.cfg.Cache.BeginMutation(st.cacheKey)
	}
	// The replacement has a new ETag, so a listing remembered from before it
	// describes an object that is gone.
	var listing *listcache.Write
	if r.h.cfg.ListFallback != nil {
		listing = r.h.cfg.ListFallback.BeginWrite(cacheKeyFor(st))
	}
	finishing, cancel := context.WithTimeout(context.WithoutCancel(ctx), completeTimeout)
	defer cancel()
	err = r.completeUpload(finishing, target, uploadID, parts, state.etag)
	mutation.End()
	outcome := listcache.Answered
	if err != nil {
		// Whether the completion landed is not known, and it may still.
		outcome = listcache.Unresolved
	}
	r.h.endListingWrite(listing, outcome)
	if err != nil {
		return err
	}
	completed = true
	r.verify(finishing, target, st)
	return nil
}

// createUpload starts the multipart upload the encrypted copy is written to,
// carrying over the object's content headers and metadata.
func (r *repairer) createUpload(ctx context.Context, target *url.URL, preserved http.Header) (string, error) {
	header := preserved.Clone()
	r.h.cfg.UpstreamSSECKey.Apply(header)
	resp, err := r.send(ctx, http.MethodPost, target, "uploads", nil, 0, sigv4.EmptyPayloadSHA256, header)
	if err != nil {
		return "", err
	}
	defer drain(resp)
	body, err := io.ReadAll(io.LimitReader(resp.Body, maxErrorBodySize))
	if err != nil {
		return "", err
	}
	if resp.StatusCode != http.StatusOK {
		return "", fmt.Errorf("starting the multipart upload answered %d", resp.StatusCode)
	}
	// Not checked for a confirmation: MinIO sends none here. The parts are
	// what confirm the key was applied, and the first one is checked.
	if r.h.cfg.UpstreamSSECKey.Check(resp.Header) == ssec.Mismatched {
		return "", errUnconfirmed
	}
	var result struct {
		UploadID string `xml:"UploadId"`
	}
	if err := xml.Unmarshal(body, &result); err != nil || result.UploadID == "" {
		return "", errors.New("starting the multipart upload returned no upload id")
	}
	return result.UploadID, nil
}

// openSource reads the whole object without the key, pinned to the observed
// version.
func (r *repairer) openSource(ctx context.Context, target *url.URL, state *objectState) (*http.Response, error) {
	header := http.Header{
		"If-Match": {s3.FormatETag(state.etag, false)},
		// Asking for identity is what stops Go's transport from asking for
		// gzip and then transparently decompressing an object that was
		// stored gzip-encoded — which would rewrite it with different bytes.
		"Accept-Encoding": {"identity"},
	}
	resp, err := r.h.plainRequest(ctx, http.MethodGet, target, header)
	if err != nil {
		return nil, err
	}
	switch {
	case resp.StatusCode == http.StatusPreconditionFailed:
		drain(resp)
		return nil, errChanged
	case resp.StatusCode == http.StatusNotFound:
		drain(resp)
		return nil, errGone
	case resp.StatusCode != http.StatusOK:
		drain(resp)
		return nil, fmt.Errorf("reading the object answered %d", resp.StatusCode)
	}
	if resp.Uncompressed || resp.ContentLength != state.size || !s3.SameETag(resp.Header.Get("Etag"), s3.FormatETag(state.etag, false)) ||
		resp.Header.Get("X-Amz-Version-Id") != state.version {
		drain(resp)
		return nil, errChanged
	}
	return resp, nil
}

// completedPart is one uploaded part, by its canonical ETag.
type completedPart struct {
	number int
	etag   string
}

// uploadParts streams the source into the upload part by part. Nothing is
// buffered: each part's body is the next stretch of the source read, and the
// part request states its exact length.
func (r *repairer) uploadParts(ctx context.Context, target *url.URL, uploadID string, source io.Reader, size int64) ([]completedPart, error) {
	partSize := int64(repairPartSize)
	if needed := (size + maxParts - 1) / maxParts; needed > partSize {
		partSize = (needed + 1<<20 - 1) &^ (1<<20 - 1)
	}
	var parts []completedPart
	for number, offset := 1, int64(0); offset < size || number == 1; number++ {
		length := min(partSize, size-offset)
		header := http.Header{}
		r.h.cfg.UpstreamSSECKey.Apply(header)
		query := url.Values{"partNumber": {strconv.Itoa(number)}, "uploadId": {uploadID}}.Encode()
		counter := &countingReader{r: io.LimitReader(source, length)}
		var body io.Reader = counter
		if length == 0 {
			body = nil
		}
		// UNSIGNED-PAYLOAD rather than a digest, because a digest would mean
		// reading the part twice — once to hash it — and so holding it in
		// memory. The connection is TLS, which SSE-C requires anyway.
		resp, err := r.send(ctx, http.MethodPut, target, query, body, length, sigv4.UnsignedPayload, header)
		if err != nil {
			return nil, err
		}
		drain(resp)
		if resp.StatusCode != http.StatusOK {
			return nil, fmt.Errorf("uploading part %d answered %d", number, resp.StatusCode)
		}
		if counter.n != length {
			return nil, fmt.Errorf("the source ended after %d of %d bytes", offset+counter.n, size)
		}
		if r.h.cfg.UpstreamSSECKey.Check(resp.Header) != ssec.Confirmed {
			return nil, errUnconfirmed
		}
		etag, _ := s3.CanonicalETag(resp.Header.Get("Etag"))
		if etag == "" {
			return nil, fmt.Errorf("part %d was accepted without an ETag", number)
		}
		parts = append(parts, completedPart{number: number, etag: etag})
		offset += length
	}
	return parts, nil
}

// completeUpload assembles the parts into the object, on condition that the
// object is still the version the source was read at.
//
// No key travels with it: see the note at the top of this file. The part ETags
// are the canonical values, which is what a part list carries.
func (r *repairer) completeUpload(ctx context.Context, target *url.URL, uploadID string, parts []completedPart, etag string) error {
	var b bytes.Buffer
	b.WriteString("<CompleteMultipartUpload>")
	for _, part := range parts {
		fmt.Fprintf(&b, "<Part><PartNumber>%d</PartNumber><ETag>", part.number)
		_ = xml.EscapeText(&b, []byte(part.etag))
		b.WriteString("</ETag></Part>")
	}
	b.WriteString("</CompleteMultipartUpload>")
	body := b.Bytes()

	header := http.Header{"If-Match": {s3.FormatETag(etag, false)}, "Content-Type": {"application/xml"}}
	resp, err := r.send(ctx, http.MethodPost, target, url.Values{"uploadId": {uploadID}}.Encode(),
		bytes.NewReader(body), int64(len(body)), payloadSHA256(body), header)
	if err != nil {
		return err
	}
	defer drain(resp)
	switch resp.StatusCode {
	case http.StatusOK:
	case http.StatusPreconditionFailed, http.StatusConflict:
		return errChanged
	case http.StatusNotFound:
		return errGone
	case http.StatusNotImplemented:
		return errNoConditionalWrites
	default:
		return fmt.Errorf("completing the upload answered %d", resp.StatusCode)
	}
	// S3 may answer a completion 200 and put the error in the body, because
	// it has to commit to a status before it knows how assembling went.
	result, err := io.ReadAll(io.LimitReader(resp.Body, maxErrorBodySize))
	if err != nil {
		return err
	}
	if bytes.Contains(result, []byte("<Error>")) {
		if bytes.Contains(result, []byte("PreconditionFailed")) {
			return errChanged
		}
		return errors.New("completing the upload failed after it started")
	}
	return nil
}

// abortUpload abandons an upload that will not be completed, so its parts do
// not linger in the bucket. It runs on its own context: a repair cancelled by
// a shutdown still has to clean up.
func (r *repairer) abortUpload(target *url.URL, uploadID string) {
	ctx, cancel := context.WithTimeout(context.Background(), abortTimeout)
	defer cancel()
	resp, err := r.send(ctx, http.MethodDelete, target, url.Values{"uploadId": {uploadID}}.Encode(), nil, 0, sigv4.EmptyPayloadSHA256, nil)
	if err != nil {
		log.WithError(err).Warn("could not abort the multipart upload of an abandoned encryption repair")
		return
	}
	drain(resp)
	if resp.StatusCode != http.StatusNoContent && resp.StatusCode != http.StatusOK && resp.StatusCode != http.StatusNotFound {
		log.WithField("status", resp.StatusCode).Warn("could not abort the multipart upload of an abandoned encryption repair")
	}
}

// verify reads the repaired object back with the key, and warns when the store
// does not confirm it. The rewrite has already happened; this only reports.
func (r *repairer) verify(ctx context.Context, target *url.URL, st *requestState) {
	header := http.Header{}
	r.h.cfg.UpstreamSSECKey.Apply(header)
	resp, err := r.send(ctx, http.MethodHead, target, "", nil, 0, sigv4.EmptyPayloadSHA256, header)
	if err != nil {
		return
	}
	drain(resp)
	if resp.StatusCode != http.StatusOK || r.h.cfg.UpstreamSSECKey.Check(resp.Header) != ssec.Confirmed {
		r.h.objectLog(st).WithField("status", resp.StatusCode).Warn("a repaired object does not read back as encrypted with the configured key")
	}
}

// send builds, signs and sends one request of a repair.
func (r *repairer) send(ctx context.Context, method string, target *url.URL, rawQuery string, body io.Reader, length int64, payloadHash string, header http.Header) (*http.Response, error) {
	u := *target
	u.RawQuery = rawQuery
	req, err := http.NewRequestWithContext(ctx, method, u.String(), body)
	if err != nil {
		return nil, err
	}
	req.ContentLength = length
	if body == nil {
		req.Body = nil
	}
	for name, values := range header {
		req.Header[http.CanonicalHeaderKey(name)] = values
	}
	req.Header.Set("X-Amz-Content-Sha256", payloadHash)
	if err := r.h.cfg.UpstreamSigner.Sign(req, r.h.cfg.UpstreamRegion, time.Now()); err != nil {
		return nil, err
	}
	return r.h.transport.RoundTrip(req)
}

// objectACL is an object's access control list, reduced to what decides
// whether a rewrite keeps it: the owner, and every grant.
type objectACL struct {
	owner  string
	grants []aclGrant
}

type aclGrant struct {
	Type       string `xml:"Grantee>Type"`
	ID         string `xml:"Grantee>ID"`
	URI        string `xml:"Grantee>URI"`
	Email      string `xml:"Grantee>EmailAddress"`
	Permission string `xml:"Permission"`
}

func (a *objectACL) equal(b *objectACL) bool {
	return a.owner == b.owner && reflect.DeepEqual(a.grants, b.grants)
}

// readACL reads the object's ACL, and reports why the object must not be
// rewritten when it cannot be read or holds anything a new object would not.
//
// A rewritten object gets the default ACL — its owner's full control and
// nothing else — because there is no way to pass an arbitrary ACL along
// without also handing the client-facing policy a decision it does not model.
// An object that has that ACL already loses nothing. Any other grant would be
// silently dropped, so such an object is left as it is. MinIO has no object
// ACLs, and answers with exactly the default.
func (r *repairer) readACL(ctx context.Context, target *url.URL) (*objectACL, string) {
	resp, err := r.send(ctx, http.MethodGet, target, "acl", nil, 0, sigv4.EmptyPayloadSHA256, nil)
	if err != nil {
		return nil, "its access control list could not be read: " + err.Error()
	}
	defer drain(resp)
	body, err := io.ReadAll(io.LimitReader(resp.Body, maxErrorBodySize))
	if err != nil || resp.StatusCode != http.StatusOK {
		return nil, fmt.Sprintf("its access control list could not be read (status %d)", resp.StatusCode)
	}
	var doc struct {
		Owner  string     `xml:"Owner>ID"`
		Grants []aclGrant `xml:"AccessControlList>Grant"`
	}
	if err := xml.Unmarshal(body, &doc); err != nil {
		return nil, "its access control list could not be parsed"
	}
	acl := &objectACL{owner: doc.Owner, grants: doc.Grants}
	if len(acl.grants) != 1 {
		return nil, "it has access grants beyond its owner's, which a rewrite would drop"
	}
	grant := acl.grants[0]
	if grant.Permission != "FULL_CONTROL" || grant.URI != "" || grant.Email != "" ||
		(grant.Type != "" && grant.Type != "CanonicalUser") || grant.ID != acl.owner {
		return nil, "it has access grants beyond its owner's, which a rewrite would drop"
	}
	return acl, ""
}

// uploadOwner is the owner the store records for an upload, which is the owner
// the object it completes will have. Empty when the store names none.
func (r *repairer) uploadOwner(ctx context.Context, target *url.URL, uploadID string) (string, error) {
	header := http.Header{}
	r.h.cfg.UpstreamSSECKey.Apply(header)
	resp, err := r.send(ctx, http.MethodGet, target, url.Values{"uploadId": {uploadID}}.Encode(), nil, 0, sigv4.EmptyPayloadSHA256, header)
	if err != nil {
		return "", err
	}
	defer drain(resp)
	body, err := io.ReadAll(io.LimitReader(resp.Body, maxErrorBodySize))
	if err != nil {
		return "", err
	}
	if resp.StatusCode != http.StatusOK {
		return "", fmt.Errorf("listing the upload's parts answered %d", resp.StatusCode)
	}
	var doc struct {
		Owner string `xml:"Owner>ID"`
	}
	if err := xml.Unmarshal(body, &doc); err != nil {
		return "", errors.New("listing the upload's parts returned an unreadable document")
	}
	return doc.Owner, nil
}

// invalidate drops whatever the caches hold for the object a request named,
// listings that cover it included.
func (h *Handler) invalidate(st *requestState) {
	if h.cfg.Cache != nil && st.cacheKey != "" {
		h.cfg.Cache.Invalidate(st.cacheKey)
	}
	if h.cfg.ListFallback != nil {
		if key := cacheKeyFor(st); key != "" {
			h.listingRefresh.schedule(h.cfg.ListFallback.Invalidate(key))
		}
	}
}

func payloadSHA256(body []byte) string {
	sum := sha256.Sum256(body)
	return hex.EncodeToString(sum[:])
}

type countingReader struct {
	r io.Reader
	n int64
}

func (c *countingReader) Read(p []byte) (int, error) {
	n, err := c.r.Read(p)
	c.n += int64(n)
	return n, err
}
