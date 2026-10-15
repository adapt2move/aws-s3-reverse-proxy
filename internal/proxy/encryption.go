package proxy

// Upstream encryption with the operator's customer key (SSE-C).
//
// The object store does the encrypting; what this file decides is which
// requests carry the key, what a response says about whether it was applied,
// and what happens when a read meets an object stored before there was a key.
// Clients are not part of any of it: they never send a key (one that tries is
// refused in s3.Classify), never receive one, and never see the response
// headers that would tell them a key was involved.
//
// One rule runs through all of it: encryption is requested and monitored, it
// does not decide whether a request succeeds. A write the object store accepted
// without confirming encryption is still a write it accepted — the client gets
// its success and the operator gets a warning. Failing it would not un-store
// the object, and retrying it without a key would store it in plaintext on
// purpose. An actual error from the store stays an error.

import (
	"bytes"
	"context"
	"encoding/xml"
	"io"
	"net/http"
	"net/url"
	"strings"
	"time"

	log "github.com/sirupsen/logrus"

	"github.com/Kriechi/aws-s3-reverse-proxy/internal/s3"
	"github.com/Kriechi/aws-s3-reverse-proxy/internal/sigv4"
	"github.com/Kriechi/aws-s3-reverse-proxy/internal/ssec"
)

const (
	// unconfirmedMarker travels on a response whose object is not known to
	// be stored encrypted: one the proxy had to fetch without the key
	// because the object is plaintext, a read the store answered without
	// confirming the key, and an upload it accepted without confirming it.
	// It reaches the cache — so a hit on that entry still has the object's
	// state checked, and its encryption scheduled, instead of hiding both
	// for as long as the entry lives — and never the client. It records a
	// question, not an answer: the repair establishes the object's state
	// itself before it writes anything.
	unconfirmedMarker = "X-S3proxy-Encryption-Unconfirmed"

	// maxErrorBodySize bounds the error document read to classify a refused
	// read. S3 error bodies are a few hundred bytes.
	maxErrorBodySize = 64 << 10

	// diagnosticTimeout bounds the extra HEAD that establishes whether a
	// refused object is plaintext. It is on a client's critical path.
	diagnosticTimeout = 10 * time.Second
)

type roundTripperFunc func(*http.Request) (*http.Response, error)

func (f roundTripperFunc) RoundTrip(r *http.Request) (*http.Response, error) { return f(r) }

// carriesCustomerKey reports whether an operation's upstream request takes
// the key.
//
// Everything that reads or writes object bytes does: the object's own
// GET/HEAD/PUT, and every multipart step that S3 lets carry one —
// initiation and parts, where it is required, and completion and the part
// listing, where AWS requires it for an upload with checksums and accepts it
// otherwise. Aborting an upload, deleting and listing never touch object
// bytes, and AWS rejects the headers on some of them, so those go without.
func carriesCustomerKey(kind s3.Kind) bool {
	switch kind {
	case s3.GetObject, s3.HeadObject, s3.PutObject,
		s3.CreateMultipartUpload, s3.UploadPart, s3.CompleteMultipartUpload, s3.ListParts:
		return true
	}
	return false
}

// confirmsEncryption reports whether a successful response to an operation is
// one the object store confirms SSE-C on — and so one where a missing
// confirmation means something.
//
// PutObject and UploadPart are, on AWS S3 and MinIO alike. A multipart
// upload's encryption is confirmed by its parts: MinIO does not echo the
// customer-key headers on CreateMultipartUpload, and no store does on
// CompleteMultipartUpload, so their absence there says nothing.
func confirmsEncryption(kind s3.Kind) bool {
	switch kind {
	case s3.PutObject, s3.UploadPart:
		return true
	}
	return false
}

// applyCustomerKey adds the key to an upstream request for an operation that
// takes one. It runs before signing, so the headers are covered by the
// upstream signature like every other x-amz- header.
func (h *Handler) applyCustomerKey(header http.Header, kind s3.Kind) {
	if h.cfg.UpstreamSSECKey != nil && carriesCustomerKey(kind) {
		h.cfg.UpstreamSSECKey.Apply(header)
	}
}

// checkWriteConfirmation warns when the object store accepted a write without
// confirming the encryption it was asked for.
//
// A warning and nothing more, deliberately: the store said yes, the object
// exists, and the client is owed that answer. The cache treats the write like
// any other successful one, too — what is cached is the object's bytes, which
// the client sent and the store accepted.
func (h *Handler) checkWriteConfirmation(resp *http.Response, st *requestState) {
	key := h.cfg.UpstreamSSECKey
	if key == nil || !confirmsEncryption(st.operation.Kind) || resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return
	}
	switch key.Check(resp.Header) {
	case ssec.Unconfirmed:
		st.unconfirmed = true
		h.objectLog(st).Warn("the object store accepted a write without confirming SSE-C; the object may be stored unencrypted")
	case ssec.Mismatched:
		st.unconfirmed = true
		h.objectLog(st).Warn("the object store confirmed a write with SSE-C parameters other than the configured key's")
	}
}

// objectLog is a log entry naming the object a request is about. It never
// carries a header, so it cannot carry the key.
func (h *Handler) objectLog(st *requestState) *log.Entry {
	fields := log.Fields{"operation": string(st.operation.Kind), "bucket": st.operation.Bucket}
	if st.identity != nil {
		fields["tenant"] = st.identity.Tenant
	}
	if st.operation.Key != "" {
		fields["key"] = st.operation.Key
	}
	return log.WithFields(fields)
}

// hideEncryption takes the customer-key headers and the legacy marker off a
// response on its way to a client. The client asked for an object, not for
// encryption, and the key digest the store confirmed is the operator's.
func hideEncryption(header http.Header) {
	ssec.StripCustomerHeaders(header)
	header.Del(unconfirmedMarker)
}

// roundTrip sends one upstream request. It is the reverse proxy's transport
// and the one the proxy's own reads go through, so that an object read is
// interpreted the same way whoever made it.
func (h *Handler) roundTrip(req *http.Request) (*http.Response, error) {
	resp, err := h.transport.RoundTrip(req)
	if err != nil {
		return resp, err
	}
	if h.cfg.UpstreamSSECKey != nil && isConditionalCompletion(req) {
		return h.retryCompletionWithoutKey(req, resp), nil
	}
	if !isObjectRead(req) {
		return resp, nil
	}
	return h.inspectRead(req, resp), nil
}

// isObjectRead reports whether an upstream request is a GET or HEAD of one
// object — the requests whose answer depends on how the object is stored.
//
// The SDKs' `?x-id=GetObject` marker is the one query parameter such a read
// may carry: it is forwarded as the client signed it, and names nothing.
func isObjectRead(req *http.Request) bool {
	if req.Method != http.MethodGet && req.Method != http.MethodHead {
		return false
	}
	if req.URL.RawQuery != "" {
		query := req.URL.Query()
		query.Del("x-id")
		if len(query) > 0 {
			return false
		}
	}
	_, key, found := strings.Cut(strings.TrimPrefix(req.URL.Path, "/"), "/")
	return found && key != ""
}

// inspectRead interprets an object read's response before anyone sees it.
//
// With a key configured, three outcomes need more than passing the response
// on:
//
//   - 400 that looks like "this object is not encrypted": possibly a legacy
//     plaintext object, which some stores (MinIO among them) refuse to read
//     with SSE-C headers instead of ignoring them. readPlaintext establishes
//     whether it is, and if so serves it without the key.
//   - 403: what a store answers for an object encrypted under a different
//     key. Logged as an error; never retried without the key.
//   - success without confirmation: the store may have ignored the key.
//     Nothing to fix in the response, but the object may be plaintext, so
//     its encryption is scheduled — the repair establishes the state first.
//
// Without a key, the one thing worth doing is noticing that an object is
// encrypted and this proxy cannot read it, which is an operator's problem
// with a clear cause.
func (h *Handler) inspectRead(req *http.Request, resp *http.Response) *http.Response {
	st := requestStateFrom(req.Context())
	key := h.cfg.UpstreamSSECKey
	if key == nil {
		h.reportUnreadableWithoutKey(req, resp, st)
		return resp
	}
	switch {
	case resp.StatusCode == http.StatusBadRequest:
		if !refusedAsPlaintext(req.Method, resp) {
			readLog(req, st).WithField("status", resp.StatusCode).Error("the object store refused a read with the configured SSE-C key")
			return resp
		}
		return h.readPlaintext(req, resp, st)
	case resp.StatusCode == http.StatusForbidden:
		readLog(req, st).WithField("status", resp.StatusCode).
			Error("the object store refused a read with the configured SSE-C key: the object is encrypted with another key, or the proxy's credentials may not read it")
	case resp.StatusCode == http.StatusOK || resp.StatusCode == http.StatusPartialContent:
		switch key.Check(resp.Header) {
		case ssec.Confirmed:
		case ssec.Mismatched:
			readLog(req, st).Warn("the object store answered a read with SSE-C parameters other than the configured key's")
		case ssec.Unconfirmed:
			// The store answered without confirming the key. That is not
			// proof the object is plaintext — only the repair's own probe
			// can establish that — so the response is served as it is, and
			// marked so a cached copy of it keeps asking.
			resp.Header.Set(unconfirmedMarker, "1")
			h.repairs.schedule(st, req.URL, resp.Header.Get("Etag"), "a read with the key was answered without confirming it")
		}
	}
	return resp
}

// reportUnreadableWithoutKey logs a read that failed because the object is
// encrypted and no key is configured — the case where a deployment lost its
// key, or never had the one the bucket was written with.
func (h *Handler) reportUnreadableWithoutKey(req *http.Request, resp *http.Response, st *requestState) {
	if resp.StatusCode != http.StatusBadRequest {
		return
	}
	if req.Method == http.MethodHead {
		readLog(req, st).WithField("status", resp.StatusCode).
			Error("the object store refused a HEAD with 400; if the object is SSE-C encrypted, this proxy has no key configured for it")
		return
	}
	if code, message := peekError(resp); code == "InvalidRequest" && strings.Contains(strings.ToLower(message), "server side encryption") {
		readLog(req, st).WithField("status", resp.StatusCode).
			Error("the object is stored with server-side encryption that needs a customer key, and this proxy has none configured")
	}
}

// readLog is objectLog for a request that may not have a requestState — a
// read the proxy made on its own carries one, but a test fixture may not.
func readLog(req *http.Request, st *requestState) *log.Entry {
	if st != nil && st.operation != nil {
		entry := log.WithFields(log.Fields{"operation": string(st.operation.Kind), "bucket": st.operation.Bucket, "key": st.operation.Key})
		if st.identity != nil {
			entry = entry.WithField("tenant", st.identity.Tenant)
		}
		return entry.WithField("method", req.Method)
	}
	return log.WithField("method", req.Method)
}

// refusedAsPlaintext reports whether a 400 to a read with the key is the one
// that means "this object is not encrypted with a customer key".
//
// For a GET that is narrow: S3 and MinIO both answer InvalidRequest with "The
// encryption parameters are not applicable to this object". A HEAD carries no
// body, so its 400 cannot be told apart from any other — which is fine,
// because this only decides whether to *ask*. The diagnostic in
// readPlaintext is what establishes the object's state; a 400 alone never
// does.
func refusedAsPlaintext(method string, resp *http.Response) bool {
	if method == http.MethodHead {
		return true
	}
	code, message := peekError(resp)
	return code == "InvalidRequest" && strings.Contains(strings.ToLower(message), "encryption parameters are not applicable")
}

// peekError reads an error response's code and message, and puts the body
// back so the response can still be forwarded as it was.
func peekError(resp *http.Response) (code, message string) {
	if resp.Body == nil {
		return "", ""
	}
	raw, err := io.ReadAll(io.LimitReader(resp.Body, maxErrorBodySize))
	rest := resp.Body
	resp.Body = struct {
		io.Reader
		io.Closer
	}{io.MultiReader(bytes.NewReader(raw), rest), rest}
	if err != nil {
		return "", ""
	}
	var doc struct {
		Code    string `xml:"Code"`
		Message string `xml:"Message"`
	}
	if xml.Unmarshal(raw, &doc) != nil {
		return "", ""
	}
	return doc.Code, doc.Message
}

// readPlaintext serves a read the object store refused with the key, when —
// and only when — the object turns out to be stored without one.
//
// The evidence is a HEAD without the key: an object encrypted with any
// customer key cannot be read that way, so a 200 that names no customer
// algorithm means the object is not SSE-C encrypted. Anything else — an
// error, a timeout, a response that does name one — leaves the state unknown,
// and the client gets the store's original refusal: never a plaintext
// fallback on a guess.
//
// Once established, the client's own request is sent again without the key,
// with its ranges and conditions intact, and that answer is what the client
// gets. The object's encryption is scheduled in the background; the read does
// not wait for it.
func (h *Handler) readPlaintext(req *http.Request, refused *http.Response, st *requestState) *http.Response {
	ctx, cancel := context.WithTimeout(req.Context(), diagnosticTimeout)
	defer cancel()
	state, plaintext := h.probePlaintext(ctx, req)
	if !plaintext {
		readLog(req, st).Error("the object store refused a read with the configured SSE-C key, and the object could not be shown to be stored without one")
		return refused
	}

	retry := req.Clone(req.Context())
	ssec.Remove(retry.Header)
	retry.Header.Del("Authorization")
	retry.Header.Del("X-Amz-Date")
	if retry.Header.Get("X-Amz-Content-Sha256") == "" {
		retry.Header.Set("X-Amz-Content-Sha256", sigv4.EmptyPayloadSHA256)
	}
	if err := h.cfg.UpstreamSigner.Sign(retry, h.cfg.UpstreamRegion, time.Now()); err != nil {
		return refused
	}
	resp, err := h.transport.RoundTrip(retry)
	if err != nil {
		readLog(req, st).WithError(err).Warn("reading a plaintext object without the key failed")
		return refused
	}
	drain(refused)
	if resp.StatusCode == http.StatusOK || resp.StatusCode == http.StatusPartialContent {
		resp.Header.Set(unconfirmedMarker, "1")
	}
	readLog(req, st).Info("served an object stored without SSE-C")
	h.repairs.scheduleObserved(st, req.URL, state)
	return resp
}

// probePlaintext sends the diagnostic HEAD without the key, and reports what
// it found and whether that shows the object is stored without SSE-C.
func (h *Handler) probePlaintext(ctx context.Context, req *http.Request) (*objectState, bool) {
	resp, err := h.plainRequest(ctx, http.MethodHead, req.URL, nil)
	if err != nil {
		return nil, false
	}
	drain(resp)
	if resp.StatusCode != http.StatusOK || resp.Header.Get(ssec.HeaderAlgorithm) != "" {
		return nil, false
	}
	return observeState(resp), true
}

// plainRequest sends a signed, body-less request for an object without the
// key and without anything a client asked for.
func (h *Handler) plainRequest(ctx context.Context, method string, target *url.URL, extra http.Header) (*http.Response, error) {
	u := *target
	u.RawQuery = ""
	req, err := http.NewRequestWithContext(ctx, method, u.String(), nil)
	if err != nil {
		return nil, err
	}
	for name, values := range extra {
		req.Header[name] = values
	}
	req.Header.Set("X-Amz-Content-Sha256", sigv4.EmptyPayloadSHA256)
	if err := h.cfg.UpstreamSigner.Sign(req, h.cfg.UpstreamRegion, time.Now()); err != nil {
		return nil, err
	}
	return h.transport.RoundTrip(req)
}

// drain empties and closes a response the caller is done with, so its
// connection goes back to the pool.
func drain(resp *http.Response) {
	if resp == nil || resp.Body == nil {
		return
	}
	_, _ = io.Copy(io.Discard, io.LimitReader(resp.Body, maxErrorBodySize))
	_ = resp.Body.Close()
}
