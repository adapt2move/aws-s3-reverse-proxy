package proxy

// Conditional writes onto objects still stored in plaintext.
//
// MinIO evaluates the precondition of a write that carries SSE-C headers by
// reading the existing object *with* the request's key. Over a plaintext
// object that read fails, and so does the write: `PUT` with `If-Match` or
// `If-None-Match`, or a conditional `CompleteMultipartUpload`, is answered
// 400 "The encryption parameters are not applicable to this object" instead
// of 200 or 412. The same store evaluates the precondition of a completion
// that carries *no* key against the object as it is stored — which is what
// the repair relies on, and what both cases below rely on too.
//
//   - A conditional completion the store refuses that way is sent once more
//     without the key. Its body is buffered (see prepareCompleteBody), so
//     resending it costs nothing, and what is assembled is still encrypted:
//     encryption is decided by the upload's initiation and its parts, which
//     carried the key. Sending the key by default stays right for AWS S3,
//     which needs it on the completion of an upload with checksums.
//   - A conditional PUT streams its body and cannot be resent, so it is
//     decided up front: a keyless HEAD shows whether the target is
//     plaintext, and if it is, the PUT is carried out as a one-part
//     encrypted multipart upload whose completion carries the client's own
//     condition and no key. The store, not this process, evaluates the
//     condition, against the object the client was asking about; the
//     object that results is encrypted. Encrypting the target first and
//     then forwarding the PUT would not do: the rewrite changes the ETag,
//     and an If-Match the client computed from the object would fail.

import (
	"bytes"
	"context"
	"encoding/xml"
	"io"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"

	"github.com/Kriechi/aws-s3-reverse-proxy/internal/listcache"
	"github.com/Kriechi/aws-s3-reverse-proxy/internal/s3"
	"github.com/Kriechi/aws-s3-reverse-proxy/internal/sigv4"
	"github.com/Kriechi/aws-s3-reverse-proxy/internal/ssec"
)

// hasWriteCondition reports whether a write carries a precondition the object
// store has to evaluate against the existing object.
func hasWriteCondition(header http.Header) bool {
	return header.Get("If-Match") != "" || header.Get("If-None-Match") != ""
}

// isConditionalCompletion reports whether an upstream request is a
// CompleteMultipartUpload with a precondition.
func isConditionalCompletion(req *http.Request) bool {
	if req.Method != http.MethodPost || !hasWriteCondition(req.Header) {
		return false
	}
	query := req.URL.Query()
	query.Del("x-id")
	_, ok := query["uploadId"]
	return ok && len(query) == 1
}

// retryCompletionWithoutKey resends a conditional completion the store refused
// because the object it would replace is plaintext — see the top of this
// file. Anything else, including a body that cannot be produced again, keeps
// the store's answer.
func (h *Handler) retryCompletionWithoutKey(req *http.Request, refused *http.Response) *http.Response {
	if refused.StatusCode != http.StatusBadRequest || req.GetBody == nil || req.Header.Get(ssec.HeaderKey) == "" ||
		!refusedAsPlaintext(req.Method, refused) {
		return refused
	}
	body, err := req.GetBody()
	if err != nil {
		return refused
	}
	retry := req.Clone(req.Context())
	retry.Body = body
	ssec.Remove(retry.Header)
	retry.Header.Del("Authorization")
	retry.Header.Del("X-Amz-Date")
	if err := h.cfg.UpstreamSigner.Sign(retry, h.cfg.UpstreamRegion, time.Now()); err != nil {
		return refused
	}
	resp, err := h.transport.RoundTrip(retry)
	if err != nil {
		return refused
	}
	drain(refused)
	readLog(req, requestStateFrom(req.Context())).Info("completed a conditional multipart upload over a plaintext object without the key on the completion")
	return resp
}

// serveConditionalPutOverPlaintext carries out a conditional PUT onto a
// plaintext object as an encrypted one-part multipart upload, and reports
// whether it answered the request. It declines — and the PUT goes upstream
// the ordinary way — when there is no key, no condition, or the target is
// not shown to be plaintext.
func (h *Handler) serveConditionalPutOverPlaintext(w http.ResponseWriter, proxyReq *http.Request, st *requestState) bool {
	if h.cfg.UpstreamSSECKey == nil || st.operation.Kind != s3.PutObject || !hasWriteCondition(proxyReq.Header) {
		return false
	}
	probe, cancel := context.WithTimeout(proxyReq.Context(), diagnosticTimeout)
	_, plaintext := h.probePlaintext(probe, proxyReq)
	cancel()
	if !plaintext {
		return false
	}

	ctx := proxyReq.Context()
	target := *proxyReq.URL
	target.RawQuery = ""
	r := h.repairs
	logger := h.objectLog(st)

	// Until the completion is sent, nothing this does can change the object:
	// an upload that is never completed is abandoned below.
	st.settleWrite(listcache.Refused)
	uploadID, err := r.createUpload(ctx, &target, objectHeaders(proxyReq.Header))
	if err != nil {
		logger.WithError(err).Warn("a conditional write onto a plaintext object could not start its upload")
		s3.WriteError(w, http.StatusBadGateway, "InternalError", "The proxy could not write the object.")
		return true
	}
	completed := false
	defer func() {
		if !completed {
			r.abortUpload(&target, uploadID)
		}
	}()

	// The client's own body, digest and length, as one part. Content-MD5
	// travels with it; flexible checksums do not, because a part checksum
	// would have to be declared when the upload is started and repeated in
	// the part list.
	partHeader := http.Header{}
	h.cfg.UpstreamSSECKey.Apply(partHeader)
	if md5 := proxyReq.Header.Get("Content-Md5"); md5 != "" {
		partHeader.Set("Content-Md5", md5)
	}
	payloadHash := proxyReq.Header.Get("X-Amz-Content-Sha256")
	if payloadHash == "" {
		payloadHash = sigv4.UnsignedPayload
	}
	var body io.Reader
	if proxyReq.ContentLength > 0 {
		body = proxyReq.Body
	}
	part, err := r.h.send(ctx, http.MethodPut, &target, url.Values{"partNumber": {"1"}, "uploadId": {uploadID}}.Encode(),
		body, proxyReq.ContentLength, payloadHash, partHeader)
	if err != nil {
		logger.WithError(err).Warn("a conditional write onto a plaintext object could not upload its body")
		s3.WriteError(w, http.StatusBadGateway, "InternalError", "The proxy could not write the object.")
		return true
	}
	if part.StatusCode != http.StatusOK {
		h.relayError(w, part, st)
		return true
	}
	drain(part)
	h.checkWriteConfirmation(part, st)
	partETag, _ := s3.CanonicalETag(part.Header.Get("Etag"))

	var list bytes.Buffer
	list.WriteString("<CompleteMultipartUpload><Part><PartNumber>1</PartNumber><ETag>")
	_ = xml.EscapeText(&list, []byte(partETag))
	list.WriteString("</ETag></Part></CompleteMultipartUpload>")
	conditions := http.Header{"Content-Type": {"application/xml"}}
	for _, name := range []string{"If-Match", "If-None-Match"} {
		if v := proxyReq.Header.Get(name); v != "" {
			conditions.Set(name, v)
		}
	}
	finish, cancelFinish := context.WithTimeout(context.WithoutCancel(ctx), completeTimeout)
	defer cancelFinish()
	// From here the object may change, and until the answer is read whether
	// it did is not known.
	st.writeSettled = false
	resp, err := r.h.send(finish, http.MethodPost, &target, url.Values{"uploadId": {uploadID}}.Encode(),
		bytes.NewReader(list.Bytes()), int64(list.Len()), payloadSHA256(list.Bytes()), conditions)
	if err != nil {
		logger.WithError(err).Warn("a conditional write onto a plaintext object could not complete its upload")
		s3.WriteError(w, http.StatusBadGateway, "InternalError", "The proxy could not write the object.")
		return true
	}
	result, readErr := io.ReadAll(io.LimitReader(resp.Body, maxErrorBodySize))
	_ = resp.Body.Close()
	if resp.StatusCode != http.StatusOK || readErr != nil || bytes.Contains(result, []byte("<Error>")) {
		status := resp.StatusCode
		if status == http.StatusOK {
			// A completion that failed after it started: S3 put the error in
			// a 200's body.
			status = http.StatusInternalServerError
			if bytes.Contains(result, []byte("PreconditionFailed")) {
				status = http.StatusPreconditionFailed
			}
		}
		if readErr == nil {
			st.settleWrite(writeOutcome(status))
		}
		h.writeRelayedError(w, status, resp.Header.Get("Content-Type"), result, st)
		return true
	}
	completed = true
	st.settleWrite(listcache.Answered)

	// Answer like a PUT: the ETag, and nothing about the upload behind it.
	header := http.Header{}
	etag := resp.Header.Get("Etag")
	if etag == "" {
		var doc struct {
			ETag string `xml:"ETag"`
		}
		if xml.Unmarshal(result, &doc) == nil {
			etag = doc.ETag
		}
	}
	if etag != "" {
		header.Set("Etag", s3.NormalizeETag(etag))
	}
	if date := resp.Header.Get("Date"); date != "" {
		header.Set("Date", date)
	}
	h.finishMutation(&http.Response{StatusCode: http.StatusOK, Header: header}, st)
	st.mutation.End()
	for name, values := range header {
		w.Header()[name] = values
	}
	w.Header().Set("Content-Length", "0")
	w.WriteHeader(http.StatusOK)
	logger.Info("wrote a conditional PUT onto a plaintext object as an encrypted upload")
	return true
}

// objectHeaders are the request headers that describe the object a PUT
// creates, for the upload that creates it instead.
func objectHeaders(header http.Header) http.Header {
	out := http.Header{}
	for name, values := range header {
		switch canonical := http.CanonicalHeaderKey(name); {
		case canonical == "Content-Type", canonical == "Content-Encoding", canonical == "Content-Disposition",
			canonical == "Content-Language", canonical == "Cache-Control", canonical == "Expires",
			strings.HasPrefix(canonical, "X-Amz-Meta-"):
			out[canonical] = values
		}
	}
	return out
}

// relayError passes an upstream error response on to the client.
func (h *Handler) relayError(w http.ResponseWriter, resp *http.Response, st *requestState) {
	body, _ := io.ReadAll(io.LimitReader(resp.Body, maxErrorBodySize))
	_ = resp.Body.Close()
	h.writeRelayedError(w, resp.StatusCode, resp.Header.Get("Content-Type"), body, st)
}

// writeRelayedError writes an upstream error document with the tenant prefix
// taken back out of it, as the response rewrite would have.
func (h *Handler) writeRelayedError(w http.ResponseWriter, status int, contentType string, body []byte, st *requestState) {
	body = s3.StripKeyPrefix(h.sanitizeErrorBody(body), st.identity.KeyPrefix)
	if contentType != "" {
		w.Header().Set("Content-Type", contentType)
	}
	w.Header().Set("Content-Length", strconv.Itoa(len(body)))
	w.WriteHeader(status)
	_, _ = w.Write(body)
}
