package proxy

import (
	"bytes"
	"crypto/md5"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"net/http"

	"github.com/Kriechi/aws-s3-reverse-proxy/internal/s3"
	"github.com/Kriechi/aws-s3-reverse-proxy/internal/sigv4"
)

// preparedBody is what the upstream request will send: a reader, the exact
// number of bytes it will produce, and any header the preparation itself has
// to add (a checksum recovered from a chunk trailer, a recomputed
// Content-MD5).
//
// contentLength is authoritative, and prepareBody refuses any request whose
// length it cannot state: forwarding one would make Go fall back to a chunked
// transfer encoding, which AWS S3 rejects outright on PUT.
type preparedBody struct {
	reader        io.Reader
	contentLength int64
	extraHeaders  http.Header
}

// errAllKeysDenied signals that policy refused every key in a batch. There is
// no upstream call left to make, so the caller answers directly.
var errAllKeysDenied = errors.New("batch delete: no key survived authorization")

// prepareBody decides, per request, between streaming the body through and
// buffering it — and returns the payload hash the upstream signature will
// commit to.
//
// Streaming is the default: an object upload passes through with per-request
// memory bounded by the copy buffer, whatever the object's size. Two shapes
// have to be buffered, and both carry an explicit, configurable cap:
//
//   - a batch delete, whose object keys live in the body and have to be
//     authorized and prefixed one by one
//   - an aws-chunked body that announces a checksum trailer, because the
//     digest arrives *after* the payload while the upstream needs it in a
//     header before the payload
func (h *Handler) prepareBody(req *http.Request, st *requestState) (*preparedBody, string, error) {
	chunked := s3.IsChunkedUpload(req)

	// A negative ContentLength means the client sent a plain
	// `Transfer-Encoding: chunked` body and never said how long it is. There
	// is nothing to forward it as except another chunked transfer encoding,
	// which S3 refuses on PUT, so refuse it here where the reason can be
	// stated. (An aws-chunked body is a different thing: it announces its
	// decoded length in a header, and is handled below.)
	if req.Body != nil && req.ContentLength < 0 && !chunked {
		return nil, "", fmt.Errorf("request body has no Content-Length")
	}

	if req.Body == nil || (req.ContentLength == 0 && !chunked) {
		if st.operation.Kind == s3.DeleteObjects {
			return nil, "", fmt.Errorf("batch delete: empty request body")
		}
		return &preparedBody{}, sigv4.EmptyPayloadSHA256, nil
	}

	if chunked {
		return h.prepareChunkedBody(req, st)
	}

	if st.operation.Kind == s3.DeleteObjects {
		raw, err := s3.ReadCapped(req.Body, h.cfg.Limits.DeleteBody, "batch delete")
		if err != nil {
			return nil, "", err
		}
		return h.prepareDeleteBody(raw, req.Header, st)
	}

	// The body travels upstream byte for byte, so the digest the client
	// signed still describes it exactly. Reusing it keeps the integrity check
	// end-to-end and costs no memory; without it we would have to read the
	// whole upload just to hash it.
	hash := sigv4.UnsignedPayload
	if v := req.Header.Get("X-Amz-Content-Sha256"); sigv4.IsPayloadHash(v) {
		hash = v
	}
	return &preparedBody{reader: req.Body, contentLength: req.ContentLength}, hash, nil
}

// prepareChunkedBody handles a body in aws-chunked framing. The framing has to
// go: we re-sign with a plain payload hash, which drops the streaming
// semantics, and the upstream would otherwise store the framed bytes verbatim.
func (h *Handler) prepareChunkedBody(req *http.Request, st *requestState) (*preparedBody, string, error) {
	decodedLength, haveLength := s3.DecodedContentLength(req)
	// `x-amz-trailer` announces that the client's flexible checksum follows
	// the last chunk instead of travelling in a header. That trailer must not
	// reach the upstream — it would promise a checksum the de-chunked body no
	// longer carries — but the digest itself has to be carried over by hand,
	// which means seeing the end of the body before sending the beginning of
	// it.
	trailerAnnounced := req.Header.Get("X-Amz-Trailer") != ""
	s3.StripStreamingMarkers(req.Header)

	if !trailerAnnounced && haveLength && st.operation.Kind != s3.DeleteObjects {
		return &preparedBody{
			reader:        s3.NewChunkedReader(req.Body),
			contentLength: decodedLength,
		}, sigv4.UnsignedPayload, nil
	}

	limit := h.cfg.Limits.ChunkedBody
	what := "aws-chunked body"
	if st.operation.Kind == s3.DeleteObjects {
		limit, what = h.cfg.Limits.DeleteBody, "batch delete"
	}
	decoded, trailers, err := s3.DecodeChunked(req.Body, limit, what)
	if err != nil {
		return nil, "", err
	}

	if st.operation.Kind == s3.DeleteObjects {
		return h.prepareDeleteBody(decoded, req.Header, st)
	}

	sum := sha256.Sum256(decoded)
	return &preparedBody{
		reader:        bytes.NewReader(decoded),
		contentLength: int64(len(decoded)),
		extraHeaders:  s3.ChecksumTrailers(trailers),
	}, hex.EncodeToString(sum[:]), nil
}

// prepareDeleteBody authorizes every key of a batch delete individually,
// prefixes the survivors and rebuilds the request body from them.
//
// This is the only place authorization runs per key rather than per request,
// because it is the only operation S3 answers per key: a key the caller may
// not delete becomes an AccessDenied entry in the result rather than a
// rejection of the whole request.
func (h *Handler) prepareDeleteBody(raw []byte, reqHeader http.Header, st *requestState) (*preparedBody, string, error) {
	parsed, err := s3.ParseDeleteRequest(raw)
	if err != nil {
		return nil, "", err
	}

	// A batch matches as many rules as it has keys, but the access log
	// carries one. Report the rule behind the first denial when there is one
	// — that is the line an operator is trying to explain — and the rule the
	// batch matched otherwise.
	var firstRule, firstDenyRule string
	var denied bool

	allowed := make([]s3.DeleteEntry, 0, len(parsed.Objects))
	for i, obj := range parsed.Objects {
		// A malformed key is refused for the whole batch rather than turned
		// into a per-key denial: it is not an authorization outcome but a
		// request we cannot safely interpret at all.
		if err := s3.ValidateObjectKey(obj.Key); err != nil {
			return nil, "", fmt.Errorf("batch delete: %w", err)
		}
		decision := st.policy.Authorize(st.identity.Level, obj.Key, http.MethodDelete)
		if i == 0 {
			firstRule = decision.Rule
		}
		if !decision.Allowed {
			if !denied {
				denied, firstDenyRule = true, decision.Rule
			}
			st.deniedDeletes = append(st.deniedDeletes, obj.Key)
			continue
		}
		obj.Key = st.identity.KeyPrefix + obj.Key
		if h.cfg.Cache != nil {
			st.deletedKeys = append(st.deletedKeys, upstreamKeyFor(st.operation.Bucket, obj.Key))
		}
		allowed = append(allowed, obj)
	}
	st.rule = firstRule
	if denied {
		st.rule = firstDenyRule
	}
	if len(allowed) == 0 {
		return nil, "", errAllKeysDenied
	}

	body := s3.BuildDeleteRequest(parsed.Quiet, allowed)
	// The client's digests describe the body it sent, not the one we are
	// about to send. S3 requires a Content-MD5 on a batch delete, so that one
	// is recomputed; the rest are dropped before they can be copied onto the
	// upstream request.
	s3.DropStaleBodyDigestHeaders(reqHeader)
	sum := md5.Sum(body)
	payload := sha256.Sum256(body)
	return &preparedBody{
		reader:        bytes.NewReader(body),
		contentLength: int64(len(body)),
		extraHeaders:  http.Header{"Content-Md5": {base64.StdEncoding.EncodeToString(sum[:])}},
	}, hex.EncodeToString(payload[:]), nil
}
