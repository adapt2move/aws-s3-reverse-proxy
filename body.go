package main

import (
	"bufio"
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"strings"
)

// preparedBody is what the upstream request will send: a reader, the exact
// number of bytes it will produce, and any header the preparation itself
// has to add (a checksum recovered from a chunk trailer, a recomputed
// Content-MD5).
//
// contentLength is authoritative. A body whose length we cannot state is
// buffered rather than forwarded with a chunked transfer encoding, which
// AWS S3 rejects outright on PUT.
type preparedBody struct {
	reader        io.Reader
	contentLength int64
	extraHeaders  http.Header
}

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
	chunked := isAwsChunkedUpload(req)

	if req.Body == nil || (req.ContentLength == 0 && !chunked) {
		if st.operation.kind == opDeleteObjects {
			return nil, "", fmt.Errorf("batch delete: empty request body")
		}
		return &preparedBody{}, emptyPayloadSHA256, nil
	}

	if chunked {
		return h.prepareChunkedBody(req, st)
	}

	if st.operation.kind == opDeleteObjects {
		raw, err := readCapped(req.Body, h.MaxDeleteBodySize, "batch delete")
		if err != nil {
			return nil, "", err
		}
		return h.prepareDeleteBody(raw, req.Header, st)
	}

	// The body travels upstream byte for byte, so the digest the client
	// signed still describes it exactly. Reusing it keeps the integrity
	// check end-to-end and costs no memory; without it we would have to
	// read the whole upload just to hash it.
	hash := unsignedPayload
	if v := req.Header.Get("X-Amz-Content-Sha256"); hexSHA256Regexp.MatchString(v) {
		hash = v
	}
	return &preparedBody{reader: req.Body, contentLength: req.ContentLength}, hash, nil
}

// prepareChunkedBody handles a body in aws-chunked framing
// (`<hex-size>[;chunk-signature=…]\r\n<data>\r\n`, ending `0\r\n…`). The
// framing has to go: we re-sign with a plain payload hash, which drops the
// streaming semantics, and the upstream would otherwise store the framed
// bytes verbatim — a parquet file arriving as `165D\r\nPAR1…\r\n\r\n`.
func (h *Handler) prepareChunkedBody(req *http.Request, st *requestState) (*preparedBody, string, error) {
	decodedLength, haveLength := decodedContentLength(req)
	// `x-amz-trailer` announces that the client's flexible checksum follows
	// the last chunk instead of travelling in a header. That trailer must
	// not reach the upstream — it would promise a checksum the de-chunked
	// body no longer carries — but the digest itself has to be carried over
	// by hand, which means seeing the end of the body before sending the
	// beginning of it.
	trailerAnnounced := req.Header.Get("X-Amz-Trailer") != ""
	stripStreamingMarkers(req.Header)

	if !trailerAnnounced && haveLength && st.operation.kind != opDeleteObjects {
		return &preparedBody{
			reader:        newAwsChunkedReader(req.Body),
			contentLength: decodedLength,
		}, unsignedPayload, nil
	}

	limit := h.MaxChunkedBodySize
	what := "aws-chunked body"
	if st.operation.kind == opDeleteObjects {
		limit, what = h.MaxDeleteBodySize, "batch delete"
	}
	decoded, trailers, err := decodeAwsChunked(req.Body, limit, what)
	if err != nil {
		return nil, "", err
	}

	if st.operation.kind == opDeleteObjects {
		return h.prepareDeleteBody(decoded, req.Header, st)
	}

	sum := sha256.Sum256(decoded)
	return &preparedBody{
		reader:        bytes.NewReader(decoded),
		contentLength: int64(len(decoded)),
		extraHeaders:  checksumTrailers(trailers),
	}, hex.EncodeToString(sum[:]), nil
}

// decodedContentLength reads the size the client declares for the decoded
// body. Without it we cannot state a Content-Length upstream and have to
// buffer instead of stream.
func decodedContentLength(req *http.Request) (int64, bool) {
	v := req.Header.Get("X-Amz-Decoded-Content-Length")
	if v == "" {
		return 0, false
	}
	n, err := strconv.ParseInt(v, 10, 64)
	if err != nil || n < 0 {
		return 0, false
	}
	return n, true
}

// stripStreamingMarkers removes the headers that describe aws-chunked
// framing, so they are neither copied upstream nor picked up by the signer
// once the framing is gone.
func stripStreamingMarkers(header http.Header) {
	stripAwsChunkedEncoding(header)
	header.Del("X-Amz-Decoded-Content-Length")
	header.Del("X-Amz-Content-Sha256")
	header.Del("X-Amz-Trailer")
}

// readCapped reads at most max bytes and reports an error when the source
// has more, so a hostile or simply oversized body cannot decide how much
// memory this process allocates.
func readCapped(r io.Reader, max int64, what string) ([]byte, error) {
	buf, err := io.ReadAll(io.LimitReader(r, max+1))
	if err != nil {
		return nil, fmt.Errorf("%s: reading request body: %w", what, err)
	}
	if int64(len(buf)) > max {
		return nil, fmt.Errorf("%s: request body exceeds %d bytes", what, max)
	}
	return buf, nil
}

// isAwsChunkedUpload reports whether the incoming request carries an
// aws-chunked (streaming) request body, identified by an x-amz-content-sha256
// of STREAMING-… (the AWS SigV4 streaming-upload markers). DuckDB's httpfs and
// the AWS SDKs use this for PUT / UploadPart bodies.
func isAwsChunkedUpload(req *http.Request) bool {
	if strings.HasPrefix(req.Header.Get("X-Amz-Content-Sha256"), "STREAMING-") {
		return true
	}
	return strings.Contains(strings.ToLower(req.Header.Get("Content-Encoding")), "aws-chunked")
}

// maxAwsChunkedTrailers bounds the trailing header lines we accept after the
// final chunk. A real client sends one (the flexible checksum), two with a
// trailer signature; the cap keeps a malformed or hostile stream from growing
// the map without end.
const maxAwsChunkedTrailers = 16

// maxAwsChunkedSizeLine bounds a single chunk-size line. It holds a hex
// length plus optional extensions; anything longer is malformed, and
// reading it unbounded would be a way to allocate memory for free.
const maxAwsChunkedSizeLine = 4096

// awsChunkedReader strips aws-chunked framing on the fly, so an upload of
// any size passes through in constant memory. Per-chunk signatures
// (STREAMING-AWS4-HMAC-SHA256-PAYLOAD) appear as chunk extensions and are
// ignored — the inbound signature was already verified from the headers,
// and only the payload bytes travel on.
//
// Trailing headers after the last chunk are consumed and dropped; a body
// that announces one is buffered instead (see prepareChunkedBody), so
// nothing that matters is lost here.
type awsChunkedReader struct {
	br        *bufio.Reader
	remaining int64
	done      bool
}

func newAwsChunkedReader(r io.Reader) *awsChunkedReader {
	return &awsChunkedReader{br: bufio.NewReader(r)}
}

func (r *awsChunkedReader) Read(p []byte) (int, error) {
	if r.done {
		return 0, io.EOF
	}
	if r.remaining == 0 {
		size, err := readChunkSize(r.br)
		if err != nil {
			return 0, err
		}
		if size == 0 {
			r.done = true
			// Drain whatever trailer lines follow so the connection is
			// left at a clean boundary.
			_, _ = readAwsChunkedTrailers(r.br)
			return 0, io.EOF
		}
		r.remaining = size
	}
	if int64(len(p)) > r.remaining {
		p = p[:r.remaining]
	}
	n, err := r.br.Read(p)
	r.remaining -= int64(n)
	if err != nil {
		if errors.Is(err, io.EOF) {
			return n, io.ErrUnexpectedEOF
		}
		return n, err
	}
	if r.remaining == 0 {
		if _, err := r.br.Discard(2); err != nil { // the CRLF after chunk data
			return n, fmt.Errorf("reading chunk terminator: %w", err)
		}
	}
	return n, nil
}

// readChunkSize parses one `<hex-size>[;extension…]` line.
func readChunkSize(br *bufio.Reader) (int64, error) {
	line, err := readLine(br, maxAwsChunkedSizeLine)
	if err != nil {
		return 0, fmt.Errorf("reading chunk size: %w", err)
	}
	sizeField := line
	if i := strings.IndexByte(sizeField, ';'); i >= 0 {
		sizeField = sizeField[:i] // drop chunk extensions (e.g. chunk-signature)
	}
	size, err := strconv.ParseInt(strings.TrimSpace(sizeField), 16, 64)
	if err != nil || size < 0 {
		return 0, fmt.Errorf("invalid chunk size %q", sizeField)
	}
	return size, nil
}

// readLine reads one CRLF-terminated line, refusing to grow past max.
func readLine(br *bufio.Reader, max int) (string, error) {
	var b strings.Builder
	for {
		chunk, isPrefix, err := br.ReadLine()
		if err != nil {
			return "", err
		}
		if b.Len()+len(chunk) > max {
			return "", fmt.Errorf("line longer than %d bytes", max)
		}
		b.Write(chunk)
		if !isPrefix {
			return b.String(), nil
		}
	}
}

// decodeAwsChunked decodes an aws-chunked body into the raw object content and
// the trailing headers that follow it, refusing to buffer more than max bytes
// of payload.
//
// The trailers matter: with STREAMING-UNSIGNED-PAYLOAD-TRAILER the client's
// flexible checksum (x-amz-checksum-crc32 & co.) lives there and nowhere else,
// so dropping it would either lose the integrity check or — worse — leave the
// upstream waiting for a checksum that the de-chunked body no longer carries.
// See checksumTrailers for what we do with them.
func decodeAwsChunked(r io.Reader, max int64, what string) ([]byte, http.Header, error) {
	br := bufio.NewReader(r)
	var out bytes.Buffer
	for {
		size, err := readChunkSize(br)
		if err != nil {
			return nil, nil, fmt.Errorf("%s: %w", what, err)
		}
		if size == 0 {
			trailers, terr := readAwsChunkedTrailers(br)
			if terr != nil {
				return nil, nil, fmt.Errorf("%s: %w", what, terr)
			}
			return out.Bytes(), trailers, nil
		}
		if int64(out.Len())+size > max {
			return nil, nil, fmt.Errorf("%s: decoded body exceeds %d bytes", what, max)
		}
		if _, err := io.CopyN(&out, br, size); err != nil {
			return nil, nil, fmt.Errorf("%s: reading chunk data: %w", what, err)
		}
		if _, err := br.Discard(2); err != nil { // consume the CRLF after chunk data
			return nil, nil, fmt.Errorf("%s: reading chunk terminator: %w", what, err)
		}
	}
}

// readAwsChunkedTrailers reads the `name:value` lines that follow the final
// (zero-size) chunk, up to the terminating empty line. A stream that simply
// ends after the last trailer — no closing empty line — is accepted too, since
// we have all the bytes either way.
func readAwsChunkedTrailers(br *bufio.Reader) (http.Header, error) {
	trailers := http.Header{}
	for i := 0; ; i++ {
		line, err := readLine(br, maxAwsChunkedSizeLine)
		if err != nil {
			if errors.Is(err, io.EOF) {
				return trailers, nil
			}
			return nil, fmt.Errorf("reading chunk trailer: %w", err)
		}
		if line == "" {
			return trailers, nil
		}
		if i >= maxAwsChunkedTrailers {
			return nil, fmt.Errorf("more than %d chunk trailers", maxAwsChunkedTrailers)
		}
		addAwsChunkedTrailer(trailers, line)
	}
}

// addAwsChunkedTrailer parses one `name:value` trailer line into h. Lines
// without a colon (or with an empty name) are skipped rather than rejected —
// they carry nothing we act on.
func addAwsChunkedTrailer(h http.Header, line string) {
	i := strings.IndexByte(line, ':')
	if i < 0 {
		return
	}
	name := strings.TrimSpace(line[:i])
	if name == "" {
		return
	}
	h.Set(name, strings.TrimSpace(line[i+1:]))
}

// checksumTrailers picks the flexible-checksum trailers (x-amz-checksum-crc32,
// -crc32c, -sha1, -sha256, …) out of a decoded aws-chunked body, so they can
// travel upstream as ordinary headers. The value is the client's own digest of
// the payload — computed over the object bytes, not over the chunk framing —
// which is exactly what the upstream receives once the framing is gone, so the
// end-to-end integrity check survives the re-signing.
//
// Everything else a client may append is dropped. x-amz-trailer-signature above
// all: it signs the framing bytes with the client's key, and neither the bytes
// nor the key exist on the upstream request.
//
// Returns nil when there is nothing to promote.
func checksumTrailers(trailers http.Header) http.Header {
	var out http.Header
	for name, values := range trailers {
		canonical := http.CanonicalHeaderKey(name)
		if !strings.HasPrefix(canonical, "X-Amz-Checksum-") {
			continue
		}
		if out == nil {
			out = http.Header{}
		}
		out[canonical] = values
	}
	return out
}

// stripAwsChunkedEncoding removes the `aws-chunked` token from Content-Encoding
// once the framing has been decoded, keeping any other encoding the client
// applied underneath it (`aws-chunked,gzip` -> `gzip`; the payload really is
// still gzipped). The header is dropped entirely when nothing else remains.
func stripAwsChunkedEncoding(header http.Header) {
	values, ok := header["Content-Encoding"]
	if !ok {
		return
	}
	var kept []string
	for _, value := range values {
		for _, token := range strings.Split(value, ",") {
			token = strings.TrimSpace(token)
			if token == "" || strings.EqualFold(token, "aws-chunked") {
				continue
			}
			kept = append(kept, token)
		}
	}
	if len(kept) == 0 {
		header.Del("Content-Encoding")
		return
	}
	header.Set("Content-Encoding", strings.Join(kept, ", "))
}

// dropStaleBodyDigestHeaders removes the digest headers a client computed over
// a body we replaced: the SigV4 payload hash and the flexible checksums.
// copyHeaderWithoutOverwrite would otherwise copy them onto the upstream
// request after signing, and the upstream would answer 400 BadDigest.
// Content-Md5 is left to the caller, which recomputes it.
func dropStaleBodyDigestHeaders(header http.Header) {
	for name := range header {
		if strings.HasPrefix(http.CanonicalHeaderKey(name), "X-Amz-Checksum-") {
			header.Del(name)
		}
	}
	header.Del("X-Amz-Sdk-Checksum-Algorithm")
	header.Del("X-Amz-Trailer")
	header.Del("X-Amz-Content-Sha256")
}
