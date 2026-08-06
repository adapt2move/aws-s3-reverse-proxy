package s3

import (
	"bufio"
	"bytes"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"strings"
)

// This file is the aws-chunked upload framing and nothing else:
//
//	<hex-size>[;chunk-signature=…]\r\n<data>\r\n … 0\r\n<trailers>\r\n
//
// A proxy that re-signs a request cannot forward that framing — the
// streaming signature it belongs to does not survive re-signing, and an
// upstream that stored the bytes verbatim would hold a parquet file spelled
// `165D\r\nPAR1…\r\n\r\n`. So the framing is decoded here, either as a
// stream (ChunkedReader) or into a buffer when a checksum trailer has to be
// recovered from the end of it (DecodeChunked).

// ReadCapped reads at most max bytes and reports an error when the source has
// more, so a hostile or simply oversized body cannot decide how much memory
// this process allocates.
func ReadCapped(r io.Reader, max int64, what string) ([]byte, error) {
	buf, err := io.ReadAll(io.LimitReader(r, max+1))
	if err != nil {
		return nil, fmt.Errorf("%s: reading request body: %w", what, err)
	}
	if int64(len(buf)) > max {
		return nil, fmt.Errorf("%s: request body exceeds %d bytes", what, max)
	}
	return buf, nil
}

// IsChunkedUpload reports whether the incoming request carries an aws-chunked
// (streaming) request body, identified by an x-amz-content-sha256 of
// STREAMING-… (the AWS SigV4 streaming-upload markers). DuckDB's httpfs and
// the AWS SDKs use this for PUT / UploadPart bodies.
func IsChunkedUpload(req *http.Request) bool {
	if strings.HasPrefix(req.Header.Get("X-Amz-Content-Sha256"), "STREAMING-") {
		return true
	}
	return strings.Contains(strings.ToLower(req.Header.Get("Content-Encoding")), "aws-chunked")
}

// DecodedContentLength reads the size the client declares for the decoded
// body. Without it the proxy cannot state a Content-Length upstream and has
// to buffer instead of stream.
func DecodedContentLength(req *http.Request) (int64, bool) {
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

// StripStreamingMarkers removes the headers that describe aws-chunked
// framing, so they are neither copied upstream nor picked up by the signer
// once the framing is gone.
func StripStreamingMarkers(header http.Header) {
	stripChunkedEncoding(header)
	header.Del("X-Amz-Decoded-Content-Length")
	header.Del("X-Amz-Content-Sha256")
	header.Del("X-Amz-Trailer")
}

// stripChunkedEncoding removes the `aws-chunked` token from Content-Encoding
// once the framing has been decoded, keeping any other encoding the client
// applied underneath it (`aws-chunked,gzip` -> `gzip`; the payload really is
// still gzipped). The header is dropped entirely when nothing else remains.
func stripChunkedEncoding(header http.Header) {
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

// DropStaleBodyDigestHeaders removes the digest headers a client computed over
// a body the proxy replaced: the SigV4 payload hash and the flexible
// checksums. A header copy onto the upstream request would otherwise carry
// them past the signer, and the upstream would answer 400 BadDigest.
// Content-Md5 is left to the caller, which recomputes it.
func DropStaleBodyDigestHeaders(header http.Header) {
	for name := range header {
		if strings.HasPrefix(http.CanonicalHeaderKey(name), "X-Amz-Checksum-") {
			header.Del(name)
		}
	}
	header.Del("X-Amz-Sdk-Checksum-Algorithm")
	header.Del("X-Amz-Trailer")
	header.Del("X-Amz-Content-Sha256")
}

// maxChunkedTrailers bounds the trailing header lines accepted after the
// final chunk. A real client sends one (the flexible checksum), two with a
// trailer signature; the cap keeps a malformed or hostile stream from growing
// the map without end.
const maxChunkedTrailers = 16

// maxChunkedSizeLine bounds a single chunk-size line. It holds a hex length
// plus optional extensions; anything longer is malformed, and reading it
// unbounded would be a way to allocate memory for free.
const maxChunkedSizeLine = 4096

// ChunkedReader strips aws-chunked framing on the fly, so an upload of any
// size passes through in constant memory. Per-chunk signatures
// (STREAMING-AWS4-HMAC-SHA256-PAYLOAD) appear as chunk extensions and are
// ignored — the inbound signature was already verified from the headers, and
// only the payload bytes travel on.
//
// Trailing headers after the last chunk are consumed and dropped. A body that
// announces one has to be buffered with DecodeChunked instead, so nothing
// that matters is lost here.
type ChunkedReader struct {
	br        *bufio.Reader
	remaining int64
	done      bool
}

// NewChunkedReader wraps an aws-chunked body in a reader that yields the
// payload bytes.
func NewChunkedReader(r io.Reader) *ChunkedReader {
	return &ChunkedReader{br: bufio.NewReader(r)}
}

func (r *ChunkedReader) Read(p []byte) (int, error) {
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
			_, _ = readTrailers(r.br)
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
	line, err := readLine(br, maxChunkedSizeLine)
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

// DecodeChunked decodes an aws-chunked body into the raw object content and
// the trailing headers that follow it, refusing to buffer more than max bytes
// of payload.
//
// The trailers matter: with STREAMING-UNSIGNED-PAYLOAD-TRAILER the client's
// flexible checksum (x-amz-checksum-crc32 & co.) lives there and nowhere else,
// so dropping it would either lose the integrity check or — worse — leave the
// upstream waiting for a checksum that the de-chunked body no longer carries.
// See ChecksumTrailers for what to do with them.
func DecodeChunked(r io.Reader, max int64, what string) ([]byte, http.Header, error) {
	br := bufio.NewReader(r)
	var out bytes.Buffer
	for {
		size, err := readChunkSize(br)
		if err != nil {
			return nil, nil, fmt.Errorf("%s: %w", what, err)
		}
		if size == 0 {
			trailers, terr := readTrailers(br)
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

// readTrailers reads the `name:value` lines that follow the final (zero-size)
// chunk, up to the terminating empty line. A stream that simply ends after the
// last trailer — no closing empty line — is accepted too, since we have all
// the bytes either way.
func readTrailers(br *bufio.Reader) (http.Header, error) {
	trailers := http.Header{}
	for i := 0; ; i++ {
		line, err := readLine(br, maxChunkedSizeLine)
		if err != nil {
			if errors.Is(err, io.EOF) {
				return trailers, nil
			}
			return nil, fmt.Errorf("reading chunk trailer: %w", err)
		}
		if line == "" {
			return trailers, nil
		}
		if i >= maxChunkedTrailers {
			return nil, fmt.Errorf("more than %d chunk trailers", maxChunkedTrailers)
		}
		addTrailer(trailers, line)
	}
}

// addTrailer parses one `name:value` trailer line into h. Lines without a
// colon (or with an empty name) are skipped rather than rejected — they carry
// nothing this proxy acts on.
func addTrailer(h http.Header, line string) {
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

// ChecksumTrailers picks the flexible-checksum trailers (x-amz-checksum-crc32,
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
func ChecksumTrailers(trailers http.Header) http.Header {
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
