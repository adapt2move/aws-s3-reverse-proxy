package main

import (
	"bufio"
	"bytes"
	"crypto/subtle"
	"fmt"
	"io"
	"io/ioutil"
	"net"
	"net/http"
	"net/http/httputil"
	"net/url"
	"regexp"
	"strconv"
	"strings"
	"time"

	v4 "github.com/aws/aws-sdk-go/aws/signer/v4"
	log "github.com/sirupsen/logrus"
)

// - new less strict regexp in order to allow different region naming (compatibility with other providers)
// - east-eu-1 => pass (aws style)
// - gra => pass (ceph style)
// - "" => pass (some S3 clients, e.g. DuckDB's httpfs, leave the region
//   segment empty when no region is configured; the signature is still
//   valid, it just has an empty region scope)
var awsAuthorizationCredentialRegexp = regexp.MustCompile("Credential=([a-zA-Z0-9]+)/[0-9]+/([a-zA-Z-0-9]*)/s3/aws4_request")
var awsAuthorizationSignedHeadersRegexp = regexp.MustCompile("SignedHeaders=([a-zA-Z0-9;-]+)")

// Handler is a special handler that re-signs any AWS S3 request and sends it upstream
type Handler struct {
	// Print debug information
	Debug bool

	// When true, only read methods (GET, HEAD) are proxied; every mutating
	// method (PUT, POST, DELETE, PATCH) is rejected with 403 before the request
	// is signed or forwarded. This is an upstream-credential-independent
	// safety boundary: even a fully valid write request never reaches S3.
	ReadOnly bool

	// Optional: a set of object-key prefixes that are protected from
	// mutation. A mutating request (PUT, POST, DELETE, PATCH) whose object
	// key starts with any of these prefixes is rejected with 403 before the
	// request is signed or forwarded — exactly like ReadOnly, but scoped to
	// the listed prefixes instead of the whole bucket. Reads are always
	// allowed. The prefixes are matched against the client-facing object key
	// (i.e. before any KeyPrefix is prepended). Empty disables the feature.
	ReadOnlyKeyPrefixes []string

	// http or https
	UpstreamScheme string

	// Upstream S3 endpoint URL
	UpstreamEndpoint string

	// Allowed endpoint, i.e., Host header to accept incoming requests from
	AllowedSourceEndpoint string

	// Allowed source IPs and subnets for incoming requests
	AllowedSourceSubnet []*net.IPNet

	// AWS Credentials, AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY
	AWSCredentials map[string]string

	// AWS Signature v4
	Signers map[string]*v4.Signer

	// Optional: prepend this prefix to every object key (and to the
	// `prefix` query parameter of bucket-level listings) before the
	// request is sent upstream. Empty disables the feature.
	KeyPrefix string

	// Optional: when set, upstream requests are re-signed with these
	// credentials instead of the client's. This lets the proxy hold
	// credentials the client never needs to know.
	UpstreamSigner *v4.Signer

	// Optional: when set, upstream requests are signed for this region
	// instead of the region from the client's request. Useful when the
	// client signs with a placeholder (or empty) region but the real
	// backend expects a specific one.
	UpstreamRegion string

	// Reverse Proxy
	Proxy *httputil.ReverseProxy
}

// isBucketLevelPath reports whether a path-style S3 request path addresses
// a bucket itself (e.g. "/my-bucket" or "/") rather than an object within
// it ("/my-bucket/some/key").
// isBucketLevelPath returns true for a path that addresses the bucket
// itself (no object key) — used to choose between injectKeyPrefix
// (object operations: GET, HEAD, PUT, DELETE) and scopeListPrefix
// (bucket-level operations: GET ?list-type=2, ?versions, ?location, …).
//
// Both `/my-bucket` and `/my-bucket/` count as bucket-level. The AWS
// SDK with `forcePathStyle: true` emits ListObjectsV2 as
// `GET /<bucket>/?list-type=2&prefix=<...>`, where the trailing slash
// is purely cosmetic — without TrimSuffix the slash would look like a
// separator into an empty object key and the request would be misrouted
// through injectKeyPrefix, producing an upstream path like
// `/<bucket>/<KeyPrefix>` (no `?prefix=` rewrite) and a confusing 404
// from the upstream store.
func isBucketLevelPath(p string) bool {
	s := strings.TrimSuffix(strings.TrimPrefix(p, "/"), "/")
	return strings.IndexByte(s, '/') < 0
}

// injectKeyPrefix prepends h.KeyPrefix to the object-key portion of a
// path-style request path: "/bucket/key" becomes "/bucket/<prefix>key".
// Bucket-level paths are returned unchanged (see scopeListPrefix). It is a
// no-op when KeyPrefix is empty.
func (h *Handler) injectKeyPrefix(p string) string {
	if h.KeyPrefix == "" || isBucketLevelPath(p) {
		return p
	}
	trimmed := strings.TrimPrefix(p, "/")
	idx := strings.IndexByte(trimmed, '/')
	bucket := trimmed[:idx]
	key := trimmed[idx+1:]
	return "/" + bucket + "/" + h.KeyPrefix + key
}

// scopeListPrefix confines a bucket-level listing to h.KeyPrefix by
// prepending it to the request's `prefix` query parameter, so a client
// cannot enumerate keys outside the configured prefix.
func (h *Handler) scopeListPrefix(u *url.URL) {
	if h.KeyPrefix == "" {
		return
	}
	q := u.Query()
	q.Set("prefix", h.KeyPrefix+q.Get("prefix"))
	u.RawQuery = q.Encode()
}

// XML elements whose text content references an object key path and
// therefore carries the upstream-prefixed form when KeyPrefix is set.
// Tokens that are opaque (ContinuationToken, NextContinuationToken,
// UploadIdMarker, …) are NOT in this set — stripping them would
// corrupt random tokens that happen to start with the prefix bytes.
//
// Element families:
//
//   - <Key> / <Prefix>                  ListObjects / ListObjectsV2
//   - <Marker> / <NextMarker>           ListObjects v1 pagination
//   - <StartAfter>                       ListObjectsV2 pagination
//   - <KeyMarker> / <NextKeyMarker>      ListObjectVersions / ListMultipartUploads
//   - <Location>                         CompleteMultipartUpload response
//                                        (a URL whose path holds the key)
//   - <Resource>                         Error response — the request path
//                                        S3 was acting on; also a URL/path
var listKeyElementRegexp = regexp.MustCompile(
	`<(Key|Prefix|Marker|NextMarker|StartAfter|KeyMarker|NextKeyMarker|Location|Resource)>([^<]*)</(Key|Prefix|Marker|NextMarker|StartAfter|KeyMarker|NextKeyMarker|Location|Resource)>`,
)

// urlEncodedPrefix returns the same prefix with every `/` replaced by
// `%2F` (uppercase). S3 responses URL-encode object keys whenever the
// client request carries `EncodingType=url` — boto3, DuckDB's httpfs
// and the standard AWS SDKs all set that by default. Without matching
// the encoded form, the prefix would stay in the response and the
// client would see upstream-shaped keys (e.g.
// `<Key>tenants%2Facme%2Fuploads%2Fx.csv</Key>`) — defeating the whole
// point of the strip.
//
// We deliberately encode ONLY the slash, not the rest of the prefix,
// because:
//   - S3's encoding-type=url percent-encodes `/` and unsafe bytes;
//     ASCII letters/digits stay literal.
//   - Encoding more aggressively (e.g. `.`) would create needles that
//     never appear in the response and silently miss real matches.
func urlEncodedPrefix(prefix []byte) []byte {
	if !bytes.ContainsRune(prefix, '/') {
		return prefix
	}
	return bytes.ReplaceAll(prefix, []byte("/"), []byte("%2F"))
}

// stripPrefixFromValue removes the KeyPrefix from an element text
// value. Three shapes are supported:
//
//  1. Bare key (literal): the value IS the prefixed key — e.g.
//     <Key>tenants/acme/uploads/x.csv</Key>. Chop the prefix off the front.
//
//  2. Bare key (URL-encoded): the value is the same key but with slashes
//     percent-encoded — e.g.
//     <Key>tenants%2Facme%2Fuploads%2Fx.csv</Key>. This is what S3
//     returns whenever the request carries `EncodingType=url` (boto3 and
//     DuckDB do that by default). Chop the encoded prefix off the front.
//
//  3. URL or absolute path: the value embeds the prefixed key after a
//     path separator — e.g.
//        <Location>https://bucket.s3.region.amazonaws.com/tenants/acme/uploads/x.csv</Location>
//        <Resource>/bucket/tenants/acme/uploads/x.csv</Resource>
//     Splice the prefix out at its `/<KeyPrefix>` occurrence.
//
// Returns the value unchanged when no occurrence is found — never
// removes "the wrong" bytes silently.
func stripPrefixFromValue(val, prefix []byte) []byte {
	if len(val) == 0 || len(prefix) == 0 {
		return val
	}
	// 1. Bare key form (literal).
	if bytes.HasPrefix(val, prefix) {
		out := make([]byte, len(val)-len(prefix))
		copy(out, val[len(prefix):])
		return out
	}
	// 2. Bare key form (URL-encoded). Only consider when the prefix
	//    actually contains a slash — otherwise the encoded form equals
	//    the literal form and the path-1 branch already handled it.
	encPrefix := urlEncodedPrefix(prefix)
	if !bytes.Equal(encPrefix, prefix) && bytes.HasPrefix(val, encPrefix) {
		out := make([]byte, len(val)-len(encPrefix))
		copy(out, val[len(encPrefix):])
		return out
	}
	// 3. URL / absolute path form: look for `/<prefix>` and splice.
	needle := append(append(make([]byte, 0, len(prefix)+1), '/'), prefix...)
	idx := bytes.Index(val, needle)
	if idx < 0 {
		return val
	}
	out := make([]byte, 0, len(val)-len(prefix))
	out = append(out, val[:idx+1]...) // keep the leading slash
	out = append(out, val[idx+len(needle):]...)
	return out
}

// stripKeyPrefixFromListBody undoes scopeListPrefix on the upstream
// response body so the client sees a fully-transparent view. Without
// this rewrite a client that pipes a Contents.Key (or a Location URL)
// straight into a follow-up GetObject would hit a double-prepended
// path upstream (the proxy adds KeyPrefix again on GET) and 404.
//
// Targets the specific XML elements that hold object key paths; leaves
// opaque pagination tokens (ContinuationToken, …) untouched. Pure
// byte-level rewrite — preserves the upstream XML formatting,
// namespaces, comments and any unknown elements.
func stripKeyPrefixFromListBody(body []byte, keyPrefix string) []byte {
	if keyPrefix == "" {
		return body
	}
	prefixBytes := []byte(keyPrefix)
	return listKeyElementRegexp.ReplaceAllFunc(body, func(match []byte) []byte {
		sm := listKeyElementRegexp.FindSubmatch(match)
		// sm = [whole, openTag, value, closeTag]
		if len(sm) != 4 || !bytes.Equal(sm[1], sm[3]) {
			return match
		}
		stripped := stripPrefixFromValue(sm[2], prefixBytes)
		if bytes.Equal(stripped, sm[2]) {
			// No occurrence found — leave the element verbatim so we
			// never silently corrupt a value that just happened to
			// share the bytes.
			return match
		}
		// Reassemble: <Tag>stripped</Tag>
		out := make([]byte, 0, len(match)-(len(sm[2])-len(stripped)))
		out = append(out, '<')
		out = append(out, sm[1]...)
		out = append(out, '>')
		out = append(out, stripped...)
		out = append(out, '<', '/')
		out = append(out, sm[3]...)
		out = append(out, '>')
		return out
	})
}

// stripKeyPrefixFromResponse is wired into httputil.ReverseProxy's
// ModifyResponse hook. It walks the upstream XML body and strips
// h.KeyPrefix from elements that carry an object key path, so the
// client sees a fully-transparent view of the proxy.
//
// XML response families this covers:
//   - LIST / ListObjects / ListObjectsV2 / ListObjectVersions /
//     ListMultipartUploads (bucket-level paths)
//   - CompleteMultipartUploadResult (object-level path; <Location>
//     and <Key> both carry the prefixed path)
//   - InitiateMultipartUploadResult (object-level; <Key>)
//   - Error responses on any path (<Resource> holds the request path)
//
// The element allow-list in `listKeyElementRegexp` plus the
// `HasPrefix`/`/<prefix>` guard inside `stripPrefixFromValue` keep
// the rewrite safe even when the body is a user-uploaded XML
// document that happens to be Content-Type: application/xml — we
// only touch values that ALSO start with the configured KeyPrefix.
//
// Skipped when:
//   - KeyPrefix is empty (proxy is fully transparent anyway)
//   - response is not XML (Content-Type check — opaque object
//     payloads are never touched)
//   - response is content-encoded (gzip etc.) — too risky to decode
//     and re-encode here; upstream S3 doesn't compress these
//     responses by default
func (h *Handler) stripKeyPrefixFromResponse(resp *http.Response) error {
	if h.KeyPrefix == "" || resp == nil {
		return nil
	}
	if resp.Header.Get("Content-Encoding") != "" {
		return nil
	}
	ct := resp.Header.Get("Content-Type")
	if !strings.Contains(ct, "xml") {
		return nil
	}
	if resp.Body == nil {
		return nil
	}
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return err
	}
	resp.Body.Close()
	rewritten := stripKeyPrefixFromListBody(body, h.KeyPrefix)
	resp.Body = io.NopCloser(bytes.NewReader(rewritten))
	resp.ContentLength = int64(len(rewritten))
	resp.Header.Set("Content-Length", strconv.Itoa(len(rewritten)))
	return nil
}

// isReadMethod reports whether an HTTP method is a non-mutating S3 read:
// GET (object download, ListObjects/V2, ListObjectVersions, location, …) and
// HEAD (object existence/metadata). Every other method mutates state.
func isReadMethod(method string) bool {
	return method == http.MethodGet || method == http.MethodHead
}

// objectKey returns the object-key portion of a path-style request path
// ("/bucket/key" -> "key", true). A bucket-level path ("/bucket" or
// "/bucket/") addresses no object key and returns ("", false).
func objectKey(p string) (string, bool) {
	if isBucketLevelPath(p) {
		return "", false
	}
	trimmed := strings.TrimPrefix(p, "/")
	idx := strings.IndexByte(trimmed, '/')
	return trimmed[idx+1:], true
}

// isProtectedKeyPath reports whether the object key in a path-style request
// path falls under one of the configured ReadOnlyKeyPrefixes. Bucket-level
// paths (no object key) are never protected. Returns false when no prefixes
// are configured.
func (h *Handler) isProtectedKeyPath(p string) bool {
	if len(h.ReadOnlyKeyPrefixes) == 0 {
		return false
	}
	key, ok := objectKey(p)
	if !ok {
		return false
	}
	for _, prefix := range h.ReadOnlyKeyPrefixes {
		if strings.HasPrefix(key, prefix) {
			return true
		}
	}
	return false
}

func (h *Handler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	// Read-only enforcement, before anything else: a mutating method is
	// rejected up front and never signed or forwarded, regardless of the
	// credentials it carries (fail closed).
	if h.ReadOnly && !isReadMethod(r.Method) {
		log.Warnf("read-only proxy: rejecting %s %s", r.Method, r.URL.Path)
		w.WriteHeader(http.StatusForbidden)
		if h.Debug {
			w.Write([]byte("read-only proxy: method not allowed"))
		}
		return
	}

	// Per-prefix read-only enforcement: a mutating request whose object key
	// falls under one of the protected prefixes is rejected up front, before
	// it is signed or forwarded — independent of the credentials it carries
	// (fail closed).
	if !isReadMethod(r.Method) && h.isProtectedKeyPath(r.URL.Path) {
		log.Warnf("read-only key prefix: rejecting %s %s", r.Method, r.URL.Path)
		w.WriteHeader(http.StatusForbidden)
		if h.Debug {
			w.Write([]byte("read-only key prefix: write not allowed"))
		}
		return
	}

	proxyReq, err := h.buildUpstreamRequest(r)
	if err != nil {
		log.WithError(err).Error("unable to proxy request")
		w.WriteHeader(http.StatusBadRequest)

		// for security reasons, only write detailed error information in debug mode
		if h.Debug {
			w.Write([]byte(err.Error()))
		}
		return
	}

	url := url.URL{Scheme: proxyReq.URL.Scheme, Host: proxyReq.Host}
	proxy := httputil.NewSingleHostReverseProxy(&url)
	proxy.FlushInterval = 1
	// Strip h.KeyPrefix from XML response bodies so the client view is
	// fully transparent — see stripKeyPrefixFromResponse for the
	// criteria.
	proxy.ModifyResponse = h.stripKeyPrefixFromResponse
	proxy.ServeHTTP(w, proxyReq)
}

func (h *Handler) sign(signer *v4.Signer, req *http.Request, region string) error {
	return h.signWithTime(signer, req, region, time.Now())
}

func (h *Handler) signWithTime(signer *v4.Signer, req *http.Request, region string, signTime time.Time) error {
	body := bytes.NewReader([]byte{})
	if req.Body != nil {
		b, err := ioutil.ReadAll(req.Body)
		if err != nil {
			return err
		}
		body = bytes.NewReader(b)
	}

	_, err := signer.Sign(req, body, "s3", region, signTime)
	return err
}

func copyHeaderWithoutOverwrite(dst http.Header, src http.Header) {
	for k, v := range src {
		if _, ok := dst[k]; !ok {
			for _, vv := range v {
				dst.Add(k, vv)
			}
		}
	}
}

func (h *Handler) validateIncomingSourceIP(req *http.Request) error {
	allowed := false
	for _, subnet := range h.AllowedSourceSubnet {
		ip, _, _ := net.SplitHostPort(req.RemoteAddr)
		userIP := net.ParseIP(ip)
		if subnet.Contains(userIP) {
			allowed = true
		}
	}
	if !allowed {
		return fmt.Errorf("source IP not allowed: %v", req)
	}
	return nil
}

func (h *Handler) validateIncomingHeaders(req *http.Request) (string, string, error) {
	amzDateHeader := req.Header["X-Amz-Date"]
	if len(amzDateHeader) != 1 {
		return "", "", fmt.Errorf("X-Amz-Date header missing or set multiple times: %v", req)
	}

	authorizationHeader := req.Header["Authorization"]
	if len(authorizationHeader) != 1 {
		return "", "", fmt.Errorf("Authorization header missing or set multiple times: %v", req)
	}
	match := awsAuthorizationCredentialRegexp.FindStringSubmatch(authorizationHeader[0])
	if len(match) != 3 {
		return "", "", fmt.Errorf("invalid Authorization header: Credential not found: %v", req)
	}
	receivedAccessKeyID := match[1]
	region := match[2]

	// Validate the received Credential (ACCESS_KEY_ID) is allowed
	for accessKeyID := range h.AWSCredentials {
		if subtle.ConstantTimeCompare([]byte(receivedAccessKeyID), []byte(accessKeyID)) == 1 {
			return accessKeyID, region, nil
		}
	}
	return "", "", fmt.Errorf("invalid AccessKeyID in Credential: %v", req)
}

func (h *Handler) generateFakeIncomingRequest(signer *v4.Signer, req *http.Request, region string) (*http.Request, error) {
	fakeReq, err := http.NewRequest(req.Method, req.URL.String(), nil)
	if err != nil {
		return nil, err
	}
	fakeReq.URL.RawPath = req.URL.Path

	// We already validated there there is exactly one Authorization header
	authorizationHeader := req.Header.Get("authorization")
	match := awsAuthorizationSignedHeadersRegexp.FindStringSubmatch(authorizationHeader)
	if len(match) == 2 {
		for _, header := range strings.Split(match[1], ";") {
			fakeReq.Header.Set(header, req.Header.Get(header))
		}
	}

	// Delete a potentially double-added header
	fakeReq.Header.Del("host")
	fakeReq.Host = h.AllowedSourceEndpoint

	// The X-Amz-Date header contains a timestamp, such as: 20190929T182805Z
	signTime, err := time.Parse("20060102T150405Z", req.Header["X-Amz-Date"][0])
	if err != nil {
		return nil, fmt.Errorf("error parsing X-Amz-Date %v - %v", req.Header["X-Amz-Date"][0], err)
	}

	// Sign the fake request with the original timestamp
	if err := h.signWithTime(signer, fakeReq, region, signTime); err != nil {
		return nil, err
	}

	return fakeReq, nil
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

// decodeAwsChunked decodes an aws-chunked body into the raw object content. Each
// chunk is `<hex-size>[;chunk-signature=…]\r\n<size bytes>\r\n`; a zero-size
// chunk ends the stream (any trailer that follows is ignored). Per-chunk
// signatures (STREAMING-AWS4-HMAC-SHA256-PAYLOAD) are accepted and ignored — we
// only need the payload bytes.
func decodeAwsChunked(r io.Reader) ([]byte, error) {
	br := bufio.NewReader(r)
	var out bytes.Buffer
	for {
		line, err := br.ReadString('\n')
		if err != nil {
			return nil, fmt.Errorf("reading chunk size: %w", err)
		}
		sizeField := strings.TrimRight(line, "\r\n")
		if i := strings.IndexByte(sizeField, ';'); i >= 0 {
			sizeField = sizeField[:i] // drop chunk extensions (e.g. chunk-signature)
		}
		size, err := strconv.ParseInt(strings.TrimSpace(sizeField), 16, 64)
		if err != nil {
			return nil, fmt.Errorf("invalid chunk size %q: %w", sizeField, err)
		}
		if size == 0 {
			return out.Bytes(), nil // final chunk; trailers (if any) ignored
		}
		if _, err := io.CopyN(&out, br, size); err != nil {
			return nil, fmt.Errorf("reading chunk data: %w", err)
		}
		if _, err := br.Discard(2); err != nil { // consume the CRLF after chunk data
			return nil, fmt.Errorf("reading chunk terminator: %w", err)
		}
	}
}

func (h *Handler) assembleUpstreamReq(signer *v4.Signer, req *http.Request, region string) (*http.Request, error) {
	upstreamEndpoint := h.UpstreamEndpoint
	if len(upstreamEndpoint) == 0 {
		upstreamEndpoint = fmt.Sprintf("s3.%s.amazonaws.com", region)
		log.Infof("Using %s as upstream endpoint", upstreamEndpoint)
	}

	proxyURL := *req.URL
	proxyURL.Scheme = h.UpstreamScheme
	proxyURL.Host = upstreamEndpoint
	// Optionally confine the request to a fixed key prefix: prepend it to
	// the object key, or — for a bucket-level listing — to the `prefix`
	// query parameter. Both are no-ops when KeyPrefix is empty.
	prefixedPath := h.injectKeyPrefix(req.URL.Path)
	proxyURL.Path = prefixedPath
	proxyURL.RawPath = prefixedPath
	if isBucketLevelPath(req.URL.Path) {
		h.scopeListPrefix(&proxyURL)
	}
	// A client streaming an upload (e.g. DuckDB httpfs, the AWS SDKs) sends the
	// body in `aws-chunked` form — `<hex-size>[;chunk-signature=…]\r\n<data>\r\n`
	// repeated, ending `0\r\n…\r\n` — signalled by an `x-amz-content-sha256` of
	// `STREAMING-…`. We re-sign the request with a plain payload hash, which
	// drops the streaming semantics, so the upstream would store the *framed*
	// bytes verbatim (corrupting the object: a parquet becomes
	// `165D\r\nPAR1…\r\n\r\n`). Decode the framing here so the upstream receives
	// the real content. Validation of the incoming signature already happened
	// above against the original headers and is unaffected (the body is not part
	// of a STREAMING canonical request).
	body := req.Body
	if isAwsChunkedUpload(req) {
		decoded, derr := decodeAwsChunked(req.Body)
		if derr != nil {
			return nil, fmt.Errorf("aws-chunked decode: %w", derr)
		}
		body = io.NopCloser(bytes.NewReader(decoded))
		req.ContentLength = int64(len(decoded))
		// Strip the streaming markers so they are not copied upstream and the
		// signer computes a normal payload hash over the decoded body.
		req.Header.Del("Content-Encoding")
		req.Header.Del("X-Amz-Decoded-Content-Length")
		req.Header.Del("X-Amz-Content-Sha256")
	}

	proxyReq, err := http.NewRequest(req.Method, proxyURL.String(), body)
	if err != nil {
		return nil, err
	}
	proxyReq.ContentLength = req.ContentLength
	if val, ok := req.Header["Content-Type"]; ok {
		proxyReq.Header["Content-Type"] = val
	}
	if val, ok := req.Header["Content-Md5"]; ok {
		proxyReq.Header["Content-Md5"] = val
	}

	// Sign the upstream request — with the dedicated upstream credentials
	// when configured, otherwise with the client's (the default). The
	// region is the client's unless an upstream region is configured.
	upstreamSigner := signer
	if h.UpstreamSigner != nil {
		upstreamSigner = h.UpstreamSigner
	}
	upstreamRegion := region
	if h.UpstreamRegion != "" {
		upstreamRegion = h.UpstreamRegion
	}
	if err := h.sign(upstreamSigner, proxyReq, upstreamRegion); err != nil {
		return nil, err
	}

	// Add origin headers after request is signed (no overwrite)
	copyHeaderWithoutOverwrite(proxyReq.Header, req.Header)

	return proxyReq, nil
}

// Do validates the incoming request and create a new request for an upstream server
func (h *Handler) buildUpstreamRequest(req *http.Request) (*http.Request, error) {
	// Ensure the request was sent from an allowed IP address
	err := h.validateIncomingSourceIP(req)
	if err != nil {
		return nil, err
	}

	// Validate incoming headers and extract AWS_ACCESS_KEY_ID
	accessKeyID, region, err := h.validateIncomingHeaders(req)
	if err != nil {
		return nil, err
	}

	// Get the AWS Signature signer for this AccessKey
	signer := h.Signers[accessKeyID]

	// Assemble a signed fake request to verify the incoming requests signature
	fakeReq, err := h.generateFakeIncomingRequest(signer, req, region)
	if err != nil {
		return nil, err
	}

	// WORKAROUND S3CMD which dont use white space before the some commas in the authorization header
	fakeAuthorizationStr := fakeReq.Header.Get("Authorization")
	// Sanitize fakeReq to add white spaces after the comma signature if missing
	authorizationStr := strings.Replace(req.Header["Authorization"][0], ",Signature", ", Signature", 1)
	// Sanitize fakeReq to add white spaces after the comma signheaders if missing
	authorizationStr = strings.Replace(authorizationStr, ",SignedHeaders", ", SignedHeaders", 1)

	// Verify that the fake request and the incoming request have the same signature
	// This ensures it was sent and signed by a client with correct AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY
	cmpResult := subtle.ConstantTimeCompare([]byte(fakeAuthorizationStr), []byte(authorizationStr))
	if cmpResult == 0 {
		v, _ := httputil.DumpRequest(fakeReq, false)
		log.Debugf("Fake request: %v", string(v))

		v, _ = httputil.DumpRequest(req, false)
		log.Debugf("Incoming request: %v", string(v))
		return nil, fmt.Errorf("invalid signature in Authorization header")
	}

	if log.GetLevel() == log.DebugLevel {
		initialReqDump, _ := httputil.DumpRequest(req, false)
		log.Debugf("Initial request dump: %v", string(initialReqDump))
	}

	// Assemble a new upstream request
	proxyReq, err := h.assembleUpstreamReq(signer, req, region)
	if err != nil {
		return nil, err
	}

	// Disable Go's "Transfer-Encoding: chunked" madness
	proxyReq.ContentLength = req.ContentLength

	if log.GetLevel() == log.DebugLevel {
		proxyReqDump, _ := httputil.DumpRequest(proxyReq, false)
		log.Debugf("Proxying request: %v", string(proxyReqDump))
	}

	return proxyReq, nil
}
