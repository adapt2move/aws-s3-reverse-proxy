package main

import (
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

// XML elements in a ListBucket / ListObjectsV2 / ListObjectVersions
// response that contain a key (and therefore carry the upstream-prefixed
// form). Tokens that are opaque (ContinuationToken, NextContinuationToken,
// UploadIdMarker, …) are NOT in this set — those must not be touched.
var listKeyElementRegexp = regexp.MustCompile(
	`<(Key|Prefix|Marker|NextMarker|StartAfter|KeyMarker|NextKeyMarker)>([^<]*)</(Key|Prefix|Marker|NextMarker|StartAfter|KeyMarker|NextKeyMarker)>`,
)

// stripKeyPrefixFromListBody undoes scopeListPrefix on the upstream
// response body so the client sees a fully-transparent view: a LIST
// for `prefix=foo/` returns Contents.Key values starting with `foo/`,
// not with `<KeyPrefix>foo/`. Without this rewrite a client that
// pipes a Contents.Key straight into a follow-up GetObject would
// hit a double-prepended path upstream (the proxy adds KeyPrefix
// again on GET) and 404.
//
// Targets the specific XML elements that hold object keys; leaves
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
		val := sm[2]
		if !bytes.HasPrefix(val, prefixBytes) {
			return match
		}
		stripped := val[len(prefixBytes):]
		// Reassemble: <Tag>stripped</Tag>
		out := make([]byte, 0, len(match)-len(prefixBytes))
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

// modifyListResponse is wired into httputil.ReverseProxy's
// ModifyResponse hook. For bucket-level requests (LIST / ListObjects
// / etc.) it strips the proxy's KeyPrefix from the response body so
// the client sees a transparent view. Non-LIST responses are passed
// through unchanged.
//
// Skipped when:
//   - KeyPrefix is empty (proxy is fully transparent anyway)
//   - request was not bucket-level (object GET/HEAD/PUT — body is
//     opaque payload, never XML-listing)
//   - response is not XML (some error responses are plain text)
//   - response is content-encoded (gzip etc.) — too risky to decode
//     and re-encode here; upstream S3 doesn't compress LIST responses
//     by default, so this is rarely hit
func (h *Handler) modifyListResponse(resp *http.Response) error {
	if h.KeyPrefix == "" || resp == nil || resp.Request == nil {
		return nil
	}
	if !isBucketLevelPath(resp.Request.URL.Path) {
		return nil
	}
	if resp.Header.Get("Content-Encoding") != "" {
		return nil
	}
	ct := resp.Header.Get("Content-Type")
	if ct != "" && !strings.Contains(ct, "xml") {
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

func (h *Handler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
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
	// Strip h.KeyPrefix from LIST response bodies so the client view is
	// fully transparent — see modifyListResponse for the criteria.
	proxy.ModifyResponse = h.modifyListResponse
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
	proxyReq, err := http.NewRequest(req.Method, proxyURL.String(), req.Body)
	if err != nil {
		return nil, err
	}
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
