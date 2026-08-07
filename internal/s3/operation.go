// Package s3 is what this proxy knows about the S3 wire protocol, and nothing
// else.
//
// It classifies a request into one of the operations the proxy implements,
// validates object keys, decodes aws-chunked upload framing, and reads and
// rewrites the XML documents S3 exchanges. It holds no policy, no tenant and
// no configuration: everything here would behave identically in a proxy with
// a completely different authorization model, which is why the one place it
// needs an authorization answer (FilterListEntries) takes a predicate rather
// than importing one.
package s3

import (
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"strings"
)

// The proxy is the security boundary for untrusted clients, so the set of
// S3 operations it understands is a whitelist, not a blacklist. Everything
// below describes what is *allowed* through; anything that does not match
// one of these shapes is rejected with 403 before a signature is checked or
// a byte is forwarded.
type Kind string

const (
	GetObject               Kind = "GetObject"
	HeadObject              Kind = "HeadObject"
	PutObject               Kind = "PutObject"
	DeleteObject            Kind = "DeleteObject"
	ListObjects             Kind = "ListObjects"
	DeleteObjects           Kind = "DeleteObjects"
	CreateMultipartUpload   Kind = "CreateMultipartUpload"
	UploadPart              Kind = "UploadPart"
	CompleteMultipartUpload Kind = "CompleteMultipartUpload"
	AbortMultipartUpload    Kind = "AbortMultipartUpload"
	ListParts               Kind = "ListParts"
)

// Operation is a classified request: which S3 call it is, and the
// client-facing subject (object key, or listing prefix) authorization has
// to decide on.
type Operation struct {
	Kind   Kind
	Bucket string

	// Key is the client-facing object key in decoded form; EscapedKey is
	// the same key exactly as the client wrote it in the request line.
	// Both are empty for bucket-level operations.
	Key        string
	EscapedKey string

	// ListPrefix is the client-facing `prefix` parameter of a listing.
	ListPrefix string

	// RewritesXML marks the operations whose successful response embeds
	// object keys and therefore has to have the tenant prefix stripped back
	// out. Object payloads (GetObject) are never touched — they stream.
	RewritesXML bool
}

// IsBucketLevel reports whether the operation addresses the bucket rather
// than one object in it.
func (o *Operation) IsBucketLevel() bool {
	return o.Kind == ListObjects || o.Kind == DeleteObjects
}

var (
	ErrUnsupportedOperation = errors.New("operation not supported")
	ErrInvalidKey           = errors.New("invalid object key")
)

// Query parameters accepted per operation shape. A parameter outside the
// matching set makes the request unclassifiable, which means 403 — that is
// how `?acl`, `?policy`, `?versioning`, `?lifecycle`, `?tagging` and every
// other bucket- or object-level administrative call is refused without
// having to enumerate them.
var (
	listObjectsParams = newParamSet(
		"list-type", "prefix", "delimiter", "max-keys", "continuation-token",
		"start-after", "encoding-type", "fetch-owner", "marker",
	)
	deleteObjectsParams = newParamSet("delete")
	listPartsParams     = newParamSet("uploadId", "max-parts", "part-number-marker", "encoding-type")
	uploadPartParams    = newParamSet("partNumber", "uploadId")
	createMPUParams     = newParamSet("uploads")
	uploadIDOnlyParams  = newParamSet("uploadId")
)

type paramSet map[string]struct{}

func newParamSet(names ...string) paramSet {
	s := make(paramSet, len(names))
	for _, n := range names {
		s[n] = struct{}{}
	}
	return s
}

// covers reports whether every query parameter of the request is in the
// set. An empty query is covered by every set.
func (s paramSet) covers(q url.Values) bool {
	for name := range q {
		if _, ok := s[name]; !ok {
			return false
		}
	}
	return true
}

// Classify matches an incoming request against the whitelist above
// and returns what it is. The error is deliberately coarse: a client learns
// that an operation is refused, not which check refused it.
func Classify(req *http.Request) (*Operation, error) {
	// A copy names a *second* object key — the source — that no other check
	// on the request path ever sees, so it would travel upstream
	// unauthorized and unprefixed. Refuse the header outright rather than
	// grow a second authorization path for it.
	if req.Header.Get("X-Amz-Copy-Source") != "" {
		return nil, fmt.Errorf("%w: x-amz-copy-source", ErrUnsupportedOperation)
	}

	// Every other `x-amz-` header is checked against the same kind of
	// whitelist, for the same reason: one this proxy does not understand is
	// one it cannot authorize, and forwarding it would hand the object store
	// an instruction no policy rule ever saw.
	if err := CheckRequestHeaders(req.Header); err != nil {
		return nil, err
	}

	query := req.URL.Query()
	// The AWS SDKs tag a request with the operation they think they are
	// issuing (`?x-id=PutObject`). It names nothing and grants nothing, and
	// every check below derives the operation from the method, the path and
	// the remaining parameters — so drop it rather than let a marker decide
	// whether a request is classifiable. `req.URL` keeps it, so the upstream
	// signature is still computed over the URL the client signed.
	query.Del("x-id")

	// Query-string (presigned) authentication is out of scope: the
	// signature would cover a URL we are about to rewrite, and a presigned
	// URL is a bearer token that outlives the request. Any `X-Amz-…` query
	// parameter marks one.
	for name := range query {
		if strings.HasPrefix(strings.ToLower(name), "x-amz-") {
			return nil, fmt.Errorf("%w: query-string authentication", ErrUnsupportedOperation)
		}
	}

	bucket, key, escapedKey, bucketLevel, err := splitBucketKey(req.URL)
	if err != nil {
		return nil, err
	}

	if bucketLevel {
		switch req.Method {
		case http.MethodGet:
			if listObjectsParams.covers(query) {
				return &Operation{
					Kind:        ListObjects,
					Bucket:      bucket,
					ListPrefix:  query.Get("prefix"),
					RewritesXML: true,
				}, nil
			}
		case http.MethodPost:
			if _, ok := query["delete"]; ok && deleteObjectsParams.covers(query) {
				return &Operation{Kind: DeleteObjects, Bucket: bucket, RewritesXML: true}, nil
			}
		}
		// Bucket creation (PUT /bucket), deletion (DELETE /bucket),
		// HeadBucket and every `?…` bucket sub-resource land here.
		return nil, fmt.Errorf("%w: %s bucket-level request", ErrUnsupportedOperation, req.Method)
	}

	base := Operation{Bucket: bucket, Key: key, EscapedKey: escapedKey}
	switch req.Method {
	case http.MethodGet:
		if len(query) == 0 {
			base.Kind = GetObject
			return &base, nil
		}
		if _, ok := query["uploadId"]; ok && listPartsParams.covers(query) {
			base.Kind = ListParts
			base.RewritesXML = true
			return &base, nil
		}
	case http.MethodHead:
		if len(query) == 0 {
			base.Kind = HeadObject
			return &base, nil
		}
	case http.MethodPut:
		if len(query) == 0 {
			base.Kind = PutObject
			return &base, nil
		}
		if _, ok := query["uploadId"]; ok && uploadPartParams.covers(query) {
			base.Kind = UploadPart
			return &base, nil
		}
	case http.MethodPost:
		if _, ok := query["uploads"]; ok && createMPUParams.covers(query) {
			base.Kind = CreateMultipartUpload
			base.RewritesXML = true
			return &base, nil
		}
		if _, ok := query["uploadId"]; ok && uploadIDOnlyParams.covers(query) {
			base.Kind = CompleteMultipartUpload
			base.RewritesXML = true
			return &base, nil
		}
	case http.MethodDelete:
		if len(query) == 0 {
			base.Kind = DeleteObject
			return &base, nil
		}
		if _, ok := query["uploadId"]; ok && uploadIDOnlyParams.covers(query) {
			base.Kind = AbortMultipartUpload
			return &base, nil
		}
	}
	return nil, fmt.Errorf("%w: %s object request", ErrUnsupportedOperation, req.Method)
}

// splitBucketKey pulls the bucket and the object key out of a path-style
// request path, in both the decoded and the as-written form.
//
// `/bucket` and `/bucket/` are bucket-level (the trailing slash is
// cosmetic — the AWS SDK with forcePathStyle emits ListObjectsV2 as
// `GET /<bucket>/?list-type=2`). Everything else addresses an object.
func splitBucketKey(u *url.URL) (bucket, key, escapedKey string, bucketLevel bool, err error) {
	path, escaped := u.Path, u.EscapedPath()
	if !strings.HasPrefix(path, "/") {
		return "", "", "", false, fmt.Errorf("%w: request path is not absolute", ErrInvalidKey)
	}

	bucket, key = splitFirstSegment(strings.TrimPrefix(path, "/"))
	_, escapedKey = splitFirstSegment(strings.TrimPrefix(escaped, "/"))
	if bucket == "" {
		// `GET /` is ListBuckets, and every other bucket-less path is some
		// account-level call. None of them is an operation this proxy
		// implements, so they are refused like the rest of them rather than
		// reported as a malformed key.
		return "", "", "", false, fmt.Errorf("%w: account-level request", ErrUnsupportedOperation)
	}
	if strings.Contains(bucket, "..") {
		return "", "", "", false, fmt.Errorf("%w: bucket name contains %q", ErrInvalidKey, "..")
	}
	if key == "" {
		return bucket, "", "", true, nil
	}
	if err := ValidateObjectKey(key); err != nil {
		return "", "", "", false, err
	}
	// The as-written form is checked too: a client that percent-encodes a
	// traversal (`%2e%2e`) is caught by the decoded check above, and one
	// that double-encodes it (`%252e%252e`) never decodes to `..` at all —
	// but a literal `..` surviving in the escaped path would still reach
	// the upstream, so refuse it here as well.
	if strings.Contains(escapedKey, "..") {
		return "", "", "", false, fmt.Errorf("%w: key contains %q", ErrInvalidKey, "..")
	}
	return bucket, key, escapedKey, false, nil
}

// splitFirstSegment splits "bucket/some/key" into ("bucket", "some/key").
func splitFirstSegment(s string) (first, rest string) {
	if i := strings.IndexByte(s, '/'); i >= 0 {
		return s[:i], s[i+1:]
	}
	return s, ""
}

// ValidateObjectKey normalizes-by-rejection: a key that is not already in
// the exact form we would forward is refused rather than rewritten. A
// silent normalization is how a key ends up outside the tenant prefix that
// was injected in front of it — `../` being the obvious case, an empty
// leading segment (`//key`, which addresses the bucket root) the subtler
// one.
func ValidateObjectKey(key string) error {
	if key == "" {
		return fmt.Errorf("%w: empty key", ErrInvalidKey)
	}
	if strings.ContainsAny(key, "\x00\r\n") {
		return fmt.Errorf("%w: key contains a control character", ErrInvalidKey)
	}
	if strings.HasPrefix(key, "/") {
		return fmt.Errorf("%w: key starts with %q", ErrInvalidKey, "/")
	}
	// A single trailing slash is a directory marker (`folder/`) — a real
	// and common S3 key — so the empty segment it produces is allowed.
	// Every other empty segment (`a//b`) is not.
	trimmed := strings.TrimSuffix(key, "/")
	if trimmed == "" {
		return fmt.Errorf("%w: empty key", ErrInvalidKey)
	}
	for _, segment := range strings.Split(trimmed, "/") {
		switch segment {
		case "":
			return fmt.Errorf("%w: key contains an empty path segment", ErrInvalidKey)
		case ".", "..":
			return fmt.Errorf("%w: key contains a %q segment", ErrInvalidKey, segment)
		}
		// A segment that is still percent-encoded after one decode (`%252e`
		// arriving as `%2e`) names nothing outside the tenant prefix by
		// itself — but only because *we* decode exactly once. Object stores
		// differ in how many times they decode a path, and a store that
		// decodes twice would see the traversal we just forwarded. Refuse
		// it instead of depending on the upstream's answer.
		if decoded, err := url.PathUnescape(segment); err == nil && decoded != segment {
			if decoded == "." || decoded == ".." || strings.Contains(decoded, "/") {
				return fmt.Errorf("%w: key contains a doubly-encoded %q segment", ErrInvalidKey, decoded)
			}
		}
	}
	return nil
}
