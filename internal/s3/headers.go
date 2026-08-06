package s3

import (
	"fmt"
	"net/http"
	"strings"
)

// Headers are a request input like any other, so they are a whitelist like
// any other.
//
// Three disjoint sets, and every header a client sends falls into exactly
// one of them:
//
//   - forwarded — what a tenant may legitimately influence about its own
//     object. These travel upstream, which means they must also be *signed*
//     upstream; see the ordering note in proxy.buildUpstreamRequest.
//   - consumed — the ones this proxy reads and acts on itself. They describe
//     the request to the proxy, not to the object store, and never travel on.
//   - everything else with an `x-amz-` prefix — refused, because an S3
//     extension header this proxy does not understand is one it cannot
//     authorize either. `x-amz-object-lock-mode` is the case that makes this
//     worth failing closed over: a client could pin an object beyond any
//     deletion, including the operator's own, for as long as it likes.
//
// Non-`x-amz-` headers outside the forwarded set are simply dropped. They are
// transport and client noise (User-Agent, Accept, Connection, …); refusing
// them would break every client without protecting anything.

// forwardedHeaders is the exact set of non-prefixed headers passed upstream.
var forwardedHeaders = map[string]bool{
	// Describe the object being written, and come back on a read.
	"Content-Type":        true,
	"Content-Md5":         true,
	"Content-Encoding":    true,
	"Content-Disposition": true,
	"Content-Language":    true,
	"Cache-Control":       true,
	"Expires":             true,

	// Let the client negotiate a compressed transfer of its own object. The
	// proxy withholds this on the responses it has to read (see
	// proxy.buildUpstreamRequest); everywhere else it is the client's call.
	"Accept-Encoding": true,

	// Describe which bytes of it a read wants, and under what condition.
	"Range":               true,
	"If-Match":            true,
	"If-None-Match":       true,
	"If-Modified-Since":   true,
	"If-Unmodified-Since": true,
	"If-Range":            true,
}

// Notable headers deliberately absent from the sets above, and therefore
// refused:
//
//   - x-amz-acl, x-amz-grant-*        an authorization decision the policy
//     does not model, made by the caller
//   - x-amz-object-lock-*             retention a tenant could set beyond any
//     deletion, the operator's included
//   - x-amz-storage-class             a cost the tenant would choose and the
//     operator would pay
//   - x-amz-server-side-encryption-*  SSE belongs on the bucket, not in a
//     header the tenant picks; SSE-C would put key material in the request
//   - x-amz-tagging                   billing and lifecycle input
//   - x-amz-security-token            this proxy derives credentials, so a
//     session token means nothing to it
//   - x-amz-expected-bucket-owner     an assertion about the account, which is
//     the operator's and not the tenant's
//
// An operator who needs one of these should configure it on the bucket, where
// it applies to every tenant equally, rather than let one tenant set it.

// forwardedPrefixes are header families passed upstream in full.
//
//   - `x-amz-meta-`     user metadata, the tenant's own key/value pairs
//   - `x-amz-checksum-` the client's digest of a body forwarded byte for
//     byte, so the end-to-end integrity check survives the re-signing
var forwardedPrefixes = []string{"X-Amz-Meta-", "X-Amz-Checksum-"}

// consumedHeaders are the `x-amz-` headers this proxy reads itself, or that
// describe the client rather than the object. They are accepted on the way in
// and never forwarded: the upstream request carries the proxy's own values
// instead, and to the object store the proxy *is* the client.
var consumedHeaders = map[string]bool{
	// Read by the request path: the signature, the aws-chunked framing.
	"X-Amz-Date":                   true,
	"X-Amz-Content-Sha256":         true,
	"X-Amz-Decoded-Content-Length": true,
	"X-Amz-Trailer":                true,

	// Telemetry about the caller. A browser cannot set `User-Agent` — the
	// Fetch spec forbids it — so the AWS JS SDK v3 sends it under an
	// `x-amz-` prefix instead. Refusing it would 403 every browser client on
	// every request, and it instructs the object store to do nothing at all.
	"X-Amz-User-Agent": true,
	"X-Amz-Te":         true,
}

// consumedPrefixes are `x-amz-` families that describe the SDK making the
// call, not the object: the checksum algorithm it chose, its retry counters
// (`x-amz-sdk-request`), its invocation id. Newer SDKs also send the same
// values without the `x-amz-` prefix, where they fall through as ordinary
// unforwarded headers.
var consumedPrefixes = []string{"X-Amz-Sdk-"}

// ForwardableHeader reports whether a client request header travels upstream.
func ForwardableHeader(name string) bool {
	canonical := http.CanonicalHeaderKey(name)
	if forwarded, known := forwardedHeaders[canonical]; known {
		return forwarded
	}
	for _, prefix := range forwardedPrefixes {
		if strings.HasPrefix(canonical, prefix) {
			return true
		}
	}
	return false
}

// consumed reports whether a header is one the proxy reads itself, rather
// than one it passes on.
func consumed(canonical string) bool {
	if consumedHeaders[canonical] {
		return true
	}
	for _, prefix := range consumedPrefixes {
		if strings.HasPrefix(canonical, prefix) {
			return true
		}
	}
	return false
}

// CheckRequestHeaders refuses any `x-amz-` header that is neither forwarded
// nor consumed — an S3 extension this proxy would otherwise hand to the
// object store without classifying, authorizing or signing it.
func CheckRequestHeaders(header http.Header) error {
	for name := range header {
		canonical := http.CanonicalHeaderKey(name)
		if !strings.HasPrefix(canonical, "X-Amz-") {
			continue
		}
		if consumed(canonical) || ForwardableHeader(canonical) {
			continue
		}
		return fmt.Errorf("%w: %s header", ErrUnsupportedOperation, canonical)
	}
	return nil
}
