package main

import (
	"bytes"
	"encoding/xml"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"regexp"
	"strconv"
	"strings"
)

// XML elements whose text content references an object key path and
// therefore carries the upstream-prefixed form.
//
// Tokens that are opaque (ContinuationToken, NextContinuationToken,
// UploadIdMarker, …) are NOT in this set — stripping them would corrupt
// random tokens that happen to start with the prefix bytes, and they are
// not injected on the way in either.
//
// Element families:
//
//   - <Key> / <Prefix>                  ListObjects / ListObjectsV2 / DeleteResult
//   - <Marker> / <NextMarker>           ListObjects v1 pagination
//   - <StartAfter>                      ListObjectsV2 pagination
//   - <KeyMarker> / <NextKeyMarker>     ListObjectVersions / ListMultipartUploads
//   - <Location>                        CompleteMultipartUpload response
//     (a URL whose path holds the key)
//   - <Resource>                        Error response — the request path
//     S3 was acting on; also a URL/path
var listKeyElementRegexp = regexp.MustCompile(
	`<(Key|Prefix|Marker|NextMarker|StartAfter|KeyMarker|NextKeyMarker|Location|Resource)>([^<]*)</(Key|Prefix|Marker|NextMarker|StartAfter|KeyMarker|NextKeyMarker|Location|Resource)>`,
)

// Bucket-level LIST responses group each object under a <Contents> element
// (with a <Key> child) and each rolled-up sub-directory under a
// <CommonPrefixes> element (with a <Prefix> child).
//
// The `[^<]*` value capture stays within a single element and the non-greedy
// `[\s\S]*?` block body stops at the first closing tag, so a block is matched
// as a unit and either kept verbatim or dropped in full.
var listContentsBlockRegexp = regexp.MustCompile(`<Contents>[\s\S]*?</Contents>`)
var listCommonPrefixesBlockRegexp = regexp.MustCompile(`<CommonPrefixes>[\s\S]*?</CommonPrefixes>`)
var listKeyValueRegexp = regexp.MustCompile(`<Key>([^<]*)</Key>`)
var listPrefixValueRegexp = regexp.MustCompile(`<Prefix>([^<]*)</Prefix>`)

// rewriteUpstreamResponse is the response half of the tenant scoping: it
// strips the injected key prefix back out, hides listing entries no rule
// grants the caller read on, and merges the per-key denials of a batch
// delete into the upstream's result.
//
// Only responses that are known to embed object keys are buffered —
// listings, batch deletes, multipart results — plus XML error bodies, which
// name the request path. An object payload is never touched, so a
// GetObject of a 10 GiB file still streams even when it happens to be XML.
func rewriteUpstreamResponse(resp *http.Response) error {
	if resp == nil || resp.Request == nil {
		return nil
	}
	st := requestStateFrom(resp.Request.Context())
	if st == nil {
		return nil
	}
	if !st.operation.rewritesXML && resp.StatusCode < 300 {
		return nil
	}
	if resp.Body == nil {
		return nil
	}
	// Content-encoded bodies would have to be decoded and re-encoded; S3
	// does not compress these responses, so refusing to touch them costs
	// nothing and avoids guessing at the encoding.
	if resp.Header.Get("Content-Encoding") != "" {
		return nil
	}
	if !strings.Contains(resp.Header.Get("Content-Type"), "xml") {
		return nil
	}

	body, err := readCapped(resp.Body, st.maxRewriteSize, "upstream response")
	resp.Body.Close()
	if err != nil {
		return err
	}

	body = stripKeyPrefixFromListBody(body, st.identity.KeyPrefix)
	if st.operation.kind == opListObjects {
		body = filterUnreadableListEntries(body, st.policy, st.identity.Level)
	}
	if st.operation.kind == opDeleteObjects {
		body = mergeDeniedDeletes(body, st.deniedDeletes)
	}

	resp.Body = io.NopCloser(bytes.NewReader(body))
	resp.ContentLength = int64(len(body))
	resp.Header.Set("Content-Length", strconv.Itoa(len(body)))
	return nil
}

// urlEncodedPrefix returns the same prefix with every `/` replaced by
// `%2F` (uppercase). S3 responses URL-encode object keys whenever the
// client request carries `EncodingType=url` — boto3, DuckDB's httpfs
// and the standard AWS SDKs all set that by default. Without matching
// the encoded form, the prefix would stay in the response and the
// client would see upstream-shaped keys (e.g.
// `<Key>acme%2Fuploads%2Fx.csv</Key>`) — defeating the whole point of
// the strip.
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

// stripPrefixFromValue removes the tenant key prefix from an element text
// value. Three shapes are supported:
//
//  1. Bare key (literal): the value IS the prefixed key — e.g.
//     <Key>acme/uploads/x.csv</Key>. Chop the prefix off the front.
//
//  2. Bare key (URL-encoded): the value is the same key but with slashes
//     percent-encoded — e.g. <Key>acme%2Fuploads%2Fx.csv</Key>. This is
//     what S3 returns whenever the request carries `EncodingType=url`
//     (boto3 and DuckDB do that by default).
//
//  3. URL or absolute path: the value embeds the prefixed key after a
//     path separator — e.g.
//     <Location>https://bucket.s3.region.amazonaws.com/acme/uploads/x.csv</Location>
//     <Resource>/bucket/acme/uploads/x.csv</Resource>
//     Splice the prefix out at its `/<prefix>` occurrence.
//
// Returns the value unchanged when no occurrence is found — never removes
// "the wrong" bytes silently.
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

// stripKeyPrefixFromListBody undoes the prefix injection on the upstream
// response body so the client sees a fully-transparent view. Without this
// rewrite a client that pipes a Contents.Key (or a Location URL) straight
// into a follow-up GetObject would hit a double-prefixed path upstream (the
// proxy injects the prefix again) and 404.
//
// Targets the specific XML elements that hold object key paths. Pure
// byte-level rewrite — preserves the upstream XML formatting, namespaces,
// comments and any unknown elements.
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

// filterUnreadableListEntries removes <Contents> and <CommonPrefixes>
// blocks the caller's level has no read permission for, so a listing can
// never enumerate a path the policy would refuse to serve.
//
// A listing is already authorized on its own `prefix`, so this only ever
// removes what the implicit deny at the end of the rule list covers — the
// paths inside the tenant's scope that no rule mentions at all. Without it,
// "not matched by a rule is denied" would hold for reads but not for the
// listing that reveals them.
//
// Values are matched against the client-facing key, so this must run AFTER
// stripKeyPrefixFromListBody. Counts such as <KeyCount> are left as-is:
// they may over-count once entries are hidden, exactly as they do when S3
// itself filters a page.
func filterUnreadableListEntries(body []byte, policy *Policy, level string) []byte {
	readable := func(raw []byte) bool {
		// S3 percent-encodes key values when the request carried
		// EncodingType=url; policy patterns are written against real keys.
		value := string(raw)
		if decoded, err := url.PathUnescape(value); err == nil {
			value = decoded
		}
		return policy.Authorize(level, value, http.MethodGet).Allowed
	}
	body = listContentsBlockRegexp.ReplaceAllFunc(body, func(block []byte) []byte {
		if m := listKeyValueRegexp.FindSubmatch(block); m != nil && !readable(m[1]) {
			return nil
		}
		return block
	})
	return listCommonPrefixesBlockRegexp.ReplaceAllFunc(body, func(block []byte) []byte {
		if m := listPrefixValueRegexp.FindSubmatch(block); m != nil && !readable(m[1]) {
			return nil
		}
		return block
	})
}

// writeS3Error answers with the XML error document S3 clients expect, so an
// SDK surfaces a real error code instead of an empty body. The message is
// generic unless the proxy runs in debug mode — a refused caller learns
// that it was refused, not which check refused it.
func writeS3Error(w http.ResponseWriter, status int, code, message string) {
	var b bytes.Buffer
	b.WriteString(xml.Header)
	b.WriteString("<Error><Code>")
	xml.EscapeText(&b, []byte(code))
	b.WriteString("</Code><Message>")
	xml.EscapeText(&b, []byte(message))
	b.WriteString("</Message></Error>")

	w.Header().Set("Content-Type", "application/xml")
	w.Header().Set("Content-Length", fmt.Sprint(b.Len()))
	w.WriteHeader(status)
	_, _ = w.Write(b.Bytes())
}

// statusRecorder remembers the status code for the access log. It forwards
// Flush so that object downloads keep streaming through the reverse proxy
// rather than filling a buffer first.
type statusRecorder struct {
	http.ResponseWriter
	status  int
	written bool
}

func (r *statusRecorder) WriteHeader(status int) {
	if !r.written {
		r.status, r.written = status, true
	}
	r.ResponseWriter.WriteHeader(status)
}

func (r *statusRecorder) Write(p []byte) (int, error) {
	r.written = true
	return r.ResponseWriter.Write(p)
}

func (r *statusRecorder) Flush() {
	if f, ok := r.ResponseWriter.(http.Flusher); ok {
		f.Flush()
	}
}
