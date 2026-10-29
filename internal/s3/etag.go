package s3

import (
	"bytes"
	"encoding/xml"
	"html"
	"regexp"
	"strings"
)

// Entity tags, kept apart from how any one protocol spells them.
//
// An ETag is opaque. This proxy compares them, stores them and sends them back
// as preconditions, and for all three it needs one canonical form: the value
// with every wrapper an object store put around it taken off. The wrappers
// themselves are serialization, and each place an ETag leaves the proxy adds
// its own — standard double quotes in responses and local cache validators,
// nothing in a multipart part list. Write If-Match conditions preserve a
// client's bare values for stores that require them.
//
// Doing it once, here, is not tidiness. At least one object store answers
// `ETag: "'abcef123'"` — a single-quoted value inside the double quotes — and
// then refuses that same value with its single quotes in a precondition or a
// part list. A proxy that compared, cached and replayed the header verbatim
// would hand every client a validator the store itself rejects.
//
// What canonicalization never does is interpret the value. An ETag is not the
// MD5 of the object: under SSE-C, or for a multipart upload, it is not even
// related to the bytes. The `-N` suffix of a multipart ETag, the case of its
// hex and any apostrophe inside it are part of the value and stay where they
// are.

// CanonicalETag returns the opaque value of an entity tag and whether it is a
// weak validator.
//
// It removes the `W/` marker and then every matching pair of surrounding
// single or double quotes, however deeply nested — `"'abc'"`, `'abc'` and
// `"abc"` are all `abc`. Only a pair that matches comes off, so `"abc'` is left
// as it is. Applying it to its own output changes nothing.
func CanonicalETag(raw string) (value string, weak bool) {
	value = strings.TrimSpace(raw)
	if strings.HasPrefix(value, "W/") {
		weak, value = true, value[2:]
	}
	for len(value) >= 2 {
		first, last := value[0], value[len(value)-1]
		if (first != '"' && first != '\'') || first != last {
			break
		}
		value = value[1 : len(value)-1]
	}
	return value, weak
}

// FormatETag spells a canonical value as an HTTP entity tag: `"abc"`, or
// `W/"abc"` for a weak one. An empty value stays empty, so a header that was
// absent does not become one that says `""`.
func FormatETag(value string, weak bool) string {
	if value == "" {
		return ""
	}
	if weak {
		return `W/"` + value + `"`
	}
	return `"` + value + `"`
}

// NormalizeETag rewrites an ETag header value into standard entity-tag syntax.
func NormalizeETag(raw string) string {
	return FormatETag(CanonicalETag(raw))
}

// SameETag reports whether two ETag header values name the same entity, by
// the strong comparison: both must be strong and their values equal. Two weak
// validators may describe the same bytes, but nothing that hangs a
// precondition or a cache entry off the answer may assume so.
func SameETag(a, b string) bool {
	av, aw := CanonicalETag(a)
	bv, bw := CanonicalETag(b)
	return av != "" && !aw && !bw && av == bv
}

// NormalizeETagList rewrites the value of an If-Match or If-None-Match
// header: `*`, or a comma-separated list of entity tags. Each tag is put into
// standard syntax and keeps its weakness, so the condition means what the
// client asked for when evaluated by the local HTTP cache.
//
// A comma inside a quoted value is part of the value, not a separator.
func NormalizeETagList(raw string) string {
	return rewriteETagList(raw, NormalizeETag)
}

// NormalizeWriteIfMatch removes malformed quote wrappers but keeps bare
// client values bare. Ceph RGW compares a write's If-Match literally with the
// stored ETag; adding double quotes would reject a matching condition. Quoted
// and weak validators retain their standard syntax.
func NormalizeWriteIfMatch(raw string) string {
	return rewriteETagList(raw, func(item string) string {
		value, weak := CanonicalETag(item)
		if !weak && value == strings.TrimSpace(item) {
			return value
		}
		return FormatETag(value, weak)
	})
}

func rewriteETagList(raw string, rewrite func(string) string) string {
	if strings.TrimSpace(raw) == "*" {
		return "*"
	}
	var out []string
	for _, item := range splitETagList(raw) {
		if normalized := rewrite(item); normalized != "" {
			out = append(out, normalized)
		}
	}
	return strings.Join(out, ", ")
}

// NormalizeIfRange rewrites an If-Range value when it is an entity tag, and
// leaves it alone when it is an HTTP date — If-Range takes either.
func NormalizeIfRange(raw string) string {
	trimmed := strings.TrimSpace(raw)
	if strings.HasPrefix(trimmed, `"`) || strings.HasPrefix(trimmed, `'`) || strings.HasPrefix(trimmed, "W/") {
		return NormalizeETag(trimmed)
	}
	return raw
}

func splitETagList(raw string) []string {
	var items []string
	var current strings.Builder
	quoted := false
	for _, r := range raw {
		switch {
		case r == '"':
			quoted = !quoted
			current.WriteRune(r)
		case r == ',' && !quoted:
			items = append(items, current.String())
			current.Reset()
		default:
			current.WriteRune(r)
		}
	}
	return append(items, current.String())
}

var etagElementRegexp = regexp.MustCompile(`<ETag>([^<]*)</ETag>`)

// NormalizeXMLETags rewrites every <ETag> element of an S3 response document
// into standard entity-tag syntax, so a listing or a multipart result hands a
// client the same validator a header would have.
func NormalizeXMLETags(body []byte) []byte {
	return rewriteXMLETags(body, NormalizeETag)
}

// CanonicalXMLETags rewrites every <ETag> element of a request document to the
// bare canonical value, which is what a CompleteMultipartUpload part list
// carries: `abc`, not `"abc"` and never `'abc'`.
func CanonicalXMLETags(body []byte) []byte {
	return rewriteXMLETags(body, func(raw string) string {
		value, _ := CanonicalETag(raw)
		return value
	})
}

func rewriteXMLETags(body []byte, rewrite func(string) string) []byte {
	return etagElementRegexp.ReplaceAllFunc(body, func(match []byte) []byte {
		inner := etagElementRegexp.FindSubmatch(match)[1]
		// The value arrives XML-escaped — `&#34;abc&#34;` or `&quot;abc&quot;`
		// for the quotes — and has to be compared unescaped.
		rewritten := rewrite(html.UnescapeString(string(inner)))
		var b bytes.Buffer
		b.WriteString("<ETag>")
		_ = xml.EscapeText(&b, []byte(rewritten))
		b.WriteString("</ETag>")
		return b.Bytes()
	})
}
