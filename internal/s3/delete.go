package s3

import (
	"bytes"
	"encoding/xml"
	"fmt"
	"net/http"
)

// A batch delete is the one S3 operation that addresses object keys in the
// request BODY instead of the URL path:
//
//	POST /<bucket>?delete
//	<Delete><Object><Key>a.csv</Key></Object><Object><Key>b.csv</Key></Object></Delete>
//
// The path carries no key, so neither a prefix injection nor an authorization
// check that runs on the URL sees anything to act on. This file is the body
// half of that operation: reading the keys out, writing a rebuilt request
// back, and answering with the per-key result S3 itself returns — the caller
// decides which keys survive.

// MaxDeleteKeys is S3's own limit on a batch delete. Enforcing it bounds the
// authorization work one request can ask for.
const MaxDeleteKeys = 1000

// DeleteEntry is one object named in a batch delete.
type DeleteEntry struct {
	Key       string `xml:"Key"`
	VersionID string `xml:"VersionId,omitempty"`
}

// DeleteRequest is the whole schema of a DeleteObjects request: an optional
// Quiet flag and one entry per object. Because it is this small and this
// fixed, the body is re-emitted from the parsed form rather than patched in
// place — a caller has to be able to *drop* entries, and a byte-level edit
// that removes elements is far easier to get subtly wrong.
type DeleteRequest struct {
	XMLName xml.Name      `xml:"Delete"`
	Quiet   bool          `xml:"Quiet"`
	Objects []DeleteEntry `xml:"Object"`
}

// ParseDeleteRequest reads a DeleteObjects body, rejecting the shapes that
// cannot be acted on: unparseable, empty, or past S3's own key limit.
func ParseDeleteRequest(raw []byte) (*DeleteRequest, error) {
	var parsed DeleteRequest
	if err := xml.Unmarshal(raw, &parsed); err != nil {
		return nil, fmt.Errorf("batch delete: cannot parse request body: %v", err)
	}
	if len(parsed.Objects) == 0 {
		return nil, fmt.Errorf("batch delete: request body contains no <Object> entry")
	}
	if len(parsed.Objects) > MaxDeleteKeys {
		return nil, fmt.Errorf("batch delete: request body contains %d keys, the limit is %d", len(parsed.Objects), MaxDeleteKeys)
	}
	return &parsed, nil
}

// BuildDeleteRequest re-emits a DeleteObjects body from the entries that are
// to be sent upstream.
func BuildDeleteRequest(quiet bool, objects []DeleteEntry) []byte {
	var b bytes.Buffer
	b.WriteString(`<?xml version="1.0" encoding="UTF-8"?>`)
	b.WriteString(`<Delete xmlns="http://s3.amazonaws.com/doc/2006-03-01/">`)
	if quiet {
		b.WriteString(`<Quiet>true</Quiet>`)
	}
	for _, obj := range objects {
		b.WriteString(`<Object><Key>`)
		xml.EscapeText(&b, []byte(obj.Key))
		b.WriteString(`</Key>`)
		if obj.VersionID != "" {
			b.WriteString(`<VersionId>`)
			xml.EscapeText(&b, []byte(obj.VersionID))
			b.WriteString(`</VersionId>`)
		}
		b.WriteString(`</Object>`)
	}
	b.WriteString(`</Delete>`)
	return b.Bytes()
}

// deleteResultCloseTag is where per-key denials are spliced into an upstream
// DeleteResult.
var deleteResultCloseTag = []byte("</DeleteResult>")

// MergeDeniedDeletes adds one AccessDenied entry per refused key to the
// upstream's DeleteResult, so the client sees exactly what S3 would have
// returned had it enforced the policy itself: the keys it was allowed to
// delete under <Deleted>, the rest under <Error>.
//
// The body is returned unchanged when it is not a DeleteResult — an upstream
// error response, for instance, which must not be dressed up as a partial
// success.
func MergeDeniedDeletes(body []byte, deniedKeys []string) []byte {
	if len(deniedKeys) == 0 {
		return body
	}
	idx := bytes.LastIndex(body, deleteResultCloseTag)
	if idx < 0 {
		return body
	}
	var b bytes.Buffer
	b.Grow(len(body) + len(deniedKeys)*96)
	b.Write(body[:idx])
	writeDeleteErrors(&b, deniedKeys)
	b.Write(body[idx:])
	return b.Bytes()
}

func writeDeleteErrors(b *bytes.Buffer, deniedKeys []string) {
	for _, key := range deniedKeys {
		b.WriteString(`<Error><Key>`)
		xml.EscapeText(b, []byte(key))
		b.WriteString(`</Key><Code>AccessDenied</Code><Message>Access Denied</Message></Error>`)
	}
}

// WriteDeleteResult answers a batch delete in which every key was refused. S3
// reports per-key failures with a 200 and an error entry per key, and a client
// that receives a blanket 403 instead cannot tell which of its keys were the
// problem.
func WriteDeleteResult(w http.ResponseWriter, deniedKeys []string) {
	var b bytes.Buffer
	b.WriteString(xml.Header)
	b.WriteString(`<DeleteResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/">`)
	writeDeleteErrors(&b, deniedKeys)
	b.WriteString(`</DeleteResult>`)

	w.Header().Set("Content-Type", "application/xml")
	w.Header().Set("Content-Length", fmt.Sprint(b.Len()))
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write(b.Bytes())
}
