package main

import (
	"bytes"
	"crypto/md5"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/xml"
	"errors"
	"fmt"
	"net/http"
)

// A batch delete is the one S3 operation that addresses object keys in the
// request BODY instead of the URL path:
//
//	POST /<bucket>?delete
//	<Delete><Object><Key>a.csv</Key></Object><Object><Key>b.csv</Key></Object></Delete>
//
// The path carries no key, so neither the prefix injection nor the
// authorization that runs on the URL sees anything to act on. Both are
// applied to the keys in the body instead — and, unlike everything else,
// per key: S3 answers a batch delete with a per-key result, so a key the
// caller may not delete becomes an AccessDenied entry in that result
// rather than a rejection of the whole request.

// maxDeleteObjectsKeys is S3's own limit on a batch delete. Enforcing it
// here bounds the authorization work one request can ask for.
const maxDeleteObjectsKeys = 1000

// errAllKeysDenied signals that policy refused every key in a batch. There
// is no upstream call left to make, so the caller answers directly.
var errAllKeysDenied = errors.New("batch delete: no key survived authorization")

type deleteObjectEntry struct {
	Key       string `xml:"Key"`
	VersionID string `xml:"VersionId,omitempty"`
}

// deleteObjectsRequestBody is the whole schema of a DeleteObjects request:
// an optional Quiet flag and one entry per object. Because it is this
// small and this fixed, the body is re-emitted from the parsed form rather
// than patched in place — the rewrite has to be able to *drop* entries, and
// a byte-level edit that removes elements is far easier to get subtly
// wrong.
type deleteObjectsRequestBody struct {
	XMLName xml.Name            `xml:"Delete"`
	Quiet   bool                `xml:"Quiet"`
	Objects []deleteObjectEntry `xml:"Object"`
}

// prepareDeleteBody authorizes every key of a batch delete individually,
// prefixes the survivors and rebuilds the request body from them.
func (h *Handler) prepareDeleteBody(raw []byte, reqHeader http.Header, st *requestState) (*preparedBody, string, error) {
	var parsed deleteObjectsRequestBody
	if err := xml.Unmarshal(raw, &parsed); err != nil {
		return nil, "", fmt.Errorf("batch delete: cannot parse request body: %v", err)
	}
	if len(parsed.Objects) == 0 {
		return nil, "", fmt.Errorf("batch delete: request body contains no <Object> entry")
	}
	if len(parsed.Objects) > maxDeleteObjectsKeys {
		return nil, "", fmt.Errorf("batch delete: request body contains %d keys, the limit is %d", len(parsed.Objects), maxDeleteObjectsKeys)
	}

	// A batch matches as many rules as it has keys, but the access log
	// carries one. Report the rule behind the first denial when there is
	// one — that is the line an operator is trying to explain — and the
	// rule the batch matched otherwise.
	var firstRule, firstDenyRule string
	var denied bool

	allowed := make([]deleteObjectEntry, 0, len(parsed.Objects))
	for i, obj := range parsed.Objects {
		// A malformed key is refused for the whole batch rather than turned
		// into a per-key denial: it is not an authorization outcome but a
		// request we cannot safely interpret at all.
		if err := validateObjectKey(obj.Key); err != nil {
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
		allowed = append(allowed, obj)
	}
	st.rule = firstRule
	if denied {
		st.rule = firstDenyRule
	}
	if len(allowed) == 0 {
		return nil, "", errAllKeysDenied
	}

	body := buildDeleteObjectsBody(parsed.Quiet, allowed)
	// The client's digests describe the body it sent, not the one we are
	// about to send. S3 requires a Content-MD5 on a batch delete, so that
	// one is recomputed; the rest are dropped before they can be copied
	// onto the upstream request.
	dropStaleBodyDigestHeaders(reqHeader)
	sum := md5.Sum(body)
	payload := sha256.Sum256(body)
	return &preparedBody{
		reader:        bytes.NewReader(body),
		contentLength: int64(len(body)),
		extraHeaders:  http.Header{"Content-Md5": {base64.StdEncoding.EncodeToString(sum[:])}},
	}, hex.EncodeToString(payload[:]), nil
}

func buildDeleteObjectsBody(quiet bool, objects []deleteObjectEntry) []byte {
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

// deleteResultCloseTag is where per-key denials are spliced into an
// upstream DeleteResult.
var deleteResultCloseTag = []byte("</DeleteResult>")

// mergeDeniedDeletes adds one AccessDenied entry per refused key to the
// upstream's DeleteResult, so the client sees exactly what S3 would have
// returned had it enforced the policy itself: the keys it was allowed to
// delete under <Deleted>, the rest under <Error>.
//
// The body is returned unchanged when it is not a DeleteResult — an
// upstream error response, for instance, which must not be dressed up as a
// partial success.
func mergeDeniedDeletes(body []byte, deniedKeys []string) []byte {
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

// writeDeleteResult answers a batch delete in which policy refused every
// key. S3 reports per-key failures with a 200 and an error entry per key,
// and a client that receives a blanket 403 instead cannot tell which of its
// keys were the problem.
func writeDeleteResult(w http.ResponseWriter, deniedKeys []string) {
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
