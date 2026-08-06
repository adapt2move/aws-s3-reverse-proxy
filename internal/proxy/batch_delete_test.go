package proxy

import (
	"encoding/xml"
	"fmt"
	"net/http"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/Kriechi/aws-s3-reverse-proxy/internal/s3"
)

// echoDeleteResult makes the fake upstream answer a batch delete the way S3
// does: one <Deleted> entry per key it received, in the prefixed form the
// proxy sent.
func echoDeleteResult(u *fakeUpstream) {
	u.respond = func(w http.ResponseWriter, r *http.Request) {
		got := u.latest()
		var b strings.Builder
		b.WriteString(xml.Header)
		b.WriteString(`<DeleteResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/">`)
		for _, key := range parsedKeys(got) {
			fmt.Fprintf(&b, `<Deleted><Key>%s</Key></Deleted>`, key)
		}
		b.WriteString(`</DeleteResult>`)
		w.Header().Set("Content-Type", "application/xml")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(b.String()))
	}
}

func parsedKeys(req recordedRequest) []string {
	var parsed s3.DeleteRequest
	_ = xml.Unmarshal([]byte(req.body), &parsed)
	keys := make([]string, 0, len(parsed.Objects))
	for _, obj := range parsed.Objects {
		keys = append(keys, obj.Key)
	}
	return keys
}

func deleteBatchBody(keys ...string) []byte {
	var b strings.Builder
	b.WriteString(`<?xml version="1.0" encoding="UTF-8"?><Delete>`)
	for _, key := range keys {
		fmt.Fprintf(&b, `<Object><Key>%s</Key></Object>`, key)
	}
	b.WriteString(`</Delete>`)
	return []byte(b.String())
}

// A batch delete is authorized key by key. The keys policy refuses come
// back as per-key AccessDenied entries — the whole batch is not rejected,
// which is what S3 itself does and what a client can act on.
func TestDeleteObjectsAuthorizesEachKey(t *testing.T) {
	h, upstream := newTestProxy(t)
	echoDeleteResult(upstream)

	body := deleteBatchBody(
		"workspaces/w1/out/keep.txt",         // full for rw
		"workspaces/w1/inbox/note.txt",       // read-only carve-out: delete denied
		"datasets/2026/old.csv",              // full for rw
		"datasets/2026/private/salaries.csv", // denied carve-out inside the same tree
		"nowhere/x.txt",                      // matched by no rule: implicit deny
	)
	rec := do(t, h, clientRequest{
		method: http.MethodPost, target: "/bucket?delete",
		body: body, tenant: tenantA, level: "rw",
	})
	require.Equal(t, http.StatusOK, rec.Code)

	// Only the authorized keys travelled upstream, each carrying the
	// tenant prefix.
	assert.Equal(t, []string{
		tenantA + "/workspaces/w1/out/keep.txt",
		tenantA + "/datasets/2026/old.csv",
	}, parsedKeys(upstream.last(t)))

	out := rec.Body.String()
	// The prefix is stripped back out of the result...
	assert.Contains(t, out, "<Deleted><Key>workspaces/w1/out/keep.txt</Key></Deleted>")
	assert.Contains(t, out, "<Deleted><Key>datasets/2026/old.csv</Key></Deleted>")
	assert.NotContains(t, out, tenantA)
	// ... and each refused key is reported individually.
	assert.Contains(t, out, "<Error><Key>workspaces/w1/inbox/note.txt</Key><Code>AccessDenied</Code>")
	assert.Contains(t, out, "<Error><Key>datasets/2026/private/salaries.csv</Key><Code>AccessDenied</Code>")
	assert.Contains(t, out, "<Error><Key>nowhere/x.txt</Key><Code>AccessDenied</Code>")
}

func TestDeleteObjectsAllKeysDenied(t *testing.T) {
	h, upstream := newTestProxy(t)

	rec := do(t, h, clientRequest{
		method: http.MethodPost, target: "/bucket?delete",
		body:   deleteBatchBody("workspaces/w1/inbox/a.txt", "nowhere/b.txt"),
		tenant: tenantA, level: "rw",
	})

	assert.Equal(t, http.StatusOK, rec.Code)
	assert.Equal(t, 0, upstream.count(), "with nothing authorized there is no upstream call to make")
	assert.Contains(t, rec.Body.String(), "<Error><Key>workspaces/w1/inbox/a.txt</Key><Code>AccessDenied</Code>")
	assert.Contains(t, rec.Body.String(), "<Error><Key>nowhere/b.txt</Key><Code>AccessDenied</Code>")
}

func TestDeleteObjectsRecomputesContentMD5(t *testing.T) {
	h, upstream := newTestProxy(t)
	echoDeleteResult(upstream)

	rec := do(t, h, clientRequest{
		method: http.MethodPost, target: "/bucket?delete",
		body:   deleteBatchBody("datasets/a.csv"),
		tenant: tenantA, level: "rw",
		headers: http.Header{"Content-Md5": {"nonsense-from-the-client"}},
	})
	require.Equal(t, http.StatusOK, rec.Code)

	got := upstream.last(t)
	assert.NotEqual(t, "nonsense-from-the-client", got.header.Get("Content-Md5"),
		"the body changed, so the client's digest cannot be forwarded")
	assert.NotEmpty(t, got.header.Get("Content-Md5"))
}

func TestDeleteObjectsPreservesQuiet(t *testing.T) {
	h, upstream := newTestProxy(t)
	echoDeleteResult(upstream)

	body := []byte(`<?xml version="1.0" encoding="UTF-8"?><Delete><Quiet>true</Quiet>` +
		`<Object><Key>datasets/a.csv</Key></Object></Delete>`)
	rec := do(t, h, clientRequest{
		method: http.MethodPost, target: "/bucket?delete",
		body: body, tenant: tenantA, level: "rw",
	})
	require.Equal(t, http.StatusOK, rec.Code)
	assert.Contains(t, upstream.last(t).body, "<Quiet>true</Quiet>")
}

// A key that cannot be interpreted safely is not an authorization outcome:
// the whole batch is refused rather than silently normalized.
func TestDeleteObjectsRejectsMalformedKeys(t *testing.T) {
	for _, key := range []string{"../../etc/passwd", "/absolute/key", "datasets/../../escape", ""} {
		t.Run(key, func(t *testing.T) {
			h, upstream := newTestProxy(t)
			rec := do(t, h, clientRequest{
				method: http.MethodPost, target: "/bucket?delete",
				body:   deleteBatchBody(key),
				tenant: tenantA, level: "rws",
			})
			assert.Equal(t, http.StatusBadRequest, rec.Code)
			assert.Equal(t, 0, upstream.count())
		})
	}
}

func TestDeleteObjectsBodyCap(t *testing.T) {
	h, upstream := newTestProxy(t, func(c *Config) { c.Limits.DeleteBody = 64 })

	keys := make([]string, 0, 64)
	for i := 0; i < 64; i++ {
		keys = append(keys, fmt.Sprintf("datasets/%d.csv", i))
	}
	rec := do(t, h, clientRequest{
		method: http.MethodPost, target: "/bucket?delete",
		body:   deleteBatchBody(keys...),
		tenant: tenantA, level: "rw",
	})
	assert.Equal(t, http.StatusBadRequest, rec.Code)
	assert.Equal(t, 0, upstream.count())
}

func TestDeleteObjectsKeyLimit(t *testing.T) {
	h, upstream := newTestProxy(t, func(c *Config) { c.Limits.DeleteBody = 1 << 20 })

	keys := make([]string, 0, s3.MaxDeleteKeys+1)
	for i := 0; i <= s3.MaxDeleteKeys; i++ {
		keys = append(keys, fmt.Sprintf("datasets/%d.csv", i))
	}
	rec := do(t, h, clientRequest{
		method: http.MethodPost, target: "/bucket?delete",
		body:   deleteBatchBody(keys...),
		tenant: tenantA, level: "rw",
	})
	assert.Equal(t, http.StatusBadRequest, rec.Code)
	assert.Equal(t, 0, upstream.count())
}

// The keys in a batch delete arriving as an aws-chunked body (the AWS JS
// SDK v3 does exactly this) are authorized and prefixed like any other.
func TestDeleteObjectsOverAwsChunked(t *testing.T) {
	h, upstream := newTestProxy(t)
	echoDeleteResult(upstream)

	body := deleteBatchBody("datasets/a.csv", "workspaces/w1/inbox/b.txt")
	framed := fmt.Sprintf("%x\r\n%s\r\n0\r\n\r\n", len(body), body)
	rec := do(t, h, clientRequest{
		method: http.MethodPost, target: "/bucket?delete",
		body: []byte(framed), tenant: tenantA, level: "rw",
		headers: http.Header{
			"X-Amz-Content-Sha256":         {"STREAMING-AWS4-HMAC-SHA256-PAYLOAD"},
			"X-Amz-Decoded-Content-Length": {fmt.Sprint(len(body))},
			"Content-Encoding":             {"aws-chunked"},
		},
	})
	require.Equal(t, http.StatusOK, rec.Code)

	assert.Equal(t, []string{tenantA + "/datasets/a.csv"}, parsedKeys(upstream.last(t)))
	assert.Contains(t, rec.Body.String(), "<Error><Key>workspaces/w1/inbox/b.txt</Key><Code>AccessDenied</Code>")
}

// A batch delete arriving in aws-chunked framing is buffered under the
// DeleteObjects cap rather than the aws-chunked one — the body is a key list
// to be parsed and rebuilt, not an object to be de-framed and passed on. That
// switch in prepareBody has its own limit, so it needs its own test.
func TestDeleteObjectsOverAwsChunkedRespectsTheDeleteCap(t *testing.T) {
	// Generous room for a framed object body, almost none for a key list: if
	// the switch picked the wrong limit, this would be accepted.
	h, upstream := newTestProxy(t, func(c *Config) {
		c.Limits.ChunkedBody = 1 << 20
		c.Limits.DeleteBody = 64
	})

	keys := make([]string, 0, 64)
	for i := 0; i < 64; i++ {
		keys = append(keys, fmt.Sprintf("datasets/%d.csv", i))
	}
	body := deleteBatchBody(keys...)
	framed := fmt.Sprintf("%x\r\n%s\r\n0\r\n\r\n", len(body), body)

	rec := do(t, h, clientRequest{
		method: http.MethodPost, target: "/bucket?delete",
		body: []byte(framed), tenant: tenantA, level: "rw",
		headers: http.Header{
			"X-Amz-Content-Sha256":         {"STREAMING-AWS4-HMAC-SHA256-PAYLOAD"},
			"X-Amz-Decoded-Content-Length": {fmt.Sprint(len(body))},
			"Content-Encoding":             {"aws-chunked"},
		},
	})
	assert.Equal(t, http.StatusBadRequest, rec.Code)
	assert.Equal(t, 0, upstream.count())
}
