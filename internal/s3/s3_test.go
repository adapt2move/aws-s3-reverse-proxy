package s3

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestStripPrefixFromValue(t *testing.T) {
	prefix := []byte("acme/")
	cases := []struct{ in, want string }{
		{"acme/a.csv", "a.csv"},
		{"acme%2Fa.csv", "a.csv"},
		{"/bucket/acme/a.csv", "/bucket/a.csv"},
		{"https://host/bucket/acme/a.csv", "https://host/bucket/a.csv"},
		// No occurrence: never remove "the wrong" bytes.
		{"other/a.csv", "other/a.csv"},
		{"", ""},
	}
	for _, tc := range cases {
		assert.Equal(t, tc.want, string(stripPrefixFromValue([]byte(tc.in), prefix)), tc.in)
	}
}

// An upstream failure must not be dressed up as a partial success by
// splicing per-key denials into a body that is not a DeleteResult.
func TestMergeDeniedDeletesLeavesForeignBodiesAlone(t *testing.T) {
	errorBody := []byte(`<?xml version="1.0"?><Error><Code>NoSuchBucket</Code></Error>`)
	assert.Equal(t, errorBody, MergeDeniedDeletes(errorBody, []string{"a.txt"}))

	result := []byte(`<DeleteResult></DeleteResult>`)
	assert.Equal(t,
		`<DeleteResult><Error><Key>a.txt</Key><Code>AccessDenied</Code><Message>Access Denied</Message></Error></DeleteResult>`,
		string(MergeDeniedDeletes(result, []string{"a.txt"})))
}

// The predicate is the whole authorization interface of this package: an
// entry it rejects has to disappear as a block, and one it accepts has to
// survive byte for byte.
func TestFilterListEntries(t *testing.T) {
	body := []byte(`<ListBucketResult>` +
		`<Contents><Key>keep/a.csv</Key><Size>1</Size></Contents>` +
		`<Contents><Key>hide/b.csv</Key><Size>2</Size></Contents>` +
		`<CommonPrefixes><Prefix>keep/sub/</Prefix></CommonPrefixes>` +
		`<CommonPrefixes><Prefix>hide/sub/</Prefix></CommonPrefixes>` +
		`</ListBucketResult>`)

	out := string(FilterListEntries(body, func(key string) bool {
		return strings.HasPrefix(key, "keep/")
	}))
	assert.Contains(t, out, "<Key>keep/a.csv</Key>")
	assert.Contains(t, out, "<Prefix>keep/sub/</Prefix>")
	assert.NotContains(t, out, "hide/")

	// A percent-encoded key (EncodingType=url) is matched decoded, because
	// that is the form policy patterns are written against.
	encoded := []byte(`<ListBucketResult><Contents><Key>keep%2Fc.csv</Key></Contents></ListBucketResult>`)
	assert.Contains(t, string(FilterListEntries(encoded, func(key string) bool {
		return strings.HasPrefix(key, "keep/")
	})), "keep%2Fc.csv")
}

// Everything the whitelist does not name is refused, and the refusal says
// which of the two kinds it is: an unsupported shape (403) or a key that
// cannot be interpreted safely (400).
func TestClassify(t *testing.T) {
	cases := []struct {
		name    string
		method  string
		target  string
		headers http.Header
		want    Kind
		wantErr error
	}{
		{name: "get object", method: http.MethodGet, target: "/b/a/x.csv", want: GetObject},
		{name: "head object", method: http.MethodHead, target: "/b/a/x.csv", want: HeadObject},
		{name: "put object", method: http.MethodPut, target: "/b/a/x.csv", want: PutObject},
		{name: "delete object", method: http.MethodDelete, target: "/b/a/x.csv", want: DeleteObject},
		{name: "list", method: http.MethodGet, target: "/b/?list-type=2&prefix=a/", want: ListObjects},
		{name: "list without the trailing slash", method: http.MethodGet, target: "/b?list-type=2", want: ListObjects},
		{name: "batch delete", method: http.MethodPost, target: "/b?delete", want: DeleteObjects},
		{name: "create multipart", method: http.MethodPost, target: "/b/a/x.csv?uploads", want: CreateMultipartUpload},
		{name: "upload part", method: http.MethodPut, target: "/b/a/x.csv?partNumber=1&uploadId=u", want: UploadPart},
		{name: "complete multipart", method: http.MethodPost, target: "/b/a/x.csv?uploadId=u", want: CompleteMultipartUpload},
		{name: "abort multipart", method: http.MethodDelete, target: "/b/a/x.csv?uploadId=u", want: AbortMultipartUpload},
		{name: "list parts", method: http.MethodGet, target: "/b/a/x.csv?uploadId=u", want: ListParts},

		{name: "copy source", method: http.MethodPut, target: "/b/a/x.csv",
			headers: http.Header{"X-Amz-Copy-Source": {"/b/a/y.csv"}}, wantErr: ErrUnsupportedOperation},
		{name: "presigned", method: http.MethodGet, target: "/b/a/x.csv?X-Amz-Signature=deadbeef", wantErr: ErrUnsupportedOperation},
		{name: "bucket acl", method: http.MethodGet, target: "/b?acl", wantErr: ErrUnsupportedOperation},
		{name: "head bucket", method: http.MethodHead, target: "/b", wantErr: ErrUnsupportedOperation},
		{name: "list buckets", method: http.MethodGet, target: "/", wantErr: ErrUnsupportedOperation},
		{name: "unsupported method", method: http.MethodPatch, target: "/b/a/x.csv", wantErr: ErrUnsupportedOperation},
		{name: "traversal", method: http.MethodGet, target: "/b/a/../../x.csv", wantErr: ErrInvalidKey},
		{name: "absolute key", method: http.MethodGet, target: "/b//etc/passwd", wantErr: ErrInvalidKey},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			req := httptest.NewRequest(tc.method, tc.target, nil)
			for name, values := range tc.headers {
				req.Header[http.CanonicalHeaderKey(name)] = values
			}
			op, err := Classify(req)
			if tc.wantErr != nil {
				require.ErrorIs(t, err, tc.wantErr)
				return
			}
			require.NoError(t, err)
			assert.Equal(t, tc.want, op.Kind)
		})
	}
}

// Keys are normalized by rejection: a key that is not already in the exact
// form the proxy would forward is refused rather than rewritten, because a
// silent normalization is how a key escapes the prefix injected in front of
// it.
func TestValidateObjectKey(t *testing.T) {
	for _, ok := range []string{"a.csv", "a/b/c.csv", "folder/", "a b+c.csv", "a%20b.csv", "ünïcode.csv"} {
		assert.NoError(t, ValidateObjectKey(ok), ok)
	}
	for _, bad := range []string{
		"", "/leading", "..", "../x", "a/../b", "a/./b", "a//b", "/",
		"a\x00b", "a\rb", "a\nb",
		// These arrive here after the http layer has decoded the request
		// path once, so on the wire they were `%252e%252e` and `a%252fb`:
		// doubly-encoded, and a traversal only for a store that decodes
		// twice. Refused rather than left to the upstream's decoder.
		"%2e%2e", "a%2fb",
	} {
		assert.Error(t, ValidateObjectKey(bad), bad)
	}
}

// The framing has to go, whether the body is streamed or buffered — an
// upstream that stored it verbatim would hold a file spelled
// `165D\r\nPAR1…\r\n\r\n`.
func TestChunkedDecoding(t *testing.T) {
	payload := "PAR1the-real-object-content"

	t.Run("streamed", func(t *testing.T) {
		framed := "1b;chunk-signature=deadbeef\r\n" + payload + "\r\n0;chunk-signature=cafe\r\n\r\n"
		out := make([]byte, 0, len(payload))
		buf := make([]byte, 7) // deliberately smaller than one chunk
		r := NewChunkedReader(strings.NewReader(framed))
		for {
			n, err := r.Read(buf)
			out = append(out, buf[:n]...)
			if err != nil {
				require.Error(t, err)
				assert.Contains(t, err.Error(), "EOF")
				break
			}
		}
		assert.Equal(t, payload, string(out))
	})

	t.Run("buffered, with the trailer recovered", func(t *testing.T) {
		framed := "1b\r\n" + payload + "\r\n0\r\nx-amz-checksum-crc32:abcd1234\r\nx-amz-trailer-signature:nope\r\n\r\n"
		decoded, trailers, err := DecodeChunked(strings.NewReader(framed), 1<<20, "test")
		require.NoError(t, err)
		assert.Equal(t, payload, string(decoded))

		promoted := ChecksumTrailers(trailers)
		assert.Equal(t, "abcd1234", promoted.Get("X-Amz-Checksum-Crc32"))
		// The trailer signature covers framing bytes signed with the client's
		// key; neither exists on the upstream request.
		assert.Empty(t, promoted.Get("X-Amz-Trailer-Signature"))
	})

	t.Run("a body past the cap is refused rather than buffered", func(t *testing.T) {
		framed := "1b\r\n" + payload + "\r\n0\r\n\r\n"
		_, _, err := DecodeChunked(strings.NewReader(framed), 8, "test")
		require.Error(t, err)
		assert.Contains(t, err.Error(), "exceeds 8 bytes")
	})
}

// `aws-chunked` is only one of the codings a client may have applied; the
// ones underneath it still describe the payload and have to survive.
func TestStripStreamingMarkers(t *testing.T) {
	header := http.Header{
		"Content-Encoding":             {"aws-chunked,gzip"},
		"X-Amz-Content-Sha256":         {"STREAMING-UNSIGNED-PAYLOAD-TRAILER"},
		"X-Amz-Decoded-Content-Length": {"27"},
		"X-Amz-Trailer":                {"x-amz-checksum-crc32"},
	}
	StripStreamingMarkers(header)
	assert.Equal(t, "gzip", header.Get("Content-Encoding"))
	assert.Empty(t, header.Get("X-Amz-Content-Sha256"))
	assert.Empty(t, header.Get("X-Amz-Decoded-Content-Length"))
	assert.Empty(t, header.Get("X-Amz-Trailer"))

	only := http.Header{"Content-Encoding": {"aws-chunked"}}
	StripStreamingMarkers(only)
	assert.NotContains(t, only, "Content-Encoding")
}

// A coding that cannot be undone is a body the tenant prefix cannot be
// stripped out of, so it fails closed rather than passing through.
func TestDecodeContentEncoding(t *testing.T) {
	plain := []byte("<ListBucketResult/>")
	for _, encoding := range []string{"", "identity", "IDENTITY"} {
		out, err := DecodeContentEncoding(plain, encoding, 1<<20)
		require.NoError(t, err, encoding)
		assert.Equal(t, plain, out)
	}
	_, err := DecodeContentEncoding(plain, "br", 1<<20)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "cannot rewrite")
}

func TestParseDeleteRequest(t *testing.T) {
	parsed, err := ParseDeleteRequest([]byte(
		`<Delete><Quiet>true</Quiet><Object><Key>a.csv</Key></Object><Object><Key>b.csv</Key><VersionId>v1</VersionId></Object></Delete>`))
	require.NoError(t, err)
	assert.True(t, parsed.Quiet)
	assert.Equal(t, []DeleteEntry{{Key: "a.csv"}, {Key: "b.csv", VersionID: "v1"}}, parsed.Objects)

	// Round-tripping the parsed form is how entries get dropped safely.
	rebuilt := BuildDeleteRequest(parsed.Quiet, parsed.Objects[:1])
	assert.Contains(t, string(rebuilt), "<Quiet>true</Quiet>")
	assert.Contains(t, string(rebuilt), "<Object><Key>a.csv</Key></Object>")
	assert.NotContains(t, string(rebuilt), "b.csv")

	for _, bad := range []string{"not xml at all", `<Delete></Delete>`} {
		_, err := ParseDeleteRequest([]byte(bad))
		assert.Error(t, err, bad)
	}

	var many strings.Builder
	many.WriteString("<Delete>")
	for i := 0; i <= MaxDeleteKeys; i++ {
		many.WriteString("<Object><Key>x</Key></Object>")
	}
	many.WriteString("</Delete>")
	_, err = ParseDeleteRequest([]byte(many.String()))
	require.Error(t, err)
	assert.Contains(t, err.Error(), "the limit is 1000")
}
