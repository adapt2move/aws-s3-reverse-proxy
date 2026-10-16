package s3

import (
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// The table from the issue, plus the cases that make "only matching
// surrounding wrappers" mean something.
func TestCanonicalETag(t *testing.T) {
	cases := []struct {
		raw   string
		value string
		weak  bool
	}{
		{`abcef123`, `abcef123`, false},
		{`"abcef123"`, `abcef123`, false},
		{`'abcef123'`, `abcef123`, false},
		{`"'abcef123'"`, `abcef123`, false},
		{`'"abcef123"'`, `abcef123`, false},
		{`W/"abcef123"`, `abcef123`, true},
		{`W/"'abcef123'"`, `abcef123`, true},
		{` "abcef123" `, `abcef123`, false},
		// Multipart suffix and case are part of the value.
		{`"9B2CF535F27731C974343645A3985328-12"`, `9B2CF535F27731C974343645A3985328-12`, false},
		// An apostrophe inside the value is not a wrapper.
		{`"it's"`, `it's`, false},
		{`"a'b'c"`, `a'b'c`, false},
		// Unmatched wrappers stay.
		{`"abc'`, `"abc'`, false},
		{`'abc`, `'abc`, false},
		{``, ``, false},
		{`""`, ``, false},
	}
	for _, tc := range cases {
		value, weak := CanonicalETag(tc.raw)
		assert.Equal(t, tc.value, value, tc.raw)
		assert.Equal(t, tc.weak, weak, tc.raw)

		// Idempotent, through the serialized form as well.
		again, againWeak := CanonicalETag(FormatETag(value, weak))
		if value != "" {
			assert.Equal(t, value, again, "re-canonicalizing %q", tc.raw)
			assert.Equal(t, weak, againWeak)
		}
	}
}

func TestNormalizeETag(t *testing.T) {
	assert.Equal(t, `"abcef123"`, NormalizeETag(`"'abcef123'"`))
	assert.Equal(t, `"abcef123"`, NormalizeETag(`abcef123`))
	assert.Equal(t, `W/"abcef123"`, NormalizeETag(`W/"'abcef123'"`), "weakness survives")
	assert.Equal(t, ``, NormalizeETag(``))
}

func TestSameETagIsAStrongComparison(t *testing.T) {
	assert.True(t, SameETag(`"'abc'"`, `"abc"`))
	assert.True(t, SameETag(`abc`, `'abc'`))
	assert.False(t, SameETag(`W/"abc"`, `"abc"`))
	assert.False(t, SameETag(`W/"abc"`, `W/"abc"`), "weak validators never satisfy a strong comparison")
	assert.False(t, SameETag(``, ``))
	assert.False(t, SameETag(`"ABC"`, `"abc"`), "case is part of the value")
}

func TestNormalizeConditionHeaders(t *testing.T) {
	assert.Equal(t, `*`, NormalizeETagList(` * `))
	assert.Equal(t, `"abc"`, NormalizeETagList(`"'abc'"`))
	assert.Equal(t, `"abc", W/"def", "g,h"`, NormalizeETagList(`"'abc'", W/'def', "g,h"`))
	assert.Equal(t, `"abc"`, NormalizeIfRange(`'abc'`))
	date := "Wed, 21 Oct 2015 07:28:00 GMT"
	assert.Equal(t, date, NormalizeIfRange(date), "an If-Range date is not an entity tag")
}

func TestXMLETags(t *testing.T) {
	response := []byte(`<ListPartsResult><Part><PartNumber>1</PartNumber><ETag>&#34;&#39;abc&#39;&#34;</ETag></Part>` +
		`<Part><PartNumber>2</PartNumber><ETag>&quot;def&quot;</ETag></Part></ListPartsResult>`)
	assert.Equal(t,
		`<ListPartsResult><Part><PartNumber>1</PartNumber><ETag>&#34;abc&#34;</ETag></Part>`+
			`<Part><PartNumber>2</PartNumber><ETag>&#34;def&#34;</ETag></Part></ListPartsResult>`,
		string(NormalizeXMLETags(response)))

	request := []byte(`<CompleteMultipartUpload><Part><ETag>"'abc'"</ETag><PartNumber>1</PartNumber></Part>` +
		`<Part><ETag>&quot;def&quot;</ETag><PartNumber>2</PartNumber></Part></CompleteMultipartUpload>`)
	assert.Equal(t,
		`<CompleteMultipartUpload><Part><ETag>abc</ETag><PartNumber>1</PartNumber></Part>`+
			`<Part><ETag>def</ETag><PartNumber>2</PartNumber></Part></CompleteMultipartUpload>`,
		string(CanonicalXMLETags(request)))
}

// A client-supplied key is refused by name, before anything else about the
// request is looked at — including the copy-source family, and including a
// header sent with no value at all.
func TestClassifyRefusesCustomerKeys(t *testing.T) {
	for _, name := range []string{
		"X-Amz-Server-Side-Encryption-Customer-Algorithm",
		"X-Amz-Server-Side-Encryption-Customer-Key",
		"X-Amz-Server-Side-Encryption-Customer-Key-Md5",
		"X-Amz-Copy-Source-Server-Side-Encryption-Customer-Key",
	} {
		for _, target := range []string{"/bucket/key", "/bucket?list-type=2", "/"} {
			req := httptest.NewRequest(http.MethodGet, target, nil)
			req.Header.Set(name, "")
			_, err := Classify(req)
			require.ErrorIs(t, err, ErrCustomerKey, "%s on %s", name, target)
		}
	}
	assert.NotContains(t, CustomerKeyMessage, "%", "the message is fixed text")
}
