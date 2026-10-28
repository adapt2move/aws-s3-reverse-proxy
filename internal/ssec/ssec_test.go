package ssec

import (
	"bytes"
	"crypto/md5"
	"encoding/base64"
	"encoding/hex"
	"fmt"
	"net/http"
	"net/url"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func encodedKey(b byte) string {
	return base64.StdEncoding.EncodeToString(bytes.Repeat([]byte{b}, KeySize))
}

func TestParseAcceptsExactlyOneShape(t *testing.T) {
	key, err := Parse(encodedKey(7))
	require.NoError(t, err)
	require.NotNil(t, key)

	for name, value := range map[string]string{
		"empty":        "",
		"not base64":   "this is not base64!",
		"too short":    base64.StdEncoding.EncodeToString(make([]byte, 16)),
		"too long":     base64.StdEncoding.EncodeToString(make([]byte, 33)),
		"url alphabet": strings.NewReplacer("+", "-", "/", "_").Replace(base64.StdEncoding.EncodeToString(bytes.Repeat([]byte{0xfb}, KeySize))),
		"unpadded":     strings.TrimRight(encodedKey(1), "="),
	} {
		_, err := Parse(value)
		assert.Error(t, err, name)
	}
}

// The error is logged at startup, so it must describe the value without
// repeating it.
func TestParseErrorsNeverContainTheValue(t *testing.T) {
	value := base64.StdEncoding.EncodeToString(bytes.Repeat([]byte{9}, 31))
	_, err := Parse(value)
	require.Error(t, err)
	assert.NotContains(t, err.Error(), value)
}

func TestApplySetsTheThreeHeaders(t *testing.T) {
	raw := bytes.Repeat([]byte{3}, KeySize)
	key, err := Parse(base64.StdEncoding.EncodeToString(raw))
	require.NoError(t, err)

	header := http.Header{}
	key.Apply(header)
	sum := md5.Sum(raw)
	assert.Equal(t, "AES256", header.Get(HeaderAlgorithm))
	assert.Equal(t, base64.StdEncoding.EncodeToString(raw), header.Get(HeaderKey))
	assert.Equal(t, base64.StdEncoding.EncodeToString(sum[:]), header.Get(HeaderKeyMD5))

	Remove(header)
	assert.Empty(t, header)
}

func TestCheckReadsTheConfirmation(t *testing.T) {
	key, _ := Parse(encodedKey(1))
	other, _ := Parse(encodedKey(2))

	confirmed := http.Header{}
	key.Apply(confirmed)
	confirmed.Del(HeaderKey) // a response carries the algorithm and digest only
	assert.Equal(t, Confirmed, key.Check(confirmed))
	assert.Equal(t, Mismatched, other.Check(confirmed), "another key's digest is not a confirmation of ours")

	assert.Equal(t, Unconfirmed, key.Check(http.Header{}))
	assert.Equal(t, Confirmed, key.Check(http.Header{HeaderAlgorithm: {"AES256"}}), "a store that confirms only the algorithm still confirms")
	assert.Equal(t, Mismatched, key.Check(http.Header{HeaderAlgorithm: {"aws:kms"}}))
}

func TestTheKeyDoesNotFormat(t *testing.T) {
	value := encodedKey(5)
	key, _ := Parse(value)
	holder := struct{ Key *Key }{key}
	for _, verb := range []string{"%v", "%+v", "%#v", "%s"} {
		out := fmt.Sprintf(verb, holder)
		assert.NotContains(t, out, value, verb)
		assert.NotContains(t, out, key.encodedMD5, verb)
	}
}

func TestCustomerHeaderFamily(t *testing.T) {
	for _, name := range []string{
		"x-amz-server-side-encryption-customer-algorithm",
		"X-Amz-Server-Side-Encryption-Customer-Key",
		"x-amz-server-side-encryption-customer-key-MD5",
		"x-amz-copy-source-server-side-encryption-customer-key",
	} {
		assert.True(t, IsCustomerHeader(name), name)
	}
	for _, name := range []string{"x-amz-server-side-encryption", "x-amz-copy-source", "Content-Type"} {
		assert.False(t, IsCustomerHeader(name), name)
	}
	assert.True(t, HasCustomerHeader(http.Header{"X-Amz-Server-Side-Encryption-Customer-Key": {""}}), "an empty value still asks for it")
}

// An error document can quote the request it refused, key and all, in more
// than one spelling.
func TestRedactRemovesEverySpelling(t *testing.T) {
	raw := bytes.Repeat([]byte{0xfb}, KeySize) // Base64 with + and /
	key, err := Parse(base64.StdEncoding.EncodeToString(raw))
	require.NoError(t, err)
	encoded := base64.StdEncoding.EncodeToString(raw)
	sum := md5.Sum(raw)
	digest := base64.StdEncoding.EncodeToString(sum[:])

	for _, spelling := range []string{
		encoded,
		digest,
		url.QueryEscape(encoded),
		strings.ToLower(url.QueryEscape(encoded)[:3]) + url.QueryEscape(encoded)[3:],
		hex.EncodeToString([]byte(encoded)),
		strings.ToUpper(hex.EncodeToString([]byte("x-amz-server-side-encryption-customer-key:" + encoded))),
		hex.EncodeToString(raw),
	} {
		doc := []byte("<Error><CanonicalRequest>before " + spelling + " after</CanonicalRequest></Error>")
		out := string(key.Redact(doc))
		assert.Contains(t, out, Redacted, spelling)
		assert.NotContains(t, out, encoded)
		assert.NotContains(t, out, digest)
		assert.NotContains(t, strings.ToLower(out), hex.EncodeToString([]byte(encoded)))
	}
	untouched := []byte("<Error><Code>AccessDenied</Code></Error>")
	assert.Equal(t, untouched, key.Redact(untouched))
}
