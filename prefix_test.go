package main

import (
	"net/url"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestIsBucketLevelPath(t *testing.T) {
	assert.True(t, isBucketLevelPath("/my-bucket"))
	assert.True(t, isBucketLevelPath("my-bucket"))
	assert.True(t, isBucketLevelPath("/"))
	assert.True(t, isBucketLevelPath(""))
	assert.False(t, isBucketLevelPath("/my-bucket/key"))
	assert.False(t, isBucketLevelPath("/my-bucket/some/nested/key"))
}

func TestInjectKeyPrefixDisabled(t *testing.T) {
	h := &Handler{KeyPrefix: ""}
	assert.Equal(t, "/my-bucket/key", h.injectKeyPrefix("/my-bucket/key"))
}

func TestInjectKeyPrefixObjectPaths(t *testing.T) {
	h := &Handler{KeyPrefix: "tenants/acme/"}
	assert.Equal(t, "/my-bucket/tenants/acme/key", h.injectKeyPrefix("/my-bucket/key"))
	assert.Equal(t,
		"/my-bucket/tenants/acme/some/nested/key.parquet",
		h.injectKeyPrefix("/my-bucket/some/nested/key.parquet"))
}

func TestInjectKeyPrefixLeavesBucketLevelUntouched(t *testing.T) {
	h := &Handler{KeyPrefix: "tenants/acme/"}
	// Bucket-level paths carry no key to prefix; scopeListPrefix handles them.
	assert.Equal(t, "/my-bucket", h.injectKeyPrefix("/my-bucket"))
	assert.Equal(t, "/", h.injectKeyPrefix("/"))
}

func TestScopeListPrefix(t *testing.T) {
	h := &Handler{KeyPrefix: "tenants/acme/"}

	// No prefix query param yet → it gets set to the bare key prefix.
	u, _ := url.Parse("http://host/my-bucket?list-type=2")
	h.scopeListPrefix(u)
	assert.Equal(t, "tenants/acme/", u.Query().Get("prefix"))
	assert.Equal(t, "2", u.Query().Get("list-type"))

	// Existing prefix → the key prefix is prepended to it.
	u, _ = url.Parse("http://host/my-bucket?list-type=2&prefix=data/")
	h.scopeListPrefix(u)
	assert.Equal(t, "tenants/acme/data/", u.Query().Get("prefix"))
}

func TestScopeListPrefixDisabled(t *testing.T) {
	h := &Handler{KeyPrefix: ""}
	u, _ := url.Parse("http://host/my-bucket?prefix=data/")
	h.scopeListPrefix(u)
	assert.Equal(t, "data/", u.Query().Get("prefix"))
}

func TestCredentialRegexpAcceptsEmptyRegion(t *testing.T) {
	// Some S3 clients (e.g. DuckDB's httpfs with no region configured)
	// leave the region segment empty: ".../<date>//s3/aws4_request".
	cases := map[string][2]string{
		"Credential=KEY/20260514/eu-central-1/s3/aws4_request": {"KEY", "eu-central-1"},
		"Credential=KEY/20260514/gra/s3/aws4_request":          {"KEY", "gra"},
		"Credential=KEY/20260514//s3/aws4_request":             {"KEY", ""},
	}
	for header, want := range cases {
		m := awsAuthorizationCredentialRegexp.FindStringSubmatch(header)
		assert.Len(t, m, 3, header)
		assert.Equal(t, want[0], m[1], header)
		assert.Equal(t, want[1], m[2], header)
	}
}
