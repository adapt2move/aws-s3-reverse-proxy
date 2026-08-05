package main

import (
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// The tenant prefix is injected, never validated against something the
// client sent — so a client cannot express a path outside its own scope.
func TestKeyPrefixInjection(t *testing.T) {
	h, upstream := newTestProxy(t)

	cases := []struct {
		name     string
		target   string
		wantPath string
	}{
		{"plain key", "/bucket/datasets/a.csv", "/bucket/" + tenantA + "/datasets/a.csv"},
		{"nested key", "/bucket/workspaces/w1/out/deep/a.csv", "/bucket/" + tenantA + "/workspaces/w1/out/deep/a.csv"},
		{"directory marker", "/bucket/workspaces/w1/out/", "/bucket/" + tenantA + "/workspaces/w1/out/"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			rec := do(t, h, clientRequest{
				method: http.MethodGet, target: tc.target, tenant: tenantA, level: "ro",
			})
			require.Equal(t, http.StatusOK, rec.Code)
			assert.Equal(t, tc.wantPath, upstream.last(t).path)
		})
	}

	t.Run("two tenants never share a path", func(t *testing.T) {
		for _, tenant := range []string{tenantA, tenantB} {
			rec := do(t, h, clientRequest{
				method: http.MethodGet, target: "/bucket/datasets/shared.csv",
				tenant: tenant, level: "ro",
			})
			require.Equal(t, http.StatusOK, rec.Code)
			assert.Equal(t, "/bucket/"+tenant+"/datasets/shared.csv", upstream.last(t).path)
		}
	})
}

// A key the client wrote percent-encoded has to reach the upstream in the
// same encoding: decoding and re-encoding it would address a different
// object.
func TestKeyPrefixInjectionPreservesEncoding(t *testing.T) {
	h, upstream := newTestProxy(t)

	rec := do(t, h, clientRequest{
		method: http.MethodGet, target: "/bucket/datasets/a%20b%2Bc.csv",
		tenant: tenantA, level: "ro",
	})
	require.Equal(t, http.StatusOK, rec.Code)
	assert.Equal(t, "/bucket/"+tenantA+"/datasets/a b+c.csv", upstream.last(t).path)
}

// A listing is confined by prefixing the parameters that name keys.
func TestListingParameterScoping(t *testing.T) {
	h, upstream := newTestProxy(t)

	t.Run("prefix", func(t *testing.T) {
		rec := do(t, h, clientRequest{
			method: http.MethodGet, target: "/bucket/?list-type=2&prefix=datasets/2026/",
			tenant: tenantA, level: "ro",
		})
		require.Equal(t, http.StatusOK, rec.Code)
		assert.Equal(t, tenantA+"/datasets/2026/", upstream.last(t).query.Get("prefix"))
	})

	t.Run("marker and start-after name real keys and are prefixed", func(t *testing.T) {
		rec := do(t, h, clientRequest{
			method: http.MethodGet,
			target: "/bucket/?list-type=2&prefix=datasets/&start-after=datasets/a.csv&marker=datasets/b.csv",
			tenant: tenantA, level: "ro",
		})
		require.Equal(t, http.StatusOK, rec.Code)
		got := upstream.last(t)
		assert.Equal(t, tenantA+"/datasets/a.csv", got.query.Get("start-after"))
		assert.Equal(t, tenantA+"/datasets/b.csv", got.query.Get("marker"))
	})

	// A continuation token is an opaque value the upstream minted for a
	// listing that was already scoped. Prefixing it would corrupt it — and
	// forwarding it verbatim is safe precisely because the client could not
	// have obtained a token for any other scope.
	t.Run("continuation token is forwarded verbatim", func(t *testing.T) {
		token := "1ueGcxLPRx1Tr/XYExHnhbYLgveDs2J/wm36Hy4vbOwM="
		rec := do(t, h, clientRequest{
			method: http.MethodGet,
			target: "/bucket/?list-type=2&prefix=datasets/&continuation-token=" +
				"1ueGcxLPRx1Tr%2FXYExHnhbYLgveDs2J%2Fwm36Hy4vbOwM%3D",
			tenant: tenantA, level: "ro",
		})
		require.Equal(t, http.StatusOK, rec.Code)
		assert.Equal(t, token, upstream.last(t).query.Get("continuation-token"))
	})

	// An empty prefix matches no rule, so the implicit deny applies: a
	// client cannot enumerate its whole tenant scope unless a rule says so.
	t.Run("a listing outside every rule is denied", func(t *testing.T) {
		before := upstream.count()
		rec := do(t, h, clientRequest{
			method: http.MethodGet, target: "/bucket/?list-type=2",
			tenant: tenantA, level: "ro",
		})
		assert.Equal(t, http.StatusForbidden, rec.Code)
		assert.Equal(t, before, upstream.count())
	})
}

// Both `/bucket` and `/bucket/` address the bucket itself; the trailing
// slash the AWS SDK emits with forcePathStyle is cosmetic.
func TestBucketLevelPathsAreListings(t *testing.T) {
	h, upstream := newTestProxy(t)

	for target, wantPath := range map[string]string{
		"/bucket?list-type=2&prefix=datasets/":  "/bucket",
		"/bucket/?list-type=2&prefix=datasets/": "/bucket/",
	} {
		rec := do(t, h, clientRequest{
			method: http.MethodGet, target: target, tenant: tenantA, level: "ro",
		})
		require.Equal(t, http.StatusOK, rec.Code, target)
		assert.Equal(t, wantPath, upstream.last(t).path, target)
		assert.Equal(t, tenantA+"/datasets/", upstream.last(t).query.Get("prefix"), target)
	}
}

func TestScopeToTenantRejectsUnsafeRenderedPrefix(t *testing.T) {
	policy, err := ParsePolicy([]byte(`
identity:
  accessKeyIdPattern: '^(?P<tenant>[^/]+)-(?P<level>ro|rw)$'
  secretTemplate: '{tenant}:{level}'
  keyPrefixTemplate: '{tenant}/'
levels: [ro, rw]
rules:
  - pathPattern: '**'
    grant: { ro: read, rw: full }
`))
	require.NoError(t, err)

	// A permissive access-key-id pattern must not let a tenant id smuggle
	// URL structure or a traversal into the upstream path.
	for _, tenant := range []string{"a?b", "a#b", "a b", "..", "a/../b"} {
		_, err := policy.ResolveIdentity(tenant+"-ro", testPepper)
		assert.Error(t, err, "tenant %q must not produce a usable key prefix", tenant)
	}
	identity, err := policy.ResolveIdentity("plain-tenant-ro", testPepper)
	require.NoError(t, err)
	assert.Equal(t, "plain-tenant/", identity.KeyPrefix)
}
