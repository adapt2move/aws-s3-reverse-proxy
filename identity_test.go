package main

import (
	"encoding/hex"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestResolveIdentity(t *testing.T) {
	policy := mustPolicy(t)

	identity, err := policy.ResolveIdentity(tenantA+"rws", testPepper)
	require.NoError(t, err)
	assert.Equal(t, tenantA, identity.Tenant)
	assert.Equal(t, "rws", identity.Level)
	assert.Equal(t, tenantA+"/", identity.KeyPrefix)
	assert.Equal(t, deriveSecret(testPepper, tenantA+":rws"), identity.SecretAccessKey)
}

// The secret is an HMAC, not a digest: tenant ids are not secret, so an
// unkeyed hash would let anyone who learns one compute that tenant's
// highest-privilege secret offline.
func TestDerivedSecretsAreIndependent(t *testing.T) {
	policy := mustPolicy(t)

	secrets := map[string]bool{}
	for _, tenant := range []string{tenantA, tenantB} {
		for _, level := range policy.Levels() {
			identity, err := policy.ResolveIdentity(tenant+level, testPepper)
			require.NoError(t, err)
			assert.Len(t, identity.SecretAccessKey, 2*32, "a derived secret is a hex SHA-256 HMAC")
			_, err = hex.DecodeString(identity.SecretAccessKey)
			require.NoError(t, err)
			assert.False(t, secrets[identity.SecretAccessKey], "secrets must not collide across tenants or levels")
			secrets[identity.SecretAccessKey] = true
		}
	}

	// Rotating the pepper rotates every credential at once.
	rotated, err := policy.ResolveIdentity(tenantA+"ro", []byte("a-different-deployment-pepper"))
	require.NoError(t, err)
	assert.False(t, secrets[rotated.SecretAccessKey])
}

func TestResolveIdentityRejects(t *testing.T) {
	policy := mustPolicy(t)

	cases := []string{
		"",                       // anonymous
		tenantA,                  // no level
		tenantA + "admin",        // level outside the pattern
		"short-ro",               // wrong tenant shape
		tenantA + "ro" + "extra", // trailing junk
		"prefix" + tenantA + "ro",
		tenantA + "RO", // levels are case-sensitive
	}
	for _, id := range cases {
		_, err := policy.ResolveIdentity(id, testPepper)
		assert.Error(t, err, "access key id %q must not resolve", id)
	}
}

// A pattern that forgot its anchors must not silently accept a decorated
// id: the match has to cover the whole access-key id.
func TestResolveIdentityRequiresFullMatch(t *testing.T) {
	policy, err := ParsePolicy([]byte(`
identity:
  accessKeyIdPattern: '(?P<tenant>[0-9a-f]{32})(?P<level>ro|rw)'
  secretTemplate: '{tenant}:{level}'
  keyPrefixTemplate: '{tenant}/'
levels: [ro, rw]
rules:
  - pathPattern: '**'
    grant: { ro: read, rw: full }
`))
	require.NoError(t, err)

	_, err = policy.ResolveIdentity(tenantA+"ro", testPepper)
	require.NoError(t, err)

	for _, id := range []string{"evil-" + tenantA + "ro", tenantA + "ro-evil"} {
		_, err := policy.ResolveIdentity(id, testPepper)
		assert.Error(t, err, id)
	}
}

// A level the access-key-id pattern can produce but `levels` does not
// declare has no rule coverage, so it must not resolve at all.
func TestResolveIdentityRejectsUndeclaredLevel(t *testing.T) {
	policy, err := ParsePolicy([]byte(`
identity:
  accessKeyIdPattern: '^(?P<tenant>[a-z]+)-(?P<level>[a-z]+)$'
  secretTemplate: '{tenant}:{level}'
  keyPrefixTemplate: '{tenant}/'
levels: [ro]
rules:
  - pathPattern: '**'
    grant: { ro: read }
`))
	require.NoError(t, err)

	_, err = policy.ResolveIdentity("acme-ro", testPepper)
	require.NoError(t, err)
	_, err = policy.ResolveIdentity("acme-admin", testPepper)
	assert.Error(t, err)
}

func TestValidateRenderedKeyPrefix(t *testing.T) {
	assert.NoError(t, validateRenderedKeyPrefix("acme/"))
	assert.NoError(t, validateRenderedKeyPrefix("tenants/acme-1_2.3~/"))
	for _, bad := range []string{"", "/acme/", "../acme/", "acme/../", "ac me/", "acme?/", "acme#/", "acme%2F"} {
		assert.Error(t, validateRenderedKeyPrefix(bad), bad)
	}
}

func TestRenderTemplate(t *testing.T) {
	values := map[string]string{"tenant": "acme", "level": "rw"}
	assert.Equal(t, "acme:rw", renderTemplate("{tenant}:{level}", values))
	assert.Equal(t, "tenants/acme/", renderTemplate("tenants/{tenant}/", values))
	assert.Equal(t, "{unknown}", renderTemplate("{unknown}", values))
}
