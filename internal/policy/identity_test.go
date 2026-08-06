package policy_test

import (
	"encoding/hex"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/Kriechi/aws-s3-reverse-proxy/internal/policy"
	"github.com/Kriechi/aws-s3-reverse-proxy/internal/policytest"
)

func TestResolveIdentity(t *testing.T) {
	compiled := policytest.Policy()

	identity, err := compiled.ResolveIdentity(policytest.TenantA+"rws", policytest.Pepper)
	require.NoError(t, err)
	assert.Equal(t, policytest.TenantA, identity.Tenant)
	assert.Equal(t, "rws", identity.Level)
	assert.Equal(t, policytest.TenantA+"/", identity.KeyPrefix)
	assert.Equal(t, policy.DeriveSecret(policytest.Pepper, policytest.TenantA+":rws"), identity.SecretAccessKey)
}

// The secret is an HMAC, not a digest: tenant ids are not secret, so an
// unkeyed hash would let anyone who learns one compute that tenant's
// highest-privilege secret offline.
func TestDerivedSecretsAreIndependent(t *testing.T) {
	compiled := policytest.Policy()

	secrets := map[string]bool{}
	for _, tenant := range []string{policytest.TenantA, policytest.TenantB} {
		for _, level := range compiled.Levels() {
			identity, err := compiled.ResolveIdentity(tenant+level, policytest.Pepper)
			require.NoError(t, err)
			assert.Len(t, identity.SecretAccessKey, 2*32, "a derived secret is a hex SHA-256 HMAC")
			_, err = hex.DecodeString(identity.SecretAccessKey)
			require.NoError(t, err)
			assert.False(t, secrets[identity.SecretAccessKey], "secrets must not collide across tenants or levels")
			secrets[identity.SecretAccessKey] = true
		}
	}

	// Rotating the pepper rotates every credential at once.
	rotated, err := compiled.ResolveIdentity(policytest.TenantA+"ro", []byte("a-different-deployment-pepper"))
	require.NoError(t, err)
	assert.False(t, secrets[rotated.SecretAccessKey])
}

func TestResolveIdentityRejects(t *testing.T) {
	compiled := policytest.Policy()

	cases := []string{
		"",                                  // anonymous
		policytest.TenantA,                  // no level
		policytest.TenantA + "admin",        // level outside the pattern
		"short-ro",                          // wrong tenant shape
		policytest.TenantA + "ro" + "extra", // trailing junk
		"prefix" + policytest.TenantA + "ro",
		policytest.TenantA + "RO", // levels are case-sensitive
	}
	for _, id := range cases {
		_, err := compiled.ResolveIdentity(id, policytest.Pepper)
		assert.Error(t, err, "access key id %q must not resolve", id)
	}
}

// A pattern that forgot its anchors must not silently accept a decorated
// id: the match has to cover the whole access-key id.
func TestResolveIdentityRequiresFullMatch(t *testing.T) {
	compiled, err := policy.Parse([]byte(`
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

	_, err = compiled.ResolveIdentity(policytest.TenantA+"ro", policytest.Pepper)
	require.NoError(t, err)

	for _, id := range []string{"evil-" + policytest.TenantA + "ro", policytest.TenantA + "ro-evil"} {
		_, err := compiled.ResolveIdentity(id, policytest.Pepper)
		assert.Error(t, err, id)
	}
}

// A level the access-key-id pattern can produce but `levels` does not
// declare has no rule coverage, so it must not resolve at all.
func TestResolveIdentityRejectsUndeclaredLevel(t *testing.T) {
	compiled, err := policy.Parse([]byte(`
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

	_, err = compiled.ResolveIdentity("acme-ro", policytest.Pepper)
	require.NoError(t, err)
	_, err = compiled.ResolveIdentity("acme-admin", policytest.Pepper)
	assert.Error(t, err)
}

func TestValidateRenderedKeyPrefix(t *testing.T) {
	assert.NoError(t, policy.ValidateRenderedKeyPrefix("acme/"))
	assert.NoError(t, policy.ValidateRenderedKeyPrefix("tenants/acme-1_2.3~/"))
	for _, bad := range []string{"", "/acme/", "../acme/", "acme/../", "ac me/", "acme?/", "acme#/", "acme%2F"} {
		assert.Error(t, policy.ValidateRenderedKeyPrefix(bad), bad)
	}
}

func TestRenderTemplate(t *testing.T) {
	values := map[string]string{"tenant": "acme", "level": "rw"}
	assert.Equal(t, "acme:rw", policy.RenderTemplate("{tenant}:{level}", values))
	assert.Equal(t, "tenants/acme/", policy.RenderTemplate("tenants/{tenant}/", values))
	assert.Equal(t, "{unknown}", policy.RenderTemplate("{unknown}", values))
}

// A permissive access-key-id pattern must not let a tenant id smuggle URL
// structure or a traversal into the upstream path. The pattern normally
// constrains the tenant capture to something harmless; this is the guard for
// when it does not.
func TestResolveIdentityRejectsUnsafeRenderedPrefix(t *testing.T) {
	compiled, err := policy.Parse([]byte(`
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

	for _, tenant := range []string{"a?b", "a#b", "a b", "..", "a/../b"} {
		_, err := compiled.ResolveIdentity(tenant+"-ro", policytest.Pepper)
		assert.Error(t, err, "tenant %q must not produce a usable key prefix", tenant)
	}
	identity, err := compiled.ResolveIdentity("plain-tenant-ro", policytest.Pepper)
	require.NoError(t, err)
	assert.Equal(t, "plain-tenant/", identity.KeyPrefix)
}

// The trailing separator the template must end in only keeps key prefixes
// mutually exclusive while the tenant capture contributes no separators of
// its own — otherwise tenant "a" and tenant "a/b" share a nested key space
// again, by a different route.
func TestResolveIdentityRejectsSeparatorInTenant(t *testing.T) {
	compiled, err := policy.Parse([]byte(`
identity:
  accessKeyIdPattern: '^(?P<tenant>[^-]+)-(?P<level>ro|rw)$'
  secretTemplate: '{tenant}:{level}'
  keyPrefixTemplate: '{tenant}/'
levels: [ro, rw]
rules:
  - pathPattern: '**'
    grant: { ro: read, rw: full }
`))
	require.NoError(t, err)

	_, err = compiled.ResolveIdentity("a/b-ro", policytest.Pepper)
	assert.Error(t, err, "a tenant containing a separator must not resolve")

	identity, err := compiled.ResolveIdentity("a-ro", policytest.Pepper)
	require.NoError(t, err)
	assert.Equal(t, "a/", identity.KeyPrefix)
}

// Two tenants whose ids differ in length must never share a key space: the
// shorter one's prefix is not a prefix of the longer one's.
func TestKeyPrefixesAreMutuallyExclusive(t *testing.T) {
	compiled, err := policy.Parse([]byte(`
identity:
  accessKeyIdPattern: '^(?P<tenant>[a-z]+)-(?P<level>ro|rw)$'
  secretTemplate: '{tenant}:{level}'
  keyPrefixTemplate: '{tenant}/'
levels: [ro, rw]
rules:
  - pathPattern: '**'
    grant: { ro: read, rw: full }
`))
	require.NoError(t, err)

	short, err := compiled.ResolveIdentity("a-rw", policytest.Pepper)
	require.NoError(t, err)
	long, err := compiled.ResolveIdentity("ab-rw", policytest.Pepper)
	require.NoError(t, err)

	// The escape this closes: prefix "a" + key "b/secret.csv" would have
	// addressed the same object as prefix "ab" + key "secret.csv".
	assert.NotEqual(t, short.KeyPrefix+"b/secret.csv", long.KeyPrefix+"secret.csv")
	assert.False(t, strings.HasPrefix(long.KeyPrefix, short.KeyPrefix+"b"),
		"one tenant's prefix must not extend into another's")
}
