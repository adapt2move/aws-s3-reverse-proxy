package policy_test

import (
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/Kriechi/aws-s3-reverse-proxy/internal/policy"
	"github.com/Kriechi/aws-s3-reverse-proxy/internal/policytest"
)

func TestGlobalAccessResolvesOnlyItsOwnID(t *testing.T) {
	g := &policy.GlobalAccess{AccessKeyID: "GLOBALKEY", SecretAccessKey: "global-secret-value", Permission: policy.PermissionRead}

	id := g.Resolve("GLOBALKEY")
	require.NotNil(t, id)
	assert.True(t, id.Global())
	assert.Equal(t, "", id.Tenant)
	assert.Equal(t, "", id.KeyPrefix, "a global identity addresses the bucket unprefixed")
	assert.Equal(t, policy.GlobalLevel, id.Level)
	assert.Equal(t, "global-secret-value", id.SecretAccessKey)

	for _, other := range []string{"", "GLOBALKE", "GLOBALKEYX", "globalkey"} {
		assert.Nil(t, g.Resolve(other), other)
	}
	var none *policy.GlobalAccess
	assert.Nil(t, none.Resolve("GLOBALKEY"), "no credential configured resolves nothing")
}

func TestAuthorizeIdentity(t *testing.T) {
	p := policytest.Policy()

	t.Run("a tenant is held to the rule list", func(t *testing.T) {
		tenant, err := p.ResolveIdentity(policytest.AccessKeyID(policytest.TenantA, "ro"), policytest.Pepper)
		require.NoError(t, err)
		assert.False(t, tenant.Global())
		assert.Equal(t, p.Authorize("ro", "datasets/x.csv", http.MethodGet), p.AuthorizeIdentity(tenant, "datasets/x.csv", http.MethodGet))
		assert.False(t, p.AuthorizeIdentity(tenant, "nowhere/x.csv", http.MethodGet).Allowed)
	})

	t.Run("a global identity is held to its permission alone", func(t *testing.T) {
		read := (&policy.GlobalAccess{AccessKeyID: "R", SecretAccessKey: "global-secret-value", Permission: policy.PermissionRead}).Resolve("R")
		full := (&policy.GlobalAccess{AccessKeyID: "F", SecretAccessKey: "global-secret-value", Permission: policy.PermissionFull}).Resolve("F")
		for _, key := range []string{"", "nowhere/x.csv", policytest.TenantA + "/datasets/2026/private/x.csv"} {
			for _, method := range []string{http.MethodGet, http.MethodHead} {
				assert.Equal(t, policy.Decision{Allowed: true, Rule: policy.GlobalRule}, p.AuthorizeIdentity(read, key, method))
				assert.True(t, p.AuthorizeIdentity(full, key, method).Allowed)
			}
			for _, method := range []string{http.MethodPut, http.MethodPost, http.MethodDelete} {
				assert.Equal(t, policy.Decision{Allowed: false, Rule: policy.GlobalRule}, p.AuthorizeIdentity(read, key, method))
				assert.True(t, p.AuthorizeIdentity(full, key, method).Allowed)
			}
			assert.False(t, p.AuthorizeIdentity(full, key, http.MethodPatch).Allowed)
		}
	})
}
