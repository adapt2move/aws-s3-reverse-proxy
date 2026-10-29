package policy

import (
	"crypto/subtle"
	"errors"
	"fmt"
)

// GlobalLevel is the level a global-access identity reports in the access log
// and in metrics. The parentheses keep it apart from any level a policy can
// declare in practice, so a dashboard never adds the two up by accident.
const GlobalLevel = "(global)"

// GlobalRule is the rule a global-access decision names, so that the access
// log explains it as plainly as one a policy rule made.
const GlobalRule = "(global access)"

// minGlobalSecretSize is the shortest secret a global credential may have.
// It unlocks the whole bucket, so it is held to at least the pepper's bar.
const minGlobalSecretSize = 16

// GlobalAccess is the operator's general-access credential: one fixed
// access-key id and secret, both from the environment, that address the
// bucket as it is upstream — no tenant prefix, no rule list.
//
// It exists for the workloads the tenant model has no place for: a backup
// job, a migration, an operator's own tooling. Going through the proxy
// rather than straight to the object store keeps them on the same cache, the
// same invalidation and the same SSE-C key as every tenant, so what they
// write is what tenants read and the other way round.
//
// What it does not bypass is everything outside the policy: the operation
// and header whitelists, read-only mode, the source subnets and the
// signature check all hold for it exactly as for a tenant.
type GlobalAccess struct {
	AccessKeyID     string
	SecretAccessKey string

	// Permission is what the credential may do anywhere in the bucket:
	// `read` or `full`. `deny` would be a credential that cannot do
	// anything, which is better expressed by not configuring one.
	Permission Permission
}

// Validate rejects a global credential that is incomplete or would grant
// nothing.
func (g *GlobalAccess) Validate() error {
	if g.AccessKeyID == "" || g.SecretAccessKey == "" {
		return errors.New("global access needs both an access-key id and a secret")
	}
	if len(g.SecretAccessKey) < minGlobalSecretSize {
		return fmt.Errorf("the global access secret must be at least %d bytes", minGlobalSecretSize)
	}
	switch g.Permission {
	case PermissionRead, PermissionFull:
		return nil
	}
	return fmt.Errorf("global access permission is %q, want %q or %q", g.Permission, PermissionRead, PermissionFull)
}

// Resolve returns the global identity when accessKeyID is the global
// credential's, and nil otherwise. The comparison runs in constant time: the
// id is not a secret, but nothing is gained by letting a caller find it out
// one byte at a time either.
func (g *GlobalAccess) Resolve(accessKeyID string) *Identity {
	if g == nil || subtle.ConstantTimeCompare([]byte(accessKeyID), []byte(g.AccessKeyID)) != 1 {
		return nil
	}
	return &Identity{
		Level:           GlobalLevel,
		SecretAccessKey: g.SecretAccessKey,
		global:          g.Permission,
	}
}

// Global reports whether the identity is the operator's general-access
// credential rather than a tenant's.
func (id *Identity) Global() bool { return id.global != "" }

// AuthorizeIdentity is Authorize for whoever the identity is. A tenant is
// held to the rule list for its level; a global identity is held to the one
// permission it was configured with, everywhere, and the rule list — written
// against tenant-facing keys it never uses — has no say.
func (p *Policy) AuthorizeIdentity(id *Identity, subject, method string) Decision {
	if id.Global() {
		return Decision{Allowed: id.global.allows(method), Rule: GlobalRule}
	}
	return p.Authorize(id.Level, subject, method)
}
