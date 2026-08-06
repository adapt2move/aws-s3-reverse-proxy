// Package policytest holds the policy fixture the unit suites share.
//
// It exists so that the authorization tests and the request-path tests are
// written against one document rather than two copies that can drift: a
// change to the fixture has to keep both green, which is the point of having
// a nested read-only carve-out in it at all.
//
// It is only ever imported from _test files.
package policytest

import (
	"fmt"

	"github.com/Kriechi/aws-s3-reverse-proxy/internal/policy"
)

// YAML is the policy from the design, verbatim: a writable dataset area
// with a denied carve-out inside it, a read-only carve-out nested *inside*
// a writable workspace tree, and the broader workspace rule after it. The
// ordering is the point — see TestPolicyNestedCarveOutDependsOnOrder.
const YAML = `
identity:
  accessKeyIdPattern: '^(?P<tenant>[0-9a-f]{32})(?P<level>ro|rw|rws)$'
  secretTemplate: '{tenant}:{level}'
  keyPrefixTemplate: '{tenant}/'

levels: [ro, rw, rws]

rules:
  - pathPattern: 'datasets/*/private/**'
    grant: { ro: deny, rw: deny, rws: full }

  - pathPattern: 'datasets/**'
    grant: { ro: read, rw: full, rws: full }

  - pathPattern: 'workspaces/*/inbox/**'
    grant: { ro: read, rw: read, rws: read }

  - pathPattern: 'workspaces/**'
    grant: { ro: full, rw: full, rws: full }
`

// Two tenants matching the fixture's accessKeyIdPattern. Every isolation
// assertion is that one cannot reach the other.
const (
	TenantA = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
	TenantB = "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"
)

// Pepper keys the fixture's derived secrets.
var Pepper = []byte("a-deployment-wide-pepper-value")

// Policy compiles the fixture. It panics rather than taking a *testing.T so
// that this package does not have to import testing.
func Policy() *policy.Policy {
	compiled, err := policy.Parse([]byte(YAML))
	if err != nil {
		panic(fmt.Sprintf("policytest: the shared fixture does not compile: %v", err))
	}
	return compiled
}

// AccessKeyID spells an id the way the fixture's pattern expects.
func AccessKeyID(tenant, level string) string { return tenant + level }

// Secret derives what a client with that id has to sign with.
func Secret(tenant, level string) string {
	return policy.DeriveSecret(Pepper, tenant+":"+level)
}
