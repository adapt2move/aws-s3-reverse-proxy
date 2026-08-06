package policy_test

import (
	"fmt"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/Kriechi/aws-s3-reverse-proxy/internal/policy"
	"github.com/Kriechi/aws-s3-reverse-proxy/internal/policytest"
)

// The full matrix: every level against every rule pattern, for every
// method. `perm` maps a level to the permission the policy grants it for
// that key; a level missing from the map is expected to fall through to the
// implicit deny.
type policyExpectation struct {
	key  string
	rule string
	perm map[string]policy.Permission
}

func TestPolicyMatrix(t *testing.T) {
	compiled := policytest.Policy()
	all := func(p policy.Permission) map[string]policy.Permission {
		return map[string]policy.Permission{"ro": p, "rw": p, "rws": p}
	}

	expectations := []policyExpectation{
		{
			// The denied carve-out sits inside the dataset tree and is listed
			// first, so the levels it denies never reach the rule that would
			// have granted them — while `rws` keeps full access to the very
			// same path.
			key: "datasets/2026/private/salaries.csv", rule: "datasets/*/private/**",
			perm: map[string]policy.Permission{"ro": policy.PermissionDeny, "rw": policy.PermissionDeny, "rws": policy.PermissionFull},
		},
		{
			key: "datasets/2026/a.csv", rule: "datasets/**",
			perm: map[string]policy.Permission{"ro": policy.PermissionRead, "rw": policy.PermissionFull, "rws": policy.PermissionFull},
		},
		{
			// `**` matches the empty remainder, so the bare prefix a
			// listing asks for is covered by the same rule as its keys.
			key: "datasets/", rule: "datasets/**",
			perm: map[string]policy.Permission{"ro": policy.PermissionRead, "rw": policy.PermissionFull, "rws": policy.PermissionFull},
		},
		{
			// The carve-out sits inside the writable tree and is listed
			// first, so it wins for every level.
			key: "workspaces/w1/inbox/note.txt", rule: "workspaces/*/inbox/**",
			perm: all(policy.PermissionRead),
		},
		{
			key: "workspaces/w1/inbox/deep/nested/note.txt", rule: "workspaces/*/inbox/**",
			perm: all(policy.PermissionRead),
		},
		{
			key: "workspaces/w1/out/report.csv", rule: "workspaces/**",
			perm: all(policy.PermissionFull),
		},
		{
			// `*` does not cross a separator, so a two-segment workspace
			// name never reaches the carve-out rule.
			key: "workspaces/w1/w2/inbox/note.txt", rule: "workspaces/**",
			perm: all(policy.PermissionFull),
		},
		{key: "nowhere/a.csv", rule: "", perm: nil},
		{key: "datasets", rule: "", perm: nil},
		{key: "", rule: "", perm: nil},
	}

	methods := []string{http.MethodGet, http.MethodHead, http.MethodPut, http.MethodPost, http.MethodDelete}

	for _, exp := range expectations {
		for _, level := range compiled.Levels() {
			for _, method := range methods {
				name := fmt.Sprintf("%s/%s/%s", exp.key, level, method)
				t.Run(name, func(t *testing.T) {
					decision := compiled.Authorize(level, exp.key, method)
					perm, granted := exp.perm[level]
					assert.Equal(t, granted && perm.Allows(method), decision.Allowed)
					assert.Equal(t, exp.rule, decision.Rule, "the decision must name the rule that made it")
				})
			}
		}
	}
}

// A method no permission names is denied — the method set is a whitelist
// too.
func TestPolicyDeniesUnknownMethods(t *testing.T) {
	compiled := policytest.Policy()
	for _, method := range []string{http.MethodPatch, http.MethodOptions, "TRACE", "PROPFIND"} {
		decision := compiled.Authorize("rws", "workspaces/w1/out/a.csv", method)
		assert.False(t, decision.Allowed, method)
	}
}

// Ordering is the whole reason the rule list is a list. The same three
// rules in the other order make the carve-out unreachable — which is
// exactly what the prefix-set model this replaces could not express.
func TestPolicyNestedCarveOutDependsOnOrder(t *testing.T) {
	carveOutFirst := policytest.Policy()
	decision := carveOutFirst.Authorize("rws", "workspaces/w1/inbox/note.txt", http.MethodPut)
	assert.False(t, decision.Allowed)
	assert.Equal(t, "workspaces/*/inbox/**", decision.Rule)

	broadFirst, err := policy.Parse([]byte(`
identity:
  accessKeyIdPattern: '^(?P<tenant>[0-9a-f]{32})(?P<level>ro|rw|rws)$'
  secretTemplate: '{tenant}:{level}'
  keyPrefixTemplate: '{tenant}/'
levels: [rws]
rules:
  - pathPattern: 'workspaces/**'
    grant: { rws: full }
  - pathPattern: 'workspaces/*/inbox/**'
    grant: { rws: read }
`))
	require.NoError(t, err)
	decision = broadFirst.Authorize("rws", "workspaces/w1/inbox/note.txt", http.MethodPut)
	assert.True(t, decision.Allowed, "the broader rule matched first, so the carve-out never applies")
	assert.Equal(t, "workspaces/**", decision.Rule)
}

// An explicit `deny` does what the implicit deny at the end of the list
// cannot: refuse a path *inside* a tree a later rule covers, and refuse it
// for some levels while another keeps it.
func TestPolicyExplicitDeny(t *testing.T) {
	compiled := policytest.Policy()
	const key = "datasets/2026/private/salaries.csv"

	for _, level := range []string{"ro", "rw"} {
		for _, method := range []string{http.MethodGet, http.MethodHead, http.MethodPut, http.MethodPost, http.MethodDelete} {
			decision := compiled.Authorize(level, key, method)
			assert.False(t, decision.Allowed, "%s %s", level, method)
			// The denying rule is what decided it, rather than the request
			// falling through to the implicit deny — which is what keeps the
			// refusal explainable from the access log.
			assert.Equal(t, "datasets/*/private/**", decision.Rule, "%s %s", level, method)
		}
	}

	// The rule after it would have granted `rw` full access to that key, and
	// still does everywhere the carve-out does not match.
	assert.True(t, compiled.Authorize("rw", "datasets/2026/a.csv", http.MethodPut).Allowed)

	// The same path stays fully available to the level the carve-out grants
	// it to: a denial is per level, not per path.
	assert.True(t, compiled.Authorize("rws", key, http.MethodPut).Allowed)
	assert.True(t, compiled.Authorize("rws", key, http.MethodGet).Allowed)
}

// Denying every declared level is a legitimate rule, and the only way to
// put a subtree out of reach that does not depend on no other rule
// covering it.
func TestPolicyDenyForEveryLevel(t *testing.T) {
	compiled, err := policy.Parse([]byte(`
identity:
  accessKeyIdPattern: '^(?P<tenant>[0-9a-f]{32})(?P<level>ro|rws)$'
  secretTemplate: '{tenant}:{level}'
  keyPrefixTemplate: '{tenant}/'
levels: [ro, rws]
rules:
  - pathPattern: 'tests/**'
    grant: { ro: deny, rws: deny }
  - pathPattern: '**'
    grant: { ro: read, rws: full }
`))
	require.NoError(t, err)

	for _, level := range []string{"ro", "rws"} {
		decision := compiled.Authorize(level, "tests/fixture.csv", http.MethodGet)
		assert.False(t, decision.Allowed, level)
		assert.Equal(t, "tests/**", decision.Rule, level)
	}
	assert.True(t, compiled.Authorize("ro", "data/a.csv", http.MethodGet).Allowed)
}

func TestCompilePathPattern(t *testing.T) {
	cases := []struct {
		pattern string
		key     string
		want    bool
	}{
		{"datasets/**", "datasets/a", true},
		{"datasets/**", "datasets/a/b/c", true},
		{"datasets/**", "datasets/", true},
		{"datasets/**", "datasets", false},
		{"datasets/**", "other/datasets/a", false},
		{"datasets/*", "datasets/a", true},
		{"datasets/*", "datasets/a/b", false},
		{"workspaces/*/inbox/**", "workspaces/w1/inbox/x", true},
		{"workspaces/*/inbox/**", "workspaces/w1/w2/inbox/x", false},
		{"a?c/**", "abc/x", true},
		{"a?c/**", "a/c/x", false},
		// A key may contain a newline; `**` must not stop at it.
		{"datasets/**", "datasets/a\nb", true},
		// Metacharacters in a pattern are literal.
		{"data.sets/**", "dataXsets/a", false},
		{"data.sets/**", "data.sets/a", true},
	}
	for _, tc := range cases {
		re, err := policy.CompilePathPattern(tc.pattern)
		require.NoError(t, err, tc.pattern)
		assert.Equal(t, tc.want, re.MatchString(tc.key), "%s vs %q", tc.pattern, tc.key)
	}

	for _, bad := range []string{"", "/absolute/**"} {
		_, err := policy.CompilePathPattern(bad)
		assert.Error(t, err, bad)
	}
}

// Invalid configuration must fail loudly, naming what is wrong and where.
func TestPolicyValidation(t *testing.T) {
	const head = `
identity:
  accessKeyIdPattern: '^(?P<tenant>[0-9a-f]{32})(?P<level>ro|rw)$'
  secretTemplate: '{tenant}:{level}'
  keyPrefixTemplate: '{tenant}/'
levels: [ro, rw]
`
	cases := []struct {
		name    string
		yaml    string
		wantErr string
	}{
		{
			name:    "grant omits a declared level",
			yaml:    head + "rules:\n  - pathPattern: 'a/**'\n    grant: { ro: read }\n",
			wantErr: `rule 0 (pathPattern "a/**"): grant is missing level "rw"`,
		},
		{
			name:    "grant names an unknown level",
			yaml:    head + "rules:\n  - pathPattern: 'a/**'\n    grant: { ro: read, rw: full, admin: full }\n",
			wantErr: `grant names unknown level "admin"`,
		},
		{
			name:    "unknown permission",
			yaml:    head + "rules:\n  - pathPattern: 'a/**'\n    grant: { ro: write, rw: full }\n",
			wantErr: `grant for level "ro" is "write", want "deny", "read" or "full"`,
		},
		{
			name:    "absolute path pattern",
			yaml:    head + "rules:\n  - pathPattern: '/a/**'\n    grant: { ro: read, rw: full }\n",
			wantErr: `pathPattern must not start with "/"`,
		},
		{
			name: "access key pattern without a level group",
			yaml: `
identity:
  accessKeyIdPattern: '^(?P<tenant>[0-9a-f]{32})$'
  secretTemplate: '{tenant}'
  keyPrefixTemplate: '{tenant}/'
levels: [ro]
rules: []
`,
			wantErr: "must contain a named capture group (?P<level>…)",
		},
		{
			name: "key prefix template without the tenant",
			yaml: `
identity:
  accessKeyIdPattern: '^(?P<tenant>[0-9a-f]{32})(?P<level>ro)$'
  secretTemplate: '{tenant}:{level}'
  keyPrefixTemplate: 'shared/'
levels: [ro]
rules: []
`,
			wantErr: "must reference {tenant}",
		},
		{
			name: "secret template referencing an unknown group",
			yaml: `
identity:
  accessKeyIdPattern: '^(?P<tenant>[0-9a-f]{32})(?P<level>ro)$'
  secretTemplate: '{tenant}:{level}:{realm}'
  keyPrefixTemplate: '{tenant}/'
levels: [ro]
rules: []
`,
			wantErr: "{realm}",
		},
		{
			name: "no levels",
			yaml: `
identity:
  accessKeyIdPattern: '^(?P<tenant>[0-9a-f]{32})(?P<level>ro)$'
  secretTemplate: '{tenant}:{level}'
  keyPrefixTemplate: '{tenant}/'
levels: []
rules: []
`,
			wantErr: "levels must declare at least one level",
		},
		{
			// A secret bound to the tenant alone is one secret for every
			// level of that tenant: a read-only holder can sign as
			// read-write.
			name: "secret template without the level",
			yaml: `
identity:
  accessKeyIdPattern: '^(?P<tenant>[0-9a-f]{32})(?P<level>ro|rw)$'
  secretTemplate: '{tenant}'
  keyPrefixTemplate: '{tenant}/'
levels: [ro, rw]
rules: []
`,
			wantErr: "identity.secretTemplate must reference {level}",
		},
		{
			// And one bound to the level alone is one secret for every
			// tenant: anyone can sign as anyone, and the proxy then injects
			// the impersonated tenant's key prefix.
			name: "secret template without the tenant",
			yaml: `
identity:
  accessKeyIdPattern: '^(?P<tenant>[0-9a-f]{32})(?P<level>ro|rw)$'
  secretTemplate: '{level}'
  keyPrefixTemplate: '{tenant}/'
levels: [ro, rw]
rules: []
`,
			wantErr: "identity.secretTemplate must reference {tenant}",
		},
		{
			// Without a trailing separator, prefix "a" and prefix "ab" are
			// not mutually exclusive: tenant a's key "b/x" is tenant ab's
			// key "x".
			name: "key prefix template without a trailing separator",
			yaml: `
identity:
  accessKeyIdPattern: '^(?P<tenant>[a-z]+)-(?P<level>ro|rw)$'
  secretTemplate: '{tenant}:{level}'
  keyPrefixTemplate: '{tenant}'
levels: [ro, rw]
rules: []
`,
			wantErr: `identity.keyPrefixTemplate must end in "/"`,
		},
		{
			name:    "typo'd key",
			yaml:    head + "rulez:\n  - pathPattern: 'a/**'\n",
			wantErr: "cannot parse",
		},
		{
			name: "unparseable access key pattern",
			yaml: `
identity:
  accessKeyIdPattern: '^(?P<tenant>['
  secretTemplate: '{tenant}'
  keyPrefixTemplate: '{tenant}/'
levels: [ro]
rules: []
`,
			wantErr: "not a valid regular expression",
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			_, err := policy.Parse([]byte(tc.yaml))
			require.Error(t, err)
			assert.Contains(t, err.Error(), tc.wantErr)
		})
	}
}

func TestPolicyExampleFileIsValid(t *testing.T) {
	compiled, err := policy.LoadFile("../../policy.example.yaml")
	require.NoError(t, err)
	assert.NotEmpty(t, compiled.Levels())
	assert.NotEmpty(t, compiled.Rules())
}
