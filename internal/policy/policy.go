// Package policy is the authorization model: who a request is, and what
// that identity may do.
//
// It owns the on-disk policy document, its compiled form, the derivation of a
// client's identity from an access-key id, and the hot-reload store that
// swaps one compiled policy for another. It knows nothing about HTTP beyond
// method names, nothing about S3, and nothing about how the proxy is wired —
// which is what lets the whole authorization model be tested without a
// request.
package policy

import (
	"fmt"
	"net/http"
	"regexp"
	"strings"

	"gopkg.in/yaml.v3"
)

// A permission is what a rule grants one level for one path pattern. It
// expands to a set of HTTP methods:
//
//	deny = nothing at all
//	read = GET / HEAD (a LIST is a GET)
//	full = read plus PUT / POST / DELETE, including every multipart step
//
// An explicit `deny` is not the same as leaving a path unmatched. The
// implicit deny at the end of the rule list only applies when *no* rule
// matched, so on its own it cannot express either half of what an operator
// usually wants:
//
//   - a hole inside a tree a rule already covers — the covering rule
//     matches first and grants, and moving the carve-out in front of it
//     needs something to put in its `grant` map
//   - a path one level must not touch while another level may — a rule has
//     to cover every declared level, so there is no way to withhold it from
//     one of them alone
//
// `deny` is first-match-wins like any other grant: it decides the request
// where it matches, and no later rule gets a say.
type Permission string

const (
	// PermissionDeny grants nothing.
	PermissionDeny Permission = "deny"
	// PermissionRead grants the non-mutating methods.
	PermissionRead Permission = "read"
	// PermissionFull grants the non-mutating methods plus every mutation.
	PermissionFull Permission = "full"
)

// allows reports whether a permission covers an HTTP method. Anything not
// named here (PATCH, OPTIONS, TRACE, …) is covered by no permission and is
// therefore denied — the method set is a whitelist, like everything else on
// the request path.
func (p Permission) allows(method string) bool {
	if p == PermissionDeny {
		return false
	}
	switch method {
	case http.MethodGet, http.MethodHead:
		return p == PermissionRead || p == PermissionFull
	case http.MethodPut, http.MethodPost, http.MethodDelete:
		return p == PermissionFull
	}
	return false
}

// File is the on-disk shape of the policy document. It is mounted
// read-only (a ConfigMap, typically), carries no secrets and nothing
// tenant-specific: onboarding a tenant never touches this file.
type File struct {
	Identity IdentityConfig `yaml:"identity"`
	Levels   []string       `yaml:"levels"`
	Rules    []RuleConfig   `yaml:"rules"`
}

// IdentityConfig describes how a request's access-key id is turned into a
// {tenant, level} pair, a derived secret and a key prefix. The named capture
// groups of AccessKeyIDPattern are the only contract between the component
// that issues credentials to clients and this proxy.
type IdentityConfig struct {
	// AccessKeyIDPattern must be a regular expression with (at least) the
	// named capture groups `tenant` and `level`. It has to match the whole
	// access-key id, not a substring.
	AccessKeyIDPattern string `yaml:"accessKeyIdPattern"`

	// SecretTemplate is the message the derived secret is an HMAC over.
	// `{name}` placeholders refer to capture groups of AccessKeyIDPattern.
	SecretTemplate string `yaml:"secretTemplate"`

	// KeyPrefixTemplate is prepended to every object key and to every
	// listing prefix/marker. It must reference `{tenant}` — without it
	// every tenant would share one key space.
	KeyPrefixTemplate string `yaml:"keyPrefixTemplate"`
}

// RuleConfig is one entry of the ordered rule list.
type RuleConfig struct {
	PathPattern string                `yaml:"pathPattern"`
	Grant       map[string]Permission `yaml:"grant"`
}

// Policy is a validated, compiled File — the form the request path
// uses. It is immutable once built, so a hot reload can swap a whole Policy
// in behind an atomic pointer without any locking on the request path.
type Policy struct {
	accessKeyIDRe     *regexp.Regexp
	secretTemplate    string
	keyPrefixTemplate string

	// levels in declaration order, plus the set for O(1) membership.
	levels   []string
	levelSet map[string]struct{}

	rules []compiledRule

	// raw is the exact bytes this Policy was built from; the reloader
	// compares against it to avoid recompiling an unchanged file.
	raw []byte
}

type compiledRule struct {
	pattern string
	re      *regexp.Regexp
	grant   map[string]Permission
}

// Levels returns the configured level names in declaration order.
func (p *Policy) Levels() []string { return p.levels }

// Rules returns the configured path patterns in evaluation order.
func (p *Policy) Rules() []string {
	out := make([]string, 0, len(p.rules))
	for _, r := range p.rules {
		out = append(out, r.pattern)
	}
	return out
}

// Decision is the outcome of an authorization check. Rule is the path
// pattern of the rule that decided it, or "" when no rule matched and the
// implicit deny at the end of the list applied — so a denial is always
// explainable from the access log alone.
type Decision struct {
	Allowed bool
	Rule    string
}

// deniedByDefault is the decision for a subject no rule matched.
var deniedByDefault = Decision{Allowed: false, Rule: ""}

// Authorize resolves `method` on the client-facing key `subject` for one
// level against the ordered rule list. The list is first-match-wins: the
// first rule whose pattern matches decides, even when a later rule would
// have granted more. That is what makes a carve-out expressible *inside* a
// broader prefix — the carve-out is simply listed first, granting `read`
// where the tree is writable, or `deny` where it must not be reachable at
// all.
//
// Anything not matched by a rule is denied (the implicit deny at the end).
func (p *Policy) Authorize(level, subject, method string) Decision {
	for i := range p.rules {
		rule := &p.rules[i]
		if !rule.re.MatchString(subject) {
			continue
		}
		perm, ok := rule.grant[level]
		if !ok {
			// Validation guarantees every rule covers every declared
			// level, so this is an unknown level — deny, and name the
			// rule that we stopped at.
			return Decision{Allowed: false, Rule: rule.pattern}
		}
		return Decision{Allowed: perm.allows(method), Rule: rule.pattern}
	}
	return deniedByDefault
}

// Parse parses and validates a policy document. Every error names the
// offending rule (or field) so an operator can fix it without a debugger —
// invalid configuration must fail startup, and a hot reload must be able to
// say what it rejected.
func Parse(raw []byte) (*Policy, error) {
	var file File
	dec := yaml.NewDecoder(strings.NewReader(string(raw)))
	dec.KnownFields(true) // a typo'd key is a misconfiguration, not a comment
	if err := dec.Decode(&file); err != nil {
		return nil, fmt.Errorf("policy: cannot parse: %v", err)
	}

	p := &Policy{
		secretTemplate:    file.Identity.SecretTemplate,
		keyPrefixTemplate: file.Identity.KeyPrefixTemplate,
		raw:               append([]byte(nil), raw...),
	}

	if err := p.compileIdentity(file.Identity); err != nil {
		return nil, err
	}
	if err := p.compileLevels(file.Levels); err != nil {
		return nil, err
	}
	if err := p.compileRules(file.Rules); err != nil {
		return nil, err
	}
	return p, nil
}

func (p *Policy) compileIdentity(cfg IdentityConfig) error {
	if cfg.AccessKeyIDPattern == "" {
		return fmt.Errorf("policy: identity.accessKeyIdPattern is required")
	}
	re, err := regexp.Compile(cfg.AccessKeyIDPattern)
	if err != nil {
		return fmt.Errorf("policy: identity.accessKeyIdPattern is not a valid regular expression: %v", err)
	}
	groups := namedGroups(re)
	for _, required := range []string{"tenant", "level"} {
		if _, ok := groups[required]; !ok {
			return fmt.Errorf("policy: identity.accessKeyIdPattern must contain a named capture group (?P<%s>…)", required)
		}
	}
	p.accessKeyIDRe = re

	if cfg.SecretTemplate == "" {
		return fmt.Errorf("policy: identity.secretTemplate is required")
	}
	if err := validateTemplate(cfg.SecretTemplate, groups); err != nil {
		return fmt.Errorf("policy: identity.secretTemplate: %v", err)
	}
	// A secret that is not bound to BOTH captures collapses the identity
	// model, because the tenant and level a request acts as come from the
	// access-key id rather than from whatever the secret was derived over:
	//
	//	secretTemplate: '{tenant}'  — every level of one tenant shares a
	//	                              secret, so a read-only holder can sign
	//	                              as read-write
	//	secretTemplate: '{level}'   — every tenant at one level shares a
	//	                              secret, so anyone can sign as anyone
	//	                              and the proxy injects *their* prefix
	//
	// Both are silent: the policy loads, and every signature verifies.
	for _, required := range []string{"tenant", "level"} {
		if !strings.Contains(cfg.SecretTemplate, "{"+required+"}") {
			return fmt.Errorf("policy: identity.secretTemplate must reference {%s}, otherwise one derived secret is valid for more than one identity", required)
		}
	}

	if cfg.KeyPrefixTemplate == "" {
		return fmt.Errorf("policy: identity.keyPrefixTemplate is required")
	}
	if err := validateTemplate(cfg.KeyPrefixTemplate, groups); err != nil {
		return fmt.Errorf("policy: identity.keyPrefixTemplate: %v", err)
	}
	// Without {tenant} in the prefix every tenant would land in the same
	// key space — the one misconfiguration that silently removes the whole
	// isolation boundary, so it is rejected outright.
	if !strings.Contains(cfg.KeyPrefixTemplate, "{tenant}") {
		return fmt.Errorf("policy: identity.keyPrefixTemplate must reference {tenant}, otherwise all tenants share one key space")
	}
	if strings.HasPrefix(cfg.KeyPrefixTemplate, "/") {
		return fmt.Errorf("policy: identity.keyPrefixTemplate must not start with %q", "/")
	}
	// A trailing separator is what makes two tenants' prefixes mutually
	// exclusive. Without it, and with a variable-length tenant capture, one
	// tenant's scope contains another's: prefix "a" plus the key "b/x.csv"
	// addresses the same object as prefix "ab" plus "x.csv" — a cross-tenant
	// read that looks entirely ordinary on the way back out, because the
	// response rewrite strips by bytes and finds its own prefix there.
	if !strings.HasSuffix(cfg.KeyPrefixTemplate, "/") {
		return fmt.Errorf("policy: identity.keyPrefixTemplate must end in %q, otherwise one tenant's key prefix can be a prefix of another's", "/")
	}
	if strings.Contains(cfg.KeyPrefixTemplate, "..") {
		return fmt.Errorf("policy: identity.keyPrefixTemplate must not contain %q", "..")
	}
	return nil
}

func (p *Policy) compileLevels(levels []string) error {
	if len(levels) == 0 {
		return fmt.Errorf("policy: levels must declare at least one level")
	}
	p.levelSet = make(map[string]struct{}, len(levels))
	for _, level := range levels {
		if level == "" {
			return fmt.Errorf("policy: levels must not contain an empty name")
		}
		if _, dup := p.levelSet[level]; dup {
			return fmt.Errorf("policy: levels declares %q twice", level)
		}
		p.levelSet[level] = struct{}{}
	}
	p.levels = append([]string(nil), levels...)
	return nil
}

func (p *Policy) compileRules(rules []RuleConfig) error {
	p.rules = make([]compiledRule, 0, len(rules))
	for i, rule := range rules {
		where := fmt.Sprintf("policy: rule %d (pathPattern %q)", i, rule.PathPattern)
		re, err := compilePathPattern(rule.PathPattern)
		if err != nil {
			return fmt.Errorf("%s: %v", where, err)
		}
		// A rule that does not cover exactly the declared levels is a
		// misconfiguration in either direction: a missing level is a silent
		// hole (the level would fall through to the implicit deny while the
		// author believed the rule covered it), an unknown one is a typo
		// that grants nothing.
		for level := range rule.Grant {
			if _, ok := p.levelSet[level]; !ok {
				return fmt.Errorf("%s: grant names unknown level %q (declared levels: %s)", where, level, strings.Join(p.levels, ", "))
			}
		}
		for _, level := range p.levels {
			perm, ok := rule.Grant[level]
			if !ok {
				return fmt.Errorf("%s: grant is missing level %q", where, level)
			}
			switch perm {
			case PermissionDeny, PermissionRead, PermissionFull:
			default:
				return fmt.Errorf("%s: grant for level %q is %q, want %q, %q or %q", where, level, perm, PermissionDeny, PermissionRead, PermissionFull)
			}
		}
		grant := make(map[string]Permission, len(rule.Grant))
		for level, perm := range rule.Grant {
			grant[level] = perm
		}
		p.rules = append(p.rules, compiledRule{pattern: rule.PathPattern, re: re, grant: grant})
	}
	return nil
}

// namedGroups returns the named capture groups of a compiled expression.
func namedGroups(re *regexp.Regexp) map[string]int {
	out := make(map[string]int)
	for i, name := range re.SubexpNames() {
		if name != "" {
			out[name] = i
		}
	}
	return out
}

// templatePlaceholderRegexp matches the `{name}` placeholders of the secret
// and key-prefix templates.
var templatePlaceholderRegexp = regexp.MustCompile(`\{([^{}]*)\}`)

// validateTemplate rejects a template referencing a capture group the
// access-key-id pattern does not define. Doing this at load time is what
// lets renderTemplate be infallible on the request path.
func validateTemplate(tmpl string, groups map[string]int) error {
	for _, m := range templatePlaceholderRegexp.FindAllStringSubmatch(tmpl, -1) {
		if _, ok := groups[m[1]]; !ok {
			return fmt.Errorf("references {%s}, which is not a named capture group of identity.accessKeyIdPattern", m[1])
		}
	}
	return nil
}

// renderTemplate substitutes the `{name}` placeholders with capture-group
// values. Unknown placeholders cannot occur — validateTemplate rejected
// them at load time — so they are left verbatim rather than erroring here.
func renderTemplate(tmpl string, values map[string]string) string {
	return templatePlaceholderRegexp.ReplaceAllStringFunc(tmpl, func(m string) string {
		name := m[1 : len(m)-1]
		if v, ok := values[name]; ok {
			return v
		}
		return m
	})
}

// compilePathPattern turns a glob path pattern into an anchored regular
// expression over client-facing object keys:
//
//	**  any sequence of characters, including `/` (crosses segments)
//	*   any sequence of characters except `/` (stays inside one segment)
//	?   exactly one character except `/`
//
// Everything else is literal. The expression is anchored at both ends, so
// `datasets/**` matches `datasets/a/b.csv` but not `other/datasets/x`.
//
// `(?s)` makes `.` match newlines too: an S3 key may legitimately contain
// one, and a key that slips past `**` because of it would be a hole in the
// pattern rather than a stricter match.
func compilePathPattern(pattern string) (*regexp.Regexp, error) {
	if pattern == "" {
		return nil, fmt.Errorf("pathPattern must not be empty")
	}
	if strings.HasPrefix(pattern, "/") {
		// Keys are matched in client-facing form, which never has a leading
		// slash — such a pattern could never match anything.
		return nil, fmt.Errorf("pathPattern must not start with %q", "/")
	}
	var b strings.Builder
	b.WriteString("(?s)^")
	for i := 0; i < len(pattern); i++ {
		switch c := pattern[i]; c {
		case '*':
			if i+1 < len(pattern) && pattern[i+1] == '*' {
				b.WriteString(".*")
				i++
			} else {
				b.WriteString("[^/]*")
			}
		case '?':
			b.WriteString("[^/]")
		default:
			b.WriteString(regexp.QuoteMeta(string(c)))
		}
	}
	b.WriteString("$")
	return regexp.Compile(b.String())
}
