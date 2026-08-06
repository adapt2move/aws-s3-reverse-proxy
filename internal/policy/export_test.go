package policy

// The test suite for this package lives in policy_test rather than in
// package policy, so that it can share the fixture in internal/policytest
// (which imports this package, and so cannot be imported from inside it).
//
// These are the internals that suite still has to reach: each one is a
// decision the exported surface only shows the consequences of, and a pattern
// compiler or a prefix validator is far better pinned down directly than
// through a request.
var (
	CompilePathPattern        = compilePathPattern
	ValidateRenderedKeyPrefix = validateRenderedKeyPrefix
	RenderTemplate            = renderTemplate
	LoadFile                  = loadFile
)

// Allows exposes the method expansion of a permission.
func (p Permission) Allows(method string) bool { return p.allows(method) }
