package policy_test

import (
	"net/http"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/Kriechi/aws-s3-reverse-proxy/internal/policy"
	"github.com/Kriechi/aws-s3-reverse-proxy/internal/policytest"
)

func writePolicyFile(t *testing.T, body string) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "policy.yaml")
	require.NoError(t, os.WriteFile(path, []byte(body), 0o600))
	return path
}

// A bad edit must not open or close the gate by accident: an invalid policy
// is rejected and the previously loaded one keeps serving.
func TestPolicyHotReload(t *testing.T) {
	path := writePolicyFile(t, policytest.YAML)
	store, err := policy.NewStore(path)
	require.NoError(t, err)
	assert.True(t, store.Current().Authorize("rw", "datasets/a.csv", http.MethodPut).Allowed)

	t.Run("an invalid edit is rejected and changes nothing", func(t *testing.T) {
		require.NoError(t, os.WriteFile(path, []byte("levels: [ro]\nrules: [\n"), 0o600))
		changed, err := store.Reload()
		require.Error(t, err)
		assert.False(t, changed)
		assert.True(t, store.Current().Authorize("rw", "datasets/a.csv", http.MethodPut).Allowed,
			"the previously loaded policy must keep serving")
	})

	t.Run("a valid edit is applied", func(t *testing.T) {
		require.NoError(t, os.WriteFile(path, []byte(`
identity:
  accessKeyIdPattern: '^(?P<tenant>[0-9a-f]{32})(?P<level>ro|rw|rws)$'
  secretTemplate: '{tenant}:{level}'
  keyPrefixTemplate: '{tenant}/'
levels: [ro, rw, rws]
rules:
  - pathPattern: 'datasets/**'
    grant: { ro: read, rw: read, rws: full }
`), 0o600))
		changed, err := store.Reload()
		require.NoError(t, err)
		assert.True(t, changed)
		assert.False(t, store.Current().Authorize("rw", "datasets/a.csv", http.MethodPut).Allowed)
		assert.True(t, store.Current().Authorize("rws", "datasets/a.csv", http.MethodPut).Allowed)
	})

	t.Run("an unchanged file is not reapplied", func(t *testing.T) {
		changed, err := store.Reload()
		require.NoError(t, err)
		assert.False(t, changed)
	})
}

// The outcome hook is how the store reports without knowing what reports:
// every attempt has to arrive there exactly once, correctly classified, or an
// operator's only signal that the file on disk is not the policy in force
// goes missing.
func TestPolicyReloadOutcomesAreReported(t *testing.T) {
	path := writePolicyFile(t, policytest.YAML)
	store, err := policy.NewStore(path)
	require.NoError(t, err)

	var seen []policy.ReloadOutcome
	var lastErr error
	store.OnReload = func(outcome policy.ReloadOutcome, err error) {
		seen = append(seen, outcome)
		lastErr = err
	}

	store.ReloadNow()
	assert.Equal(t, []policy.ReloadOutcome{policy.ReloadUnchanged}, seen)

	require.NoError(t, os.WriteFile(path, []byte("levels: [ro]\nrules: [\n"), 0o600))
	store.ReloadNow()
	assert.Equal(t, policy.ReloadRejected, seen[len(seen)-1])
	assert.Error(t, lastErr, "a rejected reload has to say why")

	require.NoError(t, os.WriteFile(path, []byte(`
identity:
  accessKeyIdPattern: '^(?P<tenant>[0-9a-f]{32})(?P<level>ro)$'
  secretTemplate: '{tenant}:{level}'
  keyPrefixTemplate: '{tenant}/'
levels: [ro]
rules:
  - pathPattern: 'datasets/**'
    grant: { ro: read }
`), 0o600))
	store.ReloadNow()
	assert.Equal(t, policy.ReloadApplied, seen[len(seen)-1])
	assert.NoError(t, lastErr)
	assert.Equal(t, []string{"ro"}, store.Current().Levels())
}

// A store built around a fixed policy has no file to poll, and must not
// pretend otherwise.
func TestStaticStoreNeverReloads(t *testing.T) {
	store := policy.NewStatic(policytest.Policy())
	assert.Empty(t, store.Path())

	changed, err := store.Reload()
	require.NoError(t, err)
	assert.False(t, changed)
	assert.Equal(t, []string{"ro", "rw", "rws"}, store.Current().Levels())
}
