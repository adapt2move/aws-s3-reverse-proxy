package main

import (
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"sync/atomic"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func writePolicyFile(t *testing.T, body string) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "policy.yaml")
	require.NoError(t, os.WriteFile(path, []byte(body), 0o600))
	return path
}

func baseOptions(t *testing.T) Options {
	return Options{
		AllowedSourceEndpoint:   "foobar.endpoint.example.com",
		AllowedSourceSubnet:     []string{"127.0.0.1/32", "192.168.1.0/24"},
		Region:                  "eu-test-1",
		PolicyFile:              writePolicyFile(t, testPolicyYAML),
		Pepper:                  testPepper,
		UpstreamAccessKeyID:     "UPSTREAMKEYID",
		UpstreamSecretAccessKey: "upstream-secret",
	}
}

func TestParseOptions(t *testing.T) {
	h, err := NewAwsS3ReverseProxy(baseOptions(t))
	require.NoError(t, err)

	assert.Equal(t, "https", h.UpstreamScheme)
	assert.Equal(t, "", h.UpstreamEndpoint)
	assert.Equal(t, "foobar.endpoint.example.com", h.AllowedSourceEndpoint)
	assert.Len(t, h.AllowedSourceSubnet, 2)
	assert.Equal(t, "127.0.0.1/32", h.AllowedSourceSubnet[0].String())
	assert.Equal(t, "192.168.1.0/24", h.AllowedSourceSubnet[1].String())
	assert.Equal(t, []string{"ro", "rw", "rws"}, h.Policy.Current().Levels())
	// With no explicit upstream region, requests are signed for the
	// configured default rather than whatever the client happened to sign.
	assert.Equal(t, "eu-test-1", h.UpstreamRegion)
}

func TestParseOptionsBrokenSubnets(t *testing.T) {
	for _, subnet := range []string{"foobar", "", "127.0.0.1/XXX", "127.0.0.1", "256.0.0.1"} {
		opts := baseOptions(t)
		opts.AllowedSourceSubnet = []string{subnet}
		_, err := NewAwsS3ReverseProxy(opts)
		require.Error(t, err, subnet)
		assert.Contains(t, err.Error(), "Invalid allowed source subnet")
	}
}

// An unparseable or contradictory policy must never start serving.
func TestStartupFailsOnInvalidPolicy(t *testing.T) {
	opts := baseOptions(t)
	opts.PolicyFile = writePolicyFile(t, `
identity:
  accessKeyIdPattern: '^(?P<tenant>[a-z]+)(?P<level>ro|rw)$'
  secretTemplate: '{tenant}:{level}'
  keyPrefixTemplate: '{tenant}/'
levels: [ro, rw]
rules:
  - pathPattern: 'datasets/**'
    grant: { ro: read }
`)
	_, err := NewAwsS3ReverseProxy(opts)
	require.Error(t, err)
	assert.Contains(t, err.Error(), `grant is missing level "rw"`)

	opts = baseOptions(t)
	opts.PolicyFile = filepath.Join(t.TempDir(), "does-not-exist.yaml")
	_, err = NewAwsS3ReverseProxy(opts)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "cannot read policy file")
}

func TestStartupRequiresSecrets(t *testing.T) {
	opts := baseOptions(t)
	opts.Pepper = nil
	_, err := NewAwsS3ReverseProxy(opts)
	assert.EqualError(t, err, "no credential pepper configured")

	opts = baseOptions(t)
	opts.UpstreamSecretAccessKey = ""
	_, err = NewAwsS3ReverseProxy(opts)
	assert.EqualError(t, err, "no upstream credentials configured")
}

// One image has to serve an in-cluster backend over http and a hosted
// provider over https, with the scheme following from the endpoint.
func TestParseUpstreamEndpoint(t *testing.T) {
	cases := []struct {
		endpoint   string
		insecure   bool
		wantScheme string
		wantHost   string
	}{
		{"http://minio.storage.svc:9000", false, "http", "minio.storage.svc:9000"},
		{"https://s3.eu-central-1.amazonaws.com", false, "https", "s3.eu-central-1.amazonaws.com"},
		{"https://s3.example.com/", false, "https", "s3.example.com"},
		{"s3.example.com", false, "https", "s3.example.com"},
		{"minio:9000", true, "http", "minio:9000"},
		{"", false, "https", ""},
	}
	for _, tc := range cases {
		scheme, host, err := parseUpstreamEndpoint(tc.endpoint, tc.insecure)
		require.NoError(t, err, tc.endpoint)
		assert.Equal(t, tc.wantScheme, scheme, tc.endpoint)
		assert.Equal(t, tc.wantHost, host, tc.endpoint)
	}

	_, _, err := parseUpstreamEndpoint("ftp://s3.example.com", false)
	assert.Error(t, err)
}

// Secrets come from the environment or a mounted file — never from a flag
// (they would show up in `ps`) and never from the policy file (it is a
// ConfigMap).
func TestLoadSecrets(t *testing.T) {
	t.Run("combined credentials and a pepper", func(t *testing.T) {
		t.Setenv(envUpstreamCredentials, "AKID,secret-value")
		t.Setenv(envCredentialPepper, "a-sufficiently-long-pepper")

		var opts Options
		require.NoError(t, loadSecrets(&opts))
		assert.Equal(t, "AKID", opts.UpstreamAccessKeyID)
		assert.Equal(t, "secret-value", opts.UpstreamSecretAccessKey)
		assert.Equal(t, "a-sufficiently-long-pepper", string(opts.Pepper))
	})

	t.Run("separate credential variables", func(t *testing.T) {
		t.Setenv(envUpstreamAccessKey, "AKID")
		t.Setenv(envUpstreamSecretKey, "secret-value")
		t.Setenv(envCredentialPepper, "a-sufficiently-long-pepper")

		var opts Options
		require.NoError(t, loadSecrets(&opts))
		assert.Equal(t, "AKID", opts.UpstreamAccessKeyID)
	})

	t.Run("mounted secret files", func(t *testing.T) {
		dir := t.TempDir()
		pepperFile := filepath.Join(dir, "pepper")
		// A trailing newline is what `echo` leaves behind, and it would
		// silently change every derived secret.
		require.NoError(t, os.WriteFile(pepperFile, []byte("a-sufficiently-long-pepper\n"), 0o600))
		t.Setenv(envCredentialPepper+"_FILE", pepperFile)
		t.Setenv(envUpstreamCredentials, "AKID,secret-value")

		var opts Options
		require.NoError(t, loadSecrets(&opts))
		assert.Equal(t, "a-sufficiently-long-pepper", string(opts.Pepper))
	})

	t.Run("missing credentials", func(t *testing.T) {
		t.Setenv(envCredentialPepper, "a-sufficiently-long-pepper")
		var opts Options
		err := loadSecrets(&opts)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "upstream credentials are required")
	})

	t.Run("missing pepper", func(t *testing.T) {
		t.Setenv(envUpstreamCredentials, "AKID,secret-value")
		var opts Options
		err := loadSecrets(&opts)
		require.Error(t, err)
		assert.Contains(t, err.Error(), envCredentialPepper)
	})

	t.Run("pepper too short", func(t *testing.T) {
		t.Setenv(envUpstreamCredentials, "AKID,secret-value")
		t.Setenv(envCredentialPepper, "short")
		var opts Options
		err := loadSecrets(&opts)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "at least")
	})

	t.Run("malformed combined credentials", func(t *testing.T) {
		t.Setenv(envUpstreamCredentials, "no-comma-here")
		t.Setenv(envCredentialPepper, "a-sufficiently-long-pepper")
		var opts Options
		err := loadSecrets(&opts)
		require.Error(t, err)
		assert.Contains(t, err.Error(), envUpstreamCredentials)
	})
}

// A bad edit must not open or close the gate by accident: an invalid
// policy is rejected and the previously loaded one keeps serving.
func TestPolicyHotReload(t *testing.T) {
	path := writePolicyFile(t, testPolicyYAML)
	store, err := NewPolicyStore(path)
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

// A rolling restart needs /readyz to start failing before the process
// stops accepting work, so the load balancer takes this replica out first.
func TestHealthEndpoints(t *testing.T) {
	var ready atomic.Bool
	ready.Store(true)
	mux := http.NewServeMux()
	registerHealthEndpoints(mux, &ready)

	get := func(path string) int {
		rec := httptest.NewRecorder()
		mux.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, path, nil))
		return rec.Code
	}

	assert.Equal(t, http.StatusOK, get("/healthz"))
	assert.Equal(t, http.StatusOK, get("/readyz"))

	ready.Store(false)
	assert.Equal(t, http.StatusOK, get("/healthz"), "the process is still alive and draining")
	assert.Equal(t, http.StatusServiceUnavailable, get("/readyz"))
}

// The policy in force is read once per request, so a reload can never
// decide half of one.
func TestHandlerUsesReloadedPolicy(t *testing.T) {
	h, upstream := newTestProxy(t)

	rec := do(t, h, clientRequest{
		method: http.MethodPut, target: "/bucket/datasets/a.csv",
		body: []byte("x"), tenant: tenantA, level: "rw",
	})
	require.Equal(t, http.StatusOK, rec.Code)

	tightened, err := ParsePolicy([]byte(`
identity:
  accessKeyIdPattern: '^(?P<tenant>[0-9a-f]{32})(?P<level>ro|rw|rws)$'
  secretTemplate: '{tenant}:{level}'
  keyPrefixTemplate: '{tenant}/'
levels: [ro, rw, rws]
rules:
  - pathPattern: 'datasets/**'
    grant: { ro: read, rw: read, rws: full }
`))
	require.NoError(t, err)
	h.Policy = NewStaticPolicyStore(tightened)

	before := upstream.count()
	rec = do(t, h, clientRequest{
		method: http.MethodPut, target: "/bucket/datasets/a.csv",
		body: []byte("x"), tenant: tenantA, level: "rw",
	})
	assert.Equal(t, http.StatusForbidden, rec.Code)
	assert.Equal(t, before, upstream.count())
}
