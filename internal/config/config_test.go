package config

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

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
		scheme, host, err := ParseUpstreamEndpoint(tc.endpoint, tc.insecure)
		require.NoError(t, err, tc.endpoint)
		assert.Equal(t, tc.wantScheme, scheme, tc.endpoint)
		assert.Equal(t, tc.wantHost, host, tc.endpoint)
	}

	_, _, err := ParseUpstreamEndpoint("ftp://s3.example.com", false)
	assert.Error(t, err)
}

func TestParseSubnets(t *testing.T) {
	subnets, err := ParseSubnets([]string{"127.0.0.1/32", "192.168.1.0/24"})
	require.NoError(t, err)
	require.Len(t, subnets, 2)
	assert.Equal(t, "127.0.0.1/32", subnets[0].String())
	assert.Equal(t, "192.168.1.0/24", subnets[1].String())

	for _, bad := range []string{"foobar", "", "127.0.0.1/XXX", "127.0.0.1", "256.0.0.1"} {
		_, err := ParseSubnets([]string{bad})
		require.Error(t, err, bad)
		assert.Contains(t, err.Error(), "invalid allowed source subnet")
	}
}

// Secrets come from the environment or a mounted file — never from a flag
// (they would show up in `ps`) and never from the policy file (it is a
// ConfigMap).
func TestLoadSecrets(t *testing.T) {
	t.Run("combined credentials and a pepper", func(t *testing.T) {
		t.Setenv(envUpstreamCredentials, "AKID,secret-value")
		t.Setenv(envCredentialPepper, "a-sufficiently-long-pepper")

		var opts Options
		require.NoError(t, LoadSecrets(&opts))
		assert.Equal(t, "AKID", opts.UpstreamAccessKeyID)
		assert.Equal(t, "secret-value", opts.UpstreamSecretAccessKey)
		assert.Equal(t, "a-sufficiently-long-pepper", string(opts.Pepper))
	})

	t.Run("separate credential variables", func(t *testing.T) {
		t.Setenv(envUpstreamAccessKey, "AKID")
		t.Setenv(envUpstreamSecretKey, "secret-value")
		t.Setenv(envCredentialPepper, "a-sufficiently-long-pepper")

		var opts Options
		require.NoError(t, LoadSecrets(&opts))
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
		require.NoError(t, LoadSecrets(&opts))
		assert.Equal(t, "a-sufficiently-long-pepper", string(opts.Pepper))
	})

	// An unreadable secret file is a misconfiguration like any other: it has
	// to stop startup with a message that names the variable, not leave the
	// process running with an empty secret.
	t.Run("an unreadable secret file is an error", func(t *testing.T) {
		t.Setenv(envUpstreamCredentials, "AKID,secret-value")
		t.Setenv(envCredentialPepper+"_FILE", filepath.Join(t.TempDir(), "absent"))

		var opts Options
		err := LoadSecrets(&opts)
		require.Error(t, err)
		assert.Contains(t, err.Error(), envCredentialPepper+"_FILE")
	})

	t.Run("missing credentials", func(t *testing.T) {
		t.Setenv(envCredentialPepper, "a-sufficiently-long-pepper")
		var opts Options
		err := LoadSecrets(&opts)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "upstream credentials are required")
	})

	t.Run("missing pepper", func(t *testing.T) {
		t.Setenv(envUpstreamCredentials, "AKID,secret-value")
		var opts Options
		err := LoadSecrets(&opts)
		require.Error(t, err)
		assert.Contains(t, err.Error(), envCredentialPepper)
	})

	t.Run("pepper too short", func(t *testing.T) {
		t.Setenv(envUpstreamCredentials, "AKID,secret-value")
		t.Setenv(envCredentialPepper, "short")
		var opts Options
		err := LoadSecrets(&opts)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "at least")
	})

	t.Run("malformed combined credentials", func(t *testing.T) {
		t.Setenv(envUpstreamCredentials, "no-comma-here")
		t.Setenv(envCredentialPepper, "a-sufficiently-long-pepper")
		var opts Options
		err := LoadSecrets(&opts)
		require.Error(t, err)
		assert.Contains(t, err.Error(), envUpstreamCredentials)
	})
}
