package config

import (
	"net/http"
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

// The SSE-C key is the one secret whose absence and whose emptiness must not
// mean the same thing: an operator who meant to turn encryption on and ended
// up with an empty value would otherwise get a proxy that writes plaintext.
func TestLoadSSECKey(t *testing.T) {
	valid := "AQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQE="
	base := func(t *testing.T) {
		t.Setenv(envUpstreamCredentials, "AKID,secret-value")
		t.Setenv(envCredentialPepper, "a-sufficiently-long-pepper")
	}
	unset := func(t *testing.T, name string) {
		// t.Setenv first, so the variable is restored afterwards.
		t.Setenv(name, "")
		require.NoError(t, os.Unsetenv(name))
	}

	t.Run("absent disables it", func(t *testing.T) {
		base(t)
		unset(t, envUpstreamSSECKey)
		unset(t, envUpstreamSSECKey+"_FILE")
		var opts Options
		require.NoError(t, LoadSecrets(&opts))
		assert.Nil(t, opts.UpstreamSSECKey)
	})

	t.Run("from the environment", func(t *testing.T) {
		base(t)
		unset(t, envUpstreamSSECKey+"_FILE")
		t.Setenv(envUpstreamSSECKey, valid)
		var opts Options
		require.NoError(t, LoadSecrets(&opts))
		require.NotNil(t, opts.UpstreamSSECKey)
	})

	t.Run("from a mounted file, trailing newline and all", func(t *testing.T) {
		base(t)
		unset(t, envUpstreamSSECKey)
		path := filepath.Join(t.TempDir(), "key")
		require.NoError(t, os.WriteFile(path, []byte(valid+"\n"), 0o600))
		t.Setenv(envUpstreamSSECKey+"_FILE", path)
		var opts Options
		require.NoError(t, LoadSecrets(&opts))
		require.NotNil(t, opts.UpstreamSSECKey)

		// Loaded once: changing the file afterwards changes nothing about
		// the key this process already holds.
		before := http.Header{}
		opts.UpstreamSSECKey.Apply(before)
		require.NoError(t, os.WriteFile(path, []byte("AgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgI="), 0o600))
		after := http.Header{}
		opts.UpstreamSSECKey.Apply(after)
		assert.Equal(t, before, after)
	})

	for name, setup := range map[string]func(t *testing.T){
		"an explicitly empty value": func(t *testing.T) {
			unset(t, envUpstreamSSECKey+"_FILE")
			t.Setenv(envUpstreamSSECKey, "")
		},
		"an explicitly empty file path": func(t *testing.T) {
			unset(t, envUpstreamSSECKey)
			t.Setenv(envUpstreamSSECKey+"_FILE", "")
		},
		"an unreadable file": func(t *testing.T) {
			unset(t, envUpstreamSSECKey)
			t.Setenv(envUpstreamSSECKey+"_FILE", filepath.Join(t.TempDir(), "missing"))
		},
		"an empty file": func(t *testing.T) {
			unset(t, envUpstreamSSECKey)
			path := filepath.Join(t.TempDir(), "key")
			require.NoError(t, os.WriteFile(path, []byte("\n"), 0o600))
			t.Setenv(envUpstreamSSECKey+"_FILE", path)
		},
		"a malformed value": func(t *testing.T) {
			unset(t, envUpstreamSSECKey+"_FILE")
			t.Setenv(envUpstreamSSECKey, "not-a-key")
		},
		"a key of the wrong length": func(t *testing.T) {
			unset(t, envUpstreamSSECKey+"_FILE")
			t.Setenv(envUpstreamSSECKey, "AQEBAQEBAQEBAQEBAQEBAQ==")
		},
		"both sources at once": func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "key")
			require.NoError(t, os.WriteFile(path, []byte(valid), 0o600))
			t.Setenv(envUpstreamSSECKey, valid)
			t.Setenv(envUpstreamSSECKey+"_FILE", path)
		},
	} {
		t.Run(name+" fails", func(t *testing.T) {
			base(t)
			setup(t)
			var opts Options
			err := LoadSecrets(&opts)
			require.Error(t, err)
			assert.Nil(t, opts.UpstreamSSECKey)
			assert.NotContains(t, err.Error(), valid, "the error never repeats a key")
		})
	}
}
