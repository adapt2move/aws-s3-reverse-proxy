package main

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/Kriechi/aws-s3-reverse-proxy/internal/config"
	"github.com/Kriechi/aws-s3-reverse-proxy/internal/policytest"
)

// Startup is the last place a misconfiguration can be caught before requests
// start arriving, so every way of getting it wrong has to end here rather
// than in a process that is listening and refusing everything.

func writePolicyFile(t *testing.T, body string) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "policy.yaml")
	require.NoError(t, os.WriteFile(path, []byte(body), 0o600))
	return path
}

func baseOptions(t *testing.T) config.Options {
	return config.Options{
		AllowedSourceEndpoint:   "foobar.endpoint.example.com",
		AllowedSourceSubnet:     []string{"127.0.0.1/32", "192.168.1.0/24"},
		UpstreamRegion:          "eu-test-1",
		PolicyFile:              writePolicyFile(t, policytest.YAML),
		Pepper:                  policytest.Pepper,
		UpstreamAccessKeyID:     "UPSTREAMKEYID",
		UpstreamSecretAccessKey: "upstream-secret",
	}
}

func TestBuildProxy(t *testing.T) {
	handler, store, objectCache, err := buildProxy(baseOptions(t))
	require.NoError(t, err)

	scheme, endpoint := handler.UpstreamAddr()
	assert.Equal(t, "https", scheme)
	assert.Equal(t, "", endpoint, "no configured endpoint means auto-detect from the region")
	assert.Equal(t, []string{"ro", "rw", "rws"}, store.Current().Levels())
	assert.Nil(t, objectCache, "caching is off unless a directory is configured for it")
}

func TestBuildProxyOpensTheCacheWhenOneIsConfigured(t *testing.T) {
	opts := baseOptions(t)
	opts.CacheDir = filepath.Join(t.TempDir(), "objects")
	opts.CacheMaxBytes = 8 << 20
	opts.CacheMaxObjectSize = 1 << 20

	_, _, objectCache, err := buildProxy(opts)
	require.NoError(t, err)
	require.NotNil(t, objectCache)
	defer objectCache.Close()

	assert.Equal(t, int64(8<<20), objectCache.Stats().Capacity)
	assert.DirExists(t, opts.CacheDir, "the cache directory is created rather than required to exist")
}

// A cache an operator asked for and did not get is worse than no cache: the
// deployment quietly performs like one without it and nothing says so.
func TestStartupFailsOnAnUnusableCacheDirectory(t *testing.T) {
	blocked := filepath.Join(t.TempDir(), "not-a-directory")
	require.NoError(t, os.WriteFile(blocked, []byte("in the way"), 0o600))

	opts := baseOptions(t)
	opts.CacheDir = filepath.Join(blocked, "objects")
	_, _, _, err := buildProxy(opts)
	require.Error(t, err)
}

func TestStartupRejectsContradictoryCacheSizes(t *testing.T) {
	opts := baseOptions(t)
	opts.CacheDir = filepath.Join(t.TempDir(), "objects")
	// An object that cannot share a segment with anything would make the
	// segment layout pointless, so it is refused rather than quietly
	// reinterpreted.
	opts.CacheSegmentSize = 4 << 20
	opts.CacheInlineMaxSize = 4 << 20

	_, _, _, err := buildProxy(opts)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "InlineMaxSize")
}

func TestBuildProxyRejectsBrokenSubnets(t *testing.T) {
	for _, subnet := range []string{"foobar", "", "127.0.0.1/XXX", "127.0.0.1", "256.0.0.1"} {
		opts := baseOptions(t)
		opts.AllowedSourceSubnet = []string{subnet}
		_, _, _, err := buildProxy(opts)
		require.Error(t, err, subnet)
		assert.Contains(t, err.Error(), "invalid allowed source subnet")
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
	_, _, _, err := buildProxy(opts)
	require.Error(t, err)
	assert.Contains(t, err.Error(), `grant is missing level "rw"`)

	opts = baseOptions(t)
	opts.PolicyFile = filepath.Join(t.TempDir(), "does-not-exist.yaml")
	_, _, _, err = buildProxy(opts)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "cannot read policy file")
}

func TestStartupRequiresSecrets(t *testing.T) {
	opts := baseOptions(t)
	opts.Pepper = nil
	_, _, _, err := buildProxy(opts)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "no credential pepper configured")

	opts = baseOptions(t)
	opts.UpstreamSecretAccessKey = ""
	_, _, _, err = buildProxy(opts)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "no upstream credentials configured")
}

func TestIsListenAddr(t *testing.T) {
	for _, addr := range []string{":8099", "127.0.0.1:8100", "0.0.0.0:0"} {
		assert.True(t, isListenAddr(addr), addr)
	}
	for _, addr := range []string{"", "8099", "no-port-here"} {
		assert.False(t, isListenAddr(addr), addr)
	}
}
