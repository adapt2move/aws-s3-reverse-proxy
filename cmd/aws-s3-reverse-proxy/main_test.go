package main

import (
	"bytes"
	"encoding/base64"
	"net/http"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/Kriechi/aws-s3-reverse-proxy/internal/cache"
	"github.com/Kriechi/aws-s3-reverse-proxy/internal/config"
	"github.com/Kriechi/aws-s3-reverse-proxy/internal/policytest"
	"github.com/Kriechi/aws-s3-reverse-proxy/internal/ssec"
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
//
// The message has to name the directory, because the most common way to get
// here is a volume mounted into a container that runs as somebody other than
// root — the directory exists and simply belongs to the wrong user, and the
// syscall's own "permission denied" on a path nobody typed does not say that.
func TestStartupFailsOnAnUnusableCacheDirectory(t *testing.T) {
	blocked := filepath.Join(t.TempDir(), "not-a-directory")
	require.NoError(t, os.WriteFile(blocked, []byte("in the way"), 0o600))

	opts := baseOptions(t)
	opts.CacheDir = filepath.Join(blocked, "objects")
	_, _, _, err := buildProxy(opts)
	require.Error(t, err)
	assert.Contains(t, err.Error(), opts.CacheDir)
	assert.Contains(t, err.Error(), "writable by the user this process runs as")
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

func sseKey(t *testing.T, b byte) *ssec.Key {
	t.Helper()
	key, err := ssec.Parse(base64.StdEncoding.EncodeToString(bytes.Repeat([]byte{b}, ssec.KeySize)))
	require.NoError(t, err)
	return key
}

// A customer key over plain http would cross the network in the clear on
// every request. It fails at startup — and before the cache is opened, which
// would otherwise already have discarded what an unencrypted deployment had
// cached under the new key's label.
func TestStartupRefusesAKeyOverHTTP(t *testing.T) {
	opts := baseOptions(t)
	opts.UpstreamEndpoint = "http://minio:9000"
	opts.UpstreamSSECKey = sseKey(t, 1)
	opts.CacheDir = filepath.Join(t.TempDir(), "objects")

	_, _, _, err := buildProxy(opts)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "https")
	assert.NoDirExists(t, opts.CacheDir, "the cache was never opened")

	opts.UpstreamEndpoint = "https://minio:9000"
	opts.CacheDir = ""
	handler, _, _, err := buildProxy(opts)
	require.NoError(t, err)
	handler.Close()
}

// What the cache holds was read with one key. A restart with another one — or
// with none — must not look healthy only because the cache still answers for
// objects the store would now refuse to decrypt.
func TestTheCacheIsEmptiedWhenTheKeyChanges(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "objects")
	// The cache is opened the way buildCache opens it, label and all; going
	// through buildCache itself would register its metrics a second time.
	start := func(key *ssec.Key) (*cache.Cache, func()) {
		opts := baseOptions(t)
		opts.UpstreamSSECKey = key
		objectCache, err := cache.New(cache.Options{Dir: dir, Label: cacheLabel(opts)})
		require.NoError(t, err)
		return objectCache, func() { require.NoError(t, objectCache.Close()) }
	}
	store := func(c *cache.Cache) {
		w, err := c.Put("bucket/tenant/object", 5)
		require.NoError(t, err)
		_, _ = w.Write([]byte("bytes"))
		require.NoError(t, w.Commit(http.Header{}))
	}

	c, stop := start(sseKey(t, 1))
	store(c)
	stop()

	c, stop = start(sseKey(t, 1))
	assert.True(t, c.Has("bucket/tenant/object"), "the same key keeps the cache")
	stop()

	c, stop = start(sseKey(t, 2))
	assert.False(t, c.Has("bucket/tenant/object"), "another key empties it")
	store(c)
	stop()

	c, stop = start(nil)
	assert.False(t, c.Has("bucket/tenant/object"), "and so does removing the key")
	stop()
}
