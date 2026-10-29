package proxy

import (
	"fmt"
	"net/http"
	"testing"

	log "github.com/sirupsen/logrus"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/Kriechi/aws-s3-reverse-proxy/internal/policy"
)

const (
	globalKeyID  = "GLOBALOPERATORKEY"
	globalSecret = "global-operator-secret-value"
)

func withGlobalAccess(permission policy.Permission) func(*Config) {
	return func(cfg *Config) {
		cfg.GlobalAccess = &policy.GlobalAccess{
			AccessKeyID:     globalKeyID,
			SecretAccessKey: globalSecret,
			Permission:      permission,
		}
	}
}

func globalRequest(method, target string, body []byte) clientRequest {
	return clientRequest{method: method, target: target, body: body, accessKeyID: globalKeyID, secret: globalSecret}
}

// The global credential addresses the bucket as it is upstream: whatever key
// it names is the key that is forwarded, a tenant's included, and no rule of
// the policy — written against tenant-facing keys — decides it.
func TestGlobalAccessIsNotScopedToATenant(t *testing.T) {
	h, upstream := newTestProxy(t, withGlobalAccess(policy.PermissionFull))

	for _, tc := range []struct{ method, target string }{
		{http.MethodGet, "/bucket/" + tenantA + "/datasets/2026/private/x.csv"},
		{http.MethodPut, "/bucket/" + tenantB + "/workspaces/w1/inbox/note.txt"},
		{http.MethodGet, "/bucket/outside-every-tenant.txt"},
		{http.MethodDelete, "/bucket/" + tenantA + "/nowhere/x.txt"},
	} {
		rec := do(t, h, globalRequest(tc.method, tc.target, nil))
		require.Equal(t, http.StatusOK, rec.Code, "%s %s", tc.method, tc.target)
		assert.Equal(t, tc.target, upstream.last(t).path, "forwarded without a prefix")
	}
}

// A listing needs no prefix, sees every tenant, and comes back as the object
// store wrote it: nothing to strip, nothing to filter.
func TestGlobalAccessListsTheWholeBucket(t *testing.T) {
	h, upstream := newTestProxy(t, withGlobalAccess(policy.PermissionRead))
	listing := fmt.Sprintf(`<?xml version="1.0" encoding="UTF-8"?>`+
		`<ListBucketResult><Name>bucket</Name><Prefix></Prefix>`+
		`<Contents><Key>%s/datasets/2026/private/x.csv</Key></Contents>`+
		`<Contents><Key>%s/nowhere/y.csv</Key></Contents>`+
		`</ListBucketResult>`, tenantA, tenantB)
	upstream.respond = func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/xml")
		_, _ = w.Write([]byte(listing))
	}

	rec := do(t, h, globalRequest(http.MethodGet, "/bucket?list-type=2", nil))
	require.Equal(t, http.StatusOK, rec.Code)
	assert.Equal(t, "", upstream.last(t).query.Get("prefix"))
	assert.Contains(t, rec.Body.String(), "<Key>"+tenantA+"/datasets/2026/private/x.csv</Key>")
	assert.Contains(t, rec.Body.String(), "<Key>"+tenantB+"/nowhere/y.csv</Key>")
}

func TestGlobalReadAccessCannotWrite(t *testing.T) {
	h, upstream := newTestProxy(t, withGlobalAccess(policy.PermissionRead))

	rec := do(t, h, globalRequest(http.MethodGet, "/bucket/"+tenantA+"/datasets/a.csv", nil))
	require.Equal(t, http.StatusOK, rec.Code)

	for _, method := range []string{http.MethodPut, http.MethodDelete} {
		rec := do(t, h, globalRequest(method, "/bucket/"+tenantA+"/datasets/a.csv", []byte("x")))
		assert.Equal(t, http.StatusForbidden, rec.Code, method)
	}
	assert.Equal(t, 1, upstream.count(), "neither write reached the object store")
}

func TestGlobalAccessBatchDelete(t *testing.T) {
	t.Run("full deletes every key as named", func(t *testing.T) {
		h, upstream := newTestProxy(t, withGlobalAccess(policy.PermissionFull))
		echoDeleteResult(upstream)
		keys := []string{tenantA + "/workspaces/w1/inbox/note.txt", tenantB + "/datasets/old.csv"}
		rec := do(t, h, globalRequest(http.MethodPost, "/bucket?delete", deleteBatchBody(keys...)))
		require.Equal(t, http.StatusOK, rec.Code)
		assert.Equal(t, keys, parsedKeys(upstream.last(t)))
	})

	t.Run("read deletes nothing", func(t *testing.T) {
		h, upstream := newTestProxy(t, withGlobalAccess(policy.PermissionRead))
		rec := do(t, h, globalRequest(http.MethodPost, "/bucket?delete", deleteBatchBody(tenantA+"/datasets/old.csv")))
		require.Equal(t, http.StatusOK, rec.Code)
		assert.Contains(t, rec.Body.String(), "<Error><Key>"+tenantA+"/datasets/old.csv</Key><Code>AccessDenied</Code>")
		assert.Equal(t, 0, upstream.count())
	})
}

// Everything outside the policy holds for the global credential exactly as
// for a tenant.
func TestGlobalAccessKeepsTheDeploymentsLimits(t *testing.T) {
	t.Run("read-only mode", func(t *testing.T) {
		h, upstream := newTestProxy(t, withGlobalAccess(policy.PermissionFull), func(cfg *Config) { cfg.ReadOnly = true })
		rec := do(t, h, globalRequest(http.MethodPut, "/bucket/x.txt", []byte("x")))
		assert.Equal(t, http.StatusForbidden, rec.Code)
		assert.Equal(t, 0, upstream.count())
	})

	t.Run("the signature", func(t *testing.T) {
		h, upstream := newTestProxy(t, withGlobalAccess(policy.PermissionFull))
		req := globalRequest(http.MethodGet, "/bucket/x.txt", nil)
		req.secret = "not-the-global-secret-at-all"
		assert.Equal(t, http.StatusForbidden, do(t, h, req).Code)
		assert.Equal(t, 0, upstream.count())
	})

	t.Run("the operation whitelist", func(t *testing.T) {
		h, upstream := newTestProxy(t, withGlobalAccess(policy.PermissionFull))
		req := globalRequest(http.MethodPut, "/bucket/copy.txt", nil)
		req.headers = http.Header{"X-Amz-Copy-Source": {"/bucket/" + tenantA + "/datasets/a.csv"}, "X-Amz-Metadata-Directive": {"REPLACE"}}
		assert.Equal(t, http.StatusForbidden, do(t, h, req).Code)
		assert.Equal(t, 0, upstream.count())
	})

	t.Run("nothing at all when it is not configured", func(t *testing.T) {
		h, upstream := newTestProxy(t)
		assert.Equal(t, http.StatusForbidden, do(t, h, globalRequest(http.MethodGet, "/bucket/x.txt", nil)).Code)
		assert.Equal(t, 0, upstream.count())
	})
}

// The point of going through the proxy at all: the global credential and the
// tenants read and write one cache, keyed by the upstream object, so a write
// by either side is what the other is served next.
func TestGlobalAccessSharesTheCacheWithTenants(t *testing.T) {
	h, upstream, _ := newCachingProxy(t, withGlobalAccess(policy.PermissionFull))
	servesObject(upstream, "written by the tenant", nil)
	tenantKey := "/bucket/datasets/2026/a.csv"
	globalKey := "/bucket/" + tenantA + "/datasets/2026/a.csv"

	first := getObject(t, h, tenantA, "rw", tenantKey, nil)
	require.Equal(t, http.StatusOK, first.StatusCode)
	assert.Equal(t, "MISS", first.Header.Get("X-Cache"))

	global := do(t, h, globalRequest(http.MethodGet, globalKey, nil)).Result()
	require.Equal(t, http.StatusOK, global.StatusCode)
	assert.Equal(t, "HIT", global.Header.Get("X-Cache"), "the tenant's read filled the entry the global read is served from")
	assert.Equal(t, "written by the tenant", readBody(t, global))
	assert.Equal(t, 1, upstream.count())

	put := do(t, h, globalRequest(http.MethodPut, globalKey, []byte("replaced globally"))).Result()
	require.Equal(t, http.StatusOK, put.StatusCode)
	after := getObject(t, h, tenantA, "rw", tenantKey, nil)
	require.Equal(t, http.StatusOK, after.StatusCode)
	assert.Equal(t, "HIT", after.Header.Get("X-Cache"))
	assert.Equal(t, "replaced globally", readBody(t, after), "the tenant is served the global write, not its stale entry")
	assert.Equal(t, 2, upstream.count(), "the upload, and nothing else")
}

// An id the tenant pattern would also accept is the global credential's: it
// verifies against the global secret, never against a derived one.
func TestGlobalAccessTakesPrecedenceOverTheTenantPattern(t *testing.T) {
	id := accessKeyFor(tenantA, "rws")
	h, upstream := newTestProxy(t, func(cfg *Config) {
		cfg.GlobalAccess = &policy.GlobalAccess{AccessKeyID: id, SecretAccessKey: globalSecret, Permission: policy.PermissionRead}
	})

	derived := do(t, h, clientRequest{method: http.MethodGet, target: "/bucket/datasets/a.csv", tenant: tenantA, level: "rws"})
	assert.Equal(t, http.StatusForbidden, derived.Code)

	global := do(t, h, clientRequest{method: http.MethodGet, target: "/bucket/datasets/a.csv", accessKeyID: id, secret: globalSecret})
	require.Equal(t, http.StatusOK, global.Code)
	assert.Equal(t, "/bucket/datasets/a.csv", upstream.last(t).path, "unprefixed")
}

func TestGlobalAccessIsValidatedAtStartup(t *testing.T) {
	upstream := newFakeUpstream(t)
	for _, g := range []policy.GlobalAccess{
		{AccessKeyID: "", SecretAccessKey: globalSecret, Permission: policy.PermissionFull},
		{AccessKeyID: globalKeyID, SecretAccessKey: "short", Permission: policy.PermissionFull},
		{AccessKeyID: globalKeyID, SecretAccessKey: globalSecret, Permission: policy.PermissionDeny},
		{AccessKeyID: globalKeyID, SecretAccessKey: globalSecret, Permission: ""},
	} {
		cfg := testConfig(t, upstream.server.URL)
		cfg.GlobalAccess = &g
		_, err := New(cfg)
		assert.Error(t, err, "%+v", g)
	}
}

// A legacy plaintext object read through the global credential is rewritten
// under the key when that credential may write — wherever it lies, since no
// rule speaks to upstream keys — and left alone when it may only read.
func TestGlobalAccessRepairsPlaintextOnlyWithFullPermission(t *testing.T) {
	t.Run("full", func(t *testing.T) {
		store := newSSEStore(t)
		h := newEncryptingProxy(t, store, withGlobalAccess(policy.PermissionFull))
		key := tenantA + "/workspaces/w1/inbox/legacy.csv" // no level may write it
		store.put(key, []byte("old"), "", nil)

		get := do(t, h, globalRequest(http.MethodGet, "/bucket/"+key, nil)).Result()
		require.Equal(t, http.StatusOK, get.StatusCode)
		assert.Equal(t, "old", readBody(t, get))
		waitForRepair(t, store, key)
	})

	t.Run("read", func(t *testing.T) {
		hook := captureLogs(t)
		store := newSSEStore(t)
		h := newEncryptingProxy(t, store, withGlobalAccess(policy.PermissionRead))
		key := upstreamKey("datasets/legacy.csv")
		store.put(key, []byte("old"), "", nil)

		get := do(t, h, globalRequest(http.MethodGet, "/bucket/"+key, nil)).Result()
		require.Equal(t, http.StatusOK, get.StatusCode)
		assert.Equal(t, "old", readBody(t, get))
		waitFor(t, "the skip", func() bool {
			return logged(hook, log.WarnLevel, "the global access credential may not write")
		})
		h.Close()
		assert.Equal(t, "", store.object(key).keyMD5)
		assert.Empty(t, store.requestsWhere(func(r recordedRequest) bool { return r.method == http.MethodPost || r.method == http.MethodPut }))
	})
}
