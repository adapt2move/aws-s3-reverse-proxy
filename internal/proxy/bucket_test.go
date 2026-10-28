package proxy

import (
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// HeadBucket is what rclone, mc and s3fs ask before anything else. It says
// whether the bucket exists, which is not any one tenant's to hide, so every
// level of every tenant may ask — and it goes to the object store, which is
// the one that knows.
func TestHeadBucketIsForwarded(t *testing.T) {
	h, upstream := newTestProxy(t)
	for _, target := range []string{"/bucket", "/bucket/"} {
		rec := do(t, h, clientRequest{method: http.MethodHead, target: target, tenant: tenantA, level: "ro"})
		assert.Equal(t, http.StatusOK, rec.Code, target)

		forwarded := upstream.last(t)
		assert.Equal(t, http.MethodHead, forwarded.method)
		assert.Equal(t, target, forwarded.path, "the bucket is addressed as it is, with no tenant prefix in it")
		assert.Empty(t, forwarded.query)
		assert.Contains(t, forwarded.header.Get("Authorization"), "UPSTREAMKEYID", "re-signed with the proxy's credentials")
	}
}

func TestHeadBucketPassesOnWhatTheObjectStoreSays(t *testing.T) {
	h, upstream := newTestProxy(t)
	upstream.mu.Lock()
	upstream.respond = func(w http.ResponseWriter, r *http.Request) { w.WriteHeader(http.StatusNotFound) }
	upstream.mu.Unlock()

	rec := do(t, h, clientRequest{method: http.MethodHead, target: "/missing", tenant: tenantA, level: "ro"})
	assert.Equal(t, http.StatusNotFound, rec.Code)
}

func TestBucketOperationsStillNeedAValidSignature(t *testing.T) {
	h, upstream := newTestProxy(t)
	for _, c := range []clientRequest{
		{method: http.MethodHead, target: "/bucket", unsigned: true},
		{method: http.MethodGet, target: "/bucket?location", unsigned: true},
		{method: http.MethodHead, target: "/bucket", tenant: tenantA, level: "ro", secret: "not-the-secret"},
	} {
		assert.Equal(t, http.StatusForbidden, do(t, h, c).Code, c.method+" "+c.target)
	}
	assert.Zero(t, upstream.count(), "nothing unauthenticated reaches the object store")
}

// GetBucketLocation is answered here: the region that works through this
// proxy is the one it signs for upstream, whatever the object store would say.
func TestBucketLocationIsTheUpstreamRegion(t *testing.T) {
	h, upstream := newTestProxy(t)
	rec := do(t, h, clientRequest{method: http.MethodGet, target: "/bucket?location", tenant: tenantA, level: "ro"})
	require.Equal(t, http.StatusOK, rec.Code)
	assert.Equal(t, "application/xml", rec.Header().Get("Content-Type"))
	assert.Contains(t, rec.Body.String(),
		`<LocationConstraint xmlns="http://s3.amazonaws.com/doc/2006-03-01/">`+testRegion+`</LocationConstraint>`)
	assert.Zero(t, upstream.count(), "the object store is not asked")
}

func TestBucketLocationInUSEast1IsTheEmptyConstraint(t *testing.T) {
	h, _ := newTestProxy(t, func(cfg *Config) { cfg.UpstreamRegion = "us-east-1" })
	rec := do(t, h, clientRequest{method: http.MethodGet, target: "/bucket?location", tenant: tenantA, level: "ro"})
	require.Equal(t, http.StatusOK, rec.Code)
	assert.Contains(t, rec.Body.String(), `<LocationConstraint xmlns="http://s3.amazonaws.com/doc/2006-03-01/"></LocationConstraint>`)
}
