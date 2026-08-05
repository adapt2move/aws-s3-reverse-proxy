package main

import (
	"fmt"
	"net/http"
	"net/http/httptest"
	"sort"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// The cross-tenant escape suite. Every case here is an attempt to reach
// bytes that belong to somebody else; all of them have to fail, and none of
// them may reach the upstream.
func TestCrossTenantEscape(t *testing.T) {
	cases := []struct {
		name string
		req  clientRequest
	}{
		{
			// The access-key id has the right shape but names a tenant the
			// attacker made up, so the derived secret is one they cannot
			// know — the signature check fails before anything else.
			name: "forged access key id",
			req: clientRequest{
				method: http.MethodGet, target: "/bucket/datasets/a.csv",
				accessKeyID: tenantB + "rws", secret: "attacker-chosen-secret",
			},
		},
		{
			// Tenant A's own credential, aimed at a key under tenant B's
			// prefix. The prefix is injected, not validated, so this asks
			// for `<A>/<B>/datasets/…` — which matches no rule at all.
			name: "valid signature from tenant A aimed at tenant B's key",
			req: clientRequest{
				method: http.MethodGet, target: "/bucket/" + tenantB + "/datasets/a.csv",
				tenant: tenantA, level: "rws",
			},
		},
		{
			name: "path traversal out of the tenant prefix",
			req: clientRequest{
				method: http.MethodGet, target: "/bucket/../" + tenantB + "/datasets/a.csv",
				tenant: tenantA, level: "rws",
			},
		},
		{
			name: "traversal inside the key",
			req: clientRequest{
				method: http.MethodGet, target: "/bucket/datasets/../../" + tenantB + "/datasets/a.csv",
				tenant: tenantA, level: "rws",
			},
		},
		{
			name: "percent-encoded traversal",
			req: clientRequest{
				method: http.MethodGet, target: "/bucket/datasets/%2e%2e/%2e%2e/" + tenantB + "/a.csv",
				tenant: tenantA, level: "rws",
			},
		},
		{
			name: "double-encoded traversal",
			req: clientRequest{
				method: http.MethodGet, target: "/bucket/datasets/%252e%252e/x.csv",
				tenant: tenantA, level: "rws",
			},
		},
		{
			name: "absolute key",
			req: clientRequest{
				method: http.MethodGet, target: "/bucket//etc/passwd",
				tenant: tenantA, level: "rws",
			},
		},
		{
			// `evil/` looks like a tenant prefix, but it is just a key
			// inside the caller's own scope once the real prefix goes in
			// front of it.
			name: "prefix collision",
			req: clientRequest{
				method: http.MethodGet, target: "/bucket/evil/datasets/a.csv",
				tenant: tenantA, level: "rws",
			},
		},
		{
			// The secret is derived per level, so a read-only credential
			// cannot produce a signature for the read-write identity.
			name: "level escalation with a lower-level secret",
			req: clientRequest{
				method: http.MethodPut, target: "/bucket/datasets/a.csv",
				body:        []byte("x"),
				accessKeyID: tenantA + "rws", secret: secretFor(tenantA, "ro"),
			},
		},
		{
			name: "replayed signature past the skew window",
			req: clientRequest{
				method: http.MethodGet, target: "/bucket/datasets/a.csv",
				tenant: tenantA, level: "ro", signTime: time.Now().Add(-2 * time.Hour),
			},
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			h, upstream := newTestProxy(t)
			rec := do(t, h, tc.req)
			assert.NotEqual(t, http.StatusOK, rec.Code, "escape attempt must not succeed")
			assert.Equal(t, 0, upstream.count(), "escape attempt must never reach the object store")
		})
	}

	// The control: the very same client, asking for something it may have.
	t.Run("control: the legitimate request still works", func(t *testing.T) {
		h, upstream := newTestProxy(t)
		rec := do(t, h, clientRequest{
			method: http.MethodGet, target: "/bucket/datasets/a.csv",
			tenant: tenantA, level: "rws",
		})
		require.Equal(t, http.StatusOK, rec.Code)
		assert.Equal(t, "/bucket/"+tenantA+"/datasets/a.csv", upstream.last(t).path)
	})
}

// A batch delete that names another tenant's key alongside one of its own
// loses only the foreign key — and the foreign key never travels upstream.
// The status stays 200 because that is how S3 reports a per-key failure;
// what matters is that the key is refused, not that the batch is.
func TestCrossTenantBatchDeleteIsPartiallyRefused(t *testing.T) {
	h, upstream := newTestProxy(t)
	echoDeleteResult(upstream)

	rec := do(t, h, clientRequest{
		method: http.MethodPost, target: "/bucket?delete",
		body:   deleteBatchBody("datasets/mine.csv", tenantB+"/datasets/theirs.csv"),
		tenant: tenantA, level: "rws",
	})
	require.Equal(t, http.StatusOK, rec.Code)

	assert.Equal(t, []string{tenantA + "/datasets/mine.csv"}, parsedKeys(upstream.last(t)))
	assert.Contains(t, rec.Body.String(), "<Error><Key>"+tenantB+"/datasets/theirs.csv</Key><Code>AccessDenied</Code>")
}

func TestCrossTenantBatchDeleteReachesNothing(t *testing.T) {
	h, upstream := newTestProxy(t)

	rec := do(t, h, clientRequest{
		method: http.MethodPost, target: "/bucket?delete",
		body:   deleteBatchBody(tenantB + "/datasets/theirs.csv"),
		tenant: tenantA, level: "rws",
	})
	require.Equal(t, http.StatusOK, rec.Code)
	assert.Equal(t, 0, upstream.count(), "a batch of foreign keys must never reach the object store")
	assert.Contains(t, rec.Body.String(), "<Error><Key>"+tenantB+"/datasets/theirs.csv</Key><Code>AccessDenied</Code>")
}

// The added latency budget for the hot path. The proxy's own work on a
// GET/HEAD is a signature verification, a policy match and a re-sign; this
// measures it against the same upstream reached directly.
func TestAddedLatencyP99(t *testing.T) {
	if testing.Short() {
		t.Skip("latency measurement is skipped in short mode")
	}
	if raceDetectorEnabled {
		t.Skip("the race detector's overhead makes a latency budget meaningless")
	}
	const (
		iterations = 500
		warmup     = 50
		budget     = 5 * time.Millisecond
	)

	h, upstream := newTestProxy(t)
	client := upstream.server.Client()

	for _, method := range []string{http.MethodGet, http.MethodHead} {
		t.Run(method, func(t *testing.T) {
			direct := make([]time.Duration, 0, iterations)
			proxied := make([]time.Duration, 0, iterations)

			for i := 0; i < warmup+iterations; i++ {
				// Baseline: the same upstream, no proxy in between.
				req, err := http.NewRequest(method, upstream.server.URL+"/bucket/x", nil)
				require.NoError(t, err)
				start := time.Now()
				resp, err := client.Do(req)
				require.NoError(t, err)
				resp.Body.Close()
				elapsed := time.Since(start)

				proxyReq := clientRequest{
					method: method, target: "/bucket/datasets/a.csv",
					tenant: tenantA, level: "ro",
				}.build(t)
				rec := httptest.NewRecorder()
				proxyStart := time.Now()
				h.ServeHTTP(rec, proxyReq)
				proxyElapsed := time.Since(proxyStart)
				require.Equal(t, http.StatusOK, rec.Code)

				if i >= warmup {
					direct = append(direct, elapsed)
					proxied = append(proxied, proxyElapsed)
				}
			}

			added := percentile(proxied, 0.99) - percentile(direct, 0.99)
			if added < 0 {
				added = 0
			}
			t.Logf("p99 direct=%s proxied=%s added=%s", percentile(direct, 0.99), percentile(proxied, 0.99), added)
			assert.LessOrEqual(t, added, budget, fmt.Sprintf("p99 added latency for %s exceeds the %s budget", method, budget))
		})
	}
}

func percentile(samples []time.Duration, p float64) time.Duration {
	sorted := append([]time.Duration(nil), samples...)
	sort.Slice(sorted, func(i, j int) bool { return sorted[i] < sorted[j] })
	idx := int(float64(len(sorted)-1) * p)
	return sorted[idx]
}
