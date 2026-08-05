package proxy

import (
	"bytes"
	"encoding/json"
	"net/http"
	"strings"
	"testing"

	log "github.com/sirupsen/logrus"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// captureLog swaps the global logger for a JSON one writing into a buffer,
// so the access log can be asserted on field by field.
func captureLog(t *testing.T) *bytes.Buffer {
	t.Helper()
	var buf bytes.Buffer
	oldOut, oldLevel, oldFormatter := log.StandardLogger().Out, log.GetLevel(), log.StandardLogger().Formatter
	log.SetOutput(&buf)
	log.SetLevel(log.DebugLevel)
	log.SetFormatter(&log.JSONFormatter{})
	t.Cleanup(func() {
		log.SetOutput(oldOut)
		log.SetLevel(oldLevel)
		log.SetFormatter(oldFormatter)
	})
	return &buf
}

func lastLogEntry(t *testing.T, buf *bytes.Buffer) map[string]any {
	t.Helper()
	lines := strings.Split(strings.TrimSpace(buf.String()), "\n")
	require.NotEmpty(t, lines[len(lines)-1])
	var entry map[string]any
	require.NoError(t, json.Unmarshal([]byte(lines[len(lines)-1]), &entry))
	return entry
}

// A denial has to be explainable from the access log alone: who, at what
// level, on what, and which rule decided it.
func TestAccessLogCarriesTheDecision(t *testing.T) {
	h, _ := newTestProxy(t)

	t.Run("allowed", func(t *testing.T) {
		buf := captureLog(t)
		rec := do(t, h, clientRequest{
			method: http.MethodGet, target: "/bucket/datasets/a.csv",
			tenant: tenantA, level: "ro",
		})
		require.Equal(t, http.StatusOK, rec.Code)

		entry := lastLogEntry(t, buf)
		assert.Equal(t, tenantA, entry["tenant"])
		assert.Equal(t, "ro", entry["access_level"])
		assert.Equal(t, "allow", entry["decision"])
		assert.Equal(t, "datasets/**", entry["rule"])
		assert.Equal(t, "GetObject", entry["operation"])
		assert.Equal(t, float64(http.StatusOK), entry["status"])
	})

	t.Run("denied by a rule", func(t *testing.T) {
		buf := captureLog(t)
		rec := do(t, h, clientRequest{
			method: http.MethodPut, target: "/bucket/workspaces/w1/inbox/x.txt",
			body: []byte("x"), tenant: tenantA, level: "rws",
		})
		require.Equal(t, http.StatusForbidden, rec.Code)

		entry := lastLogEntry(t, buf)
		assert.Equal(t, "deny", entry["decision"])
		assert.Equal(t, "workspaces/*/inbox/**", entry["rule"], "the rule that refused it has to be named")
	})

	// A request no rule matched is the case an operator most needs named,
	// so the implicit deny gets a name rather than a blank field.
	t.Run("denied by the implicit deny", func(t *testing.T) {
		buf := captureLog(t)
		rec := do(t, h, clientRequest{
			method: http.MethodGet, target: "/bucket/nowhere/x.txt",
			tenant: tenantA, level: "rws",
		})
		require.Equal(t, http.StatusForbidden, rec.Code)
		assert.Equal(t, "<implicit-deny>", lastLogEntry(t, buf)["rule"])
	})

	t.Run("batch delete reports how many keys were refused", func(t *testing.T) {
		buf := captureLog(t)
		h, upstream := newTestProxy(t)
		echoDeleteResult(upstream)
		rec := do(t, h, clientRequest{
			method: http.MethodPost, target: "/bucket?delete",
			body:   deleteBatchBody("datasets/a.csv", "nowhere/b.csv", "nowhere/c.csv"),
			tenant: tenantA, level: "rws",
		})
		require.Equal(t, http.StatusOK, rec.Code)
		assert.Equal(t, float64(2), lastLogEntry(t, buf)["denied_keys"])
	})
}

// A log that carries a signature is a log that carries a credential. This
// has to hold at the most verbose level, which is where a request dump
// would otherwise be written.
func TestLogsNeverCarryCredentials(t *testing.T) {
	h, _ := newTestProxy(t, func(c *Config) { c.Debug = true })
	buf := captureLog(t)

	secret := secretFor(tenantA, "rws")
	req := clientRequest{
		method: http.MethodPut, target: "/bucket/datasets/a.csv",
		body: []byte("payload"), tenant: tenantA, level: "rws",
	}.build(t)
	authorization := req.Header.Get("Authorization")
	require.NotEmpty(t, authorization)

	rec := doRequest(t, h, req)
	require.Equal(t, http.StatusOK, rec.Code)

	// ... and again for a request that fails the signature check, which is
	// the path most tempted to dump what it compared.
	do(t, h, clientRequest{
		method: http.MethodGet, target: "/bucket/datasets/a.csv",
		tenant: tenantA, level: "rws", secret: "wrong-secret",
	})

	logged := buf.String()
	require.NotEmpty(t, logged)
	assert.NotContains(t, logged, authorization)
	assert.NotContains(t, logged, secret)
	assert.NotContains(t, logged, string(testPepper))
	assert.NotContains(t, logged, "Signature=")
	assert.NotContains(t, logged, "AWS4-HMAC-SHA256")
	assert.NotContains(t, logged, "upstream-secret")
}
