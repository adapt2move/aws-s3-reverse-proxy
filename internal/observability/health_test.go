package observability

import (
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"

	"github.com/stretchr/testify/assert"
)

// A rolling restart needs /readyz to start failing before the process stops
// accepting work, so the load balancer takes this replica out first.
func TestHealthEndpoints(t *testing.T) {
	var ready atomic.Bool
	ready.Store(true)
	mux := http.NewServeMux()
	RegisterHealth(mux, &ready)

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

// A handler that never writes a header still answered 200, and the access log
// has to say so.
func TestStatusRecorder(t *testing.T) {
	rec := NewStatusRecorder(httptest.NewRecorder())
	assert.Equal(t, http.StatusOK, rec.Status())

	_, _ = rec.Write([]byte("body first"))
	rec.WriteHeader(http.StatusTeapot)
	assert.Equal(t, http.StatusOK, rec.Status(), "the first status written is the one that reached the client")

	rec = NewStatusRecorder(httptest.NewRecorder())
	rec.WriteHeader(http.StatusForbidden)
	rec.WriteHeader(http.StatusOK)
	assert.Equal(t, http.StatusForbidden, rec.Status())
}
