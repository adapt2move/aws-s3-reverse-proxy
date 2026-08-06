package observability

import (
	"net/http"
	"sync/atomic"
)

// RegisterHealth wires up the two probes a rolling restart needs: /healthz
// says the process is alive, /readyz says it is still willing to take new
// work.
//
// The caller is expected to put these on an admin listener rather than the S3
// one, where a path like /healthz would be indistinguishable from a bucket
// named "healthz".
func RegisterHealth(mux *http.ServeMux, ready *atomic.Bool) {
	mux.HandleFunc("/healthz", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("ok"))
	})
	mux.HandleFunc("/readyz", func(w http.ResponseWriter, r *http.Request) {
		if !ready.Load() {
			w.WriteHeader(http.StatusServiceUnavailable)
			_, _ = w.Write([]byte("shutting down"))
			return
		}
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("ok"))
	})
}
