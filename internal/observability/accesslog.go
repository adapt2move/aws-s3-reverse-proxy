// Package observability is what an operator sees: the per-request access log,
// the Prometheus series behind it, and the health probes a rolling restart
// needs.
//
// It takes a finished request as data — an AccessLog value — rather than
// reaching into the proxy for it, so what is recorded (and, more importantly,
// what is *not*) can be read and tested in one place. What is deliberately
// absent everywhere here: the Authorization header, the signature, the
// access-key id, the derived secret and the pepper. None of them is ever
// logged, at any verbosity — a log that carries a signature is a log that
// carries a credential.
package observability

import (
	"net/http"
	"time"

	log "github.com/sirupsen/logrus"
)

// AccessLog is one request's story: who asked, for what, and what the policy
// decided. It is filled in as the request travels through the handler and
// handed to Recorder.Finish exactly once, whether the request was served or
// refused.
type AccessLog struct {
	Method    string
	Path      string
	Operation string
	Tenant    string
	Level     string
	Rule      string
	Key       string

	Allowed bool
	Reason  string

	// DeniedKeys counts the keys a batch delete refused, which is the one
	// case where a single request carries more than one decision.
	DeniedKeys int

	// Cache is what the local cache made of the request — hit, miss, stale,
	// bypass — or empty when no cache is configured. It is the field that
	// turns "the hit rate dropped" into a list of the requests that missed.
	Cache string
}

func (e *AccessLog) decision() string {
	if e.Allowed {
		return "allow"
	}
	return "deny"
}

// matchedRule renders the rule that decided the request. An empty pattern
// means no rule matched and the implicit deny at the end of the list applied
// — which is exactly the case an operator needs named, so it gets a name
// rather than a blank field.
func (e *AccessLog) matchedRule() string {
	if e.Rule == "" {
		return "<implicit-deny>"
	}
	return e.Rule
}

// Recorder emits the access log and the request metrics.
type Recorder struct {
	// TenantLabel adds the tenant id as a Prometheus label. Off by default:
	// tenant count is unbounded by design (onboarding is a no-op), and an
	// unbounded label is an unbounded number of time series. The tenant is
	// always in the log line regardless.
	TenantLabel bool
}

// Finish writes the log line and the metrics for one completed request.
func (r *Recorder) Finish(e *AccessLog, status int, duration time.Duration) {
	fields := log.Fields{
		"tenant": e.Tenant,
		// `access_level`, not `level`: logrus already owns `level` for the
		// severity of the line itself, and a collision there is silently
		// renamed rather than reported.
		"access_level": e.Level,
		"operation":    e.Operation,
		"method":       e.Method,
		// The path is what a request that never got classified has instead
		// of a key — a denial before classification would otherwise be
		// unattributable.
		"path":        e.Path,
		"key":         e.Key,
		"decision":    e.decision(),
		"rule":        e.matchedRule(),
		"status":      status,
		"duration_ms": float64(duration.Nanoseconds()) / float64(time.Millisecond),
	}
	if e.DeniedKeys > 0 {
		fields["denied_keys"] = e.DeniedKeys
	}
	if e.Reason != "" {
		fields["reason"] = e.Reason
	}
	if e.Cache != "" {
		fields["cache"] = e.Cache
	}

	if e.Allowed {
		log.WithFields(fields).Info("request served")
	} else {
		log.WithFields(fields).Warn("request denied")
	}

	tenant := ""
	if r.TenantLabel {
		tenant = e.Tenant
	}
	authzDecisions.WithLabelValues(tenant, e.Level, e.Operation, e.decision(), e.matchedRule()).Inc()
	if e.DeniedKeys > 0 {
		deniedBatchKeys.WithLabelValues(tenant, e.Level).Add(float64(e.DeniedKeys))
	}
	proxiedRequestDuration.WithLabelValues(e.Operation, e.decision()).Observe(duration.Seconds())
	if e.Cache != "" {
		cacheLookups.WithLabelValues(e.Operation, e.Cache).Inc()
	}
}

// StatusRecorder remembers the status code for the access log. It forwards
// Flush so that object downloads keep streaming through the reverse proxy
// rather than filling a buffer first.
type StatusRecorder struct {
	http.ResponseWriter
	status  int
	written bool
}

// NewStatusRecorder wraps w, assuming the 200 that a handler which never
// calls WriteHeader implies.
func NewStatusRecorder(w http.ResponseWriter) *StatusRecorder {
	return &StatusRecorder{ResponseWriter: w, status: http.StatusOK}
}

// Status is the code that was written, or 200 if none was.
func (r *StatusRecorder) Status() int { return r.status }

func (r *StatusRecorder) WriteHeader(status int) {
	if !r.written {
		r.status, r.written = status, true
	}
	r.ResponseWriter.WriteHeader(status)
}

func (r *StatusRecorder) Write(p []byte) (int, error) {
	r.written = true
	return r.ResponseWriter.Write(p)
}

func (r *StatusRecorder) Flush() {
	if f, ok := r.ResponseWriter.(http.Flusher); ok {
		f.Flush()
	}
}
