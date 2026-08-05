package main

import (
	"time"

	"github.com/prometheus/client_golang/prometheus"
	log "github.com/sirupsen/logrus"
)

// accessLogEntry is one request's story: who asked, for what, and what the
// policy decided. It is filled in as the request travels through
// Handler.prepare and emitted exactly once, whether the request was served
// or refused.
//
// What is deliberately absent: the Authorization header, the signature, the
// access-key id, the derived secret and the pepper. None of them is ever
// logged, at any verbosity — a log that carries a signature is a log that
// carries a credential.
type accessLogEntry struct {
	method     string
	path       string
	operation  string
	tenant     string
	level      string
	rule       string
	key        string
	allowed    bool
	reason     string
	deniedKeys int
}

func (e *accessLogEntry) decision() string {
	if e.allowed {
		return "allow"
	}
	return "deny"
}

// matchedRule renders the rule that decided the request. An empty pattern
// means no rule matched and the implicit deny at the end of the list
// applied — which is exactly the case an operator needs named, so it gets a
// name rather than a blank field.
func (e *accessLogEntry) matchedRule() string {
	if e.rule == "" {
		return "<implicit-deny>"
	}
	return e.rule
}

func (h *Handler) finishAccessLog(entry *accessLogEntry, rec *statusRecorder, start time.Time) {
	duration := time.Since(start)
	fields := log.Fields{
		"tenant": entry.tenant,
		// `access_level`, not `level`: logrus already owns `level` for the
		// severity of the line itself, and a collision there is silently
		// renamed rather than reported.
		"access_level": entry.level,
		"operation":    entry.operation,
		"method":       entry.method,
		// The path is what a request that never got classified has instead
		// of a key — a denial before classification would otherwise be
		// unattributable.
		"path":        entry.path,
		"key":         entry.key,
		"decision":    entry.decision(),
		"rule":        entry.matchedRule(),
		"status":      rec.status,
		"duration_ms": float64(duration.Nanoseconds()) / float64(time.Millisecond),
	}
	if entry.deniedKeys > 0 {
		fields["denied_keys"] = entry.deniedKeys
	}
	if entry.reason != "" {
		fields["reason"] = entry.reason
	}

	if entry.allowed {
		log.WithFields(fields).Info("request served")
	} else {
		log.WithFields(fields).Warn("request denied")
	}

	tenant := ""
	if h.TenantMetricLabel {
		tenant = entry.tenant
	}
	authzDecisions.WithLabelValues(tenant, entry.level, entry.operation, entry.decision(), entry.matchedRule()).Inc()
	if entry.deniedKeys > 0 {
		deniedBatchKeys.WithLabelValues(tenant, entry.level).Add(float64(entry.deniedKeys))
	}
	proxiedRequestDuration.WithLabelValues(entry.operation, entry.decision()).Observe(duration.Seconds())
}

// Authorization metrics carry the same four facts as the access log —
// tenant, level, decision and the matched rule — so a spike in denials can
// be attributed without reading a single log line.
//
// The tenant label is empty unless it is explicitly switched on: tenant
// count is unbounded by design (onboarding is a no-op), and an unbounded
// label is an unbounded number of time series.
var (
	authzDecisions = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "s3proxy_authz_decisions_total",
			Help: "Authorization decisions by level, operation, outcome and matched policy rule.",
		},
		[]string{"tenant", "level", "operation", "decision", "rule"},
	)
	deniedBatchKeys = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "s3proxy_batch_delete_denied_keys_total",
			Help: "Object keys refused inside a DeleteObjects batch.",
		},
		[]string{"tenant", "level"},
	)
	proxiedRequestDuration = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Name: "s3proxy_proxied_request_duration_seconds",
			Help: "End-to-end duration of proxied requests, by operation and decision.",
			// Buckets are tightened around the added-latency budget: the
			// coarse defaults start at 5 ms and would put every fast
			// request in one bucket.
			Buckets: []float64{0.0005, 0.001, 0.0025, 0.005, 0.01, 0.025, 0.05, 0.1, 0.5, 1, 5},
		},
		[]string{"operation", "decision"},
	)
	policyReloads = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "s3proxy_policy_reloads_total",
			Help: "Policy reload attempts by outcome (applied, rejected, unchanged).",
		},
		[]string{"outcome"},
	)
)

func init() {
	prometheus.MustRegister(authzDecisions, deniedBatchKeys, proxiedRequestDuration, policyReloads)
	// Create the reload series up front. A counter that only appears after
	// the event it counts is one an alert cannot be written against — the
	// first rejected reload would look like a gap rather than a spike.
	for _, outcome := range []string{"applied", "rejected", "unchanged"} {
		policyReloads.WithLabelValues(outcome)
	}
}
