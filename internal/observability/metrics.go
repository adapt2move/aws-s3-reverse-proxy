package observability

import (
	"net/http"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

// Authorization metrics carry the same four facts as the access log —
// tenant, level, decision and the matched rule — so a spike in denials can
// be attributed without reading a single log line.
//
// The tenant label is empty unless Recorder.TenantLabel is set: tenant count
// is unbounded by design (onboarding is a no-op), and an unbounded label is
// an unbounded number of time series.
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
	cacheLookups = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "s3proxy_cache_lookups_total",
			Help: "Local cache lookups by operation and outcome (hit, miss, stale, bypass).",
		},
		[]string{"operation", "result"},
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
	prometheus.MustRegister(authzDecisions, deniedBatchKeys, proxiedRequestDuration, cacheLookups, policyReloads)
	// Create the reload series up front. A counter that only appears after
	// the event it counts is one an alert cannot be written against — the
	// first rejected reload would look like a gap rather than a spike.
	for _, outcome := range []string{"applied", "rejected", "unchanged"} {
		policyReloads.WithLabelValues(outcome)
	}
}

// RecordPolicyReload counts one reload attempt. The outcome is a plain string
// rather than the policy package's own type so that this package stays a leaf
// — the caller that owns the store is the one that names the outcome.
func RecordPolicyReload(outcome string) {
	policyReloads.WithLabelValues(outcome).Inc()
}

// CacheStats is what the cache knows about itself, reported through a
// function rather than by having the cache reach for a metrics registry. It
// is the same seam as policy.Store.OnReload: the package that owns the state
// stays testable without a Prometheus registry in the room.
type CacheStats struct {
	Entries       int
	Bytes         int64
	Capacity      int64
	Stored        uint64
	Dropped       uint64
	Invalidations uint64
	Evictions     uint64
	EvictedBytes  uint64
	WritesDropped uint64
}

// RegisterCacheStats publishes the cache's gauges and counters. It is called
// once, at startup, and only when a cache is configured — a deployment
// without one exports no cache series at all rather than a set of zeros that
// look like an idle cache.
func RegisterCacheStats(read func() CacheStats) {
	gauge := func(name, help string, value func(CacheStats) float64) prometheus.Collector {
		return prometheus.NewGaugeFunc(
			prometheus.GaugeOpts{Name: name, Help: help},
			func() float64 { return value(read()) },
		)
	}
	counter := func(name, help string, value func(CacheStats) float64) prometheus.Collector {
		return prometheus.NewCounterFunc(
			prometheus.CounterOpts{Name: name, Help: help},
			func() float64 { return value(read()) },
		)
	}
	prometheus.MustRegister(
		gauge("s3proxy_cache_entries", "Objects currently held in the local cache.",
			func(s CacheStats) float64 { return float64(s.Entries) }),
		gauge("s3proxy_cache_bytes", "Bytes the local cache occupies on disk.",
			func(s CacheStats) float64 { return float64(s.Bytes) }),
		gauge("s3proxy_cache_capacity_bytes", "Configured size limit of the local cache.",
			func(s CacheStats) float64 { return float64(s.Capacity) }),
		counter("s3proxy_cache_stores_total", "Upstream responses written into the local cache.",
			func(s CacheStats) float64 { return float64(s.Stored) }),
		counter("s3proxy_cache_store_failures_total", "Responses that could not be cached: truncated, superseded, or refused by the store.",
			func(s CacheStats) float64 { return float64(s.Dropped + s.WritesDropped) }),
		counter("s3proxy_cache_invalidations_total", "Cache keys dropped because the object behind them was written.",
			func(s CacheStats) float64 { return float64(s.Invalidations) }),
		counter("s3proxy_cache_evictions_total", "Cache files reclaimed to stay within the size limit.",
			func(s CacheStats) float64 { return float64(s.Evictions) }),
		counter("s3proxy_cache_evicted_bytes_total", "Bytes reclaimed from the local cache.",
			func(s CacheStats) float64 { return float64(s.EvictedBytes) }),
	)
}

// InstrumentHandler wraps the S3 API handler in the standard HTTP-level
// series: request count, duration, in-flight, and request/response sizes.
// These sit alongside the authorization series above, which describe the
// decision rather than the transfer.
func InstrumentHandler(handler http.Handler) http.Handler {
	counter := prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "s3proxy_api_requests_total",
			Help: "A counter for requests to the wrapped handler.",
		},
		[]string{"code", "method"},
	)
	duration := prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Name:    "s3proxy_request_duration_seconds",
			Help:    "A histogram of latencies for requests.",
			Buckets: []float64{.25, .5, 0.75, 1, 2.5, 5, 10},
		},
		[]string{"handler", "method"},
	)
	inFlight := prometheus.NewGauge(prometheus.GaugeOpts{
		Name: "s3proxy_in_flight_requests",
		Help: "A gauge of requests currently being served by the wrapped handler.",
	})
	requestSize := prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Name:    "s3proxy_request_size_bytes",
			Help:    "A histogram of request sizes.",
			Buckets: []float64{200, 500, 900, 1500, 4100, 8200, 16400, 32800},
		},
		[]string{},
	)
	responseSize := prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Name:    "s3proxy_response_size_bytes",
			Help:    "A histogram of response sizes for requests.",
			Buckets: []float64{200, 500, 900, 1500, 4100, 8200, 16400, 32800},
		},
		[]string{},
	)
	timeToWriteHeader := prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Name:    "s3proxy_time_to_write_header",
			Help:    "A histogram of time to write heaer.",
			Buckets: []float64{0.25, 0.5, 0.75, 1, 2.5, 5, 10},
		},
		[]string{},
	)

	// Register all of the metrics in the standard registry.
	prometheus.MustRegister(counter, duration, inFlight, requestSize, responseSize, timeToWriteHeader)

	return promhttp.InstrumentHandlerCounter(counter,
		promhttp.InstrumentHandlerDuration(duration.MustCurryWith(prometheus.Labels{"handler": "pull"}),
			promhttp.InstrumentHandlerInFlight(inFlight,
				promhttp.InstrumentHandlerRequestSize(requestSize,
					promhttp.InstrumentHandlerResponseSize(responseSize,
						promhttp.InstrumentHandlerTimeToWriteHeader(timeToWriteHeader, handler))))))
}

// MetricsHandler serves the Prometheus exposition endpoint.
func MetricsHandler() http.Handler { return promhttp.Handler() }
