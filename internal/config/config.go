// Package config is everything the process is told before it starts serving:
// command line flags, environment variables and mounted secrets.
//
// The split inside Options is by lifetime and sensitivity, and it is
// deliberate:
//
//   - policy     — a file, hot-reloadable, no secrets, nothing tenant-specific
//   - secrets    — environment or mounted secret only, never a flag and never
//     the policy file, so they cannot leak through `ps`, a
//     ConfigMap or a policy review
//   - deployment — flags or environment
package config

import (
	"fmt"
	"net"
	"os"
	"strings"
	"time"

	"gopkg.in/alecthomas/kingpin.v2"
)

// Options is the parsed configuration of one process.
type Options struct {
	Debug                 bool
	ListenAddr            string
	MetricsListenAddr     string
	HealthListenAddr      string
	PprofListenAddr       string
	AllowedSourceEndpoint string
	AllowedSourceSubnet   []string
	UpstreamInsecure      bool
	UpstreamEndpoint      string
	UpstreamRegion        string
	CertFile              string
	KeyFile               string
	ReadOnly              bool
	TenantMetricLabel     bool

	PolicyFile           string
	PolicyReloadInterval time.Duration

	// Cache. The whole feature is off unless CacheDir is set, and every
	// field below is ignored while it is empty.
	CacheDir           string
	CacheMaxBytes      int64
	CacheMaxEntries    int
	CacheMaxObjectSize int64
	CacheMaxAge        time.Duration
	CacheWrites        bool
	CacheRangeFills    bool
	CacheSegmentSize   int64
	CacheInlineMaxSize int64

	MaxClockSkew       time.Duration
	MaxChunkedBodySize int64
	MaxDeleteBodySize  int64
	MaxRewriteBodySize int64

	ShutdownTimeout time.Duration
	ShutdownDelay   time.Duration

	// Secrets, filled in from the environment by LoadSecrets.
	UpstreamAccessKeyID     string
	UpstreamSecretAccessKey string
	Pepper                  []byte
}

// Parse defines and parses the raw command line arguments.
func Parse() Options {
	var opts Options
	kingpin.Flag("verbose", "enable additional logging (env - VERBOSE)").Envar("VERBOSE").Short('v').BoolVar(&opts.Debug)
	kingpin.Flag("listen-addr", "address:port to listen for requests on (env - LISTEN_ADDR)").Default(":8099").Envar("LISTEN_ADDR").StringVar(&opts.ListenAddr)
	kingpin.Flag("metrics-listen-addr", "address:port to listen for Prometheus metrics on, empty to disable (env - METRICS_LISTEN_ADDR)").Default("").Envar("METRICS_LISTEN_ADDR").StringVar(&opts.MetricsListenAddr)
	kingpin.Flag("health-listen-addr", "address:port to serve /healthz and /readyz on, empty to disable (env - HEALTH_LISTEN_ADDR)").Default(":8100").Envar("HEALTH_LISTEN_ADDR").StringVar(&opts.HealthListenAddr)
	kingpin.Flag("pprof-listen-addr", "address:port to listen for pprof on, empty to disable (env - PPROF_LISTEN_ADDR)").Default("").Envar("PPROF_LISTEN_ADDR").StringVar(&opts.PprofListenAddr)
	kingpin.Flag("allowed-endpoint", "allowed endpoint (Host header) to accept for incoming requests (env - ALLOWED_ENDPOINT)").Envar("ALLOWED_ENDPOINT").Required().PlaceHolder("my.host.example.com:8099").StringVar(&opts.AllowedSourceEndpoint)
	kingpin.Flag("allowed-source-subnet", "allowed source IP addresses with netmask (env - ALLOWED_SOURCE_SUBNET)").Default("127.0.0.1/32").Envar("ALLOWED_SOURCE_SUBNET").StringsVar(&opts.AllowedSourceSubnet)
	kingpin.Flag("policy-file", "path to the policy file that defines identity, levels and the ordered rule list (env - POLICY_FILE)").Envar("POLICY_FILE").Required().PlaceHolder("/etc/s3proxy/policy.yaml").StringVar(&opts.PolicyFile)
	kingpin.Flag("policy-reload-interval", "how often to re-read the policy file, 0 to disable (SIGHUP always reloads) (env - POLICY_RELOAD_INTERVAL)").Envar("POLICY_RELOAD_INTERVAL").Default("30s").DurationVar(&opts.PolicyReloadInterval)
	kingpin.Flag("upstream-insecure", "use insecure HTTP for upstream connections when the endpoint carries no scheme (env - UPSTREAM_INSECURE)").Envar("UPSTREAM_INSECURE").BoolVar(&opts.UpstreamInsecure)
	kingpin.Flag("upstream-endpoint", "S3 endpoint for upstream connections; an http:// or https:// prefix selects the scheme (env - UPSTREAM_ENDPOINT)").Envar("UPSTREAM_ENDPOINT").PlaceHolder("http://minio.storage.svc:9000").StringVar(&opts.UpstreamEndpoint)
	// One knob, not two. The region a request is signed for upstream has
	// nothing to do with the region scope the client signed with — that one
	// is the client's business and is only ever used to verify its
	// signature. AWS_REGION is still honoured as the default so existing
	// deployments keep working.
	kingpin.Flag("upstream-region", "region to sign upstream requests for, and to auto-detect the AWS S3 endpoint from (env - UPSTREAM_REGION, falling back to AWS_REGION)").Envar("UPSTREAM_REGION").Default(envOr("AWS_REGION", "eu-central-1")).StringVar(&opts.UpstreamRegion)
	kingpin.Flag("cert-file", "path to the certificate file (env - CERT_FILE)").Envar("CERT_FILE").Default("").StringVar(&opts.CertFile)
	kingpin.Flag("key-file", "path to the private key file (env - KEY_FILE)").Envar("KEY_FILE").Default("").StringVar(&opts.KeyFile)
	kingpin.Flag("read-only", "reject every mutating request regardless of policy (env - READ_ONLY)").Envar("READ_ONLY").Default("false").BoolVar(&opts.ReadOnly)
	kingpin.Flag("metrics-tenant-label", "add the tenant id as a Prometheus label; tenant count is unbounded, so consider the cardinality first (env - METRICS_TENANT_LABEL)").Envar("METRICS_TENANT_LABEL").Default("false").BoolVar(&opts.TenantMetricLabel)
	// Caching. One flag turns it on and the rest tune it, so a deployment
	// that does not want a cache carries none of this.
	kingpin.Flag("cache-dir", "directory to cache object reads in; empty disables caching entirely (env - CACHE_DIR)").Envar("CACHE_DIR").PlaceHolder("/var/cache/s3proxy").StringVar(&opts.CacheDir)
	kingpin.Flag("cache-max-bytes", "how much disk the cache may use (env - CACHE_MAX_BYTES)").Envar("CACHE_MAX_BYTES").Default("1073741824").Int64Var(&opts.CacheMaxBytes)
	kingpin.Flag("cache-max-entries", "how many objects the cache may hold, 0 for no limit; each one costs about 100 bytes of memory (env - CACHE_MAX_ENTRIES)").Envar("CACHE_MAX_ENTRIES").Default("0").IntVar(&opts.CacheMaxEntries)
	kingpin.Flag("cache-max-object-size", "largest object the cache will accept; bigger ones are proxied without being stored (env - CACHE_MAX_OBJECT_SIZE)").Envar("CACHE_MAX_OBJECT_SIZE").Default("67108864").Int64Var(&opts.CacheMaxObjectSize)
	kingpin.Flag("cache-max-age", "how long a cached object may be served before it is checked against the object store; 0 disables expiry entirely, which assumes this proxy is the only writer (env - CACHE_MAX_AGE)").Envar("CACHE_MAX_AGE").Default("10m").DurationVar(&opts.CacheMaxAge)
	kingpin.Flag("cache-writes", "also cache the body of an accepted upload, so writing an object leaves it cached (env - CACHE_WRITES)").Envar("CACHE_WRITES").Default("true").BoolVar(&opts.CacheWrites)
	kingpin.Flag("cache-range-fills", "fetch the whole object in the background when a ranged read misses; without it a client that only reads ranges never fills the cache (env - CACHE_RANGE_FILLS)").Envar("CACHE_RANGE_FILLS").Default("true").BoolVar(&opts.CacheRangeFills)
	kingpin.Flag("cache-segment-size", "size of the cache's append-only segment files, which is also the granularity it reclaims disk at (env - CACHE_SEGMENT_SIZE)").Envar("CACHE_SEGMENT_SIZE").Default("268435456").Int64Var(&opts.CacheSegmentSize)
	kingpin.Flag("cache-inline-max-size", "objects up to this size are appended to a shared segment; larger ones get a file of their own (env - CACHE_INLINE_MAX_SIZE)").Envar("CACHE_INLINE_MAX_SIZE").Default("1048576").Int64Var(&opts.CacheInlineMaxSize)

	kingpin.Flag("max-clock-skew", "how far X-Amz-Date may be from this clock in either direction (env - MAX_CLOCK_SKEW)").Envar("MAX_CLOCK_SKEW").Default("15m").DurationVar(&opts.MaxClockSkew)
	kingpin.Flag("max-chunked-body-size", "cap on an aws-chunked body that has to be buffered to recover its checksum trailer (env - MAX_CHUNKED_BODY_SIZE)").Envar("MAX_CHUNKED_BODY_SIZE").Default("67108864").Int64Var(&opts.MaxChunkedBodySize)
	kingpin.Flag("max-delete-body-size", "cap on a DeleteObjects request body (env - MAX_DELETE_BODY_SIZE)").Envar("MAX_DELETE_BODY_SIZE").Default("2097152").Int64Var(&opts.MaxDeleteBodySize)
	kingpin.Flag("max-rewrite-body-size", "cap on an XML response buffered to strip the tenant prefix (env - MAX_REWRITE_BODY_SIZE)").Envar("MAX_REWRITE_BODY_SIZE").Default("33554432").Int64Var(&opts.MaxRewriteBodySize)
	kingpin.Flag("shutdown-timeout", "how long to wait for in-flight requests to finish on shutdown (env - SHUTDOWN_TIMEOUT)").Envar("SHUTDOWN_TIMEOUT").Default("30s").DurationVar(&opts.ShutdownTimeout)
	kingpin.Flag("shutdown-delay", "how long to keep serving after /readyz starts failing, so a load balancer can stop routing first (env - SHUTDOWN_DELAY)").Envar("SHUTDOWN_DELAY").Default("0s").DurationVar(&opts.ShutdownDelay)
	kingpin.Parse()
	return opts
}

// CacheEnabled reports whether a cache was configured. One flag decides it,
// so a deployment cannot end up half-configured.
func (o Options) CacheEnabled() bool { return o.CacheDir != "" }

// envOr is the default for a flag that has an older environment variable to
// stay compatible with.
func envOr(name, fallback string) string {
	if v := os.Getenv(name); v != "" {
		return v
	}
	return fallback
}

// Where each secret may come from. Both a direct variable and a `…_FILE`
// pointing at a mounted secret are accepted; the file form is what a
// Kubernetes Secret volume gives you, and it keeps the value out of the
// process environment entirely.
const (
	envUpstreamCredentials  = "UPSTREAM_CREDENTIALS"
	envUpstreamAccessKey    = "UPSTREAM_ACCESS_KEY_ID"
	envUpstreamSecretKey    = "UPSTREAM_SECRET_ACCESS_KEY"
	envCredentialPepper     = "CREDENTIAL_PEPPER"
	minCredentialPepperSize = 16
)

// LoadSecrets reads the two secrets this proxy holds — the upstream
// credentials and the derivation pepper — from the environment or from a
// mounted file. Neither is a command line flag (they would show up in `ps`)
// and neither may appear in the policy file (it is a ConfigMap, reviewed and
// version-controlled like any other configuration).
func LoadSecrets(opts *Options) error {
	combined, err := readSecretEnv(envUpstreamCredentials)
	if err != nil {
		return err
	}
	if combined != "" {
		parts := strings.SplitN(combined, ",", 2)
		if len(parts) != 2 || parts[0] == "" || parts[1] == "" {
			return fmt.Errorf("%s must be \"AWS_ACCESS_KEY_ID,AWS_SECRET_ACCESS_KEY\"", envUpstreamCredentials)
		}
		opts.UpstreamAccessKeyID, opts.UpstreamSecretAccessKey = parts[0], parts[1]
	} else {
		if opts.UpstreamAccessKeyID, err = readSecretEnv(envUpstreamAccessKey); err != nil {
			return err
		}
		if opts.UpstreamSecretAccessKey, err = readSecretEnv(envUpstreamSecretKey); err != nil {
			return err
		}
	}
	if opts.UpstreamAccessKeyID == "" || opts.UpstreamSecretAccessKey == "" {
		return fmt.Errorf("upstream credentials are required: set %s and %s (or %s), or their _FILE variants",
			envUpstreamAccessKey, envUpstreamSecretKey, envUpstreamCredentials)
	}

	pepper, err := readSecretEnv(envCredentialPepper)
	if err != nil {
		return err
	}
	if pepper == "" {
		return fmt.Errorf("%s (or %s_FILE) is required: it keys the HMAC that derives every client secret", envCredentialPepper, envCredentialPepper)
	}
	if len(pepper) < minCredentialPepperSize {
		return fmt.Errorf("%s must be at least %d bytes", envCredentialPepper, minCredentialPepperSize)
	}
	opts.Pepper = []byte(pepper)
	return nil
}

// readSecretEnv returns NAME, or the trimmed contents of the file NAME_FILE
// points at. Trailing whitespace is stripped because `echo -n` is easy to
// forget and a stray newline silently changes every derived secret.
func readSecretEnv(name string) (string, error) {
	if v := os.Getenv(name); v != "" {
		return v, nil
	}
	path := os.Getenv(name + "_FILE")
	if path == "" {
		return "", nil
	}
	raw, err := os.ReadFile(path)
	if err != nil {
		return "", fmt.Errorf("cannot read %s_FILE: %v", name, err)
	}
	return strings.TrimSpace(string(raw)), nil
}

// ParseUpstreamEndpoint derives the upstream scheme from the endpoint, so one
// image serves an in-cluster S3-compatible backend over http and a hosted
// provider over https without a second flag to keep in sync. A bare host
// follows `insecure` instead.
func ParseUpstreamEndpoint(endpoint string, insecure bool) (scheme, host string, err error) {
	scheme = "https"
	if insecure {
		scheme = "http"
	}
	host = strings.TrimSuffix(endpoint, "/")
	for _, candidate := range []string{"http", "https"} {
		if strings.HasPrefix(host, candidate+"://") {
			return candidate, strings.TrimPrefix(host, candidate+"://"), nil
		}
	}
	if strings.Contains(host, "://") {
		return "", "", fmt.Errorf("unsupported scheme in upstream endpoint %q, use http:// or https://", endpoint)
	}
	return scheme, host, nil
}

// ParseSubnets turns the allowed-source-subnet flag values into networks.
func ParseSubnets(values []string) ([]*net.IPNet, error) {
	var out []*net.IPNet
	for _, value := range values {
		_, subnet, err := net.ParseCIDR(value)
		if err != nil {
			return nil, fmt.Errorf("invalid allowed source subnet: %v", value)
		}
		out = append(out, subnet)
	}
	return out, nil
}
