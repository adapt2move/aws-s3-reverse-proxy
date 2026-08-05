package main

import (
	"context"
	"errors"
	"fmt"
	"net"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"sync/atomic"
	"syscall"
	"time"

	_ "net/http/pprof"

	"github.com/prometheus/client_golang/prometheus/promhttp"

	"github.com/aws/aws-sdk-go/aws/credentials"
	v4 "github.com/aws/aws-sdk-go/aws/signer/v4"
	log "github.com/sirupsen/logrus"
	"gopkg.in/alecthomas/kingpin.v2"
)

// Options for aws-s3-reverse-proxy command line arguments.
//
// The split is by lifetime and sensitivity, and it is deliberate:
//
//   - policy      — a file, hot-reloadable, no secrets, nothing tenant-specific
//   - secrets     — environment or mounted secret only, never a flag and never
//     the policy file, so they cannot leak through `ps`, a
//     ConfigMap or a policy review
//   - deployment  — flags or environment, as before
type Options struct {
	Debug                 bool
	ListenAddr            string
	MetricsListenAddr     string
	HealthListenAddr      string
	PprofListenAddr       string
	AllowedSourceEndpoint string
	AllowedSourceSubnet   []string
	Region                string
	UpstreamInsecure      bool
	UpstreamEndpoint      string
	UpstreamRegion        string
	CertFile              string
	KeyFile               string
	ReadOnly              bool
	TenantMetricLabel     bool

	PolicyFile           string
	PolicyReloadInterval time.Duration

	MaxClockSkew       time.Duration
	MaxChunkedBodySize int64
	MaxDeleteBodySize  int64
	MaxRewriteBodySize int64

	ShutdownTimeout time.Duration
	ShutdownDelay   time.Duration

	// Secrets, filled in from the environment by loadSecrets.
	UpstreamAccessKeyID     string
	UpstreamSecretAccessKey string
	Pepper                  []byte
}

// NewOptions defines and parses the raw command line arguments
func NewOptions() Options {
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
	kingpin.Flag("aws-region", "region to sign upstream requests for when the client does not name one (env - AWS_REGION)").Envar("AWS_REGION").Default("eu-central-1").StringVar(&opts.Region)
	kingpin.Flag("upstream-insecure", "use insecure HTTP for upstream connections when the endpoint carries no scheme (env - UPSTREAM_INSECURE)").Envar("UPSTREAM_INSECURE").BoolVar(&opts.UpstreamInsecure)
	kingpin.Flag("upstream-endpoint", "S3 endpoint for upstream connections; an http:// or https:// prefix selects the scheme (env - UPSTREAM_ENDPOINT)").Envar("UPSTREAM_ENDPOINT").PlaceHolder("http://minio.storage.svc:9000").StringVar(&opts.UpstreamEndpoint)
	kingpin.Flag("upstream-region", "region to sign upstream requests for, instead of the region from the client's request (env - UPSTREAM_REGION)").Envar("UPSTREAM_REGION").Default("").StringVar(&opts.UpstreamRegion)
	kingpin.Flag("cert-file", "path to the certificate file (env - CERT_FILE)").Envar("CERT_FILE").Default("").StringVar(&opts.CertFile)
	kingpin.Flag("key-file", "path to the private key file (env - KEY_FILE)").Envar("KEY_FILE").Default("").StringVar(&opts.KeyFile)
	kingpin.Flag("read-only", "reject every mutating request regardless of policy (env - READ_ONLY)").Envar("READ_ONLY").Default("false").BoolVar(&opts.ReadOnly)
	kingpin.Flag("metrics-tenant-label", "add the tenant id as a Prometheus label; tenant count is unbounded, so consider the cardinality first (env - METRICS_TENANT_LABEL)").Envar("METRICS_TENANT_LABEL").Default("false").BoolVar(&opts.TenantMetricLabel)
	kingpin.Flag("max-clock-skew", "how far X-Amz-Date may be from this clock in either direction (env - MAX_CLOCK_SKEW)").Envar("MAX_CLOCK_SKEW").Default("15m").DurationVar(&opts.MaxClockSkew)
	kingpin.Flag("max-chunked-body-size", "cap on an aws-chunked body that has to be buffered to recover its checksum trailer (env - MAX_CHUNKED_BODY_SIZE)").Envar("MAX_CHUNKED_BODY_SIZE").Default("67108864").Int64Var(&opts.MaxChunkedBodySize)
	kingpin.Flag("max-delete-body-size", "cap on a DeleteObjects request body (env - MAX_DELETE_BODY_SIZE)").Envar("MAX_DELETE_BODY_SIZE").Default("2097152").Int64Var(&opts.MaxDeleteBodySize)
	kingpin.Flag("max-rewrite-body-size", "cap on an XML response buffered to strip the tenant prefix (env - MAX_REWRITE_BODY_SIZE)").Envar("MAX_REWRITE_BODY_SIZE").Default("33554432").Int64Var(&opts.MaxRewriteBodySize)
	kingpin.Flag("shutdown-timeout", "how long to wait for in-flight requests to finish on shutdown (env - SHUTDOWN_TIMEOUT)").Envar("SHUTDOWN_TIMEOUT").Default("30s").DurationVar(&opts.ShutdownTimeout)
	kingpin.Flag("shutdown-delay", "how long to keep serving after /readyz starts failing, so a load balancer can stop routing first (env - SHUTDOWN_DELAY)").Envar("SHUTDOWN_DELAY").Default("0s").DurationVar(&opts.ShutdownDelay)
	kingpin.Parse()
	return opts
}

// secretEnvVars documents where each secret may come from. Both a direct
// variable and a `…_FILE` pointing at a mounted secret are accepted; the
// file form is what a Kubernetes Secret volume gives you, and it keeps the
// value out of the process environment entirely.
const (
	envUpstreamCredentials  = "UPSTREAM_CREDENTIALS"
	envUpstreamAccessKey    = "UPSTREAM_ACCESS_KEY_ID"
	envUpstreamSecretKey    = "UPSTREAM_SECRET_ACCESS_KEY"
	envCredentialPepper     = "CREDENTIAL_PEPPER"
	minCredentialPepperSize = 16
)

// loadSecrets reads the two secrets this proxy holds — the upstream
// credentials and the derivation pepper — from the environment or from a
// mounted file. Neither is a command line flag (they would show up in `ps`)
// and neither may appear in the policy file (it is a ConfigMap, reviewed
// and version-controlled like any other configuration).
func loadSecrets(opts *Options) error {
	if combined := readSecretEnv(envUpstreamCredentials); combined != "" {
		parts := strings.SplitN(combined, ",", 2)
		if len(parts) != 2 || parts[0] == "" || parts[1] == "" {
			return fmt.Errorf("%s must be \"AWS_ACCESS_KEY_ID,AWS_SECRET_ACCESS_KEY\"", envUpstreamCredentials)
		}
		opts.UpstreamAccessKeyID, opts.UpstreamSecretAccessKey = parts[0], parts[1]
	} else {
		opts.UpstreamAccessKeyID = readSecretEnv(envUpstreamAccessKey)
		opts.UpstreamSecretAccessKey = readSecretEnv(envUpstreamSecretKey)
	}
	if opts.UpstreamAccessKeyID == "" || opts.UpstreamSecretAccessKey == "" {
		return fmt.Errorf("upstream credentials are required: set %s and %s (or %s), or their _FILE variants",
			envUpstreamAccessKey, envUpstreamSecretKey, envUpstreamCredentials)
	}

	pepper := readSecretEnv(envCredentialPepper)
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
func readSecretEnv(name string) string {
	if v := os.Getenv(name); v != "" {
		return v
	}
	path := os.Getenv(name + "_FILE")
	if path == "" {
		return ""
	}
	raw, err := os.ReadFile(path)
	if err != nil {
		log.WithError(err).Fatalf("cannot read %s_FILE", name)
	}
	return strings.TrimSpace(string(raw))
}

// parseUpstreamEndpoint derives the upstream scheme from the endpoint, so
// one image serves an in-cluster S3-compatible backend over http and a
// hosted provider over https without a second flag to keep in sync. A bare
// host keeps the old behaviour and follows --upstream-insecure.
func parseUpstreamEndpoint(endpoint string, insecure bool) (scheme, host string, err error) {
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

// NewAwsS3ReverseProxy parses all options and creates a new HTTP Handler
func NewAwsS3ReverseProxy(opts Options) (*Handler, error) {
	log.SetLevel(log.InfoLevel)
	if opts.Debug {
		log.SetLevel(log.DebugLevel)
	}

	scheme, endpoint, err := parseUpstreamEndpoint(opts.UpstreamEndpoint, opts.UpstreamInsecure)
	if err != nil {
		return nil, err
	}

	var parsedAllowedSourceSubnet []*net.IPNet
	for _, sourceSubnet := range opts.AllowedSourceSubnet {
		_, subnet, err := net.ParseCIDR(sourceSubnet)
		if err != nil {
			return nil, fmt.Errorf("Invalid allowed source subnet: %v", sourceSubnet)
		}
		parsedAllowedSourceSubnet = append(parsedAllowedSourceSubnet, subnet)
	}

	// An unparseable or contradictory policy must never start serving:
	// there is no safe default rule list, and an empty one would deny every
	// request while looking like a healthy process.
	policyStore, err := NewPolicyStore(opts.PolicyFile)
	if err != nil {
		return nil, err
	}

	if len(opts.Pepper) == 0 {
		return nil, errors.New("no credential pepper configured")
	}
	if opts.UpstreamAccessKeyID == "" || opts.UpstreamSecretAccessKey == "" {
		return nil, errors.New("no upstream credentials configured")
	}

	upstreamRegion := opts.UpstreamRegion
	if upstreamRegion == "" {
		upstreamRegion = opts.Region
	}

	return NewHandler(&Handler{
		Debug:                 opts.Debug,
		ReadOnly:              opts.ReadOnly,
		UpstreamScheme:        scheme,
		UpstreamEndpoint:      endpoint,
		AllowedSourceEndpoint: opts.AllowedSourceEndpoint,
		AllowedSourceSubnet:   parsedAllowedSourceSubnet,
		Policy:                policyStore,
		Pepper:                opts.Pepper,
		UpstreamSigner: v4.NewSigner(credentials.NewStaticCredentialsFromCreds(credentials.Value{
			AccessKeyID:     opts.UpstreamAccessKeyID,
			SecretAccessKey: opts.UpstreamSecretAccessKey,
		})),
		UpstreamRegion:     upstreamRegion,
		MaxClockSkew:       opts.MaxClockSkew,
		MaxChunkedBodySize: opts.MaxChunkedBodySize,
		MaxDeleteBodySize:  opts.MaxDeleteBodySize,
		MaxRewriteBodySize: opts.MaxRewriteBodySize,
		TenantMetricLabel:  opts.TenantMetricLabel,
	}), nil
}

func main() {
	opts := NewOptions()
	if err := loadSecrets(&opts); err != nil {
		log.Fatal(err)
	}
	handler, err := NewAwsS3ReverseProxy(opts)
	if err != nil {
		log.Fatal(err)
	}

	if len(handler.UpstreamEndpoint) > 0 {
		log.Infof("Sending requests to upstream AWS S3 to endpoint %s://%s.", handler.UpstreamScheme, handler.UpstreamEndpoint)
	} else {
		log.Infof("Auto-detecting S3 endpoint based on region: %s://s3.{region}.amazonaws.com", handler.UpstreamScheme)
	}
	for _, subnet := range handler.AllowedSourceSubnet {
		log.Infof("Allowing connections from %v.", subnet)
	}
	log.Infof("Accepting incoming requests for this endpoint: %v", handler.AllowedSourceEndpoint)
	policy := handler.Policy.Current()
	log.Infof("Loaded policy from %s: levels %v, %d rules.", opts.PolicyFile, policy.Levels(), len(policy.Rules()))
	log.Infof("Signing upstream requests for region %q with the upstream credentials.", handler.UpstreamRegion)
	if handler.ReadOnly {
		log.Infof("Read-only mode: only GET/HEAD are proxied; mutating requests are rejected with 403.")
	}

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	go handler.Policy.Watch(ctx, opts.PolicyReloadInterval)
	go watchSIGHUP(ctx, handler.Policy)

	// ready flips to false the moment a shutdown starts, so a load balancer
	// stops routing new requests while the ones already in flight finish.
	var ready atomic.Bool
	ready.Store(true)

	var servers []*http.Server
	if addr := opts.PprofListenAddr; isListenAddr(addr) {
		// avoid leaking pprof to the main application http servers
		pprofMux := http.DefaultServeMux
		http.DefaultServeMux = http.NewServeMux()
		log.Infof("Listening for pprof connections on %s", addr)
		servers = append(servers, serveAsync(&http.Server{Addr: addr, Handler: pprofMux}))
	}

	// The admin endpoints (metrics, health) never share a listener with the
	// S3 API: on that one, a path like /healthz is indistinguishable from a
	// bucket named "healthz". They may share one with each other, which is
	// what the address-keyed mux below allows.
	adminMuxes := map[string]*http.ServeMux{}
	adminMux := func(addr string) *http.ServeMux {
		if mux, ok := adminMuxes[addr]; ok {
			return mux
		}
		mux := http.NewServeMux()
		adminMuxes[addr] = mux
		return mux
	}

	var wrappedHandler http.Handler = handler
	if addr := opts.MetricsListenAddr; isListenAddr(addr) {
		adminMux(addr).Handle("/metrics", promhttp.Handler())
		wrappedHandler = wrapPrometheusMetrics(handler)
		log.Infof("Serving Prometheus metrics on %s/metrics", addr)
	}
	if addr := opts.HealthListenAddr; isListenAddr(addr) {
		registerHealthEndpoints(adminMux(addr), &ready)
		log.Infof("Serving /healthz and /readyz on %s", addr)
	}
	for addr, mux := range adminMuxes {
		servers = append(servers, serveAsync(&http.Server{Addr: addr, Handler: mux}))
	}

	apiServer := &http.Server{Addr: opts.ListenAddr, Handler: wrappedHandler}
	servers = append(servers, apiServer)
	go func() {
		var err error
		if len(opts.CertFile) > 0 || len(opts.KeyFile) > 0 {
			log.Infof("Listening for secure HTTPS connections on %s", opts.ListenAddr)
			err = apiServer.ListenAndServeTLS(opts.CertFile, opts.KeyFile)
		} else {
			log.Infof("Listening for HTTP connections on %s", opts.ListenAddr)
			err = apiServer.ListenAndServe()
		}
		if err != nil && !errors.Is(err, http.ErrServerClosed) {
			log.Fatal(err)
		}
	}()

	<-ctx.Done()
	stop() // a second signal terminates immediately instead of hanging
	ready.Store(false)
	if opts.ShutdownDelay > 0 {
		log.Infof("Shutting down: /readyz now fails, waiting %s before draining.", opts.ShutdownDelay)
		time.Sleep(opts.ShutdownDelay)
	}
	log.Infof("Draining in-flight requests, up to %s.", opts.ShutdownTimeout)
	shutdownCtx, cancel := context.WithTimeout(context.Background(), opts.ShutdownTimeout)
	defer cancel()
	for _, server := range servers {
		if err := server.Shutdown(shutdownCtx); err != nil {
			log.WithError(err).Warn("server did not shut down cleanly")
		}
	}
	log.Info("Shutdown complete.")
}

// isListenAddr reports whether a flag value looks like an address:port we
// should listen on. Empty disables the endpoint.
func isListenAddr(addr string) bool {
	return len(addr) > 0 && len(strings.Split(addr, ":")) == 2
}

func serveAsync(server *http.Server) *http.Server {
	go func() {
		if err := server.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			log.WithError(err).Errorf("listener on %s stopped", server.Addr)
		}
	}()
	return server
}

// registerHealthEndpoints wires up the two probes a rolling restart needs:
// /healthz says the process is alive, /readyz says it is still willing to
// take new work. They live on the admin listener rather than the S3 one,
// where a path like /healthz would be indistinguishable from a bucket name.
func registerHealthEndpoints(mux *http.ServeMux, ready *atomic.Bool) {
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

// watchSIGHUP reloads the policy on demand, for deployments that would
// rather not wait out the poll interval.
func watchSIGHUP(ctx context.Context, store *PolicyStore) {
	hup := make(chan os.Signal, 1)
	signal.Notify(hup, syscall.SIGHUP)
	defer signal.Stop(hup)
	for {
		select {
		case <-ctx.Done():
			return
		case <-hup:
			store.reloadAndLog()
		}
	}
}
