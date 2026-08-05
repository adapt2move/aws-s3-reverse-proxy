// Command aws-s3-reverse-proxy runs the multi-tenant S3 reverse proxy.
//
// This file is the process, not the proxy: it reads configuration, builds the
// handler out of internal/…, puts listeners in front of it, and shuts them
// down in the right order. Everything a request does lives in internal/proxy.
package main

import (
	"context"
	"errors"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"sync/atomic"
	"syscall"
	"time"

	_ "net/http/pprof"

	log "github.com/sirupsen/logrus"

	"github.com/Kriechi/aws-s3-reverse-proxy/internal/config"
	"github.com/Kriechi/aws-s3-reverse-proxy/internal/observability"
	"github.com/Kriechi/aws-s3-reverse-proxy/internal/policy"
	"github.com/Kriechi/aws-s3-reverse-proxy/internal/proxy"
	"github.com/Kriechi/aws-s3-reverse-proxy/internal/sigv4"
)

func main() {
	opts := config.Parse()
	if err := config.LoadSecrets(&opts); err != nil {
		log.Fatal(err)
	}

	log.SetLevel(log.InfoLevel)
	if opts.Debug {
		log.SetLevel(log.DebugLevel)
	}

	handler, store, err := buildProxy(opts)
	if err != nil {
		log.Fatal(err)
	}
	logStartup(opts, handler, store)

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	go store.Watch(ctx, opts.PolicyReloadInterval)
	go watchSIGHUP(ctx, store)

	// ready flips to false the moment a shutdown starts, so a load balancer
	// stops routing new requests while the ones already in flight finish.
	var ready atomic.Bool
	ready.Store(true)

	servers := listeners(opts, handler, &ready)

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

// buildProxy loads the policy and assembles the handler from it.
//
// It is the whole of startup that can fail on configuration, kept in one
// function that returns an error rather than scattered across main: a
// misconfigured deployment has to stop here, before anything is listening.
func buildProxy(opts config.Options) (*proxy.Handler, *policy.Store, error) {
	scheme, endpoint, err := config.ParseUpstreamEndpoint(opts.UpstreamEndpoint, opts.UpstreamInsecure)
	if err != nil {
		return nil, nil, err
	}
	subnets, err := config.ParseSubnets(opts.AllowedSourceSubnet)
	if err != nil {
		return nil, nil, err
	}
	if opts.UpstreamAccessKeyID == "" || opts.UpstreamSecretAccessKey == "" {
		return nil, nil, errors.New("no upstream credentials configured")
	}

	// An unparseable or contradictory policy must never start serving: there
	// is no safe default rule list, and an empty one would deny every request
	// while looking like a healthy process.
	store, err := policy.NewStore(opts.PolicyFile)
	if err != nil {
		return nil, nil, err
	}
	store.OnReload = reportReload(store)

	upstreamRegion := opts.UpstreamRegion
	if upstreamRegion == "" {
		upstreamRegion = opts.Region
	}

	handler, err := proxy.New(proxy.Config{
		Debug:                 opts.Debug,
		ReadOnly:              opts.ReadOnly,
		UpstreamScheme:        scheme,
		UpstreamEndpoint:      endpoint,
		UpstreamRegion:        upstreamRegion,
		AllowedSourceEndpoint: opts.AllowedSourceEndpoint,
		AllowedSourceSubnet:   subnets,
		Policy:                store,
		Pepper:                opts.Pepper,
		UpstreamSigner:        sigv4.NewSigner(opts.UpstreamAccessKeyID, opts.UpstreamSecretAccessKey),
		MaxClockSkew:          opts.MaxClockSkew,
		Limits: proxy.Limits{
			ChunkedBody: opts.MaxChunkedBodySize,
			DeleteBody:  opts.MaxDeleteBodySize,
			RewriteBody: opts.MaxRewriteBodySize,
		},
		TenantMetricLabel: opts.TenantMetricLabel,
	})
	if err != nil {
		return nil, nil, err
	}
	return handler, store, nil
}

// listeners starts every server this process serves on and returns them in
// shutdown order.
//
// The admin endpoints (metrics, health) never share a listener with the S3
// API: on that one, a path like /healthz is indistinguishable from a bucket
// named "healthz". They may share one with each other, which is what the
// address-keyed mux below allows.
func listeners(opts config.Options, handler http.Handler, ready *atomic.Bool) []*http.Server {
	var servers []*http.Server

	if addr := opts.PprofListenAddr; isListenAddr(addr) {
		// avoid leaking pprof to the main application http servers
		pprofMux := http.DefaultServeMux
		http.DefaultServeMux = http.NewServeMux()
		log.Infof("Listening for pprof connections on %s", addr)
		servers = append(servers, serveAsync(&http.Server{Addr: addr, Handler: pprofMux}))
	}

	adminMuxes := map[string]*http.ServeMux{}
	adminMux := func(addr string) *http.ServeMux {
		if mux, ok := adminMuxes[addr]; ok {
			return mux
		}
		mux := http.NewServeMux()
		adminMuxes[addr] = mux
		return mux
	}

	if addr := opts.MetricsListenAddr; isListenAddr(addr) {
		adminMux(addr).Handle("/metrics", observability.MetricsHandler())
		handler = observability.InstrumentHandler(handler)
		log.Infof("Serving Prometheus metrics on %s/metrics", addr)
	}
	if addr := opts.HealthListenAddr; isListenAddr(addr) {
		observability.RegisterHealth(adminMux(addr), ready)
		log.Infof("Serving /healthz and /readyz on %s", addr)
	}
	for addr, mux := range adminMuxes {
		servers = append(servers, serveAsync(&http.Server{Addr: addr, Handler: mux}))
	}

	apiServer := &http.Server{Addr: opts.ListenAddr, Handler: handler}
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
	return servers
}

func logStartup(opts config.Options, handler *proxy.Handler, store *policy.Store) {
	scheme, endpoint := handler.UpstreamAddr()
	if endpoint != "" {
		log.Infof("Sending requests to upstream AWS S3 to endpoint %s://%s.", scheme, endpoint)
	} else {
		log.Infof("Auto-detecting S3 endpoint based on region: %s://s3.{region}.amazonaws.com", scheme)
	}
	for _, subnet := range opts.AllowedSourceSubnet {
		log.Infof("Allowing connections from %v.", subnet)
	}
	log.Infof("Accepting incoming requests for this endpoint: %v", opts.AllowedSourceEndpoint)
	current := store.Current()
	log.Infof("Loaded policy from %s: levels %v, %d rules.", opts.PolicyFile, current.Levels(), len(current.Rules()))
	if opts.ReadOnly {
		log.Infof("Read-only mode: only GET/HEAD are proxied; mutating requests are rejected with 403.")
	}
}

// reportReload is the store's window onto the operator. A rejected reload is
// logged at error level with the reason — it is the only signal that the file
// on disk is not the policy being enforced.
func reportReload(store *policy.Store) func(policy.ReloadOutcome, error) {
	return func(outcome policy.ReloadOutcome, err error) {
		observability.RecordPolicyReload(string(outcome))
		switch outcome {
		case policy.ReloadRejected:
			log.WithError(err).Errorf("policy reload rejected, keeping the previously loaded policy from %s", store.Path())
		case policy.ReloadApplied:
			current := store.Current()
			log.WithFields(log.Fields{
				"levels": current.Levels(),
				"rules":  len(current.Rules()),
			}).Infof("reloaded policy from %s", store.Path())
		}
	}
}

// watchSIGHUP reloads the policy on demand, for deployments that would rather
// not wait out the poll interval.
func watchSIGHUP(ctx context.Context, store *policy.Store) {
	hup := make(chan os.Signal, 1)
	signal.Notify(hup, syscall.SIGHUP)
	defer signal.Stop(hup)
	for {
		select {
		case <-ctx.Done():
			return
		case <-hup:
			store.ReloadNow()
		}
	}
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
