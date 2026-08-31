//go:build e2e

package e2e

import (
	"bytes"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"net/http"
	"os"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/session"
	v4 "github.com/aws/aws-sdk-go/aws/signer/v4"
	"github.com/aws/aws-sdk-go/service/s3"
)

// Env is the deployment under test. Everything that the policy file gets to
// decide — how an access-key id is spelled, what the levels are called,
// where a tenant's objects land — arrives here from the environment rather
// than being written into the tests.
//
// That is the point of parameterising it: the suite runs unchanged against
// deployments whose policies agree on nothing, which is the only way to
// demonstrate that no tenant, level name or key prefix is baked into the
// proxy.
type Env struct {
	// Name identifies the variant in test output.
	Name string

	// ProxyEndpoint is the S3 API of the proxy under test; AdminEndpoint
	// serves its /healthz, /readyz and /metrics.
	ProxyEndpoint string
	AdminEndpoint string

	// MinIO, reached directly with root credentials. The suite uses it to
	// see what actually landed on disk — a proxy that answers correctly
	// while writing to the wrong key would pass every client-side
	// assertion.
	MinioEndpoint  string
	MinioAccessKey string
	MinioSecretKey string

	Bucket string
	Region string

	// Pepper and the three templates mirror the deployment's policy file.
	Pepper            string
	AccessKeyTemplate string // e.g. "{tenant}{level}"
	SecretTemplate    string // e.g. "{tenant}:{level}"
	KeyPrefixTemplate string // e.g. "{tenant}/"

	// The level names this deployment declares, by the role the suite needs
	// rather than by name.
	ReadLevel  string
	WriteLevel string

	// TenantA and TenantB must both match the deployment's
	// accessKeyIdPattern.
	TenantA string
	TenantB string

	// PolicyFile, when set, is a path this process can write to and the
	// proxy re-reads — the hot-reload tests need both ends of that.
	PolicyFile     string
	ReloadInterval time.Duration

	// MinioCA is a PEM file for the private CA that signed the object
	// store's certificate, when it is reached over TLS.
	MinioCA string

	// ReadOnly marks a deployment running with the mutation kill switch on.
	ReadOnly bool

	// LargeObjectSize, when non-zero, is the size of the object the
	// streaming test uploads. It is meant to be set far above the memory
	// limit of the proxy container.
	LargeObjectSize int64

	// CacheMaxAge, when non-zero, marks a deployment running with the local
	// object cache on, and says how long an entry is served before the
	// proxy checks it against the object store. The variant sets it to a
	// few seconds so expiry is testable without a long sleep.
	CacheMaxAge time.Duration

	// CacheMaxObjectSize is what that deployment refuses to cache, so a
	// test can pick a size on either side of it.
	CacheMaxObjectSize int64

	// CachePurgeToken is what the deployment requires on POST
	// /cache/purge. Empty means it accepts an unauthenticated purge.
	CachePurgeToken string

	// CacheRestartCmd, when set, restarts the proxy under test with its
	// cache directory intact. It is what lets the suite check that a cache
	// survives a restart, which is the one part of recovery a unit test can
	// only simulate.
	CacheRestartCmd string
}

// Caches reports whether this deployment has the object cache on.
func (e Env) Caches() bool { return e.CacheMaxAge > 0 }

// LoadEnv reads the deployment description, skipping the whole suite when
// no stack is configured — so `go test -tags e2e ./...` on a laptop with
// nothing running is a skip, not a wall of connection errors.
func LoadEnv(t *testing.T) Env {
	t.Helper()
	endpoint := os.Getenv("E2E_PROXY_ENDPOINT")
	if endpoint == "" {
		t.Skip("E2E_PROXY_ENDPOINT is not set; start a stack with e2e/run.sh or docker compose")
	}
	env := Env{
		Name:              envOr("E2E_NAME", "default"),
		ProxyEndpoint:     endpoint,
		AdminEndpoint:     os.Getenv("E2E_ADMIN_ENDPOINT"),
		MinioEndpoint:     mustEnv(t, "E2E_MINIO_ENDPOINT"),
		MinioAccessKey:    mustEnv(t, "E2E_MINIO_ACCESS_KEY"),
		MinioSecretKey:    mustEnv(t, "E2E_MINIO_SECRET_KEY"),
		Bucket:            envOr("E2E_BUCKET", "e2e"),
		Region:            envOr("E2E_REGION", "eu-central-1"),
		Pepper:            mustEnv(t, "E2E_PEPPER"),
		AccessKeyTemplate: envOr("E2E_ACCESS_KEY_TEMPLATE", "{tenant}{level}"),
		SecretTemplate:    envOr("E2E_SECRET_TEMPLATE", "{tenant}:{level}"),
		KeyPrefixTemplate: envOr("E2E_KEY_PREFIX_TEMPLATE", "{tenant}/"),
		ReadLevel:         envOr("E2E_READ_LEVEL", "ro"),
		WriteLevel:        envOr("E2E_WRITE_LEVEL", "rw"),
		TenantA:           mustEnv(t, "E2E_TENANT_A"),
		TenantB:           mustEnv(t, "E2E_TENANT_B"),
		MinioCA:           os.Getenv("E2E_MINIO_CA"),
		PolicyFile:        os.Getenv("E2E_POLICY_FILE"),
		ReadOnly:          os.Getenv("E2E_READ_ONLY") == "true",
	}
	if v := os.Getenv("E2E_RELOAD_INTERVAL"); v != "" {
		d, err := time.ParseDuration(v)
		if err != nil {
			t.Fatalf("E2E_RELOAD_INTERVAL: %v", err)
		}
		env.ReloadInterval = d
	}
	if v := os.Getenv("E2E_LARGE_OBJECT_SIZE"); v != "" {
		n, err := strconv.ParseInt(v, 10, 64)
		if err != nil {
			t.Fatalf("E2E_LARGE_OBJECT_SIZE: %v", err)
		}
		env.LargeObjectSize = n
	}
	if v := os.Getenv("E2E_CACHE_MAX_AGE"); v != "" {
		d, err := time.ParseDuration(v)
		if err != nil {
			t.Fatalf("E2E_CACHE_MAX_AGE: %v", err)
		}
		env.CacheMaxAge = d
	}
	if v := os.Getenv("E2E_CACHE_MAX_OBJECT_SIZE"); v != "" {
		n, err := strconv.ParseInt(v, 10, 64)
		if err != nil {
			t.Fatalf("E2E_CACHE_MAX_OBJECT_SIZE: %v", err)
		}
		env.CacheMaxObjectSize = n
	}
	env.CachePurgeToken = os.Getenv("E2E_CACHE_PURGE_TOKEN")
	env.CacheRestartCmd = os.Getenv("E2E_CACHE_RESTART_CMD")
	return env
}

func mustEnv(t *testing.T, name string) string {
	t.Helper()
	v := os.Getenv(name)
	if v == "" {
		t.Fatalf("%s must be set when E2E_PROXY_ENDPOINT is", name)
	}
	return v
}

func envOr(name, fallback string) string {
	if v := os.Getenv(name); v != "" {
		return v
	}
	return fallback
}

// render substitutes {tenant} and {level}, the same two placeholders the
// proxy's own templates use.
func render(tmpl, tenant, level string) string {
	r := strings.NewReplacer("{tenant}", tenant, "{level}", level)
	return r.Replace(tmpl)
}

// AccessKeyID and SecretAccessKey reproduce, from the client's side, the
// derivation the proxy performs. Both ends computing it independently from
// nothing but the pepper is the whole credential story — if these two
// functions and the proxy ever disagree, every request 403s.
func (e Env) AccessKeyID(tenant, level string) string {
	return render(e.AccessKeyTemplate, tenant, level)
}

func (e Env) SecretAccessKey(tenant, level string) string {
	mac := hmac.New(sha256.New, []byte(e.Pepper))
	mac.Write([]byte(render(e.SecretTemplate, tenant, level)))
	return hex.EncodeToString(mac.Sum(nil))
}

// UpstreamKey is where an object a client addresses as `key` is expected to
// land in the bucket.
func (e Env) UpstreamKey(tenant, key string) string {
	return render(e.KeyPrefixTemplate, tenant, "") + key
}

// Client returns a real AWS SDK S3 client pointed at the proxy, holding a
// derived credential. Using the SDK rather than hand-built requests is
// deliberate: it signs, retries, computes checksums and drives multipart the
// way a production client does, so the suite exercises the proxy through the
// same surface real workloads use.
func (e Env) Client(t *testing.T, tenant, level string) *s3.S3 {
	t.Helper()
	return e.clientFor(t, e.ProxyEndpoint, e.AccessKeyID(tenant, level), e.SecretAccessKey(tenant, level))
}

// ClientWithCredentials builds a client with credentials of the caller's
// choosing, for the cases where the point is that they are wrong.
func (e Env) ClientWithCredentials(t *testing.T, accessKeyID, secret string) *s3.S3 {
	t.Helper()
	return e.clientFor(t, e.ProxyEndpoint, accessKeyID, secret)
}

// Admin returns a client that talks to MinIO directly, bypassing the proxy.
// It is how the suite checks what really landed in the bucket.
func (e Env) Admin(t *testing.T) *s3.S3 {
	t.Helper()
	return e.clientFor(t, e.MinioEndpoint, e.MinioAccessKey, e.MinioSecretKey)
}

func (e Env) clientFor(t *testing.T, endpoint, accessKeyID, secret string) *s3.S3 {
	t.Helper()
	opts := session.Options{
		Config: aws.Config{
			Endpoint:    aws.String(endpoint),
			Region:      aws.String(e.Region),
			Credentials: credentials.NewStaticCredentials(accessKeyID, secret, ""),
			// The proxy addresses buckets by path; virtual-host style would
			// put the bucket in the Host header, which the endpoint
			// allow-list rejects.
			S3ForcePathStyle: aws.Bool(true),
			// A retry would re-send a request the proxy deliberately refused
			// and turn one clean 403 into three.
			MaxRetries: aws.Int(0),
			HTTPClient: &http.Client{Timeout: 120 * time.Second},
		},
	}

	// The stack's private CA has to be handed to the SDK through
	// CustomCABundle rather than through a transport of our own. The SDK
	// applies AWS_CA_BUNDLE from the environment by *overwriting*
	// TLSClientConfig.RootCAs on whatever transport it is given
	// (session.loadCustomCABundle), so a pool we set ourselves is silently
	// discarded wherever that variable happens to be set — and the failure
	// then looks like an untrusted certificate rather than a clobbered
	// setting. Passing it here takes precedence over the environment.
	//
	// Verification itself stays on: a suite that skipped it would not be
	// testing the https path it claims to.
	if e.MinioCA != "" && strings.HasPrefix(endpoint, "https://") {
		pem, err := os.ReadFile(e.MinioCA)
		if err != nil {
			t.Fatalf("reading %s: %v", e.MinioCA, err)
		}
		opts.CustomCABundle = bytes.NewReader(pem)
	}

	sess, err := session.NewSessionWithOptions(opts)
	if err != nil {
		t.Fatalf("building S3 client for %s: %v", endpoint, err)
	}
	return s3.New(sess)
}

// Signer returns a SigV4 signer for a derived credential, for the handful of
// cases the SDK cannot express — an aws-chunked body, a deliberately stale
// timestamp, a path the SDK would normalize before sending.
func (e Env) Signer(tenant, level string) *v4.Signer {
	signer := v4.NewSigner(credentials.NewStaticCredentials(
		e.AccessKeyID(tenant, level), e.SecretAccessKey(tenant, level), ""))
	// S3 does not escape the canonical URI a second time.
	signer.DisableURIPathEscaping = true
	return signer
}

// ProxyHost is the host:port a raw request has to be addressed to.
func (e Env) ProxyHost() string {
	return strings.TrimPrefix(strings.TrimPrefix(e.ProxyEndpoint, "https://"), "http://")
}

func (e Env) objectURL(key string) string {
	return fmt.Sprintf("%s/%s/%s", strings.TrimSuffix(e.ProxyEndpoint, "/"), e.Bucket, key)
}
