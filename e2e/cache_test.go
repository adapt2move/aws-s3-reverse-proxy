//go:build e2e

package e2e

import (
	"bytes"
	"fmt"
	"io"
	"net/http"
	"os/exec"
	"strings"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/s3"
)

// The local cache, against a real object store.
//
// Most of what the cache must not do is already covered without a line of
// cache-specific test code: the whole suite runs against a caching variant,
// so a cache that answered a request policy would have refused, or handed one
// tenant another tenant's object, fails TestTenantIsolation and
// TestPolicyEnforcement rather than a test written to look for it.
//
// What is left is what only a real object store can show. A stub upstream
// invents its own ETags, so it cannot demonstrate that the ETag this proxy
// records for an upload is the one MinIO would report for the object
// afterwards. A stub cannot be written to behind the proxy's back, so it
// cannot show what happens when something else changes an object. And a stub
// process does not restart with its disk intact.

// cacheSetup prepares a caching deployment, and skips everything here on one
// that has no cache.
func cacheSetup(t *testing.T) Env {
	t.Helper()
	env := setup(t)
	if !env.Caches() {
		t.Skip("this deployment runs without a cache")
	}
	// The bucket was just emptied straight through MinIO, which the proxy
	// has no way to notice. Start from an empty cache too.
	purgeCache(t, env)
	return env
}

func purgeCache(t *testing.T, env Env) {
	t.Helper()
	if env.AdminEndpoint == "" {
		t.Skip("E2E_ADMIN_ENDPOINT is not set; the purge endpoint is unreachable")
	}
	resp := postPurge(t, env, env.CachePurgeToken)
	defer resp.Body.Close()
	body, _ := io.ReadAll(resp.Body)
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("POST /cache/purge returned %d: %s", resp.StatusCode, body)
	}
}

func postPurge(t *testing.T, env Env, token string) *http.Response {
	t.Helper()
	req, err := http.NewRequest(http.MethodPost, env.AdminEndpoint+"/cache/purge", nil)
	requireNoError(t, err, "building the purge request")
	if token != "" {
		req.Header.Set("Authorization", "Bearer "+token)
	}
	resp, err := http.DefaultClient.Do(req)
	requireNoError(t, err, "POST /cache/purge")
	return resp
}

// read fetches an object through the proxy as a raw request, so the test can
// see the X-Cache header the SDK gives no access to.
func read(t *testing.T, env Env, tenant, level, key string, headers http.Header) (*http.Response, []byte) {
	t.Helper()
	resp := rawSignedRequest(t, env, http.MethodGet, "/"+env.Bucket+"/"+key, nil, headers, tenant, level)
	body, err := io.ReadAll(resp.Body)
	resp.Body.Close()
	requireNoError(t, err, "reading the response body")
	return resp, body
}

// requireCache asserts what the cache made of a request. It is the one thing
// worth asserting on rather than inferring, because "the object store was not
// consulted" is otherwise invisible from the client side.
func requireCache(t *testing.T, resp *http.Response, want, what string) {
	t.Helper()
	if got := resp.Header.Get("X-Cache"); got != want {
		t.Fatalf("%s: X-Cache = %q, want %q", what, got, want)
	}
}

// putDirectly writes straight into the bucket with root credentials, so the
// proxy has no idea it happened. It is how the suite plays the part of the
// external writer the cache cannot see.
func putDirectly(t *testing.T, env Env, tenant, key string, body []byte) {
	t.Helper()
	_, err := env.Admin(t).PutObject(&s3.PutObjectInput{
		Bucket: aws.String(env.Bucket),
		Key:    aws.String(env.UpstreamKey(tenant, key)),
		Body:   aws.ReadSeekCloser(bytes.NewReader(body)),
	})
	requireNoError(t, err, "writing to the bucket behind the proxy's back")
}

func upstreamETag(t *testing.T, env Env, tenant, key string) string {
	t.Helper()
	head, err := env.Admin(t).HeadObject(&s3.HeadObjectInput{
		Bucket: aws.String(env.Bucket),
		Key:    aws.String(env.UpstreamKey(tenant, key)),
	})
	requireNoError(t, err, "heading the stored object")
	return aws.StringValue(head.ETag)
}

func TestCacheAnswersTheSecondRead(t *testing.T) {
	env := cacheSetup(t)
	key := "datasets/cache/second-read.csv"
	want := []byte("column,value\n1,2\n")

	// Placed behind the proxy's back so the cache starts cold for this key —
	// an upload through the proxy would have filled it on the way past.
	putDirectly(t, env, env.TenantA, key, want)

	first, body := read(t, env, env.TenantA, env.ReadLevel, key, nil)
	requireCache(t, first, "MISS", "a cold read")
	if !bytes.Equal(body, want) {
		t.Fatalf("first read returned %q", body)
	}

	second, body := read(t, env, env.TenantA, env.ReadLevel, key, nil)
	requireCache(t, second, "HIT", "the second read")
	if !bytes.Equal(body, want) {
		t.Fatalf("cached read returned %q, want %q", body, want)
	}
	if got, want := second.Header.Get("Content-Type"), first.Header.Get("Content-Type"); got != want {
		t.Fatalf("a replayed response reports Content-Type %q, the upstream said %q", got, want)
	}
}

// The check a stub upstream cannot make. The proxy records what an upload
// said about itself plus the ETag the object store handed back; if that
// synthesis is wrong, a later read reports an entity tag for an object that
// never had it — and every conditional request built on it is wrong too.
func TestACachedUploadReportsTheObjectStoresETag(t *testing.T) {
	env := cacheSetup(t)
	if env.ReadOnly {
		t.Skip("this deployment runs with the mutation kill switch on")
	}
	key := "datasets/cache/etag.csv"
	body := []byte("uploaded through the proxy\n")

	client := env.Client(t, env.TenantA, env.WriteLevel)
	requireNoError(t, putObject(t, client, env.Bucket, key, body), "uploading")

	// The upload should have filled the cache on its way past.
	resp, got := read(t, env, env.TenantA, env.ReadLevel, key, nil)
	requireCache(t, resp, "HIT", "a read after an upload")
	if !bytes.Equal(got, body) {
		t.Fatalf("cached upload reads back as %q", got)
	}

	stored := upstreamETag(t, env, env.TenantA, key)
	if stored == "" {
		t.Fatal("the object store reported no ETag, so there is nothing to compare")
	}
	if served := resp.Header.Get("ETag"); served != stored {
		t.Fatalf("the cache serves ETag %q for an object the store calls %q", served, stored)
	}
}

func TestCacheRevalidatesAnExpiredEntry(t *testing.T) {
	env := cacheSetup(t)
	key := "datasets/cache/revalidate.csv"
	want := []byte("unchanged all along\n")
	putDirectly(t, env, env.TenantA, key, want)

	resp, _ := read(t, env, env.TenantA, env.ReadLevel, key, nil)
	requireCache(t, resp, "MISS", "a cold read")
	resp, _ = read(t, env, env.TenantA, env.ReadLevel, key, nil)
	requireCache(t, resp, "HIT", "a warm read")

	time.Sleep(env.CacheMaxAge + time.Second)

	// The object has not changed, so the entry is confirmed rather than
	// fetched: one round trip, and the body still comes off the disk.
	resp, body := read(t, env, env.TenantA, env.ReadLevel, key, nil)
	requireCache(t, resp, "REVALIDATED", "a read past the maximum age")
	if !bytes.Equal(body, want) {
		t.Fatalf("revalidated read returned %q, want %q", body, want)
	}

	// And it is current again, so the next read does not ask a second time.
	resp, _ = read(t, env, env.TenantA, env.ReadLevel, key, nil)
	requireCache(t, resp, "HIT", "a read straight after a revalidation")
}

// The scenario the documentation warns about, played out for real: something
// other than this proxy changes an object. Nothing can detect that at the
// moment it happens — the point of the maximum age is that it bounds how long
// it goes unnoticed.
func TestAnExternalWriteIsPickedUpAfterTheMaximumAge(t *testing.T) {
	env := cacheSetup(t)
	key := "datasets/cache/external-write.csv"
	first := []byte("the version the proxy knows about\n")
	putDirectly(t, env, env.TenantA, key, first)

	resp, _ := read(t, env, env.TenantA, env.ReadLevel, key, nil)
	requireCache(t, resp, "MISS", "a cold read")
	resp, body := read(t, env, env.TenantA, env.ReadLevel, key, nil)
	requireCache(t, resp, "HIT", "a warm read")
	if !bytes.Equal(body, first) {
		t.Fatalf("warm read returned %q", body)
	}

	// Somebody else replaces it. The proxy has no way to know.
	second := []byte("written by somebody else entirely\n")
	putDirectly(t, env, env.TenantA, key, second)

	resp, body = read(t, env, env.TenantA, env.ReadLevel, key, nil)
	requireCache(t, resp, "HIT", "a read inside the staleness window")
	if !bytes.Equal(body, first) {
		t.Fatalf("inside the window the cache should still answer with what it holds, got %q", body)
	}

	// Past the maximum age the ETag no longer matches, so the object is
	// fetched rather than confirmed.
	time.Sleep(env.CacheMaxAge + time.Second)
	resp, body = read(t, env, env.TenantA, env.ReadLevel, key, nil)
	requireCache(t, resp, "STALE", "a read past the maximum age of a changed object")
	if !bytes.Equal(body, second) {
		t.Fatalf("after expiry the read returned %q, want the new version %q", body, second)
	}
}

func TestPurgeMakesTheCacheForgetEverything(t *testing.T) {
	env := cacheSetup(t)
	key := "datasets/cache/purge.csv"
	putDirectly(t, env, env.TenantA, key, []byte("contents\n"))

	read(t, env, env.TenantA, env.ReadLevel, key, nil)
	resp, _ := read(t, env, env.TenantA, env.ReadLevel, key, nil)
	requireCache(t, resp, "HIT", "a warm read")

	// The escape hatch for a write that happened outside the proxy and
	// cannot wait for the maximum age to expire.
	purgeCache(t, env)

	resp, _ = read(t, env, env.TenantA, env.ReadLevel, key, nil)
	requireCache(t, resp, "MISS", "a read after a purge")
}

// Emptying the cache in a loop is a cheap way to force full re-fetches from
// the object store, on a listener that also serves metrics. Where the
// deployment sets a token, the endpoint has to actually want it.
func TestPurgeNeedsItsToken(t *testing.T) {
	env := cacheSetup(t)
	if env.CachePurgeToken == "" {
		t.Skip("this deployment leaves the purge endpoint open")
	}
	key := "datasets/cache/purge-token.csv"
	putDirectly(t, env, env.TenantA, key, []byte("contents\n"))
	read(t, env, env.TenantA, env.ReadLevel, key, nil)
	resp, _ := read(t, env, env.TenantA, env.ReadLevel, key, nil)
	requireCache(t, resp, "HIT", "a warm read")

	for _, token := range []string{"", "not-the-token"} {
		refused := postPurge(t, env, token)
		refused.Body.Close()
		if refused.StatusCode != http.StatusUnauthorized {
			t.Fatalf("a purge with %q returned %d, want 401", token, refused.StatusCode)
		}
	}

	// And nothing was emptied by the attempts.
	resp, _ = read(t, env, env.TenantA, env.ReadLevel, key, nil)
	requireCache(t, resp, "HIT", "a read after two refused purges")
}

func TestARangedReadIsAnsweredFromTheCache(t *testing.T) {
	env := cacheSetup(t)
	key := "datasets/cache/ranged.bin"
	body := make([]byte, 4096)
	for i := range body {
		body[i] = byte('a' + i%26)
	}
	putDirectly(t, env, env.TenantA, key, body)

	read(t, env, env.TenantA, env.ReadLevel, key, nil) // warm it
	resp, got := read(t, env, env.TenantA, env.ReadLevel, key,
		http.Header{"Range": {"bytes=1000-1099"}})
	requireCache(t, resp, "HIT", "a ranged read of a cached object")
	if resp.StatusCode != http.StatusPartialContent {
		t.Fatalf("ranged read returned %d, want 206", resp.StatusCode)
	}
	if want := "bytes 1000-1099/4096"; resp.Header.Get("Content-Range") != want {
		t.Fatalf("Content-Range = %q, want %q", resp.Header.Get("Content-Range"), want)
	}
	if !bytes.Equal(got, body[1000:1100]) {
		t.Fatal("the cached range is not the same bytes the object store holds")
	}
}

// A client that reads only in ranges — DuckDB's httpfs over Parquet is the
// case in mind — would never fill the cache from what it receives. The whole
// object is fetched separately instead, and this is where that arrives.
func TestARangedMissFillsTheCacheInTheBackground(t *testing.T) {
	env := cacheSetup(t)
	key := "datasets/cache/range-fill.parquet"
	body := make([]byte, 8192)
	for i := range body {
		body[i] = byte(i % 251)
	}
	putDirectly(t, env, env.TenantA, key, body)

	resp, got := read(t, env, env.TenantA, env.ReadLevel, key,
		http.Header{"Range": {"bytes=-64"}})
	if resp.StatusCode != http.StatusPartialContent {
		t.Fatalf("cold ranged read returned %d, want 206", resp.StatusCode)
	}
	if !bytes.Equal(got, body[len(body)-64:]) {
		t.Fatal("the client's range did not come through intact")
	}

	// The fill is a background fetch, so it lands shortly afterwards rather
	// than before the response.
	deadline := time.Now().Add(10 * time.Second)
	for {
		resp, got = read(t, env, env.TenantA, env.ReadLevel, key,
			http.Header{"Range": {"bytes=0-63"}})
		if resp.Header.Get("X-Cache") == "HIT" {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("a ranged miss never filled the cache; last X-Cache was %q",
				resp.Header.Get("X-Cache"))
		}
		time.Sleep(100 * time.Millisecond)
	}
	if !bytes.Equal(got, body[0:64]) {
		t.Fatal("the range served from the filled cache is not what the object store holds")
	}
}

func TestAnObjectTooLargeForTheCacheIsStillServed(t *testing.T) {
	env := cacheSetup(t)
	if env.CacheMaxObjectSize == 0 {
		t.Skip("E2E_CACHE_MAX_OBJECT_SIZE is not set")
	}
	key := "datasets/cache/too-large.bin"
	body := make([]byte, env.CacheMaxObjectSize+(1<<20))
	for i := range body {
		body[i] = byte(i % 97)
	}
	putDirectly(t, env, env.TenantA, key, body)

	for i, what := range []string{"the first read", "the second read"} {
		resp, got := read(t, env, env.TenantA, env.ReadLevel, key, nil)
		requireCache(t, resp, "MISS", what)
		if !bytes.Equal(got, body) {
			t.Fatalf("%s (attempt %d) returned %d bytes of the wrong thing", what, i, len(got))
		}
	}
}

// Warming the cache must not change what policy decides, and a cached object
// is not a way around a level that may not read it. The unit suite asserts
// this too; here the object really is on disk and the deployment really is
// the one an operator would run.
func TestACachedObjectIsStillSubjectToPolicy(t *testing.T) {
	env := cacheSetup(t)
	// The fixture denies this path to the read level and grants it to the
	// level above, in every variant.
	key := "datasets/2026/private/secret.csv"
	secret := []byte("not for the read level\n")
	putDirectly(t, env, env.TenantA, key, secret)

	// Warm it as a level that may read it.
	resp, body := read(t, env, env.TenantA, "rws", key, nil)
	if resp.StatusCode != http.StatusOK {
		t.Skipf("this deployment does not grant rws on %s (%d)", key, resp.StatusCode)
	}
	if !bytes.Equal(body, secret) {
		t.Fatalf("the privileged read returned %q", body)
	}
	resp, _ = read(t, env, env.TenantA, "rws", key, nil)
	requireCache(t, resp, "HIT", "a warm privileged read")

	// Now ask as a level the policy refuses. The object is sitting in the
	// cache; the answer must still be 403 and must not contain it.
	resp, body = read(t, env, env.TenantA, env.ReadLevel, key, nil)
	if resp.StatusCode != http.StatusForbidden {
		t.Fatalf("a denied level got %d for a cached object, want 403", resp.StatusCode)
	}
	if bytes.Contains(body, secret) {
		t.Fatal("a denied response carried the cached object")
	}
	if got := resp.Header.Get("X-Cache"); got == "HIT" {
		t.Fatal("the cache answered a request policy refused")
	}
}

// One tenant's cached object is another tenant's cache miss, because the key
// the cache stores under is the upstream object and the tenant prefix is part
// of it. TestTenantIsolation covers the same ground for a cold cache; this
// covers it for a warm one.
func TestOneTenantsWarmCacheIsAnothersMiss(t *testing.T) {
	env := cacheSetup(t)
	key := "datasets/cache/shared-name.csv"
	putDirectly(t, env, env.TenantA, key, []byte("tenant A's object\n"))
	putDirectly(t, env, env.TenantB, key, []byte("tenant B's object\n"))

	read(t, env, env.TenantA, env.ReadLevel, key, nil)
	resp, body := read(t, env, env.TenantA, env.ReadLevel, key, nil)
	requireCache(t, resp, "HIT", "tenant A's warm read")
	if !bytes.Equal(body, []byte("tenant A's object\n")) {
		t.Fatalf("tenant A read %q", body)
	}

	resp, body = read(t, env, env.TenantB, env.ReadLevel, key, nil)
	requireCache(t, resp, "MISS", "tenant B's first read of a name tenant A has cached")
	if !bytes.Equal(body, []byte("tenant B's object\n")) {
		t.Fatalf("tenant B read %q, which is not its own object", body)
	}
}

// A restart is the one part of recovery a unit test can only simulate: this
// is a real process that really stopped, with a real directory of segments
// and blob files left behind for the next one to replay.
func TestTheCacheSurvivesARestart(t *testing.T) {
	env := cacheSetup(t)
	if env.CacheRestartCmd == "" {
		t.Skip("E2E_CACHE_RESTART_CMD is not set; this runner cannot restart the proxy")
	}
	key := "datasets/cache/survives-restart.csv"
	want := []byte("written before the restart\n")
	putDirectly(t, env, env.TenantA, key, want)

	read(t, env, env.TenantA, env.ReadLevel, key, nil)
	resp, _ := read(t, env, env.TenantA, env.ReadLevel, key, nil)
	requireCache(t, resp, "HIT", "a warm read before the restart")

	out, err := exec.Command("sh", "-c", env.CacheRestartCmd).CombinedOutput()
	if err != nil {
		t.Fatalf("restarting the proxy: %v\n%s", err, out)
	}
	waitReady(t, env)

	// Nothing was fetched in between, so a hit here can only have come off
	// the disk the previous process left behind.
	resp, body := read(t, env, env.TenantA, env.ReadLevel, key, nil)
	if got := resp.Header.Get("X-Cache"); got != "HIT" && got != "REVALIDATED" {
		t.Fatalf("after a restart the cache reports %q; it started cold", got)
	}
	if !bytes.Equal(body, want) {
		t.Fatalf("the recovered object reads back as %q", body)
	}
}

func waitReady(t *testing.T, env Env) {
	t.Helper()
	deadline := time.Now().Add(60 * time.Second)
	for time.Now().Before(deadline) {
		resp, err := http.Get(env.AdminEndpoint + "/readyz")
		if err == nil {
			resp.Body.Close()
			if resp.StatusCode == http.StatusOK {
				return
			}
		}
		time.Sleep(200 * time.Millisecond)
	}
	t.Fatal("the proxy never became ready again")
}

func TestCacheMetricsAreExposed(t *testing.T) {
	env := cacheSetup(t)
	key := "datasets/cache/metrics.csv"
	putDirectly(t, env, env.TenantA, key, []byte("contents\n"))
	read(t, env, env.TenantA, env.ReadLevel, key, nil)
	read(t, env, env.TenantA, env.ReadLevel, key, nil)

	resp, err := http.Get(env.AdminEndpoint + "/metrics")
	requireNoError(t, err, "GET /metrics")
	defer resp.Body.Close()
	body, _ := io.ReadAll(resp.Body)
	for _, metric := range []string{
		"s3proxy_cache_lookups_total",
		"s3proxy_cache_entries",
		"s3proxy_cache_bytes",
		"s3proxy_cache_capacity_bytes",
		"s3proxy_cache_stores_total",
	} {
		if !bytes.Contains(body, []byte(metric)) {
			t.Fatalf("/metrics does not expose %s on a caching deployment", metric)
		}
	}
	for _, result := range []string{`result="hit"`, `result="miss"`} {
		if !bytes.Contains(body, []byte(result)) {
			t.Fatalf("s3proxy_cache_lookups_total has no series with %s", result)
		}
	}
	if bytes.Contains(body, []byte(env.Pepper)) {
		t.Fatal("/metrics leaked the derivation pepper")
	}
	// A cache key carries the tenant prefix; it must not become a label.
	if bytes.Contains(body, []byte(fmt.Sprintf("%q", strings.TrimSuffix(env.UpstreamKey(env.TenantA, key), "/")))) {
		t.Fatal("/metrics exposes a cache key as a label value")
	}
}
