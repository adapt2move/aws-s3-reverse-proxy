//go:build e2e

package e2e

import (
	"bytes"
	"fmt"
	"io"
	"net/http"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/aws/aws-sdk-go/service/s3/s3manager"
)

// setup loads the deployment under test and puts the bucket into a known
// state for the tenants the suite uses.
func setup(t *testing.T) Env {
	t.Helper()
	env := LoadEnv(t)
	setupBucket(t, env)
	resetTenant(t, env, env.TenantA)
	resetTenant(t, env, env.TenantB)
	return env
}

// The everyday path, end to end through a real S3 SDK and a real object
// store — and, crucially, a check of where the bytes actually landed. A
// client-side assertion alone cannot tell a correctly-scoped proxy from one
// that answers well while writing to the wrong key.
func TestObjectLifecycle(t *testing.T) {
	env := setup(t)
	if env.ReadOnly {
		t.Skip("this deployment runs with the mutation kill switch on")
	}
	client := env.Client(t, env.TenantA, env.WriteLevel)
	key := "datasets/lifecycle/report.csv"
	payload := []byte("region,total\neu-central-1,42\n")

	requireNoError(t, putObject(t, client, env.Bucket, key, payload), "put")

	// The object is where the tenant prefix says it should be, and nowhere
	// else in the bucket.
	want := env.UpstreamKey(env.TenantA, key)
	if got := upstreamKeys(t, env, env.TenantA); !contains(got, want) {
		t.Fatalf("object did not land at %q; bucket holds %v", want, got)
	}

	got, err := getObject(t, client, env.Bucket, key)
	requireNoError(t, err, "get")
	if !bytes.Equal(payload, got) {
		t.Fatalf("read back %q, want %q", got, payload)
	}

	head, err := client.HeadObject(&s3.HeadObjectInput{
		Bucket: aws.String(env.Bucket), Key: aws.String(key),
	})
	requireNoError(t, err, "head")
	if aws.Int64Value(head.ContentLength) != int64(len(payload)) {
		t.Fatalf("head reports %d bytes, want %d", aws.Int64Value(head.ContentLength), len(payload))
	}

	// The listing a client sees never carries the prefix the proxy injected
	// — a key read out of a listing has to be usable in the next request.
	list, err := client.ListObjectsV2(&s3.ListObjectsV2Input{
		Bucket: aws.String(env.Bucket), Prefix: aws.String("datasets/"),
	})
	requireNoError(t, err, "list")
	var listed []string
	for _, obj := range list.Contents {
		listed = append(listed, aws.StringValue(obj.Key))
	}
	if !contains(listed, key) {
		t.Fatalf("listing did not contain %q, got %v", key, listed)
	}
	for _, k := range listed {
		if strings.Contains(k, env.TenantA) {
			t.Fatalf("listing leaked the tenant prefix: %q", k)
		}
	}

	_, err = client.DeleteObject(&s3.DeleteObjectInput{
		Bucket: aws.String(env.Bucket), Key: aws.String(key),
	})
	requireNoError(t, err, "delete")
	if got := upstreamKeys(t, env, env.TenantA); contains(got, want) {
		t.Fatalf("object still in the bucket after delete: %v", got)
	}
}

// Two tenants asking for the same key get their own object, because the
// prefix that separates them is injected rather than sent.
func TestTenantIsolation(t *testing.T) {
	env := setup(t)
	if env.ReadOnly {
		t.Skip("this deployment runs with the mutation kill switch on")
	}
	key := "datasets/shared/name-collision.txt"

	a := env.Client(t, env.TenantA, env.WriteLevel)
	b := env.Client(t, env.TenantB, env.WriteLevel)
	requireNoError(t, putObject(t, a, env.Bucket, key, []byte("belongs to A")), "A put")
	requireNoError(t, putObject(t, b, env.Bucket, key, []byte("belongs to B")), "B put")

	gotA, err := getObject(t, a, env.Bucket, key)
	requireNoError(t, err, "A get")
	gotB, err := getObject(t, b, env.Bucket, key)
	requireNoError(t, err, "B get")
	if string(gotA) != "belongs to A" || string(gotB) != "belongs to B" {
		t.Fatalf("tenants read each other's object: A=%q B=%q", gotA, gotB)
	}

	// Two distinct objects really exist upstream.
	if want := env.UpstreamKey(env.TenantA, key); !contains(upstreamKeys(t, env, env.TenantA), want) {
		t.Fatalf("A's object is not at %q", want)
	}
	if want := env.UpstreamKey(env.TenantB, key); !contains(upstreamKeys(t, env, env.TenantB), want) {
		t.Fatalf("B's object is not at %q", want)
	}

	// A listing shows a tenant only its own keys.
	list, err := a.ListObjectsV2(&s3.ListObjectsV2Input{
		Bucket: aws.String(env.Bucket), Prefix: aws.String("datasets/"),
	})
	requireNoError(t, err, "A list")
	if n := len(list.Contents); n != 1 {
		t.Fatalf("A sees %d objects under datasets/, want only its own", n)
	}
}

// Every way a client might try to name somebody else's bytes.
func TestCrossTenantEscapeAttempts(t *testing.T) {
	env := setup(t)
	client := env.Client(t, env.TenantA, env.WriteLevel)
	foreignPrefix := env.UpstreamKey(env.TenantB, "datasets/theirs.csv")

	t.Run("addressing another tenant's upstream key", func(t *testing.T) {
		_, err := client.GetObject(&s3.GetObjectInput{
			Bucket: aws.String(env.Bucket), Key: aws.String(foreignPrefix),
		})
		requireDenied(t, err, "reading a foreign upstream key")
	})

	t.Run("forged access key id", func(t *testing.T) {
		forged := env.ClientWithCredentials(t,
			env.AccessKeyID(env.TenantB, env.WriteLevel), "a-secret-the-attacker-picked")
		_, err := forged.GetObject(&s3.GetObjectInput{
			Bucket: aws.String(env.Bucket), Key: aws.String("datasets/a.csv"),
		})
		requireDenied(t, err, "a signature made with a guessed secret")
	})

	t.Run("level escalation with a lower-level secret", func(t *testing.T) {
		mixed := env.ClientWithCredentials(t,
			env.AccessKeyID(env.TenantA, env.WriteLevel),
			env.SecretAccessKey(env.TenantA, env.ReadLevel))
		err := putObject(t, mixed, env.Bucket, "datasets/escalated.csv", []byte("x"))
		requireDenied(t, err, "a write signed with the read-only secret")
	})

	// The SDK normalizes `..` out of a key before sending, so traversal has
	// to be attempted with a raw request.
	for _, raw := range []string{
		"/" + env.Bucket + "/datasets/../../etc/passwd",
		"/" + env.Bucket + "/datasets/%2e%2e/%2e%2e/escape",
		"/" + env.Bucket + "//absolute/key",
	} {
		t.Run("raw path "+raw, func(t *testing.T) {
			resp := rawSignedRequest(t, env, http.MethodGet, raw, nil, nil, env.TenantA, env.WriteLevel)
			defer resp.Body.Close()
			if resp.StatusCode == http.StatusOK {
				t.Fatalf("%s was served, expected a refusal", raw)
			}
		})
	}
}

// Levels, permissions and the nested carve-out, enforced by a running
// deployment rather than by a unit test's idea of one.
func TestPolicyEnforcement(t *testing.T) {
	env := setup(t)
	reader := env.Client(t, env.TenantA, env.ReadLevel)
	writer := env.Client(t, env.TenantA, env.WriteLevel)

	t.Run("the read level cannot write", func(t *testing.T) {
		requireDenied(t, putObject(t, reader, env.Bucket, "datasets/denied.csv", []byte("x")), "read-level put")
	})

	t.Run("a path no rule covers is denied for every level", func(t *testing.T) {
		_, err := writer.GetObject(&s3.GetObjectInput{
			Bucket: aws.String(env.Bucket), Key: aws.String("nowhere/secret.csv"),
		})
		requireDenied(t, err, "reading an uncovered path")
		requireDenied(t, putObject(t, writer, env.Bucket, "nowhere/secret.csv", []byte("x")), "writing an uncovered path")
	})

	if env.ReadOnly {
		t.Run("the kill switch refuses every mutation", func(t *testing.T) {
			requireDenied(t, putObject(t, writer, env.Bucket, "datasets/a.csv", []byte("x")), "put under --read-only")
		})
		return
	}

	t.Run("the carve-out is read-only inside a writable tree", func(t *testing.T) {
		// Seed both keys with root credentials: the point is what the proxy
		// allows a client to do to them, not how they got there.
		admin := env.Admin(t)
		for _, key := range []string{"workspaces/w1/inbox/incoming.txt", "workspaces/w1/out/report.txt"} {
			requireNoError(t, putObject(t, admin, env.Bucket, env.UpstreamKey(env.TenantA, key), []byte("seed")), "seeding "+key)
		}

		// Readable either way...
		_, err := getObject(t, writer, env.Bucket, "workspaces/w1/inbox/incoming.txt")
		requireNoError(t, err, "reading the carve-out")

		// ...writable only outside the carve-out.
		requireNoError(t, putObject(t, writer, env.Bucket, "workspaces/w1/out/report.txt", []byte("new")), "writing outside the carve-out")
		requireDenied(t, putObject(t, writer, env.Bucket, "workspaces/w1/inbox/incoming.txt", []byte("new")), "writing into the carve-out")

		_, err = writer.DeleteObject(&s3.DeleteObjectInput{
			Bucket: aws.String(env.Bucket),
			Key:    aws.String("workspaces/w1/inbox/incoming.txt"),
		})
		requireDenied(t, err, "deleting from the carve-out")
	})
}

// A real multipart upload, driven by the SDK's uploader: initiate, several
// parts, complete. The part size is below the object size on purpose, so
// this is a genuine multi-part flow and not a disguised PutObject.
func TestMultipartUpload(t *testing.T) {
	env := setup(t)
	if env.ReadOnly {
		t.Skip("this deployment runs with the mutation kill switch on")
	}
	const partSize = 5 * 1024 * 1024 // the S3 minimum
	payload := bytes.Repeat([]byte("multipart-payload-"), 800_000)
	if int64(len(payload)) <= partSize {
		t.Fatalf("payload of %d bytes would fit in one part", len(payload))
	}
	key := "datasets/multipart/big.bin"

	uploader := s3manager.NewUploaderWithClient(env.Client(t, env.TenantA, env.WriteLevel), func(u *s3manager.Uploader) {
		u.PartSize = partSize
		u.Concurrency = 3
	})
	_, err := uploader.Upload(&s3manager.UploadInput{
		Bucket: aws.String(env.Bucket),
		Key:    aws.String(key),
		Body:   bytes.NewReader(payload),
	})
	requireNoError(t, err, "multipart upload")

	if want := env.UpstreamKey(env.TenantA, key); !contains(upstreamKeys(t, env, env.TenantA), want) {
		t.Fatalf("the assembled object is not at %q", want)
	}
	got, err := getObject(t, env.Client(t, env.TenantA, env.ReadLevel), env.Bucket, key)
	requireNoError(t, err, "reading the assembled object")
	if sha256Hex(got) != sha256Hex(payload) {
		t.Fatalf("the assembled object differs from what was uploaded (%d vs %d bytes)", len(got), len(payload))
	}
}

// A read level may not start a multipart upload — every step of it is a
// write.
func TestMultipartRequiresWritePermission(t *testing.T) {
	env := setup(t)
	reader := env.Client(t, env.TenantA, env.ReadLevel)
	_, err := reader.CreateMultipartUpload(&s3.CreateMultipartUploadInput{
		Bucket: aws.String(env.Bucket), Key: aws.String("datasets/multipart/nope.bin"),
	})
	requireDenied(t, err, "initiating multipart as the read level")
}

// Pagination is where the decision to forward `continuation-token`
// untouched has to hold up. MinIO's tokens are its own business; the proxy
// hands them back and forth verbatim, and the only way to know that works is
// to page through more objects than fit in one response.
func TestPaginationAcrossContinuationTokens(t *testing.T) {
	env := setup(t)
	const objects = 250
	const pageSize = 40

	admin := env.Admin(t)
	want := make([]string, 0, objects)
	for i := 0; i < objects; i++ {
		key := fmt.Sprintf("datasets/paged/%04d.txt", i)
		want = append(want, key)
		requireNoError(t, putObject(t, admin, env.Bucket, env.UpstreamKey(env.TenantA, key), []byte("x")), "seeding "+key)
	}

	client := env.Client(t, env.TenantA, env.ReadLevel)
	var got []string
	pages := 0
	err := client.ListObjectsV2Pages(&s3.ListObjectsV2Input{
		Bucket:  aws.String(env.Bucket),
		Prefix:  aws.String("datasets/paged/"),
		MaxKeys: aws.Int64(pageSize),
	}, func(page *s3.ListObjectsV2Output, last bool) bool {
		pages++
		for _, obj := range page.Contents {
			got = append(got, aws.StringValue(obj.Key))
		}
		return true
	})
	requireNoError(t, err, "paging through the listing")

	if pages < objects/pageSize {
		t.Fatalf("only %d pages for %d objects at %d per page — pagination did not engage", pages, objects, pageSize)
	}
	sort.Strings(got)
	if len(got) != len(want) {
		t.Fatalf("paged listing returned %d keys, want %d", len(got), len(want))
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("paged listing returned %q at position %d, want %q", got[i], i, want[i])
		}
	}
}

// A batch delete is the one operation whose keys live in the body. Each is
// authorized on its own, and the refused ones come back as per-key errors
// rather than as a failure of the whole request.
func TestBatchDeletePerKeyAuthorization(t *testing.T) {
	env := setup(t)
	if env.ReadOnly {
		t.Skip("this deployment runs with the mutation kill switch on")
	}
	admin := env.Admin(t)
	allowed := []string{"datasets/batch/a.csv", "datasets/batch/b.csv"}
	refused := []string{"workspaces/w1/inbox/protected.txt", "nowhere/x.csv"}
	for _, key := range append(append([]string{}, allowed...), refused...) {
		requireNoError(t, putObject(t, admin, env.Bucket, env.UpstreamKey(env.TenantA, key), []byte("seed")), "seeding "+key)
	}

	var ids []*s3.ObjectIdentifier
	for _, key := range append(append([]string{}, allowed...), refused...) {
		ids = append(ids, &s3.ObjectIdentifier{Key: aws.String(key)})
	}
	out, err := env.Client(t, env.TenantA, env.WriteLevel).DeleteObjects(&s3.DeleteObjectsInput{
		Bucket: aws.String(env.Bucket),
		Delete: &s3.Delete{Objects: ids},
	})
	requireNoError(t, err, "batch delete")

	var deleted, errored []string
	for _, d := range out.Deleted {
		deleted = append(deleted, aws.StringValue(d.Key))
	}
	for _, e := range out.Errors {
		if code := aws.StringValue(e.Code); code != "AccessDenied" {
			t.Fatalf("refused key %q came back as %q, want AccessDenied", aws.StringValue(e.Key), code)
		}
		errored = append(errored, aws.StringValue(e.Key))
	}
	sort.Strings(deleted)
	sort.Strings(errored)
	sort.Strings(allowed)
	sort.Strings(refused)
	if strings.Join(deleted, ",") != strings.Join(allowed, ",") {
		t.Fatalf("deleted %v, want %v", deleted, allowed)
	}
	if strings.Join(errored, ",") != strings.Join(refused, ",") {
		t.Fatalf("refused %v, want %v", errored, refused)
	}

	// And the refused keys really are still there.
	remaining := upstreamKeys(t, env, env.TenantA)
	for _, key := range refused {
		if !contains(remaining, env.UpstreamKey(env.TenantA, key)) {
			t.Fatalf("refused key %q was deleted anyway", key)
		}
	}
	for _, key := range allowed {
		if contains(remaining, env.UpstreamKey(env.TenantA, key)) {
			t.Fatalf("allowed key %q survived the batch delete", key)
		}
	}
}

// An aws-chunked body — what the AWS JS SDK v3 sends by default — has to
// arrive at the object store as the object, not as the framing around it.
// The Go SDK does not produce this shape, so the request is framed by hand.
func TestAwsChunkedUpload(t *testing.T) {
	env := setup(t)
	if env.ReadOnly {
		t.Skip("this deployment runs with the mutation kill switch on")
	}
	payload := bytes.Repeat([]byte("chunked-object-content!"), 4096)
	key := "datasets/chunked/object.bin"

	framed := fmt.Sprintf("%x\r\n%s\r\n0\r\n\r\n", len(payload), payload)
	headers := http.Header{
		"X-Amz-Content-Sha256":         {"STREAMING-UNSIGNED-PAYLOAD-TRAILER"},
		"X-Amz-Decoded-Content-Length": {fmt.Sprint(len(payload))},
		"Content-Encoding":             {"aws-chunked"},
	}
	resp := rawSignedRequest(t, env, http.MethodPut, "/"+env.Bucket+"/"+key,
		[]byte(framed), headers, env.TenantA, env.WriteLevel)
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		t.Fatalf("aws-chunked upload returned %d: %s", resp.StatusCode, body)
	}

	// Read it back through MinIO directly: what matters is what was stored.
	stored, err := getObject(t, env.Admin(t), env.Bucket, env.UpstreamKey(env.TenantA, key))
	requireNoError(t, err, "reading the stored object")
	if !bytes.Equal(stored, payload) {
		t.Fatalf("the object store holds %d bytes, want %d — the chunk framing was not decoded",
			len(stored), len(payload))
	}
}

// Everything the proxy refuses to implement, refused against a real
// deployment holding real upstream credentials.
func TestUnsupportedOperationsAreRefused(t *testing.T) {
	env := setup(t)
	client := env.Client(t, env.TenantA, env.WriteLevel)

	t.Run("copy object", func(t *testing.T) {
		_, err := client.CopyObject(&s3.CopyObjectInput{
			Bucket:     aws.String(env.Bucket),
			Key:        aws.String("datasets/copy/dst.csv"),
			CopySource: aws.String(env.Bucket + "/datasets/copy/src.csv"),
		})
		requireDenied(t, err, "copy object")
	})

	t.Run("bucket administration", func(t *testing.T) {
		_, err := client.GetBucketPolicy(&s3.GetBucketPolicyInput{Bucket: aws.String(env.Bucket)})
		requireDenied(t, err, "get bucket policy")
		_, err = client.GetBucketVersioning(&s3.GetBucketVersioningInput{Bucket: aws.String(env.Bucket)})
		requireDenied(t, err, "get bucket versioning")
		_, err = client.CreateBucket(&s3.CreateBucketInput{Bucket: aws.String("a-brand-new-bucket")})
		requireDenied(t, err, "create bucket")
		_, err = client.DeleteBucket(&s3.DeleteBucketInput{Bucket: aws.String(env.Bucket)})
		requireDenied(t, err, "delete bucket")
		_, err = client.ListBuckets(&s3.ListBucketsInput{})
		requireDenied(t, err, "list buckets")
	})

	t.Run("presigned url", func(t *testing.T) {
		req, _ := client.GetObjectRequest(&s3.GetObjectInput{
			Bucket: aws.String(env.Bucket), Key: aws.String("datasets/a.csv"),
		})
		url, err := req.Presign(15 * time.Minute)
		requireNoError(t, err, "presigning")
		resp, err := http.Get(url)
		requireNoError(t, err, "fetching the presigned url")
		defer resp.Body.Close()
		if resp.StatusCode != http.StatusForbidden {
			t.Fatalf("a presigned URL returned %d, want 403", resp.StatusCode)
		}
	})

	t.Run("anonymous request", func(t *testing.T) {
		resp, err := http.Get(env.objectURL("datasets/a.csv"))
		requireNoError(t, err, "anonymous request")
		defer resp.Body.Close()
		if resp.StatusCode != http.StatusForbidden {
			t.Fatalf("an unsigned request returned %d, want 403", resp.StatusCode)
		}
	})
}

// Request headers are a whitelist, and the two halves of that have to be
// checked against a real object store: what a tenant may set has to survive
// the re-signing and come back on the read, and what it may not set has to be
// refused before the store ever sees it.
//
// The first half is what a stub upstream cannot verify — user metadata only
// round-trips if the header was copied onto the upstream request *before* it
// was signed. Real AWS S3 would answer SignatureDoesNotMatch otherwise; MinIO
// would accept it, so the assertion here is the round trip itself.
func TestRequestHeaderWhitelist(t *testing.T) {
	env := setup(t)
	if env.ReadOnly {
		t.Skip("this deployment runs with the mutation kill switch on")
	}
	client := env.Client(t, env.TenantA, env.WriteLevel)
	key := "datasets/headers/object.csv"

	t.Run("metadata a tenant may set survives the round trip", func(t *testing.T) {
		_, err := client.PutObject(&s3.PutObjectInput{
			Bucket:             aws.String(env.Bucket),
			Key:                aws.String(key),
			Body:               bytes.NewReader([]byte("column,value\n1,2\n")),
			ContentType:        aws.String("text/csv"),
			CacheControl:       aws.String("max-age=60"),
			ContentDisposition: aws.String(`attachment; filename="object.csv"`),
			Metadata:           map[string]*string{"Owner": aws.String("team-a"), "Pipeline": aws.String("nightly")},
		})
		requireNoError(t, err, "put with metadata")

		head, err := client.HeadObject(&s3.HeadObjectInput{
			Bucket: aws.String(env.Bucket), Key: aws.String(key),
		})
		requireNoError(t, err, "head")
		if got := aws.StringValue(head.ContentType); got != "text/csv" {
			t.Errorf("Content-Type = %q, want text/csv", got)
		}
		if got := aws.StringValue(head.CacheControl); got != "max-age=60" {
			t.Errorf("Cache-Control = %q, want max-age=60", got)
		}
		if got := aws.StringValue(head.Metadata["Owner"]); got != "team-a" {
			t.Errorf("x-amz-meta-owner = %q, want team-a", got)
		}
		if got := aws.StringValue(head.Metadata["Pipeline"]); got != "nightly" {
			t.Errorf("x-amz-meta-pipeline = %q, want nightly", got)
		}
	})

	t.Run("a Range read is served", func(t *testing.T) {
		out, err := client.GetObject(&s3.GetObjectInput{
			Bucket: aws.String(env.Bucket), Key: aws.String(key), Range: aws.String("bytes=0-5"),
		})
		requireNoError(t, err, "ranged get")
		defer out.Body.Close()
		body, err := io.ReadAll(out.Body)
		requireNoError(t, err, "reading the ranged body")
		if string(body) != "column" {
			t.Errorf("ranged read returned %q, want %q", body, "column")
		}
	})

	// Each of these would otherwise reach the object store unsigned,
	// unclassified and unauthorized. Object lock is the one that cannot be
	// undone: it can pin an object past any deletion, the operator's own
	// included.
	for _, header := range []string{
		"x-amz-acl",
		"x-amz-object-lock-mode",
		"x-amz-storage-class",
		"x-amz-tagging",
		"x-amz-server-side-encryption",
		"x-amz-website-redirect-location",
	} {
		t.Run("refused: "+header, func(t *testing.T) {
			body := []byte("x")
			resp := rawSignedRequest(t, env, http.MethodPut, "/"+env.Bucket+"/datasets/headers/refused.csv",
				body, http.Header{header: {"anything"}}, env.TenantA, env.WriteLevel)
			defer resp.Body.Close()
			if resp.StatusCode != http.StatusForbidden {
				t.Fatalf("%s returned %d, want 403", header, resp.StatusCode)
			}
			for _, k := range upstreamKeys(t, env, env.TenantA) {
				if strings.HasSuffix(k, "datasets/headers/refused.csv") {
					t.Fatalf("%s reached the object store: %s", header, k)
				}
			}
		})
	}
}

func TestHealthAndMetrics(t *testing.T) {
	env := LoadEnv(t)
	if env.AdminEndpoint == "" {
		t.Skip("E2E_ADMIN_ENDPOINT is not set")
	}
	for _, path := range []string{"/healthz", "/readyz"} {
		resp, err := http.Get(env.AdminEndpoint + path)
		requireNoError(t, err, "GET "+path)
		resp.Body.Close()
		if resp.StatusCode != http.StatusOK {
			t.Fatalf("%s returned %d while serving", path, resp.StatusCode)
		}
	}

	resp, err := http.Get(env.AdminEndpoint + "/metrics")
	requireNoError(t, err, "GET /metrics")
	defer resp.Body.Close()
	body, _ := io.ReadAll(resp.Body)
	for _, metric := range []string{
		"s3proxy_authz_decisions_total",
		"s3proxy_proxied_request_duration_seconds",
		"s3proxy_policy_reloads_total",
	} {
		if !bytes.Contains(body, []byte(metric)) {
			t.Fatalf("/metrics does not expose %s", metric)
		}
	}
	// A credential must never reach a metric label.
	if bytes.Contains(body, []byte(env.Pepper)) {
		t.Fatal("/metrics leaked the derivation pepper")
	}
}
