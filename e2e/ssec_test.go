//go:build e2e

package e2e

import (
	"bytes"
	"crypto/sha256"
	"encoding/base64"
	"io"
	"net/http"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/aws/aws-sdk-go/service/s3/s3manager"
)

// Upstream SSE-C, against the real TLS MinIO.
//
// Every claim here is checked from two sides: what a client of the proxy sees,
// and what the bucket holds, read straight from MinIO with the key, with the
// wrong key and with none. A proxy that answered correctly while storing
// plaintext would pass the first and fail the second.
//
// MinIO is not a permissive stand-in for any of this. It refuses SSE-C over
// plain http, refuses to read a plaintext object with SSE-C headers, refuses
// an encrypted one without them, and evaluates a conditional write against
// the stored object — which is exactly the behaviour the legacy repair has to
// work with.

// otherSSECKey is a correctly encoded key that is not the deployment's.
var otherSSECKey = base64.StdEncoding.EncodeToString(bytes.Repeat([]byte{0x5a}, 32))

func ssecSetup(t *testing.T) Env {
	t.Helper()
	env := setup(t)
	if !env.Encrypts() {
		t.Skip("this deployment does not encrypt upstream (E2E_SSEC_KEY is not set)")
	}
	if env.Caches() {
		purgeCache(t, env)
	}
	return env
}

// putPlaintext writes an object straight into the bucket without a key — a
// legacy object, written before the deployment had one.
func putPlaintext(t *testing.T, env Env, key string, body []byte, contentType string, meta map[string]string) {
	t.Helper()
	_, err := env.RawAdmin(t).PutObject(&s3.PutObjectInput{
		Bucket:      aws.String(env.Bucket),
		Key:         aws.String(env.UpstreamKey(env.TenantA, key)),
		Body:        bytes.NewReader(body),
		ContentType: aws.String(contentType),
		Metadata:    aws.StringMap(meta),
	})
	requireNoError(t, err, "writing a plaintext object directly")
}

// encryptedAtRest reports whether an object can no longer be read without a
// key — which, on MinIO, is what "stored with SSE-C" means from outside.
func encryptedAtRest(t *testing.T, env Env, key string) bool {
	t.Helper()
	_, err := env.RawAdmin(t).HeadObject(&s3.HeadObjectInput{
		Bucket: aws.String(env.Bucket), Key: aws.String(env.UpstreamKey(env.TenantA, key)),
	})
	return statusOf(err) == http.StatusBadRequest
}

func eventually(t *testing.T, what string, timeout time.Duration, condition func() bool) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if condition() {
			return
		}
		time.Sleep(250 * time.Millisecond)
	}
	t.Fatalf("timed out after %s waiting for %s", timeout, what)
}

// noOpenUploads waits until no multipart upload is left under the tenant's
// prefix: a repair finished, one way or the other, and cleaned up after itself.
func noOpenUploads(t *testing.T, env Env, timeout time.Duration) {
	t.Helper()
	eventually(t, "the repair to finish and leave no upload behind", timeout, func() bool {
		out, err := env.RawAdmin(t).ListMultipartUploads(&s3.ListMultipartUploadsInput{
			Bucket: aws.String(env.Bucket), Prefix: aws.String(env.UpstreamKey(env.TenantA, "")),
		})
		return err == nil && len(out.Uploads) == 0
	})
}

// Runs on every variant: a client's own key is refused, with a reason, before
// anything reaches the bucket — whether or not the deployment has a key of
// its own.
func TestClientSuppliedSSECHeadersAreRefused(t *testing.T) {
	env := setup(t)
	key := "datasets/ssec/refused.csv"
	for name, header := range map[string]http.Header{
		"a full key": {
			"X-Amz-Server-Side-Encryption-Customer-Algorithm": {"AES256"},
			"X-Amz-Server-Side-Encryption-Customer-Key":       {otherSSECKey},
			"X-Amz-Server-Side-Encryption-Customer-Key-Md5":   {"bm90LWNoZWNrZWQtYW55d2F5"},
		},
		"the algorithm alone": {"X-Amz-Server-Side-Encryption-Customer-Algorithm": {"AES256"}},
		"a copy-source key":   {"X-Amz-Copy-Source-Server-Side-Encryption-Customer-Key": {otherSSECKey}},
	} {
		for _, method := range []string{http.MethodPut, http.MethodGet} {
			t.Run(name+" on "+method, func(t *testing.T) {
				var body []byte
				if method == http.MethodPut {
					body = []byte("x")
				}
				resp := rawSignedRequest(t, env, method, "/"+env.Bucket+"/"+key, body, header, env.TenantA, env.WriteLevel)
				defer resp.Body.Close()
				answer, _ := io.ReadAll(resp.Body)
				if resp.StatusCode != http.StatusBadRequest {
					t.Fatalf("returned %d, want 400: %s", resp.StatusCode, answer)
				}
				if !bytes.Contains(answer, []byte("<Code>InvalidRequest</Code>")) ||
					!bytes.Contains(answer, []byte("client-provided encryption keys are not passed through")) {
					t.Fatalf("the refusal does not say why: %s", answer)
				}
				if bytes.Contains(answer, []byte(otherSSECKey)) {
					t.Fatal("the refusal repeated the key")
				}
			})
		}
	}
	if contains(upstreamKeys(t, env, env.TenantA), env.UpstreamKey(env.TenantA, key)) {
		t.Fatal("a refused write reached the bucket")
	}
}

// A client that knows nothing about encryption writes through the proxy; the
// bucket holds the object encrypted under the deployment's key, and only that
// key reads it.
func TestSSECObjectsAreEncryptedAtRest(t *testing.T) {
	env := ssecSetup(t)
	client := env.Client(t, env.TenantA, env.WriteLevel)
	key := "datasets/ssec/at-rest.csv"
	payload := []byte("region,total\neu-central-1,42\n")
	requireNoError(t, putObject(t, client, env.Bucket, key, payload), "put through the proxy")
	upstream := env.UpstreamKey(env.TenantA, key)

	t.Run("no key cannot read it", func(t *testing.T) {
		_, err := getObject(t, env.RawAdmin(t), env.Bucket, upstream)
		if statusOf(err) != http.StatusBadRequest {
			t.Fatalf("a keyless read returned %v, want 400", err)
		}
	})
	t.Run("another key cannot read it", func(t *testing.T) {
		_, err := getObject(t, env.AdminWithKey(t, otherSSECKey), env.Bucket, upstream)
		if statusOf(err) != http.StatusForbidden {
			t.Fatalf("a read with the wrong key returned %v, want 403", err)
		}
	})
	t.Run("the deployment's key reads the original bytes", func(t *testing.T) {
		got, err := getObject(t, env.Admin(t), env.Bucket, upstream)
		requireNoError(t, err, "a read with the key")
		if !bytes.Equal(got, payload) {
			t.Fatalf("read %q, want %q", got, payload)
		}
	})
	t.Run("the proxy's clients need no key", func(t *testing.T) {
		reader := env.Client(t, env.TenantA, env.ReadLevel)
		got, err := getObject(t, reader, env.Bucket, key)
		requireNoError(t, err, "get through the proxy")
		if !bytes.Equal(got, payload) {
			t.Fatalf("read %q, want %q", got, payload)
		}
		head, err := reader.HeadObject(&s3.HeadObjectInput{Bucket: aws.String(env.Bucket), Key: aws.String(key)})
		requireNoError(t, err, "head through the proxy")
		if aws.Int64Value(head.ContentLength) != int64(len(payload)) {
			t.Fatalf("HEAD reports %d bytes", aws.Int64Value(head.ContentLength))
		}
		if head.SSECustomerAlgorithm != nil || head.SSECustomerKeyMD5 != nil {
			t.Fatal("a client response revealed the upstream encryption")
		}
		ranged, err := reader.GetObject(&s3.GetObjectInput{Bucket: aws.String(env.Bucket), Key: aws.String(key), Range: aws.String("bytes=0-5")})
		requireNoError(t, err, "ranged get through the proxy")
		defer ranged.Body.Close()
		if part, _ := io.ReadAll(ranged.Body); string(part) != "region" {
			t.Fatalf("ranged read returned %q", part)
		}
	})
}

// The proxy and a direct client holding the same key read each other's
// objects, at the tenant-prefixed location — the multipart path included.
func TestSSECDirectClientsInteroperate(t *testing.T) {
	env := ssecSetup(t)
	client := env.Client(t, env.TenantA, env.WriteLevel)

	direct := "datasets/ssec/direct.csv"
	_, err := env.Admin(t).PutObject(&s3.PutObjectInput{
		Bucket: aws.String(env.Bucket), Key: aws.String(env.UpstreamKey(env.TenantA, direct)),
		Body: bytes.NewReader([]byte("written directly")),
	})
	requireNoError(t, err, "a direct write with the key")
	got, err := getObject(t, client, env.Bucket, direct)
	requireNoError(t, err, "reading a directly written object through the proxy")
	if string(got) != "written directly" {
		t.Fatalf("read %q", got)
	}

	// Multipart through the proxy, read back directly.
	key := "datasets/ssec/multipart.bin"
	part1 := bytes.Repeat([]byte("a"), 5<<20)
	part2 := []byte("tail")
	created, err := client.CreateMultipartUpload(&s3.CreateMultipartUploadInput{Bucket: aws.String(env.Bucket), Key: aws.String(key)})
	requireNoError(t, err, "create")
	var completed []*s3.CompletedPart
	for i, body := range [][]byte{part1, part2} {
		out, err := client.UploadPart(&s3.UploadPartInput{
			Bucket: aws.String(env.Bucket), Key: aws.String(key), UploadId: created.UploadId,
			PartNumber: aws.Int64(int64(i + 1)), Body: bytes.NewReader(body),
		})
		requireNoError(t, err, "upload part")
		completed = append(completed, &s3.CompletedPart{ETag: out.ETag, PartNumber: aws.Int64(int64(i + 1))})
	}
	parts, err := client.ListParts(&s3.ListPartsInput{Bucket: aws.String(env.Bucket), Key: aws.String(key), UploadId: created.UploadId})
	requireNoError(t, err, "list parts")
	if len(parts.Parts) != 2 {
		t.Fatalf("listed %d parts", len(parts.Parts))
	}
	_, err = client.CompleteMultipartUpload(&s3.CompleteMultipartUploadInput{
		Bucket: aws.String(env.Bucket), Key: aws.String(key), UploadId: created.UploadId,
		MultipartUpload: &s3.CompletedMultipartUpload{Parts: completed},
	})
	requireNoError(t, err, "complete")
	whole, err := getObject(t, env.Admin(t), env.Bucket, env.UpstreamKey(env.TenantA, key))
	requireNoError(t, err, "reading the assembled object directly with the key")
	if !bytes.Equal(whole, append(append([]byte{}, part1...), part2...)) {
		t.Fatal("the assembled object differs")
	}
	if !encryptedAtRest(t, env, key) {
		t.Fatal("the assembled object can be read without the key")
	}

	// An aborted upload goes away, without a key.
	abandoned, err := client.CreateMultipartUpload(&s3.CreateMultipartUploadInput{Bucket: aws.String(env.Bucket), Key: aws.String("datasets/ssec/abandoned.bin")})
	requireNoError(t, err, "create")
	_, err = client.AbortMultipartUpload(&s3.AbortMultipartUploadInput{Bucket: aws.String(env.Bucket), Key: aws.String("datasets/ssec/abandoned.bin"), UploadId: abandoned.UploadId})
	requireNoError(t, err, "abort")
}

// An object encrypted under a key the deployment does not hold is an error
// through the proxy, and stays exactly as it was: no keyless read, no
// rewrite.
func TestSSECAnObjectUnderAnotherKeyIsRefused(t *testing.T) {
	env := ssecSetup(t)
	key := "datasets/ssec/foreign.csv"
	_, err := env.AdminWithKey(t, otherSSECKey).PutObject(&s3.PutObjectInput{
		Bucket: aws.String(env.Bucket), Key: aws.String(env.UpstreamKey(env.TenantA, key)),
		Body: bytes.NewReader([]byte("not yours")),
	})
	requireNoError(t, err, "writing with another key")

	_, err = getObject(t, env.Client(t, env.TenantA, env.ReadLevel), env.Bucket, key)
	if statusOf(err) != http.StatusForbidden {
		t.Fatalf("reading through the proxy returned %v, want 403", err)
	}
	time.Sleep(2 * time.Second)
	got, err := getObject(t, env.AdminWithKey(t, otherSSECKey), env.Bucket, env.UpstreamKey(env.TenantA, key))
	requireNoError(t, err, "the object is still readable with its own key")
	if string(got) != "not yours" {
		t.Fatalf("the object changed: %q", got)
	}
}

// The legacy path, end to end: a plaintext object is served on read, and then
// encrypted in place — same bytes, same content type, same metadata.
func TestSSECAPlaintextObjectIsEncryptedOnRead(t *testing.T) {
	env := ssecSetup(t)
	if env.ReadOnly {
		t.Skip("a read-only deployment does not rewrite")
	}
	key := "datasets/ssec/legacy.csv"
	payload := []byte("written,before\nthere,was-a-key\n")
	putPlaintext(t, env, key, payload, "text/csv", map[string]string{"origin": "legacy"})

	reader := env.Client(t, env.TenantA, env.ReadLevel)
	head, err := reader.HeadObject(&s3.HeadObjectInput{Bucket: aws.String(env.Bucket), Key: aws.String(key)})
	requireNoError(t, err, "HEAD of a plaintext object through the proxy")
	if aws.Int64Value(head.ContentLength) != int64(len(payload)) {
		t.Fatalf("HEAD reports %d bytes", aws.Int64Value(head.ContentLength))
	}
	got, err := getObject(t, reader, env.Bucket, key)
	requireNoError(t, err, "GET of a plaintext object through the proxy")
	if !bytes.Equal(got, payload) {
		t.Fatalf("read %q", got)
	}

	eventually(t, "the object to be encrypted at rest", 30*time.Second, func() bool { return encryptedAtRest(t, env, key) })
	noOpenUploads(t, env, 30*time.Second)

	out, err := env.Admin(t).GetObject(&s3.GetObjectInput{Bucket: aws.String(env.Bucket), Key: aws.String(env.UpstreamKey(env.TenantA, key))})
	requireNoError(t, err, "reading the repaired object with the key")
	defer out.Body.Close()
	repaired, _ := io.ReadAll(out.Body)
	if !bytes.Equal(repaired, payload) {
		t.Fatalf("the repaired object holds %q", repaired)
	}
	if aws.StringValue(out.ContentType) != "text/csv" || aws.StringValue(out.Metadata["Origin"]) != "legacy" {
		t.Fatalf("content type %q and metadata %v did not survive", aws.StringValue(out.ContentType), aws.StringValueMap(out.Metadata))
	}

	again, err := getObject(t, reader, env.Bucket, key)
	requireNoError(t, err, "reading the repaired object through the proxy")
	if !bytes.Equal(again, payload) {
		t.Fatalf("read %q", again)
	}
}

// A ranged read of a plaintext object is answered with the range — and the
// object is still repaired from the whole of it.
func TestSSECARangedReadRepairsTheWholeObject(t *testing.T) {
	env := ssecSetup(t)
	if env.ReadOnly {
		t.Skip("a read-only deployment does not rewrite")
	}
	key := "datasets/ssec/legacy-ranged.bin"
	payload := bytes.Repeat([]byte("0123456789"), 1000)
	putPlaintext(t, env, key, payload, "application/octet-stream", nil)

	out, err := env.Client(t, env.TenantA, env.ReadLevel).GetObject(&s3.GetObjectInput{
		Bucket: aws.String(env.Bucket), Key: aws.String(key), Range: aws.String("bytes=10-19"),
	})
	requireNoError(t, err, "ranged read")
	part, _ := io.ReadAll(out.Body)
	out.Body.Close()
	if string(part) != "0123456789" {
		t.Fatalf("range returned %q", part)
	}
	eventually(t, "the object to be encrypted at rest", 30*time.Second, func() bool { return encryptedAtRest(t, env, key) })
	noOpenUploads(t, env, 30*time.Second)
	whole, err := getObject(t, env.Admin(t), env.Bucket, env.UpstreamKey(env.TenantA, key))
	requireNoError(t, err, "reading the repaired object")
	if !bytes.Equal(whole, payload) {
		t.Fatalf("the repaired object is %d bytes, want %d", len(whole), len(payload))
	}
}

// A path the policy lets no level write is not rewritten either; the read
// is served all the same.
func TestSSECNoRepairUnderAReadOnlyPath(t *testing.T) {
	env := ssecSetup(t)
	key := "workspaces/w1/inbox/legacy.csv"
	putPlaintext(t, env, key, []byte("inbox"), "text/plain", nil)

	got, err := getObject(t, env.Client(t, env.TenantA, env.ReadLevel), env.Bucket, key)
	requireNoError(t, err, "reading a plaintext object under a read-only path")
	if string(got) != "inbox" {
		t.Fatalf("read %q", got)
	}
	time.Sleep(3 * time.Second)
	if encryptedAtRest(t, env, key) {
		t.Fatal("an object under a path no level may write was rewritten")
	}
}

// The two ways another writer can get in while a repair runs. Whatever the
// interleaving, the other writer's outcome is what the bucket holds
// afterwards: the repair's completion is conditional on the version it read,
// and MinIO enforces it. The object is large enough that the repair is still
// streaming when the other write lands.
func TestSSECARepairNeverOverwritesAConcurrentWrite(t *testing.T) {
	env := ssecSetup(t)
	if env.ReadOnly {
		t.Skip("a read-only deployment does not rewrite")
	}
	size := int64(48 << 20)

	t.Run("replaced", func(t *testing.T) {
		key := "datasets/ssec/race-replaced.bin"
		uploadPlaintextPattern(t, env, key, size)
		triggerRead(t, env, key)
		putPlaintext(t, env, key, []byte("replacement"), "text/plain", nil)
		noOpenUploads(t, env, 2*time.Minute)

		// The replacement is plaintext too, so a repair that had not yet
		// looked at the object when it landed may encrypt *it* — which is
		// fine. What must never happen is the old bytes coming back.
		upstream := env.UpstreamKey(env.TenantA, key)
		got, err := getObject(t, env.RawAdmin(t), env.Bucket, upstream)
		if statusOf(err) == http.StatusBadRequest {
			got, err = getObject(t, env.Admin(t), env.Bucket, upstream)
		}
		requireNoError(t, err, "reading what the bucket holds")
		if string(got) != "replacement" {
			t.Fatalf("the bucket holds %d bytes, not the replacement", len(got))
		}
	})

	t.Run("deleted", func(t *testing.T) {
		key := "datasets/ssec/race-deleted.bin"
		uploadPlaintextPattern(t, env, key, size)
		triggerRead(t, env, key)
		_, err := env.RawAdmin(t).DeleteObject(&s3.DeleteObjectInput{
			Bucket: aws.String(env.Bucket), Key: aws.String(env.UpstreamKey(env.TenantA, key)),
		})
		requireNoError(t, err, "deleting behind the proxy's back")
		noOpenUploads(t, env, 2*time.Minute)

		_, err = env.RawAdmin(t).HeadObject(&s3.HeadObjectInput{
			Bucket: aws.String(env.Bucket), Key: aws.String(env.UpstreamKey(env.TenantA, key)),
		})
		if statusOf(err) != http.StatusNotFound {
			t.Fatalf("a deleted object came back: %v", err)
		}
	})
}

// uploadPlaintextPattern writes a large plaintext object directly, streamed
// from a generator — through a multipart upload, as large legacy objects
// usually were.
func uploadPlaintextPattern(t *testing.T, env Env, key string, size int64) {
	t.Helper()
	uploader := s3manager.NewUploaderWithClient(env.RawAdmin(t), func(u *s3manager.Uploader) {
		u.PartSize = 16 << 20
		u.Concurrency = 1
	})
	_, err := uploader.Upload(&s3manager.UploadInput{
		Bucket: aws.String(env.Bucket), Key: aws.String(env.UpstreamKey(env.TenantA, key)),
		Body: newPatternReader(size),
	})
	requireNoError(t, err, "writing a large plaintext object directly")
}

// triggerRead reads the first bytes of an object through the proxy, which is
// enough to have its repair scheduled.
func triggerRead(t *testing.T, env Env, key string) {
	t.Helper()
	out, err := env.Client(t, env.TenantA, env.ReadLevel).GetObject(&s3.GetObjectInput{
		Bucket: aws.String(env.Bucket), Key: aws.String(key), Range: aws.String("bytes=0-0"),
	})
	requireNoError(t, err, "reading through the proxy")
	_, _ = io.Copy(io.Discard, out.Body)
	out.Body.Close()
	// Give the repair a moment to start streaming before the other writer
	// moves; the assertion holds either way.
	time.Sleep(200 * time.Millisecond)
}

// A plaintext object far larger than the cache takes and than the proxy
// container's memory limit is still served and still repaired: the repair
// streams it, part by part, without holding it.
func TestSSECALargePlaintextObjectIsRepairedInBoundedMemory(t *testing.T) {
	env := ssecSetup(t)
	if env.ReadOnly {
		t.Skip("a read-only deployment does not rewrite")
	}
	if env.LargeObjectSize == 0 {
		t.Skip("E2E_LARGE_OBJECT_SIZE is not set")
	}
	key := "datasets/ssec/legacy-large.bin"
	size := env.LargeObjectSize
	uploadPlaintextPattern(t, env, key, size)

	out, err := env.Client(t, env.TenantA, env.ReadLevel).GetObject(&s3.GetObjectInput{Bucket: aws.String(env.Bucket), Key: aws.String(key)})
	requireNoError(t, err, "reading a large plaintext object through the proxy")
	served := sha256.New()
	n, err := io.Copy(served, out.Body)
	out.Body.Close()
	requireNoError(t, err, "streaming it")
	if n != size {
		t.Fatalf("served %d bytes, want %d", n, size)
	}

	eventually(t, "the large object to be encrypted at rest", 5*time.Minute, func() bool { return encryptedAtRest(t, env, key) })
	noOpenUploads(t, env, time.Minute)
	repaired, err := env.Admin(t).GetObject(&s3.GetObjectInput{Bucket: aws.String(env.Bucket), Key: aws.String(env.UpstreamKey(env.TenantA, key))})
	requireNoError(t, err, "reading the repaired object with the key")
	stored := sha256.New()
	m, err := io.Copy(stored, repaired.Body)
	repaired.Body.Close()
	requireNoError(t, err, "streaming the repaired object")
	if m != size || !bytes.Equal(stored.Sum(nil), served.Sum(nil)) {
		t.Fatalf("the repaired object differs: %d bytes", m)
	}
}

// The key is read once, at startup. Rewriting the mounted key file — and
// waiting out a policy reload, which is the other thing that reads files —
// changes nothing until the process restarts.
func TestSSECTheKeyFileIsNotReloaded(t *testing.T) {
	env := ssecSetup(t)
	if env.SSECKeyFile == "" {
		t.Skip("E2E_SSEC_KEY_FILE is not set: this deployment does not read its key from a file")
	}
	if env.ReadOnly {
		t.Skip("this deployment runs with the mutation kill switch on")
	}
	original, err := os.ReadFile(env.SSECKeyFile)
	requireNoError(t, err, "reading the key file")
	t.Cleanup(func() { _ = os.WriteFile(env.SSECKeyFile, original, 0o644) })
	requireNoError(t, os.WriteFile(env.SSECKeyFile, []byte(otherSSECKey), 0o644), "rewriting the key file")
	time.Sleep(3 * time.Second) // past the variant's 2s policy reload interval

	key := "datasets/ssec/after-rewrite.csv"
	requireNoError(t, putObject(t, env.Client(t, env.TenantA, env.WriteLevel), env.Bucket, key, []byte("still the old key")), "put")
	got, err := getObject(t, env.Admin(t), env.Bucket, env.UpstreamKey(env.TenantA, key))
	requireNoError(t, err, "reading with the key the process started with")
	if string(got) != "still the old key" {
		t.Fatalf("read %q", got)
	}
	_, err = getObject(t, env.AdminWithKey(t, otherSSECKey), env.Bucket, env.UpstreamKey(env.TenantA, key))
	if statusOf(err) != http.StatusForbidden {
		t.Fatalf("the rewritten file's key reads it: %v", err)
	}
	if strings.TrimSpace(string(original)) != env.SSECKey {
		t.Fatal("E2E_SSEC_KEY does not match the key file")
	}
}

// MinIO evaluates a write's precondition by reading the existing object with
// the write's key, which fails for a plaintext object. A conditional write
// through the proxy onto an object nobody has read since the key was set
// must still get the answer S3 semantics promise — 200 or 412, never 400 —
// and what it writes is encrypted.
func TestSSECConditionalWritesOntoAPlaintextObject(t *testing.T) {
	env := ssecSetup(t)
	if env.ReadOnly {
		t.Skip("this deployment runs with the mutation kill switch on")
	}
	headETag := func(key string) string {
		out, err := env.RawAdmin(t).HeadObject(&s3.HeadObjectInput{
			Bucket: aws.String(env.Bucket), Key: aws.String(env.UpstreamKey(env.TenantA, key)),
		})
		requireNoError(t, err, "reading the plaintext object's ETag")
		return aws.StringValue(out.ETag)
	}
	put := func(key, body string, header http.Header) (int, string) {
		resp := rawSignedRequest(t, env, http.MethodPut, "/"+env.Bucket+"/"+key, []byte(body), header, env.TenantA, env.WriteLevel)
		defer resp.Body.Close()
		answer, _ := io.ReadAll(resp.Body)
		return resp.StatusCode, string(answer)
	}

	t.Run("PUT If-Match", func(t *testing.T) {
		key := "datasets/ssec/conditional-put.csv"
		putPlaintext(t, env, key, []byte("old"), "text/plain", nil)
		if status, answer := put(key, "refused", http.Header{"If-Match": {`"not-the-etag"`}}); status != http.StatusPreconditionFailed {
			t.Fatalf("a mismatched If-Match returned %d, want 412: %s", status, answer)
		}
		if status, answer := put(key, "accepted", http.Header{"If-Match": {headETag(key)}}); status != http.StatusOK {
			t.Fatalf("a matching If-Match returned %d, want 200: %s", status, answer)
		}
		if !encryptedAtRest(t, env, key) {
			t.Fatal("the conditional write was stored without the key")
		}
		got, err := getObject(t, env.Admin(t), env.Bucket, env.UpstreamKey(env.TenantA, key))
		requireNoError(t, err, "reading the written object with the key")
		if string(got) != "accepted" {
			t.Fatalf("the bucket holds %q", got)
		}
	})

	t.Run("PUT If-None-Match *", func(t *testing.T) {
		key := "datasets/ssec/conditional-create.csv"
		putPlaintext(t, env, key, []byte("already here"), "text/plain", nil)
		if status, answer := put(key, "refused", http.Header{"If-None-Match": {"*"}}); status != http.StatusPreconditionFailed {
			t.Fatalf("If-None-Match: * over an existing object returned %d, want 412: %s", status, answer)
		}
		got, err := getObject(t, env.RawAdmin(t), env.Bucket, env.UpstreamKey(env.TenantA, key))
		requireNoError(t, err, "the object is untouched, in plaintext")
		if string(got) != "already here" {
			t.Fatalf("the bucket holds %q", got)
		}
	})

	t.Run("CompleteMultipartUpload If-Match", func(t *testing.T) {
		key := "datasets/ssec/conditional-complete.bin"
		putPlaintext(t, env, key, []byte("old"), "application/octet-stream", nil)
		client := env.Client(t, env.TenantA, env.WriteLevel)
		created, err := client.CreateMultipartUpload(&s3.CreateMultipartUploadInput{Bucket: aws.String(env.Bucket), Key: aws.String(key)})
		requireNoError(t, err, "create")
		part, err := client.UploadPart(&s3.UploadPartInput{
			Bucket: aws.String(env.Bucket), Key: aws.String(key), UploadId: created.UploadId,
			PartNumber: aws.Int64(1), Body: bytes.NewReader([]byte("new")),
		})
		requireNoError(t, err, "upload part")
		list := []byte(`<CompleteMultipartUpload><Part><PartNumber>1</PartNumber><ETag>` + aws.StringValue(part.ETag) + `</ETag></Part></CompleteMultipartUpload>`)
		resp := rawSignedRequest(t, env, http.MethodPost, "/"+env.Bucket+"/"+key+"?uploadId="+aws.StringValue(created.UploadId),
			list, http.Header{"If-Match": {headETag(key)}}, env.TenantA, env.WriteLevel)
		answer, _ := io.ReadAll(resp.Body)
		resp.Body.Close()
		if resp.StatusCode != http.StatusOK || bytes.Contains(answer, []byte("<Error>")) {
			t.Fatalf("a matching conditional completion returned %d: %s", resp.StatusCode, answer)
		}
		if !encryptedAtRest(t, env, key) {
			t.Fatal("the assembled object can be read without the key")
		}
		got, err := getObject(t, env.Admin(t), env.Bucket, env.UpstreamKey(env.TenantA, key))
		requireNoError(t, err, "reading the assembled object with the key")
		if string(got) != "new" {
			t.Fatalf("the bucket holds %q", got)
		}
	})
	noOpenUploads(t, env, 30*time.Second)
}
