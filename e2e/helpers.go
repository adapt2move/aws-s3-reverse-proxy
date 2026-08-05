//go:build e2e

package e2e

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"io"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/awserr"
	"github.com/aws/aws-sdk-go/service/s3"
)

// statusOf digs the HTTP status out of an AWS SDK error. A refusal by the
// proxy and a refusal by MinIO look the same to the SDK, which is exactly
// what a transparent proxy should produce.
func statusOf(err error) int {
	if err == nil {
		return 0
	}
	var reqErr awserr.RequestFailure
	if ok := asRequestFailure(err, &reqErr); ok {
		return reqErr.StatusCode()
	}
	return -1
}

func asRequestFailure(err error, target *awserr.RequestFailure) bool {
	if rf, ok := err.(awserr.RequestFailure); ok {
		*target = rf
		return true
	}
	return false
}

// requireDenied asserts that an operation was refused with 403 and not, say,
// quietly succeeded or failed for an unrelated reason.
func requireDenied(t *testing.T, err error, what string) {
	t.Helper()
	if err == nil {
		t.Fatalf("%s: expected the proxy to refuse this, it succeeded", what)
	}
	if status := statusOf(err); status != http.StatusForbidden {
		t.Fatalf("%s: expected 403, got %d (%v)", what, status, err)
	}
}

func requireNoError(t *testing.T, err error, what string) {
	t.Helper()
	if err != nil {
		t.Fatalf("%s: %v", what, err)
	}
}

// setupBucket makes sure the bucket exists, using root credentials straight
// against MinIO — bucket administration through the proxy is refused by
// design, so the suite cannot create it the way a client would.
func setupBucket(t *testing.T, env Env) {
	t.Helper()
	admin := env.Admin(t)
	_, err := admin.CreateBucket(&s3.CreateBucketInput{Bucket: aws.String(env.Bucket)})
	if err != nil {
		switch statusOf(err) {
		case http.StatusConflict, http.StatusOK:
			// Already there, from a previous run or a sibling variant.
		default:
			if aerr, ok := err.(awserr.Error); !ok ||
				(aerr.Code() != s3.ErrCodeBucketAlreadyExists && aerr.Code() != s3.ErrCodeBucketAlreadyOwnedByYou) {
				t.Fatalf("creating bucket %s: %v", env.Bucket, err)
			}
		}
	}
}

// resetTenant removes everything under a tenant's prefix so each test starts
// from a known bucket. Done with root credentials: the proxy would (rightly)
// refuse to delete keys no rule covers.
func resetTenant(t *testing.T, env Env, tenant string) {
	t.Helper()
	admin := env.Admin(t)
	prefix := env.UpstreamKey(tenant, "")

	var keys []*s3.ObjectIdentifier
	err := admin.ListObjectsV2Pages(&s3.ListObjectsV2Input{
		Bucket: aws.String(env.Bucket),
		Prefix: aws.String(prefix),
	}, func(page *s3.ListObjectsV2Output, last bool) bool {
		for _, obj := range page.Contents {
			keys = append(keys, &s3.ObjectIdentifier{Key: obj.Key})
		}
		return true
	})
	requireNoError(t, err, "listing tenant objects for cleanup")

	for len(keys) > 0 {
		batch := keys
		if len(batch) > 1000 {
			batch = batch[:1000]
		}
		_, err := admin.DeleteObjects(&s3.DeleteObjectsInput{
			Bucket: aws.String(env.Bucket),
			Delete: &s3.Delete{Objects: batch, Quiet: aws.Bool(true)},
		})
		requireNoError(t, err, "cleaning up tenant objects")
		keys = keys[len(batch):]
	}
}

// upstreamKeys lists what actually exists in the bucket under a tenant's
// prefix, in upstream form. Client-side assertions cannot catch a proxy that
// answers correctly while writing somewhere else; this can.
func upstreamKeys(t *testing.T, env Env, tenant string) []string {
	t.Helper()
	admin := env.Admin(t)
	var out []string
	err := admin.ListObjectsV2Pages(&s3.ListObjectsV2Input{
		Bucket: aws.String(env.Bucket),
		Prefix: aws.String(env.UpstreamKey(tenant, "")),
	}, func(page *s3.ListObjectsV2Output, last bool) bool {
		for _, obj := range page.Contents {
			out = append(out, aws.StringValue(obj.Key))
		}
		return true
	})
	requireNoError(t, err, "listing upstream keys")
	return out
}

func putObject(t *testing.T, client *s3.S3, bucket, key string, body []byte) error {
	t.Helper()
	_, err := client.PutObject(&s3.PutObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(key),
		Body:   aws.ReadSeekCloser(bytes.NewReader(body)),
	})
	return err
}

func getObject(t *testing.T, client *s3.S3, bucket, key string) ([]byte, error) {
	t.Helper()
	resp, err := client.GetObject(&s3.GetObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(key),
	})
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	return io.ReadAll(resp.Body)
}

func sha256Hex(b []byte) string {
	sum := sha256.Sum256(b)
	return hex.EncodeToString(sum[:])
}

func contains(haystack []string, needle string) bool {
	for _, s := range haystack {
		if s == needle {
			return true
		}
	}
	return false
}

// rawSignedRequest sends a request the AWS SDK cannot express: an
// aws-chunked body, a path the SDK would normalize away, a deliberately
// stale timestamp. It signs with a derived credential exactly as a client
// would.
func rawSignedRequest(t *testing.T, env Env, method, path string, body []byte, headers http.Header, tenant, level string) *http.Response {
	t.Helper()
	var reader io.Reader
	if body != nil {
		reader = bytes.NewReader(body)
	}
	req, err := http.NewRequest(method, strings.TrimSuffix(env.ProxyEndpoint, "/")+path, reader)
	requireNoError(t, err, "building a raw request")
	for name, values := range headers {
		req.Header[http.CanonicalHeaderKey(name)] = values
	}
	if req.Header.Get("X-Amz-Content-Sha256") == "" {
		req.Header.Set("X-Amz-Content-Sha256", sha256Hex(body))
	}
	if _, err := env.Signer(tenant, level).Sign(req, bytes.NewReader(body), "s3", env.Region, time.Now()); err != nil {
		t.Fatalf("signing a raw request: %v", err)
	}
	// The signer attaches the body it was handed, so the length has to be
	// restated afterwards.
	req.ContentLength = int64(len(body))

	resp, err := (&http.Client{Timeout: 10 * time.Minute}).Do(req)
	requireNoError(t, err, "sending a raw request")
	return resp
}
