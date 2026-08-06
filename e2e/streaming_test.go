//go:build e2e

package e2e

import (
	"io"
	"net/http"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/s3"
)

// The claim is that per-request memory is independent of object size. The
// way to test that is not to measure the proxy's heap but to take the memory
// away: the compose stack runs this variant with a hard container limit far
// below E2E_LARGE_OBJECT_SIZE. A proxy that buffered the upload would be
// killed by the OOM reaper, and this test would fail as a dropped
// connection rather than as a wrong answer.
//
// The body is generated as it is sent and never exists in one piece on the
// client side either, so the test itself cannot be what runs out of memory.
func TestLargeObjectStreamsThrough(t *testing.T) {
	env := setup(t)
	if env.ReadOnly {
		t.Skip("this deployment runs with the mutation kill switch on")
	}
	if env.LargeObjectSize == 0 {
		t.Skip("E2E_LARGE_OBJECT_SIZE is not set")
	}
	key := "datasets/streaming/large.bin"
	size := env.LargeObjectSize

	req, err := http.NewRequest(http.MethodPut,
		env.objectURL(key), io.NopCloser(newPatternReader(size)))
	requireNoError(t, err, "building the upload request")
	req.ContentLength = size
	// A client that cannot hash a stream up front signs the payload as
	// unsigned; that is precisely the case where the proxy must not read
	// the body either.
	req.Header.Set("X-Amz-Content-Sha256", "UNSIGNED-PAYLOAD")

	if _, err := env.Signer(env.TenantA, env.WriteLevel).Sign(
		req, nil, "s3", env.Region, time.Now()); err != nil {
		t.Fatalf("signing the upload: %v", err)
	}
	// The signer nils out a body it was not handed.
	req.Body = io.NopCloser(newPatternReader(size))
	req.ContentLength = size

	resp, err := (&http.Client{Timeout: 30 * time.Minute}).Do(req)
	requireNoError(t, err, "streaming a large upload")
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		t.Fatalf("large upload returned %d: %s", resp.StatusCode, body)
	}

	head, err := env.Admin(t).HeadObject(&s3.HeadObjectInput{
		Bucket: aws.String(env.Bucket),
		Key:    aws.String(env.UpstreamKey(env.TenantA, key)),
	})
	requireNoError(t, err, "heading the stored object")
	if got := aws.Int64Value(head.ContentLength); got != size {
		t.Fatalf("the object store holds %d bytes, want %d", got, size)
	}

	// Read it back through the proxy too: the download path streams as
	// well, and a truncated body would show up here.
	resp2, err := env.Client(t, env.TenantA, env.ReadLevel).GetObject(&s3.GetObjectInput{
		Bucket: aws.String(env.Bucket), Key: aws.String(key),
	})
	requireNoError(t, err, "downloading the large object")
	defer resp2.Body.Close()
	n, err := io.Copy(io.Discard, resp2.Body)
	requireNoError(t, err, "draining the download")
	if n != size {
		t.Fatalf("downloaded %d bytes, want %d", n, size)
	}
}

// patternReader produces `size` bytes of a repeating pattern without ever
// holding more than one block of it.
type patternReader struct {
	remaining int64
	block     []byte
	offset    int
}

func newPatternReader(size int64) *patternReader {
	block := make([]byte, 64*1024)
	for i := range block {
		block[i] = byte('a' + i%26)
	}
	return &patternReader{remaining: size, block: block}
}

func (r *patternReader) Read(p []byte) (int, error) {
	if r.remaining <= 0 {
		return 0, io.EOF
	}
	n := copy(p, r.block[r.offset:])
	if int64(n) > r.remaining {
		n = int(r.remaining)
	}
	r.offset = (r.offset + n) % len(r.block)
	r.remaining -= int64(n)
	return n, nil
}
