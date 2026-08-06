package sigv4

import (
	"bytes"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go/aws/credentials"
	v4 "github.com/aws/aws-sdk-go/aws/signer/v4"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const (
	testHost   = "s3.proxy.example.com:8099"
	testSecret = "a-derived-secret-value"
)

// sign builds a request signed the way a real S3 client signs one.
func sign(t *testing.T, method, target, accessKeyID, secret, region string, at time.Time, body []byte) *http.Request {
	t.Helper()
	var reader io.Reader
	if body != nil {
		reader = bytes.NewReader(body)
	}
	req := httptest.NewRequest(method, target, reader)
	req.Host = testHost
	signer := v4.NewSigner(credentials.NewStaticCredentialsFromCreds(credentials.Value{
		AccessKeyID:     accessKeyID,
		SecretAccessKey: secret,
	}))
	// S3 does not escape the canonical URI a second time.
	signer.DisableURIPathEscaping = true
	_, err := signer.Sign(req, bytes.NewReader(body), "s3", region, at)
	require.NoError(t, err)
	return req
}

func testVerifier() *Verifier {
	return &Verifier{Host: testHost, MaxClockSkew: 15 * time.Minute}
}

func TestReadCredential(t *testing.T) {
	v := testVerifier()

	t.Run("names the key and the region scope", func(t *testing.T) {
		req := sign(t, http.MethodGet, "/bucket/a.csv", "TENANTKEYID", testSecret, "eu-central-1", time.Now(), nil)
		cred, err := v.ReadCredential(req)
		require.NoError(t, err)
		assert.Equal(t, "TENANTKEYID", cred.AccessKeyID)
		assert.Equal(t, "eu-central-1", cred.Region)
	})

	// The access-key-id layout is the policy's business, not this package's:
	// anything excluded here would be a layout an operator could configure
	// but never authenticate with.
	t.Run("accepts the access-key-id layouts a policy may define", func(t *testing.T) {
		for _, id := range []string{"acmecorp1-writer", "tenant.a_b~c", "0f9e8d7c6b5a49382716f5e4d3c2b1a0rws", "a+b"} {
			req := sign(t, http.MethodGet, "/bucket/a.csv", id, testSecret, "eu-central-1", time.Now(), nil)
			cred, err := v.ReadCredential(req)
			require.NoError(t, err, id)
			assert.Equal(t, id, cred.AccessKeyID)
		}
	})

	// Some clients (DuckDB's httpfs) sign with an empty region scope. The
	// signature is still valid; refusing it would lock them out.
	t.Run("accepts an empty region scope", func(t *testing.T) {
		req := sign(t, http.MethodGet, "/bucket/a.csv", "TENANTKEYID", testSecret, "", time.Now(), nil)
		cred, err := v.ReadCredential(req)
		require.NoError(t, err)
		assert.Empty(t, cred.Region)
	})

	t.Run("anonymous", func(t *testing.T) {
		_, err := v.ReadCredential(httptest.NewRequest(http.MethodGet, "/bucket/a.csv", nil))
		require.Error(t, err)
		assert.Contains(t, err.Error(), "X-Amz-Date")
	})

	// A signature stays valid forever unless its timestamp is bounded, so a
	// captured Authorization header could otherwise be replayed at will.
	t.Run("outside the clock skew window", func(t *testing.T) {
		for _, offset := range []time.Duration{-2 * time.Hour, 2 * time.Hour} {
			req := sign(t, http.MethodGet, "/bucket/a.csv", "TENANTKEYID", testSecret, "eu-central-1", time.Now().Add(offset), nil)
			_, err := v.ReadCredential(req)
			require.Error(t, err, offset.String())
			assert.Contains(t, err.Error(), "clock skew")
		}
	})

	t.Run("a header set twice is ambiguous", func(t *testing.T) {
		req := sign(t, http.MethodGet, "/bucket/a.csv", "TENANTKEYID", testSecret, "eu-central-1", time.Now(), nil)
		req.Header.Add("Authorization", req.Header.Get("Authorization"))
		_, err := v.ReadCredential(req)
		assert.Error(t, err)
	})
}

func TestVerify(t *testing.T) {
	v := testVerifier()
	payload := []byte("column,value\n1,2\n")

	t.Run("the right secret verifies", func(t *testing.T) {
		req := sign(t, http.MethodPut, "/bucket/datasets/a.csv", "TENANTKEYID", testSecret, "eu-central-1", time.Now(), payload)
		cred, err := v.ReadCredential(req)
		require.NoError(t, err)
		require.NoError(t, v.Verify(req, cred, testSecret))
	})

	t.Run("any other secret does not", func(t *testing.T) {
		req := sign(t, http.MethodPut, "/bucket/datasets/a.csv", "TENANTKEYID", testSecret, "eu-central-1", time.Now(), payload)
		cred, err := v.ReadCredential(req)
		require.NoError(t, err)
		err = v.Verify(req, cred, "guessed-secret")
		require.Error(t, err)
		assert.Contains(t, err.Error(), "invalid signature")
	})

	// A key containing a character that needs escaping is where a proxy that
	// escaped the canonical URI a second time would reject every request.
	t.Run("keys that need escaping still verify", func(t *testing.T) {
		for _, target := range []string{
			"/bucket/datasets/a%20b.csv",
			"/bucket/datasets/a%2Bb.csv",
			"/bucket/datasets/%C3%BCnicode.csv",
		} {
			req := sign(t, http.MethodGet, target, "TENANTKEYID", testSecret, "eu-central-1", time.Now(), nil)
			cred, err := v.ReadCredential(req)
			require.NoError(t, err, target)
			assert.NoError(t, v.Verify(req, cred, testSecret), target)
		}
	})

	// Verification must not read the body: the payload hash the client signed
	// is among the signed headers, and reading a large upload just to hash it
	// again is what would put it in memory.
	t.Run("the body is never read", func(t *testing.T) {
		req := sign(t, http.MethodPut, "/bucket/datasets/a.csv", "TENANTKEYID", testSecret, "eu-central-1", time.Now(), payload)
		cred, err := v.ReadCredential(req)
		require.NoError(t, err)

		req.Body = io.NopCloser(readerThatFails{t})
		require.NoError(t, v.Verify(req, cred, testSecret))
	})

	// s3cmd omits the space before some commas in the header.
	t.Run("the s3cmd header spelling", func(t *testing.T) {
		req := sign(t, http.MethodGet, "/bucket/datasets/a.csv", "TENANTKEYID", testSecret, "eu-central-1", time.Now(), nil)
		cred, err := v.ReadCredential(req)
		require.NoError(t, err)
		req.Header.Set("Authorization", strings.NewReplacer(", Signature", ",Signature", ", SignedHeaders", ",SignedHeaders").
			Replace(req.Header.Get("Authorization")))
		assert.NoError(t, v.Verify(req, cred, testSecret))
	})
}

type readerThatFails struct{ t *testing.T }

func (r readerThatFails) Read([]byte) (int, error) {
	r.t.Fatal("verification read the request body")
	return 0, io.EOF
}

// The AWS SDK's signer assigns Request.Body from whatever body it was handed
// — and it is handed none on purpose, so that it adopts the payload hash
// already in the header rather than reading the upload. Without the
// save/restore inside Sign, the request would go upstream with a
// ContentLength and no body at all.
func TestSignerPreservesTheBody(t *testing.T) {
	payload := []byte("the object bytes")
	req, err := http.NewRequest(http.MethodPut, "https://minio:9000/bucket/a.csv", bytes.NewReader(payload))
	require.NoError(t, err)
	req.ContentLength = int64(len(payload))
	req.Header.Set("X-Amz-Content-Sha256", UnsignedPayload)

	require.NoError(t, NewSigner("UPSTREAMKEYID", "upstream-secret").Sign(req, "eu-central-1", time.Now()))

	require.NotNil(t, req.Body, "a nil body with a non-zero ContentLength is a 502 waiting to happen")
	assert.Equal(t, int64(len(payload)), req.ContentLength)
	got, err := io.ReadAll(req.Body)
	require.NoError(t, err)
	assert.Equal(t, payload, got)

	assert.Contains(t, req.Header.Get("Authorization"), "Credential=UPSTREAMKEYID/")
	assert.Equal(t, UnsignedPayload, req.Header.Get("X-Amz-Content-Sha256"),
		"the digest the caller set has to survive, or the signer would have read the body to compute one")
}

func TestIsPayloadHash(t *testing.T) {
	assert.True(t, IsPayloadHash(EmptyPayloadSHA256))
	assert.True(t, IsPayloadHash(strings.Repeat("0f", 32)))
	for _, marker := range []string{
		"", UnsignedPayload, "STREAMING-UNSIGNED-PAYLOAD-TRAILER",
		"STREAMING-AWS4-HMAC-SHA256-PAYLOAD",
		strings.ToUpper(EmptyPayloadSHA256), // hex is lower-case on the wire
		strings.Repeat("0f", 31),
	} {
		assert.False(t, IsPayloadHash(marker), marker)
	}
}
