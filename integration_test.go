package main

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/xml"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"sort"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go/aws/credentials"
	v4 "github.com/aws/aws-sdk-go/aws/signer/v4"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// objectStore is a minimal stand-in for an S3-compatible backend: enough of
// GetObject / PutObject / HeadObject / DeleteObject / ListObjectsV2 /
// DeleteObjects to run a real client end to end through the proxy. It
// stores whatever key it is given, so a scoping mistake in the proxy shows
// up as a key under the wrong prefix rather than as a 404 that could have
// any number of causes.
type objectStore struct {
	mu      sync.Mutex
	objects map[string][]byte
	server  *httptest.Server
}

func newObjectStore(t *testing.T) *objectStore {
	s := &objectStore{objects: map[string][]byte{}}
	s.server = httptest.NewServer(http.HandlerFunc(s.serve))
	t.Cleanup(s.server.Close)
	return s
}

func (s *objectStore) keys() []string {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make([]string, 0, len(s.objects))
	for key := range s.objects {
		out = append(out, key)
	}
	sort.Strings(out)
	return out
}

func (s *objectStore) serve(w http.ResponseWriter, r *http.Request) {
	_, key := splitFirstSegment(strings.TrimPrefix(r.URL.Path, "/"))

	s.mu.Lock()
	defer s.mu.Unlock()

	if _, isBatchDelete := r.URL.Query()["delete"]; isBatchDelete {
		body, _ := io.ReadAll(r.Body)
		var parsed deleteObjectsRequestBody
		if err := xml.Unmarshal(body, &parsed); err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		var b strings.Builder
		b.WriteString(`<DeleteResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/">`)
		for _, obj := range parsed.Objects {
			delete(s.objects, obj.Key)
			fmt.Fprintf(&b, `<Deleted><Key>%s</Key></Deleted>`, obj.Key)
		}
		b.WriteString(`</DeleteResult>`)
		w.Header().Set("Content-Type", "application/xml")
		_, _ = w.Write([]byte(b.String()))
		return
	}

	if key == "" && r.Method == http.MethodGet {
		prefix := r.URL.Query().Get("prefix")
		var b strings.Builder
		b.WriteString(`<ListBucketResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/">`)
		fmt.Fprintf(&b, `<Prefix>%s</Prefix>`, prefix)
		stored := make([]string, 0, len(s.objects))
		for k := range s.objects {
			stored = append(stored, k)
		}
		sort.Strings(stored)
		for _, k := range stored {
			if strings.HasPrefix(k, prefix) {
				fmt.Fprintf(&b, `<Contents><Key>%s</Key><Size>%d</Size></Contents>`, k, len(s.objects[k]))
			}
		}
		b.WriteString(`</ListBucketResult>`)
		w.Header().Set("Content-Type", "application/xml")
		_, _ = w.Write([]byte(b.String()))
		return
	}

	switch r.Method {
	case http.MethodPut:
		body, _ := io.ReadAll(r.Body)
		s.objects[key] = body
		w.WriteHeader(http.StatusOK)
	case http.MethodGet, http.MethodHead:
		body, ok := s.objects[key]
		if !ok {
			w.Header().Set("Content-Type", "application/xml")
			w.WriteHeader(http.StatusNotFound)
			fmt.Fprintf(w, `<Error><Code>NoSuchKey</Code><Resource>%s</Resource></Error>`, r.URL.Path)
			return
		}
		w.Header().Set("Content-Length", fmt.Sprint(len(body)))
		w.WriteHeader(http.StatusOK)
		if r.Method == http.MethodGet {
			_, _ = w.Write(body)
		}
	case http.MethodDelete:
		delete(s.objects, key)
		w.WriteHeader(http.StatusNoContent)
	default:
		w.WriteHeader(http.StatusMethodNotAllowed)
	}
}

func newIntegrationProxy(t *testing.T, store *objectStore) *Handler {
	t.Helper()
	return NewHandler(&Handler{
		UpstreamScheme:        "http",
		UpstreamEndpoint:      strings.TrimPrefix(store.server.URL, "http://"),
		AllowedSourceEndpoint: testEndpoint,
		AllowedSourceSubnet:   testSubnets(t, "0.0.0.0/0"),
		Policy:                NewStaticPolicyStore(mustPolicy(t)),
		Pepper:                testPepper,
		UpstreamSigner: v4.NewSigner(credentials.NewStaticCredentialsFromCreds(credentials.Value{
			AccessKeyID:     "UPSTREAMKEYID",
			SecretAccessKey: "upstream-secret",
		})),
		UpstreamRegion:     testRegion,
		MaxClockSkew:       15 * time.Minute,
		MaxChunkedBodySize: 1 << 20,
		MaxDeleteBodySize:  1 << 20,
		MaxRewriteBodySize: 1 << 20,
	})
}

// A client that signs a real payload hash and, like DuckDB's httpfs, signs
// with an empty region scope. Read, write and delete all have to work
// unchanged.
func TestIntegrationPayloadHashClient(t *testing.T) {
	store := newObjectStore(t)
	h := newIntegrationProxy(t, store)

	send := func(method, target string, body []byte) *httptest.ResponseRecorder {
		t.Helper()
		var reader io.Reader
		if body != nil {
			reader = bytes.NewReader(body)
		}
		req := httptest.NewRequest(method, target, reader)
		req.Host = testEndpoint
		req.RemoteAddr = "10.1.2.3:54321"
		sum := sha256.Sum256(body)
		req.Header.Set("X-Amz-Content-Sha256", hex.EncodeToString(sum[:]))

		signer := v4.NewSigner(credentials.NewStaticCredentialsFromCreds(credentials.Value{
			AccessKeyID:     accessKeyFor(tenantA, "rws"),
			SecretAccessKey: secretFor(tenantA, "rws"),
		}))
		signer.DisableURIPathEscaping = true
		// Empty region scope, exactly as DuckDB's httpfs signs when no
		// region is configured.
		_, err := signer.Sign(req, bytes.NewReader(body), "s3", "", time.Now())
		require.NoError(t, err)

		rec := httptest.NewRecorder()
		h.ServeHTTP(rec, req)
		return rec
	}

	payload := []byte("PAR1" + strings.Repeat("data", 1000))

	t.Run("write", func(t *testing.T) {
		require.Equal(t, http.StatusOK, send(http.MethodPut, "/bucket/datasets/2026/a.parquet", payload).Code)
		assert.Equal(t, []string{tenantA + "/datasets/2026/a.parquet"}, store.keys())
	})

	t.Run("read back the exact bytes", func(t *testing.T) {
		rec := send(http.MethodGet, "/bucket/datasets/2026/a.parquet", nil)
		require.Equal(t, http.StatusOK, rec.Code)
		assert.Equal(t, payload, rec.Body.Bytes())
	})

	t.Run("head", func(t *testing.T) {
		rec := send(http.MethodHead, "/bucket/datasets/2026/a.parquet", nil)
		assert.Equal(t, http.StatusOK, rec.Code)
	})

	t.Run("list without ever seeing the tenant prefix", func(t *testing.T) {
		rec := send(http.MethodGet, "/bucket/?list-type=2&prefix=datasets/", nil)
		require.Equal(t, http.StatusOK, rec.Code)
		assert.Contains(t, rec.Body.String(), "<Key>datasets/2026/a.parquet</Key>")
		assert.NotContains(t, rec.Body.String(), tenantA)
	})

	t.Run("delete", func(t *testing.T) {
		require.Equal(t, http.StatusNoContent, send(http.MethodDelete, "/bucket/datasets/2026/a.parquet", nil).Code)
		assert.Empty(t, store.keys())
	})

	t.Run("a missing key reports a client-facing resource", func(t *testing.T) {
		rec := send(http.MethodGet, "/bucket/datasets/2026/a.parquet", nil)
		require.Equal(t, http.StatusNotFound, rec.Code)
		assert.Contains(t, rec.Body.String(), "<Resource>/bucket/datasets/2026/a.parquet</Resource>")
	})
}

// A client that sends aws-chunked bodies — what the AWS JS SDK v3 does by
// default — has to get through unchanged, including its batch deletes.
func TestIntegrationAwsChunkedClient(t *testing.T) {
	store := newObjectStore(t)
	h := newIntegrationProxy(t, store)

	send := func(method, target string, decoded []byte, trailer string) *httptest.ResponseRecorder {
		t.Helper()
		framed := fmt.Sprintf("%x\r\n%s\r\n0\r\n", len(decoded), decoded)
		headers := http.Header{
			"X-Amz-Content-Sha256":         {"STREAMING-UNSIGNED-PAYLOAD-TRAILER"},
			"X-Amz-Decoded-Content-Length": {fmt.Sprint(len(decoded))},
			"Content-Encoding":             {"aws-chunked"},
		}
		if trailer != "" {
			framed += trailer + "\r\n"
			headers.Set("X-Amz-Trailer", strings.SplitN(trailer, ":", 2)[0])
		}
		framed += "\r\n"

		return do(t, h, clientRequest{
			method: method, target: target, body: []byte(framed),
			tenant: tenantA, level: "rws", headers: headers,
		})
	}

	payload := []byte(strings.Repeat("chunked-object-content", 100))

	t.Run("upload with a checksum trailer", func(t *testing.T) {
		rec := send(http.MethodPut, "/bucket/workspaces/w1/out/report.csv", payload, "x-amz-checksum-crc32:AAAAAA==")
		require.Equal(t, http.StatusOK, rec.Code)

		store.mu.Lock()
		defer store.mu.Unlock()
		assert.Equal(t, payload, store.objects[tenantA+"/workspaces/w1/out/report.csv"],
			"the object store must receive the de-chunked bytes")
	})

	t.Run("list", func(t *testing.T) {
		rec := do(t, h, clientRequest{
			method: http.MethodGet, target: "/bucket/?list-type=2&prefix=workspaces/",
			tenant: tenantA, level: "rws",
		})
		require.Equal(t, http.StatusOK, rec.Code)
		assert.Contains(t, rec.Body.String(), "<Key>workspaces/w1/out/report.csv</Key>")
		assert.NotContains(t, rec.Body.String(), tenantA)
	})

	t.Run("batch delete", func(t *testing.T) {
		batch := deleteBatchBody("workspaces/w1/out/report.csv", "workspaces/w1/inbox/protected.csv")
		rec := send(http.MethodPost, "/bucket?delete", batch, "")
		require.Equal(t, http.StatusOK, rec.Code)

		out := rec.Body.String()
		assert.Contains(t, out, "<Deleted><Key>workspaces/w1/out/report.csv</Key></Deleted>")
		// The read-only carve-out inside the writable tree survives the batch.
		assert.Contains(t, out, "<Error><Key>workspaces/w1/inbox/protected.csv</Key><Code>AccessDenied</Code>")
		assert.Empty(t, store.keys())
	})
}
