package cache

import (
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestPurgeHandlerEmptiesTheCache(t *testing.T) {
	c, _ := newCache(t)
	store(t, c, "k", objectHeader(), []byte("contents"))
	require.True(t, c.Has("k"))

	rec := httptest.NewRecorder()
	PurgeHandler(c, "").ServeHTTP(rec, httptest.NewRequest(http.MethodPost, "/cache/purge", nil))
	assert.Equal(t, http.StatusOK, rec.Code)
	assert.Contains(t, rec.Body.String(), "purged 1 objects")
	assert.False(t, c.Has("k"))
}

func TestPurgeHandlerRefusesAnythingButPost(t *testing.T) {
	c, _ := newCache(t)
	store(t, c, "k", objectHeader(), []byte("contents"))

	rec := httptest.NewRecorder()
	PurgeHandler(c, "").ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/cache/purge", nil))
	assert.Equal(t, http.StatusMethodNotAllowed, rec.Code)
	assert.Equal(t, http.MethodPost, rec.Header().Get("Allow"))
	assert.True(t, c.Has("k"), "a GET must not have emptied anything")
}

// Emptying the cache repeatedly is a cheap way to force full re-fetches from
// the object store — the exact egress this feature exists to save — so on a
// listener that also serves metrics it is worth being able to close.
func TestPurgeHandlerChecksItsToken(t *testing.T) {
	for _, tc := range []struct {
		name       string
		authHeader string
		want       int
	}{
		{"no credentials at all", "", http.StatusUnauthorized},
		{"the wrong token", "Bearer not-the-token", http.StatusUnauthorized},
		{"the right token, wrong scheme", "Basic s3cret", http.StatusUnauthorized},
		{"a prefix of the token", "Bearer s3cre", http.StatusUnauthorized},
		{"the token", "Bearer s3cret", http.StatusOK},
	} {
		t.Run(tc.name, func(t *testing.T) {
			c, _ := newCache(t)
			store(t, c, "k", objectHeader(), []byte("contents"))

			req := httptest.NewRequest(http.MethodPost, "/cache/purge", nil)
			if tc.authHeader != "" {
				req.Header.Set("Authorization", tc.authHeader)
			}
			rec := httptest.NewRecorder()
			PurgeHandler(c, "s3cret").ServeHTTP(rec, req)

			assert.Equal(t, tc.want, rec.Code)
			assert.Equal(t, tc.want != http.StatusOK, c.Has("k"),
				"the cache should be emptied exactly when the request was allowed")
		})
	}
}
