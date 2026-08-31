package cache

import (
	"crypto/subtle"
	"fmt"
	"net/http"
	"strings"
)

// PurgeHandler empties the cache on request.
//
// It exists for the one inconsistency the cache cannot detect on its own:
// something outside this process changed the data, and there is no list of
// which objects. Everything else — an upload, a delete, a completed multipart
// — invalidates what it touched as it goes.
//
// It belongs on the admin listener and nowhere else. On the S3 port a path
// like /cache/purge is indistinguishable from a request for an object called
// that, and a bucket that happened to be named `cache` would put it within
// reach of any tenant.
//
// A separate listener is not the same as a protected one, though. That
// listener also serves metrics and health, so without a token anything that
// can scrape can also empty the cache — in a loop, which is a cheap way to
// force full re-fetches and spend exactly the egress this feature exists to
// save. An empty token leaves the endpoint open, which is why the process
// says so at startup rather than leaving it to be discovered.
func PurgeHandler(c *Cache, token string) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			w.Header().Set("Allow", http.MethodPost)
			http.Error(w, "purging the cache is a POST\n", http.StatusMethodNotAllowed)
			return
		}
		if !authorizedToPurge(r, token) {
			w.Header().Set("WWW-Authenticate", "Bearer")
			http.Error(w, "a bearer token is required to purge the cache\n", http.StatusUnauthorized)
			return
		}
		before := c.Stats()
		if err := c.Purge(); err != nil {
			http.Error(w, "the cache could not be purged: "+err.Error()+"\n", http.StatusInternalServerError)
			return
		}
		w.Header().Set("Content-Type", "text/plain; charset=utf-8")
		fmt.Fprintf(w, "purged %d objects, %d bytes\n", before.Entries, before.Bytes)
	})
}

// authorizedToPurge compares the request's bearer token with the configured
// one, in constant time so that the comparison itself does not leak it.
func authorizedToPurge(r *http.Request, token string) bool {
	if token == "" {
		return true
	}
	presented, ok := strings.CutPrefix(r.Header.Get("Authorization"), "Bearer ")
	if !ok {
		return false
	}
	return subtle.ConstantTimeCompare([]byte(presented), []byte(token)) == 1
}
