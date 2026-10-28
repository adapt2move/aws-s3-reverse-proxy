package cache

import (
	"crypto/subtle"
	"fmt"
	"io"
	"net/http"
	"strings"
)

// PurgeTarget is one store the purge endpoint empties.
type PurgeTarget struct {
	// What names what the store holds, for the answer: "objects",
	// "listings".
	What string
	// Size reports how much it holds right now.
	Size func() (entries int, bytes int64)
	// Purge empties it.
	Purge func() error
}

// PurgeTarget is the object cache, for PurgeHandler.
func (c *Cache) PurgeTarget() PurgeTarget {
	return PurgeTarget{
		What: "objects",
		Size: func() (int, int64) {
			s := c.Stats()
			return s.Entries, s.Bytes
		},
		Purge: c.Purge,
	}
}

// PurgeHandler empties the caches on request — the object cache, the
// listing fallback, whichever of them are configured.
//
// It exists for the one inconsistency the caches cannot detect on their own:
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
func PurgeHandler(token string, targets ...PurgeTarget) http.Handler {
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
		var answer strings.Builder
		for _, target := range targets {
			entries, bytes := target.Size()
			if err := target.Purge(); err != nil {
				http.Error(w, "the cache of "+target.What+" could not be purged: "+err.Error()+"\n", http.StatusInternalServerError)
				return
			}
			fmt.Fprintf(&answer, "purged %d %s, %d bytes\n", entries, target.What, bytes)
		}
		w.Header().Set("Content-Type", "text/plain; charset=utf-8")
		_, _ = io.WriteString(w, answer.String())
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
