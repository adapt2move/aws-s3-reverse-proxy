package cache

import (
	"fmt"
	"net/http"
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
func PurgeHandler(c *Cache) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			w.Header().Set("Allow", http.MethodPost)
			http.Error(w, "purging the cache is a POST\n", http.StatusMethodNotAllowed)
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
