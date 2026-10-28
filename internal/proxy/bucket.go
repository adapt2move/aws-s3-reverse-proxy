package proxy

import (
	"net/http"

	"github.com/Kriechi/aws-s3-reverse-proxy/internal/s3"
)

// serveBucketLocation answers GetBucketLocation without asking the object
// store.
//
// Clients ask it to find out which region to sign for, and the answer that
// works through this proxy is always the same one: the region it signs its
// own requests upstream for. Asking the object store would only add a round
// trip, and a store that does not implement the call — or answers it with a
// region the client then cannot use against this endpoint — would turn a
// lookup into a failure.
func (h *Handler) serveBucketLocation(w http.ResponseWriter) {
	s3.WriteBucketLocation(w, h.cfg.UpstreamRegion)
}
