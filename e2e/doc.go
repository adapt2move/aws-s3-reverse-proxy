// Package e2e holds the end-to-end suite: real S3 clients, talking to a
// real aws-s3-reverse-proxy, in front of a real MinIO.
//
// Every file in it carries the `e2e` build tag, so `go build ./...` and
// `go test ./...` skip it — the suite needs a running stack, which
// e2e/run.sh (or docker compose) brings up. See e2e/README.md.
package e2e
