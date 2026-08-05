//go:build e2e

package e2e

import (
	"os"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/s3"
)

// A hot reload is only useful if it is safe to get wrong. This drives the
// three outcomes that matter against a running proxy: a good edit takes
// effect, a bad edit is rejected, and — the one that would be a security
// incident — a rejected edit leaves the previous policy enforcing rather
// than failing open or closed.
//
// The replacement policies are supplied by the harness rather than
// generated here, so the test needs to understand nothing about the shape
// of the deployment's policy file.
func TestPolicyHotReload(t *testing.T) {
	env := setup(t)
	if env.PolicyFile == "" {
		t.Skip("E2E_POLICY_FILE is not set; this variant does not expose its policy to the test")
	}
	tightened := os.Getenv("E2E_POLICY_TIGHTENED")
	invalid := os.Getenv("E2E_POLICY_INVALID")
	if tightened == "" || invalid == "" {
		t.Skip("E2E_POLICY_TIGHTENED and E2E_POLICY_INVALID must both be set")
	}

	original, err := os.ReadFile(env.PolicyFile)
	requireNoError(t, err, "reading the live policy")
	t.Cleanup(func() {
		if err := os.WriteFile(env.PolicyFile, original, 0o644); err != nil {
			t.Errorf("restoring the original policy: %v", err)
		}
	})

	writer := env.Client(t, env.TenantA, env.WriteLevel)
	probe := "datasets/reload/probe.csv"

	// Baseline: the write level may write.
	requireNoError(t, putObject(t, writer, env.Bucket, probe, []byte("before")), "write under the original policy")

	t.Run("a valid edit takes effect", func(t *testing.T) {
		copyFile(t, tightened, env.PolicyFile)
		waitFor(t, env, "the tightened policy to take effect", func() bool {
			return putObject(t, writer, env.Bucket, probe, []byte("after")) != nil
		})
		requireDenied(t, putObject(t, writer, env.Bucket, probe, []byte("after")), "write under the tightened policy")

		// Reads still work — the edit narrowed one permission, it did not
		// take the proxy out of service.
		_, err := getObject(t, writer, env.Bucket, probe)
		requireNoError(t, err, "read under the tightened policy")
	})

	t.Run("an invalid edit is rejected and the running policy keeps serving", func(t *testing.T) {
		copyFile(t, invalid, env.PolicyFile)
		// Give the proxy more than enough time to notice and refuse it.
		time.Sleep(reloadWait(env))

		// The tightened policy — not the broken file, and not the original
		// — is still the one being enforced.
		requireDenied(t, putObject(t, writer, env.Bucket, probe, []byte("after")), "write while a broken policy sits on disk")
		if _, err := getObject(t, writer, env.Bucket, probe); err != nil {
			t.Fatalf("a rejected reload took the proxy out of service: %v", err)
		}
	})

	t.Run("restoring the original policy restores the permission", func(t *testing.T) {
		requireNoError(t, os.WriteFile(env.PolicyFile, original, 0o644), "restoring the policy")
		waitFor(t, env, "the original policy to come back", func() bool {
			return putObject(t, writer, env.Bucket, probe, []byte("restored")) == nil
		})
	})

	_, _ = writer.DeleteObject(&s3.DeleteObjectInput{
		Bucket: aws.String(env.Bucket), Key: aws.String(probe),
	})
}

// reloadWait is how long to allow for a reload to have been noticed: a few
// poll intervals, so a slow CI runner does not read as a failure to reload.
func reloadWait(env Env) time.Duration {
	interval := env.ReloadInterval
	if interval <= 0 {
		interval = 2 * time.Second
	}
	return 3 * interval
}

func waitFor(t *testing.T, env Env, what string, done func() bool) {
	t.Helper()
	deadline := time.Now().Add(reloadWait(env) + 10*time.Second)
	for time.Now().Before(deadline) {
		if done() {
			return
		}
		time.Sleep(500 * time.Millisecond)
	}
	t.Fatalf("timed out waiting for %s", what)
}

func copyFile(t *testing.T, src, dst string) {
	t.Helper()
	body, err := os.ReadFile(src)
	requireNoError(t, err, "reading "+src)
	requireNoError(t, os.WriteFile(dst, body, 0o644), "writing "+dst)
}
