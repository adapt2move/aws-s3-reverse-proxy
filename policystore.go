package main

import (
	"bytes"
	"context"
	"fmt"
	"os"
	"sync/atomic"
	"time"

	log "github.com/sirupsen/logrus"
)

// PolicyStore holds the policy currently in force and swaps in a new one
// when the file on disk changes.
//
// The request path only ever calls Current(), which is a single atomic
// load: a reload never blocks a request and never hands out a half-built
// policy, because a Policy is immutable once compiled and is replaced as a
// whole.
//
// A reload that fails validation is *rejected* and the previously loaded
// policy keeps serving. A bad edit — a typo'd level name, an unparseable
// pattern — must not open the gate (an empty rule list would deny
// everything, a mis-merged one could grant too much); it must change
// nothing at all.
type PolicyStore struct {
	path    string
	current atomic.Value // *Policy
}

// NewPolicyStore loads the policy file once. An invalid policy here is
// fatal: the process must not start serving with no rules.
func NewPolicyStore(path string) (*PolicyStore, error) {
	if path == "" {
		return nil, fmt.Errorf("no policy file configured")
	}
	s := &PolicyStore{path: path}
	policy, err := loadPolicyFile(path)
	if err != nil {
		return nil, err
	}
	s.current.Store(policy)
	return s, nil
}

// NewStaticPolicyStore wraps an already-compiled policy. Used by tests and
// by any caller that does not want a file on disk.
func NewStaticPolicyStore(policy *Policy) *PolicyStore {
	s := &PolicyStore{}
	s.current.Store(policy)
	return s
}

// Current returns the policy in force. Never nil.
func (s *PolicyStore) Current() *Policy {
	return s.current.Load().(*Policy)
}

// Reload re-reads the policy file and swaps it in when it parses,
// validates and actually differs from what is loaded. Returns the error
// without touching the current policy otherwise.
func (s *PolicyStore) Reload() (changed bool, err error) {
	if s.path == "" {
		return false, nil
	}
	policy, err := loadPolicyFile(s.path)
	if err != nil {
		return false, err
	}
	if bytes.Equal(policy.raw, s.Current().raw) {
		return false, nil
	}
	s.current.Store(policy)
	return true, nil
}

// Watch polls the policy file until ctx is done, reloading it when the
// bytes change. Polling rather than watching inodes deliberately: a
// ConfigMap update replaces the mount's symlink target, which the usual
// filesystem-watch APIs report inconsistently across platforms and
// container runtimes, while a periodic re-read is correct everywhere and
// costs one small read per interval.
//
// An interval of zero disables reloading entirely (SIGHUP still works).
func (s *PolicyStore) Watch(ctx context.Context, interval time.Duration) {
	if interval <= 0 || s.path == "" {
		return
	}
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			s.reloadAndLog()
		}
	}
}

// reloadAndLog performs a reload and reports the outcome. A rejected
// reload is logged at error level with the reason — the operator's only
// signal that the file on disk is not the policy being enforced.
func (s *PolicyStore) reloadAndLog() {
	changed, err := s.Reload()
	switch {
	case err != nil:
		policyReloads.WithLabelValues("rejected").Inc()
		log.WithError(err).Errorf("policy reload rejected, keeping the previously loaded policy from %s", s.path)
	case !changed:
		policyReloads.WithLabelValues("unchanged").Inc()
	default:
		policyReloads.WithLabelValues("applied").Inc()
		policy := s.Current()
		log.WithFields(log.Fields{
			"levels": policy.Levels(),
			"rules":  len(policy.Rules()),
		}).Infof("reloaded policy from %s", s.path)
	}
}

func loadPolicyFile(path string) (*Policy, error) {
	raw, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("cannot read policy file %s: %v", path, err)
	}
	policy, err := ParsePolicy(raw)
	if err != nil {
		return nil, fmt.Errorf("%s: %v", path, err)
	}
	return policy, nil
}
