package policy

import (
	"bytes"
	"context"
	"fmt"
	"os"
	"sync/atomic"
	"time"
)

// Store holds the policy currently in force and swaps in a new one when the
// file on disk changes.
//
// The request path only ever calls Current(), which is a single atomic load:
// a reload never blocks a request and never hands out a half-built policy,
// because a Policy is immutable once compiled and is replaced as a whole.
//
// A reload that fails validation is *rejected* and the previously loaded
// policy keeps serving. A bad edit — a typo'd level name, an unparseable
// pattern — must not open the gate (an empty rule list would deny
// everything, a mis-merged one could grant too much); it must change nothing
// at all.
type Store struct {
	path    string
	current atomic.Value // *Policy

	// OnReload, when set, is called after every reload attempt Watch or
	// ReloadNow makes, with what happened and why.
	//
	// It is a hook rather than a direct call into the logger and the metric
	// registry so that this package stays free of both: an operator's view
	// of a rejected reload is a reporting concern, and wiring it in the
	// caller keeps the authorization model testable without either.
	OnReload func(outcome ReloadOutcome, err error)
}

// ReloadOutcome is what a reload attempt did.
type ReloadOutcome string

const (
	// ReloadApplied means a new policy is now in force.
	ReloadApplied ReloadOutcome = "applied"
	// ReloadRejected means the file did not parse or validate and the
	// previously loaded policy is still in force.
	ReloadRejected ReloadOutcome = "rejected"
	// ReloadUnchanged means the file is byte-identical to what is loaded.
	ReloadUnchanged ReloadOutcome = "unchanged"
)

// NewStore loads the policy file once. An invalid policy here is fatal: the
// process must not start serving with no rules.
func NewStore(path string) (*Store, error) {
	if path == "" {
		return nil, fmt.Errorf("no policy file configured")
	}
	s := &Store{path: path}
	policy, err := loadFile(path)
	if err != nil {
		return nil, err
	}
	s.current.Store(policy)
	return s, nil
}

// NewStatic wraps an already-compiled policy. Used by tests and by any caller
// that does not want a file on disk.
func NewStatic(policy *Policy) *Store {
	s := &Store{}
	s.current.Store(policy)
	return s
}

// Path is the file this store reloads from, for a caller that has to name it
// in a log line. Empty for a static store.
func (s *Store) Path() string { return s.path }

// Current returns the policy in force. Never nil.
func (s *Store) Current() *Policy {
	return s.current.Load().(*Policy)
}

// Reload re-reads the policy file and swaps it in when it parses, validates
// and actually differs from what is loaded. Returns the error without
// touching the current policy otherwise.
func (s *Store) Reload() (changed bool, err error) {
	if s.path == "" {
		return false, nil
	}
	policy, err := loadFile(s.path)
	if err != nil {
		return false, err
	}
	if bytes.Equal(policy.raw, s.Current().raw) {
		return false, nil
	}
	s.current.Store(policy)
	return true, nil
}

// ReloadNow reloads once and reports the outcome through OnReload. It is what
// a SIGHUP handler calls, and what Watch calls on every tick.
func (s *Store) ReloadNow() {
	changed, err := s.Reload()
	if s.OnReload == nil {
		return
	}
	switch {
	case err != nil:
		s.OnReload(ReloadRejected, err)
	case !changed:
		s.OnReload(ReloadUnchanged, nil)
	default:
		s.OnReload(ReloadApplied, nil)
	}
}

// Watch polls the policy file until ctx is done, reloading it when the bytes
// change. Polling rather than watching inodes deliberately: a ConfigMap
// update replaces the mount's symlink target, which the usual filesystem-watch
// APIs report inconsistently across platforms and container runtimes, while a
// periodic re-read is correct everywhere and costs one small read per
// interval.
//
// An interval of zero disables reloading entirely (an explicit ReloadNow —
// SIGHUP, in this proxy — still works).
func (s *Store) Watch(ctx context.Context, interval time.Duration) {
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
			s.ReloadNow()
		}
	}
}

func loadFile(path string) (*Policy, error) {
	raw, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("cannot read policy file %s: %v", path, err)
	}
	policy, err := Parse(raw)
	if err != nil {
		return nil, fmt.Errorf("%s: %v", path, err)
	}
	return policy, nil
}
