// Package filestore is a size-bounded blob store on local disk, one file per
// object.
//
// It stores opaque payloads under opaque string keys, hands back readers, and
// bounds how much disk it uses. It knows nothing about HTTP, S3, tenants or
// policy, and it never needs to: everything above it can be built on Get, Put
// and Delete.
//
// # What it is built for
//
// A local NVMe volume that belongs to one process for as long as that process
// lives — on Kubernetes, an emptyDir. Creating a file there costs
// microseconds, so nothing in here works around it: every object is a file of
// its own, every write goes straight into it, and nothing is ever fsynced.
//
// # Every start is a cold start
//
// Open empties the directory. Nothing an earlier process wrote is read back,
// which is what lets this package do without everything a persistent store
// needs: no recovery, no on-disk index, no checksums to tell a torn file from
// a whole one. A file only matters while the process that wrote it is alive,
// and within that process the page cache is coherent whether or not anything
// reached the disk.
//
// A restart therefore costs a cold cache. On an emptyDir that is what a new
// pod costs anyway, and a container that restarts inside the same pod finds
// the old files and throws them away rather than trusting them. Keeping
// entries across a restart would take a marker written at a clean shutdown and
// a scan of the directory at startup; neither exists, on purpose.
//
// # Keys
//
// The index maps the whole key to its entry. Nothing is hashed, so two keys
// that differ in a single byte are two entries, and which tenant a key belongs
// to is decided entirely by whoever builds it. Files are named by a sequence
// number rather than by anything derived from the key, and a name is never
// reused: a new version of a key is a new file. Each file also carries the key
// it was written for, and Get compares it, so even a bug that pointed an entry
// at the wrong file would produce a miss rather than somebody else's payload.
//
// # Concurrency
//
// A Store is safe for concurrent use. One mutex guards the index and the
// accounting, and no file is opened, read or written while it is held. Files
// are removed by a background goroutine; a reader that opened one before that
// keeps reading it until it closes, because an unlinked file stays readable
// through a descriptor that is still open.
package filestore

import (
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
)

// Errors returned by a Store. All of them are expected in normal operation: a
// cache that refuses work is behaving, not failing.
var (
	// ErrNotFound is returned by Get for a key the store does not hold.
	ErrNotFound = errors.New("filestore: not found")

	// ErrNoRoom is returned when a write would put the bytes in flight past
	// their allowance. Eviction cannot help: it reclaims bytes that were
	// committed, and these are bytes that are spoken for. The answer is to
	// not cache this one.
	ErrNoRoom = errors.New("filestore: no room within the size limit")

	// ErrTooLarge is returned when a payload exceeds Options.MaxObjectSize,
	// either up front from the size hint or partway through a write whose
	// length was not known in advance.
	ErrTooLarge = errors.New("filestore: object too large")

	// ErrClosed is returned once Close has been called.
	ErrClosed = errors.New("filestore: store is closed")
)

// Options configures a Store. The zero value of every field is replaced by the
// default named in its comment, so Options{Dir: d} is a working configuration.
type Options struct {
	// Dir is the directory the store owns. It is created if missing and
	// emptied by Open: every file in it belongs to the store, and nothing a
	// previous process left there is kept.
	Dir string

	// MaxBytes caps the disk the committed files occupy, counted in whole
	// filesystem blocks. Default 1 GiB.
	//
	// It is a budget rather than a hard ceiling. Eviction runs after a
	// commit, so the store briefly exceeds it by at most one object; files
	// that were evicted are unlinked in the background, so their space comes
	// back a moment after it stops being counted; and writes in flight hold
	// disk they have not committed, bounded separately at half this budget or
	// one MaxObjectSize if that is more.
	MaxBytes int64

	// MaxEntries caps the number of keys, which is what bounds the in-memory
	// index. Default 0, meaning only MaxBytes applies.
	//
	// Budget roughly 150 bytes of heap per entry plus the length of its key.
	MaxEntries int

	// MaxObjectSize is the largest payload the store accepts. Default
	// MaxBytes/8, so that no single object can evict most of the cache, and
	// never more than MaxBytes.
	MaxObjectSize int64

	// Logf reports what the store discards on its own. Nil discards it.
	Logf func(format string, args ...any)
}

const (
	defaultMaxBytes = 1 << 30

	// blockSize is what a file is assumed to occupy at least, and the unit
	// its size is rounded up to. A 200-byte object still takes a block, and
	// counting it as 200 bytes would let a cache of small objects use many
	// times its budget.
	blockSize = 4096

	// numShards spreads the files over this many directories, so creates and
	// unlinks do not all queue on one directory's lock.
	numShards = 256
)

func (o Options) withDefaults() (Options, error) {
	if o.Dir == "" {
		return o, errors.New("filestore: no directory configured")
	}
	if o.MaxBytes <= 0 {
		o.MaxBytes = defaultMaxBytes
	}
	if o.MaxObjectSize <= 0 {
		o.MaxObjectSize = o.MaxBytes / 8
	}
	if o.MaxObjectSize > o.MaxBytes {
		o.MaxObjectSize = o.MaxBytes
	}
	return o, nil
}

// Stats is a snapshot of what the store holds and what it has done. Counters
// are monotonic since Open; gauges describe the moment Stats was called.
type Stats struct {
	// Gauges.
	Entries  int   // live keys
	Bytes    int64 // disk the live entries occupy, in whole blocks
	Capacity int64 // Options.MaxBytes

	// Counters.
	Hits         uint64
	Misses       uint64
	Puts         uint64
	Deletes      uint64
	Evictions    uint64 // entries removed to stay within the limits
	EvictedBytes uint64
}

// entry is one live key and the file that holds it.
type entry struct {
	key string
	id  uint64 // names the file; never reused

	size int64 // payload bytes, which start at offset 0
	meta int64 // metadata bytes, which follow the payload
	disk int64 // what the file occupies, in whole blocks

	// prev and next place the entry in the recency list: most recently used
	// at the front, next to be evicted at the back.
	prev, next *entry
}

// Store is a blob store on local disk. Use Open to create one.
type Store struct {
	opts Options

	mu     sync.Mutex
	index  map[string]*entry
	lru    entry // sentinel: lru.next is the most recent, lru.prev the oldest
	bytes  int64
	nextID uint64
	closed bool

	// reserved is disk that writes in flight have claimed but not yet
	// committed. Without it MaxBytes would not bound anything while many
	// large objects are being written at once: nothing would count them
	// until each one committed.
	reserved int64

	reaper reaper

	counters struct {
		hits, misses, puts, deletes atomic.Uint64
		evictions, evictedBytes     atomic.Uint64
	}
}

// Open creates a store in opts.Dir, emptying it first.
//
// What a previous process left there is moved aside and removed in the
// background, so a large leftover directory does not hold up the start.
func Open(opts Options) (*Store, error) {
	opts, err := opts.withDefaults()
	if err != nil {
		return nil, err
	}
	discard, err := prepareDir(opts.Dir)
	if err != nil {
		// Worth naming what is wrong rather than passing the syscall's own
		// message up. The usual cause is a volume mounted into a container
		// that runs as somebody other than root: the directory is there, it
		// just belongs to the wrong user, and "permission denied" on a path
		// nobody chose by hand does not say that.
		return nil, fmt.Errorf("%s must be writable by the user this process runs as: %w", opts.Dir, err)
	}
	s := &Store{opts: opts, index: make(map[string]*entry)}
	s.lru.next, s.lru.prev = &s.lru, &s.lru
	s.reaper.start()
	if discard != "" {
		opts.logf("filestore: discarding what a previous process left in %s", opts.Dir)
		s.reaper.remove(discard)
	}
	return s, nil
}

// prepareDir makes dir an empty store directory: everything already in it is
// moved into one directory to be removed later, and the shard directories are
// created fresh. It returns that directory, or "" when there was nothing to
// move.
func prepareDir(dir string) (string, error) {
	if err := os.MkdirAll(dir, 0o700); err != nil {
		return "", err
	}
	leftovers, err := os.ReadDir(dir)
	if err != nil {
		return "", err
	}
	discard := ""
	if len(leftovers) > 0 {
		// The directory itself may be a mount point, so it is emptied rather
		// than replaced. A leading dot keeps the name clear of the shards.
		if discard, err = os.MkdirTemp(dir, ".discard-"); err != nil {
			return "", err
		}
		for _, e := range leftovers {
			if err := os.Rename(filepath.Join(dir, e.Name()), filepath.Join(discard, e.Name())); err != nil {
				return "", err
			}
		}
	}
	for i := 0; i < numShards; i++ {
		if err := os.Mkdir(filepath.Join(dir, fmt.Sprintf("%02x", i)), 0o700); err != nil {
			return "", err
		}
	}
	return discard, nil
}

// path is where the file with this id lives.
func (s *Store) path(id uint64) string {
	return filepath.Join(s.opts.Dir, fmt.Sprintf("%02x", id%numShards), fmt.Sprintf("%016x", id))
}

// Get returns a reader for key. The reader holds the file open, so it keeps
// working even if the entry is replaced, deleted or evicted underneath it —
// close it when done.
func (s *Store) Get(key string) (*Reader, error) {
	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()
		return nil, ErrClosed
	}
	e := s.index[key]
	if e == nil {
		s.mu.Unlock()
		s.counters.misses.Add(1)
		return nil, ErrNotFound
	}
	s.touchLocked(e)
	id, size, metaLen := e.id, e.size, e.meta
	s.mu.Unlock()

	f, err := os.Open(s.path(id))
	if err != nil {
		// Evicted or replaced between the lookup and the open, and already
		// removed. That is an ordinary miss.
		s.counters.misses.Add(1)
		return nil, ErrNotFound
	}
	tail := make([]byte, metaLen+int64(len(key)))
	if _, err := f.ReadAt(tail, size); err != nil {
		_ = f.Close()
		s.counters.misses.Add(1)
		return nil, fmt.Errorf("filestore: reading metadata for a cached entry: %w", err)
	}
	if string(tail[metaLen:]) != key {
		// The file was written for another key. Nothing in this package
		// should ever produce that, which is exactly why it is checked: the
		// cost is a comparison, and the alternative is serving one key's
		// payload as another's.
		_ = f.Close()
		s.opts.logf("filestore: %s does not hold the key its entry names; dropping the entry", s.path(id))
		s.forgetIfCurrent(key, id)
		s.counters.misses.Add(1)
		return nil, ErrNotFound
	}
	s.counters.hits.Add(1)
	return &Reader{
		f:    f,
		body: io.NewSectionReader(f, 0, size),
		meta: tail[:metaLen:metaLen],
		size: size,
	}, nil
}

// Put starts writing key. The caller writes the payload to the returned
// Writer and calls Commit with the metadata to record beside it, or Abort to
// leave the store unchanged; Abort after a successful Commit does nothing, so
// it is safe to defer.
//
// sizeHint is the payload length if it is known and negative if it is not. It
// lets the store refuse an oversized object before a byte of it is written,
// and claim the disk it will need up front.
//
// Nothing is visible to Get until Commit returns.
func (s *Store) Put(key string, sizeHint int64) (*Writer, error) {
	if key == "" {
		return nil, errors.New("filestore: empty key")
	}
	if sizeHint > s.opts.MaxObjectSize {
		return nil, ErrTooLarge
	}
	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()
		return nil, ErrClosed
	}
	var reserved int64
	if sizeHint > 0 {
		if !s.reserveLocked(sizeHint) {
			s.mu.Unlock()
			return nil, ErrNoRoom
		}
		reserved = sizeHint
	}
	s.nextID++
	id := s.nextID
	s.mu.Unlock()

	f, err := os.OpenFile(s.path(id), os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0o600)
	if err != nil {
		s.release(reserved)
		return nil, err
	}
	return &Writer{s: s, key: key, id: id, f: f, reserved: reserved}, nil
}

// reserveLocked claims disk for a write that has not been committed yet, and
// reports whether the store can afford it.
//
// What it bounds is the writes in flight, not the store as a whole. Committed
// bytes are eviction's problem and eviction handles them; these are bytes
// that are spoken for and that eviction therefore cannot reclaim — so checking
// them against the whole budget would turn a full cache into one that refuses
// everything instead of one that makes room.
//
// The allowance is half the budget, or one largest object if that is more, so
// that a single object of the maximum size can always start.
func (s *Store) reserveLocked(n int64) bool {
	allowance := s.opts.MaxBytes / 2
	if allowance < s.opts.MaxObjectSize {
		allowance = s.opts.MaxObjectSize
	}
	if s.reserved+n > allowance {
		return false
	}
	s.reserved += n
	return true
}

func (s *Store) reserve(n int64) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.reserveLocked(n)
}

func (s *Store) release(n int64) {
	if n <= 0 {
		return
	}
	s.mu.Lock()
	s.reserved -= n
	s.mu.Unlock()
}

// commit makes a finished file the entry for its key, replacing whatever was
// there, and evicts until the store is back within its limits.
func (s *Store) commit(e *entry, reserved int64) error {
	s.mu.Lock()
	s.reserved -= reserved
	if s.closed {
		s.mu.Unlock()
		s.reaper.remove(s.path(e.id))
		return ErrClosed
	}
	var gone []string
	if old := s.index[e.key]; old != nil {
		s.removeLocked(old)
		gone = append(gone, s.path(old.id))
	}
	s.index[e.key] = e
	s.pushFrontLocked(e)
	s.bytes += e.disk
	gone = append(gone, s.evictLocked()...)
	s.mu.Unlock()

	s.reaper.remove(gone...)
	s.counters.puts.Add(1)
	return nil
}

// Delete removes key. It is not an error for the key to be absent.
//
// The entry leaves the index before this returns, and that cannot fail: a
// Get that starts afterwards misses. The file follows in the background.
func (s *Store) Delete(key string) error {
	s.mu.Lock()
	e := s.index[key]
	if e != nil {
		s.removeLocked(e)
	}
	s.mu.Unlock()
	if e != nil {
		s.reaper.remove(s.path(e.id))
		s.counters.deletes.Add(1)
	}
	return nil
}

// forgetIfCurrent drops key from the index only if its entry still names the
// file with this id, so that a stale observation cannot drop a newer version.
//
// The file is left where it is. Whatever it holds is not this entry's to
// remove, and the next start discards it along with everything else.
func (s *Store) forgetIfCurrent(key string, id uint64) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if e := s.index[key]; e != nil && e.id == id {
		s.removeLocked(e)
	}
}

// Purge empties the store.
//
// It is the blunt instrument an operator reaches for when something outside
// this process changed the data and there is no list of which keys. Readers
// already streaming out of a file keep working until they close it.
func (s *Store) Purge() error {
	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()
		return ErrClosed
	}
	gone := make([]string, 0, len(s.index))
	for _, e := range s.index {
		gone = append(gone, s.path(e.id))
	}
	s.index = make(map[string]*entry)
	s.lru.next, s.lru.prev = &s.lru, &s.lru
	s.bytes = 0
	s.mu.Unlock()
	s.reaper.remove(gone...)
	return nil
}

// MaxObjectSize is the largest payload this store accepts, after the defaults
// have been applied.
func (s *Store) MaxObjectSize() int64 { return s.opts.MaxObjectSize }

// Stats reports what the store holds and what it has done since Open.
func (s *Store) Stats() Stats {
	s.mu.Lock()
	entries, bytes := len(s.index), s.bytes
	s.mu.Unlock()
	return Stats{
		Entries:      entries,
		Bytes:        bytes,
		Capacity:     s.opts.MaxBytes,
		Hits:         s.counters.hits.Load(),
		Misses:       s.counters.misses.Load(),
		Puts:         s.counters.puts.Load(),
		Deletes:      s.counters.deletes.Load(),
		Evictions:    s.counters.evictions.Load(),
		EvictedBytes: s.counters.evictedBytes.Load(),
	}
}

// Close stops the store and waits for the files already queued for removal to
// go. The files that are still entries stay where they are: the next Open
// discards them, and readers handed out by Get keep working until they are
// closed.
func (s *Store) Close() error {
	s.mu.Lock()
	already := s.closed
	s.closed = true
	s.mu.Unlock()
	if !already {
		s.reaper.stop()
	}
	return nil
}

// evictLocked removes entries from the back of the recency list until the
// store is within its limits, and returns the files to remove.
func (s *Store) evictLocked() []string {
	var gone []string
	for s.bytes > s.opts.MaxBytes || (s.opts.MaxEntries > 0 && len(s.index) > s.opts.MaxEntries) {
		victim := s.lru.prev
		if victim == &s.lru {
			break
		}
		s.removeLocked(victim)
		gone = append(gone, s.path(victim.id))
		s.counters.evictions.Add(1)
		s.counters.evictedBytes.Add(uint64(victim.disk))
	}
	return gone
}

// removeLocked takes an entry out of the index and the recency list and gives
// back the disk it accounted for.
func (s *Store) removeLocked(e *entry) {
	delete(s.index, e.key)
	e.prev.next, e.next.prev = e.next, e.prev
	e.prev, e.next = nil, nil
	s.bytes -= e.disk
}

func (s *Store) pushFrontLocked(e *entry) {
	e.prev, e.next = &s.lru, s.lru.next
	s.lru.next.prev = e
	s.lru.next = e
}

// touchLocked marks an entry as just used.
func (s *Store) touchLocked(e *entry) {
	if s.lru.next == e {
		return
	}
	e.prev.next, e.next.prev = e.next, e.prev
	s.pushFrontLocked(e)
}

func (o Options) logf(format string, args ...any) {
	if o.Logf != nil {
		o.Logf(format, args...)
	}
}

// blocks rounds a file length up to what it occupies on disk.
func blocks(n int64) int64 {
	if n <= 0 {
		return blockSize
	}
	return (n + blockSize - 1) / blockSize * blockSize
}
