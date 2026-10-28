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
// # MaxBytes is a ceiling
//
// The files the store has on disk never add up to more than MaxBytes. Every
// byte is accounted for from before it is written until after it is gone:
// a write claims its length before the first byte, making room by evicting
// first if it has to, and a file that leaves the index is counted until it
// has been unlinked — which, while a reader still has it open, is not until
// that reader closes it. A write that cannot be fitted under the ceiling is
// refused, and the object is simply not cached.
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
// the old files and removes them before it caches anything, rather than
// trusting them. Keeping entries across a restart would take a marker written
// at a clean shutdown and a scan of the directory at startup; neither exists,
// on purpose.
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
// accounting, and no file is opened, read, written or removed while it is
// held.
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

	// ErrNoRoom is returned when a write cannot be fitted under MaxBytes:
	// writes in flight already hold their allowance, or what is left on disk
	// is held open by readers. The answer is to not cache this one.
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

	// MaxBytes is the most disk the store's files ever occupy, counted in
	// whole filesystem blocks: the entries, files that left the index but
	// are still being read or removed, and writes in flight, which claim
	// their length before they write it. Default 1 GiB.
	//
	// The shard directories themselves are not counted; they take a block
	// each, about 1 MiB in all.
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

	// blockSize is the unit a file's length is rounded up to when it is
	// counted, which is what ext4 and XFS allocate on a volume formatted
	// with their defaults. A 200-byte object still takes a block, and
	// counting it as 200 bytes would let a cache of small objects use many
	// times its budget.
	blockSize = 4096

	// numShards spreads the files over this many directories, so creates and
	// unlinks do not all queue on one directory's lock.
	numShards = 256

	// metaAllowance is what Put claims for the metadata of an object whose
	// length it was told, before anybody knows what that metadata is: the
	// stored response headers, which are a few hundred bytes as a rule.
	metaAllowance = 1024
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
	Bytes    int64 // disk the store's files occupy, writes in flight included, in whole blocks
	Capacity int64 // Options.MaxBytes

	// Counters.
	Hits         uint64
	Misses       uint64
	Puts         uint64
	Deletes      uint64
	Evictions    uint64 // entries removed to stay within the limits
	EvictedBytes uint64
}

// entry is one key and the file that holds it.
type entry struct {
	key string
	id  uint64 // names the file; never reused

	size int64 // payload bytes, which start at offset 0
	meta int64 // metadata bytes, which follow the payload
	disk int64 // what the file occupies, in whole blocks

	// refs counts the readers that have the file open. The file leaves the
	// disk only once the entry has left the index and nobody is reading it.
	refs    int
	removed bool

	// orphaned marks an entry dropped because its file holds another key.
	// That file is not this entry's to remove.
	orphaned bool

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
	nextID uint64
	closed bool

	// Every byte the store can have on disk is in exactly one of these. A
	// write may start only once their sum is within MaxBytes.
	live     int64 // files the index points at
	held     int64 // files out of the index that a reader still has open
	removing int64 // files out of the index that are being unlinked
	reserved int64 // claimed by writes that have not committed

	// freed is signalled whenever removing shrinks, for a claim that is
	// waiting on a removal somebody else is in the middle of.
	freed *sync.Cond

	counters struct {
		hits, misses, puts, deletes atomic.Uint64
		evictions, evictedBytes     atomic.Uint64
	}
}

// Open creates a store in opts.Dir, emptying it first.
//
// What a previous process left there is removed before the store accepts a
// write, so that it never counts against MaxBytes without being counted. With
// a few thousand files that is instant; with hundreds of thousands it takes a
// few seconds.
func Open(opts Options) (*Store, error) {
	opts, err := opts.withDefaults()
	if err != nil {
		return nil, err
	}
	removed, err := prepareDir(opts.Dir)
	if err != nil {
		// Worth naming what is wrong rather than passing the syscall's own
		// message up. The usual cause is a volume mounted into a container
		// that runs as somebody other than root: the directory is there, it
		// just belongs to the wrong user, and "permission denied" on a path
		// nobody chose by hand does not say that.
		return nil, fmt.Errorf("%s must be writable by the user this process runs as: %w", opts.Dir, err)
	}
	if removed > 0 {
		opts.logf("filestore: removed %d entries a previous process left in %s", removed, opts.Dir)
	}
	s := &Store{opts: opts, index: make(map[string]*entry)}
	s.lru.next, s.lru.prev = &s.lru, &s.lru
	s.freed = sync.NewCond(&s.mu)
	return s, nil
}

// prepareDir makes dir an empty store directory: everything already in it is
// removed, and the shard directories are created fresh. The directory itself
// may be a mount point, so it is emptied rather than replaced.
func prepareDir(dir string) (int, error) {
	if err := os.MkdirAll(dir, 0o700); err != nil {
		return 0, err
	}
	leftovers, err := os.ReadDir(dir)
	if err != nil {
		return 0, err
	}
	for _, e := range leftovers {
		if err := os.RemoveAll(filepath.Join(dir, e.Name())); err != nil {
			return 0, err
		}
	}
	for i := 0; i < numShards; i++ {
		if err := os.Mkdir(filepath.Join(dir, fmt.Sprintf("%02x", i)), 0o700); err != nil {
			return 0, err
		}
	}
	return len(leftovers), nil
}

// path is where the file with this id lives.
func (s *Store) path(id uint64) string {
	return filepath.Join(s.opts.Dir, fmt.Sprintf("%02x", id%numShards), fmt.Sprintf("%016x", id))
}

// Get returns a reader for key. The reader holds the file, so it keeps working
// even if the entry is replaced, deleted or evicted underneath it — close it
// when done, because the file stays on disk, and counted, until then.
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
	e.refs++
	s.touchLocked(e)
	s.mu.Unlock()

	f, err := os.Open(s.path(e.id))
	if err != nil {
		s.unref(e)
		s.counters.misses.Add(1)
		return nil, fmt.Errorf("filestore: opening a cached entry: %w", err)
	}
	tail := make([]byte, e.meta+int64(len(key)))
	if _, err := f.ReadAt(tail, e.size); err != nil {
		_ = f.Close()
		s.unref(e)
		s.counters.misses.Add(1)
		return nil, fmt.Errorf("filestore: reading metadata for a cached entry: %w", err)
	}
	if string(tail[e.meta:]) != key {
		// The file was written for another key. Nothing in this package
		// should ever produce that, which is exactly why it is checked: the
		// cost is a comparison, and the alternative is serving one key's
		// payload as another's.
		_ = f.Close()
		s.opts.logf("filestore: %s does not hold the key its entry names; dropping the entry", s.path(e.id))
		s.orphan(e)
		s.unref(e)
		s.counters.misses.Add(1)
		return nil, ErrNotFound
	}
	s.counters.hits.Add(1)
	return &Reader{
		s:    s,
		e:    e,
		f:    f,
		body: io.NewSectionReader(f, 0, e.size),
		meta: tail[:e.meta:e.meta],
	}, nil
}

// Put starts writing key. The caller writes the payload to the returned
// Writer and calls Commit with the metadata to record beside it, or Abort to
// leave the store unchanged; Abort after a successful Commit does nothing, so
// it is safe to defer.
//
// sizeHint is the payload length if it is known and negative if it is not.
// A known length is claimed in full before Put returns, evicting what has to
// go to make room for it; an unknown one is claimed as it arrives.
//
// Nothing is visible to Get until Commit returns.
func (s *Store) Put(key string, sizeHint int64) (*Writer, error) {
	if key == "" {
		return nil, errors.New("filestore: empty key")
	}
	if sizeHint > s.opts.MaxObjectSize {
		return nil, ErrTooLarge
	}
	var claimed int64
	if sizeHint > 0 {
		// The metadata and the key follow the payload. Claiming room for
		// them now spares the commit a second claim in all but the rare case
		// of an object with unusually large metadata.
		claimed = blocks(sizeHint + int64(len(key)) + metaAllowance)
		if err := s.claim(claimed); err != nil {
			return nil, err
		}
	}
	s.mu.Lock()
	if s.closed {
		s.reserved -= claimed
		s.mu.Unlock()
		return nil, ErrClosed
	}
	s.nextID++
	id := s.nextID
	s.mu.Unlock()

	f, err := os.OpenFile(s.path(id), os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0o600)
	if err != nil {
		s.unclaim(claimed)
		return nil, err
	}
	return &Writer{s: s, key: key, id: id, f: f, claimed: claimed}, nil
}

// allowance is the most that writes in flight may hold between them: half the
// budget, or one largest object if that is more. Without it a burst of misses
// would evict the whole cache to make room for itself.
func (s *Store) allowance() int64 {
	if half := s.opts.MaxBytes / 2; half > s.opts.MaxObjectSize {
		return half
	}
	return s.opts.MaxObjectSize
}

// claim reserves n bytes of disk for a write, evicting the least recently
// used entries if that is what it takes to stay within MaxBytes, and returns
// only once the space is actually free.
//
// It refuses rather than wait for space that only someone else can free: when
// writes in flight already hold their allowance, or when what is left is held
// open by readers. Waiting there would make a request wait on another, and a
// cache must never do that.
func (s *Store) claim(n int64) error {
	if n <= 0 {
		return nil
	}
	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()
		return ErrClosed
	}
	if s.reserved+n > s.allowance() {
		s.mu.Unlock()
		return ErrNoRoom
	}
	// Files being removed are on their way out and do not need evicting
	// for; everything else does. When even evicting every entry would not
	// be enough, because readers hold the rest, nothing is evicted for a
	// write that is going to be refused anyway.
	if s.held+s.reserved+n > s.opts.MaxBytes {
		s.mu.Unlock()
		return ErrNoRoom
	}
	var gone []*entry
	for s.live+s.held+s.reserved+n > s.opts.MaxBytes {
		victim := s.lru.prev
		if victim == &s.lru {
			break
		}
		gone = append(gone, s.removeLocked(victim)...)
		s.counters.evictions.Add(1)
		s.counters.evictedBytes.Add(uint64(victim.disk))
	}
	if s.live+s.held+s.reserved+n > s.opts.MaxBytes {
		// Some of what was evicted is being read, and stays on disk until
		// its reader is done.
		s.mu.Unlock()
		s.unlink(gone)
		return ErrNoRoom
	}
	s.reserved += n
	s.mu.Unlock()

	s.unlink(gone)

	// The claim may be counting on a removal another goroutine is still in
	// the middle of. Starting to write before it finishes is the difference
	// between a budget and a ceiling, and it never takes longer than an
	// unlink.
	s.mu.Lock()
	for s.live+s.held+s.removing+s.reserved > s.opts.MaxBytes {
		s.freed.Wait()
	}
	s.mu.Unlock()
	return nil
}

// unclaim gives back disk a write claimed and never used. The caller has
// already removed whatever it wrote.
func (s *Store) unclaim(n int64) {
	if n <= 0 {
		return
	}
	s.mu.Lock()
	s.reserved -= n
	s.mu.Unlock()
}

// commit makes a finished file the entry for its key, replacing whatever was
// there. The file is already counted in full by what its writer claimed, so
// the store's total does not grow.
func (s *Store) commit(e *entry, claimed int64) error {
	s.mu.Lock()
	s.reserved -= claimed
	if s.closed {
		s.removing += e.disk
		s.mu.Unlock()
		s.unlink([]*entry{e})
		return ErrClosed
	}
	var gone []*entry
	if old := s.index[e.key]; old != nil {
		gone = append(gone, s.removeLocked(old)...)
	}
	s.index[e.key] = e
	s.pushFrontLocked(e)
	s.live += e.disk
	for s.opts.MaxEntries > 0 && len(s.index) > s.opts.MaxEntries {
		victim := s.lru.prev
		gone = append(gone, s.removeLocked(victim)...)
		s.counters.evictions.Add(1)
		s.counters.evictedBytes.Add(uint64(victim.disk))
	}
	s.mu.Unlock()

	s.unlink(gone)
	s.counters.puts.Add(1)
	return nil
}

// Delete removes key. It is not an error for the key to be absent.
//
// The entry leaves the index before this returns, and that cannot fail: a
// Get that starts afterwards misses. The file follows straight away, or when
// the last reader that has it open closes it.
func (s *Store) Delete(key string) error {
	s.mu.Lock()
	var gone []*entry
	if e := s.index[key]; e != nil {
		gone = s.removeLocked(e)
		s.counters.deletes.Add(1)
	}
	s.mu.Unlock()
	s.unlink(gone)
	return nil
}

// orphan drops an entry from the index without ever removing its file, which
// holds another key and is not this entry's to remove. The next start
// discards it along with everything else.
func (s *Store) orphan(e *entry) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.index[e.key] != e {
		return
	}
	delete(s.index, e.key)
	s.unlinkFromLRU(e)
	s.live -= e.disk
	e.removed, e.orphaned = true, true
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
	var gone []*entry
	for s.lru.prev != &s.lru {
		gone = append(gone, s.removeLocked(s.lru.prev)...)
	}
	s.mu.Unlock()
	s.unlink(gone)
	return nil
}

// MaxObjectSize is the largest payload this store accepts, after the defaults
// have been applied.
func (s *Store) MaxObjectSize() int64 { return s.opts.MaxObjectSize }

// Stats reports what the store holds and what it has done since Open.
func (s *Store) Stats() Stats {
	s.mu.Lock()
	entries := len(s.index)
	bytes := s.live + s.held + s.removing + s.reserved
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

// Close stops the store. The files that are still entries stay where they
// are: the next Open discards them, and readers handed out by Get keep
// working until they are closed.
func (s *Store) Close() error {
	s.mu.Lock()
	s.closed = true
	s.mu.Unlock()
	return nil
}

// removeLocked takes an entry out of the index and the recency list. Its file
// is returned for unlinking when nobody is reading it; otherwise the last
// reader to close it removes it.
func (s *Store) removeLocked(e *entry) []*entry {
	delete(s.index, e.key)
	s.unlinkFromLRU(e)
	s.live -= e.disk
	e.removed = true
	if e.refs > 0 {
		s.held += e.disk
		return nil
	}
	s.removing += e.disk
	return []*entry{e}
}

// unref is what closing a reader does: the last one out of a file that has
// left the index removes it.
func (s *Store) unref(e *entry) {
	s.mu.Lock()
	e.refs--
	var gone []*entry
	if e.refs == 0 && e.removed && !e.orphaned {
		s.held -= e.disk
		s.removing += e.disk
		gone = []*entry{e}
	}
	s.mu.Unlock()
	s.unlink(gone)
}

// unlink removes files that have left the index and gives their disk back.
// The bytes stay counted until the file is actually gone, which is what keeps
// MaxBytes a ceiling.
func (s *Store) unlink(gone []*entry) {
	if len(gone) == 0 {
		return
	}
	var freed int64
	for _, e := range gone {
		if err := os.Remove(s.path(e.id)); err != nil && !errors.Is(err, os.ErrNotExist) {
			s.opts.logf("filestore: removing %s: %v", s.path(e.id), err)
		}
		freed += e.disk
	}
	s.mu.Lock()
	s.removing -= freed
	s.freed.Broadcast()
	s.mu.Unlock()
}

func (s *Store) unlinkFromLRU(e *entry) {
	e.prev.next, e.next.prev = e.next, e.prev
	e.prev, e.next = nil, nil
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
	s.unlinkFromLRU(e)
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
		return 0
	}
	return (n + blockSize - 1) / blockSize * blockSize
}
