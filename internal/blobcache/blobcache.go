// Package blobcache is a size-bounded blob store on local disk.
//
// It is deliberately not a database. It stores opaque byte payloads under
// opaque string keys, hands back readers, and bounds how much disk it uses.
// It knows nothing about HTTP, S3, tenants or policy, and it never needs to:
// everything above it can be built on Get, Put and Delete.
//
// # What it is tuned for
//
// A network-attached volume — EBS, Ceph RBD, Longhorn — where an fsync is a
// round trip (0.5–2 ms) and so is a file create. Every design decision below
// follows from that one fact:
//
//   - Small payloads are appended to shared, sequentially written segment
//     files rather than stored one file per key. A Put costs one buffered
//     write, not create + write + rename + three inode updates.
//   - Nothing on the hot path is fsynced. A cache does not need durability;
//     it needs to never be wrong. Those are different properties, and only
//     the second one costs nothing.
//   - Correctness after a crash comes from per-record checksums. A record
//     whose CRC does not match — including one the filesystem zero-filled,
//     which is what ext4's delayed allocation leaves behind — is a miss, and
//     a miss is always safe because the truth lives upstream.
//   - Reclaim is segment-granular. Records are never rewritten, so the write
//     amplification of this store is 1.0: one unlink returns a whole segment.
//
// The two places an fsync does happen are the ones where it is amortised over
// enough bytes to disappear: sealing a full segment (once per SegmentSize),
// and committing a large blob to its own file (once per object, alongside
// megabytes of payload). Deletions are synced too, because a resurrected
// entry after a restart is the one kind of staleness this store can prevent
// on its own — see Options.SyncDeletes.
//
// # Durability contract
//
// A crash may lose any subset of recently written entries, and — with
// SyncDeletes off — any subset of recent deletions. The store must never
// return a payload that is not byte-for-byte what was written under that key.
// Everything in here exists to hold the second half of that sentence while
// giving up as much of the first half as it takes to stay fast.
//
// # Concurrency
//
// A Store is safe for concurrent use. Readers never block each other or a
// writer. All mutations are funnelled through one goroutine that owns the
// active segment, which is what makes the log ordered, lets writes batch into
// a single syscall, and keeps recovery a straight replay.
package blobcache

import (
	"errors"
	"hash/maphash"
	"time"
)

// Errors returned by a Store. All of them are expected in normal operation:
// a cache that refuses work is behaving, not failing.
var (
	// ErrNotFound is returned by Get for a key the store does not hold.
	ErrNotFound = errors.New("blobcache: not found")

	// ErrBusy is returned when the write queue is full. The caller should
	// drop the write and carry on — a cache that applies backpressure to
	// the request that feeds it has stopped being a cache.
	ErrBusy = errors.New("blobcache: write queue full")

	// ErrTooLarge is returned when a payload exceeds Options.MaxObjectSize,
	// either up front from the size hint or partway through a write whose
	// length was not known in advance.
	ErrTooLarge = errors.New("blobcache: object too large")

	// ErrClosed is returned once Close has been called.
	ErrClosed = errors.New("blobcache: store is closed")
)

// Options configures a Store. The zero value of every field is replaced by
// the default named in its comment, so Options{Dir: d} is a working
// configuration.
type Options struct {
	// Dir is the directory the store owns. It is created if missing, and
	// every file in it belongs to the store: anything unrecognised is
	// removed at Open.
	Dir string

	// MaxBytes caps the payload bytes held on disk. Default 1 GiB.
	//
	// It is a budget, not a hard ceiling: eviction runs after a write, so
	// the store transiently exceeds it by at most one object.
	MaxBytes int64

	// MaxEntries caps the number of live keys, which is what bounds the
	// in-memory index. Default 0, meaning only MaxBytes applies.
	//
	// Budget roughly 100 bytes of heap per entry.
	MaxEntries int

	// SegmentSize is how large a segment file grows before it is sealed and
	// a new one started. Default 256 MiB.
	//
	// It is the granularity of reclaim: bigger segments mean cheaper
	// eviction and coarser accounting.
	SegmentSize int64

	// InlineMaxSize is the payload size up to which a blob is appended into
	// a segment rather than given its own file. Default 1 MiB.
	//
	// Payloads at or below this size are buffered in memory before being
	// appended, so this is also the largest single buffer a write allocates.
	InlineMaxSize int64

	// MaxObjectSize is the largest payload the store accepts. Default
	// MaxBytes/8, so that no single object can evict most of the cache.
	MaxObjectSize int64

	// WriteQueue is how many commits may be waiting on the writer goroutine
	// before further ones fail with ErrBusy. Default 256.
	WriteQueue int

	// BatchSize is how many queued records the writer goroutine will fold
	// into one flush. Default 64.
	BatchSize int

	// SyncDeletes fsyncs the log after a batch containing a deletion.
	// Default true.
	//
	// This is the one sync on an otherwise sync-free path, and it buys
	// something the rest of the design cannot: without it, a crash can
	// resurrect a key that was deleted because the object behind it
	// changed. Deletions are rare next to reads and writes, so the cost is
	// paid on the operation that can least afford to be lost.
	SyncDeletes bool

	// WritebackEvery asks the kernel to start writing the active segment
	// back to disk every N appended bytes, on platforms that can express
	// that. Default 8 MiB; a negative value disables it.
	//
	// It is a hint, not a barrier — it waits for nothing. Without it, a
	// sustained write burst accumulates dirty pages until the kernel forces
	// synchronous writeback, and that stall lands on whatever request
	// happens to be in flight.
	WritebackEvery int64

	// Logf reports what the store repairs and discards on its own: corrupt
	// records dropped at recovery, orphaned files removed. Nil discards it.
	Logf func(format string, args ...any)

	// now is the clock, for tests.
	now func() time.Time
}

const (
	defaultMaxBytes       = 1 << 30
	defaultSegmentSize    = 256 << 20
	defaultInlineMaxSize  = 1 << 20
	defaultWriteQueue     = 256
	defaultBatchSize      = 64
	defaultWritebackEvery = 8 << 20
)

// withDefaults fills in the unset fields and rejects a configuration that
// cannot work, rather than one that merely performs badly.
func (o Options) withDefaults() (Options, error) {
	if o.Dir == "" {
		return o, errors.New("blobcache: no directory configured")
	}
	if o.MaxBytes <= 0 {
		o.MaxBytes = defaultMaxBytes
	}
	if o.SegmentSize <= 0 {
		o.SegmentSize = defaultSegmentSize
	}
	if o.InlineMaxSize <= 0 {
		o.InlineMaxSize = defaultInlineMaxSize
	}
	if o.MaxObjectSize <= 0 {
		o.MaxObjectSize = o.MaxBytes / 8
	}
	if o.WriteQueue <= 0 {
		o.WriteQueue = defaultWriteQueue
	}
	if o.BatchSize <= 0 {
		o.BatchSize = defaultBatchSize
	}
	if o.WritebackEvery == 0 {
		o.WritebackEvery = defaultWritebackEvery
	}
	if o.now == nil {
		o.now = time.Now
	}
	// An inline payload has to fit in a segment alongside its header, and a
	// segment that cannot hold one record would never make progress.
	if o.InlineMaxSize > o.SegmentSize/2 {
		return o, errors.New("blobcache: InlineMaxSize must be at most half of SegmentSize")
	}
	if o.MaxObjectSize < o.InlineMaxSize {
		// A store this small is a configuration mistake, not a tuning
		// choice: nothing would ever reach the inline path.
		o.MaxObjectSize = o.InlineMaxSize
	}
	return o, nil
}

func (o Options) logf(format string, args ...any) {
	if o.Logf != nil {
		o.Logf(format, args...)
	}
}

// Stats is a snapshot of what the store holds and what it has done. Counters
// are monotonic since Open; gauges describe the moment Stats was called.
type Stats struct {
	// Gauges.
	Entries  int   // live keys
	Bytes    int64 // bytes on disk, dead records included until reclaimed
	Capacity int64 // Options.MaxBytes
	Segments int   // segment files, including the active one
	Blobs    int   // single-payload files

	// Counters.
	Hits          uint64
	Misses        uint64
	Puts          uint64
	Deletes       uint64
	Evictions     uint64 // containers unlinked
	EvictedBytes  uint64
	DroppedWrites uint64 // commits refused with ErrBusy
	Corrupt       uint64 // records discarded at recovery
}

// Key hashing.
//
// The index is keyed by a 64-bit hash and stores a second, independently
// seeded one beside it, rather than storing the key. Keys are the largest
// thing an index of ten million entries would hold, and the pair is checked
// on every read: a first-hash collision is resolved without touching the
// disk, and the worst case is a spurious miss rather than a wrong answer.
var (
	seedPrimary   = maphash.MakeSeed()
	seedSecondary = maphash.MakeSeed()
)

func hashKey(key string) (primary, secondary uint64) {
	return maphash.String(seedPrimary, key), maphash.String(seedSecondary, key)
}
