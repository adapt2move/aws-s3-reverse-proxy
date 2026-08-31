package blobcache

import (
	"errors"
	"fmt"
	"io"
	"os"
	"sync"
	"sync/atomic"
)

// numShards is how far the index is split to keep readers off each other's
// locks. It has to be a power of two.
const numShards = 256

// entry is one live key. It never holds the key itself — see the note on
// hashKey — and it never holds the metadata either, because an index of ten
// million objects would then be measured in gigabytes.
type entry struct {
	h1, h2 uint64

	// data is where the payload lives, meta where the key and metadata live.
	// They are the same container for an inline record, and differ when the
	// payload was large enough to get its own file.
	data    *container
	dataOff int64
	dataLen int64

	meta    *container
	metaOff int64
	metaLen int32

	// recordBytes is what this entry costs in its meta container, so that
	// dropping it gives the right number back.
	recordBytes int64
}

type indexShard struct {
	mu sync.RWMutex
	m  map[uint64]*entry
}

// Store is a blob store on local disk. Use Open to create one.
type Store struct {
	opts Options

	shards [numShards]indexShard

	// Sends on ops are guarded by sendMu so that Close can guarantee no
	// writer is left holding a message nobody will read.
	sendMu  sync.RWMutex
	closing bool
	ops     chan *writeOp
	quit    chan struct{}
	stopped chan struct{}

	// Owned by the writer goroutine after Open returns.
	active     *activeSegment
	containers map[uint64]*container

	// reclaimable are containers that lost their last live entry, queued by
	// dropEntry so that freeing them does not mean walking every container
	// the store has.
	reclaimable []*container

	nextID   atomic.Uint64
	crashed  atomic.Bool
	entries  atomic.Int64
	disk     atomic.Int64
	segments atomic.Int64
	blobs    atomic.Int64
	failed   atomic.Pointer[error]

	counters struct {
		hits, misses, puts, deletes   atomic.Uint64
		evictions, evictedBytes       atomic.Uint64
		droppedWrites, corruptRecords atomic.Uint64
	}
}

// Open loads or creates a store in opts.Dir.
//
// Recovery is a replay of the log, so it costs a read of the segment that was
// being written when the process stopped and two small reads per segment that
// had been sealed before it. Anything that does not check out — a torn record
// at the tail, a blob file no record refers to, a file the store did not
// write — is discarded rather than repaired, and reported through opts.Logf.
func Open(opts Options) (*Store, error) {
	opts, err := opts.withDefaults()
	if err != nil {
		return nil, err
	}
	if err := os.MkdirAll(tempDir(opts.Dir), 0o700); err != nil {
		return nil, err
	}
	s := &Store{
		opts:       opts,
		ops:        make(chan *writeOp, opts.WriteQueue),
		quit:       make(chan struct{}),
		stopped:    make(chan struct{}),
		containers: make(map[uint64]*container),
	}
	for i := range s.shards {
		s.shards[i].m = make(map[uint64]*entry)
	}
	if err := s.recover(); err != nil {
		if s.active != nil {
			_ = s.active.close()
		}
		s.closeFiles()
		return nil, err
	}
	go s.writeLoop()
	return s, nil
}

// Get returns a reader for key. The reader holds the file it reads from open,
// so it keeps working even if the entry is evicted underneath it — close it
// when done.
func (s *Store) Get(key string) (*Reader, error) {
	h1, h2 := hashKey(key)
	sh := &s.shards[h1&(numShards-1)]

	sh.mu.RLock()
	e := sh.m[h1]
	if e == nil || e.h2 != h2 {
		sh.mu.RUnlock()
		s.counters.misses.Add(1)
		return nil, ErrNotFound
	}
	// Both references are taken while the shard lock is held, which is the
	// same lock eviction takes before it retires anything. Past this point
	// the containers cannot be unlinked out from under us.
	dataOK := e.data.acquire()
	metaOK := dataOK && (e.meta == e.data || e.meta.acquire())
	held := *e
	sh.mu.RUnlock()

	if !dataOK || !metaOK {
		if dataOK {
			e.data.release()
		}
		s.counters.misses.Add(1)
		return nil, ErrNotFound
	}

	meta := make([]byte, held.metaLen)
	err := held.meta.readAt(meta, held.metaOff)
	if held.meta != held.data {
		held.meta.release()
	}
	if err != nil {
		held.data.release()
		s.counters.misses.Add(1)
		return nil, fmt.Errorf("blobcache: reading metadata for a cached entry: %w", err)
	}

	now := s.opts.now().UnixNano()
	held.data.atime.Store(now)
	if held.meta != held.data {
		held.meta.atime.Store(now)
	}
	s.counters.hits.Add(1)
	return &Reader{
		c:      held.data,
		body:   io.NewSectionReader(held.data.rf, held.dataOff, held.dataLen),
		meta:   meta,
		offset: held.dataOff,
		size:   held.dataLen,
	}, nil
}

// Has reports whether key is present without reading anything from disk. It
// is the cheap half of Get, for a caller that only needs to decide.
func (s *Store) Has(key string) bool {
	h1, h2 := hashKey(key)
	sh := &s.shards[h1&(numShards-1)]
	sh.mu.RLock()
	e := sh.m[h1]
	sh.mu.RUnlock()
	return e != nil && e.h2 == h2
}

// Put starts writing key. The caller writes the payload to the returned
// Writer and calls Commit with the metadata to record beside it, or Abort to
// leave the store unchanged; Abort after a successful Commit does nothing, so
// it is safe to defer.
//
// sizeHint is the payload length if it is known and negative if it is not:
// it decides up front whether the payload is worth its own file, and lets the
// store refuse an oversized object before a byte of it is written.
//
// Nothing is visible to Get until Commit returns.
func (s *Store) Put(key string, sizeHint int64) (*Writer, error) {
	if err := s.writable(); err != nil {
		return nil, err
	}
	if key == "" {
		return nil, errors.New("blobcache: empty key")
	}
	if int64(len(key)) > int64(^uint32(0)) {
		return nil, errors.New("blobcache: key is absurdly long")
	}
	if sizeHint > s.opts.MaxObjectSize {
		return nil, ErrTooLarge
	}

	w := &Writer{s: s, key: key}
	if sizeHint > s.opts.InlineMaxSize {
		// The size is known and it is large, so skip the staging buffer
		// entirely and stream straight into a file of its own.
		blob, err := newBlobWriter(s.opts.Dir, s.nextID.Add(1), sizeHint)
		if err != nil {
			return nil, err
		}
		w.blob = blob
	}
	return w, nil
}

// Delete removes key. It is not an error for the key to be absent.
//
// The deletion is written to the log, so it survives a restart. With
// Options.SyncDeletes it also survives a crash, which is the difference
// between a cache that can be told an object changed and one that only
// believes it until the next power cut.
func (s *Store) Delete(key string) error {
	if err := s.writable(); err != nil {
		return err
	}
	op := takeOp()
	op.key = key
	op.header = recordHeader{Kind: kindTombstone, KeyLen: uint32(len(key))}
	op.record = encodeRecord(kindTombstone, key, nil, nil, 0, 0)
	op.sync = s.opts.SyncDeletes
	if err := s.submit(op); err != nil {
		return err
	}
	s.counters.deletes.Add(1)
	return nil
}

// Purge empties the store.
//
// It is the blunt instrument an operator reaches for when something outside
// this process changed the data underneath it and there is no list of which
// keys. Everything is unlinked; readers already streaming out of a file keep
// working until they close it.
func (s *Store) Purge() error {
	if err := s.writable(); err != nil {
		return err
	}
	op := takeOp()
	op.purge = true
	return s.submit(op)
}

// Stats reports what the store holds and what it has done since Open.
func (s *Store) Stats() Stats {
	return Stats{
		Entries:       int(s.entries.Load()),
		Bytes:         s.disk.Load(),
		Capacity:      s.opts.MaxBytes,
		Segments:      int(s.segments.Load()),
		Blobs:         int(s.blobs.Load()),
		Hits:          s.counters.hits.Load(),
		Misses:        s.counters.misses.Load(),
		Puts:          s.counters.puts.Load(),
		Deletes:       s.counters.deletes.Load(),
		Evictions:     s.counters.evictions.Load(),
		EvictedBytes:  s.counters.evictedBytes.Load(),
		DroppedWrites: s.counters.droppedWrites.Load(),
		Corrupt:       s.counters.corruptRecords.Load(),
	}
}

// Close stops the writer, seals the segment that was being written so the
// next Open does not have to read it back, and closes every file.
//
// Readers handed out by Get keep working until they are closed.
func (s *Store) Close() error {
	s.sendMu.Lock()
	already := s.closing
	s.closing = true
	s.sendMu.Unlock()
	if already {
		return nil
	}
	close(s.quit)
	<-s.stopped
	return nil
}

// writable reports whether the store is still accepting writes, either
// because it was closed or because the disk stopped cooperating. A store that
// cannot write keeps serving reads.
func (s *Store) writable() error {
	if err := s.failed.Load(); err != nil {
		return *err
	}
	s.sendMu.RLock()
	closed := s.closing
	s.sendMu.RUnlock()
	if closed {
		return ErrClosed
	}
	return nil
}

// submit hands a record to the writer goroutine and waits for it to be
// applied, so that a Commit that returns means a Get would find it.
//
// It never blocks on a full queue. A cache that makes the request feeding it
// wait for a disk has stopped being a cache, so the write is dropped and
// counted instead.
func (s *Store) submit(op *writeOp) error {
	s.sendMu.RLock()
	if s.closing {
		s.sendMu.RUnlock()
		releaseOp(op)
		return ErrClosed
	}
	select {
	case s.ops <- op:
	default:
		s.sendMu.RUnlock()
		releaseOp(op)
		s.counters.droppedWrites.Add(1)
		return ErrBusy
	}
	s.sendMu.RUnlock()

	<-op.done
	err := op.err
	releaseOp(op)
	return err
}

// opPool recycles the little envelopes mutations travel in, along with the
// channel each one is answered on. Both are per-write allocations on the
// hottest path in the package, and neither carries anything worth keeping.
var opPool = sync.Pool{New: func() any {
	// Buffered, so the writer goroutine hands back an answer without
	// waiting for the caller to be scheduled.
	return &writeOp{done: make(chan struct{}, 1)}
}}

func takeOp() *writeOp {
	op := opPool.Get().(*writeOp)
	op.key, op.record, op.blob, op.err = "", nil, nil, nil
	op.sync, op.purge = false, false
	op.header = recordHeader{}
	return op
}

func releaseOp(op *writeOp) {
	op.key, op.record, op.blob, op.err = "", nil, nil, nil
	opPool.Put(op)
}

// writeOp is one mutation on its way to the log.
type writeOp struct {
	key string

	// purge asks for the whole store to be emptied instead of for a record
	// to be appended.
	purge bool

	header recordHeader
	record []byte

	// blob is set when the payload was streamed into a file of its own and
	// the log only has to record where it went.
	blob *container

	sync bool

	err  error
	done chan struct{}
}

// writeLoop owns the active segment, the container set and every mutation of
// the index.
//
// Everything funnels through here on purpose. It makes the log ordered
// without a sequence number, lets a burst of commits collapse into one
// write(), and turns eviction into ordinary single-threaded code instead of a
// lock ordering problem between the index and the files under it.
func (s *Store) writeLoop() {
	defer close(s.stopped)
	batch := make([]*writeOp, 0, s.opts.BatchSize)
	for {
		select {
		case op := <-s.ops:
			batch = append(batch[:0], op)
			// Take whatever else is already queued: those commits are
			// waiting on a flush anyway, and folding them into one costs
			// nothing.
		drain:
			for len(batch) < s.opts.BatchSize {
				select {
				case next := <-s.ops:
					batch = append(batch, next)
				default:
					break drain
				}
			}
			s.applyBatch(batch)
		case <-s.quit:
			// Drain what is still queued before shutting down, so a commit
			// racing with Close gets an answer either way.
			for {
				select {
				case op := <-s.ops:
					batch = append(batch[:0], op)
					s.applyBatch(batch)
				default:
					s.shutdown()
					return
				}
			}
		}
	}
}

func (s *Store) applyBatch(batch []*writeOp) {
	needSync := false
	for _, op := range batch {
		if err := s.applyOne(op); err != nil {
			op.err = err
		}
		needSync = needSync || op.sync
	}
	// One flush for the whole batch. Without an fsync this is a copy into
	// the page cache, which is what makes the batching worth having: the
	// syscall, not the durability, is the cost being amortised.
	if err := s.active.flush(s.opts.WritebackEvery); err != nil {
		s.fail(err)
		for _, op := range batch {
			if op.err == nil {
				op.err = err
			}
		}
	} else if needSync {
		if err := s.active.sync(); err != nil {
			s.fail(err)
		}
	}
	for _, op := range batch {
		op.done <- struct{}{}
	}
	s.maintain()
}

// applyOne appends one record and updates the index to match.
func (s *Store) applyOne(op *writeOp) error {
	if err := s.failed.Load(); err != nil {
		if op.blob != nil {
			op.blob.retire()
		}
		return *err
	}
	if op.purge {
		return s.purgeAll()
	}
	offset, err := s.active.append(op.record)
	if err != nil {
		s.fail(err)
		if op.blob != nil {
			op.blob.retire()
		}
		return err
	}
	s.active.c.size.Store(s.active.end)
	s.disk.Add(int64(len(op.record)))

	if op.blob != nil {
		s.containers[op.blob.id] = op.blob
		s.blobs.Add(1)
		s.disk.Add(op.blob.size.Load())
	}
	s.apply(s.active.c, offset, op.header, op.key, op.blob)
	if op.header.Kind != kindTombstone {
		s.counters.puts.Add(1)
	}
	return nil
}

// purgeAll unlinks everything and starts a fresh log.
//
// The segment being appended to is replaced rather than emptied: a log has no
// way to say "ignore what came before" that a replay could not also get
// wrong, and starting a new file says it unambiguously.
func (s *Store) purgeAll() error {
	previous := s.active
	next, err := createSegment(s.opts.Dir, s.nextID.Add(1), s.opts.SegmentSize)
	if err != nil {
		return err
	}
	s.containers[next.c.id] = next.c
	s.segments.Add(1)
	s.active = next
	// No seal: the segment it would write an index into is about to be
	// removed.
	_ = previous.close()

	for id, c := range s.containers {
		if c == next.c {
			continue
		}
		s.retire(c)
		delete(s.containers, id)
	}
	s.reclaimable = s.reclaimable[:0]
	return nil
}

// apply is the one place the index changes, and it is shared by the writer
// goroutine and by recovery — replaying a segment and appending to it go
// through exactly the same code, so the two can never drift apart.
func (s *Store) apply(seg *container, offset int64, h recordHeader, key string, blob *container) {
	h1, h2 := hashKey(key)
	sh := &s.shards[h1&(numShards-1)]

	// The record is header, key, payload, metadata — so where the metadata
	// starts depends on whether the payload is in this segment at all.
	dataOff := offset + recordHeaderSize + int64(h.KeyLen)
	metaOff := dataOff
	if h.Kind == kindInline {
		metaOff += int64(h.DataLen)
	}
	var e *entry
	if h.Kind != kindTombstone {
		e = &entry{
			h1: h1, h2: h2,
			meta:        seg,
			metaOff:     metaOff,
			metaLen:     int32(h.MetaLen),
			dataLen:     int64(h.DataLen),
			recordBytes: h.size(),
		}
		switch h.Kind {
		case kindInline:
			e.data = seg
			e.dataOff = dataOff
		case kindBlobRef:
			e.data = blob
			e.dataOff = 0
		}
	}

	sh.mu.Lock()
	if old := sh.m[h1]; old != nil {
		s.dropEntry(old)
		delete(sh.m, h1)
	}
	if e != nil {
		sh.m[h1] = e
		s.entries.Add(1)
		seg.live.Add(e.recordBytes)
		seg.members = append(seg.members, e)
		if e.data != seg {
			e.data.members = append(e.data.members, e)
		}
	}
	sh.mu.Unlock()
}

// dropEntry gives back what an entry accounted for. The caller holds the
// entry's shard lock.
//
// Retiring the blob file here is what keeps a superseded large object from
// lingering: a blob file has exactly one referent by construction, so losing
// that referent makes it garbage immediately rather than at the next eviction.
func (s *Store) dropEntry(e *entry) {
	if e.meta.live.Add(-e.recordBytes) == 0 {
		s.markReclaimable(e.meta)
	}
	if e.data != nil && e.data != e.meta {
		e.data.live.Add(-e.dataLen)
		e.data.retire()
		s.markReclaimable(e.data)
	}
	s.entries.Add(-1)
}

// markReclaimable queues a container that has nothing left in it. Noting them
// as they empty is what keeps eviction off the critical path: a store holding
// a few hundred thousand blob files must not walk all of them after every
// batch just to discover that none of them is garbage.
func (s *Store) markReclaimable(c *container) {
	if s.active != nil && c == s.active.c {
		return
	}
	s.reclaimable = append(s.reclaimable, c)
}

// maintain rotates the segment when it is full and evicts when the store is
// over budget. It runs in the writer goroutine after every batch.
func (s *Store) maintain() {
	if s.failed.Load() != nil {
		return
	}
	if s.active.end >= s.opts.SegmentSize {
		if err := s.rotate(); err != nil {
			s.fail(err)
			return
		}
	}
	s.evict()
}

func (s *Store) rotate() error {
	// Sealing verifies nothing: these records were written by this process,
	// minutes ago, and are still in the page cache. Recovery is where the
	// checksums earn their keep.
	if err := s.active.seal(false); err != nil {
		return err
	}
	sealed := s.active.c
	sealed.size.Store(fileSize(sealed))
	next, err := createSegment(s.opts.Dir, s.nextID.Add(1), s.opts.SegmentSize)
	if err != nil {
		return err
	}
	s.containers[next.c.id] = next.c
	s.segments.Add(1)
	s.active = next
	if sealed.live.Load() == 0 {
		// Everything it held was superseded before it filled up. Now that
		// it is no longer the segment being appended to, it is just cost.
		s.markReclaimable(sealed)
	}
	return nil
}

// evictionSample is how many containers are looked at to choose a victim.
//
// Scanning every container to find the exact least recently used one costs
// more, the larger the cache gets, than the accuracy is worth — and a store
// with fewer containers than this samples all of them anyway, so the
// approximation only starts applying at the size where it has to.
const evictionSample = 32

// evict frees whole containers, never individual records.
//
// Two passes, in this order. First everything that is pure garbage — a
// container whose entries have all been superseded or deleted costs disk and
// holds nothing, so it goes regardless of budget. Then, while the store is
// still over its limits, the least recently used container of a sample, until
// it is not.
//
// Nothing is ever rewritten to compact it. That is the trade: some space
// wasted on dead records inside a live segment, in exchange for a store whose
// write amplification is exactly one.
func (s *Store) evict() {
	for _, c := range s.reclaimable {
		if _, ok := s.containers[c.id]; !ok || c == s.active.c || c.live.Load() > 0 {
			continue
		}
		s.retire(c)
		delete(s.containers, c.id)
	}
	s.reclaimable = s.reclaimable[:0]

	for s.overBudget() {
		victim := s.sampleVictim()
		if victim == nil {
			return
		}
		s.retire(victim)
		delete(s.containers, victim.id)
	}
}

// sampleVictim picks the least recently read of a bounded sample. Go
// randomises where a map range starts, which is exactly the sample wanted
// here.
//
// A container that has never been read has an access time of zero, so the
// tie-break by id makes an untouched store evict in the order it was written.
func (s *Store) sampleVictim() *container {
	var victim *container
	seen := 0
	for _, c := range s.containers {
		if c == s.active.c {
			continue
		}
		if victim == nil || c.atime.Load() < victim.atime.Load() ||
			(c.atime.Load() == victim.atime.Load() && c.id < victim.id) {
			victim = c
		}
		if seen++; seen >= evictionSample {
			break
		}
	}
	return victim
}

func (s *Store) overBudget() bool {
	if s.disk.Load() > s.opts.MaxBytes {
		return true
	}
	return s.opts.MaxEntries > 0 && int(s.entries.Load()) > s.opts.MaxEntries
}

// retire drops a container and everything the index still reaches through it.
func (s *Store) retire(c *container) {
	for _, e := range c.members {
		sh := &s.shards[e.h1&(numShards-1)]
		sh.mu.Lock()
		// A member that is no longer the entry the index holds was
		// superseded long ago and was accounted for at that point.
		if sh.m[e.h1] == e {
			s.dropEntry(e)
			delete(sh.m, e.h1)
		}
		sh.mu.Unlock()
	}
	c.members = nil
	size := c.size.Load()
	s.disk.Add(-size)
	if c.blob {
		s.blobs.Add(-1)
	} else {
		s.segments.Add(-1)
	}
	s.counters.evictions.Add(1)
	s.counters.evictedBytes.Add(uint64(size))
	c.retire()
}

// fail records that the disk stopped cooperating. Writes are refused from
// here on; reads keep working, because everything already written is still
// there and still checksummed.
func (s *Store) fail(err error) {
	if s.failed.Load() != nil {
		return
	}
	wrapped := fmt.Errorf("blobcache: write path failed, no longer caching: %w", err)
	s.failed.Store(&wrapped)
	s.opts.logf("blobcache: %v", err)
}

func (s *Store) shutdown() {
	if s.crashed.Load() {
		// A test asked for the shape a killed process leaves behind: no
		// seal, no fsync, no footer.
		s.closeFiles()
		return
	}
	if s.active != nil {
		if s.failed.Load() != nil {
			_ = s.active.close()
		} else if err := s.active.seal(false); err != nil {
			s.opts.logf("blobcache: sealing the active segment on close: %v", err)
			_ = s.active.close()
		}
	}
	s.closeFiles()
}

func (s *Store) closeFiles() {
	for _, c := range s.containers {
		// Drop the store's reference without unlinking: these files are the
		// cache, and the next Open is supposed to find them.
		c.release()
	}
	s.containers = nil
}

func fileSize(c *container) int64 {
	info, err := c.rf.Stat()
	if err != nil {
		return c.size.Load()
	}
	return info.Size()
}
