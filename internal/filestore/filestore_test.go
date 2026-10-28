package filestore

import (
	"bytes"
	"fmt"
	"io"
	"math/rand"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func open(t *testing.T, opts Options) *Store {
	t.Helper()
	if opts.Dir == "" {
		opts.Dir = t.TempDir()
	}
	opts.Logf = func(format string, args ...any) { t.Logf(format, args...) }
	s, err := Open(opts)
	require.NoError(t, err)
	t.Cleanup(func() { _ = s.Close() })
	return s
}

func put(t *testing.T, s *Store, key string, payload, meta []byte) {
	t.Helper()
	w, err := s.Put(key, int64(len(payload)))
	require.NoError(t, err)
	defer w.Abort()
	_, err = w.Write(payload)
	require.NoError(t, err)
	require.NoError(t, w.Commit(meta))
}

// get reads key back whole, and reports false for a miss.
func get(t *testing.T, s *Store, key string) (payload, meta []byte, ok bool) {
	t.Helper()
	r, err := s.Get(key)
	if err != nil {
		return nil, nil, false
	}
	defer r.Close()
	payload, err = io.ReadAll(r)
	require.NoError(t, err)
	return payload, r.Meta(), true
}

// objectFiles lists the files under the shard directories.
func objectFiles(t *testing.T, s *Store) []string {
	t.Helper()
	var files []string
	shards, err := filepath.Glob(filepath.Join(s.opts.Dir, "??", "*"))
	require.NoError(t, err)
	for _, f := range shards {
		files = append(files, filepath.Base(f))
	}
	return files
}

func TestPutAndGet(t *testing.T) {
	s := open(t, Options{})
	put(t, s, "bucket/tenant/a.parquet", []byte("the payload"), []byte(`{"etag":"x"}`))

	r, err := s.Get("bucket/tenant/a.parquet")
	require.NoError(t, err)
	defer r.Close()
	assert.Equal(t, int64(len("the payload")), r.Size())
	assert.Equal(t, `{"etag":"x"}`, string(r.Meta()))

	// A ranged read is how most of this cache is consumed, so the payload has
	// to be seekable and stop exactly where it ends, metadata and key after
	// it notwithstanding.
	part := make([]byte, 7)
	_, err = r.ReadAt(part, 4)
	require.NoError(t, err)
	assert.Equal(t, "payload", string(part))
	_, err = r.Seek(4, io.SeekStart)
	require.NoError(t, err)
	rest, err := io.ReadAll(r)
	require.NoError(t, err)
	assert.Equal(t, "payload", string(rest))
}

func TestMissForAnUnknownKey(t *testing.T) {
	s := open(t, Options{})
	_, err := s.Get("bucket/tenant/nothing")
	assert.ErrorIs(t, err, ErrNotFound)
}

// Tenant isolation is decided by whoever builds the key; what the store owes
// it is that two different keys are never one entry. Nothing is hashed, so
// keys that share everything but a prefix, a suffix or a single byte all stay
// apart.
func TestKeysAreComparedWhole(t *testing.T) {
	s := open(t, Options{})
	keys := []string{
		"bucket/tenant-a/data/a.parquet",
		"bucket/tenant-b/data/a.parquet",
		"bucket/tenant-a/data/a.parquet/",
		"bucket/tenant-a/data/a.parquef",
		"bucket/tenant-a/data/a.parque",
		"other/tenant-a/data/a.parquet",
	}
	for _, key := range keys {
		put(t, s, key, []byte("payload of "+key), []byte(key))
	}
	for _, key := range keys {
		payload, meta, ok := get(t, s, key)
		require.True(t, ok, key)
		assert.Equal(t, "payload of "+key, string(payload))
		assert.Equal(t, key, string(meta))
	}

	require.NoError(t, s.Delete(keys[0]))
	_, _, ok := get(t, s, keys[0])
	assert.False(t, ok)
	for _, key := range keys[1:] {
		payload, _, ok := get(t, s, key)
		require.True(t, ok, "deleting one key leaves %s alone", key)
		assert.Equal(t, "payload of "+key, string(payload))
	}
}

// Each file carries the key it was written for. An entry that somehow named
// another key's file is a miss, never that key's payload.
func TestAFileHoldingAnotherKeyIsAMiss(t *testing.T) {
	s := open(t, Options{})
	put(t, s, "bucket/tenant-a/x", []byte("for tenant a"), []byte("{}"))
	put(t, s, "bucket/tenant-b/x", []byte("for tenant b"), []byte("{}"))

	s.mu.Lock()
	a, b := s.index["bucket/tenant-a/x"], s.index["bucket/tenant-b/x"]
	a.id, a.size, a.meta = b.id, b.size, b.meta
	s.mu.Unlock()

	_, err := s.Get("bucket/tenant-a/x")
	assert.ErrorIs(t, err, ErrNotFound)
	assert.Equal(t, 1, s.Stats().Entries, "the misdirected entry is dropped")

	payload, _, ok := get(t, s, "bucket/tenant-b/x")
	require.True(t, ok, "and the file it pointed at still belongs to its own key")
	assert.Equal(t, "for tenant b", string(payload))
}

func TestANewVersionReplacesTheOld(t *testing.T) {
	s := open(t, Options{})
	put(t, s, "k", []byte("version one"), []byte("1"))

	// A download that started on the old version finishes on it.
	old, err := s.Get("k")
	require.NoError(t, err)
	defer old.Close()

	put(t, s, "k", []byte("version two"), []byte("2"))
	payload, meta, ok := get(t, s, "k")
	require.True(t, ok)
	assert.Equal(t, "version two", string(payload))
	assert.Equal(t, "2", string(meta))

	// The old file is out of the index but still on disk, and still counted,
	// for as long as the download that has it open runs.
	assert.Equal(t, 1, s.Stats().Entries)
	assert.Len(t, objectFiles(t, s), 2)
	assert.Equal(t, int64(2*blockSize), s.Stats().Bytes)
	body, err := io.ReadAll(old)
	require.NoError(t, err)
	assert.Equal(t, "version one", string(body))

	require.NoError(t, old.Close())
	assert.Len(t, objectFiles(t, s), 1, "the last reader out removes it")
	assert.Equal(t, int64(blockSize), s.Stats().Bytes)
}

func TestDeleteTakesEffectImmediately(t *testing.T) {
	s := open(t, Options{})
	put(t, s, "k", []byte("payload"), nil)
	reading, err := s.Get("k")
	require.NoError(t, err)
	defer reading.Close()

	require.NoError(t, s.Delete("k"))
	_, err = s.Get("k")
	assert.ErrorIs(t, err, ErrNotFound)
	stats := s.Stats()
	assert.Zero(t, stats.Entries)
	assert.Equal(t, int64(blockSize), stats.Bytes, "a file still being read stays counted")

	body, err := io.ReadAll(reading)
	require.NoError(t, err)
	assert.Equal(t, "payload", string(body))
	require.NoError(t, reading.Close())
	assert.Empty(t, objectFiles(t, s))
	assert.Equal(t, Stats{Capacity: s.opts.MaxBytes, Hits: 1, Misses: 1, Puts: 1, Deletes: 1}, s.Stats())

	require.NoError(t, s.Delete("never-stored"), "deleting an absent key is not an error")
}

// Disk is counted in whole blocks, because that is what a file occupies: a
// cache of 200-byte objects counted at 200 bytes each would overrun its budget
// many times over.
func TestSmallObjectsAreCountedInBlocks(t *testing.T) {
	s := open(t, Options{})
	for i := 0; i < 5; i++ {
		put(t, s, fmt.Sprintf("k%d", i), []byte("tiny"), nil)
	}
	put(t, s, "big", make([]byte, blockSize+1), nil)
	assert.Equal(t, int64(5*blockSize+2*blockSize), s.Stats().Bytes)
	assert.Len(t, objectFiles(t, s), 6)
}

func TestEvictionTakesTheLeastRecentlyUsed(t *testing.T) {
	s := open(t, Options{MaxBytes: 4 * blockSize, MaxObjectSize: blockSize})
	for _, key := range []string{"a", "b", "c", "d"} {
		put(t, s, key, []byte(key), nil)
	}
	_, _, ok := get(t, s, "a") // a is now the most recently used
	require.True(t, ok)

	put(t, s, "e", []byte("e"), nil)
	_, _, ok = get(t, s, "b")
	assert.False(t, ok, "b was the least recently used")
	for _, key := range []string{"a", "c", "d", "e"} {
		_, _, ok := get(t, s, key)
		assert.True(t, ok, key)
	}
	stats := s.Stats()
	assert.Equal(t, uint64(1), stats.Evictions)
	assert.Equal(t, uint64(blockSize), stats.EvictedBytes)
	assert.Equal(t, int64(4*blockSize), stats.Bytes)
	assert.Len(t, objectFiles(t, s), 4)
}

func TestMaxEntriesBoundsTheIndex(t *testing.T) {
	s := open(t, Options{MaxEntries: 2})
	for _, key := range []string{"a", "b", "c"} {
		put(t, s, key, []byte(key), nil)
	}
	assert.Equal(t, 2, s.Stats().Entries)
	_, _, ok := get(t, s, "a")
	assert.False(t, ok)
}

func TestAnOversizedObjectIsRefused(t *testing.T) {
	s := open(t, Options{MaxBytes: 64 * blockSize, MaxObjectSize: 2 * blockSize})

	_, err := s.Put("announced", 2*blockSize+1)
	assert.ErrorIs(t, err, ErrTooLarge)

	// Announced as unknown, found out on the way.
	w, err := s.Put("streamed", -1)
	require.NoError(t, err)
	_, err = w.Write(make([]byte, 2*blockSize))
	require.NoError(t, err)
	_, err = w.Write([]byte{1})
	assert.ErrorIs(t, err, ErrTooLarge)
	assert.ErrorIs(t, w.Commit(nil), ErrTooLarge)
	w.Abort()

	_, _, ok := get(t, s, "streamed")
	assert.False(t, ok)
	assert.Empty(t, objectFiles(t, s), "nothing is left behind")
}

// A write claims its whole length before its first byte, and room is made by
// evicting first: a full cache never holds more than MaxBytes, not even while
// it is taking a new object in.
func TestAWriteMakesRoomBeforeItWrites(t *testing.T) {
	s := open(t, Options{MaxBytes: 64 * blockSize, MaxObjectSize: 16 * blockSize})
	const size = 16*blockSize - 2*metaAllowance // leaves room in the last block for key and metadata
	for i := 0; i < 4; i++ {
		put(t, s, fmt.Sprintf("k%d", i), make([]byte, size), nil)
	}
	require.Equal(t, int64(64*blockSize), s.Stats().Bytes, "full")

	first, err := s.Put("new-1", size)
	require.NoError(t, err)
	_, _, ok := get(t, s, "k0")
	assert.False(t, ok, "the least recently used entry made room before anything was written")
	assert.Equal(t, int64(64*blockSize), s.Stats().Bytes, "and the claim fills exactly what it freed")

	second, err := s.Put("new-2", size)
	require.NoError(t, err)

	// Writes in flight hold at most half the budget between them, so a burst
	// of misses cannot evict the whole cache to make room for itself.
	_, err = s.Put("new-3", size)
	assert.ErrorIs(t, err, ErrNoRoom)
	for _, key := range []string{"k2", "k3"} {
		_, _, ok := get(t, s, key)
		assert.True(t, ok, "%s was not evicted for a write that was refused anyway", key)
	}
	assert.Equal(t, uint64(2), s.Stats().Evictions)

	_, err = first.Write(make([]byte, size))
	require.NoError(t, err)
	require.NoError(t, first.Commit(nil))
	second.Abort()
	assert.Zero(t, s.reserved, "a committed write and an aborted one both give their claim back")
	assert.LessOrEqual(t, s.Stats().Bytes, int64(64*blockSize))
}

// An evicted file that a download still has open stays on disk until the
// download ends. It counts against the ceiling for as long, so making room
// past it takes the next entry instead.
func TestAFileStillBeingReadStaysCounted(t *testing.T) {
	s := open(t, Options{MaxBytes: 4 * blockSize, MaxObjectSize: blockSize})
	for _, key := range []string{"a", "b", "c", "d"} {
		put(t, s, key, []byte(key), nil)
	}
	reading, err := s.Get("a")
	require.NoError(t, err)
	require.NoError(t, s.Delete("a"))

	put(t, s, "e", []byte("e"), nil)
	_, _, ok := get(t, s, "b")
	assert.False(t, ok, "b went, because a's file is still there")
	assert.Len(t, objectFiles(t, s), 4, "c, d, e and the one being read")
	assert.Equal(t, int64(4*blockSize), s.Stats().Bytes)

	require.NoError(t, reading.Close())
	assert.Len(t, objectFiles(t, s), 3)
	assert.Equal(t, int64(3*blockSize), s.Stats().Bytes)
}

// What the directory holds, counted the way the store counts it, while nothing
// can change the accounting underneath. Files being written grow without the
// lock, but only into space their writer has already claimed.
func (s *Store) diskInUse(t *testing.T) int64 {
	s.mu.Lock()
	defer s.mu.Unlock()
	var total int64
	shards, err := filepath.Glob(filepath.Join(s.opts.Dir, "??"))
	require.NoError(t, err)
	for _, shard := range shards {
		files, err := os.ReadDir(shard)
		require.NoError(t, err)
		for _, f := range files {
			info, err := f.Info()
			if err != nil {
				continue // removed between the listing and the stat
			}
			total += blocks(info.Size())
		}
	}
	return total
}

// The ceiling under concurrent writers of known and unknown length, readers
// that hold files open across evictions, and deletes. The directory is
// measured throughout, and never holds more than MaxBytes.
func TestTheCeilingHoldsUnderLoad(t *testing.T) {
	const ceiling = 64 * blockSize
	s := open(t, Options{MaxBytes: ceiling, MaxObjectSize: 8 * blockSize})

	stop := make(chan struct{})
	var peak int64
	sampled := make(chan struct{})
	go func() {
		defer close(sampled)
		for {
			select {
			case <-stop:
				return
			default:
			}
			if used := s.diskInUse(t); used > peak {
				peak = used
			}
		}
	}()

	var wg sync.WaitGroup
	for g := 0; g < 8; g++ {
		wg.Add(1)
		go func(g int) {
			defer wg.Done()
			rng := rand.New(rand.NewSource(int64(g)))
			var holding []*Reader
			for i := 0; i < 300; i++ {
				key := fmt.Sprintf("k%d", rng.Intn(40))
				switch op := rng.Intn(10); {
				case op < 5:
					payload := make([]byte, 1+rng.Intn(8*blockSize-64))
					hint := int64(len(payload))
					if op == 0 {
						hint = -1 // length unknown, claimed as it arrives
					}
					w, err := s.Put(key, hint)
					if err != nil {
						continue
					}
					for off := 0; off < len(payload); off += 3000 {
						end := min(off+3000, len(payload))
						if _, err := w.Write(payload[off:end]); err != nil {
							break
						}
					}
					_ = w.Commit([]byte("{}"))
					w.Abort()
				case op < 8:
					if r, err := s.Get(key); err == nil {
						holding = append(holding, r)
					}
					if len(holding) > 3 {
						_ = holding[0].Close()
						holding = holding[1:]
					}
				default:
					_ = s.Delete(key)
				}
			}
			for _, r := range holding {
				_ = r.Close()
			}
		}(g)
	}
	wg.Wait()
	close(stop)
	<-sampled

	t.Logf("peak %d of %d blocks", peak/blockSize, ceiling/blockSize)
	assert.LessOrEqual(t, peak, int64(ceiling))
	assert.LessOrEqual(t, s.diskInUse(t), int64(ceiling))
	assert.Equal(t, s.Stats().Bytes, s.diskInUse(t), "what is counted is what is on disk")
	assert.Zero(t, s.reserved)
	assert.Zero(t, s.held)
	assert.Zero(t, s.removing)
}

func TestAbortLeavesNothing(t *testing.T) {
	s := open(t, Options{})
	w, err := s.Put("k", 7)
	require.NoError(t, err)
	_, err = w.Write([]byte("payload"))
	require.NoError(t, err)
	w.Abort()
	w.Abort()

	_, _, ok := get(t, s, "k")
	assert.False(t, ok)
	assert.Empty(t, objectFiles(t, s))
}

func TestPurgeEmptiesTheStore(t *testing.T) {
	s := open(t, Options{})
	put(t, s, "a", []byte("a"), nil)
	put(t, s, "b", []byte("b"), nil)
	require.NoError(t, s.Purge())

	_, _, ok := get(t, s, "a")
	assert.False(t, ok)
	assert.Equal(t, 0, s.Stats().Entries)
	assert.Zero(t, s.Stats().Bytes)
	assert.Empty(t, objectFiles(t, s))

	put(t, s, "a", []byte("again"), nil)
	payload, _, ok := get(t, s, "a")
	require.True(t, ok)
	assert.Equal(t, "again", string(payload))
}

// Every start is a cold start: what the previous process left is discarded,
// not read. The directory itself stays, because on Kubernetes it is a mount
// point.
func TestOpenStartsEmpty(t *testing.T) {
	dir := t.TempDir()
	first := open(t, Options{Dir: dir})
	put(t, first, "k", []byte("from the first process"), nil)
	require.NoError(t, first.Close())
	require.NoError(t, os.WriteFile(filepath.Join(dir, "stray"), []byte("x"), 0o600))

	second := open(t, Options{Dir: dir})
	_, _, ok := get(t, second, "k")
	assert.False(t, ok)

	// The new process numbers its files from the start again, so this lands
	// on the very name the old entry had.
	put(t, second, "k", []byte("from the second process"), nil)
	payload, _, ok := get(t, second, "k")
	require.True(t, ok)
	assert.Equal(t, "from the second process", string(payload))

	names, err := os.ReadDir(dir)
	require.NoError(t, err)
	assert.Len(t, names, numShards, "only the shard directories remain")
	for _, n := range names {
		assert.True(t, n.IsDir() && len(n.Name()) == 2, n.Name())
	}
}

func TestAnUnusableDirectoryIsNamed(t *testing.T) {
	blocked := filepath.Join(t.TempDir(), "a-file")
	require.NoError(t, os.WriteFile(blocked, nil, 0o600))
	_, err := Open(Options{Dir: filepath.Join(blocked, "cache")})
	require.Error(t, err)
	assert.Contains(t, err.Error(), filepath.Join(blocked, "cache"))
}

func TestAClosedStoreRefusesWork(t *testing.T) {
	s := open(t, Options{})
	put(t, s, "k", []byte("payload"), nil)
	w, err := s.Put("late", 4)
	require.NoError(t, err)
	_, err = w.Write([]byte("late"))
	require.NoError(t, err)

	require.NoError(t, s.Close())
	require.NoError(t, s.Close())

	assert.ErrorIs(t, w.Commit(nil), ErrClosed)
	_, err = s.Get("k")
	assert.ErrorIs(t, err, ErrClosed)
	_, err = s.Put("k", 1)
	assert.ErrorIs(t, err, ErrClosed)
	assert.ErrorIs(t, s.Purge(), ErrClosed)
	assert.NotContains(t, objectFiles(t, s), fmt.Sprintf("%016x", 2), "a commit refused at close leaves no file")
}

// Concurrent writers, readers and deletes over a handful of shared keys. Every
// read that succeeds must return a payload written for the key it asked for:
// whatever else happens, no key is ever answered with another key's bytes.
func TestConcurrentUseNeverCrossesKeys(t *testing.T) {
	s := open(t, Options{MaxBytes: 32 * blockSize, MaxObjectSize: 4 * blockSize})
	keys := []string{"bucket/tenant-a/f", "bucket/tenant-b/f", "bucket/tenant-a/g", "bucket/tenant-b/g"}

	var wg sync.WaitGroup
	for g := 0; g < 8; g++ {
		wg.Add(1)
		go func(g int) {
			defer wg.Done()
			for i := 0; i < 200; i++ {
				key := keys[(g+i)%len(keys)]
				switch i % 4 {
				case 0, 1:
					payload := []byte(fmt.Sprintf("%s|%d|%d|", key, g, i))
					payload = append(payload, bytes.Repeat([]byte{'.'}, i*37%(3*blockSize))...)
					w, err := s.Put(key, int64(len(payload)))
					if err != nil {
						continue
					}
					_, _ = w.Write(payload)
					_ = w.Commit([]byte(key))
					w.Abort()
				case 2:
					r, err := s.Get(key)
					if err != nil {
						continue
					}
					body, err := io.ReadAll(r)
					_ = r.Close()
					if assert.NoError(t, err) {
						assert.True(t, strings.HasPrefix(string(body), key+"|"), "%s answered with %q", key, body[:min(len(body), 40)])
						assert.Equal(t, key, string(r.Meta()))
					}
				case 3:
					_ = s.Delete(key)
				}
			}
		}(g)
	}
	wg.Wait()

	stats := s.Stats()
	assert.LessOrEqual(t, stats.Bytes, int64(32*blockSize))
	assert.Len(t, objectFiles(t, s), stats.Entries, "every file on disk is an entry, and the reverse")
	assert.Zero(t, s.reserved)
}
