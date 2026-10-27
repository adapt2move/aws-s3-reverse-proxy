package filestore

import (
	"bytes"
	"fmt"
	"io"
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

// settle waits until every file the store has let go of is gone, so a test can
// look at the directory without racing the background removal.
func (s *Store) settle() {
	s.reaper.mu.Lock()
	for s.reaper.pending > 0 {
		s.reaper.idle.Wait()
	}
	s.reaper.mu.Unlock()
}

// objectFiles lists the files under the shard directories.
func objectFiles(t *testing.T, s *Store) []string {
	t.Helper()
	s.settle()
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

	assert.Len(t, objectFiles(t, s), 1, "the old file is gone from the directory")
	body, err := io.ReadAll(old)
	require.NoError(t, err)
	assert.Equal(t, "version one", string(body), "but still readable through a reader that had it open")
	assert.Equal(t, 1, s.Stats().Entries)
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
	assert.Equal(t, Stats{Capacity: s.opts.MaxBytes, Hits: 1, Misses: 1, Puts: 1, Deletes: 1}, s.Stats())
	assert.Empty(t, objectFiles(t, s))

	body, err := io.ReadAll(reading)
	require.NoError(t, err)
	assert.Equal(t, "payload", string(body))

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

// Writes in flight have claimed disk nothing counts yet. They are bounded on
// their own, at half the budget or one largest object.
func TestWritesInFlightAreBounded(t *testing.T) {
	s := open(t, Options{MaxBytes: 64 * blockSize, MaxObjectSize: 16 * blockSize})

	first, err := s.Put("first", 16*blockSize)
	require.NoError(t, err)
	second, err := s.Put("second", 16*blockSize)
	require.NoError(t, err)
	_, err = s.Put("third", 16*blockSize)
	assert.ErrorIs(t, err, ErrNoRoom)

	first.Abort()
	third, err := s.Put("third", 16*blockSize)
	require.NoError(t, err, "an ended write gives its claim back")

	_, err = second.Write(make([]byte, 16*blockSize))
	require.NoError(t, err)
	require.NoError(t, second.Commit(nil))
	fourth, err := s.Put("fourth", 16*blockSize)
	require.NoError(t, err, "and so does a committed one")
	third.Abort()
	fourth.Abort()
	assert.Zero(t, s.reserved)
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

	second.settle()
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
