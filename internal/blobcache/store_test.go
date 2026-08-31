package blobcache

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// open builds a store with test-sized limits. The defaults are measured in
// hundreds of megabytes, which no test should have to write.
func open(t *testing.T, dir string, tune ...func(*Options)) *Store {
	t.Helper()
	opts := Options{
		Dir:           dir,
		MaxBytes:      4 << 20,
		SegmentSize:   64 << 10,
		InlineMaxSize: 8 << 10,
		MaxObjectSize: 1 << 20,
		SyncDeletes:   true,
		Logf:          func(format string, args ...any) { t.Logf("store: "+format, args...) },
	}
	for _, f := range tune {
		f(&opts)
	}
	s, err := Open(opts)
	require.NoError(t, err)
	return s
}

// put is the whole write path in one call, for the tests that are about
// something else.
func put(t *testing.T, s *Store, key string, meta, data []byte) {
	t.Helper()
	w, err := s.Put(key, meta, int64(len(data)))
	require.NoError(t, err)
	defer w.Abort()
	_, err = w.Write(data)
	require.NoError(t, err)
	require.NoError(t, w.Commit())
}

// mustGet reads an entry back in full.
func mustGet(t *testing.T, s *Store, key string) (meta, data []byte) {
	t.Helper()
	r, err := s.Get(key)
	require.NoError(t, err, "key %q should be cached", key)
	defer r.Close()
	data, err = io.ReadAll(r)
	require.NoError(t, err)
	require.Equal(t, r.Size(), int64(len(data)), "payload length should match the recorded size")
	return r.Meta(), data
}

func mustMiss(t *testing.T, s *Store, key string) {
	t.Helper()
	_, err := s.Get(key)
	assert.ErrorIs(t, err, ErrNotFound, "key %q should not be cached", key)
}

func payload(n int, seed byte) []byte {
	b := make([]byte, n)
	for i := range b {
		b[i] = seed + byte(i%251)
	}
	return b
}

func TestRoundTripInlineAndBlob(t *testing.T) {
	s := open(t, t.TempDir())
	defer s.Close()

	// One payload comfortably inside the staging buffer and one well past
	// it, so both storage tiers are exercised through the same API.
	small, large := payload(1024, 1), payload(64<<10, 2)
	put(t, s, "small", []byte("meta-small"), small)
	put(t, s, "large", []byte("meta-large"), large)

	meta, data := mustGet(t, s, "small")
	assert.Equal(t, "meta-small", string(meta))
	assert.Equal(t, small, data)

	meta, data = mustGet(t, s, "large")
	assert.Equal(t, "meta-large", string(meta))
	assert.Equal(t, large, data)

	assert.Equal(t, 1, s.Stats().Blobs, "the large payload should have a file of its own")
	mustMiss(t, s, "absent")
}

func TestEmptyPayloadAndEmptyMeta(t *testing.T) {
	s := open(t, t.TempDir())
	defer s.Close()

	put(t, s, "empty", nil, nil)
	meta, data := mustGet(t, s, "empty")
	assert.Empty(t, meta)
	assert.Empty(t, data)
	assert.True(t, s.Has("empty"))
}

func TestOverwriteReplacesPayload(t *testing.T) {
	s := open(t, t.TempDir())
	defer s.Close()

	put(t, s, "k", []byte("v1"), payload(100, 1))
	put(t, s, "k", []byte("v2"), payload(200, 2))

	meta, data := mustGet(t, s, "k")
	assert.Equal(t, "v2", string(meta))
	assert.Equal(t, payload(200, 2), data)
	assert.Equal(t, 1, s.Stats().Entries, "an overwrite is one entry, not two")
}

func TestOverwriteFreesTheOldBlobFile(t *testing.T) {
	dir := t.TempDir()
	s := open(t, dir)
	defer s.Close()

	put(t, s, "k", nil, payload(32<<10, 1))
	put(t, s, "k", nil, payload(32<<10, 2))

	// The superseded blob has exactly one referent by construction, so
	// losing it should make the file garbage immediately rather than at the
	// next eviction.
	assert.Equal(t, 1, s.Stats().Blobs)
	blobs, err := filepath.Glob(filepath.Join(dir, "blob-*.dat"))
	require.NoError(t, err)
	assert.Len(t, blobs, 1, "the replaced payload's file should be gone")
}

func TestDeleteRemovesEntry(t *testing.T) {
	s := open(t, t.TempDir())
	defer s.Close()

	put(t, s, "k", nil, payload(64, 1))
	require.NoError(t, s.Delete("k"))
	mustMiss(t, s, "k")
	assert.False(t, s.Has("k"))

	// Deleting something absent is not an error: the caller is invalidating
	// a key, not asserting it was there.
	assert.NoError(t, s.Delete("never-existed"))
}

func TestSizeHintUnderstatesPayload(t *testing.T) {
	s := open(t, t.TempDir())
	defer s.Close()

	// An upstream that announces nothing, or lies, still has to end up
	// somewhere: the write spills out of the staging buffer into a file of
	// its own without the caller doing anything about it.
	want := payload(40<<10, 7)
	w, err := s.Put("streamed", []byte("m"), -1)
	require.NoError(t, err)
	defer w.Abort()
	_, err = io.Copy(w, bytes.NewReader(want))
	require.NoError(t, err)
	require.NoError(t, w.Commit())

	_, got := mustGet(t, s, "streamed")
	assert.Equal(t, want, got)
	assert.Equal(t, 1, s.Stats().Blobs)
}

func TestOversizedPayloadIsRefused(t *testing.T) {
	s := open(t, t.TempDir(), func(o *Options) { o.MaxObjectSize = 16 << 10 })
	defer s.Close()

	_, err := s.Put("declared", nil, 32<<10)
	assert.ErrorIs(t, err, ErrTooLarge, "a size hint over the limit should be refused before any bytes are written")

	// One that does not declare its size is refused the moment it grows
	// past the limit, so a single object can never displace the cache.
	w, err := s.Put("undeclared", nil, -1)
	require.NoError(t, err)
	defer w.Abort()
	_, err = w.Write(payload(32<<10, 3))
	assert.ErrorIs(t, err, ErrTooLarge)
	assert.ErrorIs(t, w.Commit(), ErrTooLarge)
	mustMiss(t, s, "undeclared")
}

func TestAbortLeavesNothingBehind(t *testing.T) {
	dir := t.TempDir()
	s := open(t, dir)
	defer s.Close()

	w, err := s.Put("k", nil, 64<<10)
	require.NoError(t, err)
	_, err = w.Write(payload(64<<10, 1))
	require.NoError(t, err)
	w.Abort()

	mustMiss(t, s, "k")
	leftovers, err := filepath.Glob(filepath.Join(dir, "tmp", "*"))
	require.NoError(t, err)
	assert.Empty(t, leftovers, "an aborted write should not leave a temporary file")

	// Abort after Commit is a no-op, which is what makes `defer w.Abort()`
	// the right way to use a Writer.
	w2, err := s.Put("k2", nil, 8)
	require.NoError(t, err)
	defer w2.Abort()
	_, err = w2.Write([]byte("12345678"))
	require.NoError(t, err)
	require.NoError(t, w2.Commit())
	w2.Abort()
	_, data := mustGet(t, s, "k2")
	assert.Equal(t, "12345678", string(data))
}

func TestReaderSeekAndReadAt(t *testing.T) {
	s := open(t, t.TempDir())
	defer s.Close()

	want := payload(4096, 5)
	put(t, s, "k", nil, want)

	r, err := s.Get("k")
	require.NoError(t, err)
	defer r.Close()

	// http.ServeContent needs both of these to serve a Range request out of
	// a payload that lives in the middle of a shared file.
	at := make([]byte, 100)
	_, err = r.ReadAt(at, 1000)
	require.NoError(t, err)
	assert.Equal(t, want[1000:1100], at)

	_, err = r.Seek(4000, io.SeekStart)
	require.NoError(t, err)
	tail, err := io.ReadAll(r)
	require.NoError(t, err)
	assert.Equal(t, want[4000:], tail)

	f, offset, length := r.Payload()
	assert.NotNil(t, f, "the backing file should be reachable for a sendfile copy")
	assert.Equal(t, int64(len(want)), length)
	direct := make([]byte, length)
	_, err = f.ReadAt(direct, offset)
	require.NoError(t, err)
	assert.Equal(t, want, direct, "the reported range should be the payload and nothing else")
}

func TestClosedStoreRefusesWrites(t *testing.T) {
	s := open(t, t.TempDir())
	put(t, s, "k", nil, payload(16, 1))
	require.NoError(t, s.Close())

	_, err := s.Put("k2", nil, 16)
	assert.ErrorIs(t, err, ErrClosed)
	assert.ErrorIs(t, s.Delete("k"), ErrClosed)
	assert.NoError(t, s.Close(), "closing twice should be harmless")
}

func TestStatsTrackWork(t *testing.T) {
	s := open(t, t.TempDir())
	defer s.Close()

	put(t, s, "a", nil, payload(128, 1))
	put(t, s, "b", nil, payload(128, 2))
	mustGet(t, s, "a")
	mustMiss(t, s, "zzz")
	require.NoError(t, s.Delete("b"))

	st := s.Stats()
	assert.Equal(t, uint64(1), st.Hits)
	assert.Equal(t, uint64(1), st.Misses)
	assert.Equal(t, uint64(2), st.Puts)
	assert.Equal(t, uint64(1), st.Deletes)
	assert.Equal(t, 1, st.Entries)
	assert.Greater(t, st.Bytes, int64(0))
	assert.Equal(t, int64(4<<20), st.Capacity)
}

func TestKeysThatCollideOnTheFirstHash(t *testing.T) {
	s := open(t, t.TempDir())
	defer s.Close()

	// The index is keyed by one hash and verified against a second one, so
	// a key that is absent must never be answered with a different key's
	// payload. Exhaustive proof is not on offer here; what this checks is
	// that the verification is actually consulted.
	put(t, s, "alpha", nil, []byte("A"))
	h1, _ := hashKey("alpha")
	sh := &s.shards[h1&(numShards-1)]
	sh.mu.Lock()
	sh.m[h1].h2++ // pretend the second hash belongs to some other key
	sh.mu.Unlock()

	mustMiss(t, s, "alpha")
}

func TestErrorsAreDistinguishable(t *testing.T) {
	// Callers branch on these, so they have to stay comparable with
	// errors.Is rather than by message.
	for _, err := range []error{ErrNotFound, ErrBusy, ErrTooLarge, ErrClosed} {
		assert.True(t, errors.Is(fmt.Errorf("wrapped: %w", err), err))
	}
}

func TestOpenRejectsImpossibleOptions(t *testing.T) {
	_, err := Open(Options{})
	assert.Error(t, err, "a store needs a directory")

	_, err = Open(Options{Dir: t.TempDir(), SegmentSize: 4 << 10, InlineMaxSize: 4 << 10})
	assert.Error(t, err, "an inline payload that cannot share a segment is a misconfiguration")
}

func TestForeignFilesAreRemoved(t *testing.T) {
	dir := t.TempDir()
	require.NoError(t, os.WriteFile(filepath.Join(dir, "notes.txt"), []byte("hi"), 0o600))

	s := open(t, dir)
	defer s.Close()

	// The store owns this directory outright: anything it did not write
	// would make its size budget a guess.
	_, err := os.Stat(filepath.Join(dir, "notes.txt"))
	assert.True(t, os.IsNotExist(err), "a stranger's file should not survive Open")
}

func TestPurgeEmptiesTheStore(t *testing.T) {
	dir := t.TempDir()
	s := open(t, dir)

	put(t, s, "small", nil, payload(256, 1))
	put(t, s, "large", nil, payload(48<<10, 2))
	require.Greater(t, s.Stats().Entries, 0)

	// An open reader must survive the purge: it is streaming bytes to
	// somebody, and the object it is streaming was correct when it started.
	held, err := s.Get("large")
	require.NoError(t, err)
	defer held.Close()

	require.NoError(t, s.Purge())
	assert.Equal(t, 0, s.Stats().Entries)
	assert.Equal(t, 0, s.Stats().Blobs)
	mustMiss(t, s, "small")
	mustMiss(t, s, "large")

	body, err := io.ReadAll(held)
	require.NoError(t, err)
	assert.Equal(t, payload(48<<10, 2), body)

	// And the store is immediately usable again.
	put(t, s, "after", nil, payload(64, 3))
	_, data := mustGet(t, s, "after")
	assert.Equal(t, payload(64, 3), data)

	require.NoError(t, s.Close())
	s = open(t, dir)
	defer s.Close()
	mustMiss(t, s, "small")
	_, data = mustGet(t, s, "after")
	assert.Equal(t, payload(64, 3), data)
}

func TestServeContentServesRangesFromTheStore(t *testing.T) {
	s := open(t, t.TempDir())
	defer s.Close()

	// A workload of many ranged reads over the same object — DuckDB's httpfs
	// reading a Parquet footer and then row groups is the case in mind — only
	// benefits from a cache if a range can be answered out of it. The Reader
	// is a ReadSeeker precisely so http.ServeContent can do that, conditional
	// requests and 416s included, without this package knowing what HTTP is.
	want := payload(8192, 11)
	put(t, s, "object.parquet", []byte(`{"etag":"\"abc123\""}`), want)

	handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		reader, err := s.Get("object.parquet")
		if err != nil {
			http.Error(w, "miss", http.StatusNotFound)
			return
		}
		defer reader.Close()
		w.Header().Set("ETag", `"abc123"`)
		http.ServeContent(w, r, "object.parquet", time.Time{}, reader)
	})
	srv := httptest.NewServer(handler)
	defer srv.Close()

	get := func(t *testing.T, rangeHeader string) *http.Response {
		t.Helper()
		req, err := http.NewRequest(http.MethodGet, srv.URL, nil)
		require.NoError(t, err)
		if rangeHeader != "" {
			req.Header.Set("Range", rangeHeader)
		}
		resp, err := http.DefaultClient.Do(req)
		require.NoError(t, err)
		return resp
	}

	t.Run("a range in the middle", func(t *testing.T) {
		resp := get(t, "bytes=1000-1099")
		defer resp.Body.Close()
		assert.Equal(t, http.StatusPartialContent, resp.StatusCode)
		assert.Equal(t, "bytes 1000-1099/8192", resp.Header.Get("Content-Range"))
		body, err := io.ReadAll(resp.Body)
		require.NoError(t, err)
		assert.Equal(t, want[1000:1100], body)
	})

	t.Run("a suffix range, which is how a footer is read", func(t *testing.T) {
		resp := get(t, "bytes=-64")
		defer resp.Body.Close()
		assert.Equal(t, http.StatusPartialContent, resp.StatusCode)
		body, err := io.ReadAll(resp.Body)
		require.NoError(t, err)
		assert.Equal(t, want[len(want)-64:], body)
	})

	t.Run("no range at all", func(t *testing.T) {
		resp := get(t, "")
		defer resp.Body.Close()
		assert.Equal(t, http.StatusOK, resp.StatusCode)
		assert.Equal(t, "8192", resp.Header.Get("Content-Length"))
		body, err := io.ReadAll(resp.Body)
		require.NoError(t, err)
		assert.Equal(t, want, body)
	})

	t.Run("a range past the end", func(t *testing.T) {
		resp := get(t, "bytes=99999-")
		defer resp.Body.Close()
		assert.Equal(t, http.StatusRequestedRangeNotSatisfiable, resp.StatusCode)
	})
}
