package blobcache

import (
	"fmt"
	"io"
	"os"
	"strings"
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// evicting builds a store small enough that writing a few hundred kilobytes
// through it forces reclaim.
func evicting(t *testing.T, dir string, maxBytes int64) *Store {
	return open(t, dir, func(o *Options) {
		o.MaxBytes = maxBytes
		o.SegmentSize = 8 << 10
		o.InlineMaxSize = 2 << 10
		o.MaxObjectSize = 64 << 10
	})
}

func TestEvictionKeepsTheStoreUnderBudget(t *testing.T) {
	const budget = 64 << 10
	s := evicting(t, t.TempDir(), budget)
	defer s.Close()

	for i := 0; i < 300; i++ {
		put(t, s, fmt.Sprintf("key-%03d", i), []byte("m"), payload(1024, byte(i)))
	}

	// Reclaim runs after a write rather than before one, and it frees whole
	// containers, so the budget is a budget and not a ceiling. What it must
	// not do is drift: the overshoot is bounded by one segment.
	st := s.Stats()
	assert.LessOrEqual(t, st.Bytes, int64(budget)+int64(8<<10),
		"the store should settle just above its budget, not grow past it")
	assert.Greater(t, st.Evictions, uint64(0))
	assert.Greater(t, st.EvictedBytes, uint64(0))
	assert.Greater(t, st.Entries, 0, "eviction should not empty the store")
}

func TestEvictionDropsTheOldestFirst(t *testing.T) {
	s := evicting(t, t.TempDir(), 64<<10)
	defer s.Close()

	const n = 200
	for i := 0; i < n; i++ {
		put(t, s, fmt.Sprintf("key-%03d", i), nil, payload(1024, byte(i)))
	}

	// Nothing here was ever read, so the only ordering available is the one
	// the log already has. The newest writes must be the ones still around.
	assert.False(t, s.Has("key-000"), "the first key written should be long gone")
	assert.True(t, s.Has(fmt.Sprintf("key-%03d", n-1)), "the most recent key should still be cached")
}

func TestReadingProtectsASegment(t *testing.T) {
	s := evicting(t, t.TempDir(), 96<<10)
	defer s.Close()

	const group = 30
	for i := 0; i < group; i++ {
		put(t, s, fmt.Sprintf("read-%03d", i), nil, payload(1024, byte(i)))
	}
	for i := 0; i < group; i++ {
		put(t, s, fmt.Sprintf("cold-%03d", i), nil, payload(1024, byte(i)))
	}
	// Touch the older group so its segments are no longer the least
	// recently used ones, then push both groups past the budget.
	for round := 0; round < 3; round++ {
		for i := 0; i < group; i++ {
			if r, err := s.Get(fmt.Sprintf("read-%03d", i)); err == nil {
				_, _ = io.Copy(io.Discard, r)
				_ = r.Close()
			}
		}
	}
	for i := 0; i < 60; i++ {
		put(t, s, fmt.Sprintf("fill-%03d", i), nil, payload(1024, byte(i)))
	}

	var warm, cold int
	for i := 0; i < group; i++ {
		if s.Has(fmt.Sprintf("read-%03d", i)) {
			warm++
		}
		if s.Has(fmt.Sprintf("cold-%03d", i)) {
			cold++
		}
	}
	// Reclaim is segment-granular, so this is a tendency and not a
	// guarantee about any one key — which is exactly the trade being made.
	assert.Greater(t, warm, cold, "a segment that is being read should outlive one that is not")
}

func TestGarbageSegmentsAreReclaimedWithoutPressure(t *testing.T) {
	s := evicting(t, t.TempDir(), 4<<20) // far from full
	defer s.Close()

	const n = 60
	for i := 0; i < n; i++ {
		put(t, s, fmt.Sprintf("key-%03d", i), nil, payload(1024, byte(i)))
	}
	peak := s.Stats().Bytes
	for i := 0; i < n; i++ {
		require.NoError(t, s.Delete(fmt.Sprintf("key-%03d", i)))
	}
	// A segment whose records have all been superseded holds nothing and
	// still costs disk, so it goes regardless of the budget. Records are
	// never rewritten to compact one — the whole file is the unit.
	assert.Less(t, s.Stats().Bytes, peak, "fully dead segments should be freed without waiting for pressure")
	assert.Equal(t, 0, s.Stats().Entries)
}

func TestEvictedBlobFilesLeaveTheDisk(t *testing.T) {
	dir := t.TempDir()
	s := open(t, dir, func(o *Options) {
		o.MaxBytes = 128 << 10
		o.SegmentSize = 16 << 10
		o.InlineMaxSize = 4 << 10
		o.MaxObjectSize = 64 << 10
	})

	for i := 0; i < 20; i++ {
		put(t, s, fmt.Sprintf("big-%02d", i), nil, payload(32<<10, byte(i)))
	}
	// A commit is acknowledged before the reclaim its batch triggers has run,
	// so the store is still moving when the last Put returns: counting files
	// here and reading the counters a moment later compares two different
	// instants, and on a loaded machine the difference shows. Closing stops
	// the writer, and what is measured below is the state the last reclaim
	// actually left behind.
	require.NoError(t, s.Close())

	var blobs int
	var bytes int64
	entries, err := os.ReadDir(dir)
	require.NoError(t, err)
	for _, e := range entries {
		if e.IsDir() {
			continue
		}
		info, err := e.Info()
		require.NoError(t, err)
		bytes += info.Size()
		if strings.HasPrefix(e.Name(), "blob-") {
			blobs++
		}
	}

	st := s.Stats()
	assert.LessOrEqual(t, blobs, 6, "evicted payloads should be unlinked, not merely forgotten")
	assert.Equal(t, blobs, st.Blobs, "the store's count should match what is on disk")
	assert.LessOrEqual(t, st.Bytes, bytes,
		"the store must not go on counting disk that nothing occupies: the "+
			"budget it enforces is that number, so overstating it shrinks the cache")
}

func TestReaderKeepsWorkingAfterItsEntryIsEvicted(t *testing.T) {
	s := evicting(t, t.TempDir(), 64<<10)
	defer s.Close()

	want := payload(1024, 42)
	put(t, s, "doomed", []byte("m"), want)

	r, err := s.Get("doomed")
	require.NoError(t, err)
	defer r.Close()

	// Evict it out from under the open reader. A download in progress must
	// not notice: the entry leaves the index at once, and the bytes stay
	// readable until the last reader lets go.
	//
	// The filler is read back as it is written, because otherwise the Get
	// above would leave "doomed" the most recently used thing in the store
	// and reclaim would quite correctly keep choosing something else.
	for i := 0; i < 400 && s.Has("doomed"); i++ {
		key := fmt.Sprintf("filler-%03d", i)
		put(t, s, key, nil, payload(1024, byte(i)))
		if fr, err := s.Get(key); err == nil {
			_, _ = io.Copy(io.Discard, fr)
			_ = fr.Close()
		}
	}
	require.False(t, s.Has("doomed"), "the test needs the entry to actually be evicted")

	got, err := io.ReadAll(r)
	require.NoError(t, err)
	assert.Equal(t, want, got, "an in-flight read should survive the eviction of its entry")
}

func TestConcurrentAccessIsSafe(t *testing.T) {
	s := open(t, t.TempDir(), func(o *Options) {
		o.MaxBytes = 512 << 10
		o.SegmentSize = 32 << 10
		o.InlineMaxSize = 8 << 10
		o.MaxObjectSize = 128 << 10
	})
	defer s.Close()

	const writers, readers, deleters, rounds = 4, 6, 2, 120
	var wg sync.WaitGroup

	for w := 0; w < writers; w++ {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			for i := 0; i < rounds; i++ {
				key := fmt.Sprintf("w%d-key-%03d", w, i)
				size := 512
				if i%17 == 0 {
					size = 24 << 10 // push some of them into their own files
				}
				data := payload(size, byte(i))
				wr, err := s.Put(key, int64(len(data)))
				if err != nil {
					continue
				}
				if _, err := wr.Write(data); err != nil {
					wr.Abort()
					continue
				}
				// ErrBusy is an ordinary outcome under load and never a
				// reason for the caller to fail.
				if err := wr.Commit([]byte(key)); err != nil {
					require.ErrorIs(t, err, ErrBusy)
				}
				wr.Abort()
			}
		}(w)
	}
	for r := 0; r < readers; r++ {
		wg.Add(1)
		go func(r int) {
			defer wg.Done()
			for i := 0; i < rounds*4; i++ {
				key := fmt.Sprintf("w%d-key-%03d", i%writers, i%rounds)
				reader, err := s.Get(key)
				if err != nil {
					continue
				}
				body, err := io.ReadAll(reader)
				_ = reader.Close()
				if assert.NoError(t, err) && len(body) > 0 {
					// Whatever comes back has to be a payload that was
					// actually written, not a mixture of two.
					assert.Equal(t, payload(len(body), body[0]), body,
						"a concurrent read must never see a torn payload for %q", key)
				}
			}
		}(r)
	}
	for d := 0; d < deleters; d++ {
		wg.Add(1)
		go func(d int) {
			defer wg.Done()
			for i := 0; i < rounds; i++ {
				_ = s.Delete(fmt.Sprintf("w%d-key-%03d", i%writers, i%rounds))
			}
		}(d)
	}
	wg.Wait()
}

// TestAnEntryIsNeverVisibleBeforeItsBytesAre pins down the ordering that the
// batching makes easy to get wrong.
//
// A record is appended into a buffer and only reaches the page cache at the
// flush. Publish it into the index before that and a reader can find it while
// the file still holds what the segment was preallocated with: the right
// length, the right key, and none of the payload. The writer never notices,
// because it is still waiting for its acknowledgement — so the only place
// this shows up is here.
func TestAnEntryIsNeverVisibleBeforeItsBytesAre(t *testing.T) {
	s := open(t, t.TempDir(), func(o *Options) {
		o.MaxBytes = 8 << 20
		o.SegmentSize = 128 << 10
		o.InlineMaxSize = 16 << 10
		o.BatchSize = 64 // batch aggressively, to widen the window
	})
	defer s.Close()

	const rounds = 400
	want := payload(2048, 77)

	var writers, readers sync.WaitGroup
	stop := make(chan struct{})

	// Readers poll for whatever the writers are publishing. Anything they
	// find has to be complete.
	for r := 0; r < 4; r++ {
		readers.Add(1)
		go func() {
			defer readers.Done()
			for {
				select {
				case <-stop:
					return
				default:
				}
				for i := 0; i < rounds; i++ {
					reader, err := s.Get(fmt.Sprintf("round-%04d", i))
					if err != nil {
						continue
					}
					got, err := io.ReadAll(reader)
					_ = reader.Close()
					if assert.NoError(t, err) {
						assert.Equal(t, want, got,
							"an entry the index points at must already be readable in full")
					}
				}
			}
		}()
	}

	for w := 0; w < 4; w++ {
		writers.Add(1)
		go func(w int) {
			defer writers.Done()
			for i := w; i < rounds; i += 4 {
				writer, err := s.Put(fmt.Sprintf("round-%04d", i), int64(len(want)))
				if err != nil {
					continue
				}
				if _, err := writer.Write(want); err != nil {
					writer.Abort()
					continue
				}
				if err := writer.Commit([]byte("m")); err != nil && err != ErrBusy {
					t.Errorf("commit: %v", err)
				}
				writer.Abort()
			}
		}(w)
	}

	writers.Wait()
	close(stop)
	readers.Wait()
}

// A payload large enough for its own file has that file's whole length
// allocated the moment the write starts, and the write may not commit for
// minutes. Counting only committed bytes would make the real ceiling however
// many writes happen to overlap: with a 1 GiB budget and 64 MiB objects, 64
// concurrent misses would reserve 4 GiB on a volume sized for one.
//
// Eviction cannot help, either — it reclaims what is counted, and these bytes
// are spoken for rather than written.
func TestWritesInFlightCountAgainstTheBudget(t *testing.T) {
	// Half of an 8 MiB budget is what writes in flight may claim between
	// them, so four megabyte writers fill it exactly.
	s := open(t, t.TempDir(), func(o *Options) {
		o.MaxBytes = 8 << 20
		o.SegmentSize = 256 << 10
		o.InlineMaxSize = 64 << 10
		o.MaxObjectSize = 1 << 20
	})
	defer s.Close()

	// Four writers of a megabyte each fill the allowance without a single
	// one of them having committed.
	var inFlight []*Writer
	for i := 0; i < 4; i++ {
		w, err := s.Put(fmt.Sprintf("big-%d", i), 1<<20)
		require.NoError(t, err, "writer %d should still fit in the allowance", i)
		inFlight = append(inFlight, w)
	}
	_, err := s.Put("one-too-many", 1<<20)
	assert.ErrorIs(t, err, ErrNoRoom,
		"disk claimed by writes in flight has to be counted, not only what has landed")

	// Nothing has been committed, so nothing is on the books yet — the point
	// is that the disk is already spoken for.
	assert.Equal(t, int64(0), s.Stats().Bytes)

	// A write gives its reservation back whichever way it ends: aborted,
	// committed, or failed.
	inFlight[0].Abort()
	_, err = inFlight[1].Write(payload(1<<20, 3))
	require.NoError(t, err)
	require.NoError(t, inFlight[1].Commit([]byte("m")))
	inFlight[2].Abort()
	inFlight[3].Abort()

	assert.Equal(t, int64(0), s.reserved.Load(), "every reservation has to be given back")
	assert.Greater(t, s.Stats().Bytes, int64(1<<20), "and the one that committed is now on the books")

	// With the reservations gone there is room again.
	w, err := s.Put("room-again", 1<<20)
	require.NoError(t, err)
	w.Abort()
}

// A write whose length nobody announced cannot reserve up front, so it claims
// the allowance as the payload arrives — otherwise the accounting would have
// a hole exactly where the size hint is missing, and a client that declines
// to say how much it is sending would be the way around the bound.
func TestAWriteOfUnknownLengthAlsoClaimsTheAllowance(t *testing.T) {
	s := open(t, t.TempDir(), func(o *Options) {
		o.MaxBytes = 8 << 20 // allowance: 4 MiB
		o.SegmentSize = 256 << 10
		o.InlineMaxSize = 32 << 10
		o.MaxObjectSize = 2 << 20
	})
	defer s.Close()

	// Fill the allowance with two writes that did announce their size.
	for i := 0; i < 2; i++ {
		w, err := s.Put(fmt.Sprintf("announced-%d", i), 2<<20)
		require.NoError(t, err)
		defer w.Abort()
	}

	// This one says nothing, so it is only refused once it has grown out of
	// the staging buffer and asks for disk.
	w, err := s.Put("unannounced", -1)
	require.NoError(t, err, "a write of unknown length costs nothing until it needs disk")
	defer w.Abort()

	var wrote int64
	var failure error
	for i := 0; i < 8; i++ {
		n, err := w.Write(payload(32<<10, byte(i)))
		wrote += int64(n)
		if err != nil {
			failure = err
			break
		}
	}
	assert.ErrorIs(t, failure, ErrNoRoom)
	assert.LessOrEqual(t, wrote, int64(64<<10),
		"it should stop as soon as it needs disk the allowance cannot give it")
}
