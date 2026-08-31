package blobcache

import (
	"fmt"
	"io"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// The tests in this file are the ones the no-fsync design has to earn. Each
// one damages a store the way a particular failure would, reopens it, and
// checks that what comes back is a subset of what went in — never something
// that was never written.

func TestReopenAfterCleanClose(t *testing.T) {
	dir := t.TempDir()
	s := open(t, dir)
	put(t, s, "small", []byte("m1"), payload(512, 1))
	put(t, s, "large", []byte("m2"), payload(48<<10, 2))
	require.NoError(t, s.Close())

	s = open(t, dir)
	defer s.Close()

	meta, data := mustGet(t, s, "small")
	assert.Equal(t, "m1", string(meta))
	assert.Equal(t, payload(512, 1), data)
	meta, data = mustGet(t, s, "large")
	assert.Equal(t, "m2", string(meta))
	assert.Equal(t, payload(48<<10, 2), data)
	assert.Equal(t, 2, s.Stats().Entries)
}

func TestReopenAfterCrash(t *testing.T) {
	dir := t.TempDir()
	s := open(t, dir)
	put(t, s, "a", []byte("m"), payload(300, 1))
	put(t, s, "b", []byte("m"), payload(300, 2))
	s.Crash()

	// Nothing was sealed and nothing was fsynced, so recovery has to replay
	// the records themselves and check every one of them.
	s = open(t, dir)
	defer s.Close()
	_, data := mustGet(t, s, "a")
	assert.Equal(t, payload(300, 1), data)
	_, data = mustGet(t, s, "b")
	assert.Equal(t, payload(300, 2), data)
}

func TestDeletionSurvivesCrash(t *testing.T) {
	dir := t.TempDir()
	s := open(t, dir)
	put(t, s, "gone", nil, payload(64, 1))
	put(t, s, "kept", nil, payload(64, 2))
	require.NoError(t, s.Delete("gone"))
	s.Crash()

	// A deletion is the one write this store fsyncs, because a resurrected
	// key is the only staleness it can prevent on its own.
	s = open(t, dir)
	defer s.Close()
	mustMiss(t, s, "gone")
	_, data := mustGet(t, s, "kept")
	assert.Equal(t, payload(64, 2), data)
}

func TestTruncatedTailIsDiscarded(t *testing.T) {
	dir := t.TempDir()
	s := open(t, dir)
	meta := []byte("m")
	first, second := payload(300, 1), payload(300, 2)
	put(t, s, "a", meta, first)
	put(t, s, "b", meta, second)
	path := s.ActiveSegmentPath()
	s.Crash()

	// Cut the second record in half, which is what a crash partway through
	// an append looks like from the outside.
	firstEnd := SegmentHeaderSize + RecordSize("a", meta, first)
	truncateAt := firstEnd + RecordSize("b", meta, second)/2
	require.NoError(t, os.Truncate(path, truncateAt))

	s = open(t, dir)
	defer s.Close()
	_, data := mustGet(t, s, "a")
	assert.Equal(t, first, data, "the record before the damage is intact and must survive")
	mustMiss(t, s, "b")

	// And the store keeps working: the tail was overwritten, not left for a
	// later replay to trip over.
	put(t, s, "c", meta, payload(300, 3))
	_, data = mustGet(t, s, "c")
	assert.Equal(t, payload(300, 3), data)
}

func TestZeroFilledTailIsDiscarded(t *testing.T) {
	dir := t.TempDir()
	s := open(t, dir)
	meta := []byte("m")
	first := payload(300, 1)
	put(t, s, "a", meta, first)
	put(t, s, "b", meta, payload(300, 2))
	path := s.ActiveSegmentPath()
	s.Crash()

	// This is the shape ext4's delayed allocation leaves behind: the file is
	// the right length, and the tail of it is zeroes. A design that trusted
	// lengths and offsets would hand those zeroes back as a payload.
	firstEnd := SegmentHeaderSize + RecordSize("a", meta, first)
	f, err := os.OpenFile(path, os.O_WRONLY, 0o600)
	require.NoError(t, err)
	info, err := f.Stat()
	require.NoError(t, err)
	_, err = f.WriteAt(make([]byte, info.Size()-firstEnd), firstEnd)
	require.NoError(t, err)
	require.NoError(t, f.Close())

	s = open(t, dir)
	defer s.Close()
	_, data := mustGet(t, s, "a")
	assert.Equal(t, first, data)
	mustMiss(t, s, "b")
}

func TestAlteredPayloadIsRejected(t *testing.T) {
	dir := t.TempDir()
	s := open(t, dir)
	meta := []byte("m")
	first, second := payload(300, 1), payload(300, 2)
	put(t, s, "a", meta, first)
	put(t, s, "b", meta, second)
	put(t, s, "c", meta, payload(300, 3))
	path := s.ActiveSegmentPath()
	s.Crash()

	// Flip one bit inside the second record's payload. The lengths still
	// line up and the header still decodes; only the checksum notices.
	firstEnd := SegmentHeaderSize + RecordSize("a", meta, first)
	target := firstEnd + recordHeaderSize + int64(len("b")+len(meta)) + 150
	f, err := os.OpenFile(path, os.O_RDWR, 0o600)
	require.NoError(t, err)
	one := make([]byte, 1)
	_, err = f.ReadAt(one, target)
	require.NoError(t, err)
	one[0] ^= 0x01
	_, err = f.WriteAt(one, target)
	require.NoError(t, err)
	require.NoError(t, f.Close())

	s = open(t, dir)
	defer s.Close()
	_, data := mustGet(t, s, "a")
	assert.Equal(t, first, data)
	mustMiss(t, s, "b")
	// A replay stops at the first record it cannot trust, so everything
	// after the damage is discarded too.
	mustMiss(t, s, "c")
}

func TestSealedSegmentsRecoverFromTheirFooter(t *testing.T) {
	dir := t.TempDir()
	s := open(t, dir, func(o *Options) {
		o.SegmentSize = 16 << 10
		o.InlineMaxSize = 4 << 10
		o.MaxBytes = 16 << 20
	})

	// Enough to fill several segments, so that most of them are sealed and
	// recovered from their index rather than by reading them back.
	const n = 40
	for i := 0; i < n; i++ {
		put(t, s, fmt.Sprintf("key-%03d", i), []byte(fmt.Sprintf("meta-%03d", i)), payload(1024, byte(i)))
	}
	require.NoError(t, s.Close())

	segments, err := filepath.Glob(filepath.Join(dir, "seg-*.log"))
	require.NoError(t, err)
	require.Greater(t, len(segments), 2, "the test needs several segments to be meaningful")

	s = open(t, dir, func(o *Options) {
		o.SegmentSize = 16 << 10
		o.InlineMaxSize = 4 << 10
		o.MaxBytes = 16 << 20
	})
	defer s.Close()
	for i := 0; i < n; i++ {
		meta, data := mustGet(t, s, fmt.Sprintf("key-%03d", i))
		assert.Equal(t, fmt.Sprintf("meta-%03d", i), string(meta))
		assert.Equal(t, payload(1024, byte(i)), data)
	}
	assert.Equal(t, n, s.Stats().Entries)
}

func TestDamagedFooterFallsBackToReplay(t *testing.T) {
	dir := t.TempDir()
	tune := func(o *Options) {
		o.SegmentSize = 16 << 10
		o.InlineMaxSize = 4 << 10
		o.MaxBytes = 16 << 20
	}
	s := open(t, dir, tune)
	for i := 0; i < 20; i++ {
		put(t, s, fmt.Sprintf("key-%02d", i), []byte("m"), payload(512, byte(i)))
	}
	require.NoError(t, s.Close())

	// Corrupt the index of the oldest sealed segment. It is a shortcut, not
	// a source of truth, so losing it should cost startup time and nothing
	// else.
	segments, err := filepath.Glob(filepath.Join(dir, "seg-*.log"))
	require.NoError(t, err)
	require.NotEmpty(t, segments)
	f, err := os.OpenFile(segments[0], os.O_WRONLY, 0o600)
	require.NoError(t, err)
	info, err := f.Stat()
	require.NoError(t, err)
	_, err = f.WriteAt([]byte("wrecked!"), info.Size()-footerTrailerSize)
	require.NoError(t, err)
	require.NoError(t, f.Close())

	s = open(t, dir, tune)
	defer s.Close()
	for i := 0; i < 20; i++ {
		_, data := mustGet(t, s, fmt.Sprintf("key-%02d", i))
		assert.Equal(t, payload(512, byte(i)), data)
	}
}

func TestOrphanedBlobFileIsRemoved(t *testing.T) {
	dir := t.TempDir()
	s := open(t, dir)
	put(t, s, "k", nil, payload(32<<10, 1))
	require.NoError(t, s.Close())

	// A file that finished being written but whose log record never made
	// it: the shape of a crash in the window between the rename and the
	// commit.
	orphan := filepath.Join(dir, "blob-00000000deadbeef.dat")
	trailer := blobTrailer{ID: 0xdeadbeef, DataLen: 4}
	require.NoError(t, os.WriteFile(orphan, append([]byte("data"), trailer.encode()...), 0o600))

	s = open(t, dir)
	defer s.Close()
	_, err := os.Stat(orphan)
	assert.True(t, os.IsNotExist(err), "a blob file no record refers to should be removed")
	_, data := mustGet(t, s, "k")
	assert.Equal(t, payload(32<<10, 1), data)
}

func TestMissingBlobFileDoesNotResurrectTheOldValue(t *testing.T) {
	dir := t.TempDir()
	s := open(t, dir)
	put(t, s, "k", []byte("v1"), payload(64, 1))
	put(t, s, "k", []byte("v2"), payload(32<<10, 2))
	require.NoError(t, s.Close())

	// Delete the payload behind the newer value. The log still records that
	// "k" was overwritten at that point, and honouring only half of that —
	// dropping the new value but restoring the old one — would hand back a
	// version of the object that was replaced on purpose.
	blobs, err := filepath.Glob(filepath.Join(dir, "blob-*.dat"))
	require.NoError(t, err)
	require.Len(t, blobs, 1)
	require.NoError(t, os.Remove(blobs[0]))

	s = open(t, dir)
	defer s.Close()
	mustMiss(t, s, "k")
}

func TestTemporaryFilesAreClearedOnOpen(t *testing.T) {
	dir := t.TempDir()
	s := open(t, dir)
	require.NoError(t, s.Close())

	stale := filepath.Join(dir, "tmp", "blob-000000000000002a.tmp")
	require.NoError(t, os.WriteFile(stale, []byte("half an upload"), 0o600))

	s = open(t, dir)
	defer s.Close()
	_, err := os.Stat(stale)
	assert.True(t, os.IsNotExist(err), "an interrupted upload has no trailer and nothing to salvage")
}

func TestEmptyDirectoryOpensClean(t *testing.T) {
	s := open(t, t.TempDir())
	defer s.Close()
	assert.Equal(t, 0, s.Stats().Entries)
	assert.Equal(t, 1, s.Stats().Segments)
	put(t, s, "k", nil, []byte("v"))
	_, data := mustGet(t, s, "k")
	assert.Equal(t, "v", string(data))
}

func TestBlobPayloadChecksumIsVerifiable(t *testing.T) {
	dir := t.TempDir()
	s := open(t, dir)
	want := payload(32<<10, 9)
	put(t, s, "k", nil, want)
	require.NoError(t, s.Close())

	// Startup does not read gigabytes back to check them, but the checksum
	// is recorded so an operator chasing a suspect volume can.
	blobs, err := filepath.Glob(filepath.Join(dir, "blob-*.dat"))
	require.NoError(t, err)
	require.Len(t, blobs, 1)

	c, trailer, err := openBlobFile(blobs[0])
	require.NoError(t, err)
	assert.Equal(t, int64(len(want)), trailer.DataLen)
	assert.NoError(t, verifyBlobPayload(c, trailer))

	got := make([]byte, trailer.DataLen)
	_, err = io.ReadFull(io.NewSectionReader(c.rf, 0, trailer.DataLen), got)
	require.NoError(t, err)
	assert.Equal(t, want, got)
	c.retire()
}
