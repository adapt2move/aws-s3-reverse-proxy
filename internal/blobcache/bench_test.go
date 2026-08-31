package blobcache

import (
	"fmt"
	"io"
	"os"
	"path/filepath"
	"testing"
)

// The benchmarks below exist to keep the design honest. The store's whole
// justification is that appending into a shared log beats a file per entry on
// a volume where metadata operations and fsyncs are round trips, so the
// baselines it is supposed to beat are measured right next to it.
//
// A local filesystem flatters the baselines: on network-attached storage the
// fsync variant is one to two orders of magnitude worse than what shows up
// here, and the file-per-entry variant loses a further round trip per create
// and per rename.

// benchMeta stands in for whatever a caller records beside a payload.
var benchMeta = []byte("metadata")

func benchSizes() []int { return []int{512, 4 << 10, 64 << 10} }

func BenchmarkPut(b *testing.B) {
	for _, size := range benchSizes() {
		data := payload(size, 3)
		b.Run(fmt.Sprintf("blobcache/%s", byteSize(size)), func(b *testing.B) {
			s := benchStore(b)
			defer s.Close()
			b.SetBytes(int64(size))
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				w, err := s.Put(fmt.Sprintf("key-%08d", i), int64(size))
				if err != nil {
					b.Fatal(err)
				}
				if _, err := w.Write(data); err != nil {
					b.Fatal(err)
				}
				if err := w.Commit(benchMeta); err != nil && err != ErrBusy {
					b.Fatal(err)
				}
			}
		})

		// Baseline one: what the obvious implementation does. Create a
		// temporary file, write it, rename it into place.
		b.Run(fmt.Sprintf("file-per-entry/%s", byteSize(size)), func(b *testing.B) {
			dir := b.TempDir()
			b.SetBytes(int64(size))
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				writeOneFile(b, dir, i, data, false)
			}
		})

		// Baseline two: the same, made durable the way a store that
		// confused "cache" with "database" would.
		b.Run(fmt.Sprintf("file-per-entry-fsync/%s", byteSize(size)), func(b *testing.B) {
			dir := b.TempDir()
			b.SetBytes(int64(size))
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				writeOneFile(b, dir, i, data, true)
			}
		})
	}
}

// BenchmarkPutParallel is the shape the proxy actually produces: many
// requests committing at once. It is where folding a batch of commits into
// one write() pays off.
func BenchmarkPutParallel(b *testing.B) {
	data := payload(4<<10, 3)
	s := benchStore(b)
	defer s.Close()
	b.SetBytes(int64(len(data)))
	b.ReportAllocs()
	b.ResetTimer()

	var counter int64
	b.RunParallel(func(pb *testing.PB) {
		local := 0
		for pb.Next() {
			local++
			w, err := s.Put(fmt.Sprintf("key-%d-%08d", counter, local), int64(len(data)))
			if err != nil {
				b.Fatal(err)
			}
			if _, err := w.Write(data); err != nil {
				b.Fatal(err)
			}
			if err := w.Commit(benchMeta); err != nil && err != ErrBusy {
				b.Fatal(err)
			}
		}
	})
}

func BenchmarkGet(b *testing.B) {
	for _, size := range benchSizes() {
		b.Run(byteSize(size), func(b *testing.B) {
			s := benchStore(b)
			defer s.Close()
			const keys = 512
			data := payload(size, 3)
			for i := 0; i < keys; i++ {
				w, err := s.Put(fmt.Sprintf("key-%08d", i), int64(size))
				if err != nil {
					b.Fatal(err)
				}
				if _, err := w.Write(data); err != nil {
					b.Fatal(err)
				}
				if err := w.Commit(benchMeta); err != nil {
					b.Fatal(err)
				}
			}
			b.SetBytes(int64(size))
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				r, err := s.Get(fmt.Sprintf("key-%08d", i%keys))
				if err != nil {
					b.Fatal(err)
				}
				if _, err := io.Copy(io.Discard, r); err != nil {
					b.Fatal(err)
				}
				_ = r.Close()
			}
		})
	}
}

func benchStore(b *testing.B) *Store {
	b.Helper()
	s, err := Open(Options{
		Dir:           b.TempDir(),
		MaxBytes:      512 << 20,
		SegmentSize:   64 << 20,
		InlineMaxSize: 1 << 20,
	})
	if err != nil {
		b.Fatal(err)
	}
	return s
}

func writeOneFile(b *testing.B, dir string, i int, data []byte, sync bool) {
	b.Helper()
	final := filepath.Join(dir, fmt.Sprintf("key-%08d", i))
	tmp := final + ".tmp"
	f, err := os.OpenFile(tmp, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0o600)
	if err != nil {
		b.Fatal(err)
	}
	if _, err := f.Write(data); err != nil {
		b.Fatal(err)
	}
	if sync {
		if err := f.Sync(); err != nil {
			b.Fatal(err)
		}
	}
	if err := f.Close(); err != nil {
		b.Fatal(err)
	}
	if err := os.Rename(tmp, final); err != nil {
		b.Fatal(err)
	}
}

func byteSize(n int) string {
	switch {
	case n >= 1<<20:
		return fmt.Sprintf("%dMiB", n>>20)
	case n >= 1<<10:
		return fmt.Sprintf("%dKiB", n>>10)
	default:
		return fmt.Sprintf("%dB", n)
	}
}
