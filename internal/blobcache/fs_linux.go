//go:build linux

package blobcache

import (
	"os"

	"golang.org/x/sys/unix"
)

// preallocate reserves space for a file that is about to be filled.
//
// It keeps the filesystem from extending the file one append at a time, and
// it moves "the volume is full" to the moment the segment is created instead
// of into the middle of somebody's upload. Network filesystems often cannot
// do it at all, which is fine — it is an optimisation, and its failure means
// only that the file grows the ordinary way.
func preallocate(f *os.File, offset, length int64) {
	if length <= 0 {
		return
	}
	_ = unix.Fallocate(int(f.Fd()), 0, offset, length)
}

// startWriteback asks the kernel to begin flushing a range of the file, and
// returns without waiting for it.
//
// This is not an fsync and gives no durability guarantee. It exists to keep
// the pile of dirty pages from growing until the kernel decides to force
// writeback synchronously — a stall that would land on an unrelated request
// and be very hard to attribute afterwards.
func startWriteback(f *os.File, offset, length int64) {
	if length <= 0 {
		return
	}
	_ = unix.SyncFileRange(int(f.Fd()), offset, length, unix.SYNC_FILE_RANGE_WRITE)
}
