//go:build !linux

package blobcache

import "os"

// Neither hint has a portable equivalent, and neither is load-bearing: the
// store is correct without them and merely writes less smoothly. They are
// no-ops everywhere except Linux, which is what this proxy deploys on.

func preallocate(f *os.File, offset, length int64) {}

func startWriteback(f *os.File, offset, length int64) {}
