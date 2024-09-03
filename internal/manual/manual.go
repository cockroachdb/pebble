// Copyright 2020 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package manual

// #include <stdlib.h>
import "C"
import (
	"sync/atomic"
	"unsafe"
)

// The go:linkname directives provides backdoor access to private functions in
// the runtime. Below we're accessing the throw function.

//go:linkname throw runtime.throw
func throw(s string)

type Purpose uint8

const (
	_ Purpose = iota

	BlockCacheMap
	BlockCacheEntry
	BlockCacheData
	MemTable

	NumPurposes
)

var counters [NumPurposes]struct {
	TotalAllocated atomic.Uint64
	TotalFreed     atomic.Uint64
	// Pad to separate counters into cache lines. This reduces the overhead when
	// multiple purposes are used frequently. We assume 64 byte cache line size
	// which is the case for ARM64 servers and AMD64.
	_ [6]uint64
}

// Metrics contains memory statistics by purpose.
type Metrics [NumPurposes]struct {
	// InUseBytes is the total number of bytes currently allocated. This is just
	// the sum of the lengths of the allocations and does not include any overhead
	// or fragmentation.
	InUseBytes uint64

	// InUseBytes is the total cumulative number of bytes allocated since the
	// process started. This is just the sum of the lengths of the allocations and
	// does not include any overhead or fragmentation.
	TotalBytes uint64
}

// GetMetrics returns manual memory usage statistics.
func GetMetrics() Metrics {
	var res Metrics
	for i := range res {
		res[i].TotalBytes = counters[i].TotalAllocated.Load()
		res[i].InUseBytes = res[i].TotalBytes - counters[i].TotalFreed.Load()
	}
	return res
}

// TODO(peter): Rather than relying an C malloc/free, we could fork the Go
// runtime page allocator and allocate large chunks of memory using mmap or
// similar.

// New allocates a slice of size n. The returned slice is from manually managed
// memory and MUST be released by calling Free. Failure to do so will result in
// a memory leak.
func New(purpose Purpose, n int) []byte {
	if n == 0 {
		return make([]byte, 0)
	}
	counters[purpose].TotalAllocated.Add(uint64(n))
	// We need to be conscious of the Cgo pointer passing rules:
	//
	//   https://golang.org/cmd/cgo/#hdr-Passing_pointers
	//
	//   ...
	//   Note: the current implementation has a bug. While Go code is permitted
	//   to write nil or a C pointer (but not a Go pointer) to C memory, the
	//   current implementation may sometimes cause a runtime error if the
	//   contents of the C memory appear to be a Go pointer. Therefore, avoid
	//   passing uninitialized C memory to Go code if the Go code is going to
	//   store pointer values in it. Zero out the memory in C before passing it
	//   to Go.
	ptr := C.calloc(C.size_t(n), 1)
	if ptr == nil {
		// NB: throw is like panic, except it guarantees the process will be
		// terminated. The call below is exactly what the Go runtime invokes when
		// it cannot allocate memory.
		throw("out of memory")
	}
	// Interpret the C pointer as a pointer to a Go array, then slice.
	return (*[MaxArrayLen]byte)(unsafe.Pointer(ptr))[:n:n]
}

// Free frees the specified slice.
func Free(purpose Purpose, b []byte) {
	if cap(b) != 0 {
		counters[purpose].TotalFreed.Add(uint64(cap(b)))
		if len(b) == 0 {
			b = b[:cap(b)]
		}
		ptr := unsafe.Pointer(&b[0])
		C.free(ptr)
	}
}
