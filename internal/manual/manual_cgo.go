// Copyright 2020 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package manual

// #include <stdlib.h>
import "C"
import (
	"math/rand/v2"
	"unsafe"

	"github.com/cockroachdb/pebble/internal/invariants"
)

// The go:linkname directives provides backdoor access to private functions in
// the runtime. Below we're accessing the throw function.

//go:linkname throw runtime.throw
func throw(s string)

// useGoAllocation is used in race-enabled builds to configure the package to
// use ordinary Go allocations with make([]byte, n). This is done under the
// assumption that the Go race detector will detect races within cgo-allocated
// memory. Performing some allocations using Go allows the race detector to
// observe concurrent memory access to memory allocated by this package.
//
// TODO(jackson): Confirm that the race detector does not detect races within
// cgo-allocated memory.
var useGoAllocation = invariants.RaceEnabled && rand.Uint32()%2 == 0

// TODO(peter): Rather than relying an C malloc/free, we could fork the Go
// runtime page allocator and allocate large chunks of memory using mmap or
// similar.

// New allocates a slice of size n. The returned slice is from manually
// managed memory and MUST be released by calling Free. Failure to do so will
// result in a memory leak.
func New(purpose Purpose, n uintptr) Buf {
	if n == 0 {
		return Buf{}
	}
	recordAlloc(purpose, n)

	// In race-enabled builds, we sometimes make allocations using Go to allow
	// the race detector to observe concurrent memory access to memory allocated
	// by this package. See the definition of useGoAllocation for more details.
	if invariants.RaceEnabled && useGoAllocation {
		b := make([]byte, n)
		return Buf{data: unsafe.Pointer(&b[0]), n: n}
	}
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
	return Buf{data: ptr, n: n}
}

// Free frees the specified slice. It has to be exactly the slice that was
// returned by New.
func Free(purpose Purpose, b Buf) {
	if b.n != 0 {
		invariants.MaybeMangle(b.Slice())
		recordFree(purpose, b.n)

		if !invariants.RaceEnabled || !useGoAllocation {
			C.free(b.data)
		}
	}
}
