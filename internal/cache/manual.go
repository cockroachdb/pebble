// Copyright 2020 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package cache

// #include <stdlib.h>
import "C"
import "unsafe"

// TODO(peter): Rather than relying an C malloc/free, we could fork the Go
// runtime page allocator and allocate large chunks of memory using mmap or
// similar.

// manualNew allocates a slice of size n.
func manualNew(n int) []byte {
	if n == 0 {
		return make([]byte, 0)
	}
	ptr := C.malloc(C.size_t(n))
	// Interpret the C pointer as a pointer to a Go array, then slice.
	return (*[maxArrayLen]byte)(unsafe.Pointer(ptr))[:n:n]
}

// manualFree frees the specified slice.
func manualFree(b []byte) {
	if cap(b) != 0 {
		if len(b) == 0 {
			b = b[:cap(b)]
		}
		C.free(unsafe.Pointer(&b[0]))
	}
}
