// Copyright 2024 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package aligned

import (
	"fmt"
	"unsafe"
)

// ByteSlice allocates a new byte slice of length n, ensuring the address of the
// beginning of the slice is word aligned. Go does not guarantee that a simple
// make([]byte, n) is aligned. In practice it often is, especially for larger n,
// but small n can often be misaligned.
func ByteSlice(n int) []byte {
	a := make([]uint64, (n+7)/8)
	b := unsafe.Slice((*byte)(unsafe.Pointer(&a[0])), n)

	// Verify alignment.
	ptr := uintptr(unsafe.Pointer(&b[0]))
	if ptr%unsafe.Sizeof(int(0)) != 0 {
		panic(fmt.Sprintf("allocated []uint64 slice not %d-aligned: pointer %p", unsafe.Sizeof(int(0)), &b[0]))
	}
	return b
}
