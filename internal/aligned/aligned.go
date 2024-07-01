// Copyright 2024 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package aligned

import "unsafe"

const align64 = 8

// ByteSlice allocates a new byte slice of length n, ensuring the address of the
// beginning of the slice is 64-bit aligned. Go does not guarantee that a simple
// make([]byte, n) is aligned. In practice it often is, especially for larger n,
// but small n can often be misaligned.
func ByteSlice(n int) []byte {
	// Allocate n + align64 - 1 bytes to ensure we have room to align.
	b := make([]byte, n+align64-1)
	ptr := uintptr(unsafe.Pointer(&b[0]))
	// Compute the aligned address.
	aligned := (ptr + uintptr(align64) - 1) & ^(uintptr(align64) - 1)
	// Offset the slice by the difference.
	off := aligned - ptr
	return b[off : off+uintptr(n) : off+uintptr(n)]
}
