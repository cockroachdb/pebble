// Copyright 2025 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package colblk

import (
	"math/bits"
	"unsafe"
)

// ReverseBytes16 calls bits.ReverseBytes16 on each element of the input slice.
func ReverseBytes16(s []uint16) {
	if len(s) >= 4 {
		// We convert the slice (up to the tail) to a slice of [4]uint16. This helps
		// the compiler elide bound checks.
		quads := unsafe.Slice((*[4]uint16)(unsafe.Pointer(unsafe.SliceData(s))), len(s)>>2)
		for i := range quads {
			quads[i][0] = bits.ReverseBytes16(quads[i][0]) //gcassert:bce
			quads[i][1] = bits.ReverseBytes16(quads[i][1]) //gcassert:bce
			quads[i][2] = bits.ReverseBytes16(quads[i][2]) //gcassert:bce
			quads[i][3] = bits.ReverseBytes16(quads[i][3]) //gcassert:bce
		}
	}
	tail := s[len(s)&^3:]
	for i := range tail {
		tail[i] = bits.ReverseBytes16(tail[i]) //gcassert:bce
	}
}

// ReverseBytes32 calls bits.ReverseBytes32 on each element of the input slice.
func ReverseBytes32(s []uint32) {
	if len(s) >= 4 {
		// We convert the slice (up to the tail) to a slice of [4]uint32. This helps
		// the compiler elide bound checks.
		quads := unsafe.Slice((*[4]uint32)(unsafe.Pointer(unsafe.SliceData(s))), len(s)>>2)
		for i := range quads {
			quads[i][0] = bits.ReverseBytes32(quads[i][0]) //gcassert:bce
			quads[i][1] = bits.ReverseBytes32(quads[i][1]) //gcassert:bce
			quads[i][2] = bits.ReverseBytes32(quads[i][2]) //gcassert:bce
			quads[i][3] = bits.ReverseBytes32(quads[i][3]) //gcassert:bce
		}
	}
	tail := s[len(s)&^3:]
	for i := range tail {
		tail[i] = bits.ReverseBytes32(tail[i]) //gcassert:bce
	}
}

// ReverseBytes64 calls bits.ReverseBytes64 on each element of the input slice.
func ReverseBytes64(s []uint64) {
	if len(s) >= 4 {
		// We convert the slice (up to the tail) to a slice of [4]uint64. This helps
		// the compiler elide bound checks.
		quads := unsafe.Slice((*[4]uint64)(unsafe.Pointer(unsafe.SliceData(s))), len(s)>>2)
		for i := range quads {
			quads[i][0] = bits.ReverseBytes64(quads[i][0]) //gcassert:bce
			quads[i][1] = bits.ReverseBytes64(quads[i][1]) //gcassert:bce
			quads[i][2] = bits.ReverseBytes64(quads[i][2]) //gcassert:bce
			quads[i][3] = bits.ReverseBytes64(quads[i][3]) //gcassert:bce
		}
	}
	tail := s[len(s)&^3:]
	for i := range tail {
		tail[i] = bits.ReverseBytes64(tail[i]) //gcassert:bce
	}
}
