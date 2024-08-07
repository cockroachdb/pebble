// Copyright 2024 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package colblk

import (
	"unsafe"

	"golang.org/x/exp/constraints"
)

// align returns the next value greater than or equal to offset that's divisible
// by val.
func align[T constraints.Integer](offset, val T) T {
	return (offset + val - 1) & ^(val - 1)
}

// alignWithZeroes aligns the provided offset to val, and writing zeroes to any
// bytes in buf between the old offset and new aligned offset. This provides
// determinism when reusing memory that has not been zeroed.
func alignWithZeroes[T constraints.Integer](buf []byte, offset, val T) T {
	aligned := align[T](offset, val)
	for i := offset; i < aligned; i++ {
		buf[i] = 0
	}
	return aligned
}

const (
	align16 = 2
	align32 = 4
	align64 = 8
)

// When multiplying or dividing by align{16,32,64} using signed integers, it's
// faster to shift to the left to multiply or shift to the right to divide. (The
// compiler optimization is limited to unsigned integers.) The below constants
// define the shift amounts corresponding to the above align constants.  (eg,
// alignNShift = log(alignN)).
//
// TODO(jackson): Consider updating usages to use uints? They can be less
// ergonomic.
const (
	align16Shift = 1
	align32Shift = 2
	align64Shift = 3
)

// TODO(jackson): A subsequent Go release will remove the ability to call these
// runtime functions. We should consider asm implementations that we maintain
// within the crlib repo.
//
// See https://github.com/golang/go/issues/67401

//go:linkname memmove runtime.memmove
func memmove(to, from unsafe.Pointer, n uintptr)

//go:linkname mallocgc runtime.mallocgc
func mallocgc(size uintptr, typ unsafe.Pointer, needzero bool) unsafe.Pointer
