// Copyright 2025 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

// NB: this list of tags is taken from encoding/binary/native_endian_big.go
//go:build armbe || arm64be || m68k || mips || mips64 || mips64p32 || ppc || ppc64 || s390 || s390x || shbe || sparc || sparc64

package colblk

import (
	"math/bits"
	"unsafe"
)

// BigEndian is true if the target platform is big endian.
const BigEndian = true

//gcassert:inline
func (s unsafeUint64Decoder) At(idx int) uint64 {
	return bits.ReverseBytes64(*(*uint64)(unsafe.Add(s.ptr, uintptr(idx)<<align64Shift)))
}

// At returns the `i`-th element.
func (s UnsafeUints) At(i int) uint64 {
	// One of the most common case is decoding timestamps, which require the full
	// 8 bytes (2^32 nanoseconds is only ~4 seconds).
	if s.width == 8 {
		// NB: The slice encodes 64-bit integers, there is no base (it doesn't save
		// any bits to compute a delta). We cast directly into a *uint64 pointer and
		// don't add the base.
		return bits.ReverseBytes64(*(*uint64)(unsafe.Add(s.ptr, uintptr(i)<<align64Shift)))
	}
	// Another common case is 0 width, when all keys have zero logical timestamps.
	if s.width == 0 {
		return s.base
	}
	if s.width == 4 {
		value := bits.ReverseBytes32(*(*uint32)(unsafe.Add(s.ptr, uintptr(i)<<align32Shift)))
		return s.base + uint64(value)
	}
	if s.width == 2 {
		value := bits.ReverseBytes16(*(*uint16)(unsafe.Add(s.ptr, uintptr(i)<<align16Shift)))
		return s.base + uint64(value)
	}
	return s.base + uint64(*(*uint8)(unsafe.Add(s.ptr, uintptr(i))))
}

// At returns the `i`-th offset.
//
//gcassert:inline
func (s UnsafeOffsets) At(i int) uint32 {
	// We expect offsets to be encoded as 16-bit integers in most cases.
	if s.width == 2 {
		value := *(*uint16)(unsafe.Add(s.ptr, uintptr(i)<<align16Shift))
		return uint32(bits.ReverseBytes16(value))
	}
	if s.width <= 1 {
		if s.width == 0 {
			return 0
		}
		return uint32(*(*uint8)(unsafe.Add(s.ptr, i)))
	}
	value := *(*uint32)(unsafe.Add(s.ptr, uintptr(i)<<align32Shift))
	return bits.ReverseBytes32(value)
}

// At2 returns the `i`-th and `i+1`-th offsets.
//
//gcassert:inline
func (s UnsafeOffsets) At2(i int) (uint32, uint32) {
	// We expect offsets to be encoded as 16-bit integers in most cases.
	if s.width == 2 {
		// l1 h1 l2 h2
		v := *(*uint32)(unsafe.Add(s.ptr, uintptr(i)<<align16Shift))
		v = bits.ReverseBytes32(v)
		return v & 0xFFFF, v >> 16
	}
	if s.width <= 1 {
		if s.width == 0 {
			return 0, 0
		}
		v := *(*uint16)(unsafe.Add(s.ptr, uintptr(i)))
		// No need to ReverseBytes16, we can just return in the correct order.
		return uint32(v >> 8), uint32(v & 0xFF)
	}
	v := *(*uint64)(unsafe.Add(s.ptr, uintptr(i)<<align32Shift))
	v = bits.ReverseBytes64(v)
	return uint32(v), uint32(v >> 32)
}
