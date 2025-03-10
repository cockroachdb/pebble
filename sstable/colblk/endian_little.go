// Copyright 2025 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

// NB: this list of tags is taken from encoding/binary/native_endian_little.go
//go:build 386 || amd64 || amd64p32 || alpha || arm || arm64 || loong64 || mipsle || mips64le || mips64p32le || nios2 || ppc64le || riscv || riscv64 || sh || wasm

package colblk

import "unsafe"

// BigEndian is true if the target platform is big endian.
const BigEndian = false

//gcassert:inline
func (s unsafeUint64Decoder) At(idx int) uint64 {
	return *(*uint64)(unsafe.Add(s.ptr, uintptr(idx)<<align64Shift))
}

// At returns the `i`-th element.
func (s UnsafeUints) At(i int) uint64 {
	// One of the most common case is decoding timestamps, which require the full
	// 8 bytes (2^32 nanoseconds is only ~4 seconds).
	if s.width == 8 {
		// NB: The slice encodes 64-bit integers, there is no base (it doesn't save
		// any bits to compute a delta). We cast directly into a *uint64 pointer and
		// don't add the base.
		return *(*uint64)(unsafe.Add(s.ptr, uintptr(i)<<align64Shift))
	}
	// Another common case is 0 width, when all keys have zero logical timestamps.
	if s.width == 0 {
		return s.base
	}
	if s.width == 4 {
		value := *(*uint32)(unsafe.Add(s.ptr, uintptr(i)<<align32Shift))
		return s.base + uint64(value)
	}
	if s.width == 2 {
		value := *(*uint16)(unsafe.Add(s.ptr, uintptr(i)<<align16Shift))
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
		return uint32(value)
	}
	if s.width <= 1 {
		if s.width == 0 {
			return 0
		}
		return uint32(*(*uint8)(unsafe.Add(s.ptr, i)))
	}
	return *(*uint32)(unsafe.Add(s.ptr, uintptr(i)<<align32Shift))
}

// At2 returns the `i`-th and `i+1`-th offsets.
//
//gcassert:inline
func (s UnsafeOffsets) At2(i int) (uint32, uint32) {
	// We expect offsets to be encoded as 16-bit integers in most cases.
	if s.width == 2 {
		v := *(*uint32)(unsafe.Add(s.ptr, uintptr(i)<<align16Shift))
		return v & 0xFFFF, v >> 16
	}
	if s.width <= 1 {
		if s.width == 0 {
			return 0, 0
		}
		v := *(*uint16)(unsafe.Add(s.ptr, uintptr(i)))
		return uint32(v & 0xFF), uint32(v >> 8)
	}
	v := *(*uint64)(unsafe.Add(s.ptr, uintptr(i)<<align32Shift))
	return uint32(v), uint32(v >> 32)
}
