// Copyright 2024 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package colblk

import (
	"encoding/binary"
	"unsafe"

	"github.com/cockroachdb/errors"
	"golang.org/x/exp/constraints"
)

// UnsafeRawSlice maintains a pointer to a slice of elements of type T.
// UnsafeRawSlice provides no bounds checking.
type UnsafeRawSlice[T constraints.Integer] struct {
	ptr unsafe.Pointer
}

func makeUnsafeRawSlice[T constraints.Integer](ptr unsafe.Pointer) UnsafeRawSlice[T] {
	if align(uintptr(ptr), unsafe.Sizeof(T(0))) != uintptr(ptr) {
		panic(errors.AssertionFailedf("slice pointer %p not %d-byte aligned", ptr, unsafe.Sizeof(T(0))))
	}
	return UnsafeRawSlice[T]{ptr: ptr}
}

// At returns the `i`-th element of the slice.
func (s UnsafeRawSlice[T]) At(i int) T {
	return *(*T)(unsafe.Pointer(uintptr(s.ptr) + unsafe.Sizeof(T(0))*uintptr(i)))
}

// Slice returns a go []T slice containing the first `len` elements of the
// unsafe slice.
func (s UnsafeRawSlice[T]) Slice(len int) []T {
	return unsafe.Slice((*T)(s.ptr), len)
}

// set mutates the slice, setting the `i`-th value to `v`.
func (s UnsafeRawSlice[T]) set(i int, v T) {
	*(*T)(unsafe.Pointer(uintptr(s.ptr) + unsafe.Sizeof(T(0))*uintptr(i))) = v
}

// UnsafeUints exposes a read-only view of integers from a column, transparently
// decoding data based on the UintEncoding.
//
// See UintEncoding and UintBuilder.
type UnsafeUints struct {
	base  uint64
	ptr   unsafe.Pointer
	width uint8
}

// Assert that UnsafeIntegerSlice implements Array.
var _ Array[uint64] = UnsafeUints{}

// DecodeUnsafeUints decodes the structure of a slice of uints from a
// byte slice.
func DecodeUnsafeUints(b []byte, off uint32, rows int) (_ UnsafeUints, endOffset uint32) {
	encoding := UintEncoding(b[off])
	if !encoding.IsValid() {
		panic(errors.AssertionFailedf("invalid encoding 0x%x", b))
	}
	off++
	var base uint64
	if encoding.IsDelta() {
		base = binary.LittleEndian.Uint64(b[off:])
		off += 8
	}
	w := encoding.Width()
	if w > 0 {
		off = align(off, uint32(w))
	}
	return makeUnsafeUints(base, unsafe.Pointer(&b[off]), w), off + uint32(rows*w)
}

// Assert that DecodeUnsafeIntegerSlice implements DecodeFunc.
var _ DecodeFunc[UnsafeUints] = DecodeUnsafeUints

func makeUnsafeUints(base uint64, ptr unsafe.Pointer, width int) UnsafeUints {
	switch width {
	case 0, 1, 2, 4, 8:
	default:
		panic("invalid width")
	}
	return UnsafeUints{
		base:  base,
		ptr:   ptr,
		width: uint8(width),
	}
}

// At returns the `i`-th element.
func (s UnsafeUints) At(i int) uint64 {
	// TODO(radu): this implementation assumes little-endian architecture.

	// One of the most common case is decoding timestamps, which require the full
	// 8 bytes (2^32 nanoseconds is only ~4 seconds).
	if s.width == 8 {
		// NB: The slice encodes 64-bit integers, there is no base (it doesn't save
		// any bits to compute a delta). We cast directly into a *uint64 pointer and
		// don't add the base.
		return *(*uint64)(unsafe.Pointer(uintptr(s.ptr) + uintptr(i)<<align64Shift))
	}
	// Another common case is 0 width, when all keys have zero logical timestamps.
	if s.width == 0 {
		return s.base
	}
	if s.width == 4 {
		return s.base + uint64(*(*uint32)(unsafe.Pointer(uintptr(s.ptr) + uintptr(i)<<align32Shift)))
	}
	if s.width == 2 {
		return s.base + uint64(*(*uint16)(unsafe.Pointer(uintptr(s.ptr) + uintptr(i)<<align16Shift)))
	}
	return s.base + uint64(*(*uint8)(unsafe.Pointer(uintptr(s.ptr) + uintptr(i))))
}

// UnsafeOffsets is a specialization of UnsafeInts (providing the same
// functionality) which is optimized when the integers are offsets inside a
// column block. It can only be used with 0, 1, 2, or 4 byte encoding without
// delta.
type UnsafeOffsets struct {
	ptr   unsafe.Pointer
	width uint8
}

// DecodeUnsafeOffsets decodes the structure of a slice of offsets from a byte
// slice.
func DecodeUnsafeOffsets(b []byte, off uint32, rows int) (_ UnsafeOffsets, endOffset uint32) {
	ints, endOffset := DecodeUnsafeUints(b, off, rows)
	if ints.base != 0 || ints.width == 8 {
		panic(errors.AssertionFailedf("unexpected offsets encoding (base=%d, width=%d)", ints.base, ints.width))
	}
	return UnsafeOffsets{
		ptr:   ints.ptr,
		width: ints.width,
	}, endOffset
}

// At returns the `i`-th offset.
func (s UnsafeOffsets) At(i int) uint32 {
	// TODO(radu): this implementation assumes little-endian architecture.

	// We expect offsets to be encoded as 16-bit integers in most cases.
	if s.width == 2 {
		return uint32(*(*uint16)(unsafe.Pointer(uintptr(s.ptr) + uintptr(i)<<align16Shift)))
	}
	if s.width <= 1 {
		if s.width == 0 {
			return 0
		}
		return uint32(*(*uint8)(unsafe.Pointer(uintptr(s.ptr) + uintptr(i))))
	}
	return *(*uint32)(unsafe.Pointer(uintptr(s.ptr) + uintptr(i)<<align32Shift))
}

// At2 returns the `i`-th and `i+1`-th offsets.
func (s UnsafeOffsets) At2(i int) (uint32, uint32) {
	// TODO(radu): this implementation assumes little-endian architecture.

	// We expect offsets to be encoded as 16-bit integers in most cases.
	if s.width == 2 {
		v := *(*uint32)(unsafe.Pointer(uintptr(s.ptr) + uintptr(i)<<align16Shift))
		return v & 0xFFFF, v >> 16
	}
	if s.width <= 1 {
		if s.width == 0 {
			return 0, 0
		}
		v := *(*uint16)(unsafe.Pointer(uintptr(s.ptr) + uintptr(i)))
		return uint32(v & 0xFF), uint32(v >> 8)
	}
	v := *(*uint64)(unsafe.Pointer(uintptr(s.ptr) + uintptr(i)<<align32Shift))
	return uint32(v), uint32(v >> 32)
}

// UnsafeBuf provides a buffer without bounds checking. Every buf has a len and
// capacity.
type UnsafeBuf struct {
	ptr unsafe.Pointer
	len int
	cap int
}

// Alloc allocates a buffer of size n, without zeroing its contents or copying
// previous buffer contents.
func (b *UnsafeBuf) Alloc(n int) {
	b.ptr = mallocgc(uintptr(n), nil, false)
	b.cap = n
}

// UnsafeSlice returns the current contents of the buf.
func (b *UnsafeBuf) UnsafeSlice() []byte {
	return unsafe.Slice((*byte)(b.ptr), b.len)
}
