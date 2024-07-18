// Copyright 2024 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package colblk

import (
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

// UnsafeUint8s is an UnsafeIntegerSlice of uint8s, possibly using delta
// encoding internally.
type UnsafeUint8s = UnsafeIntegerSlice[uint8]

// UnsafeUint16s is an UnsafeIntegerSlice of uint16s, possibly using delta
// encoding internally.
type UnsafeUint16s = UnsafeIntegerSlice[uint16]

// UnsafeUint32s is an UnsafeIntegerSlice of uint32s, possibly using delta
// encoding internally.
type UnsafeUint32s = UnsafeIntegerSlice[uint32]

// UnsafeUint64s is an UnsafeIntegerSlice of uint64s, possibly using delta
// encoding internally.
type UnsafeUint64s = UnsafeIntegerSlice[uint64]

// UnsafeIntegerSlice exposes a read-only slice of integers from a column. If
// the column's values are delta-encoded, UnsafeIntegerSlice transparently
// applies deltas.
//
// See DeltaEncoding and UintBuilder.
type UnsafeIntegerSlice[T constraints.Integer] struct {
	base       T
	deltaPtr   unsafe.Pointer
	deltaWidth uintptr
}

// DecodeUnsafeIntegerSlice decodes the structure of a slice of uints from a
// byte slice.
func DecodeUnsafeIntegerSlice[T constraints.Integer](
	b []byte, off uint32, rows int,
) (slice UnsafeIntegerSlice[T], endOffset uint32) {
	delta := UintDeltaEncoding(b[off])
	off++
	switch delta {
	case UintDeltaEncodingNone:
		off = align(off, uint32(unsafe.Sizeof(T(0))))
		slice = makeUnsafeIntegerSlice[T](0, unsafe.Pointer(&b[off]), int(unsafe.Sizeof(T(0))))
		off += uint32(unsafe.Sizeof(T(0))) * uint32(rows)
	case UintDeltaEncodingConstant:
		base := readLittleEndianNonaligned[T](b, off)
		off += uint32(unsafe.Sizeof(T(0)))
		slice = makeUnsafeIntegerSlice[T](base, unsafe.Pointer(&b[off]), 0)
	case UintDeltaEncoding8, UintDeltaEncoding16, UintDeltaEncoding32:
		w := delta.width()
		base := readLittleEndianNonaligned[T](b, off)
		off += uint32(unsafe.Sizeof(T(0)))
		off = align(off, uint32(w))
		slice = makeUnsafeIntegerSlice[T](base, unsafe.Pointer(&b[off]), w)
		off += uint32(rows) * uint32(w)
	default:
		panic("unreachable")
	}
	return slice, off
}

// Assert that DecodeUnsafeIntegerSlice implements DecodeFunc.
var _ DecodeFunc[UnsafeUint8s] = DecodeUnsafeIntegerSlice[uint8]

func makeUnsafeIntegerSlice[T constraints.Integer](
	base T, deltaPtr unsafe.Pointer, deltaWidth int,
) UnsafeIntegerSlice[T] {
	return UnsafeIntegerSlice[T]{
		base:       base,
		deltaPtr:   deltaPtr,
		deltaWidth: uintptr(deltaWidth),
	}
}

// TODO(jackson): Remove when more of the read path is hooked up.
var _ = makeUnsafeIntegerSlice[uint64]

// At returns the `i`-th element of the slice.
func (s UnsafeIntegerSlice[T]) At(i int) T {
	// TODO(jackson): Experiment with other alternatives that might be faster
	// and avoid switching on the width.
	switch s.deltaWidth {
	case 0:
		return s.base
	case 1:
		return s.base + T(*(*uint8)(unsafe.Pointer(uintptr(s.deltaPtr) + uintptr(i))))
	case 2:
		return s.base + T(*(*uint16)(unsafe.Pointer(uintptr(s.deltaPtr) + uintptr(i)<<align16Shift)))
	case 4:
		return s.base + T(*(*uint32)(unsafe.Pointer(uintptr(s.deltaPtr) + uintptr(i)<<align32Shift)))
	case 8:
		// NB: The slice encodes 64-bit integers, there is no base (it doesn't
		// save any bits to compute a delta) and T must be a 64-bit integer. We
		// cast directly into a *T pointer and don't add the base.
		return (*(*T)(unsafe.Pointer(uintptr(s.deltaPtr) + uintptr(i)<<align64Shift)))
	default:
		panic("unreachable")
	}
}

// Clone allocates a new slice and copies the first `rows` elements.
func (s UnsafeIntegerSlice[T]) Clone(rows int) []T {
	result := make([]T, rows)
	for i := 0; i < rows; i++ {
		result[i] = s.At(i)
	}
	return result
}
