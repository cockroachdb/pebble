// Copyright 2024 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package colblk

import (
	"unsafe"

	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/pebble/internal/invariants"
)

// serializedInt is the list of types we can serialize. Note that more types of
// integers are encoded on top of the serialized ints.
type serializedUint interface {
	~uint8 | ~uint16 | ~uint32 | ~uint64
}

// uintsEncoder is a helper struct to encode integers into a buffer using
// little-endian encoding.
type uintsEncoder[T serializedUint] struct {
	// The underlying slice contains the integers to be encoded. Integers are
	// stored in the native byte order and are converted to little-endian at the
	// end (if necessary).
	slice []T
}

// makeUintsEncoder initializes a uintsEncoder that will write out integers to
// the given buffer, in little-endian order.
// UnsafeSet() is used to set values, then Finish() must be called at the end.
func makeUintsEncoder[T serializedUint](targetBuf []byte, n int) uintsEncoder[T] {
	ptr := unsafe.Pointer(unsafe.SliceData(targetBuf))
	if align(uintptr(ptr), unsafe.Sizeof(T(0))) != uintptr(ptr) {
		panic(errors.AssertionFailedf("slice pointer %p not %d-byte aligned", ptr, unsafe.Sizeof(T(0))))
	}
	if len(targetBuf) < n*int(unsafe.Sizeof(T(0))) {
		panic(errors.AssertionFailedf("target buffer is too small"))
	}
	return uintsEncoder[T]{
		slice: unsafe.Slice((*T)(ptr), n),
	}
}

// Len returns the number of elements that the encoder holds.
func (s uintsEncoder[T]) Len() int {
	return len(s.slice)
}

// UnsafeSet sets the value at an index. It's unsafe in that we don't do bounds
// checking (except in invariant builds).
func (s uintsEncoder[T]) UnsafeSet(i int, v T) {
	if invariants.Enabled {
		_ = s.slice[i]
	}
	*(*T)(unsafe.Add(unsafe.Pointer(unsafe.SliceData(s.slice)), unsafe.Sizeof(T(0))*uintptr(i))) = v
}

// CopyFrom sets the values from the given slice, starting at the given index.
func (s uintsEncoder[T]) CopyFrom(i int, in []T) {
	copy(s.slice[i:i+len(in)], in)
}

func (s uintsEncoder[T]) Finish() {
	// TODO(radu): swap order on big-endian arch.
}
