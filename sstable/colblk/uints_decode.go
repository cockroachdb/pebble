// Copyright 2025 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package colblk

import (
	"unsafe"

	"github.com/cockroachdb/errors"
)

// unsafeUint64Decoder is used to access 64-bit unsigned integer backed by a
// buffer in little-ending format. It is desirable to keep this type as small as
// possible since it is embedded multiple times in block decoders.
type unsafeUint64Decoder struct {
	ptr unsafe.Pointer
}

func makeUnsafeUint64Decoder(buf []byte, n int) unsafeUint64Decoder {
	ptr := unsafe.Pointer(unsafe.SliceData(buf))
	if align(uintptr(ptr), align64) != uintptr(ptr) {
		panic(errors.AssertionFailedf("slice pointer %p not %d-byte aligned", ptr, align64))
	}
	if len(buf) < n<<align64Shift {
		panic(errors.AssertionFailedf("data buffer is too small"))
	}
	return unsafeUint64Decoder{ptr: ptr}
}

//gcassert:inline
func (s unsafeUint64Decoder) At(idx int) uint64 {
	// TODO(radu): handle big-endian.
	return *(*uint64)(unsafe.Add(s.ptr, uintptr(idx)<<align64Shift))
}
