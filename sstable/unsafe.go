// Copyright 2018 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package sstable

import (
	"math/bits"
	"unsafe"
)

func getBytes(ptr unsafe.Pointer, length int) []byte {
	return (*[1 << 30]byte)(ptr)[:length:length]
}

func decodeVarint(ptr unsafe.Pointer) (uint32, unsafe.Pointer) {
	src := (*[5]uint8)(ptr)
	if a := (*src)[0]; a < 128 {
		return uint32(a),
			unsafe.Pointer(uintptr(ptr) + 1)
	} else if a, b := a&0x7f, (*src)[1]; b < 128 {
		return uint32(b)<<7 | uint32(a),
			unsafe.Pointer(uintptr(ptr) + 2)
	} else if b, c := b&0x7f, (*src)[2]; c < 128 {
		return uint32(c)<<14 | uint32(b)<<7 | uint32(a),
			unsafe.Pointer(uintptr(ptr) + 3)
	} else if c, d := c&0x7f, (*src)[3]; d < 128 {
		return uint32(d)<<21 | uint32(c)<<14 | uint32(b)<<7 | uint32(a),
			unsafe.Pointer(uintptr(ptr) + 4)
	} else {
		d, e := d&0x7f, (*src)[4]
		return uint32(e)<<28 | uint32(d)<<21 | uint32(c)<<14 | uint32(b)<<7 | uint32(a),
			unsafe.Pointer(uintptr(ptr) + 5)
	}
}

// xxxxxxx1  7 bits in 1 byte
// xxxxxx10 14 bits in 2 bytes
// xxxxx100 21 bits in 3 bytes
// xxxx1000 28 bits in 4 bytes
// xxx10000 35 bits in 5 bytes

func encodePrefixVarint(buf []byte, v uint32) int {
	bits := 32 - bits.LeadingZeros32(v)
	bytes := 1 + (bits-1)/7
	_ = buf[4]
	buf[0] = byte(((v << 1) + 1) << uint(bytes-1))
	v >>= uint(8 - bytes)
	buf[1] = byte(v)
	buf[2] = byte(v >> 8)
	buf[3] = byte(v >> 16)
	buf[4] = byte(v >> 24)
	return bytes
}

var mask = [6]uint32{
	0,
	(1 << 7) - 1,
	(1 << 14) - 1,
	(1 << 21) - 1,
	(1 << 28) - 1,
	(1 << 32) - 1,
}

func decodePrefixVarint(ptr unsafe.Pointer) (uint32, unsafe.Pointer) {
	src := (*[5]uint8)(ptr)
	bytes := uint(1 + bits.TrailingZeros8(src[0]))
	v := uint64(src[0]) | uint64(src[1])<<8 | uint64(src[2])<<16 | uint64(src[3])<<24 | uint64(src[4])<<32
	v >>= bytes
	return uint32(v) & mask[bytes], unsafe.Pointer(uintptr(ptr) + uintptr(bytes))
}
