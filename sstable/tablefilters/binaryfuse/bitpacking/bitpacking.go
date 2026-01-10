// Copyright 2026 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

// Package bitpacking provides functions for encoding and decoding packed
// integer values.
//
// The encodings are optimized for fast random access to individual values
// directly from the encoded data.
package bitpacking

import (
	"encoding/binary"
	"unsafe"

	"github.com/cockroachdb/pebble/internal/invariants"
)

// EncodedSize returns the number of bytes required to encode n values
// using the specified bits-per-value (bpv).
//
// Supports bpv = 4, 8, 12, 16.
func EncodedSize(n int, bpv int) int {
	switch bpv {
	case 4:
		return (n + 1) / 2
	case 8:
		return n
	case 12:
		// See encode12bpv for the packing scheme.
		return (n+1)/2*3 + 1
	case 16:
		return n * 2
	default:
		panic("bpv must be 4, 8, 12, or 16")
	}
}

// Encode8 packs uint8 values into bytes using the specified bits-per-value
// (bpv). When bpv < 8, the lower bpv bits of each value are used.
//
// For bpv=4, two values are packed per byte (lower 4 bits each).
// For bpv=8, values are copied directly.
//
// Panics if bpv is not 4 or 8.
//
// Exactly EncodedSize(len(input), bpv) bytes of the output buffer will be
// written; the buffer must be at least as big.
func Encode8(input []uint8, bpv int, output []byte) {
	if len(input) == 0 {
		return
	}
	switch bpv {
	case 4:
		encode4bpv(input, output)
	case 8:
		copy(output[:len(input)], input)
	default:
		panic("bpv must be 4 or 8")
	}
}

// encode4bpv packs uint8 values into bytes using 4 bits per value; two values
// are packed in each byte.
func encode4bpv(input []uint8, output []byte) {
	_ = output[(len(input)+1)/2-1]
	if len(input) >= 8 {
		in8 := unsafe.Slice((*[8]uint8)(unsafe.Pointer(unsafe.SliceData(input))), len(input)/8)
		out4 := unsafe.Slice((*[4]uint8)(unsafe.Pointer(unsafe.SliceData(output))), len(input)/8)
		_ = out4[len(in8)-1]
		for i, v := range in8 {
			x := binary.LittleEndian.Uint64(v[:])
			// Use SWAR to pack the nibbles into an uint32.
			x &= 0x0F0F0F0F0F0F0F0F                              // keep the nibbles for each byte
			x = (x | (x >> 4)) & 0x00FF00FF00FF00FF              // pack pairs of nibbles into even bytes
			x = (x | (x >> 8)) & 0x0000FFFF0000FFFF              // pack even bytes into even 16-bit words
			x |= x >> 16                                         // pack into low 32 bits
			binary.LittleEndian.PutUint32(out4[i][:], uint32(x)) //gcassert:bce
		}
		input = input[len(in8)*8:]
		output = output[len(in8)*4:]
	}
	if len(input) > 1 {
		pairs := unsafe.Slice((*[2]uint8)(unsafe.Pointer(unsafe.SliceData(input))), len(input)/2)
		_ = output[len(pairs)-1]
		for i, p := range pairs {
			output[i] = (p[0] & 0x0F) | (p[1]&0x0F)<<4 //gcassert:bce
		}
	}
	if len(input)%2 == 1 {
		output[len(input)/2] = input[len(input)-1] & 0x0F
	}
}

// Encode16 packs uint16 values into bytes using the specified bits-per-value
// (bpv). When bpv < 16, the lower bpv bits of each value are used.
//
// Exactly EncodedSize(len(input), bpv) bytes of the output buffer will be
// written; the buffer must be at least as big.
//
// Only bpv values 12 and 16 are supported.
func Encode16(input []uint16, bpv int, output []byte) {
	if len(input) == 0 {
		return
	}
	switch bpv {
	case 12:
		encode12bpv(input, output)
	case 16:
		encode16bpv(input, output)
	default:
		panic("bpv must be 12 or 16")
	}
}

// encode12bpv packs uint16 values into bytes using 12 bits per value.
//
// Two values (a, b) are packed into 3 bytes:
//   - byte0 = low 8 bits of a
//   - byte1 = high 4 bits of a (low nibble) | low 4 bits of b (high nibble)
//   - byte2 = high 8 bits of b
//
// We leave a padding byte at the end so that we can use 32-bit reads (see
// Decode).
func encode12bpv(input []uint16, output []byte) {
	// We always pad with an extra 0 byte so we can do 32-bit reads and writes.
	_ = output[(len(input)+1)/2*3]

	outPos := 0
	if len(input) >= 8 {
		in8 := unsafe.Slice((*[8]uint16)(unsafe.Pointer(unsafe.SliceData(input))), len(input)/8)
		for _, v := range in8 {
			o := (*[13]byte)(unsafe.Add(unsafe.Pointer(unsafe.SliceData(output)), outPos))
			// This benchmarks better than calculating and writing a 64-bit and a
			// 32-bit value.
			binary.LittleEndian.PutUint32(o[:], uint32(v[0]&0xFFF)|uint32(v[1]&0xFFF)<<12)
			binary.LittleEndian.PutUint32(o[3:], uint32(v[2]&0xFFF)|uint32(v[3]&0xFFF)<<12)
			binary.LittleEndian.PutUint32(o[6:], uint32(v[4]&0xFFF)|uint32(v[5]&0xFFF)<<12)
			binary.LittleEndian.PutUint32(o[9:], uint32(v[6]&0xFFF)|uint32(v[7]&0xFFF)<<12)
			outPos += 12
		}
		input = input[len(in8)*8:]
	}
	for len(input) >= 2 {
		out4 := (*[4]byte)(unsafe.Add(unsafe.Pointer(unsafe.SliceData(output)), outPos))
		binary.LittleEndian.PutUint32(out4[:], uint32(input[0]&0xFFF)|uint32(input[1]&0xFFF)<<12)
		outPos += 3
		input = input[2:]
	}
	if len(input) > 0 {
		a := input[0]
		out4 := (*[4]byte)(unsafe.Add(unsafe.Pointer(unsafe.SliceData(output)), outPos))
		binary.LittleEndian.PutUint32(out4[:], uint32(a&0xFFF))
	}
}

func encode16bpv(input []uint16, output []byte) {
	// If we're little endian, we can just copy.
	if v := [2]byte{1, 0}; binary.NativeEndian.Uint16(v[:]) == 1 {
		if invariants.Enabled && invariants.Sometimes(10) {
			// Sometimes skip the little-endian path to check the general path.
		} else {
			inBytes := unsafe.Slice((*byte)(unsafe.Pointer(unsafe.SliceData(input))), len(input)*2)
			copy(output[:len(input)*2], inBytes)
			return
		}
	}

	// Verify the buffer is large enough.
	_ = output[len(input)*2-1]

	if len(input) >= 8 {
		in4 := unsafe.Slice((*[4]uint16)(unsafe.Pointer(unsafe.SliceData(input))), len(input)/8)
		out8 := unsafe.Slice((*[8]uint8)(unsafe.Pointer(unsafe.SliceData(output))), len(input)/8)
		_ = out8[len(in4)-1]
		for i, v := range in4 {
			x := uint64(v[0]) | uint64(v[1])<<16 | uint64(v[2])<<32 | uint64(v[3])<<48
			binary.LittleEndian.PutUint64(out8[i][:], x) //gcassert:bce
		}
		input = input[len(in4)*4:]
		output = output[len(in4)*8:]
	}

	if len(input) > 0 {
		out2 := unsafe.Slice((*[2]byte)(unsafe.Pointer(unsafe.SliceData(output))), len(input))
		_ = out2[len(input)-1]
		for i, v := range input {
			binary.LittleEndian.PutUint16(out2[i][:], v) //gcassert:bce
		}
	}
}

// Decode returns the i-th value from packed data, assuming it was encoded with the given bpv.
// Supports bpv = 4, 8, 12, 16.
// Panics if bpv is not one of these values.
func Decode(data []byte, i uint, bpv int) uint16 {
	switch bpv {
	case 8:
		return uint16(data[i])
	case 16:
		_ = data[i*2+1]
		return unsafeGet16(data, i*2)
	case 4:
		shift := (i & 1) * 4
		return uint16((data[i>>1] >> shift) & 0x0F)
	case 12:
		base := (i >> 1) * 3
		_ = data[base+3]
		w := unsafeGet32(data, base)
		shift := (i & 1) * 12
		return uint16((w >> shift) & 0xFFF)
	default:
		panic("bpv must be 4, 8, 12, or 16")
	}
}

// Decode3 returns the i1-th, i2-th, and i3-th value from packed data, assuming
// it was encoded with the given bpv.
//
// Supports bpv = 4, 8, 12, 16. Panics if bpv is not one of these values.
func Decode3(data []byte, i1, i2, i3 uint, bpv int) (uint16, uint16, uint16) {
	maxIdx := max(max(i1, i2), i3)
	switch bpv {
	case 8:
		_ = data[maxIdx]
		return uint16(unsafeGet8(data, i1)), uint16(unsafeGet8(data, i2)), uint16(unsafeGet8(data, i3))
	case 16:
		_ = data[maxIdx<<1+1]
		return unsafeGet16(data, i1<<1), unsafeGet16(data, i2<<1), unsafeGet16(data, i3<<1)
	case 4:
		_ = data[maxIdx>>1]
		shift1 := (i1 & 1) * 4
		shift2 := (i2 & 1) * 4
		shift3 := (i3 & 1) * 4
		return uint16((unsafeGet8(data, i1>>1) >> shift1) & 0x0F),
			uint16((unsafeGet8(data, i2>>1) >> shift2) & 0x0F),
			uint16((unsafeGet8(data, i3>>1) >> shift3) & 0x0F)
	case 12:
		_ = data[(maxIdx>>1)*3+3]
		base1 := (i1 >> 1) * 3
		base2 := (i2 >> 1) * 3
		base3 := (i3 >> 1) * 3
		w1 := unsafeGet32(data, base1)
		w2 := unsafeGet32(data, base2)
		w3 := unsafeGet32(data, base3)
		shift1 := (i1 & 1) * 12
		shift2 := (i2 & 1) * 12
		shift3 := (i3 & 1) * 12
		return uint16((w1 >> shift1) & 0xFFF), uint16((w2 >> shift2) & 0xFFF), uint16((w3 >> shift3) & 0xFFF)
	default:
		panic("bpv must be 4, 8, 12, or 16")
	}
}

func unsafeGet8(data []byte, index uint) uint8 {
	return *(*byte)(unsafe.Add(unsafe.Pointer(unsafe.SliceData(data)), index))
}

func unsafeGet16(data []byte, index uint) uint16 {
	ptr := (*[2]byte)(unsafe.Add(unsafe.Pointer(unsafe.SliceData(data)), index))
	return binary.LittleEndian.Uint16(ptr[:2])
}

func unsafeGet32(data []byte, index uint) uint32 {
	ptr := (*[4]byte)(unsafe.Add(unsafe.Pointer(unsafe.SliceData(data)), index))
	return binary.LittleEndian.Uint32(ptr[:4])
}
