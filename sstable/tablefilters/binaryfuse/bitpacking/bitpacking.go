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

// SupportedBitsPerValue contains the bits-per-value supported by the encodings in this package.
var SupportedBitsPerValue = []int{4, 8, 10, 12, 16}

// EncodedSize returns the number of bytes required to encode n values
// using the specified bits-per-value (bpv).
//
// Supports bpv = 4, 8, 10, 12, 16.
func EncodedSize(n int, bpv int) int {
	switch bpv {
	case 4:
		return (n + 1) / 2
	case 8:
		return n
	case 10:
		// 10 bits per value, plus 3 bytes padding for safe uint64 writes in
		// encode10bpv and safe uint16 reads in Decode.
		return (n*10+7)/8 + 3
	case 12:
		// See encode12bpv for the packing scheme.
		return (n+1)/2*3 + 1
	case 16:
		return n * 2
	default:
		panic("bpv must be 4, 8, 10, 12, or 16")
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
// Only bpv values 10, 12 and 16 are supported.
func Encode16(input []uint16, bpv int, output []byte) {
	if len(input) == 0 {
		return
	}
	switch bpv {
	case 10:
		encode10bpv(input, output)
	case 12:
		encode12bpv(input, output)
	case 16:
		encode16bpv(input, output)
	default:
		panic("bpv must be 10, 12, or 16")
	}
}

// encode10bpv packs uint16 values into bytes using 10 bits per value.
// 32 values pack into 5 uint64 words (40 bytes).
func encode10bpv(input []uint16, output []byte) {
	// Bounds check: we need 3 bytes of padding for safe uint64 writes.
	_ = output[(len(input)*10+7)/8+2]

	outPos := 0
	// Main loop: process 32 values into 5 uint64 words.
	if len(input) >= 32 {
		in32 := unsafe.Slice((*[32]uint16)(unsafe.Pointer(unsafe.SliceData(input))), len(input)/32)
		for _, v := range in32 {
			o := (*[40]byte)(unsafe.Add(unsafe.Pointer(unsafe.SliceData(output)), outPos))

			// Pack 32 10-bit values into 5 uint64 words.
			// Values that cross word boundaries are handled by splitting.
			word0 := uint64(v[0]&0x3FF) | uint64(v[1]&0x3FF)<<10 | uint64(v[2]&0x3FF)<<20 |
				uint64(v[3]&0x3FF)<<30 | uint64(v[4]&0x3FF)<<40 | uint64(v[5]&0x3FF)<<50 |
				uint64(v[6]&0xF)<<60
			word1 := uint64(v[6]&0x3FF)>>4 | uint64(v[7]&0x3FF)<<6 | uint64(v[8]&0x3FF)<<16 |
				uint64(v[9]&0x3FF)<<26 | uint64(v[10]&0x3FF)<<36 | uint64(v[11]&0x3FF)<<46 |
				uint64(v[12]&0xFF)<<56
			word2 := uint64(v[12]&0x3FF)>>8 | uint64(v[13]&0x3FF)<<2 | uint64(v[14]&0x3FF)<<12 |
				uint64(v[15]&0x3FF)<<22 | uint64(v[16]&0x3FF)<<32 | uint64(v[17]&0x3FF)<<42 |
				uint64(v[18]&0x3FF)<<52 | uint64(v[19]&0x3)<<62
			word3 := uint64(v[19]&0x3FF)>>2 | uint64(v[20]&0x3FF)<<8 | uint64(v[21]&0x3FF)<<18 |
				uint64(v[22]&0x3FF)<<28 | uint64(v[23]&0x3FF)<<38 | uint64(v[24]&0x3FF)<<48 |
				uint64(v[25]&0x3F)<<58
			word4 := uint64(v[25]&0x3FF)>>6 | uint64(v[26]&0x3FF)<<4 | uint64(v[27]&0x3FF)<<14 |
				uint64(v[28]&0x3FF)<<24 | uint64(v[29]&0x3FF)<<34 | uint64(v[30]&0x3FF)<<44 |
				uint64(v[31]&0x3FF)<<54

			binary.LittleEndian.PutUint64(o[0:], word0)
			binary.LittleEndian.PutUint64(o[8:], word1)
			binary.LittleEndian.PutUint64(o[16:], word2)
			binary.LittleEndian.PutUint64(o[24:], word3)
			binary.LittleEndian.PutUint64(o[32:], word4)

			outPos += 40
		}
		input = input[len(in32)*32:]
	}

	// Tail: encode remaining values using pair-wise packing.
	// Every 4 values = 40 bits = 5 bytes.
	// Clear the tail portion of the output buffer first.
	tailBytes := (len(input)*10 + 7) / 8
	for i := 0; i <= tailBytes; i++ {
		output[outPos+i] = 0
	}

	// Process 4 values at a time (40 bits = 5 bytes).
	for len(input) >= 4 {
		out8 := (*[8]byte)(unsafe.Add(unsafe.Pointer(unsafe.SliceData(output)), outPos))
		// Pack 4 values into 40 bits, write as uint64 (only low 40 bits used).
		w := uint64(input[0]&0x3FF) | uint64(input[1]&0x3FF)<<10 |
			uint64(input[2]&0x3FF)<<20 | uint64(input[3]&0x3FF)<<30
		binary.LittleEndian.PutUint64(out8[:], w)
		outPos += 5
		input = input[4:]
	}

	// Handle remaining 1-3 values.
	bitPos := outPos * 8
	for _, val := range input {
		bytePos := bitPos / 8
		bitOff := uint(bitPos % 8)
		// Write 10 bits starting at bitPos (may span 2 bytes).
		out2 := (*[2]byte)(unsafe.Add(unsafe.Pointer(unsafe.SliceData(output)), bytePos))
		w := uint16(val&0x3FF) << bitOff
		out2[0] |= byte(w)
		out2[1] |= byte(w >> 8)
		bitPos += 10
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
// Supports bpv = 4, 8, 10, 12, 16.
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
	case 10:
		base := (i * 5) / 4
		_ = data[base+1]
		w := unsafeGet16(data, base)
		shift := (i & 3) * 2
		return uint16((w >> shift) & 0x3FF)
	case 12:
		base := (i >> 1) * 3
		_ = data[base+3]
		w := unsafeGet32(data, base)
		shift := (i & 1) * 12
		return uint16((w >> shift) & 0xFFF)
	default:
		panic("bpv must be 4, 8, 10, 12, or 16")
	}
}

// Decode3 returns the i1-th, i2-th, and i3-th value from packed data, assuming
// it was encoded with the given bpv.
//
// Supports bpv = 4, 8, 10, 12, 16. Panics if bpv is not one of these values.
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
	case 10:
		_ = data[(maxIdx*5)/4+1]
		base1, base2, base3 := (i1*5)/4, (i2*5)/4, (i3*5)/4
		w1 := unsafeGet16(data, base1)
		w2 := unsafeGet16(data, base2)
		w3 := unsafeGet16(data, base3)
		shift1 := (i1 & 3) * 2
		shift2 := (i2 & 3) * 2
		shift3 := (i3 & 3) * 2
		return uint16((w1 >> shift1) & 0x3FF),
			uint16((w2 >> shift2) & 0x3FF),
			uint16((w3 >> shift3) & 0x3FF)
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
		panic("bpv must be 4, 8, 10, 12, or 16")
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
