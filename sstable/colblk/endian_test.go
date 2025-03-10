// Copyright 2025 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package colblk

import (
	"encoding/binary"
	"math/rand/v2"
	"slices"
	"testing"
	"unsafe"
)

func TestReverseBytes16(t *testing.T) {
	for it := 0; it < 100; it++ {
		n := rand.IntN(100)
		// Generate some numbers.
		vals := make([]uint16, n)
		for i := range vals {
			vals[i] = uint16(rand.Uint32())
		}
		// Encode them as little endian.
		little := make([]uint16, n)
		for i := range vals {
			binary.LittleEndian.PutUint16(unsafe.Slice((*byte)(unsafe.Pointer(&little[i])), align16), vals[i])
		}
		// Convert to big endian.
		big := slices.Clone(little)
		ReverseBytes16(big)
		// Confirm big endian encoding.
		for i := range vals {
			v := binary.BigEndian.Uint16(unsafe.Slice((*byte)(unsafe.Pointer(&big[i])), align16))
			if v != vals[i] {
				t.Fatalf("%d/%d: %X instead of %X", i, len(vals), v, vals[i])
			}
		}
	}
}

func TestReverseBytes32(t *testing.T) {
	for it := 0; it < 100; it++ {
		n := rand.IntN(100)
		// Generate some numbers.
		vals := make([]uint32, n)
		for i := range vals {
			vals[i] = rand.Uint32()
		}
		// Encode them as little endian.
		little := make([]uint32, n)
		for i := range vals {
			binary.LittleEndian.PutUint32(unsafe.Slice((*byte)(unsafe.Pointer(&little[i])), align32), vals[i])
		}
		// Convert to big endian.
		big := slices.Clone(little)
		ReverseBytes32(big)
		// Confirm big endian encoding.
		for i := range vals {
			v := binary.BigEndian.Uint32(unsafe.Slice((*byte)(unsafe.Pointer(&big[i])), align32))
			if v != vals[i] {
				t.Fatalf("%X instead of %X", v, vals[i])
			}
		}
	}
}

func TestReverseBytes64(t *testing.T) {
	for it := 0; it < 100; it++ {
		n := rand.IntN(100)
		// Generate some numbers.
		vals := make([]uint64, n)
		for i := range vals {
			vals[i] = rand.Uint64()
		}
		// Encode them as little endian.
		little := make([]uint64, n)
		for i := range vals {
			binary.LittleEndian.PutUint64(unsafe.Slice((*byte)(unsafe.Pointer(&little[i])), align64), vals[i])
		}
		// Convert to big endian.
		big := slices.Clone(little)
		ReverseBytes64(big)
		// Confirm big endian encoding.
		for i := range vals {
			v := binary.BigEndian.Uint64(unsafe.Slice((*byte)(unsafe.Pointer(&big[i])), align64))
			if v != vals[i] {
				t.Fatalf("%X instead of %X", v, vals[i])
			}
		}
	}
}
