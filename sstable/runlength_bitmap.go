// Copyright 2025 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package sstable

import (
	"encoding/binary"
	"iter"

	"github.com/cockroachdb/pebble/internal/invariants"
)

// BitmapRunLengthEncoder encodes a bitmap. It uses a run-length encoding for
// runs of all ones and all zeros. The encoding is intended for compactly
// representing bitmaps with significant spatial locality and when readers only
// read the bitmap sequentially.
//
// The BitmapRunLengthEncoder considers groups of 8-bits of the bitmap at a
// time. If one of these 8-bit groups (a byte) contains all set bits (0xFF) or
// all unset bits (0x00), the encoder uses a run-length encoding for the byte
// and any consecutive identical bytes. All other bytes are encoded as
// themselves.
//
// For example, an all-set bitmap of 1024 bits would be encoded as the 0xFF byte
// followed by the varint encoding of 128 (1024/8 = 128).
type BitmapRunLengthEncoder struct {
	// buf holds the encoded bitmap up until the beginning of the pending run of
	// all-set bits indicated by allSetRunLength.
	buf []byte
	// allSetRunLength is the count of consecutive pending bytes that are all
	// set (preceding the byte currently being buffered in currByte). When the
	// current byte is finished, if allSetRunLength is greater than zero, the
	// all-set sequence must be encoded to buf before the current byte.
	allSetRunLength int
	// currByte is the current byte being buffered. After the first call to Set,
	// currByte always has at least 1 set bit. currByteIndex is the index of the
	// buffered byte in a fully-materialized bitmap. That is, currByte
	// accumulates the bits within the bitmap in indices [currByteIndex*8,
	// (currByteIndex+1)*8).
	currByte      byte
	currByteIndex int
}

// Init initializes or resets the encoder to its initial state.
func (bb *BitmapRunLengthEncoder) Init() {
	invariants.Mangle(bb.buf)
	bb.buf = bb.buf[:0]
	bb.allSetRunLength = 0
	bb.currByte = 0
	bb.currByteIndex = -1
}

// Set records that the i'th bit in the bitmap is Set. Set must be called with i
// in increasing order.
func (bb *BitmapRunLengthEncoder) Set(i int) {
	// bi is the index of the byte that would contain the i'th bit in a
	// fully-materialized bitmap
	bi := i / 8

	// If the i'th bit falls into the same byte as the previous call to set,
	// we can just set the bit in currByte.
	if bi == bb.currByteIndex {
		bb.currByte |= 1 << (i % 8)
		return
	}

	// The i'th bit falls into a new byte. We may need to finish encoding
	// previous bits whose state is buffered on the encoder.

	// Append what we accumulated within currByte.
	switch bb.currByte {
	case 0x00:
		// If and only if this is the first call to set, currByte is all
		// zeros and we have nothing to append.
	case 0xFF:
		// The current byte contains all set bits. Increment the count of the
		// current pending run.
		bb.allSetRunLength++
	default:
		// The current byte contains a mix of set and unset bits. If there's a
		// pending all-ones run, encode it.
		bb.maybeFlushAllSetRun()
		// And append the mixed byte to buf.
		bb.buf = append(bb.buf, bb.currByte)
	}

	// If there's a gap between the index of the byte we just finished and the
	// index of the new byte we're beginning, we need to append a run of zeros.
	if n := bi - bb.currByteIndex - 1; n > 0 {
		bb.maybeFlushAllSetRun()
		bb.buf = append(bb.buf, 0x00)
		bb.buf = binary.AppendUvarint(bb.buf, uint64(n))
	}

	// Save the bit on currByte.
	bb.currByteIndex = bi
	bb.currByte = 1 << (i % 8)
}

func (bb *BitmapRunLengthEncoder) maybeFlushAllSetRun() {
	if bb.allSetRunLength > 0 {
		bb.buf = append(bb.buf, 0xFF)
		bb.buf = binary.AppendUvarint(bb.buf, uint64(bb.allSetRunLength))
		bb.allSetRunLength = 0
	}
}

// FinishAndAppend appends the encoded bitmap to buf and returns the result.
func (bb *BitmapRunLengthEncoder) FinishAndAppend(buf []byte) []byte {
	// Finish the current pending state.
	switch bb.currByte {
	case 0xFF:
		bb.allSetRunLength++
		bb.maybeFlushAllSetRun()
	case 0x00:
		// Only possible if Set was never called. Do nothing.
	default:
		bb.maybeFlushAllSetRun()
		bb.buf = append(bb.buf, bb.currByte)
	}
	return append(buf, bb.buf...)
}

// Size returns the current number of bytes in the buf, accounting for the buf
// state when our current pending byte has been written. This is useful for
// estimating the size of the bitmap without having to copy the result of
// FinishAndAppend.
func (bb *BitmapRunLengthEncoder) Size() int {
	switch bb.currByte {
	case 0xFF:
		// 1 byte for 0xFF + n bytes for varint encoding of
		// (allSetRunLength + 1).
		v := bb.allSetRunLength + 1
		varintSize := 1
		for v >= 0x80 {
			v >>= 7
			varintSize++
		}
		return len(bb.buf) + 1 + varintSize
	case 0x00:
		// Only possible if Set was never called. Do nothing.
		return len(bb.buf)
	default:
		if bb.allSetRunLength > 0 {
			// 1 byte for 0xFF + n bytes for varint encoding of
			// allSetRunLength + 1 byte for current byte.
			v := bb.allSetRunLength
			varintSize := 1
			for v >= 0x80 {
				v >>= 7
				varintSize++
			}
			return len(bb.buf) + 1 + varintSize + 1
		}
		// For mixed byte case without pending all-set run: just 1 byte for
		// current byte
		return len(bb.buf) + 1
	}
}

// IterSetBitsInRunLengthBitmap returns an iterator over the indices of set bits
// within the provided run-length encoded bitmap.
func IterSetBitsInRunLengthBitmap(bitmap []byte) iter.Seq[int] {
	return func(yield func(int) bool) {
		// i is the index of the current byte in the bitmap.
		i := 0
		// bit is the index of the current bit in the overall bitmap.
		bit := 0
		for i < len(bitmap) {
			b := bitmap[i]
			i++
			switch b {
			case 0x00:
				// Decode the varint. A decoded value of v indicates that there
				// are 8*v consecutive unset bits in the bitmap.
				v, n := binary.Uvarint(bitmap[i:])
				i += n
				bit += 8 * int(v)
			case 0xFF:
				// Decode the varint. A decoded value of v indicates that there
				// are 8*v consecutive set bits in the bitmap.
				v, n := binary.Uvarint(bitmap[i:])
				i += n
				for j := range 8 * int(v) {
					if !yield(bit + j) {
						return
					}
				}
				bit += 8 * int(v)
			default:
				// b is a byte that is neither all ones nor all zeros. Check
				// each bit and yield the index of any set bits to the iterator.
				for j := range 8 {
					if b&(1<<j) == 0 {
						continue
					}
					if !yield(bit + j) {
						return
					}
				}
				bit += 8
			}
		}
	}
}
