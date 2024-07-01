// Copyright 2024 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package colblk

import (
	"encoding/binary"
	"fmt"
	"io"
	"unsafe"

	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/pebble/internal/binfmt"
)

// RawBytes holds an array of byte slices, stored as a concatenated data section
// and a series of offsets for each slice. Byte slices within RawBytes are
// stored in their entirety without any compression, ensuring stability without
// copying.
//
// # Representation
//
// An array of N byte slices encodes N+1 offsets. The beginning of the data
// representation holds an offsets table, broken into two sections: 16-bit
// offsets and 32-bit offsets. The first two bytes of the offset table hold the
// count of 16-bit offsets. After the offsets table, the data section encodes
// raw byte data from all slices with no delimiters.
//
// The separation of offsets into 16-bit and 32-bit offsets exists because
// although the vast majority of offsets will fit in 16-bits when using 32 KiB
// blocks (the size in use by CockroachDB), a single value larger than 65535
// bytes requires an offset too large to fit within 16 bits.
//
//	+----------+--------------------------------------------------------+
//	| uint16 k |                   (aligning padding)                   |
//	+----------+--------------------------------------------------------+
//	|                16-bit offsets (k) [16-bit aligned]                |
//	|                                                                   |
//	| off_0 | off_1 | off_2 | ............................,.| off_{k-1} |
//	+-------------------------------------------------------------------+
//	|               32-bit offsets (n-k) [32-bit aligned]               |
//	|                                                                   |
//	|   off_{k}   |   off_{k+1}    | ..................|     off_n      |
//	+-------------------------------------------------------------------+
//	|                           String Data                             |
//	|  abcabcada....                                                    |
//	+-------------------------------------------------------------------+
type RawBytes struct {
	slices     int
	nOffsets16 int
	start      unsafe.Pointer
	data       unsafe.Pointer
	offsets    unsafe.Pointer
}

// MakeRawBytes constructs an accessor for an array of byte slices constructed
// by RawBytesBuilder. Count must be the number of byte slices within the array.
func MakeRawBytes(count uint32, b []byte) RawBytes {
	if count == 0 {
		return RawBytes{}
	}
	return makeRawBytes(count, unsafe.Pointer(&b[0]))
}

// makeRawBytes constructs an accessor for an array of byte slices constructed
// by bytesBuilder. Count must be the number of byte slices within the array.
func makeRawBytes(count uint32, start unsafe.Pointer) RawBytes {
	nOffsets := 1 + count
	nOffsets16 := uint32(binary.LittleEndian.Uint16(unsafe.Slice((*byte)(start), 2)))
	nOffsets32 := nOffsets - nOffsets16

	offsets16 := uintptr(start) + align16 /* nOffsets16 */
	offsets16 = align(offsets16, align16)
	offsets16 -= uintptr(start)

	var data uintptr
	if nOffsets32 == 0 {
		// The variable width data resides immediately after the 16-bit offsets.
		data = offsets16 + uintptr(nOffsets16)*align16
	} else {
		// nOffsets32 > 0
		//
		// The 16-bit offsets must be aligned on a 16-bit boundary, and the
		// 32-bit offsets which immediately follow them must be aligned on a
		// 32-bit boundary. During construction, the bytesBuilder will ensure
		// correct alignment, inserting padding between the start (where the
		// count of 16-bit offsets is encoded) and the beginning of the 16-bit
		// offset table.
		//
		// At read time, we infer the appropriate padding by finding the end of
		// the 16-bit offsets and ensuring it is aligned on a 32-bit boundary,
		// then jumping back from there to the start of the 16-bit offsets.
		offsets32 := offsets16 + uintptr(nOffsets16)*align16 + uintptr(start)
		offsets32 = align(offsets32, align32)
		offsets16 = offsets32 - uintptr(nOffsets16)*align16 - uintptr(start)
		// The variable width data resides immediately after the 32-bit offsets.
		data = offsets32 + uintptr(nOffsets32)*align32 - uintptr(start)
	}

	return RawBytes{
		slices:     int(count),
		nOffsets16: int(nOffsets16),
		start:      start,
		data:       unsafe.Pointer(uintptr(start) + data),
		offsets:    unsafe.Pointer(uintptr(start) + offsets16),
	}
}

func defaultSliceFormatter(x []byte) string {
	return string(x)
}

func rawBytesToBinFormatter(
	f *binfmt.Formatter, count uint32, sliceFormatter func([]byte) string,
) int {
	if sliceFormatter == nil {
		sliceFormatter = defaultSliceFormatter
	}
	rb := makeRawBytes(count, unsafe.Pointer(f.Pointer(0)))
	dataSectionStartOffset := f.Offset() + int(uintptr(rb.data)-uintptr(rb.start))

	var n int
	f.CommentLine("RawBytes")
	n += f.HexBytesln(2, "16-bit offset count: %d", rb.nOffsets16)
	if off := uintptr(f.Pointer(0)); off < uintptr(rb.offsets) {
		n += f.HexBytesln(int(uintptr(rb.offsets)-off), "padding to align offsets table")
	}
	for i := 0; i < rb.nOffsets16; i++ {
		n += f.HexBytesln(2, "off[%d]: %d [overall %d]", i, rb.offset(i), dataSectionStartOffset+rb.offset(i))
	}
	for i := 0; i < int(count+1)-rb.nOffsets16; i++ {
		n += f.HexBytesln(4, "off[%d]: %d [overall %d]", i+rb.nOffsets16, rb.offset(i+rb.nOffsets16), dataSectionStartOffset+rb.offset(i))
	}
	for i := 0; i < rb.slices; i++ {
		s := rb.At(i)
		n += f.HexBytesln(len(s), "data[%d]: %s", i, sliceFormatter(s))
	}
	return n
}

func (b RawBytes) ptr(offset int) unsafe.Pointer {
	return unsafe.Pointer(uintptr(b.data) + uintptr(offset))
}

func (b RawBytes) slice(start, end int) []byte {
	return unsafe.Slice((*byte)(b.ptr(start)), end-start)
}

// offset retrieves the value of the i-th offset.
func (b RawBytes) offset(i int) int {
	// The <= implies that offset 0 always fits in a 16-bit offset. That offset
	// is always zero and is serialized to avoid special-casing i=0 in
	// RawBytes.At.
	if i <= b.nOffsets16 {
		return int(*(*uint16)(unsafe.Pointer(uintptr(b.offsets) + uintptr(i)*align16)))
	}
	return int(*(*uint32)(unsafe.Pointer(uintptr(b.offsets) +
		uintptr(b.nOffsets16)*align16 + uintptr(i-b.nOffsets16)*align32)))
}

// At returns the []byte at index i. The returned slice should not be mutated.
func (b RawBytes) At(i int) []byte {
	s := b.slice(b.offset(i), b.offset(i+1))
	return s
}

// Slices returns the number of []byte slices encoded within the RawBytes.
func (b RawBytes) Slices() int {
	return b.slices
}

// RawBytesBuilder encodes a column of byte slices.
type RawBytesBuilder struct {
	data         []byte
	offsets      []uint32
	numOffsets16 uint16
	maxOffset16  uint32 // configurable for testing purposes
}

// Assert that *RawBytesBuilder implements ColumnWriter.
var _ ColumnWriter = (*RawBytesBuilder)(nil)

// Reset resets the builder to an empty state, preserving the existing bundle
// size.
func (b *RawBytesBuilder) Reset() {
	*b = RawBytesBuilder{
		data:        b.data[:0],
		offsets:     b.offsets[:0],
		maxOffset16: (1 << 16) - 1,
	}
	// Add an initial offset of zero to streamline the logic in RawBytes.At() to
	// avoid needing a special case for row 0.
	b.numOffsets16++
	b.offsets = append(b.offsets, 0)
}

// NumColumns implements ColumnWriter.
func (b *RawBytesBuilder) NumColumns() int { return 1 }

// Put appends the provided byte slice to the builder.
func (b *RawBytesBuilder) Put(s []byte) {
	b.data = append(b.data, s...)
	offset := uint32(len(b.data))
	if offset <= b.maxOffset16 {
		b.numOffsets16++
	}
	b.offsets = append(b.offsets, offset)
}

// PutConcat appends a single byte slice formed by the concatenation of the two
// byte slice arguments.
func (b *RawBytesBuilder) PutConcat(s1, s2 []byte) {
	b.data = append(append(b.data, s1...), s2...)
	offset := uint32(len(b.data))
	if offset <= b.maxOffset16 {
		b.numOffsets16++
	}
	b.offsets = append(b.offsets, offset)
}

// Finish writes the serialized byte slices to buf starting at offset. The buf
// slice must be sufficiently large to store the serialized output. The caller
// should use [Size] to size buf appropriately before calling Finish.
func (b *RawBytesBuilder) Finish(
	col int, rows int, offset uint32, buf []byte,
) (uint32, ColumnDesc) {
	desc := ColumnDesc{DataType: DataTypeBytes, Encoding: EncodingDefault}
	if rows == 0 {
		return 0, desc
	}
	numOffsets := rows + 1
	numOffsets16 := uint16(min(numOffsets, int(b.numOffsets16)))
	numOffsets32 := numOffsets - int(numOffsets16)
	sizeOf16bitOffsetTable := uint32(numOffsets16) * align16

	// Encode the count of 16-bit offsets.
	binary.LittleEndian.PutUint16(buf[offset:], numOffsets16)

	// Compute the positions of the offset tables (16-bit and 32-bit), and the
	// data section.
	offsetOfPadding := offset + align16
	offsetOf16bitOffsets := align(offsetOfPadding, align16)
	offsetOf32bitOffsets := offsetOf16bitOffsets + sizeOf16bitOffsetTable
	offsetOfData := offsetOf32bitOffsets
	if numOffsets32 > 0 {
		// RawBytes requires that the 32-bit offsets are aligned on a 32-bit
		// boundary and the 16-bit offsets which immediately preceed them are
		// aligned on a 16-bit boundary. We do this by finding the end of the
		// 16-bit offsets and ensuring it is aligned on a 32-bit boundary, then
		// jumping back from there to the start of the 16-bit offsets. This may
		// leave unused padding bytes between the first two bytes that encode
		// nOffsets16 and the first 16-bit offset.
		//
		// makeRawBytes applies the same logic in order to infer the beginning of
		// the 16-bit offsets.
		offsetOf32bitOffsets = align(offsetOf32bitOffsets, align32)
		offsetOf16bitOffsets = offsetOf32bitOffsets - sizeOf16bitOffsetTable
		offsetOfData = offsetOf32bitOffsets + uint32(numOffsets32)*align32
	}

	// Zero the padding between the count of 16-bit offsets and the start of the
	// offsets table for determinism when recycling buffers.
	clear(buf[offsetOfPadding:offsetOf16bitOffsets])

	// Write out the offset table.
	dest16 := makeUnsafeRawSlice[uint16](unsafe.Pointer(&buf[offsetOf16bitOffsets]))
	for i := 0; i < int(numOffsets16); i++ {
		if b.offsets[i] > b.maxOffset16 {
			panic(errors.AssertionFailedf("%d: encoding offset %d as 16-bit, but it exceeds maxOffset16 %d",
				i, b.offsets[i], b.maxOffset16))
		}
		dest16.set(i, uint16(b.offsets[i]))
	}
	if numOffsets32 > 0 {
		if offsetOf32bitOffsets != align(offsetOf32bitOffsets, align32) {
			errors.AssertionFailedf("offset not aligned to 32: %d", offsetOf32bitOffsets)
		}
		dest32 := makeUnsafeRawSlice[uint32](unsafe.Pointer(&buf[offsetOf32bitOffsets]))
		copy(dest32.Slice(numOffsets32), b.offsets[b.numOffsets16:])
	}
	// Copy the data section.
	endOffset := offsetOfData + uint32(copy(buf[offsetOfData:], b.data[:b.offsets[rows]]))
	return endOffset, desc
}

// Size computes the size required to encode the byte slices beginning in a
// buffer at the provided offset. The offset is required to ensure proper
// alignment. The returned uint32 is the offset of the first byte after the end
// of the encoded data. To compute the size in bytes, subtract the [offset]
// passed into Size from the returned offset.
func (b *RawBytesBuilder) Size(rows int, offset uint32) uint32 {
	if rows == 0 {
		return 0
	}
	numOffsets := rows + 1
	// The nOffsets16 count.
	offset += align16

	// The 16-bit offsets for variable width data
	nOffsets16 := min(numOffsets, int(b.numOffsets16))
	offset = align(offset, align16)
	offset += uint32(nOffsets16) * align16

	// The 32-bit offsets for variable width data
	nOffsets32 := numOffsets - nOffsets16
	if nOffsets32 > 0 {
		offset = align(offset, align32)
		offset += uint32(nOffsets32) * align32
	}
	return offset + b.offsets[rows]
}

// WriteDebug implements Encoder.
func (b *RawBytesBuilder) WriteDebug(w io.Writer, rows int) {
	fmt.Fprintf(w, "bytes: %d offsets; %d bytes in data", len(b.offsets), len(b.data))
}
