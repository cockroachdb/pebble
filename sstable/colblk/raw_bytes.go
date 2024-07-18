// Copyright 2024 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package colblk

import (
	"fmt"
	"io"
	"unsafe"

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
// representation holds an offsets table, in the same encoding as a
// DataTypeUint32 column. The Uint32 offsets may be delta-encoded to save space
// if all offsets fit within an 8-bit or 16-bit uint. Each offset is relative to
// the beginning of the string data section (after the offset table).
//
// The use of delta encoding conserves space in the common case. In the context
// of CockroachDB, the vast majority of offsets will fit in 16-bits when using
// 32 KiB blocks (the size in use by CockroachDB). However a single value larger
// than 65535 bytes requires an offset too large to fit within 16 bits, in which
// case offsets will be encoded as 32-bit integers.
//
//	+-------------------------------------------------------------------+
//	|        a uint32 offsets table, possibly delta encoded,            |
//	|                possibly padded for 32-bit alignment               |
//	|                      (see DeltaEncoding)                          |
//	+-------------------------------------------------------------------+
//	|                           String Data                             |
//	|  abcabcada....                                                    |
//	+-------------------------------------------------------------------+
//
// The DeltaEncoding bits of the ColumnEncoding for a RawBytes column describes
// the delta encoding of the offset table.
type RawBytes struct {
	slices  int
	offsets UnsafeUint32s
	start   unsafe.Pointer
	data    unsafe.Pointer
}

// Assert that RawBytes implements Array[[]byte].
var _ Array[[]byte] = RawBytes{}

// DecodeRawBytes decodes the structure of a RawBytes, constructing an accessor
// for an array of byte slices constructed by RawBytesBuilder. Count must be the
// number of byte slices within the array.
func DecodeRawBytes(b []byte, offset uint32, count int) (rawBytes RawBytes, endOffset uint32) {
	if count == 0 {
		return RawBytes{}, 0
	}
	offsets, dataOff := DecodeUnsafeIntegerSlice[uint32](b, offset, count+1 /* +1 offset */)
	return RawBytes{
		slices:  count,
		offsets: offsets,
		start:   unsafe.Pointer(&b[offset]),
		data:    unsafe.Pointer(&b[dataOff]),
	}, dataOff + offsets.At(count)
}

// Assert that DecodeRawBytes implements DecodeFunc.
var _ DecodeFunc[RawBytes] = DecodeRawBytes

func defaultSliceFormatter(x []byte) string {
	return string(x)
}

func rawBytesToBinFormatter(f *binfmt.Formatter, count int, sliceFormatter func([]byte) string) {
	if sliceFormatter == nil {
		sliceFormatter = defaultSliceFormatter
	}

	rb, _ := DecodeRawBytes(f.Data(), uint32(f.Offset()), count)
	dataOffset := uint64(f.Offset()) + uint64(uintptr(rb.data)-uintptr(rb.start))
	f.CommentLine("rawbytes")
	f.CommentLine("offsets table")
	uintsToBinFormatter(f, count+1, DataTypeUint32, func(offset, base uint64) string {
		// NB: base is always zero for RawBytes columns.
		return fmt.Sprintf("%d [%d overall]", offset+base, offset+base+dataOffset)
	})
	f.CommentLine("data")
	for i := 0; i < rb.slices; i++ {
		s := rb.At(i)
		f.HexBytesln(len(s), "data[%d]: %s", i, sliceFormatter(s))
	}
}

func (b RawBytes) ptr(offset uint32) unsafe.Pointer {
	return unsafe.Pointer(uintptr(b.data) + uintptr(offset))
}

func (b RawBytes) slice(start, end uint32) []byte {
	return unsafe.Slice((*byte)(b.ptr(start)), end-start)
}

// At returns the []byte at index i. The returned slice should not be mutated.
func (b RawBytes) At(i int) []byte {
	return b.slice(b.offsets.At(i), b.offsets.At(i+1))
}

// Slices returns the number of []byte slices encoded within the RawBytes.
func (b RawBytes) Slices() int {
	return b.slices
}

// RawBytesBuilder encodes a column of byte slices.
type RawBytesBuilder struct {
	rows    int
	data    []byte
	offsets UintBuilder[uint32]
}

// Assert that *RawBytesBuilder implements ColumnWriter.
var _ ColumnWriter = (*RawBytesBuilder)(nil)

// Reset resets the builder to an empty state, preserving the existing bundle
// size.
func (b *RawBytesBuilder) Reset() {
	b.rows = 0
	b.data = b.data[:0]
	b.offsets.Reset()
	// Add an initial offset of zero to streamline the logic in RawBytes.At() to
	// avoid needing a special case for row 0.
	b.offsets.Set(0, 0)
}

// NumColumns implements ColumnWriter.
func (b *RawBytesBuilder) NumColumns() int { return 1 }

// DataType implements ColumnWriter.
func (b *RawBytesBuilder) DataType(int) DataType { return DataTypeBytes }

// Put appends the provided byte slice to the builder.
func (b *RawBytesBuilder) Put(s []byte) {
	b.data = append(b.data, s...)
	b.rows++
	b.offsets.Set(b.rows, uint32(len(b.data)))
}

// PutConcat appends a single byte slice formed by the concatenation of the two
// byte slice arguments.
func (b *RawBytesBuilder) PutConcat(s1, s2 []byte) {
	b.data = append(append(b.data, s1...), s2...)
	b.rows++
	b.offsets.Set(b.rows, uint32(len(b.data)))
}

// Finish writes the serialized byte slices to buf starting at offset. The buf
// slice must be sufficiently large to store the serialized output. The caller
// should use [Size] to size buf appropriately before calling Finish.
func (b *RawBytesBuilder) Finish(col, rows int, offset uint32, buf []byte) uint32 {
	if rows == 0 {
		return 0
	}
	dataLen := b.offsets.Get(rows)
	offset = b.offsets.Finish(0, rows+1, offset, buf)
	// Copy the data section.
	return offset + uint32(copy(buf[offset:], b.data[:dataLen]))
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
	// Get the size needed to encode the rows+1 offsets.
	offset = b.offsets.Size(rows+1, offset)
	// Add the value of offset[rows] since that is the accumulated size of the
	// first [rows] slices.
	return offset + b.offsets.Get(rows)
}

// WriteDebug implements Encoder.
func (b *RawBytesBuilder) WriteDebug(w io.Writer, rows int) {
	fmt.Fprintf(w, "bytes: %d rows set; %d bytes in data", b.rows, len(b.data))
}
