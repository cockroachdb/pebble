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
// if all offsets fit within an 8-bit or 16-bit uint.
//
// The use of delta encoding conserves space in the common case. In the context
// of CockroachDB, the vast majority of offsets will fit in 16-bits when using
// 32 KiB blocks (the size in use by CockroachDB). However a single value larger
// than 65535 bytes requires an offset too large to fit within 16 bits, in which
// case offsets will be encoded as 32-bit integers.
//
//	+-------------------------------------------------------------------+
//	|                       32-bit aligning padding                     |
//	+-------------------------------------------------------------------+
//	|        a uint32 offsets table, possibly delta encoded             |
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

// MakeRawBytes constructs an accessor for an array of byte slices constructed
// by RawBytesBuilder. Count must be the number of byte slices within the array.
func MakeRawBytes(count int, b []byte, offset uint32, enc ColumnEncoding) RawBytes {
	if count == 0 {
		return RawBytes{}
	}
	dataOff, offsets := readUnsafeIntegerSlice[uint32](count+1 /* +1 offset */, b, offset, enc.Delta())
	return RawBytes{
		slices:  count,
		offsets: offsets,
		start:   unsafe.Pointer(&b[offset]),
		data:    unsafe.Pointer(&b[dataOff]),
	}
}

func defaultSliceFormatter(x []byte) string {
	return string(x)
}

func rawBytesToBinFormatter(
	f *binfmt.Formatter, count int, enc ColumnEncoding, sliceFormatter func([]byte) string,
) int {
	if sliceFormatter == nil {
		sliceFormatter = defaultSliceFormatter
	}
	rb := MakeRawBytes(count, f.Data(), uint32(f.Offset()), enc)

	var n int
	f.CommentLine("RawBytes")
	f.CommentLine("Offsets table")
	uintsToBinFormatter(f, count+1, ColumnDesc{DataType: DataTypeUint32, Encoding: enc})
	f.CommentLine("Data")
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

// At returns the []byte at index i. The returned slice should not be mutated.
func (b RawBytes) At(i int) []byte {
	return b.slice(int(b.offsets.At(i)), int(b.offsets.At(i+1)))
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
func (b *RawBytesBuilder) Finish(
	col int, rows int, offset uint32, buf []byte,
) (uint32, ColumnDesc) {
	desc := ColumnDesc{DataType: DataTypeBytes, Encoding: EncodingDefault}
	if rows == 0 {
		return 0, desc
	}
	dataLen := b.offsets.Get(rows)
	dataOffset, offsetColumnDesc := b.offsets.Finish(0, rows+1, offset, buf)
	desc.Encoding = offsetColumnDesc.Encoding
	// Copy the data section.
	endOffset := dataOffset + uint32(copy(buf[dataOffset:], b.data[:dataLen]))
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
	offset = b.offsets.Size(rows+1, offset)
	return offset + b.offsets.Get(rows)
}

// WriteDebug implements Encoder.
func (b *RawBytesBuilder) WriteDebug(w io.Writer, rows int) {
	fmt.Fprintf(w, "bytes: %d rows set; %d bytes in data", b.rows, len(b.data))
}
