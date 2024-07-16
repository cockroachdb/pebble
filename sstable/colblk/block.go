// Copyright 2024 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

// Package colblk implements a columnar block format.
//
// The columnar block format organizes data into columns. The number of columns
// and their data types are dependent on the use and are configurable.
//
// # Format
//
// Every columnar block begins with a header describing the structure and schema
// of the block. Then columns values are encoded in sequence. The block ends
// with a single version byte.
//
// The block header begins with:
// - The number of columns in the block (2 bytes)
// - The number of rows in the block (4 bytes)
//
// Then follows a column-header for each column. Each column header encodes the
// data type (1 byte) and the offset into the block where the column data begins
// (4 bytes).
//
//	+-----------------------+--------------------------------+
//	| # columns (2B)        | # of rows (4B)                 |
//	+-----------+-----------+---------------------+----------+
//	| Type (1B) | Page offset (4B)                | Col 0
//	+-----------+---------------------------------+
//	| Type (1B) | Page offset (4B)                | Col 1
//	+-----------+---------------------------------+
//	| ...	    | ...                             |
//	+-----------+---------------------------------+
//	| Type (1B) | Page offset (4B)                | Col n-1
//	+-----------+---------------------------------+
//	|  column 0 data...                           |
//	+---------------------------------------------+
//	|  column 1 data...                           |
//	+---------------------------------------------+
//	| ...	                                      |
//	+-----------+---------------------------------+
//	|  column n-1 data...                         |
//	+---------------------------------------------+
//	| Version (1B)                                |
//	+-----------+---------------------------------+
//
// The encoding of column data itself is dependent on the data type.
//
// # Alignment
//
// Block buffers are required to be word-aligned, during encoding and decoding.
// This ensures that if any individual column or piece of data requires
// word-alignment, the writer can align the offset into the block buffer
// appropriately to ensure that the data is word-aligned.
package colblk

import (
	"cmp"
	"encoding/binary"
	"fmt"
	"unsafe"

	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/pebble/internal/aligned"
	"github.com/cockroachdb/pebble/internal/binfmt"
	"github.com/cockroachdb/pebble/internal/invariants"
)

// Version indicates the version of the columnar block format encoded. The
// version byte is always the final byte within the block. This ensures that
// readers can switch on the version before reading the rest of the block, but
// it also serves a secondary purpose. Some columns may attempt to represent the
// end of the column as a pointer to the byte after the end of the column. If
// the column is the last column within the block, this pointer would point to a
// byte outside of the block's allocation which is a violation of Go pointer
// rules. The trailing version byte ensures that these pointers are always valid.
type Version uint8

const (
	// Version1 is the first version of the columnar block format.
	Version1 Version = 0x01
)

// Header holds the metadata extracted from a columnar block's header.
type Header struct {
	// Columns holds the number of columns encoded within the block.
	Columns uint16
	// Rows holds the number of rows encoded within the block.
	Rows uint32
}

// String implements the fmt.Stringer interface, returning a human-readable
// representation of the block header.
func (h Header) String() string {
	return fmt.Sprintf("Columns=%d; Rows=%d", h.Columns, h.Rows)
}

// Encode encodes the header to the provided buf. The length of buf must be at
// least 6 bytes.
func (h Header) Encode(buf []byte) {
	binary.LittleEndian.PutUint16(buf, h.Columns)
	binary.LittleEndian.PutUint32(buf[align16:], h.Rows)
}

// blockHeaderSize returns the size of the block header, including column
// headers, for a block with the specified number of columns and optionally a
// custom header size.
func blockHeaderSize(cols int, customHeaderSize int) uint32 {
	// Each column has a 1-byte DataType and a 4-byte offset into the block.
	return uint32(6 + cols*5 + customHeaderSize)
}

// DecodeHeader reads the block header from the provided serialized columnar block.
func DecodeHeader(data []byte) Header {
	return Header{
		Columns: uint16(binary.LittleEndian.Uint16(data)),
		Rows:    uint32(binary.LittleEndian.Uint32(data[align16:])),
	}
}

// FinishBlock writes the columnar block to a heap-allocated byte slice.
// FinishBlock assumes all columns have the same number of rows. If that's not
// the case, the caller should manually construct their own block.
func FinishBlock(rows int, writers []ColumnWriter) []byte {
	size := blockHeaderSize(len(writers), 0)
	nCols := 0
	for _, cw := range writers {
		size = cw.Size(rows, size)
		nCols += cw.NumColumns()
	}
	size++ // +1 for the trailing version byte.

	buf := aligned.ByteSlice(int(size))
	h := Header{Columns: uint16(nCols), Rows: uint32(rows)}
	h.Encode(buf)
	pageOffset := blockHeaderSize(len(writers), 0)
	col := 0
	for _, cw := range writers {
		for i := 0; i < cw.NumColumns(); i++ {
			hi := blockHeaderSize(col, 0)
			buf[hi] = byte(cw.DataType(i))
			binary.LittleEndian.PutUint32(buf[hi+1:], pageOffset)
			pageOffset = cw.Finish(i, rows, pageOffset, buf)
			col++
		}
	}
	buf[pageOffset] = byte(Version1)
	pageOffset++
	if invariants.Enabled && pageOffset != size {
		panic(fmt.Sprintf("expected pageOffset=%d to be equal to size=%d", pageOffset, size))
	}
	return buf
}

// A BlockReader holds metadata for accessing the columns of a columnar block.
type BlockReader struct {
	data             []byte
	header           Header
	customHeaderSize uint32
}

// ReadBlock decodes the header of the provided columnar block and returns a new
// BlockReader configured to read from the block. The caller must ensure that
// the data is formatted as to the block layout specification.
func ReadBlock(data []byte, customHeaderSize uint32) BlockReader {
	r := BlockReader{}
	r.Init(data, customHeaderSize)
	return r
}

// Init initializes a BlockReader with the data contained in the provided block.
func (r *BlockReader) Init(data []byte, customHeaderSize uint32) {
	*r = BlockReader{
		data:             data,
		header:           DecodeHeader(data[customHeaderSize:]),
		customHeaderSize: customHeaderSize,
	}
}

// DataType returns the data type of the col'th column. Every column's data type
// is encoded within the block header.
func (r BlockReader) DataType(col int) DataType {
	if uint16(col) >= r.header.Columns {
		panic("not reached")
	}
	return DataType(*(*uint8)(r.pointer(r.customHeaderSize + 6 + 5*uint32(col))))
}

// Bitmap retrieves the col'th column as a bitmap. The column must be of type
// DataTypeBool.
func (r BlockReader) Bitmap(col int) Bitmap {
	if dt := r.DataType(col); dt != DataTypeBool {
		panic(errors.AssertionFailedf("column %d is not a bitmap; holds data type %s", dt))
	}
	return MakeBitmap(r.data, r.pageStart(col), int(r.header.Rows))
}

// RawBytes retrieves the col'th column as a column of byte slices. The column
// must be of type DataTypeBytes.
func (r BlockReader) RawBytes(col int) RawBytes {
	if dt := r.DataType(col); dt != DataTypeBytes {
		panic(errors.AssertionFailedf("column %d is not a RawBytes column; holds data type %s", dt))
	}
	return MakeRawBytes(int(r.header.Rows), r.data, r.pageStart(col))
}

// Uint8s retrieves the col'th column as a column of uint8s. The column must be
// of type DataTypeUint8.
func (r BlockReader) Uint8s(col int) UnsafeUint8s {
	if dt := r.DataType(col); dt != DataTypeUint8 {
		panic(errors.AssertionFailedf("column %d is not a Uint8 column; holds data type %s", col, dt))
	}
	_, s := readUnsafeIntegerSlice[uint8](int(r.header.Rows), r.data, r.pageStart(col))
	return s
}

// Uint16s retrieves the col'th column as a column of uint8s. The column must be
// of type DataTypeUint16.
func (r BlockReader) Uint16s(col int) UnsafeUint16s {
	if dt := r.DataType(col); dt != DataTypeUint16 {
		panic(errors.AssertionFailedf("column %d is not a Uint16 column; holds data type %s", col, dt))
	}
	_, s := readUnsafeIntegerSlice[uint16](int(r.header.Rows), r.data, r.pageStart(col))
	return s
}

// Uint32s retrieves the col'th column as a column of uint32s. The column must be
// of type DataTypeUint32.
func (r BlockReader) Uint32s(col int) UnsafeUint32s {
	if dt := r.DataType(col); dt != DataTypeUint32 {
		panic(errors.AssertionFailedf("column %d is not a Uint32 column; holds data type %s", col, dt))
	}
	_, s := readUnsafeIntegerSlice[uint32](int(r.header.Rows), r.data, r.pageStart(col))
	return s
}

// Uint64s retrieves the col'th column as a column of uint64s. The column must be
// of type DataTypeUint64.
func (r BlockReader) Uint64s(col int) UnsafeUint64s {
	if dt := r.DataType(col); dt != DataTypeUint64 {
		panic(errors.AssertionFailedf("column %d is not a Uint64 column; holds data type %s", col, dt))
	}
	_, s := readUnsafeIntegerSlice[uint64](int(r.header.Rows), r.data, r.pageStart(col))
	return s
}

func (r BlockReader) pageStart(col int) uint32 {
	if uint16(col) >= r.header.Columns {
		// -1 for the trailing version byte
		return uint32(len(r.data) - 1)
	}
	return binary.LittleEndian.Uint32(
		unsafe.Slice((*byte)(unsafe.Pointer(r.pointer(r.customHeaderSize+uint32(6+5*col+1)))), 4))
}

func (r BlockReader) pointer(offset uint32) unsafe.Pointer {
	return unsafe.Pointer(uintptr(unsafe.Pointer(&r.data[0])) + uintptr(offset))
}

// FormattedString returns a formatted representation of the block's binary
// data.
func (r BlockReader) FormattedString() string {
	f := binfmt.New(r.data)
	r.headerToBinFormatter(f)
	for i := 0; i < int(r.header.Columns); i++ {
		r.columnToBinFormatter(f, i, int(r.header.Rows))
	}
	f.CommentLine("block trailer")
	f.HexBytesln(1, fmt.Sprintf("%T", Version(0)))
	return f.String()
}

func (r BlockReader) headerToBinFormatter(f *binfmt.Formatter) {
	f.CommentLine("columnar block header")
	f.HexBytesln(2, "%d columns", r.header.Columns)
	f.HexBytesln(4, "%d rows", r.header.Rows)
	for i := 0; i < int(r.header.Columns); i++ {
		f.CommentLine("column %d", i)
		f.Byte("%s", r.DataType(i))
		f.HexBytesln(4, "page start %d", r.pageStart(i))
	}
}

func (r BlockReader) columnToBinFormatter(f *binfmt.Formatter, col, rows int) {
	f.CommentLine("data for column %d", col)
	dataType := r.DataType(col)
	colSize := r.pageStart(col+1) - r.pageStart(col)
	endOff := f.Offset() + int(colSize)
	switch dataType {
	case DataTypeBool:
		bitmapToBinFormatter(f, rows)
	case DataTypeUint8, DataTypeUint16, DataTypeUint32, DataTypeUint64:
		uintsToBinFormatter(f, rows, dataType, nil)
	case DataTypeBytes:
		rawBytesToBinFormatter(f, rows, nil)
	default:
		panic("unimplemented")
	}

	// We expect formatting the column data to have consumed all the bytes
	// between the column's pageOffset and the next column's pageOffset.
	switch v := endOff - f.Offset(); cmp.Compare[int](v, 0) {
	case +1:
		panic(fmt.Sprintf("expected f.Offset() = %d, but found %d; did column %s format too few bytes?", endOff, f.Offset(), dataType))
	case 0:
	case -1:
		panic(fmt.Sprintf("expected f.Offset() = %d, but found %d; did column %s format too many bytes?", endOff, f.Offset(), dataType))
	}
}
