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
// with a single padding byte.
//
// The block header begins with:
// - Version number (1 byte)
// - The number of columns in the block (2 bytes)
// - The number of rows in the block (4 bytes)
//
// Then follows a column-header for each column. Each column header encodes the
// data type (1 byte) and the offset into the block where the column data begins
// (4 bytes).
//
//	+-----------+
//	| Vers (1B) |
//	+-------------------+--------------------------------+
//	| # columns (2B)    | # of rows (4B)                 |
//	+-----------+-------+---------------------+----------+
//	| Type (1B) | Page offset (4B)                | Col 0
//	+-----------+---------------------------------+
//	| Type (1B) | Page offset (4B)                | Col 1
//	+-----------+---------------------------------+
//	| ...	    | ...                             |
//	+-----------+---------------------------------+
//	| Type (1B) | Page offset (4B)                | Col n-1
//	+-----------+----------------------------------
//	|  column 0 data                                ...
//	+----------------------------------------------
//	|  column 1 data                                ...
//	+----------------------------------------------
//	| ...
//	+----------------------------------------------
//	|  column n-1 data                              ...
//	+-------------+--------------------------------
//	| Unused (1B) |
//	+-------------+
//
// The encoding of column data itself is dependent on the data type.
//
// The trailing padding byte exists to allow columns to represent the end of
// column data using a pointer to the byte after the end of the column. The
// padding byte ensures that the pointer will always fall within the allocated
// memory. Without the padding byte, such a pointer would be invalid according
// to Go's pointer rules.
//
// # Alignment
//
// Block buffers are required to be word-aligned, during encoding and decoding.
// This ensures that if any individual column or piece of data requires
// word-alignment, the writer can align the offset into the block buffer
// appropriately to ensure that the data is word-aligned.
//
// # Keyspan blocks
//
// Blocks encoding key spans (range deletions, range keys) decompose the fields
// of keyspan.Key into columns. Key spans are always stored fragmented, such
// that all overlapping keys have identical bounds. When encoding key spans to a
// columnar block, we take advantage of this fragmentation to store the set of
// unique user key span boundaries in a separate column. This column does not
// have the same number of rows as the other columns. Each individual fragment
// stores the index of its start boundary user key within the user key column.
//
// For example, consider the three unfragmented spans:
//
//	a-e:{(#0,RANGEDEL)}
//	b-d:{(#100,RANGEDEL)}
//	f-g:{(#20,RANGEDEL)}
//
// Once fragmented, these spans become five keyspan.Keys:
//
//	a-b:{(#0,RANGEDEL)}
//	b-d:{(#100,RANGEDEL), (#0,RANGEDEL)}
//	d-e:{(#0,RANGEDEL)}
//	f-g:{(#20,RANGEDEL)}
//
// When encoded within a columnar keyspan block, the boundary columns (user key
// and start indices) would hold six rows:
//
//	+-----------------+-----------------+
//	| User key        | Start index     |
//	+-----------------+-----------------+
//	| a               | 0               |
//	+-----------------+-----------------+
//	| b               | 1               |
//	+-----------------+-----------------+
//	| d               | 3               |
//	+-----------------+-----------------+
//	| e               | 4               |
//	+-----------------+-----------------+
//	| f               | 4               |
//	+-----------------+-----------------+
//	| g               | 5               |
//	+-----------------+-----------------+
//
// The remaining keyspan.Key columns would look like:
//
//	+-----------------+-----------------+-----------------+
//	| Trailer         | Suffix          | Value           |
//	+-----------------+-----------------+-----------------+
//	| (#0,RANGEDEL)   | -               | -               | (0)
//	+-----------------+-----------------+-----------------+
//	| (#100,RANGEDEL) | -               | -               | (1)
//	+-----------------+-----------------+-----------------+
//	| (#0,RANGEDEL)   | -               | -               | (2)
//	+-----------------+-----------------+-----------------+
//	| (#0,RANGEDEL)   | -               | -               | (3)
//	+-----------------+-----------------+-----------------+
//	| (#20,RANGEDEL)  | -               | -               | (4)
//	+-----------------+-----------------+-----------------+
//
// This encoding does not explicitly encode the mapping of keyspan.Key to
// boundary keys.  Rather each boundary key encodes the index where keys
// beginning at a boundary >= the key begin. Readers look up the key start index
// for the start boundary (s) and the end boundary (e). Any keys within indexes
// [s,e) have the corresponding bounds.
//
// Both range deletions and range keys are encoded with the same schema. Range
// deletion keyspan.Keys never contain suffixes or values. When one of these
// columns is encoded, the RawBytes encoding uses uintEncodingAllZero to avoid
// encoding N offsets. Each of these empty columns is encoded in just 1 byte of
// column data.
package colblk

import (
	"cmp"
	"encoding/binary"
	"fmt"
	"unsafe"

	"github.com/cockroachdb/crlib/crbytes"
	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/pebble/internal/binfmt"
	"github.com/cockroachdb/pebble/internal/invariants"
	"github.com/cockroachdb/pebble/internal/treeprinter"
)

// Version indicates the version of the columnar block format encoded. The
// version byte is always the first byte within the block. This ensures that
// readers can switch on the version before reading the rest of the block.
type Version uint8

const (
	// Version1 is the first version of the columnar block format.
	Version1 Version = 0x01
)

const blockHeaderBaseSize = 7
const columnHeaderSize = 5
const maxBlockRetainedSize = 256 << 10

// Header holds the metadata extracted from a columnar block's header.
type Header struct {
	Version Version
	// Columns holds the number of columns encoded within the block.
	Columns uint16
	// Rows holds the number of rows encoded within the block.
	Rows uint32
}

// String implements the fmt.Stringer interface, returning a human-readable
// representation of the block header.
func (h Header) String() string {
	return fmt.Sprintf("Version=%v; Columns=%d; Rows=%d", h.Version, h.Columns, h.Rows)
}

// Encode encodes the header to the provided buf. The length of buf must be at
// least 7 bytes.
func (h Header) Encode(buf []byte) {
	buf[0] = byte(h.Version)
	binary.LittleEndian.PutUint16(buf[1:], h.Columns)
	binary.LittleEndian.PutUint32(buf[1+align16:], h.Rows)
}

// HeaderSize returns the size of the block header, including column
// headers, for a block with the specified number of columns and optionally a
// custom header size.
func HeaderSize(cols int, customHeaderSize uint32) uint32 {
	// Each column has a 1-byte DataType and a 4-byte offset into the block.
	return uint32(blockHeaderBaseSize+cols*columnHeaderSize) + customHeaderSize
}

// DecodeHeader reads the block header from the provided serialized columnar
// block.
func DecodeHeader(data []byte) Header {
	return Header{
		Version: Version(data[0]),
		Columns: uint16(binary.LittleEndian.Uint16(data[1:])),
		Rows:    uint32(binary.LittleEndian.Uint32(data[1+align16:])),
	}
}

// A BlockEncoder encodes a columnar block and handles encoding the block's
// header, including individual column headers.
type BlockEncoder struct {
	buf          []byte
	headerOffset uint32
	pageOffset   uint32
}

// Reset resets an encoder for reuse.
func (e *BlockEncoder) Reset() {
	if invariants.Enabled && invariants.Sometimes(10) {
		invariants.Mangle(e.buf)
	}
	if cap(e.buf) > maxBlockRetainedSize {
		e.buf = nil
	}
	e.headerOffset = 0
	e.pageOffset = 0
}

// Init initializes the block encoder with a buffer of the specified size and
// header.
func (e *BlockEncoder) Init(size int, h Header, customHeaderSize uint32) {
	if cap(e.buf) < size {
		e.buf = crbytes.AllocAligned(size)
	} else {
		e.buf = e.buf[:size]
	}
	e.headerOffset = uint32(customHeaderSize) + blockHeaderBaseSize
	e.pageOffset = HeaderSize(int(h.Columns), customHeaderSize)
	h.Encode(e.buf[customHeaderSize:])
}

// Data returns the underlying buffer.
func (e *BlockEncoder) Data() []byte {
	return e.buf
}

// Encode encodes w's columns to the block.
func (e *BlockEncoder) Encode(rows int, w ColumnWriter) {
	for i := 0; i < w.NumColumns(); i++ {
		e.buf[e.headerOffset] = byte(w.DataType(i))
		binary.LittleEndian.PutUint32(e.buf[e.headerOffset+1:], e.pageOffset)
		e.headerOffset += columnHeaderSize
		e.pageOffset = w.Finish(i, rows, e.pageOffset, e.buf)
	}
}

// Finish finalizes the block encoding, returning the encoded block. The
// returned byte slice points to the encoder's buffer, so if the encoder is
// reused the returned slice will be invalidated.
func (e *BlockEncoder) Finish() []byte {
	e.buf[e.pageOffset] = 0x00 // Padding byte
	e.pageOffset++
	if e.pageOffset != uint32(len(e.buf)) {
		panic(errors.AssertionFailedf("expected pageOffset=%d to equal size=%d", e.pageOffset, len(e.buf)))
	}
	return e.buf
}

// FinishBlock writes the columnar block to a heap-allocated byte slice.
// FinishBlock assumes all columns have the same number of rows. If that's not
// the case, the caller should manually construct their own block.
func FinishBlock(rows int, writers []ColumnWriter) []byte {
	size := HeaderSize(len(writers), 0)
	nCols := uint16(0)
	for _, cw := range writers {
		size = cw.Size(rows, size)
		nCols += uint16(cw.NumColumns())
	}
	size++ // +1 for the trailing version byte.

	var enc BlockEncoder
	enc.Init(int(size), Header{
		Version: Version1,
		Columns: nCols,
		Rows:    uint32(rows),
	}, 0)
	for _, cw := range writers {
		enc.Encode(rows, cw)
	}
	return enc.Finish()
}

// DecodeColumn decodes the col'th column of the provided reader's block as a
// column of dataType using decodeFunc.
func DecodeColumn[V any](
	d *BlockDecoder, col int, rows int, dataType DataType, decodeFunc DecodeFunc[V],
) V {
	if uint16(col) >= d.header.Columns {
		panic(errors.AssertionFailedf("column %d is out of range [0, %d)", col, d.header.Columns))
	}
	if dt := d.dataType(col); dt != dataType {
		panic(errors.AssertionFailedf("column %d is type %s; not %s", col, dt, dataType))
	}
	v, endOffset := decodeFunc(d.data, d.pageStart(col), rows)
	if nextColumnOff := d.pageStart(col + 1); endOffset != nextColumnOff {
		panic(errors.AssertionFailedf("column %d decoded to offset %d; expected %d", col, endOffset, nextColumnOff))
	}
	return v
}

// A BlockDecoder holds metadata for accessing the columns of a columnar block.
type BlockDecoder struct {
	data             []byte
	header           Header
	customHeaderSize uint32
}

// DecodeBlock decodes the header of the provided columnar block and returns a
// new BlockDecoder configured to read from the block. The caller must ensure
// that the data is formatted as to the block layout specification.
func DecodeBlock(data []byte, customHeaderSize uint32) BlockDecoder {
	d := BlockDecoder{}
	d.Init(data, customHeaderSize)
	return d
}

// Init initializes a BlockDecoder with the data contained in the provided block.
func (d *BlockDecoder) Init(data []byte, customHeaderSize uint32) {
	*d = BlockDecoder{
		data:             data,
		header:           DecodeHeader(data[customHeaderSize:]),
		customHeaderSize: customHeaderSize,
	}
}

// Rows returns the number of rows in the block, as indicated by the block header.
func (d *BlockDecoder) Rows() int {
	return int(d.header.Rows)
}

// DataType returns the data type of the col'th column. Every column's data type
// is encoded within the block header.
func (d *BlockDecoder) DataType(col int) DataType {
	if uint16(col) >= d.header.Columns {
		panic(errors.AssertionFailedf("column %d is out of range [0, %d)", col, d.header.Columns))
	}
	return d.dataType(col)
}

func (d *BlockDecoder) dataType(col int) DataType {
	return DataType(*(*uint8)(d.pointer(d.customHeaderSize + blockHeaderBaseSize + columnHeaderSize*uint32(col))))
}

// Bitmap retrieves the col'th column as a bitmap. The column must be of type
// DataTypeBool.
func (d *BlockDecoder) Bitmap(col int) Bitmap {
	return DecodeColumn(d, col, int(d.header.Rows), DataTypeBool, DecodeBitmap)
}

// RawBytes retrieves the col'th column as a column of byte slices. The column
// must be of type DataTypeBytes.
func (d *BlockDecoder) RawBytes(col int) RawBytes {
	return DecodeColumn(d, col, int(d.header.Rows), DataTypeBytes, DecodeRawBytes)
}

// PrefixBytes retrieves the col'th column as a prefix-compressed byte slice column. The column
// must be of type DataTypePrefixBytes.
func (d *BlockDecoder) PrefixBytes(col int) PrefixBytes {
	return DecodeColumn(d, col, int(d.header.Rows), DataTypePrefixBytes, DecodePrefixBytes)
}

// Uints retrieves the col'th column as a column of uints. The column must be
// of type DataTypeUint.
func (d *BlockDecoder) Uints(col int) UnsafeUints {
	return DecodeColumn(d, col, int(d.header.Rows), DataTypeUint, DecodeUnsafeUints)
}

// Data returns the underlying buffer.
func (d *BlockDecoder) Data() []byte {
	return d.data
}

func (d *BlockDecoder) pageStart(col int) uint32 {
	if uint16(col) >= d.header.Columns {
		// -1 for the trailing version byte
		return uint32(len(d.data) - 1)
	}
	return binary.LittleEndian.Uint32(
		unsafe.Slice((*byte)(d.pointer(d.customHeaderSize+uint32(blockHeaderBaseSize+columnHeaderSize*col+1))), 4))
}

func (d *BlockDecoder) pointer(offset uint32) unsafe.Pointer {
	return unsafe.Pointer(uintptr(unsafe.Pointer(&d.data[0])) + uintptr(offset))
}

// FormattedString returns a formatted representation of the block's binary
// data.
func (d *BlockDecoder) FormattedString() string {
	f := binfmt.New(d.data)
	tp := treeprinter.New()
	n := tp.Child("block")
	d.HeaderToBinFormatter(f, n)
	for i := 0; i < int(d.header.Columns); i++ {
		d.ColumnToBinFormatter(f, n, i, int(d.header.Rows))
	}
	f.HexBytesln(1, "block trailer padding")
	f.ToTreePrinter(n)
	return tp.String()
}

// HeaderToBinFormatter formats the block header to f and tp.
func (d *BlockDecoder) HeaderToBinFormatter(f *binfmt.Formatter, tp treeprinter.Node) {
	f.HexBytesln(1, "version %v", Version(f.PeekUint(1)))
	f.HexBytesln(2, "%d columns", d.header.Columns)
	f.HexBytesln(4, "%d rows", d.header.Rows)
	for i := 0; i < int(d.header.Columns); i++ {
		f.Byte("col %d: %s", i, d.DataType(i))
		f.HexBytesln(4, "col %d: page start %d", i, d.pageStart(i))
	}
	f.ToTreePrinter(tp.Child("columnar block header"))
}

func (d *BlockDecoder) formatColumn(
	f *binfmt.Formatter,
	tp treeprinter.Node,
	col int,
	fn func(*binfmt.Formatter, treeprinter.Node, DataType),
) {
	dataType := d.DataType(col)
	colSize := d.pageStart(col+1) - d.pageStart(col)
	endOff := f.Offset() + int(colSize)
	fn(f, tp, dataType)

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

// ColumnToBinFormatter formats the col'th column to f and tp.
func (d *BlockDecoder) ColumnToBinFormatter(
	f *binfmt.Formatter, tp treeprinter.Node, col, rows int,
) {
	d.formatColumn(f, tp, col, func(f *binfmt.Formatter, tp treeprinter.Node, dataType DataType) {
		n := tp.Childf("data for column %d (%s)", col, dataType)
		switch dataType {
		case DataTypeBool:
			bitmapToBinFormatter(f, n, rows)
		case DataTypeUint:
			uintsToBinFormatter(f, n, rows, nil)
		case DataTypePrefixBytes:
			prefixBytesToBinFormatter(f, n, rows, nil)
		case DataTypeBytes:
			rawBytesToBinFormatter(f, n, rows, nil)
		default:
			panic("unimplemented")
		}
	})

}
