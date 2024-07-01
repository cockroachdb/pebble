// Copyright 2024 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package colblk

import (
	"bytes"
	"fmt"
	"io"
	"strings"
)

// DataType describes the logical type of a column's values. Some data types
// have multiple possible physical representations. Encoding a column may choose
// between possible physical representations depending on the distribution of
// values and the size of the resulting physical representation.
type DataType uint8

const (
	// DataTypeInvalid represents an unset or invalid data type.
	DataTypeInvalid DataType = 0
	// DataTypeBool is a data type encoding a bool per row.
	DataTypeBool DataType = 1
	// DataTypeUint8 is a data type encoding a fixed 8 bits per row.
	DataTypeUint8 DataType = 2
	// DataTypeUint16 is a data type encoding a fixed 16 bits per row.
	DataTypeUint16 DataType = 3
	// DataTypeUint32 is a data type encoding a fixed 32 bits per row.
	DataTypeUint32 DataType = 4
	// DataTypeUint64 is a data type encoding a fixed 64 bits per row.
	DataTypeUint64 DataType = 5
	// DataTypeBytes is a data type encoding a variable-length byte string per
	// row.
	DataTypeBytes DataType = 6
	// DataTypePrefixBytes is a data type encoding variable-length,
	// lexicographically-sorted byte strings, with prefix compression.
	DataTypePrefixBytes DataType = 7

	dataTypesCount DataType = 8
)

var dataTypeName [dataTypesCount]string = [dataTypesCount]string{
	DataTypeInvalid:     "invalid",
	DataTypeBool:        "bool",
	DataTypeUint8:       "uint8",
	DataTypeUint16:      "uint16",
	DataTypeUint32:      "uint32",
	DataTypeUint64:      "uint64",
	DataTypeBytes:       "bytes",
	DataTypePrefixBytes: "prefixbytes",
}

// String returns a human-readable string representation of the data type.
func (t DataType) String() string {
	return dataTypeName[t]
}

func (t DataType) uintWidth() uint32 {
	if t >= DataTypeUint8 && t <= DataTypeUint64 {
		return 1 << (t - DataTypeUint8)
	}
	panic("not a unit")
}

// ColumnDesc describes the column's data type and its encoding.
type ColumnDesc struct {
	DataType DataType
	Encoding ColumnEncoding
}

// TODO(jackson): Ensure we provide a mechanism for future extensibility: maybe
// a single byte to represent the colblk version?

// String returns a human-readable string describing the column encoding.
func (d ColumnDesc) String() string {
	var sb strings.Builder
	fmt.Fprint(&sb, d.DataType.String())
	if d.Encoding != EncodingDefault {
		fmt.Fprintf(&sb, "+%s", d.Encoding)
	}
	return sb.String()
}

// ColumnDescs is a slice of ColumnDesc values.
type ColumnDescs []ColumnDesc

// String returns the concatenated string representations of the column
// descriptions.
func (c ColumnDescs) String() string {
	var buf bytes.Buffer
	for i := range c {
		if i > 0 {
			buf.WriteString(" ")
		}
		buf.WriteString(c[i].String())
	}
	return buf.String()
}

// ColumnEncoding describes the encoding of a column.
//
// Column data can optionally contain a NULL bitmap, where each bit corresponds
// to a row in the column and a set bit indicates that the corresponding row is
// NULL. Column data can optionally use a delta encoding where each value is a
// delta from a base, constant value.
//
// The layout of the encoded data is as follows:
//   - [optional] NULL bitmap
//   - [optional] a "base" constant value for delta encodings
//   - n data values
//
// The ColumnEncoding byte is a combination of the following values:
//   - The bits 0,1,2 encode the DeltaEncoding enum variant.
//   - The bit 3 is set if the column has a NULL bitmap.
type ColumnEncoding uint8

const (
	// EncodingDefault indicates that the default encoding is in-use for a
	// column, encoding n values for n rows.
	EncodingDefault ColumnEncoding = 0

	encodingUintDeltaMask        = 0b00000111
	encodingUintDeltaInverseMask = 0b11111000
	encodingNullBitmapBit        = 0b00001000

	// TODO(jackson): Add additional encoding types.
)

// Delta returns the delta encoding of the column.
func (e ColumnEncoding) Delta() DeltaEncoding {
	return DeltaEncoding(e & encodingUintDeltaMask)
}

// WithDelta returns the column encoding with the provided delta encoding.
func (e ColumnEncoding) WithDelta(d DeltaEncoding) ColumnEncoding {
	return (e & encodingUintDeltaInverseMask) | ColumnEncoding(d)
}

// String returns the string representation of the column encoding.
func (e ColumnEncoding) String() string {
	var sb strings.Builder
	fmt.Fprint(&sb, e.Delta().String())
	return sb.String()
}

// DeltaEncoding indicates what delta encoding, if any is in use by a column to
// reduce the per-row storage size.
//
// A uint delta encoding represents every non-NULL element in an array of uints
// as a delta relative to the column's constant. The logical value of each row
// is computed as C + D[i] where C is the column constant and D[i] is the delta.
type DeltaEncoding uint8

const (
	// DeltaEncodingNone indicates no delta encoding is in use. N rows are
	// represented using N values of the column's logical data type.
	DeltaEncodingNone DeltaEncoding = 0
	// DeltaEncodingConstant indicates that all rows of the column share the
	// same value. The column data encodes the constant value and no deltas.
	DeltaEncodingConstant DeltaEncoding = 1
	// DeltaEncodingUint8 indicates each delta is represented as a 1-byte uint8.
	DeltaEncodingUint8 DeltaEncoding = 2
	// DeltaEncodingUint16 indicates each delta is represented as a 2-byte uint16.
	DeltaEncodingUint16 DeltaEncoding = 3
	// DeltaEncodingUint32 indicates each delta is represented as a 4-byte uint32.
	DeltaEncodingUint32 DeltaEncoding = 4
)

// String implements fmt.Stringer.
func (d DeltaEncoding) String() string {
	switch d {
	case DeltaEncodingNone:
		return ""
	case DeltaEncodingConstant:
		return "const"
	case DeltaEncodingUint8:
		return "delta8"
	case DeltaEncodingUint16:
		return "delta16"
	case DeltaEncodingUint32:
		return "delta32"
	default:
		panic("unreachable")
	}
}

// ColumnWriter is an interface implemented by column encoders that accumulate a
// column's values and then serialize them.
type ColumnWriter interface {
	Encoder
	// NumColumns returns the number of columns the ColumnWriter will encode.
	NumColumns() int
	// Finish serializes the column at the specified index, writing the column's
	// data to buf at offset, and returning the offset at which the next column
	// should be encoded. Finish also returns a column descriptor describing the
	// encoding of the column, which will be serialized within the block header.
	//
	// The supplied buf must have enough space at the provided offset to fit the
	// column. The caller may use Size() to calculate the exact size required.
	// If [rows] is ≤ the highest row index at which a value has been set,
	// Finish will only serialize the first [rows] values.
	//
	// The provided column index must be less than NumColumns(). Finish is
	// called for each index < NumColumns() in order.
	Finish(col int, rows int, offset uint32, buf []byte) (nextOffset uint32, desc ColumnDesc)
}

// Encoder is an interface implemented by column encoders.
type Encoder interface {
	// Reset clears the ColumnWriter's internal state, preparing it for reuse.
	Reset()
	// Size returns the size required to encode the column's current values.
	//
	// The `rows` argument must be the current number of logical rows in the
	// column.  Some implementations support defaults, and these implementations
	// rely on the caller to inform them the current number of logical rows. The
	// provided `rows` must be greater than or equal to the largest row set + 1.
	// In other words, Size does not support determining the size of a column's
	// earlier size before additional rows were added.
	Size(rows int, offset uint32) uint32
	// WriteDebug writes a human-readable description of the current column
	// state to the provided writer.
	WriteDebug(w io.Writer, rows int)
}
