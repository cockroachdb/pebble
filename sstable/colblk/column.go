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

	// NB: We cannot add a new DataType without adjusting the column desc.
	dataTypesCount DataType = 8

	// Assert that the number of data types fits within the alloted bits in the
	// column desc.
	_ uint = uint(dataTypesCount) - 1 - columnDescDataTypeMask
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

const (
	columnDescDataTypeMask      = (1 << 3) - 1
	columnDescNullBitmapFlagBit = (1 << 4)
	columnDescEncodingShift     = 5
	columnDescEncodingMask      = 0b00011111
)

// ColumnDesc describes the column's data type and its encoding.
//
//	high                                       low
//	  X     X     X     X     X     X     X     X
//	 \_____________/    |     |    \_____________/
//	    encoding        |   unused    data type
//	                    |
//	               null bitmap?
//
// Data type (bits 0, 1, 2):
//
// The data type is stored in the low 3 bits. It indicates the logical data type
// of the values stored within the column.
//
// Unused (bit 3):
//
// The fourth low bit is unused and reserved for future use. It may be used to
// expand the set of data types in future extensions.
//
// Null bitmap (bit 4):
//
// The bit at position 4 indicates whether the column is prefixed with a null
// bitmap. A null bitmap is a bitmap where each bit corresponds to a row in the
// column, and a set bit indicates that the corresponding row is NULL.
//
// Column encoding (bits 5, 6, 7):
//
// The bits at positions 5, 6, and 7 encode an enum describing the encoding of
// the column. See the ColumnEncoding type and its constants for details.
type ColumnDesc uint8

// DataType returns the logical data type encoded.
func (d ColumnDesc) DataType() DataType {
	// The data type is stored within the first 3 bits.
	return DataType(d & columnDescDataTypeMask)
}

// Encoding returns the column's encoding.
func (d ColumnDesc) Encoding() ColumnEncoding {
	return ColumnEncoding(d >> columnDescEncodingShift)
}

// String returns a human-readable string describing the column encoding.
func (d ColumnDesc) String() string {
	var sb strings.Builder
	dt := d.DataType()
	fmt.Fprint(&sb, dt.String())
	if enc := d.Encoding(); enc != EncodingDefault {
		fmt.Fprintf(&sb, "+%s", enc)
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

// ColumnEncoding is an enum describing the encoding of a column.
type ColumnEncoding uint8

const (
	// EncodingDefault indicates that the default encoding is in-use for a
	// column, encoding n values for n rows.
	EncodingDefault ColumnEncoding = 0
	// TODO(jackson): Add additional encoding types.
	encodingTypeCount = 7
)

// String returns the string representation of the column encoding.
func (e ColumnEncoding) String() string {
	return encodingName[e]
}

var encodingName [encodingTypeCount]string = [encodingTypeCount]string{
	EncodingDefault: "default",
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
