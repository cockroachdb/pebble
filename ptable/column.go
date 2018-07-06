package ptable

import (
	"bytes"
	"fmt"
	"math/bits"
	"unsafe"
)

// Bitmap is a simple bitmap structure implemented on top of a byte slice.
type Bitmap []byte

// Get returns true if the bit at position i is set and false otherwise.
func (b Bitmap) Get(i int) bool {
	return (b[i/8] & (1 << uint(i%8))) != 0
}

// set sets the bit at position i if v is true and clears the bit at position i
// otherwise.
func (b Bitmap) set(i int, v bool) Bitmap {
	j := i / 8
	for len(b) <= j {
		b = append(b, 0)
	}
	if v {
		b[j] |= 1 << uint(i%8)
	} else {
		b[j] &^= 1 << uint(i%8)
	}
	return b
}

// NullBitmap is a bitmap structure implemented on top of an array of 32-bit
// integers. In addition to bit testing, NullBitmap also provides a fast
// Rank(i) operation by interleaving a lookup table into the bitmap. The bitmap
// is stored in the low 16-bits of every 32-bit word, and the lookup table is
// stored in the high bits.
//
//    bits    sum    bits    sum     bits    sum     bits    sum
//   +-------+------+-------+-------+-------+-------+-------+-------+
//   | 0-15  | 0    | 16-31 | 0-15  | 32-47 | 0-31  | 48-64 | 0-63  |
//   +-------+------+-------+-------+-------+-------+-------+-------+
//
// For example, consider the following 64-bits of data:
//
//   1110011111011111 1101111011110011 1111111111111111 1111110000111111
//
// The logical bits are split at 16-bit boundaries
//
//          bits             sum
//   0-15:  1110011111011111 0
//   16-31: 1101111011110011 13
//   32-47: 1111111111111111 25
//   48-63: 1111110000011111 41
//
// The lookup table (the sum column) is interleaved with the bitmap in the high
// 16 bits. To answer a Rank query, we find the word containing the bit (i/16),
// count the number of bits that are set in the low 16 bits of the word before
// the bit we're interested in, and add the sum from the high 16 bits in the
// word. See Rank for the implementation.
//
// The number of bits used for each lookup table entry (16-bits) limits the
// size of a bitmap to 64K bits. The lookup table imposes an additional bit of
// overhead per bit in the bitmap.
//
// TODO(peter): I experimented with a few other approaches, such as maintaining
// the lookup table after the bitmap. The advantage of a separate lookup table
// is that the space overhead can be reduced. For example, we could chunk the
// bitmap into 64-bit words and use bits.OnesCount64 to do the per-word counts
// which reduce the space overhead of the lookup table to 0.25
// bits/bit. Unfortunately, that approach was twice as slow as the interleaved
// lookup table, presumably due to cache misses.
type NullBitmap struct {
	ptr unsafe.Pointer
}

func makeNullBitmap(v []uint32) NullBitmap {
	return NullBitmap{ptr: unsafe.Pointer(&v[0])}
}

// Empty returns true if the bitmap is empty and indicates that all of the
// column values are non-NULL. It is safe to call Get and Rank on an empty
// bitmap, but faster to specialize the code to not invoke them at all.
func (b NullBitmap) Empty() bool {
	return b.ptr == nil
}

// Null returns true if the bit at position i is set and false otherwise.
func (b NullBitmap) Null(i int) bool {
	if b.ptr == nil {
		return false
	}
	val := *(*uint32)(unsafe.Pointer(uintptr(b.ptr) + (uintptr(i)>>4)<<2))
	bit := uint32(1) << uint(i&0xf)
	return (val & bit) != 0
}

// Rank returns the index of the i'th non-NULL value in the value
// array. Returns -1 if the i'th value is NULL. If all values are non-NULL,
// Rank(i) == i. The pattern to iterate over the non-NULL values in a vector
// is:
//
//   vals := vec.Int64()
//   for i := 0; i < vec.N; i++ {
//     if j := vec.Rank(i); j >= 0 {
//       v := vals[j]
//       // process v
//     }
//   }
func (b NullBitmap) Rank(i int) int {
	if b.ptr == nil {
		return i
	}
	val := *(*uint32)(unsafe.Pointer(uintptr(b.ptr) + (uintptr(i)>>4)<<2))
	bit := uint32(1) << uint(i&0xf)
	if (val & bit) != 0 {
		return -1
	}
	return int(val>>16) + bits.OnesCount16(uint16(^val&(bit-1)))
}

// count returns the count of non-NULL values in the bitmap.
func (b NullBitmap) count(n int) int {
	if b.ptr == nil {
		return n
	}
	val := *(*uint32)(unsafe.Pointer(uintptr(b.ptr) + (uintptr(n-1)>>4)<<2))
	bit := uint32(1) << (uint((n-1)&0xf) + 1)
	return int(val>>16) + bits.OnesCount16(uint16(^val&(bit-1)))
}

type nullBitmapBuilder []uint32

// set sets the bit at position i if v is true and clears the bit at position i
// otherwise. Bits must be set in order and it is invalid to set a bit twice.
func (b nullBitmapBuilder) set(i int, v bool) nullBitmapBuilder {
	j := i / 16
	for len(b) <= j {
		var p uint32
		if len(b) > 0 {
			v := b[len(b)-1]
			p = ((v >> 16) + uint32(bits.OnesCount16(^uint16(v)))) << 16
		}
		b = append(b, p)
	}
	if v {
		b[j] |= uint32(1) << uint(i&0xf)
	}
	return b
}

func (b nullBitmapBuilder) verify() {
	if len(b) > 0 {
		if (b[0] >> 16) != 0 {
			panic(fmt.Sprintf("0: %08x\n", b[0]))
		}
		for i, sum := 1, uint32(0); i < len(b); i++ {
			sum += uint32(bits.OnesCount16(^uint16(b[i-1])))
			if (b[i] >> 16) != sum {
				panic(fmt.Sprintf("i: %08x vs %08x\n", b[i], (sum << 16)))
			}
		}
	}
}

// Bytes holds an array of byte slices stored as the concatenated data and
// offsets for the end of each slice in that data.
type Bytes struct {
	count   int
	data    unsafe.Pointer
	offsets unsafe.Pointer
}

// At returns the []byte at index i. The returned slice should not be mutated.
func (b Bytes) At(i int) []byte {
	offsets := (*[1 << 31]int32)(b.offsets)[:b.count:b.count]
	end := offsets[i]
	var start int32
	if i > 0 {
		start = offsets[i-1]
	}
	return (*[1 << 31]byte)(b.data)[start:end:end]
}

// ColumnType ...
type ColumnType uint8

// ColumnType definitions.
const (
	ColumnTypeInvalid ColumnType = 0
	ColumnTypeBool               = 1
	ColumnTypeInt8               = 2
	ColumnTypeInt16              = 3
	ColumnTypeInt32              = 4
	ColumnTypeInt64              = 5
	ColumnTypeFloat32            = 6
	ColumnTypeFloat64            = 7
	// TODO(peter): Should "bytes" be replaced with a bit indicating variable
	// width data that can be applied to any fixed-width data type? This would
	// allow modeling both []int8, []int64, and []float64.
	ColumnTypeBytes = 8
	// TODO(peter): decimal, uuid, ipaddr, timestamp, time, timetz, duration,
	// collated string, tuple.
)

var columnTypeAlignment = []int32{
	ColumnTypeInvalid: 0,
	ColumnTypeBool:    1,
	ColumnTypeInt8:    1,
	ColumnTypeInt16:   2,
	ColumnTypeInt32:   4,
	ColumnTypeInt64:   8,
	ColumnTypeFloat32: 4,
	ColumnTypeFloat64: 8,
	ColumnTypeBytes:   1,
}

var columnTypeName = []string{
	ColumnTypeInvalid: "invalid",
	ColumnTypeBool:    "bool",
	ColumnTypeInt8:    "int8",
	ColumnTypeInt16:   "int16",
	ColumnTypeInt32:   "int32",
	ColumnTypeInt64:   "int64",
	ColumnTypeFloat32: "float32",
	ColumnTypeFloat64: "float64",
	ColumnTypeBytes:   "bytes",
}

var columnTypeWidth = []int32{
	ColumnTypeInvalid: 0,
	ColumnTypeBool:    1,
	ColumnTypeInt8:    1,
	ColumnTypeInt16:   2,
	ColumnTypeInt32:   4,
	ColumnTypeInt64:   8,
	ColumnTypeFloat32: 4,
	ColumnTypeFloat64: 8,
	ColumnTypeBytes:   -1,
}

// Alignment ...
func (t ColumnType) Alignment() int32 {
	return columnTypeAlignment[t]
}

// String ...
func (t ColumnType) String() string {
	return columnTypeName[t]
}

// Width ...
func (t ColumnType) Width() int32 {
	return columnTypeWidth[t]
}

// ColumnTypes ...
type ColumnTypes []ColumnType

func (c ColumnTypes) String() string {
	var buf bytes.Buffer
	for i := range c {
		if i > 0 {
			buf.WriteString(",")
		}
		buf.WriteString(c[i].String())
	}
	return buf.String()
}

// ColumnDirection ...
type ColumnDirection int8

// ColumnDirection definitions.
const (
	Unsorted   ColumnDirection = 0
	Ascending                  = 1
	Descending                 = -1
)

// ColumnDef is the definition for a single column.
type ColumnDef struct {
	Type ColumnType
	Dir  ColumnDirection
	ID   int32
}

// Vec holds data for a single column. Vec provides accessors for the native
// data such as Int32() to access []int32 data.
type Vec struct {
	N    int32      // the number of elements in the bitmap
	Type ColumnType // the type of vector elements
	NullBitmap
	start unsafe.Pointer // pointer to start of the column data
	end   unsafe.Pointer // pointer to the end of column data
}

// Bool returns the vec data as a boolean bitmap. The bitmap should not be
// mutated.
func (v Vec) Bool() Bitmap {
	if v.Type != ColumnTypeBool {
		panic("vec does not hold bool data")
	}
	n := (v.count(int(v.N)) + 7) / 8
	return Bitmap((*[1 << 31]byte)(v.start)[:n:n])
}

// Int8 returns the vec data as []int8. The slice should not be mutated.
func (v Vec) Int8() []int8 {
	if v.Type != ColumnTypeInt8 {
		panic("vec does not hold int8 data")
	}
	n := v.count(int(v.N))
	return (*[1 << 31]int8)(v.start)[:n:n]
}

// Int16 returns the vec data as []int16. The slice should not be mutated.
func (v Vec) Int16() []int16 {
	if v.Type != ColumnTypeInt16 {
		panic("vec does not hold int16 data")
	}
	n := v.count(int(v.N))
	return (*[1 << 31]int16)(v.start)[:n:n]
}

// Int32 returns the vec data as []int32. The slice should not be mutated.
func (v Vec) Int32() []int32 {
	if v.Type != ColumnTypeInt32 {
		panic("vec does not hold int32 data")
	}
	n := v.count(int(v.N))
	return (*[1 << 31]int32)(v.start)[:n:n]
}

// Int64 returns the vec data as []int64. The slice should not be mutated.
func (v Vec) Int64() []int64 {
	if v.Type != ColumnTypeInt64 {
		panic("vec does not hold int64 data")
	}
	n := v.count(int(v.N))
	return (*[1 << 31]int64)(v.start)[:n:n]
}

// Float32 returns the vec data as []float32. The slice should not be mutated.
func (v Vec) Float32() []float32 {
	if v.Type != ColumnTypeFloat32 {
		panic("vec does not hold float32 data")
	}
	n := v.count(int(v.N))
	return (*[1 << 31]float32)(v.start)[:n:n]
}

// Float64 returns the vec data as []float64. The slice should not be mutated.
func (v Vec) Float64() []float64 {
	if v.Type != ColumnTypeFloat64 {
		panic("vec does not hold float64 data")
	}
	n := v.count(int(v.N))
	return (*[1 << 31]float64)(v.start)[:n:n]
}

// Bytes returns the vec data as Bytes. The underlying data should not be
// mutated.
func (v Vec) Bytes() Bytes {
	if v.Type != ColumnTypeBytes {
		panic("vec does not hold bytes data")
	}
	if uintptr(v.end)%4 != 0 {
		panic("expected offsets data to be 4-byte aligned")
	}
	n := v.N
	return Bytes{
		count:   int(n),
		data:    v.start,
		offsets: unsafe.Pointer(uintptr(v.end) - uintptr(n*4)),
	}
}
