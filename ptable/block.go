package ptable

import (
	"bytes"
	"encoding/binary"
	"math"
	"unsafe"
)

// Block layout
//
// +-----------------------------------------------------+
// | ncols(4) | nrows(4) | page1(4) | page2(4) | ...     |
// +-----------------------------------------------------+
// | col-ID1(4) | col-ID2(4) | ...                       |
// +-----------------------------------------------------+
// | <bool>  | null-bitmap | value-bitmap                |
// +-----------------------------------------------------+
// | <int32> | null-bitmap | values (4-byte aligned)     |
// +-----------------------------------------------------+
// | <bytes> | val1 | val2 | ...     | pos (4) | pos (4) |
// +-----------------------------------------------------+
// | ...                                                 |
// +-----------------------------------------------------+
//
// Blocks contain rows following a fixed schema. The data is stored in a column
// layout: all of the values for a column is stored contiguously. Column types
// have either fixed-width values, or variable-width. All variable-width values
// are stored in the "bytes" column type and it is up to higher levels to
// interpret.
//
// The data for a column is stored within a "page". The first byte in a page
// specifies the column type. Fixed width pages are then followed by a null
// bitmap with 1-bit per row indicating whether the column at that row is null
// or not. Following the null bitmap is the column data itself. The data is
// aligned to the required alignment of the column type (4 for int32, 8 for
// int64, etc) so that it can be accessed directly without decoding.
//
// Variable width data (i.e. the "bytes" column type) is stored in a different
// format. Immediately following the column type are the concatenated variable
// length values. After the concatenated data is an array of offsets indicating
// the end of each column value within the concatenated data. For example,
// offset[0] is the end of the first row's column data. A negative offset
// indicates a null value.

// TODO(peter):
//
// - Do we need to store which columns the rows are sorted on? How to store
//   sort order?
//
// - Iteration iterates over blocks. Every row has an implicit timestamp column
//   containing the hlc timestamp.
//
// - How to integrate with the memtable? The memtable contains relatively
//   little data. Do we convert to columnar data on the fly?
//
// - How to specify the schema for a given key? The number of schemas is the
//   number of indexes in all of the tables. The /table/index/ prefix is a
//   unique prefix. Perhaps there should be a callback from key to schema.

type columnWriter struct {
	def     columnDef
	data    []byte
	offsets []int32
	nulls   bitmap
	tmp     [8]byte
	count   int32
}

func (w *columnWriter) grow(n int) []byte {
	i := len(w.data)
	if cap(w.data)-i < n {
		newSize := 2 * cap(w.data)
		if newSize == 0 {
			newSize = 256
		}
		newData := make([]byte, i, newSize)
		copy(newData, w.data)
		w.data = newData
	}
	w.data = w.data[:i+n]
	return w.data[i:]
}

func (w *columnWriter) putBool(v bool) {
	if w.def.Type != columnTypeBool {
		panic("bool column value expected")
	}
	w.data = (bitmap)(w.data).set(int(w.count), v)
	w.nulls = w.nulls.set(int(w.count), false)
	w.count++
}

func (w *columnWriter) putInt8(v int8) {
	if w.def.Type != columnTypeInt8 {
		panic("int8 column value expected")
	}
	w.data = append(w.data, byte(v))
	w.nulls = w.nulls.set(int(w.count), false)
	w.count++
}

func (w *columnWriter) putInt16(v int16) {
	if w.def.Type != columnTypeInt16 {
		panic("int16 column value expected")
	}
	binary.LittleEndian.PutUint16(w.grow(2), uint16(v))
	w.nulls = w.nulls.set(int(w.count), false)
	w.count++
}

func (w *columnWriter) putInt32(v int32) {
	if w.def.Type != columnTypeInt32 {
		panic("int32 column value expected")
	}
	binary.LittleEndian.PutUint32(w.grow(4), uint32(v))
	w.nulls = w.nulls.set(int(w.count), false)
	w.count++
}

func (w *columnWriter) putInt64(v int64) {
	if w.def.Type != columnTypeInt64 {
		panic("int64 column value expected")
	}
	binary.LittleEndian.PutUint64(w.grow(8), uint64(v))
	w.nulls = w.nulls.set(int(w.count), false)
	w.count++
}

func (w *columnWriter) putFloat32(v float32) {
	if w.def.Type != columnTypeFloat32 {
		panic("float32 column value expected")
	}
	binary.LittleEndian.PutUint32(w.grow(4), math.Float32bits(v))
	w.nulls = w.nulls.set(int(w.count), false)
	w.count++
}

func (w *columnWriter) putFloat64(v float64) {
	if w.def.Type != columnTypeFloat64 {
		panic("float64 column value expected")
	}
	binary.LittleEndian.PutUint64(w.grow(8), math.Float64bits(v))
	w.nulls = w.nulls.set(int(w.count), false)
	w.count++
}

func (w *columnWriter) putBytes(v []byte) {
	if w.def.Type != columnTypeBytes {
		panic("bytes column value expected")
	}
	w.data = append(w.data, v...)
	w.offsets = append(w.offsets, int32(len(w.data)))
	w.count++
}

func (w *columnWriter) putNull() {
	switch w.def.Type {
	case columnTypeBool,
		columnTypeInt8,
		columnTypeInt16,
		columnTypeInt32,
		columnTypeInt64,
		columnTypeFloat32,
		columnTypeFloat64:
		w.nulls = w.nulls.set(int(w.count), true)
	case columnTypeBytes:
		w.offsets = append(w.offsets, -1)
	default:
		panic("not reached")
	}
	w.count++
}

func align(offset, val int32) int32 {
	return (offset + val - 1) & ^(val - 1)
}

func (w *columnWriter) encode(offset int32, buf []byte) {
	buf[0] = byte(w.def.Type)
	buf = buf[1:]
	offset++
	if w.def.Type.width() > 0 {
		copy(buf, w.nulls)
		n := align(offset+int32(len(w.nulls)), w.def.Type.alignment()) - offset
		copy(buf[n:], w.data)
	} else {
		n := copy(buf, w.data)
		offset += int32(n)
		pad := align(offset, 4) - offset
		buf = buf[int32(n)+pad:]
		for i := range w.offsets {
			binary.LittleEndian.PutUint32(buf[i*4:], uint32(w.offsets[i]))
		}
	}
}

func (w *columnWriter) size(offset int32) int32 {
	newOffset := offset + 1
	if w.def.Type.width() > 0 {
		newOffset += int32(w.count+7) / 8
		newOffset = align(newOffset, w.def.Type.alignment())
		newOffset += int32(len(w.data))
	} else {
		newOffset += int32(len(w.data))
		newOffset = align(newOffset, 4)
		newOffset += int32(len(w.offsets) * 4)
	}
	return newOffset - offset
}

func blockHeaderSize(n int) int32 {
	return int32(8 + n*8)
}

func pageOffsetPos(i int) int32 {
	return int32(8 + i*4)
}

func colIDPos(i, n int) int32 {
	return int32(8 + (n+i)*4)
}

type blockWriter struct {
	cols []columnWriter
}

func (w *blockWriter) String() string {
	var buf bytes.Buffer
	for i := range w.cols {
		if i > 0 {
			buf.WriteString(",")
		}
		switch w.cols[i].def.Type {
		case columnTypeBool:
			buf.WriteString("bool")
		case columnTypeInt8:
			buf.WriteString("int8")
		case columnTypeInt16:
			buf.WriteString("int16")
		case columnTypeInt32:
			buf.WriteString("int32")
		case columnTypeInt64:
			buf.WriteString("int64")
		case columnTypeFloat32:
			buf.WriteString("float32")
		case columnTypeFloat64:
			buf.WriteString("float64")
		case columnTypeBytes:
			buf.WriteString("bytes")
		default:
			buf.WriteString("unknown")
		}
	}
	return buf.String()
}

func (w *blockWriter) Finish() []byte {
	buf := make([]byte, w.Size())
	n := len(w.cols)
	binary.LittleEndian.PutUint32(buf[0:], uint32(n))
	binary.LittleEndian.PutUint32(buf[4:], uint32(w.cols[0].count))
	pageOffset := blockHeaderSize(n)
	for i := range w.cols {
		col := &w.cols[i]
		binary.LittleEndian.PutUint32(buf[pageOffsetPos(i):], uint32(pageOffset))
		binary.LittleEndian.PutUint32(buf[colIDPos(i, n):], uint32(col.def.ID))
		col.encode(pageOffset, buf[pageOffset:])
		pageOffset += col.size(pageOffset)
	}
	return buf
}

func (w *blockWriter) Size() int32 {
	size := blockHeaderSize(len(w.cols))
	for i := range w.cols {
		size += w.cols[i].size(size)
	}
	return size
}

func (w *blockWriter) SetSchema(s []columnDef) bool {
	if len(w.cols) == 0 {
		w.cols = make([]columnWriter, len(s))
		for i := range s {
			w.cols[i].def = s[i]
		}
		return true
	}
	if len(w.cols) != len(s) {
		return false
	}
	for i := range s {
		if w.cols[i].def != s[i] {
			return false
		}
	}
	return true
}

func (w *blockWriter) PutBool(i int, v bool) {
	w.cols[i].putBool(v)
}

func (w *blockWriter) PutInt8(i int, v int8) {
	w.cols[i].putInt8(v)
}

func (w *blockWriter) PutInt16(i int, v int16) {
	w.cols[i].putInt16(v)
}

func (w *blockWriter) PutInt32(i int, v int32) {
	w.cols[i].putInt32(v)
}

func (w *blockWriter) PutInt64(i int, v int64) {
	w.cols[i].putInt64(v)
}

func (w *blockWriter) PutFloat32(i int, v float32) {
	w.cols[i].putFloat32(v)
}

func (w *blockWriter) PutFloat64(i int, v float64) {
	w.cols[i].putFloat64(v)
}

func (w *blockWriter) PutBytes(i int, v []byte) {
	w.cols[i].putBytes(v)
}

type blockReader struct {
	data []byte
	cols int32
	rows int32
}

func newReader(data []byte) *blockReader {
	r := &blockReader{}
	r.init(data)
	return r
}

func (r *blockReader) init(data []byte) {
	r.data = data
	r.cols = int32(binary.LittleEndian.Uint32(data[0:]))
	r.rows = int32(binary.LittleEndian.Uint32(data[4:]))
}

func (r *blockReader) pageStart(j int) int32 {
	pages := unsafe.Pointer(&r.data[8])
	return *(*int32)(unsafe.Pointer(uintptr(pages) + uintptr(j*4)))
}

func (r *blockReader) pageEnd(j int) int32 {
	if int32(j+1) < r.cols {
		return r.pageStart(j + 1)
	}
	return int32(len(r.data))
}

func (r *blockReader) columnType(j int) columnType {
	return columnType(r.data[r.pageStart(j)])
}

func (r *blockReader) pageData(j int) unsafe.Pointer {
	return unsafe.Pointer(&r.data[r.pageStart(j)+1])
}

func (r *blockReader) blockOffset(p unsafe.Pointer) int32 {
	return int32(uintptr(p) - uintptr(unsafe.Pointer(&r.data[0])))
}

func (r *blockReader) Column(j int) columnDef {
	k := colIDPos(j, int(r.cols))
	id := int32(binary.LittleEndian.Uint32(r.data[k:]))
	return columnDef{Type: r.columnType(j), ID: id}
}

// Bool returns column j as int bool data.
func (r *blockReader) Bool(j int) (_ bitmap, nulls bitmap) {
	p := r.pageData(j)
	n := int32(r.rows+7) / 8
	nulls = bitmap((*[1 << 31]byte)(p)[:n:n])
	p = unsafe.Pointer(uintptr(p) + uintptr(n))
	n = int32(r.rows+7) / 8
	return bitmap((*[1 << 31]byte)(p)[:n:n]), nulls
}

// Int8 returns column j as int column data.
func (r *blockReader) Int8(j int) (_ []int8, nulls bitmap) {
	p := r.pageData(j)
	n := int32(r.rows+7) / 8
	nulls = bitmap((*[1 << 31]byte)(p)[:n:n])
	p = unsafe.Pointer(uintptr(p) + uintptr(n))
	return (*[1 << 31]int8)(p)[:r.rows:r.rows], nil
}

// Int16 returns column j as int column data.
func (r *blockReader) Int16(j int) (_ []int16, nulls bitmap) {
	p := r.pageData(j)
	s := r.blockOffset(p)
	n := int32(r.rows+7) / 8
	nulls = bitmap((*[1 << 31]byte)(p)[:n:n])
	p = unsafe.Pointer(uintptr(p) + uintptr(align(s+n, 2)-s))
	return (*[1 << 31]int16)(p)[:r.rows:r.rows], nil
}

// Int32 returns column j as int column data.
func (r *blockReader) Int32(j int) (_ []int32, nulls bitmap) {
	p := r.pageData(j)
	s := r.blockOffset(p)
	n := int32(r.rows+7) / 8
	nulls = bitmap((*[1 << 31]byte)(p)[:n:n])
	p = unsafe.Pointer(uintptr(p) + uintptr(align(s+n, 4)-s))
	return (*[1 << 31]int32)(p)[:r.rows:r.rows], nil
}

// Int64 returns column j as int column data.
func (r *blockReader) Int64(j int) (_ []int64, nulls bitmap) {
	p := r.pageData(j)
	s := r.blockOffset(p)
	n := int32(r.rows+7) / 8
	nulls = bitmap((*[1 << 31]byte)(p)[:n:n])
	p = unsafe.Pointer(uintptr(p) + uintptr(align(s+n, 8)-s))
	return (*[1 << 31]int64)(p)[:r.rows:r.rows], nil
}

// Float32 returns column j as float column data.
func (r *blockReader) Float32(j int) (_ []float32, nulls bitmap) {
	p := r.pageData(j)
	s := r.blockOffset(p)
	n := int32(r.rows+7) / 8
	nulls = bitmap((*[1 << 31]byte)(p)[:n:n])
	p = unsafe.Pointer(uintptr(p) + uintptr(align(s+n, 4)-s))
	return (*[1 << 31]float32)(p)[:r.rows:r.rows], nil
}

// Float64 returns column j as float column data.
func (r *blockReader) Float64(j int) (_ []float64, nulls bitmap) {
	p := r.pageData(j)
	s := r.blockOffset(p)
	n := int32(r.rows+7) / 8
	nulls = bitmap((*[1 << 31]byte)(p)[:n:n])
	p = unsafe.Pointer(uintptr(p) + uintptr(align(s+n, 8)-s))
	return (*[1 << 31]float64)(p)[:r.rows:r.rows], nil
}

// Bytes returns column j as byte column data. The first return value is the
// concatenated byte data. The second return value is a series of offsets where
// each offset specifies the end position in the concatenated data. The start
// position for column i can be determined from the end position of column
// i-1. A negative offset indicates a null value.
func (r *blockReader) Bytes(j int) ([]byte, []int32) {
	pageEnd := r.pageEnd(j)
	i := pageEnd - r.rows*4
	if i%4 != 0 {
		panic("expected offsets data to be 4-byte aligned")
	}
	offsets := (*[1 << 31]int32)(unsafe.Pointer(&r.data[pageEnd-r.rows*4]))[:r.rows:r.rows]
	n := offsets[r.rows-1]
	return (*[1 << 31]byte)(r.pageData(j))[:n:n], offsets
}
