// Copyright 2024 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package colblk

import (
	"encoding/binary"
	"fmt"
	"io"
	"math"
	"math/bits"
	"unsafe"

	"github.com/cockroachdb/pebble/internal/binfmt"
	"github.com/cockroachdb/pebble/internal/invariants"
	"golang.org/x/exp/constraints"
)

// Uint is a constraint that permits any unsigned integer type with an
// explicit size.
type Uint interface {
	~uint8 | ~uint16 | ~uint32 | ~uint64
}

// UintEncoding indicates how unsigned integers (of at most 64 bits) are
// encoded. It has two components:
//   - the low bits indicate how many bytes per integer are used, with
//     allowed values 0, 1, 2, 4, or 8.
//   - whether we are using a delta encoding, meaning that a base (64-bit) value
//     is encoded separately and each encoded value is a delta from that base.
//     Delta encoding is never necessary when we use 8 bytes per integer.
//
// Note that 0-byte encodings imply that all values are equal (either to the
// base value if we are using a delta encoding, otherwise to 0).
//
// The UintEncoding byte is serialized to the uint column before the column
// data.
type UintEncoding uint8

const uintEncodingDeltaBit UintEncoding = 1 << 7
const uintEncodingAllZero UintEncoding = 0

// IsDelta returns true if it is a delta encoding.
func (e UintEncoding) IsDelta() bool {
	return e&uintEncodingDeltaBit != 0
}

// Width returns the number of bytes used per integer. It can be 0, 1, 2, 4, or 8.
func (e UintEncoding) Width() int {
	return int(e &^ uintEncodingDeltaBit)
}

// IsValid returns true if the encoding is valid.
func (e UintEncoding) IsValid() bool {
	switch e.Width() {
	case 0, 1, 2, 4:
		return true
	case 8:
		// We should never need to do delta encoding if we store all 64 bits.
		return !e.IsDelta()
	default:
		return false
	}
}

// String implements fmt.Stringer.
func (e UintEncoding) String() string {
	if e.Width() == 0 {
		if e.IsDelta() {
			return "const"
		}
		return "zero"
	}
	deltaString := ""
	if e.IsDelta() {
		deltaString = ",delta"
	}
	return fmt.Sprintf("%db%s", e.Width(), deltaString)
}

// DetermineUintEncoding returns the best valid encoding that can be used to
// represent integers in the range [minValue, maxValue].
func DetermineUintEncoding(minValue, maxValue uint64) UintEncoding {
	// Find the number of bytes-per-value necessary for a delta encoding.
	b := (bits.Len64(maxValue-minValue) + 7) >> 3
	// Round up to the nearest allowed value (0, 1, 2, 4, or 8).
	if b > 4 {
		return UintEncoding(8)
	}
	if b == 3 {
		b = 4
	}
	// Check if we can use the same number of bytes without a delta encoding.
	isDelta := maxValue >= (1 << (b << 3))
	return makeUintEncoding(b, isDelta)
}

func makeUintEncoding(width int, isDelta bool) UintEncoding {
	e := UintEncoding(width)
	if isDelta {
		e |= uintEncodingDeltaBit
	}
	if invariants.Enabled && !e.IsValid() {
		panic(e)
	}
	return e
}

// UintBuilder builds a column of unsigned integers. It uses the smallest
// possible UintEncoding, depending on the values.
type UintBuilder struct {
	// configuration fixed on Init; preserved across Reset
	useDefault bool

	// array holds the underlying heap-allocated array in which values are
	// stored.
	array struct {
		// n is the size of the array (in count of T elements; not bytes). n is
		// NOT the number of elements that have been populated by the user.
		n int
		// elems provides access to elements without bounds checking. elems is
		// grown automatically in Set.
		elems UnsafeRawSlice[uint64]
	}
	// stats holds state for the purpose of tracking which UintEncoding would
	// be used if the caller Finished the column including all elements Set so
	// far. The stats state is used by Size (and Finish) to cheaply determine
	// which encoding may most concisely encode the array.
	//
	// Every Set(i, v) call updates minimum and maximum if necessary. If a call
	// updates minimum, maximum or both, it recalculates the encoding and if it
	// changed sets sets encodingRow=i, indicating which row last updated the
	// width.
	//
	// Any call to Size or Finish that supplies [rows] that's inclusive of the
	// index stored in widthRow may use the stored width. Calls with fewer
	// [rows] must recompute the min/max. In expected usage, only Finish will be
	// called with fewer rows and only with one less row than has been set,
	// meaning that only if the last row updated the width is a recomputation
	// necessary.
	//
	// TODO(jackson): There is a small discrete set of possible encodings, so we
	// could instead track the index of the first row that makes each encoding
	// impossible. This would allow us to avoid recomputing the min/max in all
	// cases. Or, if we limit the API to only allow Finish to be called with one
	// less than the last set row, we could maintain the width of only the last
	// two rows.
	stats struct {
		minimum     uint64
		maximum     uint64
		encoding    UintEncoding
		encodingRow int // index of last update to encoding
	}
}

// Init initializes the UintBuilder.
func (b *UintBuilder) Init() {
	b.init(false)
}

// InitWithDefault initializes the UintBuilder. Any rows that are not explicitly
// set are assumed to be zero. For the purpose of determining whether a delta
// encoding is possible, the column is assumed to contain at least 1 default
// value.
//
// InitWithDefault may be preferrable when a nonzero value is uncommon, and the
// caller can avoid explicitly Set-ing every zero value.
func (b *UintBuilder) InitWithDefault() {
	b.init(true)
}

func (b *UintBuilder) init(useDefault bool) {
	b.useDefault = useDefault
	b.Reset()
}

// NumColumns implements ColumnWriter.
func (b *UintBuilder) NumColumns() int { return 1 }

// DataType implements ColumnWriter.
func (b *UintBuilder) DataType(int) DataType { return DataTypeUint }

// Reset implements ColumnWriter and resets the builder, reusing existing
// allocated memory.
func (b *UintBuilder) Reset() {
	if b.useDefault {
		// If the caller configured a default zero, we assume that the array
		// will include at least one default value.
		b.stats.minimum = 0
		b.stats.maximum = 0
		clear(b.array.elems.Slice(b.array.n))
	} else {
		b.stats.minimum = math.MaxUint64
		b.stats.maximum = 0
		// We could reset all values as a precaution, but it has a visible cost
		// in benchmarks.
		if invariants.Sometimes(50) {
			for i := 0; i < b.array.n; i++ {
				b.array.elems.set(i, math.MaxUint64)
			}
		}
	}
	b.stats.encoding = uintEncodingAllZero
	b.stats.encodingRow = 0
}

// Get gets the value of the provided row index. The provided row must have been
// Set.
func (b *UintBuilder) Get(row int) uint64 {
	return b.array.elems.At(row)
}

// Set sets the value of the provided row index to v.
func (b *UintBuilder) Set(row int, v uint64) {
	if b.array.n <= row {
		// Double the size of the allocated array, or initialize it to at least 32
		// values (256 bytes) if this is the first allocation. Then double until
		// there's sufficient space.
		n2 := max(b.array.n<<1, 32)
		for n2 <= row {
			n2 <<= 1 /* double the size */
		}
		// NB: Go guarantees the allocated array will be 64-bit aligned.
		newDataTyped := make([]uint64, n2)
		copy(newDataTyped, b.array.elems.Slice(b.array.n))
		newElems := makeUnsafeRawSlice[uint64](unsafe.Pointer(&newDataTyped[0]))
		b.array.n = n2
		b.array.elems = newElems

	}
	// Maintain the running minimum and maximum for the purpose of maintaining
	// knowledge of the delta encoding that would be used.
	if b.stats.minimum > v || b.stats.maximum < v {
		b.stats.minimum = min(v, b.stats.minimum)
		b.stats.maximum = max(v, b.stats.maximum)
		// If updating the minimum and maximum means that we now much use a wider
		// width integer, update the encoding and the index of the update to it.
		if e := DetermineUintEncoding(b.stats.minimum, b.stats.maximum); e != b.stats.encoding {
			b.stats.encoding = e
			b.stats.encodingRow = row
		}
	}
	b.array.elems.set(row, v)
}

// Size implements ColumnWriter and returns the size of the column if its first
// [rows] rows were serialized, serializing the column into offset [offset].
func (b *UintBuilder) Size(rows int, offset uint32) uint32 {
	if rows == 0 {
		return offset
	}
	e, _ := b.determineEncoding(rows)
	return uintColumnSize(uint32(rows), offset, e)
}

// determineEncoding determines the best encoding for a column containing the
// first [rows], along with the minimum value (used as the "base" value when we
// use a stats encoding).
func (b *UintBuilder) determineEncoding(rows int) (_ UintEncoding, minimum uint64) {
	if b.stats.encodingRow < rows {
		// b.delta.encoding became the current value within the first [rows], so we
		// can use it.
		return b.stats.encoding, b.stats.minimum
	}

	// We have to recalculate the minimum and maximum.
	minimum, maximum := computeMinMax(b.array.elems.Slice(rows))
	return DetermineUintEncoding(minimum, maximum), minimum
}

// uintColumnSize returns the size of a column of unsigned integers, encoded at
// the provided offset using the provided width. If width < sizeof(T), then a
// delta encoding is assumed.
func uintColumnSize(rows, offset uint32, e UintEncoding) uint32 {
	offset++ // DeltaEncoding byte
	if e.IsDelta() {
		// A delta encoding will be used. We need to first account for the constant
		// that encodes the base value.
		offset += 8
	}
	width := uint32(e.Width())
	// Include alignment bytes necessary to align offset appropriately for
	// elements of the delta width.
	if width > 0 {
		offset = align(offset, width)
	}
	// Now account for the array of [rows] x w elements encoding the deltas.
	return offset + rows*width
}

// Finish implements ColumnWriter, serializing the column into offset [offset] of
// [buf].
func (b *UintBuilder) Finish(col, rows int, offset uint32, buf []byte) uint32 {
	if rows == 0 {
		return offset
	}

	e, minimum := b.determineEncoding(rows)

	// NB: In some circumstances, it's possible for b.array.elems.ptr to be nil.
	// Specifically, if the builder is initialized using InitWithDefault and no
	// non-default values exist, no array will have been allocated (we lazily
	// allocate b.array.elems.ptr). It's illegal to try to construct an unsafe
	// slice from a nil ptr with non-zero rows. Only attempt to construct the
	// values slice if there's actually a non-nil ptr.
	var valuesSlice []uint64
	if b.array.elems.ptr != nil {
		valuesSlice = b.array.elems.Slice(rows)
	}
	return uintColumnFinish(minimum, valuesSlice, e, offset, buf)
}

// uintColumnFinish finishes the column of unsigned integers of type T, applying
// the given encoding.
func uintColumnFinish(
	minimum uint64, values []uint64, e UintEncoding, offset uint32, buf []byte,
) uint32 {
	buf[offset] = byte(e)
	offset++

	deltaBase := uint64(0)
	if e.IsDelta() {
		deltaBase = minimum
		binary.LittleEndian.PutUint64(buf[offset:], minimum)
		offset += 8
	}
	width := uint32(e.Width())
	if width == 0 {
		// All the column values are the same.
		return offset
	}
	// Align the offset appropriately.
	offset = alignWithZeroes(buf, offset, width)

	switch e.Width() {
	case 1:
		dest := makeUnsafeRawSlice[uint8](unsafe.Pointer(&buf[offset])).Slice(len(values))
		reduceUints(deltaBase, values, dest)

	case 2:
		dest := makeUnsafeRawSlice[uint16](unsafe.Pointer(&buf[offset])).Slice(len(values))
		reduceUints(deltaBase, values, dest)

	case 4:
		dest := makeUnsafeRawSlice[uint32](unsafe.Pointer(&buf[offset])).Slice(len(values))
		reduceUints(deltaBase, values, dest)

	case 8:
		if deltaBase != 0 {
			panic("unreachable")
		}
		dest := makeUnsafeRawSlice[uint64](unsafe.Pointer(&buf[offset])).Slice(len(values))
		copy(dest, values)

	default:
		panic("unreachable")
	}
	return offset + uint32(len(values))*width
}

// WriteDebug implements Encoder.
func (b *UintBuilder) WriteDebug(w io.Writer, rows int) {
	fmt.Fprintf(w, "%s: %d rows", DataTypeUint, rows)
}

// reduceUints reduces the bit-width of a slice of unsigned by subtracting a
// minimum value from each element and writing it to dst. For example,
//
//	reduceUints[uint64, uint8](10, []uint64{10, 11, 12}, dst)
//
// could be used to reduce a slice of uint64 values to uint8 values {0, 1, 2}.
func reduceUints[O constraints.Integer, N constraints.Integer](minimum O, values []O, dst []N) {
	for i := 0; i < len(values); i++ {
		dst[i] = N(values[i] - minimum)
	}
}

// computeMinMax computes the minimum and the maximum of the provided slice of
// unsigned integers.
func computeMinMax[I constraints.Unsigned](values []I) (I, I) {
	minimum := I(0) - 1
	maximum := I(0)
	for _, v := range values {
		minimum = min(minimum, v)
		maximum = max(maximum, v)
	}
	return minimum, maximum
}

func uintsToBinFormatter(
	f *binfmt.Formatter, rows int, uintFormatter func(el, base uint64) string,
) {
	if rows == 0 {
		return
	}
	if uintFormatter == nil {
		uintFormatter = func(v, base uint64) string {
			if base == 0 {
				return fmt.Sprint(v)
			}
			return fmt.Sprintf("%d + %d = %d", v, base, base+v)
		}
	}

	e := UintEncoding(f.PeekUint(1)) // UintEncoding byte
	if !e.IsValid() {
		panic(fmt.Sprintf("%d", e))
	}
	f.HexBytesln(1, "encoding: %s", e)

	var base uint64
	if e.IsDelta() {
		base = f.PeekUint(8)
		f.HexBytesln(8, "64-bit constant: %d", base)
	}
	width := e.Width()
	if width == 0 {
		// The column is zero or constant.
		return
	}

	if off := align(f.Offset(), width); off != f.Offset() {
		f.HexBytesln(off-f.Offset(), "padding (aligning to %d-bit boundary)", width*8)
	}
	for i := 0; i < rows; i++ {
		f.HexBytesln(width, "data[%d] = %s", i, uintFormatter(f.PeekUint(width), base))
	}
}
