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

	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/pebble/internal/binfmt"
	"github.com/cockroachdb/pebble/internal/invariants"
	"github.com/cockroachdb/pebble/internal/treeprinter"
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

// UintEncodingRowThreshold is the threshold under which the number of rows can
// affect the best encoding. This happens when the constant 8 bytes for the
// delta base doesn't make up for saving a byte or two in the per-row encoding.
const UintEncodingRowThreshold = 8

// DetermineUintEncoding returns the best valid encoding that can be used to
// represent numRows integers in the range [minValue, maxValue].
//
// DetermineUintEncoding returns the same result for any value of rows >=
// UintEncodingRowThreshold.
func DetermineUintEncoding(minValue, maxValue uint64, numRows int) UintEncoding {
	b := byteWidth(maxValue - minValue)
	if b == 8 {
		return UintEncoding(8)
	}
	// Check if we can use the same number of bytes without a delta encoding.
	isDelta := maxValue >= (1 << (b << 3))
	if isDelta && numRows < UintEncodingRowThreshold {
		bNoDelta := byteWidth(maxValue)
		// Check if saving (bNoDelta-b) bytes per row makes up for the 8 bytes
		// required by the delta base.
		if numRows*int(bNoDelta-b) < 8 {
			b = bNoDelta
			isDelta = false
		}
	}
	return makeUintEncoding(b, isDelta)
}

// DetermineUintEncodingNoDelta is a more efficient variant of
// DetermineUintEncoding when minValue is zero (or we don't need a delta
// encoding).
func DetermineUintEncodingNoDelta(maxValue uint64) UintEncoding {
	return makeUintEncoding(byteWidth(maxValue), false /* isDelta */)
}

// byteWidthTable maps a number’s bit‐length to the number of bytes needed.
var byteWidthTable = [65]uint8{
	// 0 bits => 0 bytes
	0,
	// 1..8 bits => 1 byte
	1, 1, 1, 1, 1, 1, 1, 1,
	// 9..16 bits => 2 bytes
	2, 2, 2, 2, 2, 2, 2, 2,
	// 17..32 bits => 4 bytes
	4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4,
	// 33..64 bits => 8 bytes
	8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8,
	8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8,
}

// byteWidth returns the number of bytes necessary to represent the given value,
// either 0, 1, 2, 4, or 8.
func byteWidth(maxValue uint64) uint8 {
	// bits.Len64 returns 0 for 0, 1..64 for others.
	// We then simply return the precomputed result.
	return byteWidthTable[bits.Len64(maxValue)]
}

func makeUintEncoding(width uint8, isDelta bool) UintEncoding {
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

	elems []uint64

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
		clear(b.elems)
	} else {
		b.stats.minimum = math.MaxUint64
		b.stats.maximum = 0
		// We could reset all values as a precaution, but it has a visible cost
		// in benchmarks.
		if invariants.Enabled && invariants.Sometimes(50) {
			for i := range b.elems {
				b.elems[i] = math.MaxUint64
			}
		}
	}
	b.stats.encoding = uintEncodingAllZero
	b.stats.encodingRow = 0
}

// Get gets the value of the provided row index. The provided row must have been
// Set or the builder must have been initialized with InitWithDefault.
func (b *UintBuilder) Get(row int) uint64 {
	// If the UintBuilder is configured to use a zero value for unset rows, it's
	// possible that the array has not been grown to a size that includes [row].
	if len(b.elems) <= row {
		if invariants.Enabled && !b.useDefault {
			panic(errors.AssertionFailedf("Get(%d) on UintBuilder with array of size %d", row, len(b.elems)))
		}
		return 0
	}
	return b.elems[row]
}

// Set sets the value of the provided row index to v.
func (b *UintBuilder) Set(row int, v uint64) {
	if len(b.elems) <= row {
		// Double the size of the allocated array, or initialize it to at least 32
		// values (256 bytes) if this is the first allocation. Then double until
		// there's sufficient space.
		n2 := max(len(b.elems)<<1, 32)
		for n2 <= row {
			n2 <<= 1 // double the size
		}
		// NB: Go guarantees the allocated array will be 64-bit aligned.
		newElems := make([]uint64, n2)
		copy(newElems, b.elems)
		b.elems = newElems
	}
	// Maintain the running minimum and maximum for the purpose of maintaining
	// knowledge of the delta encoding that would be used.
	if b.stats.minimum > v || b.stats.maximum < v || row < UintEncodingRowThreshold {
		b.stats.minimum = min(v, b.stats.minimum)
		b.stats.maximum = max(v, b.stats.maximum)
		// If updating the minimum and maximum means that we now much use a wider
		// width integer, update the encoding and the index of the update to it.
		if e := DetermineUintEncoding(b.stats.minimum, b.stats.maximum, row+1); e != b.stats.encoding {
			b.stats.encoding = e
			b.stats.encodingRow = row
		}
	}
	b.elems[row] = v
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
// first [rows], along with a lower bound on all the values which can be used as
// a "base" if the encoding is a delta encoding.
func (b *UintBuilder) determineEncoding(rows int) (_ UintEncoding, deltaBase uint64) {
	if b.stats.encodingRow < rows {
		// b.delta.encoding became the current value within the first [rows], so we
		// can use it.
		//
		// Note that if useDefault is set, this encoding assumes there is at least
		// one element with the default (zero) value, which might be pessimistic.
		//
		// Note that b.stats.minimum includes all rows set so far so it might be
		// strictly smaller than all values up to [rows]; but it is still a suitable
		// base for b.stats.encoding.
		if invariants.Enabled && invariants.Sometimes(1) && rows > 0 {
			if enc, _ := b.recalculateEncoding(rows); enc != b.stats.encoding {
				panic(fmt.Sprintf("fast and slow paths don't agree: %s vs %s", b.stats.encoding, enc))
			}
		}
		return b.stats.encoding, b.stats.minimum
	}
	return b.recalculateEncoding(rows)
}

func (b *UintBuilder) recalculateEncoding(rows int) (_ UintEncoding, deltaBase uint64) {
	// We have to recalculate the minimum and maximum.
	minimum, maximum := computeMinMax(b.elems[:min(rows, len(b.elems))])
	if b.useDefault {
		// Mirror the pessimism of the fast path so that the result is consistent.
		// Otherwise, adding a row can result in a different encoding even when not
		// including that row.
		minimum = 0
	}
	return DetermineUintEncoding(minimum, maximum, rows), minimum
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

	values := b.elems[:min(rows, len(b.elems))]
	return uintColumnFinish(rows, minimum, values, e, offset, buf)
}

// uintColumnFinish finishes the column of unsigned integers of type T, applying
// the given encoding.
func uintColumnFinish(
	rows int, minimum uint64, values []uint64, e UintEncoding, offset uint32, buf []byte,
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
		dest := buf[offset : offset+uint32(rows)]
		reduceUints(deltaBase, values, dest)

	case 2:
		dest := unsafe.Slice((*uint16)(unsafe.Pointer(&buf[offset])), rows)
		reduceUints(deltaBase, values, dest)

	case 4:
		dest := unsafe.Slice((*uint32)(unsafe.Pointer(&buf[offset])), rows)
		reduceUints(deltaBase, values, dest)

	case 8:
		if deltaBase != 0 {
			panic("unreachable")
		}
		dest := unsafe.Slice((*uint64)(unsafe.Pointer(&buf[offset])), rows)
		copy(dest, values)

	default:
		panic("unreachable")
	}
	return offset + uint32(rows)*width
}

// WriteDebug implements Encoder.
func (b *UintBuilder) WriteDebug(w io.Writer, rows int) {
	fmt.Fprintf(w, "%s: %d rows", DataTypeUint, rows)
}

// reduceUints reduces the bit-width of a slice of unsigned by subtracting a
// minimum value from each element and writing it to dst. For example,
//
//	reduceUints[uint8](10, []uint64{10, 11, 12}, dst)
//
// could be used to reduce a slice of uint64 values to uint8 values {0, 1, 2}.
//
// The values slice can be smaller than dst; in that case, the values between
// len(values) and len(dst) are assumed to be 0.
func reduceUints[N constraints.Integer](minimum uint64, values []uint64, dst []N) {
	_ = dst[len(values)-1]
	for i, v := range values {
		if invariants.Enabled {
			if v < minimum {
				panic("incorrect minimum value")
			}
			if v-minimum > uint64(N(0)-1) {
				panic("incorrect target width")
			}
		}
		//gcassert:bce
		dst[i] = N(v - minimum)
	}
	if invariants.Enabled && len(values) < len(dst) && minimum != 0 {
		panic("incorrect minimum value")
	}
	for i := len(values); i < len(dst); i++ {
		dst[i] = 0
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
	f *binfmt.Formatter, tp treeprinter.Node, rows int, uintFormatter func(el, base uint64) string,
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
		f.ToTreePrinter(tp)
		return
	}

	if off := align(f.RelativeOffset(), width); off != f.RelativeOffset() {
		f.HexBytesln(off-f.RelativeOffset(), "padding (aligning to %d-bit boundary)", width*8)
	}
	for i := 0; i < rows; i++ {
		f.HexBytesln(width, "data[%d] = %s", i, uintFormatter(f.PeekUint(width), base))
	}
	f.ToTreePrinter(tp)
}
