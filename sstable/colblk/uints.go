// Copyright 2024 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package colblk

import (
	"fmt"
	"io"
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

// UintBuilder builds a column of unsigned integers of the same width.
// UintBuilder uses a delta encoding when possible to store values using
// lower-width integers. See DeltaEncoding.
type UintBuilder[T Uint] struct {
	// configuration fixed on Init; preserved across Reset
	dt         DataType
	useDefault bool

	// array holds the underlying heap-allocated array in which values are
	// stored.
	array struct {
		// n is the size of the array (in count of T elements; not bytes). n is
		// NOT the number of elements that have been populated by the user.
		n int
		// elems provides access to elements without bounds checking. elems is
		// grown automatically in Set.
		elems UnsafeRawSlice[T]
	}
	// delta holds state for the purpose of tracking which DeltaEncoding would
	// be used if the caller Finished the column including all elements Set so
	// far. The delta state is used by Size (and Finish) to cheaply determine
	// which encoding may most concisely encode the array.
	//
	// Every Set(i, v) call updates minimum and maximum if necessary. If a call
	// updates minimum, maximum or both, it sets the width to the number of
	// bytes necessary to represent the new difference between maximum and
	// minimum. It also sets widthRow=i, indicating which row last updated the
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
	delta struct {
		minimum  T
		maximum  T
		width    uint32 // 0, 1, 2, 4, or 8
		widthRow int    // index of last update to width
	}
}

// Init initializes the UintBuilder.
func (b *UintBuilder[T]) Init() {
	b.init(false)
}

// InitWithDefault initializes the UintBuilder. Any rows that are not explicitly
// set are assumed to be zero. For the purpose of determining whether a delta
// encoding is possible, the column is assumed to contain at least 1 default
// value.
//
// InitWithDefault may be preferrable when a nonzero value is uncommon, and the
// caller can avoid explicitly Set-ing every zero value.
func (b *UintBuilder[T]) InitWithDefault() {
	b.init(true)
}

func (b *UintBuilder[T]) init(useDefault bool) {
	b.useDefault = useDefault
	switch unsafe.Sizeof(T(0)) {
	case 1:
		b.dt = DataTypeUint8
	case 2:
		b.dt = DataTypeUint16
	case 4:
		b.dt = DataTypeUint32
	case 8:
		b.dt = DataTypeUint64
	default:
		panic("unreachable")
	}
	b.Reset()
}

// NumColumns implements ColumnWriter.
func (b *UintBuilder[T]) NumColumns() int { return 1 }

// Reset implements ColumnWriter and resets the builder, reusing existing
// allocated memory.
func (b *UintBuilder[T]) Reset() {
	if b.useDefault {
		// If the caller configured a default zero, we assume that the array
		// will include at least one default value.
		b.delta.minimum = 0
		b.delta.maximum = 0
		clear(b.array.elems.Slice(b.array.n))
	} else {
		// Initialize the minimum to the max value that a T can represent. We
		// subtract from zero, relying on the fact that T is unsigned and will
		// wrap around to the maximal value.
		b.delta.minimum = T(0) - 1
		b.delta.maximum = 0
		// We could reset all values as a precaution, but it has a visible cost
		// in benchmarks.
		if invariants.Sometimes(50) {
			for i := 0; i < b.array.n; i++ {
				b.array.elems.set(i, T(0)-1)
			}
		}
	}
	b.delta.widthRow = 0
	b.delta.width = 0
}

// Set sets the value of the provided row index to v.
func (b *UintBuilder[T]) Set(row int, v T) {
	if b.array.n <= row {
		// Double the size of the allocated array, or initialize it to at least
		// 256 bytes if this is the first allocation. Then double until there's
		// sufficient space for n bytes.
		n2 := max(b.array.n<<1, 256/int(unsafe.Sizeof(T(0))))
		for n2 <= row {
			n2 <<= 1 /* double the size */
		}
		// NB: Go guarantees the allocated array will be T-aligned.
		newDataTyped := make([]T, n2)
		copy(newDataTyped, b.array.elems.Slice(b.array.n))
		newElems := makeUnsafeRawSlice[T](unsafe.Pointer(&newDataTyped[0]))
		b.array.n = n2
		b.array.elems = newElems

	}
	// Maintain the running minimum and maximum for the purpose of maintaining
	// knowledge of the delta encoding that would be used.
	if b.delta.minimum > v || b.delta.maximum < v {
		b.delta.minimum = min(v, b.delta.minimum)
		b.delta.maximum = max(v, b.delta.maximum)
		// If updating the minimum and maximum means that we now much use a
		// wider width integer, update the width and the index of the update to
		// it.
		if w := deltaWidth(uint64(b.delta.maximum - b.delta.minimum)); w != b.delta.width {
			b.delta.width = w
			b.delta.widthRow = row
		}
	}
	b.array.elems.set(row, v)
}

// Size implements ColumnWriter and returns the size of the column if its first
// [rows] rows were serialized, serializing the column into offset [offset].
func (b *UintBuilder[T]) Size(rows int, offset uint32) uint32 {
	if rows == 0 {
		return 0
	}
	// Determine the width of each element with delta-encoding applied.
	// b.delta.width is the precomputed width for all rows. It's the best
	// encoding we can use as long as b.delta.widthRow is included. If
	// b.delta.widthRow is not included (b.delta.widthRow > rows-1), we need to
	// scan the [rows] elements of the array to recalculate the appropriate
	// delta.
	w := b.delta.width
	if b.delta.widthRow > rows-1 {
		minimum, maximum := computeMinMax(b.array.elems.Slice(rows))
		w = deltaWidth(uint64(maximum - minimum))
	}
	return uintColumnSize[T](uint32(rows), offset, w)
}

// uintColumnSize returns the size of a column of unsigned integers of type T,
// encoded at the provided offset using the provided width. If width <
// sizeof(T), then a delta encoding is assumed.
func uintColumnSize[T Uint](rows, offset, width uint32) uint32 {
	logicalWidth := uint32(unsafe.Sizeof(T(0)))
	// Include alignment bytes necessary to align offset appropriately for
	// elements of type T.
	offset = align(offset, logicalWidth)
	if width != logicalWidth {
		// A delta encoding will be used. We need to first account for the constant
		// that encodes the minimum. This constant is the full width of the column's
		// logical data type.
		offset += logicalWidth
	}
	// Now account for the array of [rows] x w elements encoding the deltas.
	return offset + rows*width
}

// Finish implements ColumnWriter, serializing the column into offset [offset] of
// [buf].
func (b *UintBuilder[T]) Finish(col, rows int, offset uint32, buf []byte) (uint32, ColumnDesc) {
	desc := ColumnDesc{DataType: b.dt}
	if rows == 0 {
		return offset, desc
	}

	// Determine the width of each element with delta-encoding applied.
	// b.delta.width is the precomputed width for all rows. It's the best
	// encoding we can use as long as b.delta.widthRow is included. If
	// b.delta.widthRow is not included (b.delta.widthRow > rows-1), we need to
	// scan the [rows] elements of the array to recalculate the appropriate
	// delta.
	minimum := b.delta.minimum
	w := b.delta.width
	if b.delta.widthRow > rows-1 {
		var maximum T
		minimum, maximum = computeMinMax(b.array.elems.Slice(rows))
		w = deltaWidth(uint64(maximum - minimum))
	}
	offset, desc.Encoding = uintColumnFinish[T](minimum, b.array.elems.Slice(rows), w, offset, buf)
	return offset, desc
}

// uintColumnFinish finishes the column of unsigned integers of type T, encoding
// per-row deltas of size width if width < sizeof(T).
func uintColumnFinish[T Uint](
	minimum T, values []T, width, offset uint32, buf []byte,
) (uint32, ColumnEncoding) {
	enc := EncodingDefault
	// Align the offset appropriately for elements of type T.
	offset = alignWithZeroes(buf, offset, uint32(unsafe.Sizeof(T(0))))

	// Compare the computed delta width to see if we're able to use an array of
	// lower-width deltas to encode the column.
	if uintptr(width) < unsafe.Sizeof(T(0)) {
		// Regardless of the width, we encode a constant of size T encoding the
		// minimum across all the values.
		dest := makeUnsafeRawSlice[T](unsafe.Pointer(&buf[offset]))
		dest.set(0, minimum)
		offset += uint32(unsafe.Sizeof(T(0)))

		switch width {
		case 0:
			// All the column values are the same and we can elide any deltas at
			// all.
			enc = enc.WithDelta(DeltaEncodingConstant)
			return offset, enc
		case 1:
			enc = enc.WithDelta(DeltaEncodingUint8)
			dest := makeUnsafeRawSlice[uint8](unsafe.Pointer(&buf[offset]))
			reduceUints[T, uint8](minimum, values, dest.Slice(len(values)))
			offset += uint32(len(values))
			return offset, enc
		case align16:
			enc = enc.WithDelta(DeltaEncodingUint16)
			dest := makeUnsafeRawSlice[uint16](unsafe.Pointer(&buf[offset]))
			reduceUints[T, uint16](minimum, values, dest.Slice(len(values)))
			offset += uint32(len(values)) * align16
			return offset, enc
		case align32:
			enc = enc.WithDelta(DeltaEncodingUint32)
			dest := makeUnsafeRawSlice[uint32](unsafe.Pointer(&buf[offset]))
			reduceUints[T, uint32](minimum, values, dest.Slice(len(values)))
			offset += uint32(len(values)) * align32
			return offset, enc
		default:
			panic("unreachable")
		}
	}
	dest := makeUnsafeRawSlice[T](unsafe.Pointer(&buf[offset])).Slice(len(values))
	offset += uint32(copy(dest, values)) * uint32(unsafe.Sizeof(T(0)))
	return offset, enc
}

// WriteDebug implements Encoder.
func (b *UintBuilder[T]) WriteDebug(w io.Writer, rows int) {
	fmt.Fprintf(w, "%s: %d rows", b.dt, rows)
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

// deltaWidth returns the width in bytes of the integer type that can represent
// the provided value.
func deltaWidth(delta uint64) uint32 {
	// TODO(jackson): Consider making this generic; We could compare against
	// unsafe.Sizeof(T(0)) to ensure that we don't overflow T and that the
	// higher width cases get elided at compile time for the smaller width Ts.
	switch {
	case delta == 0:
		return 0
	case delta < (1 << 8):
		return 1
	case delta < (1 << 16):
		return align16
	case delta < (1 << 32):
		return align32
	default:
		return align64
	}
}

func uintsToBinFormatter(f *binfmt.Formatter, rows int, desc ColumnDesc) {
	logicalWidth := desc.DataType.uintWidth()
	elementWidth := int(logicalWidth)
	if desc.Encoding.Delta() != DeltaEncodingNone {
		f.HexBytesln(int(logicalWidth), "%d-bit constant: %d", logicalWidth*8, f.PeekUint(int(logicalWidth)))

		switch desc.Encoding.Delta() {
		case DeltaEncodingConstant:
			// This is just a constant (that was already read/formatted).
			rows = 0
		case DeltaEncodingUint8:
			elementWidth = 1
		case DeltaEncodingUint16:
			elementWidth = align16
		case DeltaEncodingUint32:
			elementWidth = align32
		default:
			panic("unreachable")
		}
	}
	for i := 0; i < rows; i++ {
		f.HexBytesln(elementWidth, "data[%d] = %d", i, f.PeekUint(elementWidth))
	}
}
