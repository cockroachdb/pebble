// Copyright 2024 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package colblk

import (
	"fmt"
	"io"
	"math"
	"math/bits"
	"unsafe"

	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/pebble/internal/base"
	"github.com/cockroachdb/pebble/internal/binfmt"
	"github.com/cockroachdb/pebble/internal/invariants"
)

// presenceEncoding is an enum used for columns that permit values to be absent.
// It encodes whether a column has all values present, all values absent, or
// encodes a presence bitmap indicating which values are present.
type presenceEncoding int8

const (
	// presenceEncodingAllAbsent indicates that all values in the column are
	// absent. A reader does not need to read any further column data.
	presenceEncodingAllAbsent presenceEncoding = 0x00
	// presenceEncodingAllPresent indicates that all values in the column are
	// present. No presence bitmap is encoded, and immediately following the
	// presenceEncoding bit is the column data.
	presenceEncodingAllPresent presenceEncoding = 0x01
	// presenceEncodingSomeAbsent indicates that some but not all values are
	// absent from the column. The column data is prefixed with a presence
	// bitmap that should be read first. The actual column data that follows
	// will only encode as many values as the presence bitmap indicates are
	// present.
	presenceEncodingSomeAbsent presenceEncoding = 0x02
)

func (p presenceEncoding) String() string {
	switch p {
	case presenceEncodingAllAbsent:
		return "AllAbsent"
	case presenceEncodingAllPresent:
		return "AllPresent"
	case presenceEncodingSomeAbsent:
		return "SomeAbsent"
	default:
		return fmt.Sprintf("unknown(%d)", p)
	}
}

// AllPresentDecoder is a Decoder that decodes a data structure that is statically
// known to be prefixed with an all-present presence encoding. It uses the inner
// Decoder to decode the data structure that follows the presence encoding.  If
// the presence encoding is not all-present, decoding panics.
//
// Callers should prefer using AllPresentDecoder when they know that all values
// are present, to avoid the indirection of a runtime lookup of a value's
// presence.
type AllPresentDecoder[V any, D Decoder[V]] struct {
	Decoder D
}

// Assert that AllPresent implements Decoder.
var _ Decoder[RawBytes] = AllPresentDecoder[RawBytes, DecodeFunc[RawBytes]]{Decoder: DecodeRawBytes}

// Decode implements Decoder.
func (a AllPresentDecoder[V, D]) Decode(
	buf []byte, offset uint32, rows int,
) (r V, nextOffset uint32) {
	if enc := presenceEncoding(buf[offset]); enc != presenceEncodingAllPresent {
		panic(base.MarkCorruptionError(errors.Newf("expected all present encoding; found %s at offset %d", enc, offset)))
	}
	return a.Decode(buf, offset+1, rows)
}

// PresenceWithDefault is an Array used for reading a data structure that was
// encoded with potentially some values absent. PresenceWithDefault wraps
// another Array.  When a value is present, it returns the value from the
// wrapped Array.  When a value is absent, it returns configured default value.
//
// Users should prefer using AllPresent when they know that all values are
// present, to avoid the indirection of a runtime lookup of a value's presence.
type PresenceWithDefault[V any, A Array[V]] struct {
	PresenceBitmap
	values       A
	defaultValue V
}

// Assert that PresenceWithDefault implements Array.
var _ Array[uint8] = PresenceWithDefault[uint8, Array[uint8]]{}

// At implements ColumnReader, returning the row'th value of the column.
func (a PresenceWithDefault[V, R]) At(row int) V {
	idx := a.PresenceBitmap.Rank(row)
	if idx == -1 {
		return a.defaultValue
	}
	return a.values.At(idx)
}

// PresenceWithDefaultDecoder is a Decoder that decodes a data structure that
// was prefixed with presence encoding potentially absent values.  It returns a
// PresenceWithDefault that implements Array[V], returning the default value for
// any absent rows.
//
// If a caller statically knows that a data structure's presence encoding
// encodes that all values are present, it can use AllPresent instead to avoid
// the runtime overhead when accessing values.
type PresenceWithDefaultDecoder[V any, A Array[V]] struct {
	// DefaultValue is the value to return from PresenceWithDefault.At when a
	// value is absent.
	DefaultValue V
	// ArrayDecoder is the Decoder used to decode the array of present values.
	ArrayDecoder Decoder[A]
}

// Assert that PresenceWithDefaultDecoder implements Decoder.
var _ Decoder[PresenceWithDefault[[]byte, RawBytes]] = PresenceWithDefaultDecoder[[]byte, RawBytes]{}

// Decode implements Decoder
func (a PresenceWithDefaultDecoder[V, A]) Decode(
	buf []byte, offset uint32, rows int,
) (p PresenceWithDefault[V, A], nextOffset uint32) {
	p.defaultValue = a.DefaultValue
	p.PresenceBitmap, offset = DecodePresence(buf, offset, rows)
	if !p.PresenceBitmap.AllAbsent() {
		p.values, offset = a.ArrayDecoder.Decode(buf, offset, p.PresenceBitmap.PresentCount())
	}
	return p, offset
}

// DecodePresence decodes the presence encoding from buf at the provided offset.
// It returns a PresenceBitmap that may be used to determine which values are
// present and which are absent. It returns an endOffset pointing to the first
// byte after the presence encoding.
//
// An example of reading all the values in a RawBytes column with a presence
// bitmap, interpreting absence as the empty byte slice.
//
//		presence, offset := DecodePresence(buf, offset, rows)
//		rawByteValues := MakeRawBytes(presence.PresentCount(rows), buf, offset)
//		for i := 0; i < rows; i++ {
//	      var value []byte
//		  if idx := presence.Rank(i); idx != -1 {
//		    value = rawByteValues.At(idx)
//		  }
//		  // process value
//		}
//
// Users should generally prefer using PresenceWithDefaultDecoder or
// AllPresentDecoder for a more ergonomic interface when feasible.
func DecodePresence(
	buf []byte, offset uint32, rows int,
) (presence PresenceBitmap, endOffset uint32) {
	// The first byte is an enum indicating how presence is encoded.
	switch presenceEncoding(buf[offset]) {
	case presenceEncodingAllAbsent:
		return allAbsentPresenceBitmap(rows), offset + 1
	case presenceEncodingAllPresent:
		return allPresentPresenceBitmap(rows), offset + 1
	case presenceEncodingSomeAbsent:
		offset = align(offset+1, presenceBitmapAlign)
		presence, endOffset = decodePresenceBitmap(buf, offset, rows)
		return presence, endOffset
	default:
		panic(base.MarkCorruptionError(errors.Newf("unknown presence encoding: %d", buf[offset])))
	}
}

// Assert that DecodePresence implements DecodeFunc.
var _ DecodeFunc[PresenceBitmap] = DecodePresence

func presenceToBinFormatter(f *binfmt.Formatter, rows int) (remainingRows int) {
	enc := presenceEncoding(f.Data()[f.Offset()])
	f.HexBytesln(1, "presence encoding: %s", enc)
	switch enc {
	case presenceEncodingAllAbsent:
		return 0
	case presenceEncodingAllPresent:
		return rows
	case presenceEncodingSomeAbsent:
		return presenceBitmapToBinFormatter(f, rows)
	default:
		panic("unreachable")
	}
}

// AllPresentWriter implements ColumnWriter, wrapping another ColumnWriter and
// encoding that all values are present.
type AllPresentWriter[W ColumnWriter] struct {
	Writer W
}

// Assert that AllPresentWriter implements ColumnWriter.
var _ ColumnWriter = AllPresentWriter[ColumnWriter]{}

// NumColumns implements ColumnWriter.
func (a AllPresentWriter[W]) NumColumns() int { return a.Writer.NumColumns() }

// DataType implements ColumnWriter.
func (a AllPresentWriter[W]) DataType(col int) DataType { return a.Writer.DataType(col) }

// Reset implements ColumnWriter.
func (a AllPresentWriter[W]) Reset() { a.Writer.Reset() }

// Size implements ColumnWriter.
func (a AllPresentWriter[W]) Size(rows int, offset uint32) (endOffset uint32) {
	return a.Writer.Size(rows, offset+1)
}

// Finish implements ColumnWriter.
func (a AllPresentWriter[W]) Finish(col, rows int, offset uint32, buf []byte) (endOffset uint32) {
	buf[offset] = byte(presenceEncodingAllPresent)
	return a.Writer.Finish(col, rows, offset+1, buf)
}

// WriteDebug implements ColumnWriter.
func (a AllPresentWriter[W]) WriteDebug(w io.Writer, rows int) { a.Writer.WriteDebug(w, rows) }

// DefaultAbsentWriter implements ColumnWriter, wrapping another ColumnWriter
// and handling cheaply encoding absent values using a presence bitmap. Rows
// default to absent unless explicitly set through Present.
//
// Columns written with DefaultAbsentWriter prefix the column data with a single
// byte indicating how the data is encoded and possibly a presence bitmap if the
// column contains both absent and present values.  Readers of a column written
// with DefaultAbsentWriter should use DecodePresence to retrieve a
// PresenceBitmap for the column.
type DefaultAbsentWriter[W ColumnWriter] struct {
	Writer W

	bitmap       presenceBitmapBuilder
	presentCount int
}

// Assert that DefaultAbsentWriter implements ColumnWriter.
var _ ColumnWriter = (*DefaultAbsentWriter[ColumnWriter])(nil)

// Present marks the value at the given row as present and returns the index to
// use for its value within the underlying value array:
//
//	cw, idx = cw.Present(idx)
//	cw.Set(idx, value)
func (a *DefaultAbsentWriter[W]) Present(row int) (W, int) {
	a.bitmap.Set(row, true)
	a.presentCount++
	if invariants.Enabled && a.bitmap.PresentCount(row+1) != a.presentCount {
		panic(errors.AssertionFailedf("inconsistent present count: %d vs %d", a.bitmap.PresentCount(row+1), a.presentCount))
	}
	return a.Writer, a.presentCount - 1
}

// NumColumns implements ColumnWriter.
func (a *DefaultAbsentWriter[W]) NumColumns() int { return a.Writer.NumColumns() }

// DataType implements ColumnWriter.
func (a *DefaultAbsentWriter[W]) DataType(col int) DataType { return a.Writer.DataType(col) }

// Reset implements ColumnWriter, resetting the column (and its underlying
// ColumnWriter) to its empty state.
func (a *DefaultAbsentWriter[W]) Reset() {
	clear(a.bitmap.words)
	a.bitmap.words = a.bitmap.words[:0]
	a.presentCount = 0
	a.Writer.Reset()
}

// Size implements ColumnWriter.
func (a *DefaultAbsentWriter[W]) Size(rows int, offset uint32) (endOffset uint32) {
	offset++ // the presenceEncoding enum byte

	// We can use n.presentCount because the contract of the Size method
	// requires rows to be >= the number of rows set. Assert that the bitmap
	// agrees with the aggregate presentCount.
	if invariants.Enabled && a.bitmap.PresentCount(rows) != a.presentCount {
		panic(errors.AssertionFailedf("inconsistent present count: %d vs %d", a.bitmap.PresentCount(rows), a.presentCount))
	}
	switch a.presentCount {
	case 0:
		// All absent; doesn't need to be encoded into column data.
		return offset
	case rows:
		// All values are present.
		return a.Writer.Size(rows, offset)
	default:
		// Mix of present and absent values. We need a bitmap to encode which
		// values are absent.
		return a.Writer.Size(a.presentCount, a.bitmap.Size(rows, offset))
	}
}

// Finish implements ColumnWriter.
func (a *DefaultAbsentWriter[W]) Finish(
	col, rows int, offset uint32, buf []byte,
) (endOffset uint32) {
	// NB: The struct field n.presentCount may have been computed for a number
	// of rows higher than [rows]. Use the bitmap's lookup table to compute the
	// number of present rows up to [rows].
	switch present := a.bitmap.PresentCount(rows); present {
	case 0:
		buf[offset] = byte(presenceEncodingAllAbsent)
		return offset + 1
	case rows:
		buf[offset] = byte(presenceEncodingAllPresent)
		return a.Writer.Finish(col, present, offset+1, buf)
	default:
		buf[offset] = byte(presenceEncodingSomeAbsent)
		offset = a.bitmap.Finish(rows, offset+1, buf)
		offset = a.Writer.Finish(col, present, offset, buf)
		return offset
	}
}

// WriteDebug implements ColumnWriter.
func (a *DefaultAbsentWriter[W]) WriteDebug(w io.Writer, rows int) {
	fmt.Fprintf(w, "defaultAbsent[%d present](", a.presentCount)
	a.Writer.WriteDebug(w, a.presentCount)
	fmt.Fprintf(w, ")")
}

const presenceBitmapAlign = align32
const presenceBitmapAlignShift = align32Shift
const presenceBitmapWordSize = 32
const presenceBitmapHalfWordSize = presenceBitmapWordSize / 2
const presenceBitmapHalfWordSizeShift = 4
const presenceBitmapHalfWordMask = presenceBitmapHalfWordSize - 1
const maxPresenceBitmapLogicalRows = math.MaxUint16

// allPresentSentinelPtr is a sentinel pointer value used to indicate that the
// presence bitmap was completely full (i.e. all values for the column were
// present).
var allPresentSentinel = []presenceBitmapWord{presenceBitmapWord(0xFFFFFFFF)}
var allPresentSentinelPtr = unsafe.Pointer(unsafe.SliceData(allPresentSentinel))

// PresenceBitmap provides a bitmap for recording the presence of a column
// value. If the i'th bit of the presence bitmap for a column is 1, a value is
// stored for the column at that row index. In addition to presence testing, the
// presence bitmap provides a fast Rank(i) operation by interleaving a lookup
// table into the bitmap. The rank is the number of present values present in
// bitmap[0,i), or equivalently, the index of the i'th value among present
// values.
//
// The meaning of an absent value is context-specific. Columns encode a
// PresenceBitmap assigning a "zero value" to the absence of a value, making the
// PresenceBitmap a performance optimization to reduce the size of a column with
// many zero values. Users that require storing null values (as distinct from a
// data type's zero value), may additionally use a presence bitmap to encode
// nulls.
//
// The bitmap is organized as an array of 32-bit words where the bitmap is
// stored in the low 16-bits of every 32-bit word and the lookup table is stored
// in the high bits.
//
//	 bits    sum    bits    sum     bits    sum     bits    sum
//	+-------+------+-------+-------+-------+-------+-------+-------+
//	| 0-15  | 0    | 16-31 | 0-15  | 32-47 | 0-31  | 48-64 | 0-63  |
//	+-------+------+-------+-------+-------+-------+-------+-------+
//
// For example, consider the following 64-bits of data:
//
//	1110011111011111 1101111011110011 1111111111111111 1111110000111111
//
// The logical bits are split at 16-bit boundaries
//
//	       bits             sum
//	0-15:  1110011111011111 0
//	16-31: 1101111011110011 13
//	32-47: 1111111111111111 25
//	48-63: 1111110000011111 41
//
// The lookup table (the sum column) is interleaved with the bitmap in the high
// 16 bits. To answer a Rank query, we find the word containing the bit (i/16),
// count the number of bits that are set in the low 16 bits of the word before
// the bit we're interested in, and add the sum from the high 16 bits in the
// word.
//
// The number of bits used for each lookup table entry (16-bits) limits the size
// of a bitmap to 64K bits which limits the number of rows in a block to 64K.
// The lookup table imposes an additional bit of overhead per bit in the bitmap
// (thus 2-bits per row).
type PresenceBitmap struct {
	rows int
	data UnsafeRawSlice[presenceBitmapWord]
}

func allPresentPresenceBitmap(rows int) PresenceBitmap {
	return PresenceBitmap{
		rows: rows,
		data: makeUnsafeRawSlice[presenceBitmapWord](allPresentSentinelPtr),
	}
}

func allAbsentPresenceBitmap(rows int) PresenceBitmap {
	return PresenceBitmap{
		rows: rows,
		data: makeUnsafeRawSlice[presenceBitmapWord](nil),
	}
}

func decodePresenceBitmap(buf []byte, offset uint32, rows int) (PresenceBitmap, uint32) {
	offset = align(offset, presenceBitmapAlign)
	return PresenceBitmap{
		rows: rows,
		data: makeUnsafeRawSlice[presenceBitmapWord](unsafe.Pointer(&buf[offset])),
	}, presenceBitmapSize(rows, offset)
}

// AllPresent returns true if the bitmap is empty and indicates that all of the
// column values are present. It is safe to call Null and Rank on such a bitmap,
// but faster to specialize code to not invoke them at all.
func (b PresenceBitmap) AllPresent() bool {
	return b.data.ptr == allPresentSentinelPtr
}

// AllAbsent returns true if the bitmap indicates that all of the column values
// are absent. It is safe to call Null and Rank on a full bitmap, but faster to
// specialize code to not invoke them at all.
func (b PresenceBitmap) AllAbsent() bool {
	return b.data.ptr == nil
}

// Present returns true if the bit at position i is set, indicating a value is
// present, and false otherwise.
func (b PresenceBitmap) Present(i int) bool {
	switch b.data.ptr {
	case nil:
		return false
	case allPresentSentinelPtr:
		return true
	default:
		return b.data.At(presenceWordIndex(i)).isPresent(i)
	}
}

// Rank looks up the existence of the i'th row. If the i'th row's value is
// present, it returns the index of the value in the value array. Rank returns
// -1 if the i'th value is absent. If all values are present, Rank(i) == i.
func (b PresenceBitmap) Rank(i int) int {
	switch b.data.ptr {
	case nil:
		return -1
	case allPresentSentinelPtr:
		return i
	default:
		return b.data.At(presenceWordIndex(i)).rank(i)
	}
}

// PresentCount returns the number of values that are recorded as present.
func (b PresenceBitmap) PresentCount() int {
	if b.data.ptr == nil {
		return 0
	}
	if b.data.ptr == allPresentSentinelPtr {
		return b.rows
	}
	return b.data.At(presenceWordIndex(b.rows - 1)).sumN(b.rows)
}

func presenceBitmapToBinFormatter(f *binfmt.Formatter, rows int) (remainingRows int) {
	if off := align(f.Offset(), presenceBitmapAlign); off != f.Offset() {
		f.HexBytesln(off-f.Offset(), "padding to align to 32-bit")
	}
	bm, _ := decodePresenceBitmap(f.Data(), uint32(f.Offset()), rows)
	f.CommentLine("presence bitmap (%d present out of %d)", bm.PresentCount(), rows)
	for wi := 0; wi < presenceWordCount(rows); wi++ {
		word := bm.data.At(wi)
		f.Line(4).Binary(1).Append(" ").Binary(1).Append(" ").HexBytes(2).
			Done("word %2d: %016b (sum %d)", wi, uint16(word), int(word.sum()))
	}
	return bm.PresentCount()
}

// presenceBitmapBuilder builds a PresenceBitmap.
type presenceBitmapBuilder struct {
	words []presenceBitmapWord
}

// Set sets the bit at position i if v is true and clears the bit at position i
// otherwise. Bits must be set in order and it is invalid to set a bit twice.
func (b *presenceBitmapBuilder) Set(i int, v bool) {
	j := presenceWordIndex(i)
	// Grow the bitmap to the required size, populating the lookup table's high
	// bits as we go.
	if len(b.words) == 0 {
		b.words = append(b.words, 0)
	}
	for len(b.words) <= j {
		b.words = append(b.words, b.words[len(b.words)-1].totalSum()<<presenceBitmapHalfWordSize)
	}
	// Set requires that bits be set in order and a bit cannot be set twice. In
	// invariants builds ensure that the bit is not already set, which would
	// indicate a violation of the contract.
	if invariants.Enabled && b.words[j].isPresent(i) {
		panic(errors.AssertionFailedf("bit %d already set", i))
	}
	if v {
		// Set the bit within this word.
		b.words[j] |= presenceBitmapWord(1) << uint(i&presenceBitmapHalfWordMask)
	}
}

// PresentCount returns the number of present values in the bitmap among the
// first n rows.
func (b presenceBitmapBuilder) PresentCount(n int) int {
	if len(b.words) == 0 {
		return 0
	}
	wordIndex := presenceWordIndex(n - 1)
	if wordIndex >= len(b.words) {
		return int(b.words[len(b.words)-1].totalSum())
	}
	return b.words[wordIndex].sumN(n)
}

// verify validates the integrity of the interleaved lookup table.
func (b presenceBitmapBuilder) verify() {
	if len(b.words) > 0 {
		if b.words[0].sum() != 0 {
			panic(errors.AssertionFailedf("presenceBitmapBuilder: 0'th word should have sum=0: %s", b.words[0]))
		}
		for i, sum := 1, presenceBitmapWord(0); i < len(b.words); i++ {
			sum += presenceBitmapWord(bits.OnesCount16(uint16(b.words[i-1])))
			if b.words[i].sum() != sum {
				panic(errors.AssertionFailedf("presenceBitmapBuilder: %d'th word: %s expected sum=%d",
					i, b.words[i], sum))
			}
		}
	}
}

// Size returns the size of the presence bitmap in bytes, when written at the
// provided offset. The serialized bitmap is stored aligned at a 32-bit offset,
// so the space required for serializing the bitmap is dependent on the current
// offset within a buffer.
//
// Size does not have special casing for full or empty bitmaps, and it's the
// responsibility of the caller to special case those cases if necessary.
func (b presenceBitmapBuilder) Size(rows int, offset uint32) (endOffset uint32) {
	return presenceBitmapSize(rows, offset)
}

// presenceBitmapSize computes the size of a presence bitmap (not including the
// presence byte).
func presenceBitmapSize(rows int, offset uint32) uint32 {
	offset = align(offset, presenceBitmapAlign)
	numWords := (rows + presenceBitmapHalfWordSize - 1) >> presenceBitmapHalfWordSizeShift
	return offset + uint32(numWords)<<presenceBitmapAlignShift
}

// Finish serializes the NULL bitmap into the provided buffer at the provided
// offset. The buffer must be at least Size(offset) bytes in length.
func (b presenceBitmapBuilder) Finish(rows int, offset uint32, buf []byte) uint32 {
	// A presence bitmap's sum table is stored in the high 16 bits of each word,
	// which limits the maximum number of expressible rows to 2^16-1. Ensure
	// that we never attempt to build a presence bitmap with more logical rows
	// than the format is capable of representing.
	if rows > maxPresenceBitmapLogicalRows {
		panic(errors.AssertionFailedf("presence bitmap rows %d exceeds maximum rows %d", rows, maxPresenceBitmapLogicalRows))
	}

	// Grow the bitmap to the required size if it's not already. Callers are
	// permitted to omit calls to Set for absent values, so the builder may have
	// fewer words than [rows]. Populate the lookup table's high bits as we go.
	// j is the index of the word containing the rows-1'th bit.
	n := presenceWordCount(rows)
	if len(b.words) < n {
		if len(b.words) == 0 {
			b.words = append(b.words, 0)
		}
		for len(b.words) < n {
			b.words = append(b.words, b.words[len(b.words)-1].totalSum()<<presenceBitmapHalfWordSize)
		}
	}
	if invariants.Enabled {
		b.verify()
	}
	offset = alignWithZeroes(buf, offset, presenceBitmapAlign)
	dest := makeUnsafeRawSlice[presenceBitmapWord](unsafe.Pointer(&buf[offset]))
	destSlice := dest.Slice(n)

	// Copy all but the last word.
	copy(destSlice, b.words[:max(0, n-1)])
	// The caller may have Set bits in the bitmap portion of the last word that
	// are beyond the first [rows] bits. Write this word with those bits
	// cleared.
	destSlice[n-1] = b.words[n-1].withTruncatedBitmap(rows - (n-1)*presenceBitmapHalfWordSize)
	offset += uint32(n) << presenceBitmapAlignShift
	return offset
}

// presenceBitmapWord is a word within the PresenceBitmap. Its low 16 bits form
// a bitmap indicating which values are present, and its high 16 bits form a
// lookup table for fast rank queries, encoding the sum of present values in all
// prior words.
type presenceBitmapWord uint32

func (w presenceBitmapWord) String() string {
	return fmt.Sprintf("%032b (sum=%d)", uint32(w), w.sum())
}

// sum returns the value encoded within the sum bits (high 16 bits) of the word.
// The sum represents the count of present values in the bitmap in all prior
// words.
func (w presenceBitmapWord) sum() presenceBitmapWord {
	return w >> presenceBitmapHalfWordSize
}

// totalSum returns the count of present values in the bitmap in this word and
// all prior words. It adds the word's lookup table sum bits with the count of
// bits set in the low 16 bits.
func (w presenceBitmapWord) totalSum() presenceBitmapWord {
	return w.sum() + presenceBitmapWord(bits.OnesCount16(uint16(w)))
}

// sumN returns the count of present values among the first n bits of the
// bitmap. The receiver must be the last word of a bitmap with n rows.
func (w presenceBitmapWord) sumN(n int) int {
	bit := presenceBitmapWord(1) << (uint((n-1)&presenceBitmapHalfWordMask) + 1)
	return int(w.sum()) + bits.OnesCount16(uint16(w&(bit-1)))
}

// isPresent returns true if the i%16'th bit is set in the word.
func (w presenceBitmapWord) isPresent(i int) bool {
	bit := presenceBitmapWord(1) << uint(i&presenceBitmapHalfWordMask)
	return (w & bit) != 0
}

// rank looks up the existence of the i'th row. If the i'th row's value is
// present, it returns the index of the value in the value array. Rank returns
// -1 if the i'th value is absent.
func (w presenceBitmapWord) rank(i int) int {
	bit := presenceBitmapWord(1) << uint(i&presenceBitmapHalfWordMask)
	if (w & bit) == 0 {
		// Not present.
		return -1
	}
	// The rank of the i'th bit is the total in the lookup table sum (stored in
	// the high 16 bits), plus the number of ones in the low 16 bits that
	// precede the bit.
	return int(w.sum()) + bits.OnesCount16(uint16(w&(bit-1)))
}

// withTruncatedBitmap returns the receiver with any bitmap bits beyond the
// first n cleared within the word's bitmap. The sum bits are preserved. n must
// be less than or equal to 16.
func (w presenceBitmapWord) withTruncatedBitmap(n int) presenceBitmapWord {
	if invariants.Enabled && n > presenceBitmapHalfWordSize {
		panic(errors.AssertionFailedf("truncating to more than %d bits: %d", presenceBitmapHalfWordSize, n))
	}
	// Build a mask that preserves the sum table bits and the first n bits of
	// the word's bitmap bits.
	mask := presenceBitmapWord((1<<presenceBitmapHalfWordSize - 1) << presenceBitmapHalfWordSize)
	mask |= presenceBitmapWord((1 << n) - 1)
	return w & mask
}

// presenceWordIndex returns the index of the word containing the i'th bit.
func presenceWordIndex(i int) int {
	return i >> presenceBitmapHalfWordSizeShift
}

// presenceWordCount returns the number of words required to store a presence
// bitmap for n rows.
func presenceWordCount(n int) int {
	return (n + presenceBitmapHalfWordSize - 1) >> presenceBitmapHalfWordSizeShift
}
