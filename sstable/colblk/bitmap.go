// Copyright 2024 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package colblk

import (
	"fmt"
	"io"
	"math"
	"math/bits"
	"strings"

	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/pebble/internal/binfmt"
	"github.com/cockroachdb/pebble/internal/invariants"
	"github.com/cockroachdb/pebble/internal/treeprinter"
)

// Bitmap is a bitmap structure built on a []uint64. A bitmap utilizes ~1
// physical bit/logical bit (~0.125 bytes/row). The bitmap is encoded into an
// 8-byte aligned array of 64-bit words which is (nRows+63)/64 words in length.
//
// A summary bitmap is stored after the primary bitmap in which each bit in the
// summary bitmap corresponds to 1 word in the primary bitmap. A bit is set in
// the summary bitmap if the corresponding word in the primary bitmap is
// non-zero. The summary bitmap accelerates predecessor and successor
// operations.
type Bitmap struct {
	// data contains the bitmap data, according to defaultBitmapEncoding, or it
	// is nil if the bitmap is all zeros.
	data     unsafeUint64Decoder
	bitCount int
}

// Assert that Bitmap implements Array[bool].
var _ Array[bool] = Bitmap{}

// DecodeBitmap decodes the structure of a Bitmap and returns a Bitmap that
// reads from b supporting bitCount logical bits. No bounds checking is
// performed, so the caller must guarantee the bitmap is appropriately sized and
// the provided bitCount correctly identifies the number of bits in the bitmap.
func DecodeBitmap(b []byte, off uint32, bitCount int) (bitmap Bitmap, endOffset uint32) {
	encoding := bitmapEncoding(b[off])
	off++
	if encoding == zeroBitmapEncoding {
		return Bitmap{bitCount: bitCount}, off
	}
	off = align(off, align64)
	sz := bitmapRequiredSize(bitCount)
	if len(b) < int(off)+sz {
		panic(errors.AssertionFailedf("bitmap of %d bits requires at least %d bytes; provided with %d-byte slice",
			bitCount, bitmapRequiredSize(bitCount), len(b[off:])))
	}
	return Bitmap{
		data:     makeUnsafeUint64Decoder(b[off:], sz>>align64Shift),
		bitCount: bitCount,
	}, off + uint32(sz)
}

// Assert that DecodeBitmap implements DecodeFunc.
var _ DecodeFunc[Bitmap] = DecodeBitmap

// At returns true if the bit at position i is set and false otherwise.
//
//gcassert:inline
func (b Bitmap) At(i int) bool {
	if b.data.ptr == nil {
		// zero bitmap case.
		return false
	}
	invariants.CheckBounds(i, b.bitCount)
	val := b.data.At(int(uint(i) >> 6)) // aka At(i/64)
	return val&(1<<(uint(i)&63)) != 0
}

// SeekSetBitGE returns the next bit greater than or equal to i set in the bitmap.
// The i parameter must be ≥ 0. Returns the number of bits
// represented by the bitmap if no next bit is set (or if i >= bitCount).
func (b Bitmap) SeekSetBitGE(i int) int {
	if b.data.ptr == nil || i >= b.bitCount {
		// Zero bitmap case.
		return b.bitCount
	}

	wordIdx := i >> 6 // i/64
	// Fast path for common case of reasonably dense bitmaps; if the there's a
	// bit ≥ i set in the same word, return it.
	if next := nextBitInWord(b.data.At(wordIdx), uint(i)&63); next < 64 {
		return wordIdx<<6 + next
	}

	// Consult summary structure to find the next word with a set bit. The word
	// we just checked (wordIdx) is represented by the wordIdx%64'th bit in the
	// wordIdx/64'th summary word. We want to know if any of the other later
	// words that are summarized together have a set bit. We call [nextInWord]
	// on the summary word to get the index of which word has a set bit, if any.
	summaryTableOffset, summaryTableEnd := b.summaryTableBounds()
	summaryWordIdx := summaryTableOffset + wordIdx>>6
	if invariants.Enabled {
		sz := bitmapRequiredSize(b.bitCount)
		invariants.CheckBounds(summaryTableEnd-1, sz)
	}
	summaryNext := nextBitInWord(b.data.At(summaryWordIdx), uint(wordIdx%64)+1)
	// If [summaryNext] == 64, then there are no set bits in any of the earlier
	// words represented by the summary word at [summaryWordIdx]. In that case,
	// we need to keep scanning the summary table forwards.
	if summaryNext == 64 {
		for summaryWordIdx++; ; summaryWordIdx++ {
			// When we fall off the end of the summary table, we've determined
			// there are no set bits after i across the entirety of the bitmap.
			if summaryWordIdx >= summaryTableEnd {
				return b.bitCount
			}
			if summaryWord := b.data.At(summaryWordIdx); summaryWord != 0 {
				summaryNext = bits.TrailingZeros64(summaryWord)
				break
			}
		}
	}
	// The summary word index and the summaryNext together tell us which word
	// has a set bit. The number of leading zeros in the word itself tell us
	// which bit is set.
	wordIdx = ((summaryWordIdx - summaryTableOffset) << 6) + summaryNext
	return (wordIdx << 6) + bits.TrailingZeros64(b.data.At(wordIdx))
}

// SeekSetBitLE returns the previous bit less than or equal to i set in the
// bitmap. The i parameter must be in [0, bitCount). Returns -1 if no previous
// bit is set.
func (b Bitmap) SeekSetBitLE(i int) int {
	if b.data.ptr == nil {
		// Zero bitmap case.
		return -1
	}
	wordIdx := i >> 6 // i/64
	// Fast path for common case of reasonably dense bitmaps; if the there's a
	// bit ≤ i set in the same word, return it.
	if prev := prevBitInWord(b.data.At(wordIdx), uint(i)&63); prev >= 0 {
		return (wordIdx << 6) + prev
	}

	// Consult summary structure to find the next word with a set bit. The word
	// we just checked (wordIdx) is represented by the wordIdx%64'th bit in the
	// wordIdx/64'th summary word. We want to know if any of other earlier words
	// that are summarized together have a set bit. We call [prevInWord] on the
	// summary word to get the index of which word has a set bit, if any.
	summaryTableOffset, _ := b.summaryTableBounds()
	summaryWordIdx := summaryTableOffset + wordIdx>>6
	summaryPrev := prevBitInWord(b.data.At(summaryWordIdx), uint(wordIdx%64)-1)
	// If [summaryPrev] is negative, then there are no set bits in any of the
	// earlier words represented by the summary word at [summaryWordIdx]. In
	// that case, we need to keep scanning the summary table backwards.
	if summaryPrev < 0 {
		for summaryWordIdx--; ; summaryWordIdx-- {
			// When we fall below the beginning of the summary table, we've
			// determined there are no set bits before i across the entirety of
			// the bitmap.
			if summaryWordIdx < summaryTableOffset {
				return -1
			}
			if summaryWord := b.data.At(summaryWordIdx); summaryWord != 0 {
				summaryPrev = 63 - bits.LeadingZeros64(summaryWord)
				break
			}
		}
	}
	// The summary word index and the summary prev together tell us which word
	// has a set bit. The number of trailing zeros in the word itself tell us
	// which bit is set.
	wordIdx = ((summaryWordIdx - summaryTableOffset) << 6) + summaryPrev
	return (wordIdx << 6) + 63 - bits.LeadingZeros64(b.data.At(wordIdx))
}

// SeekUnsetBitGE returns the next bit greater than or equal to i that is unset
// in the bitmap. The i parameter must be in [0, bitCount). Returns the number
// of bits represented by the bitmap if no next bit is unset.
func (b Bitmap) SeekUnsetBitGE(i int) int {
	invariants.CheckBounds(i, b.bitCount)
	if b.data.ptr == nil {
		// Zero bitmap case.
		return i
	}
	wordIdx := i >> 6 // i/64
	// If the there's a bit ≥ i unset in the same word, return it.
	if next := nextBitInWord(^b.data.At(wordIdx), uint(i)&63); next < 64 {
		return wordIdx<<6 + next
	}
	numWords := (b.bitCount + 63) >> 6
	var word uint64
	for wordIdx++; ; wordIdx++ {
		if wordIdx >= numWords {
			return b.bitCount
		}
		word = b.data.At(wordIdx)
		if word != math.MaxUint64 {
			break
		}
	}
	return wordIdx<<6 + bits.TrailingZeros64(^word)
}

// SeekUnsetBitLE returns the previous bit less than or equal to i set in the
// bitmap. The i parameter must be in [0, bitCount). Returns -1 if no previous
// bit is unset.
func (b Bitmap) SeekUnsetBitLE(i int) int {
	invariants.CheckBounds(i, b.bitCount)
	if b.data.ptr == nil {
		// Zero bitmap case.
		return i
	}

	wordIdx := i >> 6 // i/64
	// If there's a bit ≤ i unset in the same word, return it.
	if prev := prevBitInWord(^b.data.At(wordIdx), uint(i)&63); prev >= 0 {
		return (wordIdx << 6) + prev
	}
	var word uint64
	for wordIdx--; ; wordIdx-- {
		if wordIdx < 0 {
			return -1
		}
		word = b.data.At(wordIdx)
		if word != math.MaxUint64 {
			break
		}
	}
	return (wordIdx << 6) + 63 - bits.LeadingZeros64(^word)
}

// summaryTableBounds returns the indexes of the bitmap words containing the
// summary table. The summary table's words lie within [startOffset, endOffset).
func (b Bitmap) summaryTableBounds() (startOffset, endOffset int) {
	startOffset = (b.bitCount + 63) >> 6
	endOffset = startOffset + (startOffset+63)>>6
	return startOffset, endOffset
}

// String returns a string representation of the entire bitmap.
func (b Bitmap) String() string {
	var sb strings.Builder
	for w := 0; w < (b.bitCount+63)/64; w++ {
		fmt.Fprintf(&sb, "%064b", b.data.At(w))
	}
	return sb.String()
}

// BitmapBuilder constructs a Bitmap. Bits default to false.
type BitmapBuilder struct {
	words []uint64
	// minNonZeroRowCount is the row count at which the bitmap should begin to
	// use the defaultBitmapEncoding (as opposed to the zeroBitmapEncoding).
	// It's updated on the first call to Set and defaults to zero.
	minNonZeroRowCount int
}

type bitmapEncoding uint8

const (
	// defaultBitmapEncoding encodes the bitmap using ⌈n/64⌉ words followed by
	// ⌈⌈n/64⌉/64⌉ summary words.
	defaultBitmapEncoding bitmapEncoding = iota
	// zeroBitmapEncoding is used for the special case when the bitmap is empty.
	zeroBitmapEncoding
)

// Assert that BitmapBuilder implements ColumnWriter.
var _ ColumnWriter = (*BitmapBuilder)(nil)

// bitmapRequiredSize returns the size of an encoded bitmap in bytes, using the
// defaultBitmapEncoding.
func bitmapRequiredSize(total int) int {
	nWords := (total + 63) >> 6          // divide by 64
	nSummaryWords := (nWords + 63) >> 6  // divide by 64
	return (nWords + nSummaryWords) << 3 // multiply by 8
}

// Set sets the bit at position i to true.
func (b *BitmapBuilder) Set(i int) {
	// Update minNonZeroRowCount if necessary. This is used to determine whether
	// the bitmap should be encoded using the all-zeros encoding.
	if b.isZero(i + 1) {
		b.minNonZeroRowCount = i + 1
	}
	w := i >> 6 // divide by 64
	for len(b.words) <= w {
		// We append zeros because if b.words has additional capacity, it has
		// not been zeroed.
		b.words = append(b.words, 0)
	}
	b.words[w] |= 1 << uint(i&63)
}

// isZero returns true if no bits are set and Invert was not called.
//
//gcassert:inline
func (b *BitmapBuilder) isZero(rows int) bool {
	return b.minNonZeroRowCount == 0 || rows < b.minNonZeroRowCount
}

// Reset resets the bitmap to the empty state.
func (b *BitmapBuilder) Reset() {
	if invariants.Sometimes(50) {
		// Sometimes trash the bitmap with all ones to catch bugs that assume
		// b.words is zeroed.
		for i := 0; i < len(b.words); i++ {
			b.words[i] = ^uint64(0)
		}
	}

	// NB: We don't zero the contents of b.words. When the BitmapBuilder reuses
	// b.words, it must ensure it zeroes the contents as necessary.
	b.words = b.words[:0]
	b.minNonZeroRowCount = 0
}

// NumColumns implements the ColumnWriter interface.
func (b *BitmapBuilder) NumColumns() int { return 1 }

// DataType implements the ColumnWriter interface.
func (b *BitmapBuilder) DataType(int) DataType { return DataTypeBool }

// Size implements the ColumnWriter interface.
func (b *BitmapBuilder) Size(rows int, offset uint32) uint32 {
	// First byte will be the encoding type.
	offset++
	if b.isZero(rows) {
		return offset
	}
	offset = align(offset, align64)
	return offset + uint32(bitmapRequiredSize(rows))
}

// InvertedSize returns the size of the encoded bitmap, assuming Invert will be called.
func (b *BitmapBuilder) InvertedSize(rows int, offset uint32) uint32 {
	// First byte will be the encoding type.
	offset++
	// An inverted bitmap will never use all-zeros encoding (even if it happens to
	// be all zero).
	offset = align(offset, align64)
	return offset + uint32(bitmapRequiredSize(rows))
}

// Invert inverts the bitmap, setting all bits that are not set and clearing all
// bits that are set. If the bitmap's tail is sparse and is not large enough to
// represent nRows rows, it's first materialized.
//
// Note that Invert can affect the Size of the bitmap. Use InvertedSize() if you
// intend to invert the bitmap before finishing.
func (b *BitmapBuilder) Invert(nRows int) {
	// Inverted bitmaps never use the all-zero encoding, so we set
	// rowCountIncludingFirstSetBit to 1 so that as long as the bitmap is
	// finished encoding any rows at all, it uses the default encoding.
	b.minNonZeroRowCount = 1
	// If the tail of b is sparse, fill in zeroes before inverting.
	nBitmapWords := (nRows + 63) >> 6
	for len(b.words) < nBitmapWords {
		// We append zeros because if b.words has additional capacity, it has
		// not been zeroed.
		b.words = append(b.words, 0)
	}
	b.words = b.words[:nBitmapWords]
	for i := range b.words {
		b.words[i] = ^b.words[i]
	}
}

// Finish finalizes the bitmap, computing the per-word summary bitmap and
// writing the resulting data to buf at offset.
func (b *BitmapBuilder) Finish(col, nRows int, offset uint32, buf []byte) uint32 {
	if b.isZero(nRows) {
		buf[offset] = byte(zeroBitmapEncoding)
		return offset + 1
	}
	buf[offset] = byte(defaultBitmapEncoding)
	offset++
	offset = alignWithZeroes(buf, offset, align64)

	nBitmapWords := (nRows + 63) >> 6
	// Truncate the bitmap to the number of words required to represent nRows.
	// The caller may have written more bits than nRows and no longer cares to
	// write them out.
	if len(b.words) > nBitmapWords {
		b.words = b.words[:nBitmapWords]
	}
	// Ensure the last word of the bitmap does not contain any set bits beyond
	// the last row. This is not just for determinism but also to ensure that
	// the summary bitmap is correct (which is necessary for Bitmap.SeekSetBitGE
	// correctness).
	if i := nRows % 64; len(b.words) >= nBitmapWords && i != 0 {
		b.words[nBitmapWords-1] &= (1 << i) - 1
	}

	nSummaryWords := (nBitmapWords + 63) >> 6
	dest := makeUintsEncoder[uint64](buf[offset:], nBitmapWords+nSummaryWords)
	// Copy all the words of the bitmap into the destination buffer.
	dest.CopyFrom(0, b.words)
	offset += uint32(len(b.words)) << align64Shift

	// The caller may have written fewer than nRows rows if the tail is all
	// zeroes, relying on these bits being implicitly zero. If the tail of b is
	// sparse, fill in zeroes.
	for i := len(b.words); i < nBitmapWords; i++ {
		dest.UnsafeSet(i, 0)
		offset += align64
	}

	// Add the summary bitmap.
	for i := 0; i < nSummaryWords; i++ {
		wordsOff := (i << 6) // i*64
		nWords := min(64, len(b.words)-wordsOff)
		var summaryWord uint64
		for j := 0; j < nWords; j++ {
			if (b.words)[wordsOff+j] != 0 {
				summaryWord |= 1 << j
			}
		}
		dest.UnsafeSet(nBitmapWords+i, summaryWord)
	}
	dest.Finish()
	return offset + uint32(nSummaryWords)<<align64Shift
}

// WriteDebug implements the ColumnWriter interface.
func (b *BitmapBuilder) WriteDebug(w io.Writer, rows int) {
	// TODO(jackson): Add more detailed debugging information.
	fmt.Fprint(w, "bitmap")
}

func bitmapToBinFormatter(f *binfmt.Formatter, tp treeprinter.Node, rows int) {
	encoding := bitmapEncoding(f.PeekUint(1))
	if encoding == zeroBitmapEncoding {
		f.HexBytesln(1, "zero bitmap encoding")
		f.ToTreePrinter(tp)
		return
	}
	if encoding != defaultBitmapEncoding {
		panic(fmt.Sprintf("unknown bitmap encoding %d", encoding))
	}
	f.HexBytesln(1, "default bitmap encoding")
	if aligned := align(f.RelativeOffset(), 8); aligned-f.RelativeOffset() != 0 {
		f.HexBytesln(aligned-f.RelativeOffset(), "padding to align to 64-bit boundary")
	}
	bitmapWords := (rows + 63) / 64
	for i := 0; i < bitmapWords; i++ {
		f.Line(8).Append("b ").Binary(8).Done("bitmap word %d", i)
	}
	summaryWords := (bitmapWords + 63) / 64
	for i := 0; i < summaryWords; i++ {
		f.Line(8).Append("b ").Binary(8).Done("bitmap summary word %d-%d", i*64, i*64+63)
	}
	f.ToTreePrinter(tp)
}

// nextBitInWord returns the index of the smallest set bit with an index ≥ bit
// within the provided word. The given index must be in the [0, 63] interval.
// The returned index is an index local to the word. Returns 64 if no set bit is
// found.
func nextBitInWord(word uint64, bit uint) int {
	// We want to find the index of the next set bit. We can accomplish this
	// by clearing the trailing `bit` bits from the word and counting the
	// number of trailing zeros. For example, consider the word and bit=37:
	//
	//           word: 1010101010111111111110000001110101010101011111111111000000111011
	//
	//         1<<bit: 0000000000000000000000000010000000000000000000000000000000000000
	//       1<<bit-1: 0000000000000000000000000001111111111111111111111111111111111111
	//      ^1<<bit-1: 1111111111111111111111111110000000000000000000000000000000000000
	// word&^1<<bit-1: 1010101010111111111110000000000000000000000000000000000000000000
	//
	// Counting the trailing zeroes of this last value gives us 43. For
	// visualizing, 1<<43 is:
	//
	//                 0000000000000000000010000000000000000000000000000000000000000000
	//
	return bits.TrailingZeros64(word &^ ((1 << bit) - 1))
}

// prevBitInWord returns the index of the largest set bit ≤ bit within the
// provided word. The given bit index must be in the [0, 63] interval. The
// returned bit index is an index local to the word. Returns -1 if no set bit is
// found.
func prevBitInWord(word uint64, bit uint) int {
	// We want to find the index of the previous set bit. We can accomplish
	// this by clearing the leading `bit` bits from the word and counting
	// the number of leading zeros. For example, consider the word and
	// bit=42:
	//
	//              word: 1010101010111111111110000001110101010101011111111111000000111011
	//
	//        1<<(bit+1): 0000000000000000000010000000000000000000000000000000000000000000
	//      1<<(bit+1)-1: 0000000000000000000001111111111111111111111111111111111111111111
	// word&1<<(bit+1)-1: 0000000000000000000000000001110101010101011111111111000000111011
	//
	// Counting the leading zeroes of this last value gives us 27 leading
	// zeros. 63-27 gives index 36. For visualizing, 1<<36 is:
	//
	//                    0000000000000000000000000001000000000000000000000000000000000000
	//
	return 63 - bits.LeadingZeros64(word&((1<<(bit+1))-1))
}
