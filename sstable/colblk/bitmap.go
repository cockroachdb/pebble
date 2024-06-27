// Copyright 2024 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package colblk

import (
	"fmt"
	"io"
	"math/bits"
	"slices"
	"strings"
	"unsafe"

	"github.com/cockroachdb/pebble/internal/binfmt"
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
	data     UnsafeRawSlice[uint64]
	bitCount int
}

// MakeBitmap returns a Bitmap that reads from memory at ptr supporting bitCount
// logical bits. No bounds checking is performed, so the caller must guarantee
// the bitmap is appropriately sized and the provided bitCount correctly
// identifies the number of bits in the bitmap.
func MakeBitmap(ptr unsafe.Pointer, bitCount int) Bitmap {
	return Bitmap{
		data:     makeUnsafeRawSlice[uint64](ptr),
		bitCount: bitCount,
	}
}

// Get returns true if the bit at position i is set and false otherwise.
func (b Bitmap) Get(i int) bool {
	return (b.data.At(i>>6 /* i/64 */) & (1 << uint(i%64))) != 0
}

// Successor returns the next bit greater than or equal to i set in the bitmap.
// Returns the number of bits represented by the bitmap if no next bit is set.
func (b Bitmap) Successor(i int) int {
	// nextInWord returns the index of the smallest set bit with an index >= bit
	// within the provided word.  The returned index is an index local to the
	// word.
	nextInWord := func(word uint64, bit uint) int {
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

	wordIdx := i >> 6 // i/64
	// Fast path for common case of reasonably dense bitmaps; if the there's a
	// bit > i set in the same word, return it.
	if next := nextInWord(b.data.At(wordIdx), uint(i%64)); next < 64 {
		return wordIdx<<6 + next
	}

	// Consult summary structure to find the next word with a set bit. The word
	// we just checked (wordIdx) is represented by the wordIdx%64'th bit in the
	// wordIdx/64'th summary word. We want to know if any of the other later
	// words that are summarized together have a set bit. We call [nextInWord]
	// on the summary word to get the index of which word has a set bit, if any.
	summaryTableOffset := (b.bitCount + 63) >> 6
	summaryWordIdx := summaryTableOffset + wordIdx>>6
	summaryNext := nextInWord(b.data.At(summaryWordIdx), uint(wordIdx%64)+1)
	// If [summaryNext] == 64, then there are no set bits in any of the earlier
	// words represented by the summary word at [summaryWordIdx]. In that case,
	// we need to keep scanning the summary table forwards.
	if summaryNext == 64 {
		summaryTableEnd := summaryTableOffset + summaryTableOffset>>6
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

// Predecessor returns the previous bit less than or equal to i set in the
// bitmap. Returns -1 if no previous bit is set.
func (b Bitmap) Predecessor(i int) int {
	// prevInWord returns the index of the largest set bit ≤ bit within the
	// provided word. The returned index is an index local to the word. Returns
	// -1 if no set bit is found.
	prevInWord := func(word uint64, bit uint) int {
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

	wordIdx := i >> 6 // i/64
	// Fast path for common case of reasonably dense bitmaps; if the there's a
	// bit < i set in the same word, return it.
	if prev := prevInWord(b.data.At(wordIdx), uint(i%64)); prev >= 0 {
		return (wordIdx << 6) + prev
	}

	// Consult summary structure to find the next word with a set bit. The word
	// we just checked (wordIdx) is represented by the wordIdx%64'th bit in the
	// wordIdx/64'th summary word. We want to know if any of other earlier words
	// that are summarized together have a set bit. We call [prevInWord] on the
	// summary word to get the index of which word has a set bit, if any.
	summaryTableOffset := (b.bitCount + 63) >> 6
	summaryWordIdx := summaryTableOffset + wordIdx>>6
	summaryPrev := prevInWord(b.data.At(summaryWordIdx), uint(wordIdx%64)-1)
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

// String returns a string representation of the entire bitmap.
func (b Bitmap) String() string {
	var sb strings.Builder
	for w := 0; w < (b.bitCount+63)/64; w++ {
		fmt.Fprintf(&sb, "%064b", b.data.At(w))
	}
	return sb.String()
}

// BitmapBuilder constructs a Bitmap. Bits are default false.
type BitmapBuilder struct {
	words []uint64
}

func bitmapRequiredSize(total int) int {
	nWords := (total + 63) >> 6          // divide by 64
	nSummaryWords := (nWords + 63) >> 6  // divide by 64
	return (nWords + nSummaryWords) << 3 // multiply by 8
}

// Set sets the bit at position i if v is true and clears the bit at position i
// otherwise. Callers need not call Set if v is false and Set(i, true) has not
// been set yet.
func (b *BitmapBuilder) Set(i int, v bool) {
	w := i >> 6 // divide by 64
	for len(b.words) <= w {
		b.words = append(b.words, 0)
	}
	if v {
		b.words[w] |= 1 << uint(i%64)
	} else {
		b.words[w] &^= 1 << uint(i%64)
	}
}

// Reset resets the bitmap to the empty state.
func (b *BitmapBuilder) Reset() {
	clear(b.words)
	b.words = b.words[:0]
}

// NumColumns implements the ColumnWriter interface.
func (b *BitmapBuilder) NumColumns() int { return 1 }

// Size implements the ColumnWriter interface.
func (b *BitmapBuilder) Size(rows int, offset uint32) uint32 {
	offset = align(offset, align64)
	return offset + uint32(bitmapRequiredSize(rows))
}

// Invert inverts the bitmap, setting all bits that are not set and clearing all
// bits that are set. If the bitmap's tail is sparse and is not large enough to
// represent nRows rows, it's first materialized.
func (b *BitmapBuilder) Invert(nRows int) {
	// If the tail of b is sparse, fill in zeroes before inverting.
	nBitmapWords := (nRows + 63) >> 6
	b.words = slices.Grow(b.words, nBitmapWords-len(b.words))[:nBitmapWords]
	for i := range b.words {
		b.words[i] = ^b.words[i]
	}
}

// Finish finalizes the bitmap, computing the per-word summary bitmap and
// writing the resulting data to buf at offset.
func (b *BitmapBuilder) Finish(col, nRows int, offset uint32, buf []byte) (uint32, ColumnDesc) {
	offset = alignWithZeroes(buf, offset, align64)
	dest := makeUnsafeRawSlice[uint64](unsafe.Pointer(&buf[offset]))

	nBitmapWords := (nRows + 63) >> 6
	// Truncate the bitmap to the number of words required to represent nRows.
	// The caller may have written more bits than nRows and no longer cares to
	// write them out.
	if len(b.words) > nBitmapWords {
		b.words = b.words[:nBitmapWords]
	}
	// Ensure the last word of the bitmap does not contain any set bits beyond
	// the last row. This is not just for determinism but also to ensure that
	// the summary bitmap is correct (which is necessary for Bitmap.Successor
	// correctness).
	if i := nRows % 64; len(b.words) >= nBitmapWords && i != 0 {
		b.words[nBitmapWords-1] &= (1 << i) - 1
	}

	// Copy all the words of the bitmap into the destination buffer.
	offset += uint32(copy(dest.Slice(len(b.words)), b.words)) << align64Shift

	// The caller may have written fewer than nRows rows if the tail is all
	// zeroes, relying on these bits being implicitly zero. If the tail of b is
	// sparse, fill in zeroes.
	for i := len(b.words); i < nBitmapWords; i++ {
		dest.set(i, 0)
		offset += align64
	}

	// Add the summary bitmap.
	nSummaryWords := (nBitmapWords + 63) >> 6
	for i := 0; i < nSummaryWords; i++ {
		wordsOff := (i << 6) // i*64
		nWords := min(64, len(b.words)-wordsOff)
		var summaryWord uint64
		for j := 0; j < nWords; j++ {
			if (b.words)[wordsOff+j] != 0 {
				summaryWord |= 1 << j
			}
		}
		dest.set(nBitmapWords+i, summaryWord)
	}
	offset += uint32(nSummaryWords) << align64Shift
	return offset, ColumnDesc(DataTypeBool)
}

// WriteDebug implements the ColumnWriter interface.
func (b *BitmapBuilder) WriteDebug(w io.Writer, rows int) {
	// TODO(jackson): Add more detailed debugging information.
	fmt.Fprint(w, "bitmap")
}

func bitmapToBinFormatter(f *binfmt.Formatter, rows int) int {
	bitmapWords := (rows + 63) / 64
	for i := 0; i < bitmapWords; i++ {
		f.Line(8).Append("b ").Binary(8).Done("bitmap word %d", i)
	}
	summaryWords := (bitmapWords + 63) / 64
	for i := 0; i < summaryWords; i++ {
		f.Line(8).Append("b ").Binary(8).Done("bitmap summary word %d-%d", i*64, i*64+63)
	}
	return (bitmapWords + summaryWords) * align64
}
