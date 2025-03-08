// Copyright 2024 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package colblk

import (
	"bytes"
	"fmt"
	"io"
	"math/bits"
	"slices"
	"strings"
	"unsafe"

	"github.com/cockroachdb/crlib/crbytes"
	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/pebble/internal/binfmt"
	"github.com/cockroachdb/pebble/internal/invariants"
	"github.com/cockroachdb/pebble/internal/treeprinter"
	"github.com/cockroachdb/pebble/sstable/block"
)

// PrefixBytes holds an array of lexicographically ordered byte slices. It
// provides prefix compression. Prefix compression applies strongly to two cases
// in CockroachDB: removal of the "[/tenantID]/tableID/indexID" prefix that is
// present on all table data keys, and multiple versions of a key that are
// distinguished only by different timestamp suffixes. With columnar blocks
// enabling the timestamp to be placed in a separate column, the multiple
// version problem becomes one of efficiently handling exact duplicate keys.
// PrefixBytes builds off of the RawBytes encoding, introducing additional
// slices for encoding (n+bundleSize-1)/bundleSize bundle prefixes and 1
// block-level shared prefix for the column.
//
// Unlike the original prefix compression performed by rowblk (inherited from
// LevelDB and RocksDB), PrefixBytes does not perform all prefix compression
// relative to the previous key. Rather it performs prefix compression relative
// to the first key of a key's bundle. This can result in less compression, but
// simplifies reverse iteration and allows iteration to be largely stateless.
//
// To understand the PrefixBytes layout, we'll work through an example using
// these 15 keys:
//
//	   0123456789
//	 0 aaabbbc
//	 1 aaabbbcc
//	 2 aaabbbcde
//	 3 aaabbbce
//	 4 aaabbbdee
//	 5 aaabbbdee
//	 6 aaabbbdee
//	 7 aaabbbeff
//	 8 aaabbe
//	 9 aaabbeef
//	10 aaabbeef
//	11 aaabc
//	12 aabcceef
//	13 aabcceef
//	14 aabcceef
//
// The total length of these keys is 119 bytes. There are 3 keys which occur
// multiple times (rows 4-6, 9-10, 12-14) which models multiple versions of the
// same MVCC key in CockroachDB. There is a shared prefix to all of the keys
// which models the "[/tenantID]/tableID/indexID" present on CockroachDB table
// data keys. There are other shared prefixes which model identical values in
// table key columns.
//
// The table below shows the components of the KeyBytes encoding for these 15
// keys when using a bundle size of 4 which results in 4 bundles. The 15 keys
// are encoded into 20 slices: 1 block prefix, 4 bundle prefixes, and 15
// suffixes. The first slice in the table is the block prefix that is shared by
// all keys in the block. The first slice in each bundle is the bundle prefix
// which is shared by all keys in the bundle.
//
//	 idx   | row   | end offset | data
//	-------+-------+------------+----------
//	     0 |       |          2 | aa
//	     1 |       |          7 | ..abbbc
//	     2 |     0 |          7 | .......
//	     3 |     1 |          8 | .......c
//	     4 |     2 |         10 | .......de
//	     5 |     3 |         11 | .......e
//	     6 |       |         15 | ..abbb
//	     7 |     4 |         18 | ......dee
//	     8 |     5 |         18 | .........
//	     9 |     6 |         18 | .........
//	    10 |     7 |         21 | ......eff
//	    11 |       |         23 | ..ab
//	    12 |     8 |         25 | ....be
//	    13 |     9 |         29 | ....beef
//	    14 |    10 |         29 | ........
//	    15 |    11 |         30 | ....c
//	    16 |       |         36 | ..bcceef
//	    17 |    12 |         36 | ........
//	    18 |    13 |         36 | ........
//	    19 |    14 |         36 | ........
//
// The 'end offset' column in the table encodes the exclusive offset within the
// string data section where each of the slices end. Each slice starts at the
// previous slice's end offset. The first slice (the block prefix)'s start
// offset is implicitly zero. Note that this differs from the plain RawBytes
// encoding which always stores a zero offset at the beginning of the offsets
// array to avoid special-casing the first slice. The block prefix already
// requires special-casing, so materializing the zero start offset is not
// needed.
//
// The table above defines 20 slices: the 1 block key prefix, the 4 bundle key
// prefixes and the 15 key suffixes. Offset[0] is the length of the first slice
// which is always anchored at data[0]. The data columns display the portion of
// the data array the slice covers. For row slices, an empty suffix column
// indicates that the slice is identical to the slice at the previous index
// which is indicated by the slice's offset being equal to the previous slice's
// offset. Due to the lexicographic sorting, the key at row i can't be a prefix
// of the key at row i-1 or it would have sorted before the key at row i-1. And
// if the key differs then only the differing bytes will be part of the suffix
// and not contained in the bundle prefix.
//
// The end result of this encoding is that we can store the 119 bytes of the 15
// keys plus their start and end offsets (which would naively consume 15*4=60
// bytes for at least the key lengths) in 61 bytes (36 bytes of data + 4 bytes
// of offset constant + 20 bytes of offset delta data + 1 byte of bundle size).
//
// # Physical representation
//
//	+==================================================================+
//	|                        Bundle size (1 byte)                      |
//	|                                                                  |
//	| The bundle size indicates how many keys prefix compression may   |
//	| apply across. Every bundleSize keys, prefix compression restarts.|
//	| The bundleSize is required to be a power of two,  and this 1-    |
//	| byte prefix stores log2(bundleSize).                             |
//	+==================================================================+
//	|                            RawBytes                              |
//	|                                                                  |
//	| A modified RawBytes encoding is used to store the data slices. A |
//	| PrefixBytes column storing n keys will encode                    |
//	|                                                                  |
//	|                        1 block prefix                            |
//	|                               +                                  |
//	|          (n + bundleSize-1)/bundleSize bundle prefixes           |
//	|                               +                                  |
//	|                         n row suffixes                           |
//	|                                                                  |
//	| slices. Unlike the RawBytes encoding, the first offset encoded   |
//	| is not guaranteed to be zero. In the PrefixBytes encoding, the   |
//	| first offset encodes the length of the column-wide prefix. The   |
//	| column-wide prefix is stored in slice(0, offset(0)).             |
//	|                                                                  |
//	|  +------------------------------------------------------------+  |
//	|  |                       Offset table                         |  |
//	|  |                                                            |  |
//	|  | A Uint32 column encoding offsets into the string data,     |  |
//	|  | possibly delta8 or delta16 encoded. When a delta encoding  |  |
//	|  | is used, the base constant is always zero.                 |  |
//	|  +------------------------------------------------------------+  |
//	|  | offsetDelta[0] | offsetDelta[1] | ... | offsetDelta[m]     |  |
//	|  +------------------------------------------------------------+  |
//	|  | prefix-compressed string data                              |  |
//	|  | ...                                                        |  |
//	|  +------------------------------------------------------------+  |
//	+==================================================================+
//
// TODO(jackson): Consider stealing the low bit of the offset for a flag
// indicating that a key is a duplicate and then using the remaining bits to
// encode the relative index of the duplicated key's end offset. This would
// avoid the O(bundle size) scan in the case of duplicate keys, but at the cost
// of complicating logic to look up a bundle prefix (which may need to follow a
// duplicate key's relative index to uncover the start offset of the bundle
// prefix).
//
// # Reads
//
// This encoding provides O(1) access to any row by calculating the bundle for
// the row (see bundleOffsetIndexForRow), then the per-row's suffix (see
// rowSuffixIndex). If the per-row suffix's end offset equals the previous
// offset, then the row is a duplicate key and we need to step backward until we
// find a non-empty slice or the start of the bundle (a variable number of
// steps, but bounded by the bundle size).
//
// Forward iteration can easily reuse the previous row's key with a check on
// whether the row's slice is empty. Reverse iteration within a run of equal
// keys can reuse the next row's key. When reverse iteration steps backward from
// a non-empty slice onto an empty slice, it must continue backward until a
// non-empty slice is found (just as in absolute positioning) to discover the
// row suffix that is duplicated.
//
// The Seek{GE,LT} routines first binary search on the first key of each bundle
// which can be retrieved without data movement because the bundle prefix is
// immediately adjacent to it in the data array. We can slightly optimize the
// binary search by skipping over all of the keys in the bundle on prefix
// mismatches.
type PrefixBytes struct {
	bundleCalc
	rows            int
	sharedPrefixLen int
	rawBytes        RawBytes
}

// Assert that PrefixBytes implements Array[[]byte].
var _ Array[[]byte] = PrefixBytes{}

// DecodePrefixBytes decodes the structure of a PrefixBytes, constructing an
// accessor for an array of lexicographically sorted byte slices constructed by
// PrefixBytesBuilder. Count must be the number of logical slices within the
// array.
func DecodePrefixBytes(
	b []byte, offset uint32, count int,
) (prefixBytes PrefixBytes, endOffset uint32) {
	if count == 0 {
		panic(errors.AssertionFailedf("empty PrefixBytes"))
	}
	// The first byte of a PrefixBytes-encoded column is the bundle size
	// expressed as log2 of the bundle size (the bundle size must always be a
	// power of two)
	bundleShift := uint32(*((*uint8)(unsafe.Pointer(&b[offset]))))
	calc := makeBundleCalc(bundleShift)
	nBundles := int(calc.bundleCount(count))

	rb, endOffset := DecodeRawBytes(b, offset+1, count+nBundles)
	pb := PrefixBytes{
		bundleCalc: calc,
		rows:       count,
		rawBytes:   rb,
	}
	pb.sharedPrefixLen = int(pb.rawBytes.offsets.At(0))
	return pb, endOffset
}

// Assert that DecodePrefixBytes implements DecodeFunc.
var _ DecodeFunc[PrefixBytes] = DecodePrefixBytes

// At returns the i'th []byte slice in the PrefixBytes. At must allocate, so
// callers should prefer accessing a slice's constituent components through
// SharedPrefix, BundlePrefix and RowSuffix.
func (b PrefixBytes) At(i int) []byte {
	return slices.Concat(b.SharedPrefix(), b.RowBundlePrefix(i), b.RowSuffix(i))
}

// UnsafeFirstSlice returns first slice in the PrefixBytes. The returned slice
// points directly into the PrefixBytes buffer and must not be mutated.
func (b *PrefixBytes) UnsafeFirstSlice() []byte {
	return b.rawBytes.slice(0, b.rawBytes.offsets.At(2))
}

// PrefixBytesIter is an iterator and associated buffers for PrefixBytes. It
// provides a means for efficiently iterating over the []byte slices contained
// within a PrefixBytes, avoiding unnecessary copying when portions of slices
// are shared.
type PrefixBytesIter struct {
	// Buf is used for materializing a user key. It is preallocated to the maximum
	// key length in the data block.
	Buf                      []byte
	syntheticPrefixLen       uint32
	sharedAndBundlePrefixLen uint32
	offsetIndex              int
	nextBundleOffsetIndex    int
}

// Init initializes the prefix bytes iterator; maxKeyLength must be
// large enough to fit any key in the block after applying any synthetic prefix
// and/or suffix.
func (i *PrefixBytesIter) Init(maxKeyLength int, syntheticPrefix block.SyntheticPrefix) {
	// Allocate a buffer that's large enough to hold the largest user key in the
	// block with 1 byte to spare (so that pointer arithmetic is never pointing
	// beyond the allocation, which would violate Go rules).
	n := maxKeyLength + 1
	if cap(i.Buf) < n {
		ptr := mallocgc(uintptr(n), nil, false)
		i.Buf = unsafe.Slice((*byte)(ptr), n)
	}
	i.Buf = i.Buf[:0]
	i.syntheticPrefixLen = uint32(len(syntheticPrefix))
	if syntheticPrefix.IsSet() {
		i.Buf = append(i.Buf, syntheticPrefix...)
	}
}

// SetAt updates the provided PrefixBytesIter to hold the i'th []byte slice in
// the PrefixBytes. The PrefixBytesIter's buffer must be sufficiently large to
// hold the i'th []byte slice, and the caller is required to statically ensure
// this.
func (b *PrefixBytes) SetAt(it *PrefixBytesIter, i int) {
	// Determine the offset and length of the bundle prefix.
	bundleOffsetIndex := b.bundleOffsetIndexForRow(i)
	bundleOffsetStart, bundleOffsetEnd := b.rawBytes.offsets.At2(bundleOffsetIndex)
	bundlePrefixLen := bundleOffsetEnd - bundleOffsetStart

	// Determine the offset and length of the row's individual suffix.
	it.offsetIndex = b.rowSuffixIndex(i)
	// TODO(jackson): rowSuffixOffsets will recompute bundleOffsetIndexForRow in
	// the case that the row is a duplicate key. Is it worth optimizing to avoid
	// this recomputation? The expected case is non-duplicate keys, so it may
	// not be worthwhile.
	rowSuffixStart, rowSuffixEnd := b.rowSuffixOffsets(i, it.offsetIndex)
	rowSuffixLen := rowSuffixEnd - rowSuffixStart

	it.sharedAndBundlePrefixLen = it.syntheticPrefixLen + uint32(b.sharedPrefixLen) + bundlePrefixLen
	it.Buf = it.Buf[:it.sharedAndBundlePrefixLen+rowSuffixLen]

	ptr := unsafe.Pointer(unsafe.SliceData(it.Buf))
	ptr = unsafe.Pointer(uintptr(ptr) + uintptr(it.syntheticPrefixLen))
	// Copy the shared key prefix.
	memmove(ptr, b.rawBytes.data, uintptr(b.sharedPrefixLen))
	// Copy the bundle prefix.
	ptr = unsafe.Pointer(uintptr(ptr) + uintptr(b.sharedPrefixLen))
	memmove(
		ptr,
		unsafe.Pointer(uintptr(b.rawBytes.data)+uintptr(bundleOffsetStart)),
		uintptr(bundlePrefixLen))

	// Copy the per-row suffix.
	ptr = unsafe.Pointer(uintptr(ptr) + uintptr(bundlePrefixLen))
	memmove(
		ptr,
		unsafe.Pointer(uintptr(b.rawBytes.data)+uintptr(rowSuffixStart)),
		uintptr(rowSuffixLen))
	// Set nextBundleOffsetIndex so that a call to SetNext can cheaply determine
	// whether the next row is in the same bundle.
	it.nextBundleOffsetIndex = bundleOffsetIndex + (1 << b.bundleShift) + 1
}

// SetNext updates the provided PrefixBytesIter to hold the next []byte slice in
// the PrefixBytes. SetNext requires the provided iter to currently hold a slice
// and for a subsequent slice to exist within the PrefixBytes.  The
// PrefixBytesIter's buffer must be sufficiently large to hold the next []byte
// slice, and the caller is required to statically ensure this.
func (b *PrefixBytes) SetNext(it *PrefixBytesIter) {
	it.offsetIndex++
	// If the next row is in the same bundle, we can take a fast path of only
	// updating the per-row suffix.
	if it.offsetIndex < it.nextBundleOffsetIndex {
		rowSuffixStart, rowSuffixEnd := b.rawBytes.offsets.At2(it.offsetIndex)
		rowSuffixLen := rowSuffixEnd - rowSuffixStart
		if rowSuffixLen == 0 {
			// The start and end offsets are equal, indicating that the key is a
			// duplicate. Since it's identical to the previous key, there's
			// nothing left to do, we can leave buf as-is.
			return
		}
		it.Buf = it.Buf[:it.sharedAndBundlePrefixLen+rowSuffixLen]
		// Copy in the per-row suffix.
		ptr := unsafe.Pointer(unsafe.SliceData(it.Buf))
		memmove(
			unsafe.Pointer(uintptr(ptr)+uintptr(it.sharedAndBundlePrefixLen)),
			unsafe.Pointer(uintptr(b.rawBytes.data)+uintptr(rowSuffixStart)),
			uintptr(rowSuffixLen))
		return
	}

	// We've reached the end of the bundle. We need to update the bundle prefix.
	// The offsetIndex is currently pointing to the start of the new bundle
	// prefix. Increment it to point at the start of the new row suffix.
	it.offsetIndex++
	rowSuffixStart, rowSuffixEnd := b.rawBytes.offsets.At2(it.offsetIndex)
	rowSuffixLen := rowSuffixEnd - rowSuffixStart

	// Read the offsets of the new bundle prefix and update the index of the
	// next bundle.
	bundlePrefixStart := b.rawBytes.offsets.At(it.nextBundleOffsetIndex)
	bundlePrefixLen := rowSuffixStart - bundlePrefixStart
	it.nextBundleOffsetIndex = it.offsetIndex + (1 << b.bundleShift)

	it.sharedAndBundlePrefixLen = it.syntheticPrefixLen + uint32(b.sharedPrefixLen) + bundlePrefixLen
	it.Buf = it.Buf[:it.sharedAndBundlePrefixLen+rowSuffixLen]
	// Copy in the new bundle suffix.
	ptr := unsafe.Pointer(unsafe.SliceData(it.Buf))
	ptr = unsafe.Pointer(uintptr(ptr) + uintptr(it.syntheticPrefixLen) + uintptr(b.sharedPrefixLen))
	memmove(
		ptr,
		unsafe.Pointer(uintptr(b.rawBytes.data)+uintptr(bundlePrefixStart)),
		uintptr(bundlePrefixLen))
	// Copy in the per-row suffix.
	ptr = unsafe.Pointer(uintptr(ptr) + uintptr(bundlePrefixLen))
	memmove(
		ptr,
		unsafe.Pointer(uintptr(b.rawBytes.data)+uintptr(rowSuffixStart)),
		uintptr(rowSuffixLen))
}

// SharedPrefix return a []byte of the shared prefix that was extracted from
// all of the values in the Bytes vector. The returned slice should not be
// mutated.
func (b *PrefixBytes) SharedPrefix() []byte {
	// The very first slice is the prefix for the entire column.
	return b.rawBytes.slice(0, b.rawBytes.offsets.At(0))
}

// RowBundlePrefix takes a row index and returns a []byte of the prefix shared
// among all the keys in the row's bundle, but without the block-level shared
// prefix for the column. The returned slice should not be mutated.
func (b *PrefixBytes) RowBundlePrefix(row int) []byte {
	i := b.bundleOffsetIndexForRow(row)
	return b.rawBytes.slice(b.rawBytes.offsets.At(i), b.rawBytes.offsets.At(i+1))
}

// BundlePrefix returns the prefix of the i-th bundle in the column. The
// provided i must be in the range [0, BundleCount()). The returned slice should
// not be mutated.
func (b *PrefixBytes) BundlePrefix(i int) []byte {
	j := b.offsetIndexByBundleIndex(i)
	return b.rawBytes.slice(b.rawBytes.offsets.At(j), b.rawBytes.offsets.At(j+1))
}

// RowSuffix returns a []byte of the suffix unique to the row. A row's full key
// is the result of concatenating SharedPrefix(), BundlePrefix() and
// RowSuffix().
//
// The returned slice should not be mutated.
func (b *PrefixBytes) RowSuffix(row int) []byte {
	return b.rawBytes.slice(b.rowSuffixOffsets(row, b.rowSuffixIndex(row)))
}

// rowSuffixOffsets finds the start and end offsets of the row's suffix slice,
// accounting for duplicate keys. It takes the index of the row, and the value
// of rowSuffixIndex(row).
func (b *PrefixBytes) rowSuffixOffsets(row, i int) (low uint32, high uint32) {
	// Retrieve the low and high offsets indicating the start and end of the
	// row's suffix slice.
	low, high = b.rawBytes.offsets.At2(i)
	// If there's a non-empty slice for the row, this row is different than its
	// predecessor.
	if low != high {
		return low, high
	}
	// Otherwise, an empty slice indicates a duplicate key. We need to find the
	// first non-empty predecessor within the bundle, or if all the rows are
	// empty, return arbitrary equal low and high.
	//
	// Compute the index of the first row in the bundle so we know when to stop.
	firstIndex := 1 + b.bundleOffsetIndexForRow(row)
	for i > firstIndex {
		// Step back a row, and check if the slice is non-empty.
		i--
		high = low
		low = b.rawBytes.offsets.At(i)
		if low != high {
			return low, high
		}
	}
	// All the rows in the bundle are empty.
	return low, high
}

// Rows returns the count of rows whose keys are encoded within the PrefixBytes.
func (b *PrefixBytes) Rows() int {
	return b.rows
}

// BundleCount returns the count of bundles within the PrefixBytes.
func (b *PrefixBytes) BundleCount() int {
	return b.bundleCount(b.rows)
}

// Search searches for the first key in the PrefixBytes that is greater than or
// equal to k, returning the index of the key and whether an equal key was
// found. If multiple keys are equal, the index of the first such key is
// returned. If all keys are < k, Search returns Rows() for the row index.
func (b *PrefixBytes) Search(k []byte) (rowIndex int, isEqual bool) {
	// First compare to the block-level shared prefix.
	n := min(len(k), b.sharedPrefixLen)
	c := bytes.Compare(k[:n], unsafe.Slice((*byte)(b.rawBytes.data), b.sharedPrefixLen))
	// Note that c cannot be 0 when n < b.sharedPrefixLen.
	if c != 0 {
		if c < 0 {
			// Search key is less than any prefix in the block.
			return 0, false
		}
		// Search key is greater than any key in the block.
		return b.rows, false
	}
	// Trim the block-level shared prefix from the search key.
	k = k[b.sharedPrefixLen:]

	// Binary search among the first keys of each bundle.
	//
	// Define f(-1) == false and f(upper) == true.
	// Invariant: f(bi-1) == false, f(upper) == true.
	nBundles := b.BundleCount()
	bi, upper := 0, nBundles
	upperEqual := false
	for bi < upper {
		h := int(uint(bi+upper) >> 1) // avoid overflow when computing h
		// bi ≤ h < upper

		// Retrieve the first key in the h-th (zero-indexed) bundle. We take
		// advantage of the fact that the first row is stored contiguously in
		// the data array (modulo the block prefix) to slice the entirety of the
		// first key:
		//
		//       b u n d l e p r e f i x f i r s t k e y r e m a i n d e r
		//       ^                       ^                                 ^
		//     offset(j)             offset(j+1)                       offset(j+2)
		//
		j := b.offsetIndexByBundleIndex(h)
		bundleFirstKey := b.rawBytes.slice(b.rawBytes.offsets.At(j), b.rawBytes.offsets.At(j+2))
		c = bytes.Compare(k, bundleFirstKey)
		switch {
		case c > 0:
			bi = h + 1 // preserves f(bi-1) == false
		case c < 0:
			upper = h // preserves f(upper) == true
			upperEqual = false
		default:
			// c == 0
			upper = h // preserves f(upper) == true
			upperEqual = true
		}
	}
	if bi == 0 {
		// The very first key is ≥ k. Return it.
		return 0, upperEqual
	}
	// The first key of the bundle bi is ≥ k, but any of the keys in the
	// previous bundle besides the first could also be ≥ k. We can binary search
	// among them, but if the seek key doesn't share the previous bundle's
	// prefix there's no need.
	j := b.offsetIndexByBundleIndex(bi - 1)
	bundlePrefix := b.rawBytes.slice(b.rawBytes.offsets.At(j), b.rawBytes.offsets.At(j+1))

	// The row we are looking for might still be in the previous bundle even
	// though the seek key is greater than the first key. This is possible only
	// if the search key shares the first bundle's prefix (eg, the search key
	// equals a row in the previous bundle or falls between two rows within the
	// previous bundle).
	if len(bundlePrefix) > len(k) || !bytes.Equal(k[:len(bundlePrefix)], bundlePrefix) {
		// The search key doesn't share the previous bundle's prefix, so all of
		// the keys in the previous bundle must be less than k. We know the
		// first key of bi is ≥ k, so return it.
		if bi >= nBundles {
			return b.rows, false
		}
		return bi << b.bundleShift, upperEqual
	}
	// Binary search among bundle bi-1's key remainders after stripping bundle
	// bi-1's prefix.
	//
	// Define f(l-1) == false and f(u) == true.
	// Invariant: f(l-1) == false, f(u) == true.
	k = k[len(bundlePrefix):]
	l := 1
	u := min(1<<b.bundleShift, b.rows-(bi-1)<<b.bundleShift)
	for l < u {
		h := int(uint(l+u) >> 1) // avoid overflow when computing h
		// l ≤ h < u

		// j is currently the index of the offset of bundle bi-i's prefix.
		//
		//     b u n d l e p r e f i x f i r s t k e y s e c o n d k e y
		//     ^                       ^               ^
		//  offset(j)              offset(j+1)     offset(j+2)
		//
		// The beginning of the zero-indexed i-th key of the bundle is at
		// offset(j+i+1).
		//
		hStart, hEnd := b.rawBytes.offsets.At2(j + h + 1)
		// There's a complication with duplicate keys. When keys are repeated,
		// the PrefixBytes encoding avoids re-encoding the duplicate key,
		// instead encoding an empty slice. While binary searching, if we land
		// on an empty slice, we need to back up until we find a non-empty slice
		// which is the key at index h. We iterate with p. If we eventually find
		// the duplicated key at index p < h and determine f(p) == true, then we
		// can set u=p (rather than h). If we determine f(p)==false, then we
		// know f(h)==false too and set l=h+1.
		p := h
		if hStart == hEnd {
			// Back up looking for an empty slice.
			for hStart == hEnd && p >= l {
				p--
				hEnd = hStart
				hStart = b.rawBytes.offsets.At(j + p + 1)
			}
			// If we backed up to l-1, then all the rows in indexes [l, h] have
			// the same keys as index l-1. We know f(l-1) == false [see the
			// invariants above], so we can move l to h+1 and continue the loop
			// without performing any key comparisons.
			if p < l {
				l = h + 1
				continue
			}
		}
		rem := b.rawBytes.slice(hStart, hEnd)
		c = bytes.Compare(k, rem)
		switch {
		case c > 0:
			l = h + 1 // preserves f(l-1) == false
		case c < 0:
			u = p // preserves f(u) == true
			upperEqual = false
		default:
			// c == 0
			u = p // preserves f(u) == true
			upperEqual = true
		}
	}
	i := (bi-1)<<b.bundleShift + l
	if i < b.rows {
		return i, upperEqual
	}
	return b.rows, false
}

func prefixBytesToBinFormatter(
	f *binfmt.Formatter, tp treeprinter.Node, count int, sliceFormatter func([]byte) string,
) {
	if sliceFormatter == nil {
		sliceFormatter = defaultSliceFormatter
	}
	pb, _ := DecodePrefixBytes(f.RelativeData(), uint32(f.RelativeOffset()), count)

	f.HexBytesln(1, "bundle size: %d", 1<<pb.bundleShift)
	f.ToTreePrinter(tp)

	n := tp.Child("offsets table")
	dataOffset := uint64(f.RelativeOffset()) + uint64(uintptr(pb.rawBytes.data)-uintptr(pb.rawBytes.start))
	uintsToBinFormatter(f, n, pb.rawBytes.slices+1, func(offsetDelta, offsetBase uint64) string {
		// NB: offsetBase will always be zero for PrefixBytes columns.
		return fmt.Sprintf("%d [%d overall]", offsetDelta+offsetBase, offsetDelta+offsetBase+dataOffset)
	})

	n = tp.Child("data")

	// The first offset encodes the length of the block prefix.
	blockPrefixLen := pb.rawBytes.offsets.At(0)
	f.HexBytesln(int(blockPrefixLen), "data[00]: %s (block prefix)",
		sliceFormatter(pb.rawBytes.slice(0, blockPrefixLen)))

	k := 2 + (count-1)>>pb.bundleShift + count
	startOff := blockPrefixLen
	prevLen := blockPrefixLen

	// Use dots to indicate string data that's elided because it falls within
	// the block or bundle prefix.
	dots := strings.Repeat(".", int(blockPrefixLen))
	// Iterate through all the slices in the data section, annotating bundle
	// prefixes and using dots to indicate elided data.
	for i := 0; i < k-1; i++ {
		endOff := pb.rawBytes.offsets.At(i + 1)
		if i%(1+(1<<pb.bundleShift)) == 0 {
			// This is a bundle prefix.
			dots = strings.Repeat(".", int(blockPrefixLen))
			f.HexBytesln(int(endOff-startOff), "data[%02d]: %s%s (bundle prefix)", i+1, dots, sliceFormatter(pb.rawBytes.At(i)))
			dots = strings.Repeat(".", int(endOff-startOff+blockPrefixLen))
			prevLen = endOff - startOff + blockPrefixLen
		} else if startOff == endOff {
			// An empty slice that's not a block or bundle prefix indicates a
			// repeat key.
			f.HexBytesln(0, "data[%02d]: %s", i+1, strings.Repeat(".", int(prevLen)))
		} else {
			f.HexBytesln(int(endOff-startOff), "data[%02d]: %s%s", i+1, dots, sliceFormatter(pb.rawBytes.At(i)))
			prevLen = uint32(len(dots)) + endOff - startOff
		}
		startOff = endOff
	}
	f.ToTreePrinter(n)
}

// PrefixBytesBuilder encodes a column of lexicographically-sorted byte slices,
// applying prefix compression to reduce the encoded size.
type PrefixBytesBuilder struct {
	bundleCalc
	// TODO(jackson): If prefix compression is very effective, the encoded size
	// may remain very small while the physical in-memory size of the
	// in-progress data slice may grow very large. This may pose memory usage
	// problems during block building.
	data               []byte // The raw, concatenated keys w/o any prefix compression
	nKeys              int    // The number of keys added to the builder
	bundleSize         int    // The number of keys per bundle
	completedBundleLen int    // The encoded size of completed bundles
	// sizings maintains metadata about the size of the accumulated data at both
	// nKeys and nKeys-1. Information for the state after the most recently
	// added key is stored at (b.nKeys+1)%2.
	sizings [2]prefixBytesSizing
	offsets struct {
		count int // The number of offsets in the builder
		// elems provides access to elements without bounds checking. elems is
		// grown automatically in addOffset.
		elems []uint32
	}
	maxShared uint16
}

// Assert that PrefixBytesBuilder implements ColumnWriter.
var _ ColumnWriter = (*PrefixBytesBuilder)(nil)

// Init initializes the PrefixBytesBuilder with the specified bundle size. The
// builder will produce a prefix-compressed column of data type
// DataTypePrefixBytes. The [bundleSize] indicates the number of keys that form
// a "bundle," across which prefix-compression is applied. All keys in the
// column will share a column-wide prefix if there is one.
func (b *PrefixBytesBuilder) Init(bundleSize int) {
	if bundleSize > 0 && (bundleSize&(bundleSize-1)) != 0 {
		panic(errors.AssertionFailedf("prefixbytes bundle size %d is not a power of 2", bundleSize))
	}
	*b = PrefixBytesBuilder{
		bundleCalc: makeBundleCalc(uint32(bits.TrailingZeros32(uint32(bundleSize)))),
		data:       b.data[:0],
		bundleSize: bundleSize,
		offsets:    b.offsets,
		maxShared:  (1 << 16) - 1,
	}
	b.offsets.count = 0
}

// NumColumns implements ColumnWriter.
func (b *PrefixBytesBuilder) NumColumns() int { return 1 }

// DataType implements ColumnWriter.
func (b *PrefixBytesBuilder) DataType(int) DataType { return DataTypePrefixBytes }

// Reset resets the builder to an empty state, preserving the existing bundle
// size.
func (b *PrefixBytesBuilder) Reset() {
	const maxRetainedData = 512 << 10 // 512 KB
	*b = PrefixBytesBuilder{
		bundleCalc: b.bundleCalc,
		data:       b.data[:0],
		bundleSize: b.bundleSize,
		offsets:    b.offsets,
		maxShared:  b.maxShared,
		sizings:    [2]prefixBytesSizing{},
	}
	b.offsets.count = 0
	if len(b.data) > maxRetainedData {
		b.data = nil
	}
}

// Rows returns the number of keys added to the builder.
func (b *PrefixBytesBuilder) Rows() int { return b.nKeys }

// prefixBytesSizing maintains metadata about the size of the accumulated data
// and its encoded size. Every key addition computes a new prefixBytesSizing
// struct. The PrefixBytesBuilder maintains two prefixBytesSizing structs, one
// for the state after the most recent key addition, and one for the state after
// the second most recent key addition.
type prefixBytesSizing struct {
	lastKeyOff                int // the offset in data where the last key added begins
	offsetCount               int // the count of offsets required to encode the data
	blockPrefixLen            int // the length of the block prefix
	currentBundleDistinctLen  int // the length of the "current" bundle's distinct keys
	currentBundleDistinctKeys int // the number of distinct keys in the "current" bundle
	// currentBundlePrefixLen is the length of the "current" bundle's prefix.
	// The current bundle holds all keys that are not included within
	// PrefixBytesBuilder.completedBundleLen. If the addition of a key causes
	// the creation of a new bundle, the previous bundle's size is incorporated
	// into completedBundleLen and currentBundlePrefixLen is updated to the
	// length of the new bundle key. This ensures that there's always at least 1
	// key in the "current" bundle allowing Finish to accept rows = nKeys-1.
	//
	// Note that currentBundlePrefixLen is inclusive of the blockPrefixLen.
	//
	// INVARIANT: currentBundlePrefixLen >= blockPrefixLen
	currentBundlePrefixLen    int          // the length of the "current" bundle's prefix
	currentBundlePrefixOffset int          // the index of the offset of the "current" bundle's prefix
	compressedDataLen         int          // the compressed, encoded size of data
	offsetEncoding            UintEncoding // the encoding necessary to encode the offsets
}

func (sz *prefixBytesSizing) String() string {
	return fmt.Sprintf("lastKeyOff:%d offsetCount:%d blockPrefixLen:%d\n"+
		"currentBundleDistinct{Len,Keys}: (%d,%d)\n"+
		"currentBundlePrefix{Len,Offset}: (%d,%d)\n"+
		"compressedDataLen:%d offsetEncoding:%s",
		sz.lastKeyOff, sz.offsetCount, sz.blockPrefixLen, sz.currentBundleDistinctLen,
		sz.currentBundleDistinctKeys, sz.currentBundlePrefixLen, sz.currentBundlePrefixOffset,
		sz.compressedDataLen, sz.offsetEncoding)
}

// Put adds the provided key to the column. The provided key must be
// lexicographically greater than or equal to the previous key added to the
// builder.
//
// The provided bytesSharedWithPrev must be the length of the byte prefix the
// provided key shares with the previous key. The caller is required to provide
// this because in the primary expected use, the caller will already need to
// compute it for the purpose of determining whether successive keys share the
// same prefix.
func (b *PrefixBytesBuilder) Put(key []byte, bytesSharedWithPrev int) {
	currIdx := b.nKeys & 1 // %2
	curr := &b.sizings[currIdx]
	prev := &b.sizings[currIdx^1]

	if invariants.Enabled {
		if len(key) == 0 {
			panic(errors.AssertionFailedf("key must be non-empty"))
		}
		if b.maxShared == 0 {
			panic(errors.AssertionFailedf("maxShared must be positive"))
		}
		if b.nKeys > 0 {
			if bytes.Compare(key, b.data[prev.lastKeyOff:]) < 0 {
				panic(errors.AssertionFailedf("keys must be added in order: %q < %q", key, b.data[prev.lastKeyOff:]))
			}
			if bytesSharedWithPrev != crbytes.CommonPrefix(key, b.data[prev.lastKeyOff:]) {
				panic(errors.AssertionFailedf("bytesSharedWithPrev %d != %d", bytesSharedWithPrev,
					crbytes.CommonPrefix(key, b.data[prev.lastKeyOff:])))
			}
		}
	}

	// Check if this is the first key in a bundle.
	if b.nKeys&(b.bundleSize-1) == 0 {
		if b.nKeys == 0 {
			// We're adding the first key to the block.
			// Set a placeholder offset for the block prefix length.
			b.addOffset(0)
			// Set a placeholder offset for the bundle prefix length.
			b.addOffset(0)
			b.nKeys++
			b.data = append(b.data, key...)
			b.addOffset(uint32(len(b.data)))
			*curr = prefixBytesSizing{
				lastKeyOff:                0,
				offsetCount:               b.offsets.count,
				blockPrefixLen:            min(len(key), int(b.maxShared)),
				currentBundleDistinctLen:  len(key),
				currentBundleDistinctKeys: 1,
				currentBundlePrefixLen:    min(len(key), int(b.maxShared)),
				currentBundlePrefixOffset: 1,
				compressedDataLen:         len(key),
				offsetEncoding:            DetermineUintEncodingNoDelta(uint64(len(key))),
			}
			return
		}
		// We're starting a new bundle.

		// Set the bundle prefix length of the previous bundle.
		unsafeSetUint32(
			b.offsets.elems, prev.currentBundlePrefixOffset,
			unsafeGetUint32(b.offsets.elems, prev.currentBundlePrefixOffset-1)+uint32(prev.currentBundlePrefixLen),
		)

		// Finalize the encoded size of the previous bundle.
		bundleSizeJustCompleted := prev.currentBundleDistinctLen - (prev.currentBundleDistinctKeys-1)*prev.currentBundlePrefixLen
		b.completedBundleLen += bundleSizeJustCompleted

		// Update the block prefix length if necessary. The caller tells us how
		// many bytes of prefix this key shares with the previous key. The block
		// prefix can only shrink if the bytes shared with the previous key are
		// less than the block prefix length, in which case the new block prefix
		// is the number of bytes shared with the previous key.
		blockPrefixLen := min(prev.blockPrefixLen, bytesSharedWithPrev)
		b.nKeys++
		*curr = prefixBytesSizing{
			lastKeyOff:     len(b.data),
			offsetCount:    b.offsets.count + 2,
			blockPrefixLen: blockPrefixLen,
			// We're adding the first key to the current bundle. Initialize
			// the current bundle prefix.
			currentBundlePrefixOffset: b.offsets.count,
			currentBundlePrefixLen:    min(len(key), int(b.maxShared)),
			currentBundleDistinctLen:  len(key),
			currentBundleDistinctKeys: 1,
			compressedDataLen:         b.completedBundleLen + len(key) - (b.bundleCount(b.nKeys)-1)*blockPrefixLen,
		}
		curr.offsetEncoding = DetermineUintEncodingNoDelta(uint64(curr.compressedDataLen))
		b.data = append(b.data, key...)
		b.addOffset(0) // Placeholder for bundle prefix.
		b.addOffset(uint32(len(b.data)))
		return
	}
	// We're adding a new key to an existing bundle.
	b.nKeys++

	if bytesSharedWithPrev == len(key) {
		// Duplicate key; don't add it to the data slice and don't adjust
		// currentBundleDistinct{Len,Keys}.
		*curr = *prev
		curr.offsetCount++
		b.addOffset(unsafeGetUint32(b.offsets.elems, b.offsets.count-1))
		return
	}

	// Update the bundle prefix length. Note that the shared prefix length
	// can only shrink as new values are added. During construction, the
	// bundle prefix value is stored contiguously in the data array so even
	// if the bundle prefix length changes no adjustment is needed to that
	// value or to the first key in the bundle.
	*curr = prefixBytesSizing{
		lastKeyOff:                len(b.data),
		offsetCount:               prev.offsetCount + 1,
		blockPrefixLen:            min(prev.blockPrefixLen, bytesSharedWithPrev),
		currentBundleDistinctLen:  prev.currentBundleDistinctLen + len(key),
		currentBundleDistinctKeys: prev.currentBundleDistinctKeys + 1,
		currentBundlePrefixLen:    min(prev.currentBundlePrefixLen, bytesSharedWithPrev),
		currentBundlePrefixOffset: prev.currentBundlePrefixOffset,
	}
	// Compute the correct compressedDataLen.
	curr.compressedDataLen = b.completedBundleLen +
		curr.currentBundleDistinctLen -
		(curr.currentBundleDistinctKeys-1)*curr.currentBundlePrefixLen
	// Currently compressedDataLen is correct, except that it includes the block
	// prefix length for all bundle prefixes. Adjust the length to account for
	// the block prefix being stripped from every bundle except the first one.
	curr.compressedDataLen -= (b.bundleCount(b.nKeys) - 1) * curr.blockPrefixLen
	// The compressedDataLen is the largest offset we'll need to encode in the
	// offset table.
	curr.offsetEncoding = DetermineUintEncodingNoDelta(uint64(curr.compressedDataLen))
	b.data = append(b.data, key...)
	b.addOffset(uint32(len(b.data)))
}

// UnsafeGet returns the zero-indexed i'th key added to the builder through Put.
// UnsafeGet may only be used to retrieve the Rows()-1'th or Rows()-2'th keys.
// If called with a different i value, UnsafeGet panics.  The keys returned by
// UnsafeGet are guaranteed to be stable until Finish or Reset is called. The
// caller must not mutate the returned slice.
func (b *PrefixBytesBuilder) UnsafeGet(i int) []byte {
	switch i {
	case b.nKeys - 1:
		lastKeyOff := b.sizings[i&1].lastKeyOff
		return b.data[lastKeyOff:]
	case b.nKeys - 2:
		lastKeyOff := b.sizings[(i^1)&1].lastKeyOff
		secondLastKeyOff := b.sizings[i&1].lastKeyOff
		if secondLastKeyOff == lastKeyOff {
			// The last key is a duplicate of the second-to-last key.
			return b.data[secondLastKeyOff:]
		}
		return b.data[secondLastKeyOff:lastKeyOff]
	default:
		panic(errors.AssertionFailedf("UnsafeGet(%d) called on PrefixBytes with %d keys", i, b.nKeys))
	}
}

// addOffset adds an offset to the offsets table. If necessary, addOffset will
// grow the offset table to accommodate the new offset.
func (b *PrefixBytesBuilder) addOffset(offset uint32) {
	if b.offsets.count == len(b.offsets.elems) {
		// Double the size of the allocated array, or initialize it to at least
		// 64 rows if this is the first allocation.
		n2 := max(len(b.offsets.elems)<<1, 64)
		newSlice := make([]uint32, n2)
		copy(newSlice, b.offsets.elems)
		b.offsets.elems = newSlice
	}
	unsafeSetUint32(b.offsets.elems, b.offsets.count, offset)
	b.offsets.count++
}

// writePrefixCompressed writes the provided builder's first [rows] rows with
// prefix-compression applied. It writes offsets and string data in tandem,
// writing offsets of width T into [offsetDeltas] and compressed string data
// into [buf]. The builder's internal state is not modified by
// writePrefixCompressed. writePrefixCompressed is generic in terms of the type
// T of the offset deltas.
//
// The caller must have correctly constructed [offsetDeltas] such that writing
// [sizing.offsetCount] offsets of size T does not overwrite the beginning of
// [buf]:
//
//	+-------------------------------------+  <- offsetDeltas.ptr
//	| offsetDeltas[0]                     |
//	+-------------------------------------+
//	| offsetDeltas[1]                     |
//	+-------------------------------------+
//	| ...                                 |
//	+-------------------------------------+
//	| offsetDeltas[sizing.offsetCount-1]  |
//	+-------------------------------------+ <- &buf[0]
//	| buf (string data)                   |
//	| ...                                 |
//	+-------------------------------------+
func writePrefixCompressed[T Uint](
	b *PrefixBytesBuilder, rows int, sz *prefixBytesSizing, offsetDeltas uintsEncoder[T], buf []byte,
) {
	if invariants.Enabled && offsetDeltas.Len() != sz.offsetCount {
		panic("incorrect offsetDeltas length")
	}
	if rows <= 1 {
		if rows == 1 {
			// If there's just 1 row, no prefix compression is necessary and we can
			// just encode the first key as the entire block prefix and first bundle
			// prefix.
			e := b.offsets.elems[2]
			offsetDeltas.UnsafeSet(0, T(e))
			offsetDeltas.UnsafeSet(1, T(e))
			offsetDeltas.UnsafeSet(2, T(e))
			copy(buf[:e], b.data[:e])
		}
		return
	}

	// The offset at index 0 is the block prefix length.
	copy(buf[:sz.blockPrefixLen], b.data[:sz.blockPrefixLen])
	destOffset := T(sz.blockPrefixLen)
	offsetDeltas.UnsafeSet(0, destOffset)
	var lastRowOffset uint32
	var shared int

	// Loop over the slices starting at the bundle prefix of the first bundle.
	// If the slice is a bundle prefix, carve off the suffix that excludes the
	// block prefix. Otherwise, carve off the suffix that excludes the block
	// prefix + bundle prefix.
	for i := 1; i < sz.offsetCount; i++ {
		off := unsafeGetUint32(b.offsets.elems, i)
		var suffix []byte
		if (i-1)%(b.bundleSize+1) == 0 {
			// This is a bundle prefix.
			if i == sz.currentBundlePrefixOffset {
				suffix = b.data[lastRowOffset+uint32(sz.blockPrefixLen) : lastRowOffset+uint32(sz.currentBundlePrefixLen)]
			} else {
				suffix = b.data[lastRowOffset+uint32(sz.blockPrefixLen) : off]
			}
			shared = sz.blockPrefixLen + len(suffix)
			// We don't update lastRowOffset here because the bundle prefix
			// was never actually stored separately in the data array.
		} else {
			// If the offset of this key is the same as the offset of the
			// previous key, then the key is a duplicate. All we need to do is
			// set the same offset in the destination.
			if off == lastRowOffset {
				offsetDeltas.UnsafeSet(i, destOffset)
				continue
			}
			suffix = b.data[lastRowOffset+uint32(shared) : off]
			// Update lastRowOffset for the next iteration of this loop.
			lastRowOffset = off
		}
		if invariants.Enabled && len(buf) < int(destOffset)+len(suffix) {
			panic(errors.AssertionFailedf("buf is too small: %d < %d", len(buf[destOffset:]), len(suffix)))
		}
		memmove(
			unsafe.Add(unsafe.Pointer(unsafe.SliceData(buf)), destOffset),
			unsafe.Pointer(unsafe.SliceData(suffix)),
			uintptr(len(suffix)),
		)
		destOffset += T(len(suffix))
		offsetDeltas.UnsafeSet(i, destOffset)
	}
	if destOffset != T(sz.compressedDataLen) {
		panic(errors.AssertionFailedf("wrote %d, expected %d", destOffset, sz.compressedDataLen))
	}
}

// Finish writes the serialized byte slices to buf starting at offset. The buf
// slice must be sufficiently large to store the serialized output. The caller
// should use [Size] to size buf appropriately before calling Finish.
//
// Finish only supports values of [rows] equal to the number of keys set on the
// builder, or one less.
func (b *PrefixBytesBuilder) Finish(
	col int, rows int, offset uint32, buf []byte,
) (endOffset uint32) {
	if rows < b.nKeys-1 || rows > b.nKeys {
		panic(errors.AssertionFailedf("PrefixBytes has accumulated %d keys, asked to Finish %d", b.nKeys, rows))
	}
	if rows == 0 {
		return offset
	}
	// Encode the bundle shift.
	buf[offset] = byte(b.bundleShift)
	offset++

	sz := &b.sizings[rows&1^1]
	stringDataOffset := uintColumnSize(uint32(sz.offsetCount), offset, sz.offsetEncoding)
	if sz.offsetEncoding.IsDelta() {
		panic(errors.AssertionFailedf("offsets never need delta encoding"))
	}

	width := uint32(sz.offsetEncoding.Width())
	buf[offset] = byte(sz.offsetEncoding)
	offset++
	offset = alignWithZeroes(buf, offset, width)
	switch width {
	case 1:
		offsetDest := makeUintsEncoder[uint8](buf[offset:], sz.offsetCount)
		writePrefixCompressed[uint8](b, rows, sz, offsetDest, buf[stringDataOffset:])
		offsetDest.Finish()
	case align16:
		offsetDest := makeUintsEncoder[uint16](buf[offset:], sz.offsetCount)
		writePrefixCompressed[uint16](b, rows, sz, offsetDest, buf[stringDataOffset:])
		offsetDest.Finish()
	case align32:
		offsetDest := makeUintsEncoder[uint32](buf[offset:], sz.offsetCount)
		writePrefixCompressed[uint32](b, rows, sz, offsetDest, buf[stringDataOffset:])
		offsetDest.Finish()
	default:
		panic("unreachable")
	}
	return stringDataOffset + uint32(sz.compressedDataLen)
}

// Size computes the size required to encode the byte slices beginning at the
// provided offset. The offset is required to ensure proper alignment. The
// returned uint32 is the offset of the first byte after the end of the encoded
// data. To compute the size in bytes, subtract the [offset] passed into Size
// from the returned offset.
func (b *PrefixBytesBuilder) Size(rows int, offset uint32) uint32 {
	if rows == 0 {
		return offset
	} else if rows != b.nKeys && rows != b.nKeys-1 {
		panic(errors.AssertionFailedf("PrefixBytes has accumulated %d keys, asked to Size %d", b.nKeys, rows))
	}
	sz := &b.sizings[rows&1^1]
	// The 1-byte bundleSize.
	offset++
	// Compute the size of the offsets table.
	offset = uintColumnSize(uint32(sz.offsetCount), offset, sz.offsetEncoding)
	return offset + uint32(sz.compressedDataLen)
}

// WriteDebug implements the Encoder interface.
func (b *PrefixBytesBuilder) WriteDebug(w io.Writer, rows int) {
	fmt.Fprintf(w, "prefixbytes(%d): %d keys", b.bundleSize, b.nKeys)
}

// bundleCalc provides facilities for computing indexes and offsets within a
// PrefixBytes structure.
type bundleCalc struct {
	bundleShift uint32 // log2(bundleSize)
	// bundleMask is a mask with 1s across the high bits that indicate the
	// bundle and 0s for the bits that indicate the position within the bundle.
	bundleMask uint32
}

func makeBundleCalc(bundleShift uint32) bundleCalc {
	return bundleCalc{
		bundleShift: bundleShift,
		bundleMask:  ^((1 << bundleShift) - 1),
	}
}

// rowSuffixIndex computes the index of the offset encoding the start of a row's
// suffix. Example usage of retrieving the row's suffix:
//
//	i := b.rowSuffixIndex(row)
//	l := b.rawBytes.offsets.At(i)
//	h := b.rawBytes.offsets.At(i + 1)
//	suffix := b.rawBytes.slice(l, h)
func (b bundleCalc) rowSuffixIndex(row int) int {
	return 1 + (row >> b.bundleShift) + row
}

// bundleOffsetIndexForRow computes the index of the offset encoding the start
// of a bundle's prefix.
func (b bundleCalc) bundleOffsetIndexForRow(row int) int {
	// AND-ing the row with the bundle mask removes the least significant bits
	// of the row, which encode the row's index within the bundle.
	return int((uint32(row) >> b.bundleShift) + (uint32(row) & b.bundleMask))
}

// offsetIndexByBundleIndex computes the index of the offset encoding the start
// of a bundle's prefix when given the bundle's index (an index in
// [0,Rows/BundleSize)).
func (b bundleCalc) offsetIndexByBundleIndex(bi int) int {
	return bi<<b.bundleShift + bi
}

func (b bundleCalc) bundleCount(rows int) int {
	return 1 + (rows-1)>>b.bundleShift
}
