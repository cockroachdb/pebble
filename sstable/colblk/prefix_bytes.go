// Copyright 2024 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package colblk

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"io"
	"math/bits"
	"strings"
	"unsafe"

	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/pebble/internal/binfmt"
	"github.com/cockroachdb/pebble/internal/invariants"
)

// PrefixBytes holds an array of lexicographically ordered byte slices. It
// provides prefix compression. Prefix compression applies strongly to two cases
// in CockroachDB: removal of the "[/tenantID]/tableID/indexID" prefix that is
// present on all table data keys, and multiple versions of a key that are
// distinguished only by different timestamp suffixes. With columnar blocks
// enabling the timestamp to be placed in a separate column, the multiple
// version problem becomes one of efficiently handling exact duplicate keys.
// PrefixBytes builds off of the RawBytes encoding, introducing n/bundleSize+1
// additional slices for encoding n/bundleSize bundle prefixes and 1 block-level
// shared prefix for the column.
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
// The offset column in the table points to the start and end index within the
// RawBytes data array for each of the 20 slices defined above (the 15 key
// suffixes + 4 bundle key prefixes + block key prefix). Offset[0] is the length
// of the first slice which is always anchored at data[0]. The data columns
// display the portion of the data array the slice covers. For row slices, an
// empty suffix column indicates that the slice is identical to the slice at the
// previous index which is indicated by the slice's offset being equal to the
// previous slice's offset. Due to the lexicographic sorting, the key at row i
// can't be a prefix of the key at row i-1 or it would have sorted before the
// key at row i-1. And if the key differs then only the differing bytes will be
// part of the suffix and not contained in the bundle prefix.
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
//	| PrefixBytes column storing n keys will encode 2+n+n/bundleSize   |
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
// the row (5*(row/4)), then the row's index within the bundle (1+(row%4)). If
// the slice's offset equals the previous slice's offset then we step backward
// until we find a non-empty slice or the start of the bundle (a variable number
// of steps, but bounded by the bundle size).
//
// Forward iteration can easily reuse the previous row's key with a check on
// whether the row's slice is empty. Reverse iteration can reuse the next row's
// key by looking at the next row's offset to determine whether we are in the
// middle of a run of equal keys or at an edge. When reverse iteration steps
// over an edge it has to continue backward until a non-empty slice is found
// (just as in absolute positioning).
//
// The Seek{GE,LT} routines first binary search on the first key of each bundle
// which can be retrieved without data movement because the bundle prefix is
// immediately adjacent to it in the data array. We can slightly optimize the
// binary search by skipping over all of the keys in the bundle on prefix
// mismatches.
type PrefixBytes struct {
	rows            int
	bundleShift     int
	bundleMask      int
	sharedPrefixLen int
	rawBytes        RawBytes
}

// MakePrefixBytes constructs an accessor for an array of lexicographically
// sorted byte slices constructed by PrefixBytesBuilder. Count must be the
// number of logical slices within the array.
func MakePrefixBytes(count int, b []byte, offset uint32, enc ColumnEncoding) PrefixBytes {
	// The first byte of a PrefixBytes-encoded column is the bundle size
	// expressed as log2 of the bundle size (the bundle size must always be a
	// power of two).
	bundleShift := int(*((*uint8)(unsafe.Pointer(&b[offset]))))
	nBundles := 1 + (count-1)>>bundleShift

	pb := PrefixBytes{
		rows:        count,
		bundleShift: bundleShift,
		bundleMask:  ^((1 << bundleShift) - 1),
		rawBytes:    MakeRawBytes(count+nBundles, b, offset+1, enc),
	}
	// We always set the base to zero.
	if pb.rawBytes.offsets.base != 0 {
		panic(errors.AssertionFailedf("unexpected non-zero base in offsets"))
	}
	pb.sharedPrefixLen = int(pb.rawBytes.offsets.At(0))
	return pb
}

// SharedPrefix return a []byte of the shared prefix that was extracted from
// all of the values in the Bytes vector. The returned slice should not be
// mutated.
func (b PrefixBytes) SharedPrefix() []byte {
	// The very first slice is the prefix for the entire column.
	return b.rawBytes.slice(0, b.rawBytes.offsets.At(0))
}

// RowBundlePrefix takes a row index and returns a []byte of the prefix shared
// among all the keys in the row's bundle. The returned slice should not be
// mutated.
func (b PrefixBytes) RowBundlePrefix(row int) []byte {
	// AND-ing the row with the bundle mask removes the least significant bits
	// of the row, which encode the row's index within the bundle.
	i := (row >> b.bundleShift) + (row & b.bundleMask)
	return b.rawBytes.slice(b.rawBytes.offsets.At(i), b.rawBytes.offsets.At(i+1))
}

// BundlePrefix returns the prefix of the i-th bundle in the column. The
// provided i must be in the range [0, BundleCount()). The returned slice should
// not be mutated.
func (b PrefixBytes) BundlePrefix(i int) []byte {
	j := (i << b.bundleShift) + i
	return b.rawBytes.slice(b.rawBytes.offsets.At(j), b.rawBytes.offsets.At(j+1))
}

// RowSuffix returns a []byte of the suffix unique to the row. A row's full key
// is the result of concatenating SharedPrefix(), BundlePrefix() and
// RowSuffix().
//
// The returned slice should not be mutated.
func (b PrefixBytes) RowSuffix(row int) []byte {
	i := 1 + (row >> b.bundleShift) + row
	// Retrieve the low and high offsets indicating the start and end of the
	// row's suffix slice.
	lowOff := b.rawBytes.offsets.At(i)
	highOff := b.rawBytes.offsets.At(i + 1)
	// If there's a non-empty slice for the row, this row is different than its
	// predecessor.
	if lowOff != highOff {
		return b.rawBytes.slice(lowOff, highOff)
	}
	// Otherwise, an empty slice indicates a duplicate key. We need to find the
	// first non-empty predecessor within the bundle, or if all the rows are
	// empty, return nil.
	//
	// Compute the index of the first row in the bundle so we know when to stop.
	firstIndex := 1 + (row >> b.bundleShift) + (row & b.bundleMask)
	for i > firstIndex {
		// Step back a row, and check if the slice is non-empty.
		i--
		highOff = lowOff
		lowOff = b.rawBytes.offsets.At(i)
		if lowOff != highOff {
			return b.rawBytes.slice(lowOff, highOff)
		}
	}
	// All the rows in the bundle are empty.
	return nil
}

// Rows returns the count of rows whose keys are encoded within the PrefixBytes.
func (b PrefixBytes) Rows() int {
	return b.rows
}

// BundleCount returns the count of bundles within the PrefixBytes.
func (b PrefixBytes) BundleCount() int {
	return 1 + (b.rows-1)>>b.bundleShift
}

// Search searchs for the first key in the PrefixBytes that is greater than or
// equal to k, returning the index of the key and whether an equal key was
// found.
func (b PrefixBytes) Search(k []byte) (int, bool) {
	// First compare to the block-level shared prefix.
	n := min(len(k), b.sharedPrefixLen)
	c := bytes.Compare(k[:n], unsafe.Slice((*byte)(b.rawBytes.data), b.sharedPrefixLen))
	switch {
	case c < 0 || (c == 0 && n < b.sharedPrefixLen):
		// Search key is less than any prefix in the block.
		return 0, false
	case c > 0:
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
		j := (h << b.bundleShift) + h
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
	j := ((bi - 1) << b.bundleShift) + (bi - 1)
	bundlePrefix := b.rawBytes.slice(b.rawBytes.offsets.At(j), b.rawBytes.offsets.At(j+1))
	if len(bundlePrefix) > len(k) || !bytes.Equal(k[:len(bundlePrefix)], bundlePrefix) {
		// The search key doesn't share the previous bundle's prefix, so all of
		// the keys in the previous bundle must be less than k. We know the
		// first key of bi is ≥ k, so return it.
		if bi<<b.bundleShift < b.rows {
			return bi << b.bundleShift, upperEqual
		}
		return b.rows, false
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
		hStart := b.rawBytes.offsets.At(j + h + 1)
		hEnd := b.rawBytes.offsets.At(j + h + 2)
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
	f *binfmt.Formatter, count int, enc ColumnEncoding, sliceFormatter func([]byte) string,
) {
	if sliceFormatter == nil {
		sliceFormatter = defaultSliceFormatter
	}
	pb := MakePrefixBytes(count, f.Data(), uint32(f.Offset()), enc)
	f.CommentLine("PrefixBytes")
	f.HexBytesln(1, "bundleSize: %d", 1<<pb.bundleShift)
	f.CommentLine("Offsets table")
	dataOffset := uint64(f.Offset()) + uint64(uintptr(pb.rawBytes.data)-uintptr(pb.rawBytes.start))
	uintsToBinFormatter(f, pb.rawBytes.slices+1, ColumnDesc{DataType: DataTypeUint32, Encoding: enc},
		func(offsetDelta, offsetBase uint64) string {
			// NB: offsetBase will always be zero for PrefixBytes columns.
			return fmt.Sprintf("%d [%d overall]", offsetDelta+offsetBase, offsetDelta+offsetBase+dataOffset)
		})
	f.CommentLine("Data")

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
}

// PrefixBytesBuilder encodes a column of lexicographically-sorted byte slices,
// applying prefix compression to reduce the encoded size.
type PrefixBytesBuilder struct {
	// TODO(jackson): If prefix compression is very effective, the encoded size
	// may remain very small while the physical in-memory size of the
	// in-progress data slice may grow very large. This may pose memory usage
	// problems during block building.
	data                      []byte // The raw, concatenated keys w/o any prefix compression
	nKeys                     int    // The number of keys added to the builder
	bundleSize                int    // The number of keys per bundle
	bundleShift               int    // log2(bundleSize)
	completedBundleLen        int    // The encoded size of completed bundles
	currentBundleDistinctLen  int    // The raw size of the current bundle's (distinct) keys
	currentBundleDistinctKeys int    // The number of physical (distinct) keys in the current bundle
	currentBundlePrefixOffset int    // The index of the offset for the current bundle prefix
	blockPrefixLen            uint32 // The length of the block-level prefix
	blockPrefixLenUpdated     int    // The row index of the last row that updated the block prefix
	lastKeyLen                int    // The length of the last key added to the builder
	lastLastKeyLen            int    // The length of the key before the last key added to the builder
	offsets                   struct {
		count int // The number of offsets in the builder
		// elemsSize is the size of the array (in count of uint32 elements; not
		// bytes)
		elemsSize int
		// elems provides access to elements without bounds checking. elems is
		// grown automatically in addOffset.
		elems UnsafeRawSlice[uint32]
	}
	maxShared uint16
}

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
		data:       b.data[:0],
		bundleSize: bundleSize,
		offsets:    b.offsets,
	}
	b.offsets.count = 0
	if b.bundleSize > 0 {
		b.bundleShift = bits.TrailingZeros32(uint32(bundleSize))
		b.maxShared = (1 << 16) - 1
	}
}

// NumColumns implements ColumnWriter.
func (b *PrefixBytesBuilder) NumColumns() int { return 1 }

// Reset resets the builder to an empty state, preserving the existing bundle
// size.
func (b *PrefixBytesBuilder) Reset() {
	*b = PrefixBytesBuilder{
		data:        b.data[:0],
		bundleSize:  b.bundleSize,
		bundleShift: b.bundleShift,
		offsets:     b.offsets,
		maxShared:   b.maxShared,
	}
	b.offsets.count = 0
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
	if invariants.Enabled {
		if b.maxShared == 0 {
			panic(errors.AssertionFailedf("maxShared must be positive"))
		}
		if b.nKeys > 0 {
			if bytes.Compare(key, b.data[len(b.data)-b.lastKeyLen:]) < 0 {
				panic(errors.AssertionFailedf("keys must be added in order: %q < %q", key, b.data[len(b.data)-b.lastKeyLen:]))
			}
			if bytesSharedWithPrev != bytesSharedPrefix(key, b.data[len(b.data)-b.lastKeyLen:]) {
				panic(errors.AssertionFailedf("bytesSharedWithPrev %d != %d", bytesSharedWithPrev,
					bytesSharedPrefix(key, b.data[len(b.data)-b.lastKeyLen:])))
			}
		}
	}

	switch {
	case b.nKeys == 0:
		// We're adding the first key to the block. Initialize the
		// block prefix to the length of this key.
		b.blockPrefixLen = uint32(min(len(key), int(b.maxShared)))
		b.blockPrefixLenUpdated = 0
		// Set a placeholder offset for the block prefix length.
		b.addOffset(0)
		// Add an offset for the bundle prefix length.
		b.addOffset(uint32(len(b.data) + min(len(key), int(b.maxShared))))
		b.nKeys++
		b.lastLastKeyLen = b.lastKeyLen
		b.lastKeyLen = len(key)
		b.currentBundleDistinctLen = len(key)
		b.currentBundlePrefixOffset = 1
		b.currentBundleDistinctKeys = 1
		b.data = append(b.data, key...)
		b.addOffset(uint32(len(b.data)))
	case b.nKeys%b.bundleSize == 0:
		// We're starting a new bundle so we can compute what the
		// encoded size of the previous bundle will be.
		bundlePrefixLen := b.offsets.elems.At(b.currentBundlePrefixOffset) - b.offsets.elems.At(b.currentBundlePrefixOffset-1)
		b.completedBundleLen += b.currentBundleDistinctLen - (b.currentBundleDistinctKeys-1)*int(bundlePrefixLen)

		// Update the block prefix length if necessary. The caller tells us how
		// many bytes of prefix this key shares with the previous key. The block
		// prefix can only shrink if the bytes shared with the previous key are
		// less than the block prefix length, in which case the new block prefix
		// is the number of bytes shared with the previous key.
		if uint32(bytesSharedWithPrev) < b.blockPrefixLen {
			b.blockPrefixLen = uint32(bytesSharedWithPrev)
			b.blockPrefixLenUpdated = b.nKeys
		}

		// We're adding the first key to the current bundle. Initialize
		// the bundle prefix to the length of this key.
		b.currentBundlePrefixOffset = b.offsets.count
		b.addOffset(uint32(len(b.data) + min(len(key), int(b.maxShared))))
		b.nKeys++
		b.lastLastKeyLen = b.lastKeyLen
		b.lastKeyLen = len(key)
		b.currentBundleDistinctLen = len(key)
		b.currentBundleDistinctKeys = 1
		b.data = append(b.data, key...)
		b.addOffset(uint32(len(b.data)))
	default:
		// Adding a new key to an existing bundle.
		// Update the bundle prefix length. Note that the shared prefix length
		// can only shrink as new values are added. During construction, the
		// bundle prefix value is stored contiguously in the data array so even
		// if the bundle prefix length changes no adjustment is needed to that
		// value or to the first key in the bundle.
		bundlePrefixLen := b.offsets.elems.At(b.currentBundlePrefixOffset) - b.offsets.elems.At(b.currentBundlePrefixOffset-1)
		if uint32(bytesSharedWithPrev) < bundlePrefixLen {
			b.offsets.elems.set(b.currentBundlePrefixOffset, b.offsets.elems.At(b.currentBundlePrefixOffset-1)+uint32(bytesSharedWithPrev))
			if uint32(bytesSharedWithPrev) < b.blockPrefixLen {
				b.blockPrefixLen = uint32(bytesSharedWithPrev)
				b.blockPrefixLenUpdated = b.nKeys
			}
		}
		b.nKeys++
		if bytesSharedWithPrev == len(key) {
			b.lastLastKeyLen = b.lastKeyLen
			b.addOffset(b.offsets.elems.At(b.offsets.count - 1))
			return
		}
		b.lastLastKeyLen = b.lastKeyLen
		b.lastKeyLen = len(key)
		b.currentBundleDistinctLen += len(key)
		b.currentBundleDistinctKeys++
		b.data = append(b.data, key...)
		b.addOffset(uint32(len(b.data)))
	}
}

// PrevKey returns the previous key added to the builder through Put. The key is
// guaranteed to be stable until Finish or Reset is called.
func (b *PrefixBytesBuilder) PrevKey() []byte {
	return b.data[len(b.data)-b.lastKeyLen:]
}

// addOffset adds an offset to the offsets table. If necessary, addOffset will
// grow the offset table to accommodate the new offset.
func (b *PrefixBytesBuilder) addOffset(offset uint32) {
	if b.offsets.count == b.offsets.elemsSize {
		// Double the size of the allocated array, or initialize it to at least
		// 64 rows if this is the first allocation.
		n2 := max(b.offsets.elemsSize<<1, 64)
		newDataTyped := make([]uint32, n2)
		copy(newDataTyped, b.offsets.elems.Slice(b.offsets.elemsSize))
		b.offsets.elems = makeUnsafeRawSlice[uint32](unsafe.Pointer(&newDataTyped[0]))
		b.offsets.elemsSize = n2
	}
	b.offsets.elems.set(b.offsets.count, offset)
	b.offsets.count++
}

// prefixCompressionSizing holds the sizing information for the
// prefix-compressed data structure. It's used in
// PrefixBytesBuilder.{Size,Finish}.
type prefixCompressionSizing struct {
	offsetCount       uint32 // number of offsets that will be encoded
	offsetDeltaWidth  uint32 // width of each offset delta (1, 2 or 4)
	compressedDataLen uint32 // length of the compressed string data
	blockPrefixLen    uint32 // length of the prefix shared by all keys
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
	currentBundlePrefixLen uint32
}

// computePrefixCompressionSizing computes the size of the prefix-compressed
// PrefixBytes structure's components if encoding the first [rows] rows. Like
// [Finish], computePrefixCompressionSizing requires [rows] == nKeys or nKeys-1.
func (b *PrefixBytesBuilder) computePrefixCompressionSizing(
	rows int,
) (sizing prefixCompressionSizing) {
	// To encode [rows] rows, we need to encode:
	// - 1 block prefix,
	// - (rows-1)/bundleSize+1 bundle prefixes
	// - [rows] per-row suffixes
	sizing.offsetCount = uint32(2 + (rows-1)>>b.bundleShift + rows)

	if rows <= 1 {
		// If we haven't added enough offsets to perform prefix compression,
		// then the compressed length is the uncompressed length.
		sizing.compressedDataLen = b.offsets.elems.At(int(sizing.offsetCount) - 1)
		sizing.offsetDeltaWidth = max(1, deltaWidth(uint64(sizing.compressedDataLen)))
		sizing.blockPrefixLen = sizing.compressedDataLen
		sizing.currentBundlePrefixLen = sizing.compressedDataLen
		return sizing
	}

	// We support computing the prefix-compressed size of serializing either
	// with or without the last row (eg, rows == b.nKeys or rows == b.nKeys-1).
	// Regardless, the completedBundle{Len,Keys} fields are accurate because a
	// bundle is only considered completed once the first key of the next bundle
	// is added.
	//
	// Any difference in the size comes from
	// a) a difference in the current bundle size
	// b) a difference in the block-prefix len
	//
	// We compute a) here and b) down below.
	currentBundleDistinctLen := b.currentBundleDistinctLen
	currentBundleDistinctKeys := b.currentBundleDistinctKeys
	sizing.currentBundlePrefixLen = b.offsets.elems.At(b.currentBundlePrefixOffset) -
		b.offsets.elems.At(b.currentBundlePrefixOffset-1)
	if int(rows) == b.nKeys-1 {
		// Determine if the last key (the trimmed key) was a duplicate of the
		// preceding key. We need to know this to adjust the current bundle
		// sizing correctly, because duplicate keys are NOT appended to b.data
		// and NOT represented within currentBundleDistinctLen or
		// currentBundleDistinctKeys.
		trimmedKeyStartOffset := b.offsets.elems.At(int(sizing.offsetCount) - 1)
		trimmedKeyEndOffset := b.offsets.elems.At(int(sizing.offsetCount))
		if trimmedKeyStartOffset != trimmedKeyEndOffset {
			// Not a duplicate key; trimming this key needs to adjust the
			// current bundle data stats.
			currentBundleDistinctKeys--
			currentBundleDistinctLen -= b.lastKeyLen
		}
		// The bundle prefix length currently saved in the offsets table may be
		// shorter than it needs to be. We can compute the correct bundle
		// length. Note that if there's now only 1 distinct key, bundleFirstKey
		// and bundleLastKey may be the same key, but that's okay—we'll just
		// compute a shared prefix that's the entirety of the key.
		bundleFirstKey := b.data[b.offsets.elems.At(b.currentBundlePrefixOffset-1):b.offsets.elems.At(b.currentBundlePrefixOffset+1)]
		bundleLastKey := b.data[trimmedKeyStartOffset-uint32(b.lastLastKeyLen) : trimmedKeyStartOffset]
		sizing.currentBundlePrefixLen = min(uint32(b.maxShared), uint32(bytesSharedPrefix(bundleFirstKey, bundleLastKey)))
	} else if int(rows) != b.nKeys {
		panic(errors.AssertionFailedf("PrefixBytes has accumulated %d keys, asked to size %d", b.nKeys, rows))
	}

	// Compute the block prefix length for the first [rows] rows. If [rows]
	// includes all rows that have been Put, we can use a cached value.
	// Otherwise, we need to recompute it the block prefix len by computing the
	// shared prefix between the first and last bundles.
	sizing.blockPrefixLen = b.blockPrefixLen
	if b.blockPrefixLenUpdated >= rows {
		// If we're not writing out all the rows that have been Put (ie, rows <
		// b.nKeys), and one of the later rows that we're not writing updated the
		// block prefix length, then the cached block prefix length may be shorter
		// than necessary. In this case, we recompute it from the bundle prefixes.
		// Initialize the block prefix length to the current bundle prefix.
		sizing.blockPrefixLen = sizing.currentBundlePrefixLen
		if rows > b.bundleSize {
			bi := (rows - 1) >> b.bundleShift
			// Find the last bundle prefix. If the current bundle is not empty,
			// the current bundle is the last bundle and we should use its
			// length rather than the currently saved end offset (which would've
			// been modified by the trimmed key that we're omitting).
			i := bi<<b.bundleShift + bi
			lastBundlePrefixStart := b.offsets.elems.At(i)
			lastBundlePrefixEnd := b.offsets.elems.At(i + 1)
			if currentBundleDistinctKeys > 0 {
				lastBundlePrefixEnd = lastBundlePrefixStart + sizing.currentBundlePrefixLen
			}
			sizing.blockPrefixLen = uint32(bytesSharedPrefix(
				b.data[b.offsets.elems.At(0):b.offsets.elems.At(1)],
				b.data[lastBundlePrefixStart:lastBundlePrefixEnd]))
		}
	}

	// Validate the invariant that blockPrefixLen <= currentBundlePrefixLen.
	if invariants.Enabled && sizing.blockPrefixLen > sizing.currentBundlePrefixLen {
		panic(errors.AssertionFailedf("block prefix len %d > current bundle prefix len %d",
			sizing.blockPrefixLen, sizing.currentBundlePrefixLen))
	}

	// Compute the length of the compressed string data. The
	// b.completedBundleLen is pre-computed and holds length of the compressed
	// string data for all keys except the "current" bundle's keys. Note that
	// [completedBundleLen] assumes a blockPrefixLen=0, so we'll need to adjust
	// for the block prefix length down below.
	sizing.compressedDataLen = uint32(b.completedBundleLen)
	// If there are any keys that aren't included within the 'completed
	// bundles,' add them.
	if currentBundleDistinctKeys > 0 {
		// Adjust the current bundle length by stripping off the bundle prefix
		// from all but one of the keys in the bundle (which is accounting for
		// storage of the bundle prefix itself).
		sizing.compressedDataLen += uint32(currentBundleDistinctLen) - uint32(currentBundleDistinctKeys-1)*sizing.currentBundlePrefixLen
	}
	// Currently compressedDataLen is correct, except that it includes the block
	// prefix length for all bundle prefixes. Adjust the length to account for
	// the block prefix being stripped from every bundle except the first one.
	numBundles := uint32(rows+b.bundleSize-1) >> b.bundleShift
	sizing.compressedDataLen -= (numBundles - 1) * sizing.blockPrefixLen
	// The compressedDataLen is the largest offset we'll need to encode in the
	// offset table, so we can use it to compute the width of the offset deltas.
	sizing.offsetDeltaWidth = max(1, deltaWidth(uint64(sizing.compressedDataLen)))
	return sizing
}

// writePrefixCompressed writes the provided builder's first [rows] rows with
// prefix-compression applied. It writes offsets and string data in tandem,
// writing offsets of width T into [offsetDeltas] and compressed string data
// into [buf]. The builder's internal state is not modified by
// writePrefixCompressed. writePrefixCompressed is generic in terms of the type
// T of the offset deltas.
//
// The caller must have correctly constructed [offsetDeltas] such that writing
// [sizing.nOffsets] offsets of size T does not overwrite the beginning of
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
	b *PrefixBytesBuilder,
	rows int,
	sizing prefixCompressionSizing,
	offsetDeltas UnsafeRawSlice[T],
	buf []byte,
) {
	if rows == 0 {
		return
	} else if rows == 1 {
		// If there's just 1 row, no prefix compression is necessary and we can
		// just encode the first key as the entire block prefix and first bundle
		// prefix.
		e := b.offsets.elems.At(2)
		offsetDeltas.set(0, T(e))
		offsetDeltas.set(1, T(e))
		offsetDeltas.set(2, T(e))
		copy(buf, b.data[:e])
		return
	}

	// The offset at index 0 is the block prefix length.
	offsetDeltas.set(0, T(sizing.blockPrefixLen))
	destOffset := T(copy(buf, b.data[:sizing.blockPrefixLen]))
	var lastRowOffset uint32
	var shared uint32

	// Loop over the slices starting at the bundle prefix of the first bundle.
	// If the slice is a bundle prefix, carve off the suffix that excludes the
	// block prefix. Otherwise, carve off the suffix that excludes the block
	// prefix + bundle prefix.
	for i := 1; i < int(sizing.offsetCount); i++ {
		off := b.offsets.elems.At(i)
		var suffix []byte
		if (i-1)%(b.bundleSize+1) == 0 {
			// This is a bundle prefix.
			if i == b.currentBundlePrefixOffset {
				suffix = b.data[lastRowOffset+sizing.blockPrefixLen : lastRowOffset+sizing.currentBundlePrefixLen]
			} else {
				suffix = b.data[lastRowOffset+sizing.blockPrefixLen : off]
			}
			shared = sizing.blockPrefixLen + uint32(len(suffix))
			// We don't update lastRowOffset here because the bundle prefix
			// was never actually stored separately in the data array.
		} else {
			// If the offset of this key is the same as the offset of the
			// previous key, then the key is a duplicate. All we need to do is
			// set the same offset in the destination.
			if off == lastRowOffset {
				offsetDeltas.set(i, offsetDeltas.At(i-1))
				continue
			}
			suffix = b.data[lastRowOffset+shared : off]
			// Update lastRowOffset for the next iteration of this loop.
			lastRowOffset = off
		}
		if invariants.Enabled && len(buf[destOffset:]) < len(suffix) {
			panic(errors.AssertionFailedf("buf is too small: %d < %d", len(buf[destOffset:]), len(suffix)))
		}
		destOffset += T(copy(buf[destOffset:], suffix))
		offsetDeltas.set(i, destOffset)
	}
	if destOffset != T(sizing.compressedDataLen) {
		panic(errors.AssertionFailedf("wrote %d, expected %d", destOffset, sizing.compressedDataLen))
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
) (uint32, ColumnDesc) {
	if rows < b.nKeys-1 || rows > b.nKeys {
		panic(errors.AssertionFailedf("PrefixBytes has accumulated %d keys, asked to Finish %d", b.nKeys, rows))
	}

	desc := ColumnDesc{DataType: DataTypePrefixBytes}
	if rows == 0 {
		return offset, desc
	}
	// Encode the bundle shift.
	buf[offset] = byte(b.bundleShift)
	offset++

	// Compute the length of the prefix-compressed data. The size of the offsets
	// table is dependent on the value of the largest offset, which is dependent
	// on the length of the compressed data.
	sizing := b.computePrefixCompressionSizing(rows)

	// With the sizing computed, we can compute the size of the offsets table.
	// The actual offsets themselves haven't been computed yet, but precomputing
	// the size allows us to write the offset table and the string data to buf
	// in tandem.
	stringDataOffset := uintColumnSize[uint32](sizing.offsetCount, offset, sizing.offsetDeltaWidth)
	switch sizing.offsetDeltaWidth {
	case 1:
		// The uint32 delta-encoding requires a uint32 constant representing the
		// base value. This is always zero for PrefixBytes, but we include it
		// anyways for format consistency.
		buf[offset], buf[offset+1], buf[offset+2], buf[offset+3] = 0, 0, 0, 0
		offset += align32

		offsetDest := makeUnsafeRawSlice[uint8](unsafe.Pointer(&buf[offset]))
		desc.Encoding = desc.Encoding.WithDelta(DeltaEncodingUint8)
		writePrefixCompressed[uint8](b, rows, sizing, offsetDest, buf[stringDataOffset:])
	case align16:
		// The uint32 delta-encoding requires a uint32 constant representing the
		// base value. This is always zero for PrefixBytes, but we include it
		// anyways for format consistency.
		buf[offset], buf[offset+1], buf[offset+2], buf[offset+3] = 0, 0, 0, 0
		offset += align32

		offset = alignWithZeroes(buf, offset, align16)
		offsetDest := makeUnsafeRawSlice[uint16](unsafe.Pointer(&buf[offset]))
		desc.Encoding = desc.Encoding.WithDelta(DeltaEncodingUint16)
		writePrefixCompressed[uint16](b, rows, sizing, offsetDest, buf[stringDataOffset:])
	case align32:
		offset = alignWithZeroes(buf, offset, align32)
		offsetDest := makeUnsafeRawSlice[uint32](unsafe.Pointer(&buf[offset]))
		writePrefixCompressed[uint32](b, rows, sizing, offsetDest, buf[stringDataOffset:])
	default:
		panic("unreachable")
	}
	return stringDataOffset + sizing.compressedDataLen, desc
}

// Size computes the size required to encode the byte slices beginning at the
// provided offset. The offset is required to ensure proper alignment. The
// returned uint32 is the offset of the first byte after the end of the encoded
// data. To compute the size in bytes, subtract the [offset] passed into Size
// from the returned offset.
func (b *PrefixBytesBuilder) Size(rows int, offset uint32) uint32 {
	if rows == 0 {
		return 0
	}
	// The 1-byte bundleSize.
	offset++

	// Compute the prefix-compressed sizing.
	sizing := b.computePrefixCompressionSizing(rows)
	// Compute the size of the offsets table.
	offset = uintColumnSize[uint32](sizing.offsetCount, offset, sizing.offsetDeltaWidth)
	return offset + sizing.compressedDataLen
}

// WriteDebug implements the Encoder interface.
func (b *PrefixBytesBuilder) WriteDebug(w io.Writer, rows int) {
	fmt.Fprintf(w, "prefixbytes(%d): %d keys", b.bundleSize, b.nKeys)
}

// bytesSharedPrefix returns the length of the shared prefix between a and b.
func bytesSharedPrefix(a, b []byte) int {
	asUint64 := func(data []byte, i int) uint64 {
		return binary.LittleEndian.Uint64(data[i:])
	}
	var shared int
	n := min(len(a), len(b))
	for shared < n-7 && asUint64(a, shared) == asUint64(b, shared) {
		shared += 8
	}
	for shared < n && a[shared] == b[shared] {
		shared++
	}
	return shared
}
