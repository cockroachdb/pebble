// Copyright 2024 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package colblk

import (
	"bytes"
	"context"
	"encoding/binary"
	"fmt"

	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/pebble/internal/base"
	"github.com/cockroachdb/pebble/internal/binfmt"
	"github.com/cockroachdb/pebble/internal/invariants"
	"github.com/cockroachdb/pebble/internal/keyspan"
	"github.com/cockroachdb/pebble/internal/treeprinter"
)

// the keyspan header encodes a 32-bit count of the number of unique boundary
// user keys in the block.
const keyspanHeaderSize = 4

// keyspan block column indexes
const (
	// Columns with 1 row per unique boundary user key contained within the
	// block (with the count indicated via the keyspan custom block header).
	keyspanColBoundaryUserKeys   = 0
	keyspanColBoundaryKeyIndices = 1
	// Columns with 1 row per keyspan.Key (with the count indicated via the
	// columnar header's row count).
	keyspanColTrailers = 2
	keyspanColSuffixes = 3
	keyspanColValues   = 4
	keyspanColumnCount = 5
)

// A KeyspanBlockWriter writes keyspan blocks. See the colblk package
// documentation for more details on the schema.
type KeyspanBlockWriter struct {
	equal base.Equal

	// boundary columns
	boundaryUserKeys   RawBytesBuilder
	boundaryKeyIndexes UintBuilder

	// keyspan.Key columns
	trailers UintBuilder
	suffixes RawBytesBuilder
	values   RawBytesBuilder

	enc               blockEncoder
	keyCount          int
	unsafeLastUserKey []byte
}

// Init initializes a keyspan block writer.
func (w *KeyspanBlockWriter) Init(equal base.Equal) {
	w.equal = equal
	w.boundaryUserKeys.Init()
	w.boundaryKeyIndexes.Init()
	w.trailers.Init()
	w.suffixes.Init()
	w.values.Init()
	w.keyCount = 0
	w.unsafeLastUserKey = nil
}

// Reset resets the keyspan block writer to an empty state, retaining memory for
// reuse.
func (w *KeyspanBlockWriter) Reset() {
	w.boundaryUserKeys.Reset()
	w.boundaryKeyIndexes.Reset()
	w.trailers.Reset()
	w.suffixes.Reset()
	w.values.Reset()
	w.enc.reset()
	w.keyCount = 0
	w.unsafeLastUserKey = nil
}

// AddSpan appends a new Span to the pending block. Spans must already be
// fragmented (non-overlapping) and added in sorted order.
func (w *KeyspanBlockWriter) AddSpan(s keyspan.Span) {
	// When keyspans are fragmented, abutting spans share a user key. One span's
	// end key is the next span's start key.  Check if the previous user key
	// equals this span's start key, and avoid encoding it again if so.
	if w.unsafeLastUserKey == nil || !w.equal(w.unsafeLastUserKey, s.Start) {
		w.boundaryKeyIndexes.Set(w.boundaryUserKeys.rows, uint64(w.keyCount))
		w.boundaryUserKeys.Put(s.Start)
	}
	// The end key must be strictly greater than the start key and spans are
	// already sorted, so the end key is guaranteed to not be present in the
	// column yet. We need to encode it.
	w.boundaryKeyIndexes.Set(w.boundaryUserKeys.rows, uint64(w.keyCount+len(s.Keys)))
	w.boundaryUserKeys.Put(s.End)

	// Hold on to a slice of the copy of s.End we just added to the bytes
	// builder so that we can compare it to the next span's start key.
	w.unsafeLastUserKey = w.boundaryUserKeys.data[len(w.boundaryUserKeys.data)-len(s.End):]

	// Encode each keyspan.Key in the span.
	for i := range s.Keys {
		w.trailers.Set(w.keyCount, uint64(s.Keys[i].Trailer))
		w.suffixes.Put(s.Keys[i].Suffix)
		w.values.Put(s.Keys[i].Value)
		w.keyCount++
	}
}

// KeyCount returns the count of keyspan.Keys written to the writer.
func (w *KeyspanBlockWriter) KeyCount() int {
	return w.keyCount
}

// UnsafeBoundaryKeys returns the smallest and largest keys written to the
// keyspan block so far. The returned internal keys have user keys that point
// directly into the block writer's memory and must not be mutated.
func (w *KeyspanBlockWriter) UnsafeBoundaryKeys() (smallest, largest base.InternalKey) {
	if w.keyCount == 0 {
		return smallest, largest
	}
	smallest.UserKey = w.boundaryUserKeys.UnsafeGet(0)
	smallest.Trailer = base.InternalKeyTrailer(w.trailers.Get(0))
	largest.UserKey = w.boundaryUserKeys.UnsafeGet(w.boundaryUserKeys.rows - 1)
	largest.Trailer = base.MakeTrailer(base.SeqNumMax,
		base.InternalKeyTrailer(w.trailers.Get(w.keyCount-1)).Kind())
	return smallest, largest
}

// Size returns the size of the pending block.
func (w *KeyspanBlockWriter) Size() int {
	off := blockHeaderSize(keyspanColumnCount, keyspanHeaderSize)
	// Span boundary columns (with userKeyCount elements).
	off = w.boundaryUserKeys.Size(w.boundaryUserKeys.rows, off)
	off = w.boundaryKeyIndexes.Size(w.boundaryUserKeys.rows, off)

	// keyspan.Key columns (with keyCount elements).
	off = w.trailers.Size(w.keyCount, off)
	off = w.suffixes.Size(w.keyCount, off)
	off = w.values.Size(w.keyCount, off)
	off++ // trailing padding
	return int(off)
}

// Finish finalizes the pending block and returns the encoded block.
func (w *KeyspanBlockWriter) Finish() []byte {
	w.enc.init(w.Size(), Header{
		Version: Version1,
		Columns: keyspanColumnCount,
		Rows:    uint32(w.keyCount),
	}, keyspanHeaderSize)

	// The keyspan block has a 4-byte custom header used to encode the number of
	// user keys encoded within the user key and start indices columns. All
	// other columns have the number of rows indicated by the shared columnar
	// block header.
	binary.LittleEndian.PutUint32(w.enc.data()[:keyspanHeaderSize], uint32(w.boundaryUserKeys.rows))

	// Columns with userKeyCount elements.
	w.enc.encode(w.boundaryUserKeys.rows, &w.boundaryUserKeys)
	w.enc.encode(w.boundaryUserKeys.rows, &w.boundaryKeyIndexes)
	// Columns with keyCount elements.
	w.enc.encode(w.keyCount, &w.trailers)
	w.enc.encode(w.keyCount, &w.suffixes)
	w.enc.encode(w.keyCount, &w.values)
	return w.enc.finish()
}

// String returns a string representation of the pending block's state.
func (w *KeyspanBlockWriter) String() string {
	var buf bytes.Buffer
	size := uint32(w.Size())
	fmt.Fprintf(&buf, "size=%d:\n", size)

	fmt.Fprint(&buf, "0: user keys:      ")
	w.boundaryUserKeys.WriteDebug(&buf, w.boundaryUserKeys.rows)
	fmt.Fprintln(&buf)
	fmt.Fprint(&buf, "1: start indices:  ")
	w.boundaryKeyIndexes.WriteDebug(&buf, w.boundaryUserKeys.rows)
	fmt.Fprintln(&buf)

	fmt.Fprint(&buf, "2: trailers:       ")
	w.trailers.WriteDebug(&buf, w.keyCount)
	fmt.Fprintln(&buf)
	fmt.Fprint(&buf, "3: suffixes:       ")
	w.suffixes.WriteDebug(&buf, w.keyCount)
	fmt.Fprintln(&buf)
	fmt.Fprint(&buf, "4: values:         ")
	w.values.WriteDebug(&buf, w.keyCount)
	fmt.Fprintln(&buf)

	return buf.String()
}

// A KeyspanReader exposes facilities for reading a keyspan block. A
// KeyspanReader is safe for concurrent use and may be used by multiple
// KeyspanIters concurrently.
type KeyspanReader struct {
	blockReader BlockReader
	// Span boundary columns with boundaryKeysCount elements.
	boundaryKeysCount  uint32
	boundaryKeys       RawBytes
	boundaryKeyIndices UnsafeUints

	// keyspan.Key columns with blockReader.header.Rows elements.
	trailers UnsafeUints
	suffixes RawBytes
	values   RawBytes
}

// Init initializes the keyspan reader with the given block data.
func (r *KeyspanReader) Init(data []byte) {
	r.boundaryKeysCount = binary.LittleEndian.Uint32(data[:4])
	r.blockReader.Init(data, keyspanHeaderSize)
	// The boundary key columns have a different number of rows than the other
	// columns, so we call DecodeColumn directly, taking care to pass in
	// rows=r.boundaryKeysCount.
	r.boundaryKeys = DecodeColumn(&r.blockReader, keyspanColBoundaryUserKeys,
		int(r.boundaryKeysCount), DataTypeBytes, DecodeRawBytes)
	r.boundaryKeyIndices = DecodeColumn(&r.blockReader, keyspanColBoundaryKeyIndices,
		int(r.boundaryKeysCount), DataTypeUint, DecodeUnsafeUints)

	r.trailers = r.blockReader.Uints(keyspanColTrailers)
	r.suffixes = r.blockReader.RawBytes(keyspanColSuffixes)
	r.values = r.blockReader.RawBytes(keyspanColValues)
}

// DebugString prints a human-readable explanation of the keyspan block's binary
// representation.
func (r *KeyspanReader) DebugString() string {
	f := binfmt.New(r.blockReader.data).LineWidth(20)
	r.Describe(f)
	return f.String()
}

// Describe describes the binary format of the keyspan block, assuming
// f.Offset() is positioned at the beginning of the same keyspan block described
// by r.
func (r *KeyspanReader) Describe(f *binfmt.Formatter) {
	// Set the relative offset. When loaded into memory, the beginning of blocks
	// are aligned. Padding that ensures alignment is done relative to the
	// current offset. Setting the relative offset ensures that if we're
	// describing this block within a larger structure (eg, f.Offset()>0), we
	// compute padding appropriately assuming the current byte f.Offset() is
	// aligned.
	f.SetAnchorOffset()

	f.CommentLine("keyspan block header")
	f.HexBytesln(4, "user key count: %d", r.boundaryKeysCount)
	r.blockReader.headerToBinFormatter(f)

	for i := 0; i < keyspanColumnCount; i++ {
		// Not all columns in a keyspan block have the same number of rows; the
		// boundary columns columns are different (and their lengths are held in
		// the keyspan block header that precedes the ordinary columnar block
		// header).
		rows := int(r.blockReader.header.Rows)
		if i == keyspanColBoundaryUserKeys || i == keyspanColBoundaryKeyIndices {
			rows = int(r.boundaryKeysCount)
		}
		r.blockReader.columnToBinFormatter(f, i, rows)
	}
	f.HexBytesln(1, "block padding byte")
}

// searchBoundaryKeys returns the index of the first boundary key greater than
// or equal to key and whether or not the key was found exactly.
func (r *KeyspanReader) searchBoundaryKeys(cmp base.Compare, key []byte) (index int, equal bool) {
	i, j := 0, int(r.boundaryKeysCount)
	for i < j {
		h := int(uint(i+j) >> 1) // avoid overflow when computing h
		// i ≤ h < j
		switch cmp(key, r.boundaryKeys.At(h)) {
		case +1:
			i = h + 1
		case 0:
			return h, true
		default:
			// -1
			j = h
		}
	}
	return i, false
}

// A KeyspanIter is an iterator over a keyspan block. It implements the
// keyspan.FragmentIterator interface.
type KeyspanIter struct {
	r    *KeyspanReader
	cmp  base.Compare
	span keyspan.Span
	// When positioned, the current span's start key is the user key at
	//   i.r.userKeys.At(i.startBoundIndex)
	// and the current span's end key is the user key at
	//   i.r.userKeys.At(i.startBoundIndex+1)
	startBoundIndex int
	keyBuf          [2]keyspan.Key
}

// Assert that KeyspanIter implements the FragmentIterator interface.
var _ keyspan.FragmentIterator = (*KeyspanIter)(nil)

// Init initializes the iterator with the given comparison function and keyspan
// reader.
func (i *KeyspanIter) Init(cmp base.Compare, r *KeyspanReader) {
	i.r = r
	i.cmp = cmp
	i.span.Start, i.span.End = nil, nil
	i.startBoundIndex = -1
	if i.span.Keys == nil {
		i.span.Keys = i.keyBuf[:0]
	}
}

// SeekGE moves the iterator to the first span covering a key greater than
// or equal to the given key. This is equivalent to seeking to the first
// span with an end key greater than the given key.
func (i *KeyspanIter) SeekGE(key []byte) (*keyspan.Span, error) {
	// Seek among the boundary keys.
	j, eq := i.r.searchBoundaryKeys(i.cmp, key)
	// If the found boundary key does not exactly equal the given key, it's
	// strictly greater than key. We need to back up one to consider the span
	// that ends at the this boundary key.
	if !eq {
		j = max(j-1, 0)
	}
	return i.gatherKeysForward(j), nil
}

// SeekLT moves the iterator to the last span covering a key less than the
// given key. This is equivalent to seeking to the last span with a start
// key less than the given key.
func (i *KeyspanIter) SeekLT(key []byte) (*keyspan.Span, error) {
	// Seek among the boundary keys.
	j, eq := i.r.searchBoundaryKeys(i.cmp, key)
	// If eq is true, the found boundary key exactly equals the given key. A
	// span that starts at exactly [key] does not contain any keys strictly less
	// than [key], so back up one index.
	if eq {
		j--
	}
	// If all boundaries are less than [key], or only the last boundary is
	// greater than the key, then we want the last span so we clamp the index to
	// the second to last boundary.
	return i.gatherKeysBackward(min(j, int(i.r.boundaryKeysCount)-2)), nil
}

// First moves the iterator to the first span.
func (i *KeyspanIter) First() (*keyspan.Span, error) {
	return i.gatherKeysForward(0), nil
}

// Last moves the iterator to the last span.
func (i *KeyspanIter) Last() (*keyspan.Span, error) {
	return i.gatherKeysBackward(int(i.r.boundaryKeysCount) - 2), nil
}

// Next moves the iterator to the next span.
func (i *KeyspanIter) Next() (*keyspan.Span, error) {
	return i.gatherKeysForward(i.startBoundIndex + 1), nil
}

// Prev moves the iterator to the previous span.
func (i *KeyspanIter) Prev() (*keyspan.Span, error) {
	return i.gatherKeysBackward(max(i.startBoundIndex-1, -1)), nil
}

// gatherKeysForward returns the first non-empty Span in the forward direction,
// starting with the span formed by using the boundary key at index
// [startBoundIndex] as the span's start boundary.
func (i *KeyspanIter) gatherKeysForward(startBoundIndex int) *keyspan.Span {
	if invariants.Enabled && startBoundIndex < 0 {
		panic(errors.AssertionFailedf("out of bounds: i.startBoundIndex=%d", startBoundIndex))
	}
	i.startBoundIndex = startBoundIndex
	if i.startBoundIndex >= int(i.r.boundaryKeysCount)-1 {
		return nil
	}
	if !i.isNonemptySpan(i.startBoundIndex) {
		if i.startBoundIndex == int(i.r.boundaryKeysCount)-2 {
			// Corruption error
			panic(base.CorruptionErrorf("keyspan block has empty span at end"))
		}
		i.startBoundIndex++
		if !i.isNonemptySpan(i.startBoundIndex) {
			panic(base.CorruptionErrorf("keyspan block has consecutive empty spans"))
		}
	}
	return i.materializeSpan()
}

// gatherKeysBackward returns the first non-empty Span in the backward direction,
// starting with the span formed by using the boundary key at index
// [startBoundIndex] as the span's start boundary.
func (i *KeyspanIter) gatherKeysBackward(startBoundIndex int) *keyspan.Span {
	i.startBoundIndex = startBoundIndex
	if i.startBoundIndex < 0 {
		return nil
	}
	if invariants.Enabled && i.startBoundIndex >= int(i.r.boundaryKeysCount)-1 {
		panic(errors.AssertionFailedf("out of bounds: i.startBoundIndex=%d, i.r.boundaryKeysCount=%d",
			i.startBoundIndex, i.r.boundaryKeysCount))
	}
	if !i.isNonemptySpan(i.startBoundIndex) {
		if i.startBoundIndex == 0 {
			// Corruption error
			panic(base.CorruptionErrorf("keyspan block has empty span at beginning"))
		}
		i.startBoundIndex--
		if !i.isNonemptySpan(i.startBoundIndex) {
			panic(base.CorruptionErrorf("keyspan block has consecutive empty spans"))
		}
	}
	return i.materializeSpan()
}

// isNonemptySpan returns true if the span starting at i.startBoundIndex
// contains keys.
func (i *KeyspanIter) isNonemptySpan(startBoundIndex int) bool {
	return i.r.boundaryKeyIndices.At(startBoundIndex) < i.r.boundaryKeyIndices.At(startBoundIndex+1)
}

// materializeSpan constructs the current span from i.startBoundIndex and
// i.{start,end}KeyIndex.
func (i *KeyspanIter) materializeSpan() *keyspan.Span {
	i.span = keyspan.Span{
		Start: i.r.boundaryKeys.At(i.startBoundIndex),
		End:   i.r.boundaryKeys.At(i.startBoundIndex + 1),
		Keys:  i.span.Keys[:0],
	}
	startIndex := i.r.boundaryKeyIndices.At(i.startBoundIndex)
	endIndex := i.r.boundaryKeyIndices.At(i.startBoundIndex + 1)
	if cap(i.span.Keys) < int(endIndex-startIndex) {
		i.span.Keys = make([]keyspan.Key, 0, int(endIndex-startIndex))
	}
	for j := startIndex; j < endIndex; j++ {
		i.span.Keys = append(i.span.Keys, keyspan.Key{
			Trailer: base.InternalKeyTrailer(i.r.trailers.At(int(j))),
			Suffix:  i.r.suffixes.At(int(j)),
			Value:   i.r.values.At(int(j)),
		})
	}
	return &i.span
}

// Close closes the iterator.
func (i *KeyspanIter) Close() {}

// SetContext implements keyspan.FragmentIterator.
func (i *KeyspanIter) SetContext(context.Context) {}

// WrapChildren implements keyspan.FragmentIterator.
func (i *KeyspanIter) WrapChildren(keyspan.WrapFn) {}

// DebugTree is part of the FragmentIterator interface.
func (i *KeyspanIter) DebugTree(tp treeprinter.Node) {
	tp.Childf("%T(%p)", i, i)
}
