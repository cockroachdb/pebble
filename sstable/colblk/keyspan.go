// Copyright 2024 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package colblk

import (
	"bytes"
	"context"
	"encoding/binary"
	"fmt"
	"math"

	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/pebble/internal/base"
	"github.com/cockroachdb/pebble/internal/binfmt"
	"github.com/cockroachdb/pebble/internal/invariants"
	"github.com/cockroachdb/pebble/internal/keyspan"
	"github.com/cockroachdb/pebble/internal/treeprinter"
)

const keyspanHeaderSize = 4
const maxKeyspanBlockRetainedSize = 32 << 10

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
	boundaryKeyIndexes UintBuilder[uint32]

	// keyspan.Key columns
	trailers UintBuilder[uint64]
	suffixes RawBytesBuilder
	values   RawBytesBuilder

	buf               []byte
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
	if cap(w.buf) > maxKeyspanBlockRetainedSize {
		w.buf = nil
	} else {
		w.buf = w.buf[:0]
	}
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
	if cap(w.buf) > maxKeyspanBlockRetainedSize {
		w.buf = nil
	} else {
		w.buf = w.buf[:0]
	}
	w.keyCount = 0
	w.unsafeLastUserKey = nil
}

// AddSpan appends a new Span to the pending block. Spans must already be
// fragmented (non-overlapping) and added in sorted order.
func (w *KeyspanBlockWriter) AddSpan(s *keyspan.Span) {
	// When keyspans are fragmented, abutting spans share a user key. One span's
	// end key is the next span's start key.  Check if the previous user key
	// equals this span's start key, and avoid encoding it again if so.
	if w.unsafeLastUserKey == nil || !w.equal(w.unsafeLastUserKey, s.Start) {
		w.boundaryKeyIndexes.Set(w.boundaryUserKeys.rows, uint32(w.keyCount))
		w.boundaryUserKeys.Put(s.Start)
	}
	// The end key must be strictly greater than the start key and spans are
	// already sorted, so the end key is guaranteed to not be present in the
	// column yet. We need to encode it.
	w.boundaryKeyIndexes.Set(w.boundaryUserKeys.rows, uint32(w.keyCount+len(s.Keys)))
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
	size := w.Size()
	if cap(w.buf) < size {
		w.buf = make([]byte, size)
	}
	w.buf = w.buf[:size]

	// The keyspan block has a 4-byte custom header used to encode the number of
	// user keys encoded within the user key and start indices columns. All
	// other columns have the number of rows indicated by the shared columnar
	// block header.
	binary.LittleEndian.PutUint32(w.buf, uint32(w.boundaryUserKeys.rows))

	Header{
		Version: Version1,
		Columns: uint16(keyspanColumnCount),
		Rows:    uint32(w.keyCount),
	}.Encode(w.buf[keyspanHeaderSize:])

	pageOffset := blockHeaderSize(keyspanColumnCount, keyspanHeaderSize)

	// Columns with userKeyCount elements.

	// Write the user keys.
	hOff := blockHeaderSize(keyspanColBoundaryUserKeys, keyspanHeaderSize)
	binary.LittleEndian.PutUint32(w.buf[1+hOff:], pageOffset)
	pageOffset = w.boundaryUserKeys.Finish(0, w.boundaryUserKeys.rows, pageOffset, w.buf)
	w.buf[hOff] = byte(DataTypeBytes)

	// Write the boundary key indices.
	hOff = blockHeaderSize(keyspanColBoundaryKeyIndices, keyspanHeaderSize)
	binary.LittleEndian.PutUint32(w.buf[1+hOff:], pageOffset)
	pageOffset = w.boundaryKeyIndexes.Finish(0, w.boundaryUserKeys.rows, pageOffset, w.buf)
	w.buf[hOff] = byte(DataTypeUint32)

	// Columns with keyCount elements.

	// Write the trailers.
	hOff = blockHeaderSize(keyspanColTrailers, keyspanHeaderSize)
	binary.LittleEndian.PutUint32(w.buf[1+hOff:], pageOffset)
	pageOffset = w.trailers.Finish(0, w.keyCount, pageOffset, w.buf)
	w.buf[hOff] = byte(DataTypeUint64)

	// Write the suffixes.
	hOff = blockHeaderSize(keyspanColSuffixes, keyspanHeaderSize)
	binary.LittleEndian.PutUint32(w.buf[1+hOff:], pageOffset)
	pageOffset = w.suffixes.Finish(0, w.keyCount, pageOffset, w.buf)
	w.buf[hOff] = byte(DataTypeBytes)

	// Write the values.
	hOff = blockHeaderSize(keyspanColValues, keyspanHeaderSize)
	binary.LittleEndian.PutUint32(w.buf[1+hOff:], pageOffset)
	pageOffset = w.values.Finish(0, w.keyCount, pageOffset, w.buf)
	w.buf[hOff] = byte(DataTypeBytes)

	w.buf[pageOffset] = 0x00 // padding byte
	return w.buf
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
	boundaryKeyIndices UnsafeUint32s

	// keyspan.Key columns with blockReader.header.Rows elements.
	trailers UnsafeUint64s
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
		int(r.boundaryKeysCount), DataTypeUint32, DecodeUnsafeIntegerSlice[uint32])

	r.trailers = r.blockReader.Uint64s(keyspanColTrailers)
	r.suffixes = r.blockReader.RawBytes(keyspanColSuffixes)
	r.values = r.blockReader.RawBytes(keyspanColValues)
}

// DebugString prints a human-readable explanation of the keyspan block's binary
// representation.
func (r *KeyspanReader) DebugString() string {
	f := binfmt.New(r.blockReader.data).LineWidth(20)
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
	return f.String()
}

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
		case -1:
			j = h
		default:
			panic("unreachable")
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
	// When positioned, the current span's keys are the keys at indices
	// [startKeyIndex, endKeyIndex). When positioned, endKeyIndex >=
	// startKeyIndex.
	startKeyIndex uint32
	endKeyIndex   uint32
	keyBuf        [2]keyspan.Key
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
	i.startKeyIndex = math.MaxUint32
	i.endKeyIndex = math.MaxUint32
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
	// strictly greater than key. We need to back up one consider the span that
	// ends at the this boundary key.
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
	// If all boundries are less than [key], or only the last boundary is
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
	i.startBoundIndex = startBoundIndex
	if i.startBoundIndex >= int(i.r.boundaryKeysCount)-1 {
		i.startKeyIndex = i.r.blockReader.header.Rows
		i.endKeyIndex = i.r.blockReader.header.Rows
		return nil
	}
	if invariants.Enabled && i.startBoundIndex < 0 {
		panic(errors.AssertionFailedf("out of bounds: i.startBoundIndex=%d", i.startBoundIndex))
	}

	// i.startBoundIndex is the index of the first user key >= key. Find the
	// keys that exist between the user key at index i.startBoundIndex and the
	// user key at index i.startBoundIndex+1. There may be none if there are two
	// non-abutting spans that we seeked between.
	i.startKeyIndex = i.r.boundaryKeyIndices.At(i.startBoundIndex)
	i.endKeyIndex = i.startKeyIndex
	for i.startKeyIndex < i.r.blockReader.header.Rows {
		// The set of keyspan.Keys with the bounds between
		//   [i.r.userKeys.At(i.startBoundIndex), i.r.userKeys.At(i.startBoundIndex+1))
		// is the keys between the indices
		//   [i.r.startIndices.At(i.startBoundIndex), i.r.startIndices.At(i.startBoundIndex+1))
		//
		// If the bounds' associated start indices are equal, then there are no
		// Keys that exist between the two bounds, and we should progress to the
		// next set of bounds.
		i.endKeyIndex = i.r.boundaryKeyIndices.At(i.startBoundIndex + 1)
		if i.endKeyIndex > i.startKeyIndex {
			return i.materializeSpan()
		}
		i.startBoundIndex++
		i.startKeyIndex = i.endKeyIndex
		// Advance to considering the span starting at the previous end
		// bound.
	}
	i.startKeyIndex = i.r.blockReader.header.Rows
	i.endKeyIndex = i.r.blockReader.header.Rows
	return nil
}

// gatherKeysBackward returns the first non-empty Span in the backward direction,
// starting with the span formed by using the boundary key at index
// [startBoundIndex] as the span's start boundary.
func (i *KeyspanIter) gatherKeysBackward(startBoundIndex int) *keyspan.Span {
	i.startBoundIndex = startBoundIndex
	if i.startBoundIndex < 0 {
		i.startKeyIndex = 0
		i.endKeyIndex = 0
		return nil
	}
	if invariants.Enabled && i.startBoundIndex >= int(i.r.boundaryKeysCount)-1 {
		panic(errors.AssertionFailedf("out of bounds: i.startBoundIndex=%d, i.r.boundaryKeysCount=%d",
			i.startBoundIndex, i.r.boundaryKeysCount))
	}

	i.endKeyIndex = i.r.boundaryKeyIndices.At(i.startBoundIndex + 1)
	for i.endKeyIndex > 0 {
		// The set of keyspan.Keys with the bounds between
		//   [i.r.userKeys.At(i.startBoundIndex), i.r.userKeys.At(i.startBoundIndex+1))
		// is the keys between the indices
		//   [i.r.startIndices.At(i.startBoundIndex), i.r.startIndices.At(i.startBoundIndex+1))
		//
		// If the bounds' associated start indices are equal, then there are no
		// Keys that exist between the two bounds, and we should progress to the
		// next set of bounds.
		i.startKeyIndex = i.r.boundaryKeyIndices.At(i.startBoundIndex)
		if i.endKeyIndex > i.startKeyIndex {
			return i.materializeSpan()
		}
		i.startBoundIndex--
		i.endKeyIndex = i.startKeyIndex
		// Advance to considering the span starting at the previous end
		// bound.
	}
	i.startKeyIndex = 0
	i.endKeyIndex = 0
	return nil
}

// materializeSpan constructs the current span from i.startBoundIndex and
// i.{start,end}KeyIndex.
func (i *KeyspanIter) materializeSpan() *keyspan.Span {
	i.span = keyspan.Span{
		Start: i.r.boundaryKeys.At(i.startBoundIndex),
		End:   i.r.boundaryKeys.At(i.startBoundIndex + 1),
		Keys:  i.span.Keys[:0],
	}
	if cap(i.span.Keys) < int(i.endKeyIndex-i.startKeyIndex) {
		i.span.Keys = make([]keyspan.Key, 0, int(i.endKeyIndex-i.startKeyIndex))
	}
	for j := i.startKeyIndex; j < i.endKeyIndex; j++ {
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
