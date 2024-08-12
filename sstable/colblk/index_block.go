// Copyright 2024 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package colblk

import (
	"bytes"

	"github.com/cockroachdb/pebble/internal/binfmt"
	"github.com/cockroachdb/pebble/sstable/block"
)

const indexBlockCustomHeaderSize = 0

// IndexBlockWriter writes columnar index blocks. The writer is used for both
// first-level and second-level index blocks. The index block schema consists of
// three primary columns:
//   - Separators: a user key that is ≥ the largest user key in the corresponding
//     entry.
//   - Offsets: the offset of the end of the corresponding block.
//   - Lengths: the length in bytes of the corresponding block.
//
// TODO(jackson): Consider splitting separators into prefixes and suffixes (even
// without user-defined columns). This would allow us to use prefix compression
// for the prefix. Separators should typically be suffixless unless two KVs with
// the same prefix straddle a block boundary. We would need to use a buffer to
// materialize the separator key when we need to use it outside the context of
// seeking within the block.
type IndexBlockWriter struct {
	separators      RawBytesBuilder
	offsets         UintBuilder
	lengths         UintBuilder
	blockProperties RawBytesBuilder
	numColumns      int
	rows            int
	enc             blockEncoder
}

const (
	indexBlockColumnSeparator = iota
	indexBlockColumnOffsets
	indexBlockColumnLengths
	indexBlockColumnBlockProperties
	indexBlockColumnCount
)

// Init initializes the index block writer.
func (w *IndexBlockWriter) Init(blockProperties ...ColumnWriter) {
	w.separators.Init()
	w.offsets.Init()
	w.lengths.InitWithDefault()
	w.numColumns = indexBlockColumnCount
	w.blockProperties.Init()
	w.rows = 0
}

// Reset resets the index block writer to its initial state, retaining buffers.
func (w *IndexBlockWriter) Reset() {
	w.separators.Reset()
	w.offsets.Reset()
	w.lengths.Reset()
	w.blockProperties.Reset()
	w.rows = 0
	w.enc.reset()
}

// AddDataBlockHandle adds a new separator and end offset of a data block to the
// index block.  Add returns the index of the row. The caller should add rows to
// the block properties columns too if necessary.
//
// AddDataBlockHandle should only be used for first-level index blocks.
func (w *IndexBlockWriter) AddDataBlockHandle(separator []byte, offset, length uint64, blockProperties []byte) int {
	idx := w.rows
	w.separators.Put(separator)
	w.offsets.Set(w.rows, offset)
	w.lengths.Set(w.rows, length)
	w.blockProperties.Put(blockProperties)
	w.rows++
	return idx
}

// AddIndexBlockHandle adds a new separator and end offset of an index block to
// the second-level index block.  Add returns the index of the row. The caller
// should add rows to the block properties columns too if necessary.
//
// AddIndexBlockHandle should only be used for second-level index blocks.
func (w *IndexBlockWriter) AddIndexBlockHandle(separator []byte, offset, length uint64, blockProperties []byte) int {
	idx := w.rows
	w.separators.Put(separator)
	w.offsets.Set(idx, offset)
	w.lengths.Set(idx, length)
	w.blockProperties.Put(blockProperties)
	w.rows++
	return idx
}

// Size returns the size of the pending index block.
func (w *IndexBlockWriter) Size() int {
	off := blockHeaderSize(w.numColumns, indexBlockCustomHeaderSize)
	off = w.separators.Size(w.rows, off)
	off = w.offsets.Size(w.rows, off)
	off = w.lengths.Size(w.rows, off)
	off = w.blockProperties.Size(w.rows, off)
	off++
	return int(off)
}

// Finish serializes the pending index block.
func (w *IndexBlockWriter) Finish() []byte {
	w.enc.init(w.Size(), Header{
		Version: Version1,
		Columns: uint16(w.numColumns),
		Rows:    uint32(w.rows),
	}, indexBlockCustomHeaderSize)
	w.enc.encode(w.rows, &w.separators)
	w.enc.encode(w.rows, &w.offsets)
	w.enc.encode(w.rows, &w.lengths)
	w.enc.encode(w.rows, &w.blockProperties)
	return w.enc.finish()
}

// An IndexReader reads columnar index blocks.
type IndexReader struct {
	separators RawBytes
	offsets    UnsafeUints
	lengths    UnsafeUints // only used for second-level index blocks
	br         BlockReader
}

// Init initializes the index reader with the given serialized index block.
func (r *IndexReader) Init(data []byte) {
	r.br.Init(data, indexBlockCustomHeaderSize)
	r.separators = r.br.RawBytes(indexBlockColumnSeparator)
	r.offsets = r.br.Uints(indexBlockColumnOffsets)
	r.lengths = r.br.Uints(indexBlockColumnLengths)
}

// DebugString prints a human-readable explanation of the keyspan block's binary
// representation.
func (r *IndexReader) DebugString() string {
	f := binfmt.New(r.br.data).LineWidth(20)
	f.CommentLine("index block header")
	r.br.headerToBinFormatter(f)
	for i := 0; i < indexBlockColumnCount; i++ {
		r.br.columnToBinFormatter(f, i, int(r.br.header.Rows))
	}
	return f.String()
}

// IndexIter is an iterator over the block entries in an index block.
type IndexIter struct {
	r   *IndexReader
	n   int
	row int
}

// Init initializes an index iterator from the provided reader.
func (i *IndexIter) Init(r *IndexReader) {
	*i = IndexIter{r: r, n: int(r.br.header.Rows)}
}

// RowIndex returns the index of the block entry at the iterator's current
// position.
func (i *IndexIter) RowIndex() int {
	return i.row
}

// DataBlockHandle returns the data block handle at the iterator's current
// position.
func (i *IndexIter) DataBlockHandle() block.Handle {
	return block.Handle{
		Offset: i.r.offsets.At(i.row),
		Length: i.r.lengths.At(i.row),
	}
}

// IndexBlockHandle returns the index block handle at the iterator's current
// position.
func (i *IndexIter) IndexBlockHandle() block.Handle {
	return block.Handle{
		// The end offset of the previous block points to its trailer. We need
		// to add the trailer length to get the start offset of the current
		// block.
		Offset: i.r.offsets.At(i.row),
		Length: i.r.lengths.At(i.row),
	}
}

// Separator returns the separator at the iterator's current position. The
// iterator must be positioned at a valid row.
func (i *IndexIter) Separator() []byte {
	return i.r.separators.At(i.row)
}

// BlockProperties returns the encoded block properties at the iterator's
// current position.
func (i *IndexIter) BlockProperties() []byte {
	return i.r.br.RawBytes(indexBlockColumnBlockProperties).At(i.row)
}

// SeekGE seeks the index iterator to the first block entry with a separator key
// greater or equal to the given key. It returns false if the seek key is
// greater than all index block separators.
func (i *IndexIter) SeekGE(key []byte) bool {
	// Define f(-1) == false and f(upper) == true.
	// Invariant: f(index-1) == false, f(upper) == true.
	index, upper := 0, i.n
	for index < upper {
		h := int(uint(index+upper) >> 1) // avoid overflow when computing h
		// index ≤ h < upper

		// TODO(jackson): Is Bytes.At or Bytes.Slice(Bytes.Offset(h),
		// Bytes.Offset(h+1)) faster in this code?
		c := bytes.Compare(key, i.r.separators.At(h))
		if c > 0 {
			index = h + 1 // preserves f(index-1) == false
		} else {
			upper = h // preserves f(upper) == true
		}
	}
	// index == upper, f(index-1) == false, and f(upper) (= f(index)) == true  =>  answer is index.
	i.row = index
	return index < i.n
}

// First seeks index iterator to the first block entry. It returns false if the
// index block is empty.
func (i *IndexIter) First() bool {
	i.row = 0
	return i.n > 0
}

// Last seeks index iterator to the last block entry. It returns false if the
// index block is empty.
func (i *IndexIter) Last() bool {
	i.row = i.n - 1
	return i.n > 0
}

// Next steps the index iterator to the next block entry. It returns false if
// the index block is exhausted.
func (i *IndexIter) Next() bool {
	i.row++
	return i.row < i.n
}

// Prev steps the index iterator to the previous block entry. It returns false
// if the index block is exhausted.
func (i *IndexIter) Prev() bool {
	i.row--
	return i.row >= 0
}
