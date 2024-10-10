// Copyright 2024 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package colblk

import (
	"slices"

	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/pebble/internal/base"
	"github.com/cockroachdb/pebble/internal/binfmt"
	"github.com/cockroachdb/pebble/internal/invariants"
	"github.com/cockroachdb/pebble/sstable/block"
)

const indexBlockCustomHeaderSize = 0

// IndexBlockWriter writes columnar index blocks. The writer is used for both
// first-level and second-level index blocks. The index block schema consists of
// three primary columns:
//   - Separators: a user key that is ≥ the largest user key in the
//     corresponding entry, and ≤ the smallest user key in the next entry.
//     Note that this allows consecutive separators to be equal. This is
//     possible when snapshots required we preserve duplicate user keys at
//     different sequence numbers.
//   - Offsets: the offset of the end of the corresponding block.
//   - Lengths: the length in bytes of the corresponding block.
//   - Block properties: a slice encoding arbitrary user-defined block
//     properties.
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
func (w *IndexBlockWriter) Init() {
	w.separators.Init()
	w.offsets.Init()
	w.lengths.InitWithDefault()
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

// Rows returns the number of entries in the index block so far.
func (w *IndexBlockWriter) Rows() int {
	return w.rows
}

// AddBlockHandle adds a new separator and end offset of a data block to the
// index block.  Add returns the index of the row.
//
// AddBlockHandle should only be used for first-level index blocks.
func (w *IndexBlockWriter) AddBlockHandle(
	separator []byte, handle block.Handle, blockProperties []byte,
) int {
	idx := w.rows
	w.separators.Put(separator)
	w.offsets.Set(w.rows, handle.Offset)
	w.lengths.Set(w.rows, handle.Length)
	w.blockProperties.Put(blockProperties)
	w.rows++
	return idx
}

// UnsafeSeparator returns the separator of the i'th entry.
func (w *IndexBlockWriter) UnsafeSeparator(i int) []byte {
	return w.separators.UnsafeGet(i)
}

// Size returns the size of the pending index block.
func (w *IndexBlockWriter) Size() int {
	return w.size(w.rows)
}

func (w *IndexBlockWriter) size(rows int) int {
	off := blockHeaderSize(indexBlockColumnCount, indexBlockCustomHeaderSize)
	off = w.separators.Size(rows, off)
	off = w.offsets.Size(rows, off)
	off = w.lengths.Size(rows, off)
	off = w.blockProperties.Size(rows, off)
	off++
	return int(off)
}

// Finish serializes the pending index block, including the first [rows] rows.
// The value of [rows] must be Rows() or Rows()-1.
func (w *IndexBlockWriter) Finish(rows int) []byte {
	if invariants.Enabled && rows != w.rows && rows != w.rows-1 {
		panic(errors.AssertionFailedf("index block has %d rows; asked to finish %d", w.rows, rows))
	}

	w.enc.init(w.size(rows), Header{
		Version: Version1,
		Columns: indexBlockColumnCount,
		Rows:    uint32(rows),
	}, indexBlockCustomHeaderSize)
	w.enc.encode(rows, &w.separators)
	w.enc.encode(rows, &w.offsets)
	w.enc.encode(rows, &w.lengths)
	w.enc.encode(rows, &w.blockProperties)
	return w.enc.finish()
}

// An IndexBlockDecoder reads columnar index blocks.
type IndexBlockDecoder struct {
	separators RawBytes
	offsets    UnsafeUints
	lengths    UnsafeUints // only used for second-level index blocks
	blockProps RawBytes
	br         BlockDecoder
}

// Init initializes the index block decoder with the given serialized index
// block.
func (r *IndexBlockDecoder) Init(data []byte) {
	r.br.Init(data, indexBlockCustomHeaderSize)
	r.separators = r.br.RawBytes(indexBlockColumnSeparator)
	r.offsets = r.br.Uints(indexBlockColumnOffsets)
	r.lengths = r.br.Uints(indexBlockColumnLengths)
	r.blockProps = r.br.RawBytes(indexBlockColumnBlockProperties)
}

// DebugString prints a human-readable explanation of the keyspan block's binary
// representation.
func (r *IndexBlockDecoder) DebugString() string {
	f := binfmt.New(r.br.data).LineWidth(20)
	r.Describe(f)
	return f.String()
}

// Describe describes the binary format of the index block, assuming f.Offset()
// is positioned at the beginning of the same index block described by r.
func (r *IndexBlockDecoder) Describe(f *binfmt.Formatter) {
	// Set the relative offset. When loaded into memory, the beginning of blocks
	// are aligned. Padding that ensures alignment is done relative to the
	// current offset. Setting the relative offset ensures that if we're
	// describing this block within a larger structure (eg, f.Offset()>0), we
	// compute padding appropriately assuming the current byte f.Offset() is
	// aligned.
	f.SetAnchorOffset()

	f.CommentLine("index block header")
	r.br.headerToBinFormatter(f)
	for i := 0; i < indexBlockColumnCount; i++ {
		r.br.columnToBinFormatter(f, i, int(r.br.header.Rows))
	}
	f.HexBytesln(1, "block padding byte")
}

// IndexIter is an iterator over the block entries in an index block.
type IndexIter struct {
	compare base.Compare
	split   base.Split
	d       *IndexBlockDecoder
	n       int
	row     int

	syntheticPrefix block.SyntheticPrefix
	syntheticSuffix block.SyntheticSuffix
	noTransforms    bool

	h            block.BufferHandle
	allocDecoder IndexBlockDecoder
	keyBuf       []byte
}

// Assert that IndexIter satisfies the block.IndexBlockIterator interface.
var _ block.IndexBlockIterator = (*IndexIter)(nil)

// InitWithDecoder initializes an index iterator from the provided reader.
func (i *IndexIter) InitWithDecoder(
	compare base.Compare, split base.Split, d *IndexBlockDecoder, transforms block.IterTransforms,
) {
	*i = IndexIter{
		compare:         compare,
		split:           split,
		d:               d,
		n:               int(d.br.header.Rows),
		h:               i.h,
		allocDecoder:    i.allocDecoder,
		keyBuf:          i.keyBuf,
		syntheticPrefix: transforms.SyntheticPrefix,
		syntheticSuffix: transforms.SyntheticSuffix,
		noTransforms:    !transforms.SyntheticPrefix.IsSet() && !transforms.SyntheticSuffix.IsSet(),
	}
}

// Init initializes an iterator from the provided block data slice.
func (i *IndexIter) Init(
	cmp base.Compare, split base.Split, blk []byte, transforms block.IterTransforms,
) error {
	i.h.Release()
	i.h = block.BufferHandle{}
	i.allocDecoder.Init(blk)
	i.InitWithDecoder(cmp, split, &i.allocDecoder, transforms)
	return nil
}

// InitHandle initializes an iterator from the provided block handle.
func (i *IndexIter) InitHandle(
	cmp base.Compare, split base.Split, blk block.BufferHandle, transforms block.IterTransforms,
) error {
	// TODO(jackson): If block.h != nil, use a *IndexBlockDecoder that's allocated
	// when the block is loaded into the block cache. On cache hits, this will
	// reduce the amount of setup necessary to use an iterator. (It's relatively
	// common to open an iterator and perform just a few seeks, so avoiding the
	// overhead can be material.)
	i.h.Release()
	i.h = blk
	i.allocDecoder.Init(i.h.Get())
	i.InitWithDecoder(cmp, split, &i.allocDecoder, transforms)
	return nil
}

// RowIndex returns the index of the block entry at the iterator's current
// position.
func (i *IndexIter) RowIndex() int {
	return i.row
}

// ResetForReuse resets the IndexIter for reuse, retaining buffers to avoid
// future allocations.
func (i *IndexIter) ResetForReuse() IndexIter {
	if invariants.Enabled && i.h != (block.BufferHandle{}) {
		panic(errors.AssertionFailedf("IndexIter reset for reuse with non-empty handle"))
	}
	return IndexIter{ /* nothing to retain */ }
}

// Valid returns true if the iterator is currently positioned at a valid block
// handle.
func (i *IndexIter) Valid() bool {
	return 0 <= i.row && i.row < i.n
}

// Invalidate invalidates the block iterator, removing references to the block
// it was initialized with.
func (i *IndexIter) Invalidate() {
	i.d = nil
	i.n = 0
}

// IsDataInvalidated returns true when the iterator has been invalidated
// using an Invalidate call. NB: this is different from Valid.
func (i *IndexIter) IsDataInvalidated() bool {
	return i.d == nil
}

// Handle returns the underlying block buffer handle, if the iterator was
// initialized with one.
func (i *IndexIter) Handle() block.BufferHandle {
	return i.h
}

// Separator returns the separator at the iterator's current position. The
// iterator must be positioned at a valid row.
func (i *IndexIter) Separator() []byte {
	key := i.d.separators.At(i.row)
	if i.noTransforms {
		return key
	}
	return i.applyTransforms(key)
}

func (i *IndexIter) applyTransforms(key []byte) []byte {
	if i.syntheticSuffix.IsSet() {
		key = key[:i.split(key)]
	}
	i.keyBuf = slices.Grow(i.keyBuf[:0], len(i.syntheticPrefix)+len(key)+len(i.syntheticSuffix))
	i.keyBuf = append(i.keyBuf, i.syntheticPrefix...)
	i.keyBuf = append(i.keyBuf, key...)
	i.keyBuf = append(i.keyBuf, i.syntheticSuffix...)
	return i.keyBuf
}

// BlockHandleWithProperties decodes the block handle with any encoded
// properties at the iterator's current position.
func (i *IndexIter) BlockHandleWithProperties() (block.HandleWithProperties, error) {
	return block.HandleWithProperties{
		Handle: block.Handle{
			Offset: i.d.offsets.At(i.row),
			Length: i.d.lengths.At(i.row),
		},
		Props: i.d.blockProps.At(i.row),
	}, nil
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
		separator := i.d.separators.At(h)
		if !i.noTransforms {
			// TODO(radu): compare without materializing the transformed key.
			separator = i.applyTransforms(separator)
		}
		// TODO(radu): experiment with splitting the separator prefix and suffix in
		// separate columns and using bytes.Compare() on the prefix in the hot path.
		c := i.compare(key, separator)
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
// the index block is exhausted in the forward direction. A call to Next while
// already exhausted in the forward direction is a no-op.
func (i *IndexIter) Next() bool {
	i.row = min(i.n, i.row+1)
	return i.row < i.n
}

// Prev steps the index iterator to the previous block entry. It returns false
// if the index block is exhausted in the reverse direction. A call to Prev
// while already exhausted in the reverse direction is a no-op.
func (i *IndexIter) Prev() bool {
	i.row = max(-1, i.row-1)
	return i.row >= 0 && i.row < i.n
}

// Close closes the iterator, releasing any resources it holds.
func (i *IndexIter) Close() error {
	i.h.Release()
	*i = IndexIter{}
	return nil
}
