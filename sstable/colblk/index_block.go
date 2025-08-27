// Copyright 2024 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package colblk

import (
	"slices"
	"unsafe"

	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/pebble/v2/internal/base"
	"github.com/cockroachdb/pebble/v2/internal/binfmt"
	"github.com/cockroachdb/pebble/v2/internal/invariants"
	"github.com/cockroachdb/pebble/v2/internal/treeprinter"
	"github.com/cockroachdb/pebble/v2/sstable/block"
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
	enc             BlockEncoder
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
	w.lengths.Init()
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
	w.enc.Reset()
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
	off := HeaderSize(indexBlockColumnCount, indexBlockCustomHeaderSize)
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

	w.enc.Init(w.size(rows), Header{
		Version: Version1,
		Columns: indexBlockColumnCount,
		Rows:    uint32(rows),
	}, indexBlockCustomHeaderSize)
	w.enc.Encode(rows, &w.separators)
	w.enc.Encode(rows, &w.offsets)
	w.enc.Encode(rows, &w.lengths)
	w.enc.Encode(rows, &w.blockProperties)
	return w.enc.Finish()
}

// An IndexBlockDecoder reads columnar index blocks.
type IndexBlockDecoder struct {
	separators RawBytes
	offsets    UnsafeUints
	lengths    UnsafeUints // only used for second-level index blocks
	blockProps RawBytes
	bd         BlockDecoder
}

// Init initializes the index block decoder with the given serialized index
// block.
func (r *IndexBlockDecoder) Init(data []byte) {
	r.bd.Init(data, indexBlockCustomHeaderSize)
	r.separators = r.bd.RawBytes(indexBlockColumnSeparator)
	r.offsets = r.bd.Uints(indexBlockColumnOffsets)
	r.lengths = r.bd.Uints(indexBlockColumnLengths)
	r.blockProps = r.bd.RawBytes(indexBlockColumnBlockProperties)
}

// DebugString prints a human-readable explanation of the keyspan block's binary
// representation.
func (r *IndexBlockDecoder) DebugString() string {
	f := binfmt.New(r.bd.data).LineWidth(20)
	tp := treeprinter.New()
	r.Describe(f, tp.Child("index-block-decoder"))
	return tp.String()
}

// Describe describes the binary format of the index block, assuming f.Offset()
// is positioned at the beginning of the same index block described by r.
func (r *IndexBlockDecoder) Describe(f *binfmt.Formatter, tp treeprinter.Node) {
	// Set the relative offset. When loaded into memory, the beginning of blocks
	// are aligned. Padding that ensures alignment is done relative to the
	// current offset. Setting the relative offset ensures that if we're
	// describing this block within a larger structure (eg, f.Offset()>0), we
	// compute padding appropriately assuming the current byte f.Offset() is
	// aligned.
	f.SetAnchorOffset()

	n := tp.Child("index block header")
	r.bd.HeaderToBinFormatter(f, n)
	for i := 0; i < indexBlockColumnCount; i++ {
		r.bd.ColumnToBinFormatter(f, n, i, int(r.bd.header.Rows))
	}
	f.HexBytesln(1, "block padding byte")
	f.ToTreePrinter(n)
}

// IndexIter is an iterator over the block entries in an index block.
type IndexIter struct {
	compare base.Compare
	split   base.Split
	d       *IndexBlockDecoder
	n       int
	row     int

	syntheticPrefixAndSuffix block.SyntheticPrefixAndSuffix

	h block.BufferHandle
	// TODO(radu): remove allocDecoder and require any Init callers to provide the
	// decoder.
	allocDecoder IndexBlockDecoder
	keyBuf       []byte
}

// Assert that IndexIter satisfies the block.IndexBlockIterator interface.
var _ block.IndexBlockIterator = (*IndexIter)(nil)

// InitWithDecoder initializes an index iterator from the provided decoder.
func (i *IndexIter) InitWithDecoder(
	comparer *base.Comparer, d *IndexBlockDecoder, transforms block.IterTransforms,
) {
	i.compare = comparer.Compare
	i.split = comparer.Split
	i.d = d
	i.n = int(d.bd.header.Rows)
	i.row = -1
	i.syntheticPrefixAndSuffix = transforms.SyntheticPrefixAndSuffix
	// Leave h, allocDecoder, keyBuf unchanged.
}

// Init initializes an iterator from the provided block data slice.
func (i *IndexIter) Init(
	comparer *base.Comparer, blk []byte, transforms block.IterTransforms,
) error {
	i.h.Release()
	i.h = block.BufferHandle{}
	i.allocDecoder.Init(blk)
	i.InitWithDecoder(comparer, &i.allocDecoder, transforms)
	return nil
}

// InitHandle initializes an iterator from the provided block handle.
func (i *IndexIter) InitHandle(
	comparer *base.Comparer, blk block.BufferHandle, transforms block.IterTransforms,
) error {
	i.h.Release()
	i.h = blk
	d := (*IndexBlockDecoder)(unsafe.Pointer(blk.BlockMetadata()))
	i.InitWithDecoder(comparer, d, transforms)
	return nil
}

// RowIndex returns the index of the block entry at the iterator's current
// position.
func (i *IndexIter) RowIndex() int {
	return i.row
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
	if i.syntheticPrefixAndSuffix.IsUnset() {
		return key
	}
	return i.applyTransforms(key)
}

// SeparatorLT returns true if the separator at the iterator's current
// position is strictly less than the provided key.
func (i *IndexIter) SeparatorLT(key []byte) bool {
	return i.compare(i.Separator(), key) < 0
}

// SeparatorGT returns true if the separator at the iterator's current position
// is strictly greater than (or equal, if orEqual=true) the provided key.
func (i *IndexIter) SeparatorGT(key []byte, inclusively bool) bool {
	cmp := i.compare(i.Separator(), key)
	return cmp > 0 || (cmp == 0 && inclusively)
}

func (i *IndexIter) applyTransforms(key []byte) []byte {
	syntheticPrefix := i.syntheticPrefixAndSuffix.Prefix()
	syntheticSuffix := i.syntheticPrefixAndSuffix.Suffix()
	if syntheticSuffix.IsSet() {
		key = key[:i.split(key)]
	}
	i.keyBuf = slices.Grow(i.keyBuf[:0], len(syntheticPrefix)+len(key)+len(syntheticSuffix))
	i.keyBuf = append(i.keyBuf, syntheticPrefix...)
	i.keyBuf = append(i.keyBuf, key...)
	i.keyBuf = append(i.keyBuf, syntheticSuffix...)
	return i.keyBuf
}

// BlockHandleWithProperties decodes the block handle with any encoded
// properties at the iterator's current position.
func (i *IndexIter) BlockHandleWithProperties() (block.HandleWithProperties, error) {
	if invariants.Enabled && !i.Valid() {
		panic(errors.AssertionFailedf("invalid row %d (n=%d)", i.row, i.n))
	}
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
		if !i.syntheticPrefixAndSuffix.IsUnset() {
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
	i.h = block.BufferHandle{}
	i.d = nil
	i.n = 0
	i.syntheticPrefixAndSuffix = block.SyntheticPrefixAndSuffix{}
	return nil
}
