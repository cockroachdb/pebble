// Copyright 2024 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package colblk

import (
	"bytes"
	"encoding/binary"
	"unsafe"

	"github.com/cockroachdb/crlib/crbytes"
	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/pebble/internal/base"
	"github.com/cockroachdb/pebble/internal/binfmt"
	"github.com/cockroachdb/pebble/internal/invariants"
	"github.com/cockroachdb/pebble/internal/treeprinter"
	"github.com/cockroachdb/pebble/sstable/block"
)

const indexBlockCustomHeaderSize = 4

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
	separatorPrefixes PrefixBytesBuilder
	separatorSuffixes RawBytesBuilder
	offsets           UintBuilder
	lengths           UintBuilder
	blockProperties   RawBytesBuilder
	rows              int
	enc               blockEncoder
	separatorBuf      []byte
	split             base.Split
	maxSeparatorLen   int
}

const (
	indexBlockColumnSeparatorPrefix = iota
	indexBlockColumnSeparatorSuffix
	indexBlockColumnOffsets
	indexBlockColumnLengths
	indexBlockColumnBlockProperties
	indexBlockColumnCount
)

// Init initializes the index block writer.
func (w *IndexBlockWriter) Init(split base.Split, bundleSize int) {
	w.separatorPrefixes.Init(bundleSize)
	w.separatorSuffixes.Init()
	w.offsets.Init()
	w.lengths.Init()
	w.blockProperties.Init()
	w.rows = 0
	w.split = split
	w.maxSeparatorLen = 0
}

// Reset resets the index block writer to its initial state, retaining buffers.
func (w *IndexBlockWriter) Reset() {
	w.separatorPrefixes.Reset()
	w.separatorSuffixes.Reset()
	w.offsets.Reset()
	w.lengths.Reset()
	w.blockProperties.Reset()
	w.rows = 0
	w.enc.reset()
	w.separatorBuf = w.separatorBuf[:0]
	w.maxSeparatorLen = 0
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

	// Decompose the separator into prefix and suffix.
	s := w.split(separator)
	commonPrefixLen := s
	if idx > 0 {
		commonPrefixLen = crbytes.CommonPrefix(w.separatorPrefixes.UnsafeGet(idx-1), separator[:s])
	}
	w.maxSeparatorLen = max(w.maxSeparatorLen, len(separator))
	w.separatorPrefixes.Put(separator[:s], commonPrefixLen)
	w.separatorSuffixes.Put(separator[s:])
	w.offsets.Set(w.rows, handle.Offset)
	w.lengths.Set(w.rows, handle.Length)
	w.blockProperties.Put(blockProperties)
	w.rows++
	return idx
}

// UnsafeSeparator returns the separator of the i'th entry. If r is the number
// of rows, it is required that r-2 <= i < r.
func (w *IndexBlockWriter) UnsafeSeparator(i int) []byte {
	if i < w.rows-2 || i >= w.rows {
		panic(errors.AssertionFailedf("UnsafeSeparator(%d); writer has %d rows", i, w.rows))
	}
	w.separatorBuf = append(w.separatorBuf[:0], w.separatorPrefixes.UnsafeGet(i)...)
	w.separatorBuf = append(w.separatorBuf, w.separatorSuffixes.UnsafeGet(i)...)
	return w.separatorBuf
}

// Size returns the size of the pending index block.
func (w *IndexBlockWriter) Size() int {
	return w.size(w.rows)
}

func (w *IndexBlockWriter) size(rows int) int {
	off := blockHeaderSize(indexBlockColumnCount, indexBlockCustomHeaderSize)
	off = w.separatorPrefixes.Size(rows, off)
	off = w.separatorSuffixes.Size(rows, off)
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

	// Write the max key length in the custom header.
	binary.LittleEndian.PutUint32(w.enc.data()[:indexBlockCustomHeaderSize], uint32(w.maxSeparatorLen))
	if w.rows == 0 {
		return w.enc.finish()
	}
	w.enc.encode(rows, &w.separatorPrefixes)
	w.enc.encode(rows, &w.separatorSuffixes)
	w.enc.encode(rows, &w.offsets)
	w.enc.encode(rows, &w.lengths)
	w.enc.encode(rows, &w.blockProperties)
	return w.enc.finish()
}

// An IndexBlockDecoder reads columnar index blocks.
type IndexBlockDecoder struct {
	separatorPrefixes PrefixBytes
	separatorSuffixes RawBytes
	offsets           UnsafeUints
	lengths           UnsafeUints // only used for second-level index blocks
	blockProps        RawBytes
	bd                BlockDecoder
	maxSeparatorLen   uint32
}

// Init initializes the index block decoder with the given serialized index
// block.
func (r *IndexBlockDecoder) Init(data []byte) {
	r.bd.Init(data, indexBlockCustomHeaderSize)
	r.maxSeparatorLen = binary.LittleEndian.Uint32(data[:indexBlockCustomHeaderSize])
	if r.bd.header.Rows > 0 {
		r.separatorPrefixes = r.bd.PrefixBytes(indexBlockColumnSeparatorPrefix)
		r.separatorSuffixes = r.bd.RawBytes(indexBlockColumnSeparatorSuffix)
		r.offsets = r.bd.Uints(indexBlockColumnOffsets)
		r.lengths = r.bd.Uints(indexBlockColumnLengths)
		r.blockProps = r.bd.RawBytes(indexBlockColumnBlockProperties)
	}
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
	f.HexBytesln(4, "maximum sep length: %d", r.maxSeparatorLen)
	r.bd.headerToBinFormatter(f, n)
	for i := 0; i < indexBlockColumnCount; i++ {
		r.bd.columnToBinFormatter(f, n, i, int(r.bd.header.Rows))
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

	syntheticPrefix block.SyntheticPrefix
	syntheticSuffix block.SyntheticSuffix
	noTransforms    bool

	h block.BufferHandle
	// TODO(radu): remove allocDecoder and require any Init callers to provide the
	// decoder.
	allocDecoder IndexBlockDecoder
	sepBuf       PrefixBytesIter
}

// Assert that IndexIter satisfies the block.IndexBlockIterator interface.
var _ block.IndexBlockIterator = (*IndexIter)(nil)

// InitWithDecoder initializes an index iterator from the provided decoder.
func (i *IndexIter) InitWithDecoder(
	compare base.Compare, split base.Split, d *IndexBlockDecoder, transforms block.IterTransforms,
) {
	i.compare = compare
	i.split = split
	i.d = d
	i.n = int(d.bd.header.Rows)
	i.row = -1
	i.syntheticPrefix = transforms.SyntheticPrefix
	i.syntheticSuffix = transforms.SyntheticSuffix
	i.noTransforms = !transforms.SyntheticPrefix.IsSet() && !transforms.SyntheticSuffix.IsSet()
	sepBufBuf := i.sepBuf.Buf
	i.sepBuf = PrefixBytesIter{Buf: sepBufBuf}
	i.sepBuf.Init(int(i.d.maxSeparatorLen)+len(transforms.SyntheticPrefix)+len(transforms.SyntheticSuffix), transforms.SyntheticPrefix)
	// Leave h, allocDecoder unchanged.
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
	i.h.Release()
	i.h = blk
	d := (*IndexBlockDecoder)(unsafe.Pointer(blk.BlockMetadata()))
	i.InitWithDecoder(cmp, split, d, transforms)
	return nil
}

// RowIndex returns the index of the block entry at the iterator's current
// position.
func (i *IndexIter) RowIndex() int {
	return i.row
}

// ResetForReuse resets the IndexIter for reuse, retaining buffers to avoid
// future allocations.
func (i *IndexIter) ResetForReuse() {
	if invariants.Enabled && i.h != (block.BufferHandle{}) {
		panic(errors.AssertionFailedf("IndexIter reset for reuse with non-empty handle"))
	}
	i.d = nil
	i.syntheticPrefix = nil
	i.syntheticSuffix = nil
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
	i.d.separatorPrefixes.SetAt(&i.sepBuf, i.row)
	if i.syntheticSuffix.IsSet() {
		return append(i.sepBuf.Buf, i.syntheticSuffix...)
	}
	return append(i.sepBuf.Buf, i.d.separatorSuffixes.At(i.row)...)
}

// SeparatorLT returns true if the separator at the iterator's current
// position is strictly less than the provided key.
func (i *IndexIter) SeparatorLT(key []byte) bool {
	return i.compare(i.Separator(), key) < 0
}

// SeparatorGT returns true if the separator at the iterator's current position
// is strictly greater than (or equal, if orEqual=true) the provided key.
func (i *IndexIter) SeparatorGT(key []byte, orEqual bool) bool {
	cmp := i.compare(i.Separator(), key)
	return cmp > 0 || (cmp == 0 && orEqual)
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
	if i.n == 0 {
		i.row = 0
		return i.row < i.n
	}
	if i.syntheticPrefix.IsSet() {
		var keyPrefix []byte
		keyPrefix, key = splitKey(key, len(i.syntheticPrefix))
		if cmp := bytes.Compare(keyPrefix, i.syntheticPrefix); cmp != 0 {
			if cmp < 0 {
				i.row = 0
				return i.row < i.n
			}
			i.row = i.n
			return false
		}
	}
	s := i.split(key)
	// Search for the separator prefix.
	var eq bool
	i.row, eq = i.d.separatorPrefixes.Search(key[:s])
	if !eq {
		return i.row < i.n
	}

	// The separator prefix is equal, so we need to compare against the suffix
	// as well.
	keySuffix := key[s:]
	if i.syntheticSuffix.IsSet() {
		// NB: With synthetic suffix, it's required that each key within the
		// sstable have a unique prefix. This ensures that there is at most 1
		// key with the prefix key[:s], and we know that the separator at
		// i.row+1 has a new prefix.
		if i.compare(i.syntheticSuffix, keySuffix) < 0 {
			i.row++
		}
		return i.row < i.n
	}
	if i.compare(i.d.separatorSuffixes.At(i.row), keySuffix) >= 0 {
		// Fast path; we landed on a separator that is greater than or equal to
		// the seek key.
		return i.row < i.n
	}

	// Fall back to a slow scan forward.
	//
	// TODO(jackson): This can be improved by adding a PrefixBytes.NextUniqueKey
	// method and then binary searching among the suffixes. The logic for
	// implementing NextUniqueKey is finicky requiring lots of delicate fiddling
	// with offset indexes, so it's deferred for now.
	for i.row++; i.row < i.d.bd.Rows(); i.row++ {
		if i.SeparatorGT(key, true /* orEqual */) {
			return i.row < i.n
		}
	}
	return false
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
	return nil
}
