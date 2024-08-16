// Copyright 2024 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package rowblk

import (
	"github.com/cockroachdb/pebble/internal/base"
	"github.com/cockroachdb/pebble/sstable/block"
)

// IndexIter is a lightweight adapter that implements block.IndexIterator for a
// row-based index block.
type IndexIter struct {
	iter Iter
}

// InitHandle initializes an iterator from the provided block handle.
func (i *IndexIter) InitHandle(
	cmp base.Compare, split base.Split, block block.BufferHandle, transforms block.IterTransforms,
) error {
	return i.iter.InitHandle(cmp, split, block, transforms)
}

// ResetForReuse resets the index iterator for reuse, retaining buffers to avoid
// future allocations.
func (i *IndexIter) ResetForReuse() IndexIter {
	return IndexIter{iter: i.iter.ResetForReuse()}
}

// Valid returns true if the iterator is currently positioned at a valid block
// handle.
func (i *IndexIter) Valid() bool {
	return i.iter.offset >= 0 && i.iter.offset < i.iter.restarts
}

// IsDataInvalidated returns true when the blockIter has been invalidated
// using an invalidate call. NB: this is different from blockIter.Valid
// which is part of the InternalIterator implementation.
func (i *IndexIter) IsDataInvalidated() bool {
	return i.iter.IsDataInvalidated()
}

// Invalidate invalidates the block iterator, removing references to the block
// it was initialized with.
func (i *IndexIter) Invalidate() {
	i.iter.Invalidate()
}

// Handle returns the underlying block buffer handle, if the iterator was
// initialized with one.
func (i *IndexIter) Handle() block.BufferHandle {
	return i.iter.handle
}

// Separator returns the separator at the iterator's current position. The
// iterator must be positioned at a valid row.
func (i *IndexIter) Separator() []byte {
	return i.iter.ikv.K.UserKey
}

// BlockHandleWithProperties decodes the block handle with any encoded
// properties at the iterator's current position.
func (i *IndexIter) BlockHandleWithProperties() (block.HandleWithProperties, error) {
	return block.DecodeHandleWithProperties(i.iter.ikv.V.ValueOrHandle)
}

// SeekGE seeks the index iterator to the first block entry with a separator
// key greater or equal to the given key. It returns false if the seek key
// is greater than all index block separators.
func (i *IndexIter) SeekGE(key []byte) bool {
	return i.iter.SeekGE(key, base.SeekGEFlagsNone) != nil
}

// First seeks index iterator to the first block entry. It returns false if
// the index block is empty.
func (i *IndexIter) First() bool {
	return i.iter.First() != nil
}

// Last seeks index iterator to the last block entry. It returns false if
// the index block is empty.
func (i *IndexIter) Last() bool {
	return i.iter.Last() != nil
}

// Next steps the index iterator to the next block entry. It returns false
// if the index block is exhausted.
func (i *IndexIter) Next() bool {
	return i.iter.Next() != nil
}

// Prev steps the index iterator to the previous block entry. It returns
// false if the index block is exhausted.
func (i *IndexIter) Prev() bool {
	return i.iter.Prev() != nil
}

// Close closes the iterator, releasing any resources it holds.
func (i *IndexIter) Close() error {
	return i.iter.Close()
}
