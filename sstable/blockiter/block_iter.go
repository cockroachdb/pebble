// Copyright 2025 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package blockiter

import (
	"github.com/cockroachdb/pebble/internal/base"
	"github.com/cockroachdb/pebble/internal/treesteps"
	"github.com/cockroachdb/pebble/sstable/block"
)

// PrefixMatchResult is a tri-state indicating whether the current key has the
// same prefix as a reference key from the last positioning operation.
type PrefixMatchResult int8

const (
	// PrefixMatchUnknown indicates that the prefix match information is not
	// cheaply available (e.g. row format, transforms active, iterator invalid).
	PrefixMatchUnknown PrefixMatchResult = 0
	// PrefixMatchYes indicates that the current key has the same prefix as the
	// reference key.
	PrefixMatchYes PrefixMatchResult = 1
	// PrefixMatchNo indicates that the current key has a different prefix than
	// the reference key.
	PrefixMatchNo PrefixMatchResult = 2
)

// Data is a type constraint for implementations of block iterators over data
// blocks. It's implemented by *rowblk.Iter and *colblk.DataBlockIter.
//
// Unlike base.InternalIterator, a data block iterator does not support
// SeekPrefixGE, SetBounds, or SetContext — those are handled at a higher level
// by the sstable iterator.
type Data interface {
	// SeekGE moves the iterator to the first key/value pair whose key is greater
	// than or equal to the given key. Returns the key/value if the iterator is
	// pointing at a valid entry, and nil otherwise.
	SeekGE(key []byte, flags base.SeekGEFlags) *base.InternalKV
	// SeekLT moves the iterator to the last key/value pair whose key is less
	// than the given key. Returns the key/value if the iterator is pointing at a
	// valid entry, and nil otherwise.
	SeekLT(key []byte, flags base.SeekLTFlags) *base.InternalKV
	// First moves the iterator to the first key/value pair. Returns the
	// key/value if the iterator is pointing at a valid entry, and nil otherwise.
	First() *base.InternalKV
	// Last moves the iterator to the last key/value pair. Returns the key/value
	// if the iterator is pointing at a valid entry, and nil otherwise.
	Last() *base.InternalKV
	// Next moves the iterator to the next key/value pair. Returns the key/value
	// if the iterator is pointing at a valid entry, and nil otherwise.
	Next() *base.InternalKV
	// NextPrefix moves the iterator to the next key/value pair with a different
	// prefix than the key at the current iterator position. succKey is the
	// immediate successor to the current prefix key.
	NextPrefix(succKey []byte) *base.InternalKV
	// Prev moves the iterator to the previous key/value pair. Returns the
	// key/value if the iterator is pointing at a valid entry, and nil otherwise.
	Prev() *base.InternalKV
	// Error returns any accumulated error.
	Error() error
	// Close closes the iterator, releasing any resources it holds.
	Close() error

	// Handle returns the handle to the block.
	Handle() block.BufferHandle
	// InitHandle initializes the block from the provided buffer handle.
	//
	// The iterator takes ownership of the BufferHandle and releases it when it is
	// closed (or re-initialized with another handle). This happens even in error
	// cases.
	InitHandle(*base.Comparer, block.BufferHandle, Transforms) error
	// Valid returns true if the iterator is currently positioned at a valid KV.
	Valid() bool
	// KV returns the key-value pair at the current iterator position. The
	// iterator must be Valid().
	KV() *base.InternalKV
	// IsLowerBound returns true if all keys produced by this iterator are >= the
	// given key. The function is best effort; false negatives are allowed.
	//
	// If IsLowerBound is true then Compare(First().UserKey, k) >= 0.
	//
	// If the iterator produces no keys (i.e. First() is nil), IsLowerBound can
	// return true for any key.
	IsLowerBound(k []byte) bool
	// Invalidate invalidates the block iterator, removing references to the
	// block it was initialized with. The iterator may continue to be used after
	// a call to Invalidate, but all positioning methods should return nil.
	// Valid() must also return false.
	Invalidate()
	// IsDataInvalidated returns true when the iterator has been invalidated
	// using an Invalidate call.
	//
	// NB: this is different from Valid which indicates whether the current *KV*
	// is valid.
	IsDataInvalidated() bool
	// PrefixMatched reports whether the current key has the same prefix as
	// the reference key from the last positioning operation:
	//   - After SeekGE: the reference is the seek key's prefix.
	//   - After Next: the reference is the previous row's prefix.
	//
	// Returns PrefixMatchUnknown if the information is not cheaply available
	// (e.g. row format, transforms active, iterator invalid).
	PrefixMatched() PrefixMatchResult

	treesteps.Node
}

// Index is an interface for implementations of block iterators over index
// blocks. It's implemented by *rowblk.IndexIter and *colblk.IndexBlockIter.
type Index interface {
	// Init initializes the block iterator from the provided block.
	Init(*base.Comparer, []byte, Transforms) error
	// InitHandle initializes an iterator from the provided block handle.
	//
	// The iterator takes ownership of the BufferHandle and releases it when it is
	// closed (or re-initialized with another handle). This happens even in error
	// cases.
	InitHandle(*base.Comparer, block.BufferHandle, Transforms) error
	// Valid returns true if the iterator is currently positioned at a valid
	// block handle.
	Valid() bool
	// IsDataInvalidated returns true when the iterator has been invalidated
	// using an Invalidate call.
	//
	// NB: this is different from Valid which indicates whether the iterator is
	// currently positioned over a valid block entry.
	IsDataInvalidated() bool
	// Invalidate invalidates the block iterator, removing references to the
	// block it was initialized with. The iterator may continue to be used after
	// a call to Invalidate, but all positioning methods should return false.
	// Valid() must also return false.
	Invalidate()
	// Handle returns the underlying block buffer handle, if the iterator was
	// initialized with one.
	Handle() block.BufferHandle
	// Separator returns the separator at the iterator's current position. The
	// iterator must be positioned at a valid row. A Separator is a user key
	// guaranteed to be greater than or equal to every key contained within the
	// referenced block(s).
	Separator() []byte
	// SeparatorLT returns true if the separator at the iterator's current
	// position is strictly less than the provided key. For some
	// implementations, it may be more performant to call SeparatorLT rather
	// than explicitly performing Compare(Separator(), key) < 0.
	SeparatorLT(key []byte) bool
	// SeparatorGT returns true if the separator at the iterator's current
	// position is strictly greater than (or equal, if orEqual=true) the
	// provided key. For some implementations, it may be more performant to call
	// SeparatorGT rather than explicitly performing a comparison using the key
	// returned by Separator.
	SeparatorGT(key []byte, orEqual bool) bool
	// BlockHandleWithProperties decodes the block handle with any encoded
	// properties at the iterator's current position.
	BlockHandleWithProperties() (block.HandleWithProperties, error)
	// SeekGE seeks the index iterator to the first block entry with a separator
	// key greater or equal to the given key. If it returns true, the iterator
	// is positioned over the first block that might contain the key [key], and
	// following blocks have keys ≥ Separator(). It returns false if the seek
	// key is greater than all index block separators.
	SeekGE(key []byte) bool
	// First seeks index iterator to the first block entry. It returns false if
	// the index block is empty.
	First() bool
	// Last seeks index iterator to the last block entry. It returns false if
	// the index block is empty.
	Last() bool
	// Next steps the index iterator to the next block entry. It returns false
	// if the index block is exhausted in the forward direction. A call to Next
	// while already exhausted in the forward direction is a no-op.
	Next() bool
	// Prev steps the index iterator to the previous block entry. It returns
	// false if the index block is exhausted in the reverse direction. A call to
	// Prev while already exhausted in the reverse direction is a no-op.
	Prev() bool
	// Close closes the iterator, releasing any resources it holds. After Close,
	// the iterator must be reset such that it could be reused after a call to
	// Init or InitHandle.
	Close() error

	treesteps.Node
}
