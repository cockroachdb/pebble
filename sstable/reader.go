// Copyright 2011 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package sstable

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"sort"
	"sync"
	"unsafe"

	"github.com/cockroachdb/pebble/internal/base"
	"github.com/cockroachdb/pebble/internal/cache"
	"github.com/cockroachdb/pebble/internal/crc"
	"github.com/cockroachdb/pebble/internal/private"
	"github.com/cockroachdb/pebble/internal/rangedel"
	"github.com/cockroachdb/pebble/vfs"
	"github.com/golang/snappy"
)

var errCorruptIndexEntry = errors.New("pebble/table: corrupt index entry")

// decodeBlockHandle returns the block handle encoded at the start of src, as
// well as the number of bytes it occupies. It returns zero if given invalid
// input.
func decodeBlockHandle(src []byte) (BlockHandle, int) {
	offset, n := binary.Uvarint(src)
	length, m := binary.Uvarint(src[n:])
	if n == 0 || m == 0 {
		return BlockHandle{}, 0
	}
	return BlockHandle{offset, length}, n + m
}

func encodeBlockHandle(dst []byte, b BlockHandle) int {
	n := binary.PutUvarint(dst, b.Offset)
	m := binary.PutUvarint(dst[n:], b.Length)
	return n + m
}

// block is a []byte that holds a sequence of key/value pairs plus an index
// over those pairs.
type block []byte

// Iterator iterates over an entire table of data.
type Iterator interface {
	base.InternalIterator

	Init(r *Reader, lower, upper []byte) error
	SetCloseHook(fn func(i Iterator) error)
}

// singleLevelIterator iterates over an entire table of data. To seek for a given
// key, it first looks in the index for the block that contains that key, and then
// looks inside that block.
type singleLevelIterator struct {
	cmp Compare
	// Global lower/upper bound for the iterator.
	lower []byte
	upper []byte
	// Per-block lower/upper bound. Nil if the bound does not apply to the block
	// because we determined the block lies completely within the bound.
	blockLower []byte
	blockUpper []byte
	reader     *Reader
	index      blockIter
	data       blockIter
	dataBH     BlockHandle
	err        error
	closeHook  func(i Iterator) error
}

var singleLevelIterPool = sync.Pool{
	New: func() interface{} {
		return &singleLevelIterator{}
	},
}

var twoLevelIterPool = sync.Pool{
	New: func() interface{} {
		return &twoLevelIterator{}
	},
}

// Init initializes a singleLevelIterator for reading from the table. It is
// synonmous with Reader.NewIter, but allows for reusing of the iterator
// between different Readers.
func (i *singleLevelIterator) Init(r *Reader, lower, upper []byte) error {
	i.lower = lower
	i.upper = upper
	i.reader = r
	i.err = r.err

	if i.err == nil {
		var index block
		index, i.err = r.readIndex()
		if i.err != nil {
			return i.err
		}
		i.cmp = r.Compare
		i.err = i.index.init(i.cmp, index, r.Properties.GlobalSeqNum)
	}
	return i.err
}

func (i *singleLevelIterator) resetForReuse() singleLevelIterator {
	return singleLevelIterator{
		index: i.index.resetForReuse(),
		data:  i.data.resetForReuse(),
	}
}

func (i *singleLevelIterator) initBounds() {
	if i.lower == nil && i.upper == nil {
		return
	}

	// Trim the iteration bounds for the current block. We don't have to check
	// the bounds on each iteration if the block is entirely contained within the
	// iteration bounds.
	i.blockLower = i.lower
	if i.blockLower != nil {
		key, _ := i.data.First()
		if key != nil && i.cmp(i.blockLower, key.UserKey) < 0 {
			// The lower-bound is less than the first key in the block. No need
			// to check the lower-bound again for this block.
			i.blockLower = nil
		}
	}
	i.blockUpper = i.upper
	if i.blockUpper != nil && i.cmp(i.blockUpper, i.index.Key().UserKey) > 0 {
		// The upper-bound is greater than the index key which itself is greater
		// than or equal to every key in the block. No need to check the
		// upper-bound again for this block.
		i.blockUpper = nil
	}
}

// loadBlock loads the block at the current index position and leaves i.data
// unpositioned. If unsuccessful, it sets i.err to any error encountered, which
// may be nil if we have simply exhausted the entire table.
func (i *singleLevelIterator) loadBlock() bool {
	if !i.index.Valid() {
		i.err = i.index.err
		// TODO(peter): Need to test that seeking to a key outside of the sstable
		// invalidates the iterator.
		i.data.offset = 0
		i.data.restarts = 0
		return false
	}
	// Load the next block.
	v := i.index.Value()
	var n int
	i.dataBH, n = decodeBlockHandle(v)
	if n == 0 || n != len(v) {
		i.err = errCorruptIndexEntry
		return false
	}
	block, err := i.reader.readBlock(i.dataBH, nil /* transform */)
	if err != nil {
		i.err = err
		return false
	}
	i.data.setCacheHandle(block)
	i.err = i.data.init(i.cmp, block.Get(), i.reader.Properties.GlobalSeqNum)
	if i.err != nil {
		return false
	}
	i.initBounds()
	return true
}

// SeekGE implements internalIterator.SeekGE, as documented in the pebble
// package. Note that SeekGE only checks the upper bound. It is up to the
// caller to ensure that key is greater than or equal to the lower bound.
func (i *singleLevelIterator) SeekGE(key []byte) (*InternalKey, []byte) {
	if i.err != nil {
		return nil, nil
	}

	if ikey, _ := i.index.SeekGE(key); ikey == nil {
		return nil, nil
	}
	if !i.loadBlock() {
		return nil, nil
	}
	ikey, val := i.data.SeekGE(key)
	if ikey == nil {
		return nil, nil
	}
	if i.blockUpper != nil && i.cmp(ikey.UserKey, i.blockUpper) >= 0 {
		return nil, nil
	}
	return ikey, val
}

// SeekPrefixGE implements internalIterator.SeekPrefixGE, as documented in the
// pebble package. Note that SeekPrefixGE only checks the upper bound. It is up
// to the caller to ensure that key is greater than or equal to the lower bound.
func (i *singleLevelIterator) SeekPrefixGE(prefix, key []byte) (*InternalKey, []byte) {
	if i.err != nil {
		return nil, nil
	}

	// Check prefix bloom filter.
	if i.reader.tableFilter != nil {
		data, err := i.reader.readFilter()
		if err != nil {
			return nil, nil
		}
		if !i.reader.tableFilter.mayContain(data, prefix) {
			return nil, nil
		}
	}

	if ikey, _ := i.index.SeekGE(key); ikey == nil {
		return nil, nil
	}
	if !i.loadBlock() {
		return nil, nil
	}
	ikey, val := i.data.SeekGE(key)
	if ikey == nil {
		return nil, nil
	}
	if i.blockUpper != nil && i.cmp(ikey.UserKey, i.blockUpper) >= 0 {
		return nil, nil
	}
	return ikey, val
}

// SeekLT implements internalIterator.SeekLT, as documented in the pebble
// package. Note that SeekLT only checks the lower bound. It is up to the
// caller to ensure that key is less than the upper bound.
func (i *singleLevelIterator) SeekLT(key []byte) (*InternalKey, []byte) {
	if i.err != nil {
		return nil, nil
	}

	if ikey, _ := i.index.SeekGE(key); ikey == nil {
		i.index.Last()
	}
	if !i.loadBlock() {
		return nil, nil
	}
	ikey, val := i.data.SeekLT(key)
	if ikey == nil {
		// The index contains separator keys which may lie between
		// user-keys. Consider the user-keys:
		//
		//   complete
		// ---- new block ---
		//   complexion
		//
		// If these two keys end one block and start the next, the index key may
		// be chosen as "compleu". The SeekGE in the index block will then point
		// us to the block containing "complexion". If this happens, we want the
		// last key from the previous data block.
		if ikey, _ = i.index.Prev(); ikey == nil {
			return nil, nil
		}
		if !i.loadBlock() {
			return nil, nil
		}
		if ikey, val = i.data.Last(); ikey == nil {
			return nil, nil
		}
	}
	if i.blockLower != nil && i.cmp(ikey.UserKey, i.blockLower) < 0 {
		return nil, nil
	}
	return ikey, val
}

// First implements internalIterator.First, as documented in the pebble
// package. Note that First only checks the upper bound. It is up to the caller
// to ensure that key is greater than or equal to the lower bound (e.g. via a
// call to SeekGE(lower)).
func (i *singleLevelIterator) First() (*InternalKey, []byte) {
	if i.err != nil {
		return nil, nil
	}

	if ikey, _ := i.index.First(); ikey == nil {
		return nil, nil
	}
	if !i.loadBlock() {
		return nil, nil
	}
	ikey, val := i.data.First()
	if ikey == nil {
		return nil, nil
	}
	if i.blockUpper != nil && i.cmp(ikey.UserKey, i.blockUpper) >= 0 {
		return nil, nil
	}
	return ikey, val
}

// Last implements internalIterator.Last, as documented in the pebble
// package. Note that Last only checks the lower bound. It is up to the caller
// to ensure that key is less than the upper bound (e.g. via a call to
// SeekLT(upper))
func (i *singleLevelIterator) Last() (*InternalKey, []byte) {
	if i.err != nil {
		return nil, nil
	}

	if ikey, _ := i.index.Last(); ikey == nil {
		return nil, nil
	}
	if !i.loadBlock() {
		return nil, nil
	}
	if ikey, _ := i.data.Last(); ikey == nil {
		return nil, nil
	}
	if i.blockLower != nil && i.cmp(i.data.ikey.UserKey, i.blockLower) < 0 {
		return nil, nil
	}
	return &i.data.ikey, i.data.val
}

// Next implements internalIterator.Next, as documented in the pebble
// package.
// Note: compactionIterator.Next mirrors the implementation of Iterator.Next
// due to performance. Keep the two in sync.
func (i *singleLevelIterator) Next() (*InternalKey, []byte) {
	if i.err != nil {
		return nil, nil
	}
	if key, val := i.data.Next(); key != nil {
		if i.blockUpper != nil && i.cmp(key.UserKey, i.blockUpper) >= 0 {
			return nil, nil
		}
		return key, val
	}
	for {
		if i.data.err != nil {
			i.err = i.data.err
			break
		}
		if key, _ := i.index.Next(); key == nil {
			break
		}
		if i.loadBlock() {
			key, val := i.data.First()
			if key == nil {
				return nil, nil
			}
			if i.blockUpper != nil && i.cmp(key.UserKey, i.blockUpper) >= 0 {
				return nil, nil
			}
			return key, val
		}
	}
	return nil, nil
}

// Prev implements internalIterator.Prev, as documented in the pebble
// package.
func (i *singleLevelIterator) Prev() (*InternalKey, []byte) {
	if i.err != nil {
		return nil, nil
	}
	if key, val := i.data.Prev(); key != nil {
		if i.blockLower != nil && i.cmp(key.UserKey, i.blockLower) < 0 {
			return nil, nil
		}
		return key, val
	}
	for {
		if i.data.err != nil {
			i.err = i.data.err
			break
		}
		if key, _ := i.index.Prev(); key == nil {
			break
		}
		if i.loadBlock() {
			key, val := i.data.Last()
			if key == nil {
				return nil, nil
			}
			if i.blockLower != nil && i.cmp(key.UserKey, i.blockLower) < 0 {
				return nil, nil
			}
			return key, val
		}
	}
	return nil, nil
}

// Error implements internalIterator.Error, as documented in the pebble
// package.
func (i *singleLevelIterator) Error() error {
	if err := i.data.Error(); err != nil {
		return err
	}
	return i.err
}

// SetCloseHook sets a function that will be called when the iterator is
// closed.
func (i *singleLevelIterator) SetCloseHook(fn func(i Iterator) error) {
	i.closeHook = fn
}

// Close implements internalIterator.Close, as documented in the pebble
// package.
func (i *singleLevelIterator) Close() error {
	if i.closeHook != nil {
		if err := i.closeHook(i); err != nil {
			return err
		}
	}
	if err := i.data.Close(); err != nil {
		return err
	}
	err := i.err
	*i = i.resetForReuse()
	singleLevelIterPool.Put(i)
	return err
}

// SetBounds implements internalIterator.SetBounds, as documented in the pebble
// package.
func (i *singleLevelIterator) SetBounds(lower, upper []byte) {
	i.lower = lower
	i.upper = upper
}

// compactionIterator is similar to Iterator but it increments the number of
// bytes that have been iterated through.
type compactionIterator struct {
	*singleLevelIterator
	bytesIterated *uint64
	prevOffset    uint64
}

func (i *compactionIterator) SeekGE(key []byte) (*InternalKey, []byte) {
	panic("pebble: SeekGE unimplemented")
}

func (i *compactionIterator) SeekPrefixGE(prefix, key []byte) (*InternalKey, []byte) {
	panic("pebble: SeekPrefixGE unimplemented")
}

func (i *compactionIterator) SeekLT(key []byte) (*InternalKey, []byte) {
	panic("pebble: SeekLT unimplemented")
}

func (i *compactionIterator) First() (*InternalKey, []byte) {
	key, val := i.singleLevelIterator.First()
	if key == nil {
		// An empty sstable will still encode the block trailer and restart points, so bytes
		// iterated must be incremented.

		// We must use i.dataBH.Length instead of (4*(i.data.numRestarts+1)) to calculate the
		// number of bytes for the restart points, since i.dataBH.Length accounts for
		// compression. When uncompressed, i.dataBH.Length == (4*(i.data.numRestarts+1))
		*i.bytesIterated += blockTrailerLen + i.dataBH.Length
		return nil, nil
	}
	// If the sstable only has 1 entry, we are at the last entry in the block and we must
	// increment bytes iterated by the size of the block trailer and restart points.
	if i.data.nextOffset+(4*(i.data.numRestarts+1)) == int32(len(i.data.data)) {
		i.prevOffset = blockTrailerLen + i.dataBH.Length
	} else {
		// i.dataBH.Length/len(i.data.data) is the compression ratio. If uncompressed, this is 1.
		// i.data.nextOffset is the uncompressed size of the first record.
		i.prevOffset = (uint64(i.data.nextOffset) * i.dataBH.Length) / uint64(len(i.data.data))
	}
	*i.bytesIterated += i.prevOffset
	return key, val
}

func (i *compactionIterator) Last() (*InternalKey, []byte) {
	panic("pebble: Last unimplemented")
}

// Note: compactionIterator.Next mirrors the implementation of Iterator.Next
// due to performance. Keep the two in sync.
func (i *compactionIterator) Next() (*InternalKey, []byte) {
	if i.err != nil {
		return nil, nil
	}
	key, val := i.data.Next()
	if key == nil {
		for {
			if i.data.err != nil {
				i.err = i.data.err
				return nil, nil
			}
			if key, _ := i.index.Next(); key == nil {
				return nil, nil
			}
			if i.loadBlock() {
				key, val = i.data.First()
				if key == nil {
					return nil, nil
				}
				break
			}
		}
	}

	// i.dataBH.Length/len(i.data.data) is the compression ratio. If uncompressed, this is 1.
	// i.data.nextOffset is the uncompressed position of the current record in the block.
	// i.dataBH.Offset is the offset of the block in the sstable before decompression.
	recordOffset := (uint64(i.data.nextOffset) * i.dataBH.Length) / uint64(len(i.data.data))
	curOffset := i.dataBH.Offset + recordOffset
	// Last entry in the block must increment bytes iterated by the size of the block trailer
	// and restart points.
	if i.data.nextOffset+(4*(i.data.numRestarts+1)) == int32(len(i.data.data)) {
		curOffset = i.dataBH.Offset + i.dataBH.Length + blockTrailerLen
	}
	*i.bytesIterated += uint64(curOffset - i.prevOffset)
	i.prevOffset = curOffset
	return key, val
}

func (i *compactionIterator) Prev() (*InternalKey, []byte) {
	panic("pebble: Prev unimplemented")
}

type twoLevelIterator struct {
	singleLevelIterator
	topLevelIndex blockIter
}

// loadIndex loads the index block at the current top level index position and
// leaves i.index unpositioned. If unsuccessful, it gets i.err to any error
// encountered, which may be nil if we have simply exhausted the entire table.
// This is used for two level indexes.
func (i *twoLevelIterator) loadIndex() bool {
	if !i.topLevelIndex.Valid() {
		i.err = i.topLevelIndex.err
		i.index.offset = 0
		i.index.restarts = 0
		return false
	}
	h, n := decodeBlockHandle(i.topLevelIndex.Value())
	if n == 0 || n != len(i.topLevelIndex.Value()) {
		i.err = errors.New("pebble/table: corrupt top level index entry")
		return false
	}
	indexBlock, err := i.reader.readBlock(h, nil /* transform */)
	if err != nil {
		i.err = err
		return false
	}
	i.index.setCacheHandle(indexBlock)
	i.err = i.index.init(i.cmp, indexBlock.Get(), i.reader.Properties.GlobalSeqNum)
	if i.err != nil {
		return false
	}
	return true
}

func (i *twoLevelIterator) Init(r *Reader, lower, upper []byte) error {
	i.lower = lower
	i.upper = upper
	i.reader = r
	i.err = r.err

	if i.err == nil {
		topLevelIndex, err := r.readIndex()
		if i.err != nil {
			i.err = err
			return i.err
		}
		i.cmp = r.Compare
		i.err = i.topLevelIndex.init(i.cmp, topLevelIndex, r.Properties.GlobalSeqNum)
	}
	return i.err
}

// SeekGE implements internalIterator.SeekGE, as documented in the pebble
// package. Note that SeekGE only checks the upper bound. It is up to the
// caller to ensure that key is greater than or equal to the lower bound.
func (i *twoLevelIterator) SeekGE(key []byte) (*InternalKey, []byte) {
	if i.err != nil {
		return nil, nil
	}

	if ikey, _ := i.topLevelIndex.SeekGE(key); ikey == nil {
		return nil, nil
	}

	if !i.loadIndex() {
		return nil, nil
	}

	return i.singleLevelIterator.SeekGE(key)
}

// SeekPrefixGE implements internalIterator.SeekPrefixGE, as documented in the
// pebble package. Note that SeekPrefixGE only checks the upper bound. It is up
// to the caller to ensure that key is greater than or equal to the lower bound.
func (i *twoLevelIterator) SeekPrefixGE(prefix, key []byte) (*InternalKey, []byte) {
	if i.err != nil {
		return nil, nil
	}

	if ikey, _ := i.topLevelIndex.SeekGE(key); ikey == nil {
		return nil, nil
	}

	if !i.loadIndex() {
		return nil, nil
	}

	return i.singleLevelIterator.SeekPrefixGE(prefix, key)
}

// SeekLT implements internalIterator.SeekLT, as documented in the pebble
// package. Note that SeekLT only checks the lower bound. It is up to the
// caller to ensure that key is less than the upper bound.
func (i *twoLevelIterator) SeekLT(key []byte) (*InternalKey, []byte) {
	if i.err != nil {
		return nil, nil
	}

	if ikey, _ := i.topLevelIndex.SeekGE(key); ikey == nil {
		if ikey, _ := i.topLevelIndex.Last(); ikey == nil {
			return nil, nil
		}

		if !i.loadIndex() {
			return nil, nil
		}

		return i.singleLevelIterator.Last()
	}

	if !i.loadIndex() {
		return nil, nil
	}

	ikey, val := i.singleLevelIterator.SeekLT(key)
	if ikey == nil {
		if ikey, val = i.topLevelIndex.Prev(); ikey == nil {
			return nil, nil
		}
		if !i.loadIndex() {
			return nil, nil
		}
		if ikey, val = i.singleLevelIterator.Last(); ikey == nil {
			return nil, nil
		}
	}

	return ikey, val
}

// First implements internalIterator.First, as documented in the pebble
// package. Note that First only checks the upper bound. It is up to the caller
// to ensure that key is greater than or equal to the lower bound (e.g. via a
// call to SeekGE(lower)).
func (i *twoLevelIterator) First() (*InternalKey, []byte) {
	if i.err != nil {
		return nil, nil
	}

	if ikey, _ := i.topLevelIndex.First(); ikey == nil {
		return nil, nil
	}

	if !i.loadIndex() {
		return nil, nil
	}

	return i.singleLevelIterator.First()
}

// Last implements internalIterator.Last, as documented in the pebble
// package. Note that Last only checks the lower bound. It is up to the caller
// to ensure that key is less than the upper bound (e.g. via a call to
// SeekLT(upper))
func (i *twoLevelIterator) Last() (*InternalKey, []byte) {
	if i.err != nil {
		return nil, nil
	}

	if ikey, _ := i.topLevelIndex.Last(); ikey == nil {
		return nil, nil
	}

	if !i.loadIndex() {
		return nil, nil
	}

	return i.singleLevelIterator.Last()
}

// Next implements internalIterator.Next, as documented in the pebble
// package.
// Note: twoLevelCompactionIterator.Next mirrors the implementation of
// twoLevelIterator.Next due to performance. Keep the two in sync.
func (i *twoLevelIterator) Next() (*InternalKey, []byte) {
	if i.err != nil {
		return nil, nil
	}
	if key, val := i.singleLevelIterator.Next(); key != nil {
		return key, val
	}
	for {
		if i.index.err != nil {
			i.err = i.index.err
			break
		}
		if ikey, _ := i.topLevelIndex.Next(); ikey == nil {
			return nil, nil
		}
		if !i.loadIndex() {
			return nil, nil
		}
		return i.singleLevelIterator.First()
	}
	return nil, nil
}

// Prev implements internalIterator.Prev, as documented in the pebble
// package.
func (i *twoLevelIterator) Prev() (*InternalKey, []byte) {
	if i.err != nil {
		return nil, nil
	}
	if key, val := i.singleLevelIterator.Prev(); key != nil {
		return key, val
	}
	for {
		if i.index.err != nil {
			i.err = i.index.err
			break
		}
		if ikey, _ := i.topLevelIndex.Prev(); ikey == nil {
			return nil, nil
		}
		if !i.loadIndex() {
			return nil, nil
		}
		return i.singleLevelIterator.Last()
	}
	return nil, nil
}

// Close implements internalIterator.Close, as documented in the pebble
// package.
func (i *twoLevelIterator) Close() error {
	if i.closeHook != nil {
		if err := i.closeHook(i); err != nil {
			return err
		}
	}
	if err := i.data.Close(); err != nil {
		return err
	}
	err := i.err
	*i = twoLevelIterator{
		singleLevelIterator: i.singleLevelIterator.resetForReuse(),
		topLevelIndex:       i.topLevelIndex.resetForReuse(),
	}
	twoLevelIterPool.Put(i)
	return err
}

// Note: twoLevelCompactionIterator and compactionIterator are very similar but
// were separated due to performance.
type twoLevelCompactionIterator struct {
	*twoLevelIterator
	bytesIterated *uint64
	prevOffset    uint64
}

func (i *twoLevelCompactionIterator) SeekGE(key []byte) (*InternalKey, []byte) {
	panic("pebble: SeekGE unimplemented")
}

func (i *twoLevelCompactionIterator) SeekPrefixGE(prefix, key []byte) (*InternalKey, []byte) {
	panic("pebble: SeekPrefixGE unimplemented")
}

func (i *twoLevelCompactionIterator) SeekLT(key []byte) (*InternalKey, []byte) {
	panic("pebble: SeekLT unimplemented")
}

func (i *twoLevelCompactionIterator) First() (*InternalKey, []byte) {
	key, val := i.twoLevelIterator.First()
	if key == nil {
		// An empty sstable will still encode the block trailer and restart points, so bytes
		// iterated must be incremented.

		// We must use i.dataBH.Length instead of (4*(i.data.numRestarts+1)) to calculate the
		// number of bytes for the restart points, since i.dataBH.Length accounts for
		// compression. When uncompressed, i.dataBH.Length == (4*(i.data.numRestarts+1))
		*i.bytesIterated += blockTrailerLen + i.dataBH.Length
		return nil, nil
	}
	// If the sstable only has 1 entry, we are at the last entry in the block and we must
	// increment bytes iterated by the size of the block trailer and restart points.
	if i.data.nextOffset+(4*(i.data.numRestarts+1)) == int32(len(i.data.data)) {
		i.prevOffset = blockTrailerLen + i.dataBH.Length
	} else {
		// i.dataBH.Length/len(i.data.data) is the compression ratio. If uncompressed, this is 1.
		// i.data.nextOffset is the uncompressed size of the first record.
		i.prevOffset = (uint64(i.data.nextOffset) * i.dataBH.Length) / uint64(len(i.data.data))
	}
	*i.bytesIterated += i.prevOffset
	return key, val
}

func (i *twoLevelCompactionIterator) Last() (*InternalKey, []byte) {
	panic("pebble: Last unimplemented")
}

// Note: twoLevelCompactionIterator.Next mirrors the implementation of
// twoLevelIterator.Next due to performance. Keep the two in sync.
func (i *twoLevelCompactionIterator) Next() (*InternalKey, []byte) {
	if i.err != nil {
		return nil, nil
	}
	key, val := i.singleLevelIterator.Next()
	if key == nil {
		for {
			if i.index.err != nil {
				i.err = i.index.err
				return nil, nil
			}
			if key, _ := i.topLevelIndex.Next(); key == nil {
				return nil, nil
			}
			if i.loadIndex() {
				key, val = i.singleLevelIterator.First()
				if key == nil {
					return nil, nil
				}
				break
			}
		}
	}

	// i.dataBH.Length/len(i.data.data) is the compression ratio. If uncompressed, this is 1.
	// i.data.nextOffset is the uncompressed position of the current record in the block.
	// i.dataBH.Offset is the offset of the block in the sstable before decompression.
	recordOffset := (uint64(i.data.nextOffset) * i.dataBH.Length) / uint64(len(i.data.data))
	curOffset := i.dataBH.Offset + recordOffset
	// Last entry in the block must increment bytes iterated by the size of the block trailer
	// and restart points.
	if i.data.nextOffset+(4*(i.data.numRestarts+1)) == int32(len(i.data.data)) {
		curOffset = i.dataBH.Offset + i.dataBH.Length + blockTrailerLen
	}
	*i.bytesIterated += uint64(curOffset - i.prevOffset)
	i.prevOffset = curOffset
	return key, val
}

func (i *twoLevelCompactionIterator) Prev() (*InternalKey, []byte) {
	panic("pebble: Prev unimplemented")
}

type weakCachedBlock struct {
	bh     BlockHandle
	mu     sync.RWMutex
	handle cache.WeakHandle
}

type blockTransform func([]byte) ([]byte, error)

// ReaderOption provide an interface to do work on Reader while it is being
// opened.
type ReaderOption interface {
	// readerApply is called on the reader during opening in order to set internal
	// parameters.
	readerApply(*Reader)
}

// Comparers is a map from comparer name to comparer. It is used for debugging
// tools which may be used on multiple databases configured with different
// comparers. Comparers implements the OpenOption interface and can be passed
// as a parameter to NewReader.
type Comparers map[string]*Comparer

func (c Comparers) readerApply(r *Reader) {
	if r.Compare != nil || r.Properties.ComparerName == "" {
		return
	}
	if comparer, ok := c[r.Properties.ComparerName]; ok {
		r.Compare = comparer.Compare
		r.split = comparer.Split
	}
}

// Mergers is a map from merger name to merger. It is used for debugging tools
// which may be used on multiple databases configured with different
// mergers. Mergers implements the OpenOption interface and can be passed as
// a parameter to NewReader.
type Mergers map[string]*Merger

func (m Mergers) readerApply(r *Reader) {
	if r.mergerOK || r.Properties.MergerName == "" {
		return
	}
	_, r.mergerOK = m[r.Properties.MergerName]
}

// cacheOpts is a Reader open option for specifying the cache ID and sstable file
// number. If not specified, a unique cache ID will be used.
type cacheOpts struct {
	cacheID uint64
	fileNum uint64
}

// Marker function to indicate the option should be applied before reading the
// sstable properties.
func (c *cacheOpts) preApply() {}

func (c *cacheOpts) readerApply(r *Reader) {
	if r.cacheID == 0 {
		r.cacheID = c.cacheID
	}
	if r.fileNum == 0 {
		r.fileNum = c.fileNum
	}
}

func (c *cacheOpts) writerApply(w *Writer) {
	if w.cacheID == 0 {
		w.cacheID = c.cacheID
	}
	if w.fileNum == 0 {
		w.fileNum = c.fileNum
	}
}

// rawTombstonesOpt is a Reader open option for specifying that range
// tombstones returned by Reader.NewRangeDelIter() should not be
// fragmented. Used by debug tools to get a raw view of the tombstones
// contained in an sstable.
type rawTombstonesOpt struct{}

func (rawTombstonesOpt) preApply() {}

func (rawTombstonesOpt) readerApply(r *Reader) {
	r.rawTombstones = true
}

func init() {
	private.SSTableCacheOpts = func(cacheID, fileNum uint64) interface{} {
		return &cacheOpts{cacheID, fileNum}
	}
	private.SSTableRawTombstonesOpt = rawTombstonesOpt{}
}

// Reader is a table reader.
type Reader struct {
	file              vfs.File
	cacheID           uint64
	fileNum           uint64
	rawTombstones     bool
	err               error
	index             weakCachedBlock
	filter            weakCachedBlock
	rangeDel          weakCachedBlock
	rangeDelTransform blockTransform
	propertiesBH      BlockHandle
	metaIndexBH       BlockHandle
	footerBH          BlockHandle
	opts              ReaderOptions
	Compare           Compare
	split             Split
	mergerOK          bool
	tableFilter       *tableFilterReader
	Properties        Properties
}

// Close implements DB.Close, as documented in the pebble package.
func (r *Reader) Close() error {
	if r.err != nil {
		if r.file != nil {
			r.file.Close()
			r.file = nil
		}
		return r.err
	}
	if r.file != nil {
		r.err = r.file.Close()
		r.file = nil
		if r.err != nil {
			return r.err
		}
	}
	// Make any future calls to Get, NewIter or Close return an error.
	r.err = errors.New("pebble/table: reader is closed")
	return nil
}

// get is a testing helper that simulates a read and helps verify bloom filters
// until they are available through iterators.
func (r *Reader) get(key []byte) (value []byte, err error) {
	if r.err != nil {
		return nil, r.err
	}

	if r.tableFilter != nil {
		data, err := r.readFilter()
		if err != nil {
			return nil, err
		}
		var lookupKey []byte
		if r.split != nil {
			lookupKey = key[:r.split(key)]
		} else {
			lookupKey = key
		}
		if !r.tableFilter.mayContain(data, lookupKey) {
			return nil, base.ErrNotFound
		}
	}

	i := r.NewIter(nil /* lower */, nil /* upper */)
	ikey, value := i.SeekGE(key)

	if ikey == nil || r.Compare(key, ikey.UserKey) != 0 {
		err := i.Close()
		if err == nil {
			err = base.ErrNotFound
		}
		return nil, err
	}
	return value, i.Close()
}

// NewIter returns an iterator for the contents of the table.
func (r *Reader) NewIter(lower, upper []byte) Iterator {
	// NB: pebble.tableCache wraps the returned iterator with one which performs
	// reference counting on the Reader, preventing the Reader from being closed
	// until the final iterator closes.
	var i Iterator
	if r.Properties.IndexType == twoLevelIndex {
		i = twoLevelIterPool.Get().(*twoLevelIterator)
	} else {
		i = singleLevelIterPool.Get().(*singleLevelIterator)
	}
	_ = i.Init(r, lower, upper)
	return i
}

// NewCompactionIter returns an iterator similar to NewIter but it also increments
// the number of bytes iterated.
func (r *Reader) NewCompactionIter(bytesIterated *uint64) Iterator {
	if r.Properties.IndexType == twoLevelIndex {
		i := twoLevelIterPool.Get().(*twoLevelIterator)
		_ = i.Init(r, nil /* lower */, nil /* upper */)
		return &twoLevelCompactionIterator{
			twoLevelIterator: i,
			bytesIterated:    bytesIterated,
		}
	}
	i := singleLevelIterPool.Get().(*singleLevelIterator)
	_ = i.Init(r, nil /* lower */, nil /* upper */)
	return &compactionIterator{
		singleLevelIterator: i,
		bytesIterated:       bytesIterated,
	}
}

// NewRangeDelIter returns an internal iterator for the contents of the
// range-del block for the table. Returns nil if the table does not contain any
// range deletions.
func (r *Reader) NewRangeDelIter() base.InternalIterator {
	if r.rangeDel.bh.Length == 0 {
		return nil
	}
	b, err := r.readRangeDel()
	if err != nil {
		// TODO(peter): propagate the error
		panic(err)
	}
	i := &blockIter{}
	if err := i.init(r.Compare, b, r.Properties.GlobalSeqNum); err != nil {
		// TODO(peter): propagate the error
		panic(err)
	}
	return i
}

func (r *Reader) readIndex() (block, error) {
	return r.readWeakCachedBlock(&r.index, nil /* transform */)
}

func (r *Reader) readFilter() (block, error) {
	return r.readWeakCachedBlock(&r.filter, nil /* transform */)
}

func (r *Reader) readRangeDel() (block, error) {
	return r.readWeakCachedBlock(&r.rangeDel, r.rangeDelTransform)
}

func (r *Reader) readWeakCachedBlock(w *weakCachedBlock, transform blockTransform) (block, error) {
	// Fast-path for retrieving the block from a weak cache handle.
	w.mu.RLock()
	var b []byte
	if w.handle != nil {
		b = w.handle.Get()
	}
	w.mu.RUnlock()
	if b != nil {
		return b, nil
	}

	// Slow-path: read the index block from disk. This checks the cache again,
	// but that is ok because somebody else might have inserted it for us.
	h, err := r.readBlock(w.bh, transform)
	if err != nil {
		return nil, err
	}
	b = h.Get()
	if wh := h.Weak(); wh != nil {
		w.mu.Lock()
		w.handle = wh
		w.mu.Unlock()
	}
	return b, err
}

// readBlock reads and decompresses a block from disk into memory.
func (r *Reader) readBlock(bh BlockHandle, transform blockTransform) (cache.Handle, error) {
	if h := r.opts.Cache.Get(r.cacheID, r.fileNum, bh.Offset); h.Get() != nil {
		return h, nil
	}

	b := r.opts.Cache.Alloc(int(bh.Length + blockTrailerLen))
	if _, err := r.file.ReadAt(b, int64(bh.Offset)); err != nil {
		return cache.Handle{}, err
	}

	checksum0 := binary.LittleEndian.Uint32(b[bh.Length+1:])
	checksum1 := crc.New(b[:bh.Length+1]).Value()
	if checksum0 != checksum1 {
		return cache.Handle{}, errors.New("pebble/table: invalid table (checksum mismatch)")
	}

	typ := b[bh.Length]
	b = b[:bh.Length]

	switch typ {
	case noCompressionBlockType:
		break
	case snappyCompressionBlockType:
		decodedLen, err := snappy.DecodedLen(b)
		if err != nil {
			return cache.Handle{}, err
		}
		decoded := r.opts.Cache.Alloc(decodedLen)
		decoded, err = snappy.Decode(decoded, b)
		if err != nil {
			return cache.Handle{}, err
		}
		r.opts.Cache.Free(b)
		b = decoded
	default:
		return cache.Handle{}, fmt.Errorf("pebble/table: unknown block compression: %d", typ)
	}

	if transform != nil {
		// Transforming blocks is rare, so we don't bother to use cache.Alloc.
		var err error
		b, err = transform(b)
		if err != nil {
			return cache.Handle{}, err
		}
	}

	h := r.opts.Cache.Set(r.cacheID, r.fileNum, bh.Offset, b)
	return h, nil
}

func (r *Reader) transformRangeDelV1(b []byte) ([]byte, error) {
	// Convert v1 (RocksDB format) range-del blocks to v2 blocks on the fly. The
	// v1 format range-del blocks have unfragmented and unsorted range
	// tombstones. We need properly fragmented and sorted range tombstones in
	// order to serve from them directly.
	iter := &blockIter{}
	if err := iter.init(r.Compare, b, r.Properties.GlobalSeqNum); err != nil {
		return nil, err
	}
	var tombstones []rangedel.Tombstone
	for key, value := iter.First(); key != nil; key, value = iter.Next() {
		t := rangedel.Tombstone{
			Start: *key,
			End:   value,
		}
		tombstones = append(tombstones, t)
	}
	rangedel.Sort(r.Compare, tombstones)

	// Fragment the tombstones, outputting them directly to a block writer.
	rangeDelBlock := blockWriter{
		restartInterval: 1,
	}
	frag := rangedel.Fragmenter{
		Cmp: r.Compare,
		Emit: func(fragmented []rangedel.Tombstone) {
			for i := range fragmented {
				t := &fragmented[i]
				rangeDelBlock.add(t.Start, t.End)
			}
		},
	}
	for i := range tombstones {
		t := &tombstones[i]
		frag.Add(t.Start, t.End)
	}
	frag.Finish()

	// Return the contents of the constructed v2 format range-del block.
	return rangeDelBlock.finish(), nil
}

func (r *Reader) readMetaindex(metaindexBH BlockHandle) error {
	b, err := r.readBlock(metaindexBH, nil /* transform */)
	if err != nil {
		return err
	}
	i, err := newRawBlockIter(bytes.Compare, b.Get())
	b.Release()
	if err != nil {
		return err
	}

	meta := map[string]BlockHandle{}
	for valid := i.First(); valid; valid = i.Next() {
		bh, n := decodeBlockHandle(i.Value())
		if n == 0 {
			return errors.New("pebble/table: invalid table (bad filter block handle)")
		}
		meta[string(i.Key().UserKey)] = bh
	}
	if err := i.Close(); err != nil {
		return err
	}

	if bh, ok := meta[metaPropertiesName]; ok {
		b, err = r.readBlock(bh, nil /* transform */)
		if err != nil {
			return err
		}
		data := b.Get()
		r.propertiesBH = bh
		err := r.Properties.load(data, bh.Offset)
		b.Release()
		if err != nil {
			return err
		}
	}

	if bh, ok := meta[metaRangeDelV2Name]; ok {
		r.rangeDel.bh = bh
	} else if bh, ok := meta[metaRangeDelName]; ok {
		r.rangeDel.bh = bh
		if !r.rawTombstones {
			r.rangeDelTransform = r.transformRangeDelV1
		}
	}

	for name, fp := range r.opts.Filters {
		types := []struct {
			ftype  FilterType
			prefix string
		}{
			{TableFilter, "fullfilter."},
		}
		var done bool
		for _, t := range types {
			if bh, ok := meta[t.prefix+name]; ok {
				r.filter.bh = bh

				switch t.ftype {
				case TableFilter:
					r.tableFilter = newTableFilterReader(fp)
				default:
					return fmt.Errorf("unknown filter type: %v", t.ftype)
				}

				done = true
				break
			}
		}
		if done {
			break
		}
	}
	return nil
}

// Layout returns the layout (block organization) for an sstable.
func (r *Reader) Layout() (*Layout, error) {
	if r.err != nil {
		return nil, r.err
	}

	l := &Layout{
		Data:       make([]BlockHandle, 0, r.Properties.NumDataBlocks),
		Filter:     r.filter.bh,
		RangeDel:   r.rangeDel.bh,
		Properties: r.propertiesBH,
		MetaIndex:  r.metaIndexBH,
		Footer:     r.footerBH,
	}

	index, err := r.readIndex()
	if err != nil {
		return nil, err
	}

	if r.Properties.IndexPartitions == 0 {
		l.Index = append(l.Index, r.index.bh)
		iter, _ := newBlockIter(r.Compare, index)
		for key, value := iter.First(); key != nil; key, value = iter.Next() {
			dataBH, n := decodeBlockHandle(value)
			if n == 0 || n != len(value) {
				return nil, errCorruptIndexEntry
			}
			l.Data = append(l.Data, dataBH)
		}
	} else {
		l.TopIndex = r.index.bh
		topIter, _ := newBlockIter(r.Compare, index)
		for key, value := topIter.First(); key != nil; key, value = topIter.Next() {
			indexBH, n := decodeBlockHandle(value)
			if n == 0 || n != len(value) {
				return nil, errCorruptIndexEntry
			}
			l.Index = append(l.Index, indexBH)

			subIndex, err := r.readBlock(indexBH, nil /* transform */)
			if err != nil {
				return nil, err
			}
			iter, _ := newBlockIter(r.Compare, subIndex.Get())
			for key, value := iter.First(); key != nil; key, value = iter.Next() {
				dataBH, n := decodeBlockHandle(value)
				if n == 0 || n != len(value) {
					return nil, errCorruptIndexEntry
				}
				l.Data = append(l.Data, dataBH)
			}
			subIndex.Release()
		}
	}

	return l, nil
}

// EstimateDiskUsage returns the total size of data blocks overlapping the range
// `[start, end]`. Even if a data block partially overlaps, or we cannot determine
// overlap due to abbreviated index keys, the full data block size is included in
// the estimation. This function does not account for any metablock space usage.
// Assumes there is at least partial overlap, i.e., `[start, end]` falls neither
// completely before nor completely after the file's range.
//
// TODO(ajkr): account for metablock space usage. Perhaps look at the fraction of
// data blocks overlapped and add that same fraction of the metadata blocks to the
// estimate.
func (r *Reader) EstimateDiskUsage(start, end []byte) (uint64, error) {
	if r.err != nil {
		return 0, r.err
	}

	index, err := r.readIndex()
	if err != nil {
		return 0, err
	}

	// Iterators over the bottom-level index blocks containing start and end.
	// These may be different in case of partitioned index but will both point
	// to the same blockIter over the single index in the unpartitioned case.
	var startIdxIter, endIdxIter *blockIter
	if r.Properties.IndexPartitions == 0 {
		iter, err := newBlockIter(r.Compare, index)
		if err != nil {
			return 0, err
		}
		startIdxIter = iter
		endIdxIter = iter
	} else {
		topIter, err := newBlockIter(r.Compare, index)
		if err != nil {
			return 0, err
		}

		key, val := topIter.SeekGE(start)
		if key == nil {
			// The range falls completely after this file, or an error occurred.
			return 0, topIter.Error()
		}
		startIdxBH, n := decodeBlockHandle(val)
		if n == 0 || n != len(val) {
			return 0, errCorruptIndexEntry
		}
		startIdxBlock, err := r.readBlock(startIdxBH, nil /* transform */)
		if err != nil {
			return 0, err
		}
		startIdxIter, err = newBlockIter(r.Compare, startIdxBlock.Get())
		if err != nil {
			return 0, err
		}

		key, val = topIter.SeekGE(end)
		if key == nil {
			if err := topIter.Error(); err != nil {
				return 0, err
			}
		} else {
			endIdxBH, n := decodeBlockHandle(val)
			if n == 0 || n != len(val) {
				return 0, errCorruptIndexEntry
			}
			endIdxBlock, err := r.readBlock(endIdxBH, nil /* transform */)
			if err != nil {
				return 0, err
			}
			endIdxIter, err = newBlockIter(r.Compare, endIdxBlock.Get())
			if err != nil {
				return 0, err
			}
		}
	}
	// startIdxIter should not be nil at this point, while endIdxIter can be if the
	// range spans past the end of the file.

	key, val := startIdxIter.SeekGE(start)
	if key == nil {
		// The range falls completely after this file, or an error occurred.
		return 0, startIdxIter.Error()
	}
	startBH, n := decodeBlockHandle(val)
	if n == 0 || n != len(val) {
		return 0, errCorruptIndexEntry
	}

	if endIdxIter == nil {
		// The range spans beyond this file. Include data blocks through the last.
		return r.Properties.DataSize - startBH.Offset, nil
	}
	key, val = endIdxIter.SeekGE(end)
	if key == nil {
		if err := endIdxIter.Error(); err != nil {
			return 0, err
		}
		// The range spans beyond this file. Include data blocks through the last.
		return r.Properties.DataSize - startBH.Offset, nil
	}
	endBH, n := decodeBlockHandle(val)
	if n == 0 || n != len(val) {
		return 0, errCorruptIndexEntry
	}
	return endBH.Offset + endBH.Length + blockTrailerLen - startBH.Offset, nil
}

// NewReader returns a new table reader for the file. Closing the reader will
// close the file.
func NewReader(f vfs.File, o ReaderOptions, extraOpts ...ReaderOption) (*Reader, error) {
	o = o.ensureDefaults()
	r := &Reader{
		file: f,
		opts: o,
	}
	if f == nil {
		r.err = errors.New("pebble/table: nil file")
		return nil, r.Close()
	}

	// Note that the extra options are applied twice. First here for pre-apply
	// options, and then below for post-apply options. Pre and post refer to
	// before and after reading the metaindex and properties.
	type preApply interface{ preApply() }
	for _, opt := range extraOpts {
		if _, ok := opt.(preApply); ok {
			opt.readerApply(r)
		}
	}
	if r.cacheID == 0 {
		r.cacheID = r.opts.Cache.NewID()
	}

	footer, err := readFooter(f)
	if err != nil {
		r.err = err
		return nil, r.Close()
	}
	// Read the metaindex.
	if err := r.readMetaindex(footer.metaindexBH); err != nil {
		r.err = err
		return nil, r.Close()
	}
	r.index.bh = footer.indexBH
	r.metaIndexBH = footer.metaindexBH
	r.footerBH = footer.footerBH

	if r.Properties.ComparerName == "" || o.Comparer.Name == r.Properties.ComparerName {
		r.Compare = o.Comparer.Compare
		r.split = o.Comparer.Split
	}

	if o.MergerName == r.Properties.MergerName {
		r.mergerOK = true
	}

	// Apply the extra options again now that the comparer and merger names are
	// known.
	for _, opt := range extraOpts {
		if _, ok := opt.(preApply); !ok {
			opt.readerApply(r)
		}
	}

	if r.Compare == nil {
		r.err = fmt.Errorf("pebble/table: %d: unknown comparer %s",
			r.fileNum, r.Properties.ComparerName)
	}
	if !r.mergerOK {
		if name := r.Properties.MergerName; name != "" && name != "nullptr" {
			r.err = fmt.Errorf("pebble/table: %d: unknown merger %s",
				r.fileNum, r.Properties.MergerName)
		}
	}
	if r.err != nil {
		return nil, r.Close()
	}
	return r, nil
}

// Layout describes the block organization of an sstable.
type Layout struct {
	Data       []BlockHandle
	Index      []BlockHandle
	TopIndex   BlockHandle
	Filter     BlockHandle
	RangeDel   BlockHandle
	Properties BlockHandle
	MetaIndex  BlockHandle
	Footer     BlockHandle
}

// Describe returns a description of the layout. If the verbose parameter is
// true, details of the structure of each block are returned as well.
func (l *Layout) Describe(
	w io.Writer, verbose bool, r *Reader, fmtRecord func(key *base.InternalKey, value []byte),
) {
	type block struct {
		BlockHandle
		name string
	}
	var blocks []block

	for i := range l.Data {
		blocks = append(blocks, block{l.Data[i], "data"})
	}
	for i := range l.Index {
		blocks = append(blocks, block{l.Index[i], "index"})
	}
	if l.TopIndex.Length != 0 {
		blocks = append(blocks, block{l.TopIndex, "top-index"})
	}
	if l.Filter.Length != 0 {
		blocks = append(blocks, block{l.Filter, "filter"})
	}
	if l.RangeDel.Length != 0 {
		blocks = append(blocks, block{l.RangeDel, "range-del"})
	}
	if l.Properties.Length != 0 {
		blocks = append(blocks, block{l.Properties, "properties"})
	}
	if l.MetaIndex.Length != 0 {
		blocks = append(blocks, block{l.MetaIndex, "meta-index"})
	}
	if l.Footer.Length != 0 {
		if l.Footer.Length == levelDBFooterLen {
			blocks = append(blocks, block{l.Footer, "leveldb-footer"})
		} else {
			blocks = append(blocks, block{l.Footer, "footer"})
		}
	}

	sort.Slice(blocks, func(i, j int) bool {
		return blocks[i].Offset < blocks[j].Offset
	})

	for i := range blocks {
		b := &blocks[i]
		fmt.Fprintf(w, "%10d  %s (%d)\n", b.Offset, b.name, b.Length)

		if !verbose {
			continue
		}
		if b.name == "footer" || b.name == "leveldb-footer" || b.name == "filter" {
			continue
		}

		h, err := r.readBlock(b.BlockHandle, nil /* transform */)
		if err != nil {
			fmt.Fprintf(w, "  [err: %s]\n", err)
			continue
		}

		getRestart := func(data []byte, restarts, i int32) int32 {
			return int32(binary.LittleEndian.Uint32(data[restarts+4*i:]))
		}

		formatIsRestart := func(data []byte, restarts, numRestarts, offset int32) {
			i := sort.Search(int(numRestarts), func(i int) bool {
				return getRestart(data, restarts, int32(i)) >= offset
			})
			if i < int(numRestarts) && getRestart(data, restarts, int32(i)) == offset {
				fmt.Fprintf(w, " [restart]\n")
			} else {
				fmt.Fprintf(w, "\n")
			}
		}

		formatRestarts := func(data []byte, restarts, numRestarts int32) {
			for i := int32(0); i < numRestarts; i++ {
				offset := getRestart(data, restarts, i)
				fmt.Fprintf(w, "%10d    [restart %d]\n",
					b.Offset+uint64(restarts+4*i), b.Offset+uint64(offset))
			}
		}

		var lastKey InternalKey
		switch b.name {
		case "data", "range-del":
			iter, _ := newBlockIter(r.Compare, h.Get())
			for key, value := iter.First(); key != nil; key, value = iter.Next() {
				ptr := unsafe.Pointer(uintptr(iter.ptr) + uintptr(iter.offset))
				shared, ptr := decodeVarint(ptr)
				unshared, ptr := decodeVarint(ptr)
				value2, _ := decodeVarint(ptr)

				total := iter.nextOffset - iter.offset
				// The format of the numbers in the record line is:
				//
				//   (<total> = <length> [<shared>] + <unshared> + <value>)
				//
				// <total>    is the total number of bytes for the record.
				// <length>   is the size of the 3 varint encoded integers for <shared>,
				//            <unshared>, and <value>.
				// <shared>   is the number of key bytes shared with the previous key.
				// <unshared> is the number of unshared key bytes.
				// <value>    is the number of value bytes.
				fmt.Fprintf(w, "%10d    record (%d = %d [%d] + %d + %d)",
					b.Offset+uint64(iter.offset), total,
					total-int32(unshared+value2), shared, unshared, value2)
				formatIsRestart(iter.data, iter.restarts, iter.numRestarts, iter.offset)
				if fmtRecord != nil {
					fmt.Fprintf(w, "              ")
					fmtRecord(key, value)
				}

				if base.InternalCompare(r.Compare, lastKey, *key) >= 0 {
					fmt.Fprintf(w, "              WARNING: OUT OF ORDER KEYS!\n")
				}
				lastKey.Trailer = key.Trailer
				lastKey.UserKey = append(lastKey.UserKey[:0], key.UserKey...)
			}
			formatRestarts(iter.data, iter.restarts, iter.numRestarts)
		case "index", "top-index":
			iter, _ := newBlockIter(r.Compare, h.Get())
			for key, value := iter.First(); key != nil; key, value = iter.Next() {
				bh, n := decodeBlockHandle(value)
				if n == 0 || n != len(value) {
					fmt.Fprintf(w, "%10d    [err: %s]\n", b.Offset+uint64(iter.offset), err)
					continue
				}
				fmt.Fprintf(w, "%10d    block:%d/%d",
					b.Offset+uint64(iter.offset), bh.Offset, bh.Length)
				formatIsRestart(iter.data, iter.restarts, iter.numRestarts, iter.offset)
			}
			formatRestarts(iter.data, iter.restarts, iter.numRestarts)
		case "properties":
			iter, _ := newRawBlockIter(r.Compare, h.Get())
			for valid := iter.First(); valid; valid = iter.Next() {
				fmt.Fprintf(w, "%10d    %s (%d)",
					b.Offset+uint64(iter.offset), iter.Key().UserKey, iter.nextOffset-iter.offset)
				formatIsRestart(iter.data, iter.restarts, iter.numRestarts, iter.offset)
			}
			formatRestarts(iter.data, iter.restarts, iter.numRestarts)
		case "meta-index":
			iter, _ := newRawBlockIter(r.Compare, h.Get())
			for valid := iter.First(); valid; valid = iter.Next() {
				value := iter.Value()
				bh, n := decodeBlockHandle(value)
				if n == 0 || n != len(value) {
					fmt.Fprintf(w, "%10d    [err: %s]\n", b.Offset+uint64(iter.offset), err)
					continue
				}

				fmt.Fprintf(w, "%10d    %s block:%d/%d",
					b.Offset+uint64(iter.offset), iter.Key().UserKey,
					bh.Offset, bh.Length)
				formatIsRestart(iter.data, iter.restarts, iter.numRestarts, iter.offset)
			}
			formatRestarts(iter.data, iter.restarts, iter.numRestarts)
		}

		h.Release()
	}
}
