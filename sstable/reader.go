// Copyright 2011 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package sstable

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"io"
	"os"
	"runtime"
	"sort"
	"sync"
	"unsafe"

	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/pebble/internal/base"
	"github.com/cockroachdb/pebble/internal/cache"
	"github.com/cockroachdb/pebble/internal/crc"
	"github.com/cockroachdb/pebble/internal/invariants"
	"github.com/cockroachdb/pebble/internal/private"
	"github.com/cockroachdb/pebble/internal/rangedel"
	"github.com/cockroachdb/pebble/vfs"
	"github.com/golang/snappy"
)

var errCorruptIndexEntry = base.CorruptionErrorf("pebble/table: corrupt index entry")

const (
	// Constants for dynamic readahead of data blocks. Note that the size values
	// make sense as some multiple of the default block size; and they should
	// both be larger than the default block size.
	minFileReadsForReadahead = 2
	// TODO(bilal): Have the initial size value be a factor of the block size,
	// as opposed to a hardcoded value.
	initialReadaheadSize = 64 << 10  /* 64KB */
	maxReadaheadSize     = 256 << 10 /* 256KB */
)

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
	dataRS     readaheadState
	dataBH     BlockHandle
	err        error
	closeHook  func(i Iterator) error
}

// singleLevelIterator implements the base.InternalIterator interface.
var _ base.InternalIterator = (*singleLevelIterator)(nil)

var singleLevelIterPool = sync.Pool{
	New: func() interface{} {
		i := &singleLevelIterator{}
		if invariants.Enabled {
			runtime.SetFinalizer(i, checkSingleLevelIterator)
		}
		return i
	},
}

var twoLevelIterPool = sync.Pool{
	New: func() interface{} {
		i := &twoLevelIterator{}
		if invariants.Enabled {
			runtime.SetFinalizer(i, checkTwoLevelIterator)
		}
		return i
	},
}

func checkSingleLevelIterator(obj interface{}) {
	i := obj.(*singleLevelIterator)
	if p := i.data.cacheHandle.Get(); p != nil {
		fmt.Fprintf(os.Stderr, "singleLevelIterator.data.cacheHandle is not nil: %p\n", p)
		os.Exit(1)
	}
	if p := i.index.cacheHandle.Get(); p != nil {
		fmt.Fprintf(os.Stderr, "singleLevelIterator.index.cacheHandle is not nil: %p\n", p)
		os.Exit(1)
	}
}

func checkTwoLevelIterator(obj interface{}) {
	i := obj.(*twoLevelIterator)
	if p := i.data.cacheHandle.Get(); p != nil {
		fmt.Fprintf(os.Stderr, "singleLevelIterator.data.cacheHandle is not nil: %p\n", p)
		os.Exit(1)
	}
	if p := i.index.cacheHandle.Get(); p != nil {
		fmt.Fprintf(os.Stderr, "singleLevelIterator.index.cacheHandle is not nil: %p\n", p)
		os.Exit(1)
	}
}

// init initializes a singleLevelIterator for reading from the table. It is
// synonmous with Reader.NewIter, but allows for reusing of the iterator
// between different Readers.
func (i *singleLevelIterator) init(r *Reader, lower, upper []byte) error {
	if r.err != nil {
		return r.err
	}
	indexH, err := r.readIndex()
	if err != nil {
		return err
	}

	i.lower = lower
	i.upper = upper
	i.reader = r
	i.cmp = r.Compare
	err = i.index.initHandle(i.cmp, indexH, r.Properties.GlobalSeqNum)
	if err != nil {
		// blockIter.Close releases indexH and always returns a nil error
		_ = i.index.Close()
		return err
	}
	i.dataRS.size = initialReadaheadSize
	return nil
}

// setupForCompaction sets up the singleLevelIterator for use with compactionIter.
// Currently, it skips readahead ramp-up. It should be called after init is called.
func (i *singleLevelIterator) setupForCompaction() {
	if i.reader.fs != nil {
		f, err := i.reader.fs.Open(i.reader.filename, vfs.SequentialReadsOption)
		if err == nil {
			// Given that this iterator is for a compaction, we can assume that it
			// will be read sequentially and we can skip the readahead ramp-up.
			i.dataRS.sequentialFile = f
		}
	}
}

func (i *singleLevelIterator) resetForReuse() singleLevelIterator {
	return singleLevelIterator{
		index: i.index.resetForReuse(),
		data:  i.data.resetForReuse(),
	}
}

func (i *singleLevelIterator) initBounds() {
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
	// Ensure the data block iterator is invalidated even if loading of the block
	// fails.
	i.data.invalidate()
	if !i.index.Valid() {
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
	block, err := i.reader.readBlock(i.dataBH, nil /* transform */, &i.dataRS)
	if err != nil {
		i.err = err
		return false
	}
	i.err = i.data.initHandle(i.cmp, block, i.reader.Properties.GlobalSeqNum)
	if i.err != nil {
		return false
	}
	i.initBounds()
	return true
}

func (i *singleLevelIterator) recordOffset() uint64 {
	offset := i.dataBH.Offset
	if i.data.Valid() {
		// - i.dataBH.Length/len(i.data.data) is the compression ratio. If
		//   uncompressed, this is 1.
		// - i.data.nextOffset is the uncompressed position of the current record
		//   in the block.
		// - i.dataBH.Offset is the offset of the block in the sstable before
		//   decompression.
		offset += (uint64(i.data.nextOffset) * i.dataBH.Length) / uint64(len(i.data.data))
	} else {
		// Last entry in the block must increment bytes iterated by the size of the block trailer
		// and restart points.
		offset += i.dataBH.Length + blockTrailerLen
	}
	return offset
}

// SeekGE implements internalIterator.SeekGE, as documented in the pebble
// package. Note that SeekGE only checks the upper bound. It is up to the
// caller to ensure that key is greater than or equal to the lower bound.
func (i *singleLevelIterator) SeekGE(key []byte) (*InternalKey, []byte) {
	i.err = nil // clear cached iteration error

	if ikey, _ := i.index.SeekGE(key); ikey == nil {
		// The target key is greater than any key in the sstable. Invalidate the
		// block iterator so that a subsequent call to Prev() will return the last
		// key in the table.
		i.data.invalidate()
		return nil, nil
	}
	if !i.loadBlock() {
		return nil, nil
	}
	if ikey, val := i.data.SeekGE(key); ikey != nil {
		if i.blockUpper != nil && i.cmp(ikey.UserKey, i.blockUpper) >= 0 {
			return nil, nil
		}
		return ikey, val
	}
	return i.skipForward()
}

// SeekPrefixGE implements internalIterator.SeekPrefixGE, as documented in the
// pebble package. Note that SeekPrefixGE only checks the upper bound. It is up
// to the caller to ensure that key is greater than or equal to the lower bound.
func (i *singleLevelIterator) SeekPrefixGE(prefix, key []byte) (*InternalKey, []byte) {
	i.err = nil // clear cached iteration error

	// Check prefix bloom filter.
	if i.reader.tableFilter != nil {
		var dataH cache.Handle
		dataH, i.err = i.reader.readFilter()
		if i.err != nil {
			i.data.invalidate()
			return nil, nil
		}
		mayContain := i.reader.tableFilter.mayContain(dataH.Get(), prefix)
		dataH.Release()
		if !mayContain {
			i.data.invalidate()
			return nil, nil
		}
	}

	if ikey, _ := i.index.SeekGE(key); ikey == nil {
		i.data.invalidate()
		return nil, nil
	}
	if !i.loadBlock() {
		return nil, nil
	}
	if ikey, val := i.data.SeekGE(key); ikey != nil {
		if i.blockUpper != nil && i.cmp(ikey.UserKey, i.blockUpper) >= 0 {
			return nil, nil
		}
		return ikey, val
	}
	return i.skipForward()
}

// SeekLT implements internalIterator.SeekLT, as documented in the pebble
// package. Note that SeekLT only checks the lower bound. It is up to the
// caller to ensure that key is less than the upper bound.
func (i *singleLevelIterator) SeekLT(key []byte) (*InternalKey, []byte) {
	i.err = nil // clear cached iteration error

	if ikey, _ := i.index.SeekGE(key); ikey == nil {
		i.index.Last()
	}
	if !i.loadBlock() {
		return nil, nil
	}
	if ikey, val := i.data.SeekLT(key); ikey != nil {
		if i.blockLower != nil && i.cmp(ikey.UserKey, i.blockLower) < 0 {
			return nil, nil
		}
		return ikey, val
	}
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
	return i.skipBackward()
}

// First implements internalIterator.First, as documented in the pebble
// package. Note that First only checks the upper bound. It is up to the caller
// to ensure that key is greater than or equal to the lower bound (e.g. via a
// call to SeekGE(lower)).
func (i *singleLevelIterator) First() (*InternalKey, []byte) {
	i.err = nil // clear cached iteration error

	if ikey, _ := i.index.First(); ikey == nil {
		i.data.invalidate()
		return nil, nil
	}
	if !i.loadBlock() {
		return nil, nil
	}
	if ikey, val := i.data.First(); ikey != nil {
		if i.blockUpper != nil && i.cmp(ikey.UserKey, i.blockUpper) >= 0 {
			return nil, nil
		}
		return ikey, val
	}
	return i.skipForward()
}

// Last implements internalIterator.Last, as documented in the pebble
// package. Note that Last only checks the lower bound. It is up to the caller
// to ensure that key is less than the upper bound (e.g. via a call to
// SeekLT(upper))
func (i *singleLevelIterator) Last() (*InternalKey, []byte) {
	i.err = nil // clear cached iteration error

	if ikey, _ := i.index.Last(); ikey == nil {
		i.data.invalidate()
		return nil, nil
	}
	if !i.loadBlock() {
		return nil, nil
	}
	if ikey, val := i.data.Last(); ikey != nil {
		if i.blockLower != nil && i.cmp(ikey.UserKey, i.blockLower) < 0 {
			return nil, nil
		}
		return ikey, val
	}
	return i.skipBackward()
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
	return i.skipForward()
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
	return i.skipBackward()
}

func (i *singleLevelIterator) skipForward() (*InternalKey, []byte) {
	for {
		if key, _ := i.index.Next(); key == nil {
			i.data.invalidate()
			break
		}
		if !i.loadBlock() {
			if i.err != nil {
				break
			}
			continue
		}
		if key, val := i.data.First(); key != nil {
			if i.blockUpper != nil && i.cmp(key.UserKey, i.blockUpper) >= 0 {
				return nil, nil
			}
			return key, val
		}
	}
	return nil, nil
}

func (i *singleLevelIterator) skipBackward() (*InternalKey, []byte) {
	for {
		if key, _ := i.index.Prev(); key == nil {
			i.data.invalidate()
			break
		}
		if !i.loadBlock() {
			if i.err != nil {
				break
			}
			continue
		}
		key, val := i.data.Last()
		if key == nil {
			return nil, nil
		}
		if i.blockLower != nil && i.cmp(key.UserKey, i.blockLower) < 0 {
			return nil, nil
		}
		return key, val
	}
	return nil, nil
}

// Returns true if the data block iterator points to a valid entry. If a
// positioning operation (e.g. SeekGE, SeekLT, Next, Prev, etc) returns (nil,
// nil) and valid() is true, the iterator has reached either the upper or lower
// bound.
func (i *singleLevelIterator) valid() bool {
	return i.data.Valid()
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

func firstError(err0, err1 error) error {
	if err0 != nil {
		return err0
	}
	return err1
}

// Close implements internalIterator.Close, as documented in the pebble
// package.
func (i *singleLevelIterator) Close() error {
	var err error
	if i.closeHook != nil {
		err = firstError(err, i.closeHook(i))
	}
	err = firstError(err, i.data.Close())
	err = firstError(err, i.index.Close())
	if i.dataRS.sequentialFile != nil {
		err = firstError(err, i.dataRS.sequentialFile.Close())
		i.dataRS.sequentialFile = nil
	}
	err = firstError(err, i.err)
	*i = i.resetForReuse()
	singleLevelIterPool.Put(i)
	return err
}

func (i *singleLevelIterator) String() string {
	return i.reader.fileNum.String()
}

// SetBounds implements internalIterator.SetBounds, as documented in the pebble
// package.
func (i *singleLevelIterator) SetBounds(lower, upper []byte) {
	i.lower = lower
	i.upper = upper
	i.blockLower = nil
	i.blockUpper = nil
}

// compactionIterator is similar to Iterator but it increments the number of
// bytes that have been iterated through.
type compactionIterator struct {
	*singleLevelIterator
	bytesIterated *uint64
	prevOffset    uint64
}

// compactionIterator implements the base.InternalIterator interface.
var _ base.InternalIterator = (*compactionIterator)(nil)

func (i *compactionIterator) String() string {
	return i.reader.fileNum.String()
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
	i.err = nil // clear cached iteration error
	return i.skipForward(i.singleLevelIterator.First())
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
	return i.skipForward(i.data.Next())
}

func (i *compactionIterator) Prev() (*InternalKey, []byte) {
	panic("pebble: Prev unimplemented")
}

func (i *compactionIterator) skipForward(key *InternalKey, val []byte) (*InternalKey, []byte) {
	if key == nil {
		for {
			if key, _ := i.index.Next(); key == nil {
				break
			}
			if !i.loadBlock() {
				if i.err != nil {
					break
				}
				continue
			}
			if key, val = i.data.First(); key != nil {
				break
			}
		}
	}

	curOffset := i.recordOffset()
	*i.bytesIterated += uint64(curOffset - i.prevOffset)
	i.prevOffset = curOffset
	return key, val
}

type twoLevelIterator struct {
	singleLevelIterator
	topLevelIndex blockIter
}

// twoLevelIterator implements the base.InternalIterator interface.
var _ base.InternalIterator = (*twoLevelIterator)(nil)

// loadIndex loads the index block at the current top level index position and
// leaves i.index unpositioned. If unsuccessful, it gets i.err to any error
// encountered, which may be nil if we have simply exhausted the entire table.
// This is used for two level indexes.
func (i *twoLevelIterator) loadIndex() bool {
	// Ensure the data block iterator is invalidated even if loading of the
	// index fails.
	i.data.invalidate()
	if !i.topLevelIndex.Valid() {
		i.index.offset = 0
		i.index.restarts = 0
		return false
	}
	h, n := decodeBlockHandle(i.topLevelIndex.Value())
	if n == 0 || n != len(i.topLevelIndex.Value()) {
		i.err = base.CorruptionErrorf("pebble/table: corrupt top level index entry")
		return false
	}
	indexBlock, err := i.reader.readBlock(h, nil /* transform */, nil /* readaheadState */)
	if err != nil {
		i.err = err
		return false
	}
	i.err = i.index.initHandle(i.cmp, indexBlock, i.reader.Properties.GlobalSeqNum)
	return i.err == nil
}

func (i *twoLevelIterator) init(r *Reader, lower, upper []byte) error {
	if r.err != nil {
		return r.err
	}
	topLevelIndexH, err := r.readIndex()
	if err != nil {
		return err
	}

	i.lower = lower
	i.upper = upper
	i.reader = r
	i.cmp = r.Compare
	err = i.topLevelIndex.initHandle(i.cmp, topLevelIndexH, r.Properties.GlobalSeqNum)
	if err != nil {
		// blockIter.Close releases topLevelIndexH and always returns a nil error
		_ = i.topLevelIndex.Close()
		return err
	}
	return nil
}

func (i *twoLevelIterator) String() string {
	return i.reader.fileNum.String()
}

// SeekGE implements internalIterator.SeekGE, as documented in the pebble
// package. Note that SeekGE only checks the upper bound. It is up to the
// caller to ensure that key is greater than or equal to the lower bound.
func (i *twoLevelIterator) SeekGE(key []byte) (*InternalKey, []byte) {
	i.err = nil // clear cached iteration error

	if ikey, _ := i.topLevelIndex.SeekGE(key); ikey == nil {
		i.data.invalidate()
		i.index.invalidate()
		return nil, nil
	}

	if !i.loadIndex() {
		return nil, nil
	}

	if ikey, val := i.singleLevelIterator.SeekGE(key); ikey != nil {
		return ikey, val
	}
	return i.skipForward()
}

// SeekPrefixGE implements internalIterator.SeekPrefixGE, as documented in the
// pebble package. Note that SeekPrefixGE only checks the upper bound. It is up
// to the caller to ensure that key is greater than or equal to the lower bound.
func (i *twoLevelIterator) SeekPrefixGE(prefix, key []byte) (*InternalKey, []byte) {
	i.err = nil // clear cached iteration error

	if ikey, _ := i.topLevelIndex.SeekGE(key); ikey == nil {
		i.data.invalidate()
		i.index.invalidate()
		return nil, nil
	}

	if !i.loadIndex() {
		return nil, nil
	}

	if ikey, val := i.singleLevelIterator.SeekPrefixGE(prefix, key); ikey != nil {
		return ikey, val
	}
	return i.skipForward()
}

// SeekLT implements internalIterator.SeekLT, as documented in the pebble
// package. Note that SeekLT only checks the lower bound. It is up to the
// caller to ensure that key is less than the upper bound.
func (i *twoLevelIterator) SeekLT(key []byte) (*InternalKey, []byte) {
	i.err = nil // clear cached iteration error

	if ikey, _ := i.topLevelIndex.SeekGE(key); ikey == nil {
		if ikey, _ := i.topLevelIndex.Last(); ikey == nil {
			i.data.invalidate()
			i.index.invalidate()
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

	if ikey, val := i.singleLevelIterator.SeekLT(key); ikey != nil {
		return ikey, val
	}
	return i.skipBackward()
}

// First implements internalIterator.First, as documented in the pebble
// package. Note that First only checks the upper bound. It is up to the caller
// to ensure that key is greater than or equal to the lower bound (e.g. via a
// call to SeekGE(lower)).
func (i *twoLevelIterator) First() (*InternalKey, []byte) {
	i.err = nil // clear cached iteration error

	if ikey, _ := i.topLevelIndex.First(); ikey == nil {
		return nil, nil
	}

	if !i.loadIndex() {
		return nil, nil
	}

	if ikey, val := i.singleLevelIterator.First(); ikey != nil {
		return ikey, val
	}
	return i.skipForward()
}

// Last implements internalIterator.Last, as documented in the pebble
// package. Note that Last only checks the lower bound. It is up to the caller
// to ensure that key is less than the upper bound (e.g. via a call to
// SeekLT(upper))
func (i *twoLevelIterator) Last() (*InternalKey, []byte) {
	i.err = nil // clear cached iteration error

	if ikey, _ := i.topLevelIndex.Last(); ikey == nil {
		return nil, nil
	}

	if !i.loadIndex() {
		return nil, nil
	}

	if ikey, val := i.singleLevelIterator.Last(); ikey != nil {
		return ikey, val
	}
	return i.skipBackward()
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
	return i.skipForward()
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
	return i.skipBackward()
}

func (i *twoLevelIterator) skipForward() (*InternalKey, []byte) {
	for {
		if i.err != nil {
			return nil, nil
		}
		if i.singleLevelIterator.valid() {
			// The iterator is positioned at valid record in the current data block
			// which implies the previous positioning call reached the upper bound.
			return nil, nil
		}
		if ikey, _ := i.topLevelIndex.Next(); ikey == nil {
			i.data.invalidate()
			i.index.invalidate()
			return nil, nil
		}
		if !i.loadIndex() {
			return nil, nil
		}
		if ikey, val := i.singleLevelIterator.First(); ikey != nil {
			return ikey, val
		}
	}
}

func (i *twoLevelIterator) skipBackward() (*InternalKey, []byte) {
	for {
		if i.err != nil {
			return nil, nil
		}
		if i.singleLevelIterator.valid() {
			// The iterator is positioned at valid record in the current data block
			// which implies the previous positioning call reached the lower bound.
			return nil, nil
		}
		if ikey, _ := i.topLevelIndex.Prev(); ikey == nil {
			i.data.invalidate()
			i.index.invalidate()
			return nil, nil
		}
		if !i.loadIndex() {
			return nil, nil
		}
		if ikey, val := i.singleLevelIterator.Last(); ikey != nil {
			return ikey, val
		}
	}
}

// Close implements internalIterator.Close, as documented in the pebble
// package.
func (i *twoLevelIterator) Close() error {
	var err error
	if i.closeHook != nil {
		err = firstError(err, i.closeHook(i))
	}
	err = firstError(err, i.data.Close())
	err = firstError(err, i.index.Close())
	err = firstError(err, i.topLevelIndex.Close())
	if i.dataRS.sequentialFile != nil {
		err = firstError(err, i.dataRS.sequentialFile.Close())
		i.dataRS.sequentialFile = nil
	}
	err = firstError(err, i.err)
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

// twoLevelCompactionIterator implements the base.InternalIterator interface.
var _ base.InternalIterator = (*twoLevelCompactionIterator)(nil)

func (i *twoLevelCompactionIterator) Close() error {
	return i.twoLevelIterator.Close()
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
	i.err = nil // clear cached iteration error
	return i.skipForward(i.twoLevelIterator.First())
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
	return i.skipForward(i.singleLevelIterator.Next())
}

func (i *twoLevelCompactionIterator) Prev() (*InternalKey, []byte) {
	panic("pebble: Prev unimplemented")
}

func (i *twoLevelCompactionIterator) String() string {
	return i.reader.fileNum.String()
}

func (i *twoLevelCompactionIterator) skipForward(
	key *InternalKey, val []byte,
) (*InternalKey, []byte) {
	if key == nil {
		for {
			if key, _ := i.topLevelIndex.Next(); key == nil {
				break
			}
			if i.loadIndex() {
				if key, val = i.singleLevelIterator.First(); key != nil {
					break
				}
			}
		}
	}

	curOffset := i.recordOffset()
	*i.bytesIterated += uint64(curOffset - i.prevOffset)
	i.prevOffset = curOffset
	return key, val
}

type blockTransform func([]byte) ([]byte, error)

// readaheadState contains state variables related to readahead. Updated on
// file reads.
type readaheadState struct {
	// Number of sequential reads.
	numReads int64
	// Size issued to the next call to Prefetch. Starts at or above
	// initialReadaheadSize and grows exponentially until maxReadaheadSize.
	size int64
	// prevSize is the size used in the last Prefetch call.
	prevSize int64
	// The byte offset up to which the OS has been asked to read ahead / cached.
	// When reading ahead, reads up to this limit should not incur an IO
	// operation. Reads after this limit can benefit from a new call to
	// Prefetch.
	limit int64
	// sequentialFile holds a file descriptor to the same underlying File,
	// except with fadvise(FADV_SEQUENTIAL) called on it to take advantage of
	// OS-level readahead. Initialized when the iterator has been consistently
	// reading blocks in a sequential access pattern. Once this is non-nil,
	// the other variables in readaheadState don't matter much as we defer
	// to OS-level readahead.
	sequentialFile vfs.File
}

func (rs *readaheadState) recordCacheHit(offset, blockLength int64) {
	currentReadEnd := offset + blockLength
	if rs.sequentialFile != nil {
		// Using OS-level readahead instead, so do nothing.
		return
	}
	if rs.numReads >= minFileReadsForReadahead {
		if currentReadEnd >= rs.limit && offset <= rs.limit+maxReadaheadSize {
			// This is a read that would have resulted in a readahead, had it
			// not been a cache hit.
			rs.limit = currentReadEnd
			return
		}
		if currentReadEnd < rs.limit-rs.prevSize || offset > rs.limit+maxReadaheadSize {
			// We read too far away from rs.limit to benefit from readahead in
			// any scenario. Reset all variables.
			rs.numReads = 1
			rs.limit = currentReadEnd
			rs.size = initialReadaheadSize
			rs.prevSize = 0
			return
		}
		// Reads in the range [rs.limit - rs.prevSize, rs.limit] end up
		// here. This is a read that is potentially benefitting from a past
		// readahead.
		return
	}
	if currentReadEnd >= rs.limit && offset <= rs.limit+maxReadaheadSize {
		// Blocks are being read sequentially and would benefit from readahead
		// down the line.
		rs.numReads++
		return
	}
	// We read too far ahead of the last read, or before it. This indicates
	// a random read, where readahead is not desirable. Reset all variables.
	rs.numReads = 1
	rs.limit = currentReadEnd
	rs.size = initialReadaheadSize
	rs.prevSize = 0
}

// maybeReadahead updates state and determines whether to issue a readahead /
// prefetch call for a block read at offset for blockLength bytes.
// Returns a size value (greater than 0) that should be prefetched if readahead
// would be beneficial.
func (rs *readaheadState) maybeReadahead(offset, blockLength int64) int64 {
	currentReadEnd := offset + blockLength
	if rs.sequentialFile != nil {
		// Using OS-level readahead instead, so do nothing.
		return 0
	}
	if rs.numReads >= minFileReadsForReadahead {
		// The minimum threshold of sequential reads to justify reading ahead
		// has been reached.
		// There are two intervals: the interval being read:
		// [offset, currentReadEnd]
		// as well as the interval where a read would benefit from read ahead:
		// [rs.limit, rs.limit + rs.size]
		// We increase the latter interval to
		// [rs.limit, rs.limit + maxReadaheadSize] to account for cases where
		// readahead may not be beneficial with a small readahead size, but over
		// time the readahead size would increase exponentially to make it
		// beneficial.
		if currentReadEnd >= rs.limit && offset <= rs.limit+maxReadaheadSize {
			// We are doing a read in the interval ahead of
			// the last readahead range. In the diagrams below, ++++ is the last
			// readahead range, ==== is the range represented by
			// [rs.limit, rs.limit + maxReadaheadSize], and ---- is the range
			// being read.
			//
			//               rs.limit           rs.limit + maxReadaheadSize
			//         ++++++++++|===========================|
			//
			//              |-------------|
			//            offset       currentReadEnd
			//
			// This case is also possible, as are all cases with an overlap
			// between [rs.limit, rs.limit + maxReadaheadSize] and [offset,
			// currentReadEnd]:
			//
			//               rs.limit           rs.limit + maxReadaheadSize
			//         ++++++++++|===========================|
			//
			//                                            |-------------|
			//                                         offset       currentReadEnd
			//
			//
			rs.numReads++
			rs.limit = offset + rs.size
			rs.prevSize = rs.size
			// Increase rs.size for the next read.
			rs.size *= 2
			if rs.size > maxReadaheadSize {
				rs.size = maxReadaheadSize
			}
			return rs.prevSize
		}
		if currentReadEnd < rs.limit-rs.prevSize || offset > rs.limit+maxReadaheadSize {
			// The above conditional has rs.limit > rs.prevSize to confirm that
			// rs.limit - rs.prevSize would not underflow.
			// We read too far away from rs.limit to benefit from readahead in
			// any scenario. Reset all variables.
			// The case where we read too far ahead:
			//
			// (rs.limit - rs.prevSize)    (rs.limit)   (rs.limit + maxReadaheadSize)
			//                    |+++++++++++++|=============|
			//
			//                                                  |-------------|
			//                                             offset       currentReadEnd
			//
			// Or too far behind:
			//
			// (rs.limit - rs.prevSize)    (rs.limit)   (rs.limit + maxReadaheadSize)
			//                    |+++++++++++++|=============|
			//
			//    |-------------|
			// offset       currentReadEnd
			//
			rs.numReads = 1
			rs.limit = currentReadEnd
			rs.size = initialReadaheadSize
			rs.prevSize = 0
			return 0
		}
		// Reads in the range [rs.limit - rs.prevSize, rs.limit] end up
		// here. This is a read that is potentially benefitting from a past
		// readahead, but there's no reason to issue a readahead call at the
		// moment.
		//
		// (rs.limit - rs.prevSize)            (rs.limit + maxReadaheadSize)
		//                    |+++++++++++++|===============|
		//                             (rs.limit)
		//
		//                        |-------|
		//                     offset    currentReadEnd
		//
		rs.numReads++
		return 0
	}
	if currentReadEnd >= rs.limit && offset <= rs.limit+maxReadaheadSize {
		// Blocks are being read sequentially and would benefit from readahead
		// down the line.
		//
		//                       (rs.limit)   (rs.limit + maxReadaheadSize)
		//                         |=============|
		//
		//                    |-------|
		//                offset    currentReadEnd
		//
		rs.numReads++
		return 0
	}
	// We read too far ahead of the last read, or before it. This indicates
	// a random read, where readahead is not desirable. Reset all variables.
	//
	// (rs.limit - maxReadaheadSize)  (rs.limit)   (rs.limit + maxReadaheadSize)
	//                     |+++++++++++++|=============|
	//
	//                                                    |-------|
	//                                                offset    currentReadEnd
	//
	rs.numReads = 1
	rs.limit = currentReadEnd
	rs.size = initialReadaheadSize
	rs.prevSize = 0
	return 0
}

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
		r.FormatKey = comparer.FormatKey
		r.Split = comparer.Split
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
	fileNum base.FileNum
}

// Marker function to indicate the option should be applied before reading the
// sstable properties and, in the write path, before writing the default
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

// FileReopenOpt is specified if this reader is allowed to reopen additional
// file descriptors for this file. Used to take advantage of OS-level readahead.
type FileReopenOpt struct {
	FS       vfs.FS
	Filename string
}

func (f FileReopenOpt) readerApply(r *Reader) {
	if r.fs == nil {
		r.fs = f.FS
		r.filename = f.Filename
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
	private.SSTableCacheOpts = func(cacheID uint64, fileNum base.FileNum) interface{} {
		return &cacheOpts{cacheID, fileNum}
	}
	private.SSTableRawTombstonesOpt = rawTombstonesOpt{}
}

// Reader is a table reader.
type Reader struct {
	file              vfs.File
	fs                vfs.FS
	filename          string
	cacheID           uint64
	fileNum           base.FileNum
	rawTombstones     bool
	err               error
	indexBH           BlockHandle
	filterBH          BlockHandle
	rangeDelBH        BlockHandle
	rangeDelTransform blockTransform
	propertiesBH      BlockHandle
	metaIndexBH       BlockHandle
	footerBH          BlockHandle
	opts              ReaderOptions
	Compare           Compare
	FormatKey         base.FormatKey
	Split             Split
	mergerOK          bool
	tableFilter       *tableFilterReader
	Properties        Properties
}

// Close implements DB.Close, as documented in the pebble package.
func (r *Reader) Close() error {
	r.opts.Cache.Unref()

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
		dataH, err := r.readFilter()
		if err != nil {
			return nil, err
		}
		var lookupKey []byte
		if r.Split != nil {
			lookupKey = key[:r.Split(key)]
		} else {
			lookupKey = key
		}
		mayContain := r.tableFilter.mayContain(dataH.Get(), lookupKey)
		dataH.Release()
		if !mayContain {
			return nil, base.ErrNotFound
		}
	}

	i, err := r.NewIter(nil /* lower */, nil /* upper */)
	if err != nil {
		return nil, err
	}
	ikey, value := i.SeekGE(key)

	if ikey == nil || r.Compare(key, ikey.UserKey) != 0 {
		err := i.Close()
		if err == nil {
			err = base.ErrNotFound
		}
		return nil, err
	}

	// The value will be "freed" when the iterator is closed, so make a copy
	// which will outlast the lifetime of the iterator.
	newValue := make([]byte, len(value))
	copy(newValue, value)
	if err := i.Close(); err != nil {
		return nil, err
	}
	return newValue, nil
}

// NewIter returns an iterator for the contents of the table. If an error
// occurs, NewIter cleans up after itself and returns a nil iterator.
func (r *Reader) NewIter(lower, upper []byte) (Iterator, error) {
	// NB: pebble.tableCache wraps the returned iterator with one which performs
	// reference counting on the Reader, preventing the Reader from being closed
	// until the final iterator closes.
	if r.Properties.IndexType == twoLevelIndex {
		i := twoLevelIterPool.Get().(*twoLevelIterator)
		err := i.init(r, lower, upper)
		if err != nil {
			return nil, err
		}
		return i, nil
	}

	i := singleLevelIterPool.Get().(*singleLevelIterator)
	err := i.init(r, lower, upper)
	if err != nil {
		return nil, err
	}
	return i, nil
}

// NewCompactionIter returns an iterator similar to NewIter but it also increments
// the number of bytes iterated. If an error occurs, NewCompactionIter cleans up
// after itself and returns a nil iterator.
func (r *Reader) NewCompactionIter(bytesIterated *uint64) (Iterator, error) {
	if r.Properties.IndexType == twoLevelIndex {
		i := twoLevelIterPool.Get().(*twoLevelIterator)
		err := i.init(r, nil /* lower */, nil /* upper */)
		if err != nil {
			return nil, err
		}
		i.setupForCompaction()
		return &twoLevelCompactionIterator{
			twoLevelIterator: i,
			bytesIterated:    bytesIterated,
		}, nil
	}
	i := singleLevelIterPool.Get().(*singleLevelIterator)
	err := i.init(r, nil /* lower */, nil /* upper */)
	if err != nil {
		return nil, err
	}
	i.setupForCompaction()
	return &compactionIterator{
		singleLevelIterator: i,
		bytesIterated:       bytesIterated,
	}, nil
}

// NewRawRangeDelIter returns an internal iterator for the contents of the
// range-del block for the table. Returns nil if the table does not contain
// any range deletions.
func (r *Reader) NewRawRangeDelIter() (base.InternalIterator, error) {
	if r.rangeDelBH.Length == 0 {
		return nil, nil
	}
	h, err := r.readRangeDel()
	if err != nil {
		return nil, err
	}
	i := &blockIter{}
	if err := i.initHandle(r.Compare, h, r.Properties.GlobalSeqNum); err != nil {
		return nil, err
	}
	return i, nil
}

func (r *Reader) readIndex() (cache.Handle, error) {
	return r.readBlock(r.indexBH, nil /* transform */, nil /* readaheadState */)
}

func (r *Reader) readFilter() (cache.Handle, error) {
	return r.readBlock(r.filterBH, nil /* transform */, nil /* readaheadState */)
}

func (r *Reader) readRangeDel() (cache.Handle, error) {
	return r.readBlock(r.rangeDelBH, r.rangeDelTransform, nil /* readaheadState */)
}

// readBlock reads and decompresses a block from disk into memory.
func (r *Reader) readBlock(
	bh BlockHandle, transform blockTransform, raState *readaheadState,
) (cache.Handle, error) {
	if h := r.opts.Cache.Get(r.cacheID, r.fileNum, bh.Offset); h.Get() != nil {
		if raState != nil {
			raState.recordCacheHit(int64(bh.Offset), int64(bh.Length+blockTrailerLen))
		}
		return h, nil
	}
	file := r.file

	if raState != nil {
		if raState.sequentialFile != nil {
			file = raState.sequentialFile
		} else if readaheadSize := raState.maybeReadahead(int64(bh.Offset), int64(bh.Length+blockTrailerLen)); readaheadSize > 0 {
			if readaheadSize >= maxReadaheadSize {
				// We've reached the maximum readahead size. Beyond this
				// point, rely on OS-level readahead. Note that we can only
				// reopen a new file handle with this optimization if
				// r.fs != nil. This reader must have been created with the
				// FileReopenOpt for this field to be set.
				if r.fs != nil {
					f, err := r.fs.Open(r.filename, vfs.SequentialReadsOption)
					if err == nil {
						// Use this new file handle for all sequential reads by
						// this iterator going forward.
						raState.sequentialFile = f
						file = f
					}
				}
			}
			if raState.sequentialFile != nil {
				_ = vfs.Prefetch(r.file, bh.Offset, uint64(readaheadSize))
			}
		}
	}

	v := r.opts.Cache.Alloc(int(bh.Length + blockTrailerLen))
	b := v.Buf()
	if _, err := file.ReadAt(b, int64(bh.Offset)); err != nil {
		r.opts.Cache.Free(v)
		return cache.Handle{}, err
	}

	checksum0 := binary.LittleEndian.Uint32(b[bh.Length+1:])
	checksum1 := crc.New(b[:bh.Length+1]).Value()
	if checksum0 != checksum1 {
		r.opts.Cache.Free(v)
		return cache.Handle{}, base.CorruptionErrorf(
			"pebble/table: invalid table %s (checksum mismatch at %d/%d)",
			errors.Safe(r.fileNum), errors.Safe(bh.Offset), errors.Safe(bh.Length))
	}

	typ := b[bh.Length]
	b = b[:bh.Length]
	v.Truncate(len(b))

	switch typ {
	case noCompressionBlockType:
		break
	case snappyCompressionBlockType:
		decodedLen, err := snappy.DecodedLen(b)
		if err != nil {
			r.opts.Cache.Free(v)
			return cache.Handle{}, base.MarkCorruptionError(err)
		}
		decoded := r.opts.Cache.Alloc(decodedLen)
		decodedBuf := decoded.Buf()
		result, err := snappy.Decode(decodedBuf, b)
		r.opts.Cache.Free(v)
		if err != nil {
			r.opts.Cache.Free(decoded)
			return cache.Handle{}, base.MarkCorruptionError(err)
		}
		if len(result) != 0 &&
			(len(result) != len(decodedBuf) || &result[0] != &decodedBuf[0]) {
			r.opts.Cache.Free(decoded)
			return cache.Handle{}, base.CorruptionErrorf("pebble/table: snappy decoded into unexpected buffer: %p != %p",
				errors.Safe(result), errors.Safe(decodedBuf))
		}
		v, b = decoded, decodedBuf
	default:
		r.opts.Cache.Free(v)
		return cache.Handle{}, base.CorruptionErrorf("pebble/table: unknown block compression: %d", errors.Safe(typ))
	}

	if transform != nil {
		// Transforming blocks is rare, so the extra copy of the transformed data
		// is not problematic.
		var err error
		b, err = transform(b)
		if err != nil {
			r.opts.Cache.Free(v)
			return cache.Handle{}, err
		}
		newV := r.opts.Cache.Alloc(len(b))
		copy(newV.Buf(), b)
		r.opts.Cache.Free(v)
		v = newV
	}

	h := r.opts.Cache.Set(r.cacheID, r.fileNum, bh.Offset, v)
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
		Cmp:    r.Compare,
		Format: r.FormatKey,
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
	b, err := r.readBlock(metaindexBH, nil /* transform */, nil /* readaheadState */)
	if err != nil {
		return err
	}
	data := b.Get()
	defer b.Release()

	if uint64(len(data)) != metaindexBH.Length {
		return base.CorruptionErrorf("pebble/table: unexpected metaindex block size: %d vs %d",
			errors.Safe(len(data)), errors.Safe(metaindexBH.Length))
	}

	i, err := newRawBlockIter(bytes.Compare, data)
	if err != nil {
		return err
	}

	meta := map[string]BlockHandle{}
	for valid := i.First(); valid; valid = i.Next() {
		bh, n := decodeBlockHandle(i.Value())
		if n == 0 {
			return base.CorruptionErrorf("pebble/table: invalid table (bad filter block handle)")
		}
		meta[string(i.Key().UserKey)] = bh
	}
	if err := i.Close(); err != nil {
		return err
	}

	if bh, ok := meta[metaPropertiesName]; ok {
		b, err = r.readBlock(bh, nil /* transform */, nil /* readaheadState */)
		if err != nil {
			return err
		}
		r.propertiesBH = bh
		err := r.Properties.load(b.Get(), bh.Offset)
		b.Release()
		if err != nil {
			return err
		}
	}

	if bh, ok := meta[metaRangeDelV2Name]; ok {
		r.rangeDelBH = bh
	} else if bh, ok := meta[metaRangeDelName]; ok {
		r.rangeDelBH = bh
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
				r.filterBH = bh

				switch t.ftype {
				case TableFilter:
					r.tableFilter = newTableFilterReader(fp)
				default:
					return base.CorruptionErrorf("unknown filter type: %v", errors.Safe(t.ftype))
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
		Filter:     r.filterBH,
		RangeDel:   r.rangeDelBH,
		Properties: r.propertiesBH,
		MetaIndex:  r.metaIndexBH,
		Footer:     r.footerBH,
	}

	indexH, err := r.readIndex()
	if err != nil {
		return nil, err
	}
	defer indexH.Release()

	if r.Properties.IndexPartitions == 0 {
		l.Index = append(l.Index, r.indexBH)
		iter, _ := newBlockIter(r.Compare, indexH.Get())
		for key, value := iter.First(); key != nil; key, value = iter.Next() {
			dataBH, n := decodeBlockHandle(value)
			if n == 0 || n != len(value) {
				return nil, errCorruptIndexEntry
			}
			l.Data = append(l.Data, dataBH)
		}
	} else {
		l.TopIndex = r.indexBH
		topIter, _ := newBlockIter(r.Compare, indexH.Get())
		for key, value := topIter.First(); key != nil; key, value = topIter.Next() {
			indexBH, n := decodeBlockHandle(value)
			if n == 0 || n != len(value) {
				return nil, errCorruptIndexEntry
			}
			l.Index = append(l.Index, indexBH)

			subIndex, err := r.readBlock(indexBH, nil /* transform */, nil /* readaheadState */)
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

	indexH, err := r.readIndex()
	if err != nil {
		return 0, err
	}
	defer indexH.Release()

	// Iterators over the bottom-level index blocks containing start and end.
	// These may be different in case of partitioned index but will both point
	// to the same blockIter over the single index in the unpartitioned case.
	var startIdxIter, endIdxIter *blockIter
	if r.Properties.IndexPartitions == 0 {
		iter, err := newBlockIter(r.Compare, indexH.Get())
		if err != nil {
			return 0, err
		}
		startIdxIter = iter
		endIdxIter = iter
	} else {
		topIter, err := newBlockIter(r.Compare, indexH.Get())
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
		startIdxBlock, err := r.readBlock(startIdxBH, nil /* transform */, nil /* readaheadState */)
		if err != nil {
			return 0, err
		}
		defer startIdxBlock.Release()
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
			endIdxBlock, err := r.readBlock(endIdxBH, nil /* transform */, nil /* readaheadState */)
			if err != nil {
				return 0, err
			}
			defer endIdxBlock.Release()
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
	if r.opts.Cache == nil {
		r.opts.Cache = cache.New(0)
	} else {
		r.opts.Cache.Ref()
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
	r.indexBH = footer.indexBH
	r.metaIndexBH = footer.metaindexBH
	r.footerBH = footer.footerBH

	if r.Properties.ComparerName == "" || o.Comparer.Name == r.Properties.ComparerName {
		r.Compare = o.Comparer.Compare
		r.FormatKey = o.Comparer.FormatKey
		r.Split = o.Comparer.Split
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
		r.err = errors.Errorf("pebble/table: %d: unknown comparer %s",
			errors.Safe(r.fileNum), errors.Safe(r.Properties.ComparerName))
	}
	if !r.mergerOK {
		if name := r.Properties.MergerName; name != "" && name != "nullptr" {
			r.err = errors.Errorf("pebble/table: %d: unknown merger %s",
				errors.Safe(r.fileNum), errors.Safe(r.Properties.MergerName))
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

		h, err := r.readBlock(b.BlockHandle, nil /* transform */, nil /* readaheadState */)
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
