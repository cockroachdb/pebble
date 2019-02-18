// Copyright 2011 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package sstable

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"sync"

	"github.com/golang/snappy"
	"github.com/petermattis/pebble/cache"
	"github.com/petermattis/pebble/db"
	"github.com/petermattis/pebble/internal/crc"
	"github.com/petermattis/pebble/storage"
)

// blockHandle is the file offset and length of a block.
type blockHandle struct {
	offset, length uint64
}

// decodeBlockHandle returns the block handle encoded at the start of src, as
// well as the number of bytes it occupies. It returns zero if given invalid
// input.
func decodeBlockHandle(src []byte) (blockHandle, int) {
	offset, n := binary.Uvarint(src)
	length, m := binary.Uvarint(src[n:])
	if n == 0 || m == 0 {
		return blockHandle{}, 0
	}
	return blockHandle{offset, length}, n + m
}

func encodeBlockHandle(dst []byte, b blockHandle) int {
	n := binary.PutUvarint(dst, b.offset)
	m := binary.PutUvarint(dst[n:], b.length)
	return n + m
}

// block is a []byte that holds a sequence of key/value pairs plus an index
// over those pairs.
type block []byte

// Iterator iterates over an entire table of data. It is a two-level iterator:
// to seek for a given key, it first looks in the index for the block that
// contains that key, and then looks inside that block.
type Iterator struct {
	reader    *Reader
	index     blockIter
	data      blockIter
	err       error
	closeHook func() error
}

func (i *Iterator) init(r *Reader) error {
	i.reader = r
	var index block
	index, i.err = r.readIndex()
	if i.err != nil {
		return i.err
	}
	i.err = i.index.init(r.compare, index, r.Properties.GlobalSeqNum)
	return i.err
}

// loadBlock loads the block at the current index position and leaves i.data
// unpositioned. If unsuccessful, it sets i.err to any error encountered, which
// may be nil if we have simply exhausted the entire table.
func (i *Iterator) loadBlock() bool {
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
	h, n := decodeBlockHandle(v)
	if n == 0 || n != len(v) {
		i.err = errors.New("pebble/table: corrupt index entry")
		return false
	}
	block, _, err := i.reader.readBlock(h)
	if err != nil {
		i.err = err
		return false
	}
	i.err = i.data.init(i.reader.compare, block, i.reader.Properties.GlobalSeqNum)
	return i.err == nil
}

// seekBlock loads the block at the current index position and positions i.data
// at the first key in that block which is >= the given key. If unsuccessful,
// it sets i.err to any error encountered, which may be nil if we have simply
// exhausted the entire table.
func (i *Iterator) seekBlock(key []byte) bool {
	if !i.index.Valid() {
		i.err = i.index.err
		return false
	}
	// Load the next block.
	v := i.index.Value()
	h, n := decodeBlockHandle(v)
	if n == 0 || n != len(v) {
		i.err = errors.New("pebble/table: corrupt index entry")
		return false
	}
	block, _, err := i.reader.readBlock(h)
	if err != nil {
		i.err = err
		return false
	}
	i.err = i.data.init(i.reader.compare, block, i.reader.Properties.GlobalSeqNum)
	if i.err != nil {
		return false
	}
	// Look for the key inside that block.
	i.data.SeekGE(key)
	return true
}

// SeekGE implements internalIterator.SeekGE, as documented in the pebble
// package.
func (i *Iterator) SeekGE(key []byte) bool {
	if i.err != nil {
		return false
	}

	// NB: the top-level Iterator has already adjusted key based on
	// IterOptions.LowerBound.

	if !i.index.SeekGE(key) {
		return false
	}
	if !i.loadBlock() {
		return false
	}
	return i.data.SeekGE(key)
}

// SeekLT implements internalIterator.SeekLT, as documented in the pebble
// package.
func (i *Iterator) SeekLT(key []byte) bool {
	if i.err != nil {
		return false
	}

	// NB: the top-level Iterator has already adjusted key based on
	// IterOptions.UpperBound.

	if !i.index.SeekGE(key) {
		i.index.Last()
	}
	if !i.loadBlock() {
		return false
	}
	if i.data.SeekLT(key) {
		return true
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
	if !i.index.Prev() {
		return false
	}
	if !i.loadBlock() {
		return false
	}
	return i.data.Last()
}

// First implements internalIterator.First, as documented in the pebble
// package.
func (i *Iterator) First() bool {
	if i.err != nil {
		return false
	}

	// NB: the top-level Iterator will call SeekGE if IterOptions.LowerBound is
	// set.

	if !i.index.First() {
		return false
	}
	if !i.loadBlock() {
		return false
	}
	return i.data.First()
}

// Last implements internalIterator.Last, as documented in the pebble
// package.
func (i *Iterator) Last() bool {
	if i.err != nil {
		return false
	}

	// NB: the top-level Iterator will call SeekLT if IterOptions.UpperBound is
	// set.

	if !i.index.Last() {
		return false
	}
	if !i.loadBlock() {
		return false
	}
	return i.data.Last()
}

// Next implements internalIterator.Next, as documented in the pebble
// package.
func (i *Iterator) Next() bool {
	if i.err != nil {
		return false
	}
	if i.data.Next() {
		return true
	}
	for {
		if i.data.err != nil {
			i.err = i.data.err
			break
		}
		if !i.index.Next() {
			break
		}
		if i.loadBlock() {
			return i.data.First()
		}
	}
	return false
}

// Prev implements internalIterator.Prev, as documented in the pebble
// package.
func (i *Iterator) Prev() bool {
	if i.err != nil {
		return false
	}
	if i.data.Prev() {
		return true
	}
	for {
		if i.data.err != nil {
			i.err = i.data.err
			break
		}
		if !i.index.Prev() {
			break
		}
		if i.loadBlock() {
			return i.data.Last()
		}
	}
	return false
}

// Key implements internalIterator.Key, as documented in the pebble package.
func (i *Iterator) Key() db.InternalKey {
	return i.data.Key()
}

// Value implements internalIterator.Value, as documented in the pebble
// package.
func (i *Iterator) Value() []byte {
	return i.data.Value()
}

// Valid implements internalIterator.Valid, as documented in the pebble
// package.
func (i *Iterator) Valid() bool {
	return i.data.Valid()
}

// Error implements internalIterator.Error, as documented in the pebble
// package.
func (i *Iterator) Error() error {
	if err := i.data.Error(); err != nil {
		return err
	}
	return i.err
}

// SetCloseHook sets a function that will be called when the iterator is
// closed.
func (i *Iterator) SetCloseHook(fn func() error) {
	i.closeHook = fn
}

// Close implements internalIterator.Close, as documented in the pebble
// package.
func (i *Iterator) Close() error {
	if i.closeHook != nil {
		if err := i.closeHook(); err != nil {
			return err
		}
	}
	if err := i.data.Close(); err != nil {
		return err
	}
	return i.err
}

type weakCachedBlock struct {
	bh     blockHandle
	mu     sync.RWMutex
	handle cache.WeakHandle
}

// Reader is a table reader.
type Reader struct {
	file        storage.File
	fileNum     uint64
	err         error
	index       weakCachedBlock
	filter      weakCachedBlock
	rangeDel    weakCachedBlock
	rangeDelV2  bool
	opts        *db.Options
	cache       *cache.Cache
	compare     db.Compare
	split       db.Split
	tableFilter *tableFilterReader
	Properties  Properties
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
func (r *Reader) get(key []byte, o *db.IterOptions) (value []byte, err error) {
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
			return nil, db.ErrNotFound
		}
	}

	i := &Iterator{}
	if err := i.init(r); err == nil {
		i.index.SeekGE(key)
		i.seekBlock(key)
	}

	if !i.Valid() || r.compare(key, i.Key().UserKey) != 0 {
		err := i.Close()
		if err == nil {
			err = db.ErrNotFound
		}
		return nil, err
	}
	return i.Value(), i.Close()
}

// NewIter returns an internal iterator for the contents of the table.
func (r *Reader) NewIter(o *db.IterOptions) *Iterator {
	// NB: pebble.tableCache wraps the returned iterator with one which performs
	// reference counting on the Reader, preventing the Reader from being closed
	// until the final iterator closes.
	if r.err != nil {
		return &Iterator{err: r.err}
	}
	i := &Iterator{}
	_ = i.init(r)
	return i
}

// NewRangeDelIter returns an internal iterator for the contents of the
// range-del block for the table. Returns nil if the table does not contain any
// range deletions.
func (r *Reader) NewRangeDelIter(o *db.IterOptions) *blockIter {
	if r.rangeDel.bh.length == 0 {
		return nil
	}
	b, err := r.readRangeDel()
	if err != nil {
		// TODO(peter): propagate the error
		panic(err)
	}
	i := &blockIter{}
	if err := i.init(r.compare, b, r.Properties.GlobalSeqNum); err != nil {
		// TODO(peter): propagate the error
		panic(err)
	}
	return i
}

func (r *Reader) readIndex() (block, error) {
	return r.readWeakCachedBlock(&r.index)
}

func (r *Reader) readFilter() (block, error) {
	return r.readWeakCachedBlock(&r.filter)
}

func (r *Reader) readRangeDel() (block, error) {
	// Fast-path for retrieving the block from a weak cache handle.
	r.rangeDel.mu.RLock()
	var b []byte
	if r.rangeDel.handle != nil {
		b = r.rangeDel.handle.Get()
	}
	r.rangeDel.mu.RUnlock()
	if b != nil {
		return b, nil
	}

	// Slow-path: read the index block from disk. This checks the cache again,
	// but that is ok because somebody else might have inserted it for us.
	b, h, err := r.readBlock(r.rangeDel.bh)
	if err == nil && h != nil {
		if !r.rangeDelV2 {
			// TODO(peter): if we have a v1 range-del block, convert it on the fly
			// and cache the converted version. We just need to create a
			// rangedel.Fragmenter and loop over the v1 block and add all of the
			// contents. Note that the contents of the v1 block may not be sorted, so
			// we'll have to sort them first. We also need to truncate the v1
			// tombstones to the sstable boundaries.
		}

		r.rangeDel.mu.Lock()
		r.rangeDel.handle = h
		r.rangeDel.mu.Unlock()
	}
	return b, err
}

func (r *Reader) readWeakCachedBlock(w *weakCachedBlock) (block, error) {
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
	b, h, err := r.readBlock(w.bh)
	if err == nil && h != nil {
		w.mu.Lock()
		w.handle = h
		w.mu.Unlock()
	}
	return b, err
}

// readBlock reads and decompresses a block from disk into memory.
func (r *Reader) readBlock(bh blockHandle) (block, cache.WeakHandle, error) {
	if b := r.cache.Get(r.fileNum, bh.offset); b != nil {
		return b, nil, nil
	}

	b := make([]byte, bh.length+blockTrailerLen)
	if _, err := r.file.ReadAt(b, int64(bh.offset)); err != nil {
		return nil, nil, err
	}
	checksum0 := binary.LittleEndian.Uint32(b[bh.length+1:])
	checksum1 := crc.New(b[:bh.length+1]).Value()
	if checksum0 != checksum1 {
		return nil, nil, errors.New("pebble/table: invalid table (checksum mismatch)")
	}
	switch b[bh.length] {
	case noCompressionBlockType:
		b = b[:bh.length]
		h := r.cache.Set(r.fileNum, bh.offset, b)
		return b, h, nil
	case snappyCompressionBlockType:
		b, err := snappy.Decode(nil, b[:bh.length])
		if err != nil {
			return nil, nil, err
		}
		h := r.cache.Set(r.fileNum, bh.offset, b)
		return b, h, nil
	}
	return nil, nil, fmt.Errorf("pebble/table: unknown block compression: %d", b[bh.length])
}

func (r *Reader) readMetaindex(metaindexBH blockHandle, o *db.Options) error {
	b, _, err := r.readBlock(metaindexBH)
	if err != nil {
		return err
	}
	i, err := newRawBlockIter(bytes.Compare, b)
	if err != nil {
		return err
	}

	meta := map[string]blockHandle{}
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
		b, _, err = r.readBlock(bh)
		if err != nil {
			return err
		}
		if err := r.Properties.load(b, bh.offset); err != nil {
			return err
		}
	}

	if bh, ok := meta[metaRangeDelV2Name]; ok {
		r.rangeDel.bh = bh
		r.rangeDelV2 = true
	} else if bh, ok := meta[metaRangeDelName]; ok {
		r.rangeDel.bh = bh
	}

	for level := range r.opts.Levels {
		fp := r.opts.Levels[level].FilterPolicy
		if fp == nil {
			continue
		}
		types := []struct {
			ftype  db.FilterType
			prefix string
		}{
			{db.TableFilter, "fullfilter."},
		}
		var done bool
		for _, t := range types {
			if bh, ok := meta[t.prefix+fp.Name()]; ok {
				r.filter.bh = bh

				switch t.ftype {
				case db.TableFilter:
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

// NewReader returns a new table reader for the file. Closing the reader will
// close the file.
func NewReader(f storage.File, fileNum uint64, o *db.Options) *Reader {
	o = o.EnsureDefaults()
	r := &Reader{
		file:    f,
		fileNum: fileNum,
		opts:    o,
		cache:   o.Cache,
		compare: o.Comparer.Compare,
		split:   o.Comparer.Split,
	}
	if f == nil {
		r.err = errors.New("pebble/table: nil file")
		return r
	}
	footer, err := readFooter(f)
	if err != nil {
		r.err = err
		return r
	}
	// Read the metaindex.
	if err := r.readMetaindex(footer.metaindexBH, o); err != nil {
		r.err = err
		return r
	}
	r.index.bh = footer.indexBH

	// index, r.err = r.readIndex()
	// iter, _ := newBlockIter(r.compare, index)
	// for valid := iter.First(); valid; valid = iter.Next() {
	// 	fmt.Printf("%s#%d\n", iter.Key().UserKey, iter.Key().SeqNum())
	// }
	return r
}
