// Copyright 2011 The LevelDB-Go Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package table

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"io"

	"github.com/golang/snappy"
	"github.com/petermattis/pebble/cache"
	"github.com/petermattis/pebble/crc"
	"github.com/petermattis/pebble/db"
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

// tableIter is an iterator over an entire table of data. It is a two-level
// iterator: to seek for a given key, it first looks in the index for the
// block that contains that key, and then looks inside that block.
type tableIter struct {
	reader *Reader
	index  blockIter
	data   blockIter
	err    error
}

// tableIter implements the db.InternalIterator interface.
var _ db.InternalIterator = (*tableIter)(nil)

// laodBlock loads the block at the current index position and leaves i.data
// unpositioned. If unsuccessful, it sets i.err to any error encountered, which
// may be nil if we have simply exhausted the entire table.
func (i *tableIter) loadBlock() bool {
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
	block, err := i.reader.readBlock(h)
	if err != nil {
		i.err = err
		return false
	}
	i.err = i.data.init(i.reader.compare, i.reader.coder, block)
	if i.err != nil {
		return false
	}
	return true
}

// seekBlock loads the block at the current index position and positions i.data
// at the first key in that block which is >= the given key. If unsuccessful,
// it sets i.err to any error encountered, which may be nil if we have simply
// exhausted the entire table.
//
// If f is non-nil, the caller is presumably looking for one specific key, as
// opposed to iterating over a range of keys (where the minimum of that range
// isn't necessarily in the table). In that case, i.err will be set to
// db.ErrNotFound if f does not contain the key.
func (i *tableIter) seekBlock(key *db.InternalKey, f *filterReader) bool {
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
	if f != nil && !f.mayContain(h.offset, key) {
		i.err = db.ErrNotFound
		return false
	}
	block, err := i.reader.readBlock(h)
	if err != nil {
		i.err = err
		return false
	}
	i.err = i.data.init(i.reader.compare, i.reader.coder, block)
	if i.err != nil {
		return false
	}
	// Look for the key inside that block.
	i.data.SeekGE(key)
	return true
}

// SeekGE implements Iterator.SeekGE, as documented in the pebble/db package.
func (i *tableIter) SeekGE(key *db.InternalKey) {
	i.index.SeekGE(key)
	if i.loadBlock() {
		i.data.SeekGE(key)
	}
}

// SeekLE implements Iterator.SeekLE, as documented in the pebble/db package.
func (i *tableIter) SeekLE(key *db.InternalKey) {
	panic("pebble/table: SeekLE unimplemented")
}

// First implements Iterator.First, as documented in the pebble/db package.
func (i *tableIter) First() {
	i.SeekGE(nil)
}

// Last implements Iterator.Last, as documented in the pebble/db package.
func (i *tableIter) Last() {
	i.index.Last()
	if i.loadBlock() {
		i.data.Last()
	}
}

// Next implements Iterator.Next, as documented in the pebble/db package.
func (i *tableIter) Next() bool {
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
			i.data.First()
			return true
		}
	}
	return false
}

// Prev implements Iterator.Prev, as documented in the pebble/db package.
func (i *tableIter) Prev() bool {
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
			i.data.Last()
			return true
		}
	}
	return false
}

// Key implements Iterator.Key, as documented in the pebble/db package.
func (i *tableIter) Key() *db.InternalKey {
	return i.data.Key()
}

// Value implements Iterator.Value, as documented in the pebble/db package.
func (i *tableIter) Value() []byte {
	return i.data.Value()
}

// Valid implements Iterator.Valid, as documented in the pebble/db package.
func (i *tableIter) Valid() bool {
	return i.data.Valid()
}

// Error implements Iterator.Error, as documented in the pebble/db package.
func (i *tableIter) Error() error {
	if err := i.data.Error(); err != nil {
		return err
	}
	return i.err
}

// Close implements Iterator.Close, as documented in the pebble/db package.
func (i *tableIter) Close() error {
	if err := i.data.Close(); err != nil {
		return err
	}
	return i.err
}

type filterReader struct {
	data    []byte
	offsets []byte // len(offsets) must be a multiple of 4.
	policy  db.FilterPolicy
	shift   uint32
}

func (f *filterReader) valid() bool {
	return f.data != nil
}

func (f *filterReader) init(data []byte, policy db.FilterPolicy) (ok bool) {
	if len(data) < 5 {
		return false
	}
	lastOffset := binary.LittleEndian.Uint32(data[len(data)-5:])
	if uint64(lastOffset) > uint64(len(data)-5) {
		return false
	}
	data, offsets, shift := data[:lastOffset], data[lastOffset:len(data)-1], uint32(data[len(data)-1])
	if len(offsets)&3 != 0 {
		return false
	}
	f.data = data
	f.offsets = offsets
	f.policy = policy
	f.shift = shift
	return true
}

func (f *filterReader) mayContain(blockOffset uint64, key *db.InternalKey) bool {
	index := blockOffset >> f.shift
	if index >= uint64(len(f.offsets)/4-1) {
		return true
	}
	i := binary.LittleEndian.Uint32(f.offsets[4*index+0:])
	j := binary.LittleEndian.Uint32(f.offsets[4*index+4:])
	if i >= j || uint64(j) > uint64(len(f.data)) {
		return true
	}
	return f.policy.MayContain(f.data[i:j], key.UserKey)
}

// Reader is a table reader. It implements the DB interface, as documented
// in the pebble/db package.
type Reader struct {
	file            storage.File
	fileNum         uint64
	err             error
	index           block
	cache           *cache.BlockCache
	compare         db.Compare
	coder           coder
	filter          filterReader
	verifyChecksums bool
}

// Reader implements the db.InternalReader interface.
var _ db.InternalReader = (*Reader)(nil)

// Close implements DB.Close, as documented in the pebble/db package.
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

// Get implements DB.Get, as documented in the pebble/db package.
func (r *Reader) Get(key *db.InternalKey, o *db.ReadOptions) (value []byte, err error) {
	if r.err != nil {
		return nil, r.err
	}
	f := (*filterReader)(nil)
	if r.filter.valid() {
		f = &r.filter
	}

	i := &tableIter{
		reader: r,
	}
	i.err = i.index.init(r.compare, r.coder, r.index)
	if i.err == nil {
		i.index.SeekGE(key)
		i.seekBlock(key, f)
	}

	if !i.Valid() || db.InternalCompare(r.compare, *key, *i.Key()) != 0 {
		err := i.Close()
		if err == nil {
			err = db.ErrNotFound
		}
		return nil, err
	}
	return i.Value(), i.Close()
}

// NewIter implements DB.NewIter, as documented in the pebble/db package.
func (r *Reader) NewIter(o *db.ReadOptions) db.InternalIterator {
	// TODO(peter): Don't allow the Reader to be closed while a tableIter exists
	// on it.
	if r.err != nil {
		return &tableIter{err: r.err}
	}
	i := &tableIter{
		reader: r,
	}
	i.err = i.index.init(r.compare, r.coder, r.index)
	return i
}

// readBlock reads and decompresses a block from disk into memory.
func (r *Reader) readBlock(bh blockHandle) (block, error) {
	if b := r.cache.Get(r.fileNum, bh.offset); b != nil {
		return b, nil
	}

	b := make([]byte, bh.length+blockTrailerLen)
	if _, err := r.file.ReadAt(b, int64(bh.offset)); err != nil {
		return nil, err
	}
	if r.verifyChecksums {
		checksum0 := binary.LittleEndian.Uint32(b[bh.length+1:])
		checksum1 := crc.New(b[:bh.length+1]).Value()
		if checksum0 != checksum1 {
			return nil, errors.New("pebble/table: invalid table (checksum mismatch)")
		}
	}
	switch b[bh.length] {
	case noCompressionBlockType:
		b = b[:bh.length]
		r.cache.Insert(r.fileNum, bh.offset, b)
		return b, nil
	case snappyCompressionBlockType:
		b, err := snappy.Decode(nil, b[:bh.length])
		if err != nil {
			return nil, err
		}
		r.cache.Insert(r.fileNum, bh.offset, b)
		return b, nil
	}
	return nil, fmt.Errorf("pebble/table: unknown block compression: %d", b[bh.length])
}

func (r *Reader) readMetaindex(metaindexBH blockHandle, o *db.Options) error {
	fp := o.GetFilterPolicy()
	if fp == nil {
		// The only metaindex entry we care about is the filter. If o doesn't
		// specify a filter policy, we can ignore the entire metaindex block.
		//
		// TODO: also return early if metaindexBH.length indicates an empty
		// block.
		return nil
	}

	b, err := r.readBlock(metaindexBH)
	if err != nil {
		return err
	}
	i, err := newBlockIter(bytes.Compare, raw{}, b)
	if err != nil {
		return err
	}
	i.SeekGE(nil)
	filterName := "filter." + fp.Name()
	filterBH := blockHandle{}
	for ; i.Valid(); i.Next() {
		if filterName != string(i.Key().UserKey) {
			continue
		}
		var n int
		filterBH, n = decodeBlockHandle(i.Value())
		if n == 0 {
			return errors.New("pebble/table: invalid table (bad filter block handle)")
		}
		break
	}
	if err := i.Close(); err != nil {
		return err
	}

	if filterBH != (blockHandle{}) {
		b, err = r.readBlock(filterBH)
		if err != nil {
			return err
		}
		if !r.filter.init(b, fp) {
			return errors.New("pebble/table: invalid table (bad filter block)")
		}
	}
	return nil
}

func newReader(f storage.File, fileNum uint64, o *db.Options, coder coder) *Reader {
	r := &Reader{
		file:            f,
		fileNum:         fileNum,
		cache:           o.GetCache(),
		compare:         o.GetComparer().Compare,
		coder:           coder,
		verifyChecksums: o.GetVerifyChecksums(),
	}
	if f == nil {
		r.err = errors.New("pebble/table: nil file")
		return r
	}
	stat, err := f.Stat()
	if err != nil {
		r.err = fmt.Errorf("pebble/table: invalid table (could not stat file): %v", err)
		return r
	}
	var footer [footerLen]byte
	if stat.Size() < int64(len(footer)) {
		r.err = errors.New("pebble/table: invalid table (file size is too small)")
		return r
	}
	_, err = f.ReadAt(footer[:], stat.Size()-int64(len(footer)))
	if err != nil && err != io.EOF {
		r.err = fmt.Errorf("pebble/table: invalid table (could not read footer): %v", err)
		return r
	}
	if string(footer[footerLen-len(magic):footerLen]) != magic {
		r.err = errors.New("pebble/table: invalid table (bad magic number)")
		return r
	}

	// Read the metaindex.
	metaindexBH, n := decodeBlockHandle(footer[:])
	if n == 0 {
		r.err = errors.New("pebble/table: invalid table (bad metaindex block handle)")
		return r
	}
	if err := r.readMetaindex(metaindexBH, o); err != nil {
		r.err = err
		return r
	}

	// Read the index into memory.
	//
	// TODO(peter): Allow the index block to be placed in the block cache.
	indexBH, n := decodeBlockHandle(footer[n:])
	if n == 0 {
		r.err = errors.New("pebble/table: invalid table (bad index block handle)")
		return r
	}
	r.index, r.err = r.readBlock(indexBH)
	return r
}

// NewReader returns a new table reader for the file. Closing the reader will
// close the file.
func NewReader(f storage.File, fileNum uint64, o *db.Options) *Reader {
	return newReader(f, fileNum, o, internalKeyCoder{})
}
