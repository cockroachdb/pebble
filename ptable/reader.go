// Copyright 2018 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package ptable

import (
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"sort"

	"github.com/golang/snappy"
	"github.com/petermattis/pebble/cache"
	"github.com/petermattis/pebble/crc"
	"github.com/petermattis/pebble/db"
	"github.com/petermattis/pebble/storage"
)

// Iter ...
type Iter struct {
	reader *Reader
	cmp    db.Compare
	index  Block
	data   Block
	pos    int32
	err    error
}

// Init ...
func (i *Iter) Init(r *Reader) error {
	i.reader = r
	i.cmp = i.reader.cmp
	i.err = r.err
	if i.err != nil {
		return i.err
	}
	i.index.init(r.index)
	i.pos = -1
	return nil
}

// SeekGE moves the iterator to the first block containing keys greater than or
// equal to the given key.
func (i *Iter) SeekGE(key []byte) {
	keys := i.index.Column(0).Bytes()
	index := sort.Search(int(i.index.rows-1), func(j int) bool {
		return i.cmp(key, keys.At(j)) < 0
	})
	if index > 0 {
		index--
	}
	i.pos = int32(index)
	i.loadBlock()
}

// SeekLT moves the iterator to the last block containing keys less than the
// given key.
func (i *Iter) SeekLT(key []byte) {
	panic("pebble/ptable: SeekLT unimplemented")
}

// First moves the iterator to the first block in the table.
func (i *Iter) First() {
	i.pos = 0
	i.loadBlock()
}

// Last moves the iterator to the last block in the table.
func (i *Iter) Last() {
	// NB: the index block has 1 more row than there are data blocks in the
	// table.
	i.pos = i.index.rows - 2
	i.loadBlock()
}

// Next moves the iterator to the next block in the table.
func (i *Iter) Next() {
	if i.pos+1 >= i.index.rows {
		return
	}
	i.pos++
	i.loadBlock()
}

// Prev moves the iterator to the previous block in the table.
func (i *Iter) Prev() {
	if i.pos < 0 {
		return
	}
	i.pos--
	i.loadBlock()
}

// Valid returns true if the iterator is positioned at a valid block and false
// otherwise.
func (i *Iter) Valid() bool {
	return i.err == nil && i.pos >= 0 && i.pos+1 < i.index.rows
}

// Block returns the block the iterator is currently pointed out.
func (i *Iter) Block() *Block {
	return &i.data
}

func (i *Iter) loadBlock() {
	if !i.Valid() {
		return
	}
	offsets := i.index.Column(1).Int64()
	bh := blockHandle{
		offset: uint64(offsets[i.pos]),
		length: uint64(offsets[i.pos+1]-offsets[i.pos]) - blockTrailerLen,
	}
	b, err := i.reader.readBlock(bh)
	if err != nil {
		i.err = err
		return
	}
	i.data.init(b)
}

// Reader ...
type Reader struct {
	file    storage.File
	fileNum uint64 // TODO(peter): needed for block cache
	err     error
	index   []byte
	cache   *cache.Cache
	cmp     db.Compare
}

// NewReader ...
func NewReader(f storage.File, fileNum uint64, o *db.Options) *Reader {
	o = o.EnsureDefaults()
	r := &Reader{
		file:    f,
		fileNum: fileNum,
		cache:   o.Cache,
		cmp:     o.Comparer.Compare,
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

	// legacy footer format:
	//    metaindex handle (varint64 offset, varint64 size)
	//    index handle     (varint64 offset, varint64 size)
	//    <padding> to make the total size 2 * BlockHandle::kMaxEncodedLength
	//    table_magic_number (8 bytes)
	// new footer format:
	//    checksum type (char, 1 byte)
	//    metaindex handle (varint64 offset, varint64 size)
	//    index handle     (varint64 offset, varint64 size)
	//    <padding> to make the total size 2 * BlockHandle::kMaxEncodedLength + 1
	//    footer version (4 bytes)
	//    table_magic_number (8 bytes)
	footer := make([]byte, footerLen)
	if stat.Size() < int64(len(footer)) {
		r.err = errors.New("pebble/table: invalid table (file size is too small)")
		return r
	}
	_, err = f.ReadAt(footer, stat.Size()-int64(len(footer)))
	if err != nil && err != io.EOF {
		r.err = fmt.Errorf("pebble/table: invalid table (could not read footer): %v", err)
		return r
	}
	if string(footer[magicOffset:footerLen]) != magic {
		r.err = errors.New("pebble/table: invalid table (bad magic number)")
		return r
	}

	version := binary.LittleEndian.Uint32(footer[versionOffset:magicOffset])
	if version != formatVersion {
		r.err = fmt.Errorf("pebble/table: unsupported format version %d", version)
		return r
	}

	if footer[0] != checksumCRC32c {
		r.err = fmt.Errorf("pebble/table: unsupported checksum type %d", footer[0])
		return r
	}
	footer = footer[1:]

	// TODO(peter): Read the metaindex.
	_, n := decodeBlockHandle(footer)
	if n == 0 {
		r.err = errors.New("pebble/table: invalid table (bad metaindex block handle)")
		return r
	}
	footer = footer[n:]

	// Read the index into memory.
	//
	// TODO(peter): Allow the index block to be placed in the block cache.
	indexBH, n := decodeBlockHandle(footer)
	if n == 0 {
		r.err = errors.New("pebble/table: invalid table (bad index block handle)")
		return r
	}
	r.index, r.err = r.readBlock(indexBH)
	return r
}

// Close ...
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

// NewIter ...
func (r *Reader) NewIter() *Iter {
	// TODO(peter): Don't allow the Reader to be closed while a tableIter exists
	// on it.
	if r.err != nil {
		return &Iter{err: r.err}
	}
	i := &Iter{}
	_ = i.Init(r)
	return i
}

// readBlock reads and decompresses a block from disk into memory.
func (r *Reader) readBlock(bh blockHandle) ([]byte, error) {
	if b := r.cache.Get(r.fileNum, bh.offset); b != nil {
		return b, nil
	}

	b := make([]byte, bh.length+blockTrailerLen)
	if _, err := r.file.ReadAt(b, int64(bh.offset)); err != nil {
		return nil, err
	}
	checksum0 := binary.LittleEndian.Uint32(b[bh.length+1:])
	checksum1 := crc.New(b[:bh.length+1]).Value()
	if checksum0 != checksum1 {
		return nil, errors.New("pebble/table: invalid table (checksum mismatch)")
	}
	switch b[bh.length] {
	case noCompressionBlockType:
		b = b[:bh.length]
		r.cache.Set(r.fileNum, bh.offset, b)
		return b, nil
	case snappyCompressionBlockType:
		b, err := snappy.Decode(nil, b[:bh.length])
		if err != nil {
			return nil, err
		}
		r.cache.Set(r.fileNum, bh.offset, b)
		return b, nil
	}
	return nil, fmt.Errorf("pebble/table: unknown block compression: %d", b[bh.length])
}
