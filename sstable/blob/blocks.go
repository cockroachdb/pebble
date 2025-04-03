// Copyright 2025 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package blob

import (
	"unsafe"

	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/pebble/internal/base"
	"github.com/cockroachdb/pebble/internal/binfmt"
	"github.com/cockroachdb/pebble/internal/treeprinter"
	"github.com/cockroachdb/pebble/sstable/block"
	"github.com/cockroachdb/pebble/sstable/colblk"
)

const indexBlockColumnCount = 1

// indexBlockEncoder encodes a blob index block.
//
// A blob index block is a columnar block containing a single column: an array
// of uints encoding the file offset at which each block begins. The last entry
// in the array points after the last block.
type indexBlockEncoder struct {
	offsets          colblk.UintBuilder
	previousBlockLen uint64
	rows             int
	enc              colblk.BlockEncoder
}

// Init initializes the index block encoder.
func (e *indexBlockEncoder) Init() {
	e.offsets.Init()
	e.rows = 0
}

// Reset resets the index block encoder to its initial state, retaining buffers.
func (e *indexBlockEncoder) Reset() {
	e.offsets.Reset()
	e.rows = 0
	e.enc.Reset()
}

// AddBlockHandle adds a block handle to the index block.
func (e *indexBlockEncoder) AddBlockHandle(h block.Handle) {
	if e.rows > 0 {
		prevOffset := e.offsets.Get(e.rows - 1)
		if expected := prevOffset + e.previousBlockLen + block.TrailerLen; expected != h.Offset {
			panic(errors.AssertionFailedf("block handle %s doesn't have expected offset of %d", h, expected))
		}
	}
	e.offsets.Set(e.rows, h.Offset)
	e.rows++
	e.previousBlockLen = h.Length
}

func (e *indexBlockEncoder) size() int {
	off := colblk.HeaderSize(indexBlockColumnCount, 0 /* custom header size */)
	off = e.offsets.Size(e.rows, off)
	off++
	return int(off)
}

// Finish serializes the pending index block.
func (e *indexBlockEncoder) Finish() []byte {
	// Add a final offset that points after the last block. This ensures that at
	// read time, we don't need to special case the last block.
	e.offsets.Set(e.rows, e.previousBlockLen+e.offsets.Get(e.rows-1)+block.TrailerLen)
	e.rows++

	e.enc.Init(e.size(), colblk.Header{
		Version: colblk.Version1,
		Columns: indexBlockColumnCount,
		Rows:    uint32(e.rows),
	}, 0 /* custom header size */)
	e.enc.Encode(e.rows, &e.offsets)
	return e.enc.Finish()
}

// An indexBlockDecoder reads columnar index blocks.
type indexBlockDecoder struct {
	offsets colblk.UnsafeUints
	bd      colblk.BlockDecoder
}

// Init initializes the index block decoder with the given serialized index
// block.
func (r *indexBlockDecoder) Init(data []byte) {
	r.bd.Init(data, 0 /* custom header size */)
	r.offsets = r.bd.Uints(0 /* column index */)
}

// BlockHandle returns the block handle for the given block number.
func (r *indexBlockDecoder) BlockHandle(blockNum uint32) block.Handle {
	if blockNum >= uint32(r.bd.Rows())-1 {
		panic(errors.AssertionFailedf("block number %d is out of range [0, %d)", blockNum, r.bd.Rows()-1))
	}
	// TODO(jackson): Add an At2 method to the UnsafeUints type too.
	offset := r.offsets.At(int(blockNum))
	offset2 := r.offsets.At(int(blockNum) + 1)
	return block.Handle{
		Offset: offset,
		Length: offset2 - offset - block.TrailerLen,
	}
}

// DebugString prints a human-readable explanation of the keyspan block's binary
// representation.
func (r *indexBlockDecoder) DebugString() string {
	f := binfmt.New(r.bd.Data()).LineWidth(20)
	tp := treeprinter.New()
	r.Describe(f, tp.Child("index-block-decoder"))
	return tp.String()
}

// Describe describes the binary format of the index block, assuming f.Offset()
// is positioned at the beginning of the same index block described by r.
func (r *indexBlockDecoder) Describe(f *binfmt.Formatter, tp treeprinter.Node) {
	// Set the relative offset. When loaded into memory, the beginning of blocks
	// are aligned. Padding that ensures alignment is done relative to the
	// current offset. Setting the relative offset ensures that if we're
	// describing this block within a larger structure (eg, f.Offset()>0), we
	// compute padding appropriately assuming the current byte f.Offset() is
	// aligned.
	f.SetAnchorOffset()

	n := tp.Child("index block header")
	r.bd.HeaderToBinFormatter(f, n)
	r.bd.ColumnToBinFormatter(f, n, 0 /* column index */, r.bd.Rows())
	f.HexBytesln(1, "block padding byte")
	f.ToTreePrinter(n)
}

// Assert that an IndexBlockDecoder can fit inside block.Metadata.
const _ uint = block.MetadataSize - uint(unsafe.Sizeof(indexBlockDecoder{}))

// initIndexBlockMetadata initializes the index block metadata.
func initIndexBlockMetadata(md *block.Metadata, data []byte) (err error) {
	if uintptr(unsafe.Pointer(md))%8 != 0 {
		return errors.AssertionFailedf("metadata is not 8-byte aligned")
	}
	d := (*indexBlockDecoder)(unsafe.Pointer(md))
	// Initialization can panic; convert panics to corruption errors (so higher
	// layers can add file number and offset information).
	defer func() {
		if r := recover(); r != nil {
			err = base.CorruptionErrorf("error initializing index block metadata: %v", r)
		}
	}()
	d.Init(data)
	return nil
}
