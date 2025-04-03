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
	offsets colblk.UintBuilder
	// countBlocks is the number of blocks in the index block. The number of
	// rows in the offsets column is countBlocks+1. The last offset points to
	// the first byte after the last block so that a reader can compute the
	// length of the last block.
	countBlocks int
	enc         colblk.BlockEncoder
}

// Init initializes the index block encoder.
func (e *indexBlockEncoder) Init() {
	e.offsets.Init()
	e.countBlocks = 0
}

// Reset resets the index block encoder to its initial state, retaining buffers.
func (e *indexBlockEncoder) Reset() {
	e.offsets.Reset()
	e.countBlocks = 0
	e.enc.Reset()
}

// AddBlockHandle adds a block handle to the index block.
func (e *indexBlockEncoder) AddBlockHandle(h block.Handle) {
	// Every call to AddBlockHandle adds its end offset (i.e, the next block's
	// start offset) to the offsets column.
	//
	// The first call to AddBlockHandle must also add the start offset the first
	// block. We also verify that for subsequent blocks, the start offset
	// matches the offset encoded by the previous call to AddBlockHandle.
	if e.countBlocks == 0 {
		e.offsets.Set(0, h.Offset)
	} else if expected := e.offsets.Get(e.countBlocks); expected != h.Offset {
		panic(errors.AssertionFailedf("block handle %s doesn't have expected offset of %d", h, expected))
	}

	// Increment the number blocks, and set the endOffset.
	e.countBlocks++
	endOffset := h.Offset + h.Length + block.TrailerLen
	e.offsets.Set(e.countBlocks, endOffset)
}

func (e *indexBlockEncoder) size() int {
	off := colblk.HeaderSize(indexBlockColumnCount, 0 /* custom header size */)
	if e.countBlocks > 0 {
		off = e.offsets.Size(e.countBlocks+1, off)
	}
	off++
	return int(off)
}

// Finish serializes the pending index block.
func (e *indexBlockEncoder) Finish() []byte {
	e.enc.Init(e.size(), colblk.Header{
		Version: colblk.Version1,
		Columns: indexBlockColumnCount,
		Rows:    uint32(e.countBlocks),
	}, 0 /* custom header size */)
	e.enc.Encode(e.countBlocks+1, &e.offsets)
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
	// Decode the offsets column. We pass rows+1 because an index block encoding
	// n block handles encodes n+1 offsets.
	r.offsets = colblk.DecodeColumn(&r.bd, 0, r.bd.Rows()+1,
		colblk.DataTypeUint, colblk.DecodeUnsafeUints)
}

// BlockHandle returns the block handle for the given block number.
func (r *indexBlockDecoder) BlockHandle(blockNum uint32) block.Handle {
	if blockNum >= uint32(r.bd.Rows()) {
		panic(errors.AssertionFailedf("block number %d is out of range [0, %d)", blockNum, r.bd.Rows()))
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
	r.bd.ColumnToBinFormatter(f, n, 0 /* column index */, r.bd.Rows()+1)
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
