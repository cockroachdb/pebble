// Copyright 2025 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package blob

import (
	"encoding/binary"
	"unsafe"

	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/pebble/internal/base"
	"github.com/cockroachdb/pebble/internal/binfmt"
	"github.com/cockroachdb/pebble/internal/invariants"
	"github.com/cockroachdb/pebble/internal/treeprinter"
	"github.com/cockroachdb/pebble/sstable/block"
	"github.com/cockroachdb/pebble/sstable/colblk"
)

const (
	indexBlockCustomHeaderSize       = 4
	indexBlockColumnCount            = 2
	indexBlockColumnVirtualBlocksIdx = 0
	indexBlockColumnOffsetsIdx       = 1

	virtualBlockIndexMask = 0x00000000ffffffff
)

// indexBlockEncoder encodes a blob index block.
//
// A blob index block tells a reader where in the file each physical blob-value
// block is located. Its format is a columnar block with two columns. The
// offsets column is an array of uints encoding the file offset at which each
// block begins. The last entry in the array points after the last block. The
// virtualBlocks column is an array of uints that is only non-empty in the case
// of rewritten blob files. It encodes a mapping from the original blob file's
// blockIDs to a tuple of the index of the physical block containing the block's
// data and offset that should be added to the value ID to get the index of the
// block's values within the physical block.
type indexBlockEncoder struct {
	// countBlocks is the number of physical blocks in the blob file. The number
	// of rows in the offsets column is countBlocks+1. The last offset points to
	// the first byte after the last block so that a reader can compute the
	// length of the last block.
	countBlocks        int
	countVirtualBlocks int
	virtualBlocks      colblk.UintBuilder
	// offsets contains the offset of the start of each block. There is +1 more
	// offset than there are blocks, with the last offset pointing to the first
	// byte after the last block. Block lengths are inferred from the difference
	// between consecutive offsets.
	offsets colblk.UintBuilder
	enc     colblk.BlockEncoder
}

// Init initializes the index block encoder.
func (e *indexBlockEncoder) Init() {
	e.countBlocks = 0
	e.countVirtualBlocks = 0
	e.offsets.Init()
	e.virtualBlocks.Init()
}

// Reset resets the index block encoder to its initial state, retaining buffers.
func (e *indexBlockEncoder) Reset() {
	e.countBlocks = 0
	e.countVirtualBlocks = 0
	e.offsets.Reset()
	e.virtualBlocks.Reset()
	e.enc.Reset()
}

// AddBlockHandle adds a handle to a blob-value block to the index block.
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

// AddVirtualBlockMapping adds a mapping from a virtual block ID to a physical
// block ID and a value ID offset. It's used when rewriting a blob file.
func (e *indexBlockEncoder) AddVirtualBlockMapping(
	virtualBlockID BlockID, physicalBlockIndex int, valueIDOffset BlockValueID,
) {
	// Require that virtual blocks are added in order.
	if virtualBlockID < BlockID(e.countVirtualBlocks) {
		panic(errors.AssertionFailedf("virtual block ID %d is out of order; expected %d", virtualBlockID, e.countVirtualBlocks))
	}
	// If there's a gap within the virtual block IDs, we fill in the gap with
	// entries that clarify these blocks are empty.
	for id := BlockID(e.countVirtualBlocks); id < virtualBlockID; id++ {
		e.virtualBlocks.Set(int(id), virtualBlockIndexMask)
		e.countVirtualBlocks++
	}
	e.virtualBlocks.Set(int(virtualBlockID), uint64(physicalBlockIndex)|(uint64(valueIDOffset)<<32))
	e.countVirtualBlocks++
}

func (e *indexBlockEncoder) size() int {
	off := colblk.HeaderSize(indexBlockColumnCount, indexBlockCustomHeaderSize)
	if e.countVirtualBlocks > 0 {
		off = e.virtualBlocks.Size(e.countVirtualBlocks, off)
	}
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
	}, indexBlockCustomHeaderSize)
	e.enc.Encode(e.countVirtualBlocks, &e.virtualBlocks)
	e.enc.Encode(e.countBlocks+1, &e.offsets)
	data := e.enc.Finish()
	binary.LittleEndian.PutUint32(data, uint32(e.countVirtualBlocks))
	return data
}

// An indexBlockDecoder decodes blob file index blocks. See the doc comment for
// details on the encoding.
type indexBlockDecoder struct {
	// virtualBlockCount is zero for blob files created during ordinary
	// compactions. When a blob file is rewritten, virtualBlockCount is nonzero
	// and holds the count of blocks in the original blob file. The
	// virtualBlocks column contains virtualBlockCount rows.
	virtualBlockCount int
	// virtualBlocks is a column of uints remapping a BlockID to a tuple of
	// (physicalBlockIndex, valueIDOffset). The valueIDOffset is encoded in the
	// most-significant 32 bits of each uint value.
	virtualBlocks colblk.UnsafeUints
	// offsets contains the offset of the start of each block. There is +1 more
	// offset than there are blocks, with the last offset pointing to the first
	// byte after the last block. Block lengths are inferred from the difference
	// between consecutive offsets.
	offsets colblk.UnsafeUints
	bd      colblk.BlockDecoder
}

// Init initializes the index block decoder with the given serialized index
// block.
func (d *indexBlockDecoder) Init(data []byte) {
	d.virtualBlockCount = int(binary.LittleEndian.Uint32(data))
	d.bd.Init(data, indexBlockCustomHeaderSize)
	d.virtualBlocks = colblk.DecodeColumn(&d.bd, indexBlockColumnVirtualBlocksIdx,
		d.virtualBlockCount, colblk.DataTypeUint, colblk.DecodeUnsafeUints)
	// Decode the offsets column. We pass rows+1 because an index block encoding
	// n block handles encodes n+1 offsets.
	d.offsets = colblk.DecodeColumn(&d.bd, indexBlockColumnOffsetsIdx,
		d.bd.Rows()+1, colblk.DataTypeUint, colblk.DecodeUnsafeUints)
}

// BlockHandle returns the block handle for the given block index in the
// range [0, bd.Rows()).
func (d *indexBlockDecoder) BlockHandle(blockIndex int) block.Handle {
	invariants.CheckBounds(blockIndex, d.bd.Rows())
	// TODO(jackson): Add an At2 method to the UnsafeUints type too.
	offset := d.offsets.At(blockIndex)
	offset2 := d.offsets.At(blockIndex + 1)
	return block.Handle{
		Offset: offset,
		Length: offset2 - offset - block.TrailerLen,
	}
}

// RemapVirtualBlockID remaps a virtual block ID to a physical block index and a
// value ID offset. RemapVirtualBlockID should only be called on index blocks
// with a non-empty virtual blocks column (i.e., index blocks for rewritten blob
// files).
//
// REQUIRES: d.virtualBlockCount > 0
func (d *indexBlockDecoder) RemapVirtualBlockID(
	blockID BlockID,
) (blockIndex int, valueIDOffset BlockValueID) {
	invariants.CheckBounds(int(blockID), d.virtualBlockCount)
	v := d.virtualBlocks.At(int(blockID))
	blockIndex = int(v & virtualBlockIndexMask)
	valueIDOffset = BlockValueID(v >> 32)
	return blockIndex, valueIDOffset
}

// BlockCount returns the number of physical blocks encoded in the index block.
func (d *indexBlockDecoder) BlockCount() int {
	return int(d.bd.Rows())
}

// DebugString prints a human-readable explanation of the index block's binary
// representation.
func (d *indexBlockDecoder) DebugString() string {
	f := binfmt.New(d.bd.Data()).LineWidth(20)
	tp := treeprinter.New()
	d.Describe(f, tp.Child("index-block-decoder"))
	return tp.String()
}

// Describe describes the binary format of the index block, assuming f.Offset()
// is positioned at the beginning of the same index block described by d.
func (d *indexBlockDecoder) Describe(f *binfmt.Formatter, tp treeprinter.Node) {
	// Set the relative offset. When loaded into memory, the beginning of blocks
	// are aligned. Padding that ensures alignment is done relative to the
	// current offset. Setting the relative offset ensures that if we're
	// describing this block within a larger structure (eg, f.Offset()>0), we
	// compute padding appropriately assuming the current byte f.Offset() is
	// aligned.
	f.SetAnchorOffset()

	n := tp.Child("index block header")
	f.HexBytesln(4, "virtual block count: %d", d.virtualBlockCount)
	d.bd.HeaderToBinFormatter(f, n)
	d.bd.ColumnToBinFormatter(f, n, indexBlockColumnVirtualBlocksIdx, d.virtualBlockCount)
	d.bd.ColumnToBinFormatter(f, n, indexBlockColumnOffsetsIdx, d.bd.Rows()+1)
	f.HexBytesln(1, "block padding byte")
	f.ToTreePrinter(n)
}

// Assert that an IndexBlockDecoder can fit inside block.Metadata.
const _ uint = block.MetadataSize - uint(unsafe.Sizeof(indexBlockDecoder{}))

// initIndexBlockMetadata initializes the index block metadata.
func initIndexBlockMetadata(md *block.Metadata, data []byte) (err error) {
	d := block.CastMetadataZero[indexBlockDecoder](md)
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

const (
	blobValueBlockCustomHeaderSize          = 0
	blobValueBlockColumnCount               = 1
	blobValueBlockAdditionalColumnCount     = 2
	blobValueBlockColumnValuesIdx           = 0
	blobValueBlockColumnTieringSpanIDIdx    = 1
	blobValueBlockColumnTieringAttributeIdx = 2
)

// blobValueBlockEncoder encodes a blob value block.
//
// A blob value block is a columnar block containing a column representing an
// array of bytes encoding values. It may optionally contain two additional
// columns, for tiering.
type blobValueBlockEncoder struct {
	format FileFormat
	values colblk.RawBytesBuilder
	// These two columns are only accessed when the blob file is rewritten. Both
	// will be accessed together, and we expect that in most cases there will be
	// at most one unique spanID in a block.
	tieringSpanIDs    colblk.UintBuilder
	tieringAttributes colblk.UintBuilder
	enc               colblk.BlockEncoder
}

// Init initializes the blob value block encoder.
func (e *blobValueBlockEncoder) Init(format FileFormat) {
	e.format = format
	e.values.Init()
	if e.format >= FileFormatV3 {
		e.tieringSpanIDs.InitWithDefault()
		e.tieringAttributes.InitWithDefault()
	}
}

// Reset resets the blob value block encoder to its initial state, retaining
// buffers.
func (e *blobValueBlockEncoder) Reset() {
	e.values.Reset()
	e.enc.Reset()
	e.tieringSpanIDs.Reset()
	e.tieringAttributes.Reset()
}

// AddValue adds a value to the blob value block.
func (e *blobValueBlockEncoder) AddValue(v []byte, meta base.TieringMeta) {
	rows := e.values.Rows()
	e.values.Put(v)
	if e.format >= FileFormatV3 && meta != (base.TieringMeta{}) {
		e.tieringSpanIDs.Set(rows, meta.SpanID)
		e.tieringAttributes.Set(rows, uint64(meta.Attribute))
	}
}

// Count returns the number of values in the blob value block.
func (e *blobValueBlockEncoder) Count() int {
	return e.values.Rows()
}

func (e *blobValueBlockEncoder) size() int {
	rows := e.values.Rows()
	if rows == 0 {
		return 0
	}
	off := colblk.HeaderSize(e.columnCount(), blobValueBlockCustomHeaderSize)
	off = e.values.Size(rows, off)
	if e.format >= FileFormatV3 {
		off = e.tieringSpanIDs.Size(rows, off)
		off = e.tieringAttributes.Size(rows, off)
	}
	off++ // trailer padding byte
	return int(off)
}

// Finish serializes the pending blob value block.
func (e *blobValueBlockEncoder) Finish() []byte {
	e.enc.Init(e.size(), colblk.Header{
		Version: colblk.Version1,
		Columns: uint16(e.columnCount()),
		Rows:    uint32(e.values.Rows()),
	}, blobValueBlockCustomHeaderSize)
	rows := e.values.Rows()
	e.enc.Encode(rows, &e.values)
	if e.format >= FileFormatV3 {
		e.enc.Encode(rows, &e.tieringSpanIDs)
		e.enc.Encode(rows, &e.tieringAttributes)
	}
	return e.enc.Finish()
}

func (e *blobValueBlockEncoder) columnCount() int {
	count := blobValueBlockColumnCount
	if e.format >= FileFormatV3 {
		count += blobValueBlockAdditionalColumnCount
	}
	return count
}

// A blobValueBlockDecoder reads columnar blob value blocks.
type blobValueBlockDecoder struct {
	format            FileFormat
	values            colblk.RawBytes
	tieringSpanIDs    colblk.UnsafeUints
	tieringAttributes colblk.UnsafeUints
	bd                colblk.BlockDecoder
}

// Init initializes the decoder with the given serialized blob value block.
func (d *blobValueBlockDecoder) Init(format FileFormat, data []byte) {
	d.format = format
	d.bd.Init(data, blobValueBlockCustomHeaderSize)
	d.values = d.bd.RawBytes(blobValueBlockColumnValuesIdx)
	if format >= FileFormatV3 {
		d.tieringSpanIDs = d.bd.Uints(blobValueBlockColumnTieringSpanIDIdx)
		d.tieringAttributes = d.bd.Uints(blobValueBlockColumnTieringAttributeIdx)
	}
}

// DebugString prints a human-readable explanation of the blob value block's
// binary representation.
func (d *blobValueBlockDecoder) DebugString() string {
	f := binfmt.New(d.bd.Data()).LineWidth(20)
	tp := treeprinter.New()
	d.Describe(f, tp.Child("blob-value-block-decoder"))
	return tp.String()
}

// Describe describes the binary format of the blob value block, assuming
// f.Offset() is positioned at the beginning of the same blob value block
// described by d.
func (d *blobValueBlockDecoder) Describe(f *binfmt.Formatter, tp treeprinter.Node) {
	// Set the relative offset. When loaded into memory, the beginning of blocks
	// are aligned. Padding that ensures alignment is done relative to the
	// current offset. Setting the relative offset ensures that if we're
	// describing this block within a larger structure (eg, f.Offset()>0), we
	// compute padding appropriately assuming the current byte f.Offset() is
	// aligned.
	f.SetAnchorOffset()

	n := tp.Child("blob value block header")
	d.bd.HeaderToBinFormatter(f, n)
	d.bd.ColumnToBinFormatter(f, n, blobValueBlockColumnValuesIdx, d.bd.Rows())
	if d.format >= FileFormatV3 {
		d.bd.ColumnToBinFormatter(f, n, blobValueBlockColumnTieringSpanIDIdx, d.bd.Rows())
		d.bd.ColumnToBinFormatter(f, n, blobValueBlockColumnTieringAttributeIdx, d.bd.Rows())
	}
	f.HexBytesln(1, "block padding byte")
	f.ToTreePrinter(n)
}

// Assert that an BlobBlockDecoder can fit inside block.Metadata.
const _ uint = block.MetadataSize - uint(unsafe.Sizeof(blobValueBlockDecoder{}))
