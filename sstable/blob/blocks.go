// Copyright 2025 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

// Package blob implements mechanics for encoding and decoding values into blob
// files.
//
// # Blob file format
//
// A blob file consists of a sequence of blob-value blocks containing values,
// followed by an index block describing the location of the blob-value blocks.
// At the tail of the file is a fixed-size footer encoding the exact offset and
// length of the index block.
//
// Semantically, a blob file implements an array of blob values. SSTables that
// reference separated blob values encode a tuple of a (blockID, blockValueID)
// to identify the value within the blob file. The blockID identifies the
// blob-value block that contains the value. The blockValueID identifies the
// value within the block. A reader retrieving a particular value uses the index
// block to identify the offset and length of the blob-value block containing
// the referenced value. It loads the identified blob-value block and then uses
// the block's internal structure to retrieve the value based on the
// blockValueID.
//
// A blob file may be rewritten (without rewriting referencing sstables) to
// remove unused values. Extant handles within sstables must continue to work.
// To accommodate this, rewritten blob files contain a 'virtual blocks' column
// within their index block. This column maps original blockIDs to a tuple of
// the index of the physical block containing the block's data and offset that
// should be added to the value ID to get the index of the value within the
// physical block.
//
// ## Index Block
//
// The index block is used to determine which blob-value block contains a
// particular value and the block's physical offset and length within the file.
// The index block uses a columnar encoding (see pkg colblk) to encode two
// columns:
//
// **Virtual Blocks**:
// an array of uints that is only non-empty for blob files that have been
// rewritten. The length of the array is identified by the first 4 bytes of the
// index block as a custom block header. Within the array each 64-bit uint
// value's least significant 32 bits encode the index of the physical block
// containing the original block's data. This index can be used to look up the
// byte offset and length of the physical block within the index block's offsets
// column. The most significant 32 bits of each uint value encode a BlockValueID
// offset that remaps the original BlockValueID to the new physical block's
// BlockValueID. A reader adds this BlockValueID offset to a handle's BlockValueID
// to get the index of the value within the physical block.
//
// **Offsets**:
// an array of uints encoding the offset in the blob file at which each block
// begins. There is +1 offset than there are physical blocks. The last offset
// points to the first byte after the last block. The length of blocks is
// inferred through the difference between consecutive offsets.
//
// ## Blob Value Blocks
//
// A blob value block is a columnar block encoding blob values. It encodes a
// single column: a RawBytes of values. The colblk.RawBytes encoding allows
// constant-time access to the i'th value within the block.
//
// ## Sparseness
//
// A rewrite of a blob file elides values that are no longer referenced,
// conserving disk space. Within a value block, an absent value is represented
// as an empty byte slice within the RawBytes column. This requires the overhead
// of 1 additional offset within the RawBytes encoding (typically 2-4 bytes).
//
// If a wide swath of values are no longer referenced, entire blocks may elided.
// When this occurs, the index block's virtual blocks column will map multiple
// of the original blockIDs to the same physical block.
//
// We expect significant locality to gaps in referenced values. Compactions will
// remove swaths of references all at once, typically all the values of keys
// that fall within a narrow keyspan. This locality allows us to represent most
// sparseness using the gaps between blocks, without suffering the 2-4 bytes of
// overhead for absent values internally within a block.
//
// Note: If we find this locality not hold for some reason, we can extend the
// blob-value block format to encode a NullBitmap. This would allow us to
// represent missing values using 2-bits per missing value.
//
// ## Diagram
//
// +------------------------------------------------------------------------------+
// |                             BLOB FILE FORMAT                                 |
// +------------------------------------------------------------------------------+
// |                              Value Block #0                                  |
// |   +----------------------------------------------------------------------+   |
// |   | RawBytes[...]                                                        |   |
// |   +----------------------------------------------------------------------+   |
// |                              Value Block #1                                  |
// |   +----------------------------------------------------------------------+   |
// |   | RawBytes[...]                                                        |   |
// |   +----------------------------------------------------------------------+   |
// |                                 ...                                          |
// |                              Value Block #N                                  |
// |   +----------------------------------------------------------------------+   |
// |   | RawBytes[...]                                                        |   |
// |   +----------------------------------------------------------------------+   |
// |                                                                              |
// +------------------------------- Index Block ----------------------------------+
// | Custom Header (4 bytes)                                                      |
// |   Num virtual blocks: M                                                      |
// |   +---------Virtual blocks (M)--------+    +--------Offsets(N+1)---------+   |
// |   | idx    block index  valueIDoffset |    | idx         offset          |   |
// |   | 0      0            0             |    | 0           0               |   |
// |   | 1      0            0             |    | 1           32952           |   |
// |   | 2      0            32            |    | 2           65904           |   |
// |   | 3      1            0             |    | 3           92522	          |   |
// |   | 4      2            0             |    | 4           125474          |   |
// |   | 5      3            0             |    +-----------------------------+   |
// |   +-----------------------------------+                                      |
// +----------------------------- Footer (30 bytes) ------------------------------+
// | CRC Checksum (4 bytes)                                                       |
// | Index Block Offset (8 bytes)                                                 |
// | Index Block Length (8 bytes)                                                 |
// | Checksum Type (1 byte)                                                       |
// | Format (1 byte)                                                              |
// | Magic String (8 bytes)                                                       |
// +------------------------------------------------------------------------------+
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
	if virtualBlockID != BlockID(e.countVirtualBlocks) {
		panic(errors.AssertionFailedf("virtual block ID %d is out of order; expected %d", virtualBlockID, e.countVirtualBlocks))
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

// An indexBlockDecoder decodes blob index blocks.
type indexBlockDecoder struct {
	// virtualBlockCount ...
	virtualBlockCount int
	virtualBlocks     colblk.UnsafeUints
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

const (
	blobValueBlockCustomHeaderSize = 0
	blobValueBlockColumnCount      = 1
	blobValueBlockColumnValuesIdx  = 0
)

// blobValueBlockEncoder encodes a blob value block.
//
// A blob value block is a columnar block containing a single column: an array
// of bytes encoding values.
type blobValueBlockEncoder struct {
	values colblk.RawBytesBuilder
	enc    colblk.BlockEncoder
}

// Init initializes the blob value block encoder.
func (e *blobValueBlockEncoder) Init() {
	e.values.Init()
}

// Reset resets the blob value block encoder to its initial state, retaining
// buffers.
func (e *blobValueBlockEncoder) Reset() {
	e.values.Reset()
	e.enc.Reset()
}

// AddValue adds a value to the blob value block.
func (e *blobValueBlockEncoder) AddValue(v []byte) {
	e.values.Put(v)
}

// Count returns the number of values in the blob value block.
func (e *blobValueBlockEncoder) Count() int {
	return e.values.Rows()
}

func (e *blobValueBlockEncoder) size() int {
	off := colblk.HeaderSize(blobValueBlockColumnCount, blobValueBlockCustomHeaderSize)
	if rows := e.values.Rows(); rows > 0 {
		off = e.values.Size(rows, off)
	}
	off++
	return int(off)
}

// Finish serializes the pending blob value block.
func (e *blobValueBlockEncoder) Finish() []byte {
	e.enc.Init(e.size(), colblk.Header{
		Version: colblk.Version1,
		Columns: blobValueBlockColumnCount,
		Rows:    uint32(e.values.Rows()),
	}, blobValueBlockCustomHeaderSize)
	e.enc.Encode(e.values.Rows(), &e.values)
	return e.enc.Finish()
}

// A blobValueBlockDecoder reads columnar blob value blocks.
type blobValueBlockDecoder struct {
	values colblk.RawBytes
	bd     colblk.BlockDecoder
}

// Init initializes the decoder with the given serialized blob value block.
func (d *blobValueBlockDecoder) Init(data []byte) {
	d.bd.Init(data, blobValueBlockCustomHeaderSize)
	d.values = d.bd.RawBytes(blobValueBlockColumnValuesIdx)
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
// described by r.
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
	f.HexBytesln(1, "block padding byte")
	f.ToTreePrinter(n)
}

// Assert that an BlobBlockDecoder can fit inside block.Metadata.
const _ uint = block.MetadataSize - uint(unsafe.Sizeof(blobValueBlockDecoder{}))

// initBlobValueBlockMetadata initializes the blob value block metadata.
func initBlobValueBlockMetadata(md *block.Metadata, data []byte) (err error) {
	if uintptr(unsafe.Pointer(md))%8 != 0 {
		return errors.AssertionFailedf("metadata is not 8-byte aligned")
	}
	d := (*blobValueBlockDecoder)(unsafe.Pointer(md))
	// Initialization can panic; convert panics to corruption errors (so higher
	// layers can add file number and offset information).
	defer func() {
		if r := recover(); r != nil {
			err = base.CorruptionErrorf("error initializing blob value block metadata: %v", r)
		}
	}()
	d.Init(data)
	return nil
}
