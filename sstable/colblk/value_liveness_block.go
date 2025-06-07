// Copyright 2024 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package colblk

import (
	"unsafe"

	"github.com/cockroachdb/pebble/internal/binfmt"
	"github.com/cockroachdb/pebble/internal/treeprinter"
	"github.com/cockroachdb/pebble/sstable/block"
)

const (
	valueLivenessBlockCustomHeaderSize = 0
	valueLivenessBlockColumnCount      = 1
	valueLivenessBlockColumnIndex      = 0
)

// BlobRefValueLivenessIndexBlockEncoder encodes a value liveness block.
// A value liveness block is a columnar block that contains a single column:
// an array of bytes encoding the liveness of values within a blob files
// referenced by an sstable.
type BlobRefValueLivenessIndexBlockEncoder struct {
	values RawBytesBuilder
	enc    BlockEncoder
}

// Init initializes the value liveness block encoder.
func (e *BlobRefValueLivenessIndexBlockEncoder) Init() {
	e.values.Init()
}

// Reset resets the value liveness block encoder.
func (e *BlobRefValueLivenessIndexBlockEncoder) Reset() {
	e.values.Reset()
	e.enc.Reset()
}

// AddValue adds a value to the value liveness block.
func (e *BlobRefValueLivenessIndexBlockEncoder) AddValue(v []byte) {
	e.values.Put(v)
}

// Count returns the number of values in the value liveness block.
func (e *BlobRefValueLivenessIndexBlockEncoder) Count() int {
	return e.values.Rows()
}

func (e *BlobRefValueLivenessIndexBlockEncoder) size() int {
	rows := e.values.Rows()
	if rows == 0 {
		return 0
	}
	off := HeaderSize(valueLivenessBlockColumnCount, valueLivenessBlockCustomHeaderSize)
	off = e.values.Size(rows, off)
	// Add a padding byte at the end to allow the block's end to be represented
	// as a pointer to allocated memory.
	off++
	return int(off)
}

// Finish serializes the pending value liveness block.
func (e *BlobRefValueLivenessIndexBlockEncoder) Finish() []byte {
	e.enc.Init(e.size(), Header{
		Version: Version1,
		Columns: valueLivenessBlockColumnCount,
		Rows:    uint32(e.values.Rows()),
	}, valueLivenessBlockCustomHeaderSize)
	e.enc.Encode(e.values.Rows(), &e.values)
	return e.enc.Finish()
}

// ValueLivenessBlockDecoder reads columnar value liveness blocks.
type ValueLivenessBlockDecoder struct {
	values RawBytes
	bd     BlockDecoder
}

// Init initializes the decoder with the given serialized value liveness block.
func (d *ValueLivenessBlockDecoder) Init(data []byte) {
	d.bd.Init(data, valueLivenessBlockCustomHeaderSize)
	d.values = d.bd.RawBytes(valueLivenessBlockColumnIndex)
}

// DebugString prints a human-readable explanation of the value liveness block's
// binary representation.
func (d *ValueLivenessBlockDecoder) DebugString() string {
	f := binfmt.New(d.bd.Data()).LineWidth(20)
	tp := treeprinter.New()
	d.Describe(f, tp.Child("value-liveness-block-decoder"))
	return tp.String()
}

// Describe describes the binary format of the value liveness block, assuming
// f.Offset() is positioned at the beginning of the same value liveness block
// described by d.
func (d *ValueLivenessBlockDecoder) Describe(f *binfmt.Formatter, tp treeprinter.Node) {
	// Set the relative offset. When loaded into memory, the beginning of blocks
	// are aligned. Padding that ensures alignment is done relative to the
	// current offset. Setting the relative offset ensures that if we're
	// describing this block within a larger structure (eg, f.Offset()>0), we
	// compute padding appropriately assuming the current byte f.Offset() is
	// aligned.
	f.SetAnchorOffset()

	n := tp.Child("value liveness block header")
	d.bd.HeaderToBinFormatter(f, n)
	d.bd.ColumnToBinFormatter(f, n, 0, d.bd.Rows())
	f.HexBytesln(1, "block padding byte")
	f.ToTreePrinter(n)
}

func (d *ValueLivenessBlockDecoder) BlockDecoder() *BlockDecoder {
	return &d.bd
}

func (d *ValueLivenessBlockDecoder) ValueAt(i int) []byte {
	return d.values.At(i)
}

// Assert that an ValueLivenessBlockDecoder can fit inside block.Metadata.
const _ uint = block.MetadataSize - uint(unsafe.Sizeof(ValueLivenessBlockDecoder{}))
