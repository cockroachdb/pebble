// Copyright 2025 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package sstable

import (
	"encoding/binary"
	"iter"
	"unsafe"

	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/pebble/internal/base"
	"github.com/cockroachdb/pebble/internal/binfmt"
	"github.com/cockroachdb/pebble/internal/treeprinter"
	"github.com/cockroachdb/pebble/sstable/blob"
	"github.com/cockroachdb/pebble/sstable/block"
	"github.com/cockroachdb/pebble/sstable/colblk"
)

const (
	valueLivenessBlockCustomHeaderSize = 0
	valueLivenessBlockColumnCount      = 1
	valueLivenessBlockColumnIndex      = 0
)

// referenceLivenessBlockEncoder encodes a reference liveness block.
// A reference liveness block is a columnar block that contains a single column:
// an array of bytes encoding the liveness of values within a sstable's blob
// references. The indexes into this array are the blob.ReferenceIDs contained
// within the sstable.
type referenceLivenessBlockEncoder struct {
	values colblk.RawBytesBuilder
	enc    colblk.BlockEncoder
}

// Init initializes the reference liveness block encoder.
func (e *referenceLivenessBlockEncoder) Init() {
	e.values.Init()
}

// Reset resets the reference liveness block encoder.
func (e *referenceLivenessBlockEncoder) Reset() {
	e.values.Reset()
	e.enc.Reset()
}

// AddReferenceLiveness adds a value to the reference liveness block.
func (e *referenceLivenessBlockEncoder) AddReferenceLiveness(referenceID int, v []byte) {
	if e.values.Rows() != referenceID {
		panic(base.AssertionFailedf("referenceID %d does not match number of rows %d", referenceID, e.values.Rows()))
	}
	e.values.Put(v)
}

// Count returns the number of values in the reference liveness block.
func (e *referenceLivenessBlockEncoder) Count() int {
	return e.values.Rows()
}

func (e *referenceLivenessBlockEncoder) size() int {
	rows := e.values.Rows()
	if rows == 0 {
		return 0
	}
	off := colblk.HeaderSize(valueLivenessBlockColumnCount, valueLivenessBlockCustomHeaderSize)
	off = e.values.Size(rows, off)
	// Add a padding byte at the end to allow the block's end to be represented
	// as a pointer to allocated memory.
	off++
	return int(off)
}

// Finish serializes the pending reference liveness block.
func (e *referenceLivenessBlockEncoder) Finish() []byte {
	e.enc.Init(e.size(), colblk.Header{
		Version: colblk.Version1,
		Columns: valueLivenessBlockColumnCount,
		Rows:    uint32(e.values.Rows()),
	}, valueLivenessBlockCustomHeaderSize)
	e.enc.Encode(e.values.Rows(), &e.values)
	return e.enc.Finish()
}

// blobReferenceValues tracks which values within a blob file are referenced
// from an sstable being constructed.
type blobReferenceValues struct {
	// currentBlock tracks which values are referenced within the most-recently
	// observed BlockID of the referenced blob file.
	//
	// Values are written to blob files in the order of their referencing keys,
	// which guarantees that compactions always observe the blob value handles
	// referencing a particular blob file in order by (blockID, valueID).
	//
	// We take advantage of this ordering in the blobReferenceValues type,
	// accumulating a bitmap of values referenced in an individual block of a
	// blob file at a time. Once we see a reference to a new block within the
	// same blob file, the buffered bitmap is encoded and appended to
	// encodedFinishedBlocks.
	currentBlock struct {
		blockID    blob.BlockID
		bitmap     BitmapRunLengthEncoder
		valuesSize uint64
	}
	// encodedFinishedBlocks is a buffer encoding which values were referenced
	// within the blob file among all the referenced blob file's blocks prior to
	// currentBlock.blockID.
	encodedFinishedBlocks []byte
}

// initNewBlock initializes the state for a new block, resetting all fields to
// their initial values.
func (s *blobReferenceValues) initNewBlock(blockID blob.BlockID) {
	s.currentBlock.blockID = blockID
	s.currentBlock.bitmap.Init()
	s.currentBlock.valuesSize = 0
}

// finishCurrentBlock writes the in-progress value liveness encoding for a blob
// value block to the encoder's buffer.
//
//	<block ID> <values size> <n bytes of bitmap> [<bitmap>]
func (s *blobReferenceValues) finishCurrentBlock() {
	if s.currentBlock.valuesSize == 0 {
		panic(errors.AssertionFailedf("no pending current block"))
	}
	s.encodedFinishedBlocks = binary.AppendUvarint(s.encodedFinishedBlocks, uint64(s.currentBlock.blockID))
	s.encodedFinishedBlocks = binary.AppendUvarint(s.encodedFinishedBlocks, s.currentBlock.valuesSize)
	s.encodedFinishedBlocks = binary.AppendUvarint(s.encodedFinishedBlocks, uint64(s.currentBlock.bitmap.Size()))
	s.encodedFinishedBlocks = s.currentBlock.bitmap.FinishAndAppend(s.encodedFinishedBlocks)
}

// blobRefValueLivenessWriter helps maintain the liveness of values in blob value
// blocks for a sstable's blob references. It maintains a refState, a slice of
// blobRefValueLivenessState. This tracks the in-progress value liveness for
// each blob value block for our sstable's blob references. The index of the
// slice corresponds to the blob.ReferenceID.
type blobRefValueLivenessWriter struct {
	refState []blobReferenceValues
}

// init initializes the writer's state.
func (w *blobRefValueLivenessWriter) init() {
	clear(w.refState)
	w.refState = w.refState[:0]
}

// numReferences returns the number of references that have liveness encodings
// that have been added to the writer.
func (w *blobRefValueLivenessWriter) numReferences() int {
	return len(w.refState)
}

// addLiveValue adds a live value to the state maintained by refID. If the
// current blockID for this in-progress state is different from the provided
// blockID, a new state is created.
//
// addLiveValue adds a new state for the provided refID if one does
// not already exist. It assumes that any new blob.ReferenceIDs are visited in
// monotonically increasing order.
func (w *blobRefValueLivenessWriter) addLiveValue(
	refID blob.ReferenceID, blockID blob.BlockID, valueID blob.BlockValueID, valueSize uint64,
) error {
	// Compute the minimum expected length of the state slice in order for our
	// refID to be indexable.
	minLen := int(refID) + 1

	// If we don't already have a state for this reference, we might just need
	// to grow.
	if len(w.refState) < minLen {
		// Check if we have jumped ahead more than one reference.
		if len(w.refState) < minLen && len(w.refState)+1 != minLen {
			return base.AssertionFailedf("jump from greatest reference ID %d to new reference "+
				"ID %d greater than 1", len(w.refState)-1, refID)
		}

		// We have a new reference.
		state := blobReferenceValues{}
		state.initNewBlock(blockID)
		w.refState = append(w.refState, state)
	}

	state := &w.refState[refID]
	if state.currentBlock.blockID != blockID {
		state.finishCurrentBlock()
		state.initNewBlock(blockID)
	}
	state.currentBlock.valuesSize += valueSize
	state.currentBlock.bitmap.Set(int(valueID))
	return nil
}

// finish finishes encoding the per-blob reference liveness encodings, and
// returns an in-order sequence of (referenceID, encoding) pairs.
func (w *blobRefValueLivenessWriter) finish() iter.Seq2[blob.ReferenceID, []byte] {
	return func(yield func(blob.ReferenceID, []byte) bool) {
		// N.B. `i` is equivalent to blob.ReferenceID.
		for i, state := range w.refState {
			state.finishCurrentBlock()
			if !yield(blob.ReferenceID(i), state.encodedFinishedBlocks) {
				return
			}
		}
	}
}

// BlobRefLivenessEncoding represents the decoded form of a blob reference
// liveness encoding. The encoding format is:
//
//	<block ID> <values size> <len of bitmap> [<bitmap>]
type BlobRefLivenessEncoding struct {
	BlockID    blob.BlockID
	ValuesSize int
	BitmapSize int
	Bitmap     []byte
}

// DecodeBlobRefLivenessEncoding decodes a sequence of blob reference liveness
// encodings from the provided buffer. Each encoding has the format:
//
//	<block ID> <values size> <len of bitmap> [<bitmap>]
func DecodeBlobRefLivenessEncoding(buf []byte) []BlobRefLivenessEncoding {
	var encodings []BlobRefLivenessEncoding
	for len(buf) > 0 {
		var enc BlobRefLivenessEncoding
		var n int

		blockIDVal, n := binary.Uvarint(buf)
		buf = buf[n:]
		enc.BlockID = blob.BlockID(blockIDVal)

		valuesSizeVal, n := binary.Uvarint(buf)
		buf = buf[n:]
		enc.ValuesSize = int(valuesSizeVal)

		bitmapSizeVal, n := binary.Uvarint(buf)
		buf = buf[n:]
		enc.BitmapSize = int(bitmapSizeVal)

		// The bitmap takes up the remaining bitmapSize bytes for this encoding.
		enc.Bitmap = buf[:enc.BitmapSize]
		buf = buf[enc.BitmapSize:]

		encodings = append(encodings, enc)
	}
	return encodings
}

// ReferenceLivenessBlockDecoder reads columnar reference liveness blocks.
type ReferenceLivenessBlockDecoder struct {
	values colblk.RawBytes
	bd     colblk.BlockDecoder
}

// Init initializes the decoder with the given serialized reference liveness
// block.
func (d *ReferenceLivenessBlockDecoder) Init(data []byte) {
	d.bd.Init(data, valueLivenessBlockCustomHeaderSize)
	d.values = d.bd.RawBytes(valueLivenessBlockColumnIndex)
}

// DebugString prints a human-readable explanation of the reference liveness
// block's binary representation.
func (d *ReferenceLivenessBlockDecoder) DebugString() string {
	f := binfmt.New(d.bd.Data()).LineWidth(20)
	tp := treeprinter.New()
	d.Describe(f, tp.Child("reference-liveness-block-decoder"))
	return tp.String()
}

// Describe describes the binary format of the reference liveness block, assuming
// f.Offset() is positioned at the beginning of the same reference liveness
// block described by d.
func (d *ReferenceLivenessBlockDecoder) Describe(f *binfmt.Formatter, tp treeprinter.Node) {
	// Set the relative offset. When loaded into memory, the beginning of blocks
	// are aligned. Padding that ensures alignment is done relative to the
	// current offset. Setting the relative offset ensures that if we're
	// describing this block within a larger structure (eg, f.Offset()>0), we
	// compute padding appropriately assuming the current byte f.Offset() is
	// aligned.
	f.SetAnchorOffset()

	n := tp.Child("reference liveness block header")
	d.bd.HeaderToBinFormatter(f, n)
	d.bd.ColumnToBinFormatter(f, n, 0, d.bd.Rows())
	f.HexBytesln(1, "block padding byte")
	f.ToTreePrinter(n)
}

// BlockDecoder returns the block decoder for the reference liveness block.
func (d *ReferenceLivenessBlockDecoder) BlockDecoder() *colblk.BlockDecoder {
	return &d.bd
}

// LivenessAtReference returns the liveness of the reference (the given index).
func (d *ReferenceLivenessBlockDecoder) LivenessAtReference(i int) []byte {
	return d.values.At(i)
}

// Assert that an ReferenceLivenessBlockDecoder can fit inside block.Metadata.
const _ uint = block.MetadataSize - uint(unsafe.Sizeof(ReferenceLivenessBlockDecoder{}))
