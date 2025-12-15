// Copyright 2025 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package sstable

import (
	"encoding/binary"
	"iter"
	"slices"

	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/pebble/internal/base"
	"github.com/cockroachdb/pebble/sstable/blob"
)

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
	// Ensure there's enough space for the bytes we're about to append.
	bitmapSize := s.currentBlock.bitmap.Size()
	s.encodedFinishedBlocks = slices.Grow(s.encodedFinishedBlocks, 3*binary.MaxVarintLen64+bitmapSize)
	s.encodedFinishedBlocks = binary.AppendUvarint(s.encodedFinishedBlocks, uint64(s.currentBlock.blockID))
	s.encodedFinishedBlocks = binary.AppendUvarint(s.encodedFinishedBlocks, s.currentBlock.valuesSize)
	s.encodedFinishedBlocks = binary.AppendUvarint(s.encodedFinishedBlocks, uint64(bitmapSize))
	s.encodedFinishedBlocks = s.currentBlock.bitmap.FinishAndAppend(s.encodedFinishedBlocks)
}

// blobRefValueLivenessWriter helps maintain the liveness of values in blob value
// blocks for a sstable's blob references. It maintains a refState, a slice of
// blobRefValueLivenessState. This tracks the in-progress value liveness for
// each blob value block for our sstable's blob references. The index of the
// slice corresponds to the base.BlobReferenceID.
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
// not already exist. It assumes that any new base.BlobReferenceIDs are visited in
// monotonically increasing order.
func (w *blobRefValueLivenessWriter) addLiveValue(
	refID base.BlobReferenceID, blockID blob.BlockID, valueID blob.BlockValueID, valueSize uint64,
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
		state.encodedFinishedBlocks = slices.Grow(state.encodedFinishedBlocks, 512)
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
func (w *blobRefValueLivenessWriter) finish() iter.Seq2[base.BlobReferenceID, []byte] {
	return func(yield func(base.BlobReferenceID, []byte) bool) {
		// N.B. `i` is equivalent to base.BlobReferenceID.
		for i, state := range w.refState {
			state.finishCurrentBlock()
			if !yield(base.BlobReferenceID(i), state.encodedFinishedBlocks) {
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
func DecodeBlobRefLivenessEncoding(buf []byte) ([]BlobRefLivenessEncoding, error) {
	var encodings []BlobRefLivenessEncoding
	for len(buf) > 0 {
		var enc BlobRefLivenessEncoding
		var n int

		blockIDVal, n := binary.Uvarint(buf)
		if n <= 0 {
			return nil, base.CorruptionErrorf("cannot decode block ID from buf %x", buf)
		}
		buf = buf[n:]
		enc.BlockID = blob.BlockID(blockIDVal)

		valuesSizeVal, n := binary.Uvarint(buf)
		if n <= 0 {
			return nil, base.CorruptionErrorf("cannot decode values size from buf %x", buf)
		}
		buf = buf[n:]
		enc.ValuesSize = int(valuesSizeVal)

		bitmapSizeVal, n := binary.Uvarint(buf)
		if n <= 0 {
			return nil, base.CorruptionErrorf("cannot decode bitmap size from buf %x", buf)
		}
		buf = buf[n:]
		enc.BitmapSize = int(bitmapSizeVal)

		// The bitmap takes up the remaining bitmapSize bytes for this encoding.
		enc.Bitmap = buf[:enc.BitmapSize]
		buf = buf[enc.BitmapSize:]

		encodings = append(encodings, enc)
	}
	return encodings, nil
}
