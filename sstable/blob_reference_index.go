// Copyright 2025 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package sstable

import (
	"encoding/binary"
	"iter"

	"github.com/cockroachdb/pebble/v2/internal/base"
	"github.com/cockroachdb/pebble/v2/sstable/blob"
)

// blobRefValueLivenessState tracks the liveness of values within a blob value
// block via a BitmapRunLengthEncoder.
type blobRefValueLivenessState struct {
	currentBlock struct {
		bitmap     BitmapRunLengthEncoder
		refID      blob.ReferenceID
		blockID    blob.BlockID
		valuesSize uint64
	}

	finishedBlocks []byte
}

// initNewBlock initializes the state for a new block, resetting all fields to
// their initial values.
func (s *blobRefValueLivenessState) initNewBlock(refID blob.ReferenceID, blockID blob.BlockID) {
	s.currentBlock.bitmap.Init()
	s.currentBlock.refID = refID
	s.currentBlock.blockID = blockID
	s.currentBlock.valuesSize = 0
}

// finishCurrentBlock writes the in-progress value liveness encoding for a blob
// value block to the encoder's buffer.
//
//	<block ID> <values size> <n bytes of bitmap> [<bitmap>]
func (s *blobRefValueLivenessState) finishCurrentBlock() {
	s.finishedBlocks = binary.AppendUvarint(s.finishedBlocks, uint64(s.currentBlock.blockID))
	s.finishedBlocks = binary.AppendUvarint(s.finishedBlocks, s.currentBlock.valuesSize)
	s.finishedBlocks = binary.AppendUvarint(s.finishedBlocks, uint64(s.currentBlock.bitmap.Size()))
	s.finishedBlocks = s.currentBlock.bitmap.FinishAndAppend(s.finishedBlocks)
}

// blobRefValueLivenessWriter helps maintain the liveness of values in blob value
// blocks for a sstable's blob references. It maintains:
//   - bufs: serialized value liveness encodings that will be written to the
//     sstable.
//   - refState: a slice of blobRefValueLivenessState. This tracks the
//     in-progress value liveness for each blob value block for our sstable's
//     blob references. The index of the slice corresponds to the blob.ReferenceID.
type blobRefValueLivenessWriter struct {
	refState []blobRefValueLivenessState
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
// blockID, a new state is created and the old one is preserved to the buffer
// at w.bufs[refID].
//
// addLiveValue adds a new state for the provided refID if one does
// not already exist. It assumes that any new blob.ReferenceIDs are visited in
// monotonically increasing order.
//
// INVARIANT: len(w.refState) == len(w.bufs).
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
		state := blobRefValueLivenessState{}
		state.initNewBlock(refID, blockID)
		w.refState = append(w.refState, state)
	}

	state := &w.refState[refID]
	if state.currentBlock.blockID != blockID {
		state.finishCurrentBlock()
		state.initNewBlock(refID, blockID)
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
			if !yield(blob.ReferenceID(i), state.finishedBlocks) {
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
// <block ID> <values size> <n bytes of bitmap> [<bitmap>]
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
