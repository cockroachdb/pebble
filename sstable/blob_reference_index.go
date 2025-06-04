// Copyright 2025 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package sstable

import (
	"encoding/binary"
	"math"

	"github.com/cockroachdb/pebble/internal/base"
	"github.com/cockroachdb/pebble/internal/invariants"
	"github.com/cockroachdb/pebble/sstable/blob"
)

// blobRefValueLivenessState tracks the liveness of values within a blob value
// block via a BitmapRunLengthEncoder.
type blobRefValueLivenessState struct {
	bitmap     BitmapRunLengthEncoder
	refID      blob.ReferenceID
	blockID    blob.BlockID
	valuesSize uint64
}

// init initializes the state, resetting all fields to their initial values.
func (s *blobRefValueLivenessState) init(refID blob.ReferenceID, blockID blob.BlockID) {
	s.bitmap.Init()
	s.refID = refID
	s.blockID = blockID
	s.valuesSize = 0
}

// finishOutput writes the in-progress value liveness encoding for a blob value
// block to the provided buffer, returning the modified buffer. The encoding is:
//
//	<block ID> <values size> <n bytes of bitmap> [<bitmap>]
func (s *blobRefValueLivenessState) finishOutput(buf []byte) []byte {
	buf = binary.AppendUvarint(buf, uint64(s.blockID))
	buf = binary.AppendUvarint(buf, s.valuesSize)
	var bitmap []byte
	// Save the results of FinishAndAppend in case we haven't flushed in-progress
	// bytes to the buf yet.
	bitmap = s.bitmap.FinishAndAppend(bitmap)
	buf = binary.AppendUvarint(buf, uint64(math.Ceil(float64(s.bitmap.Size())/8)))
	return append(buf, bitmap...)
}

// blobRefValueLivenessWriter helps maintain the liveness of values in blob value
// blocks for a sstable's blob references. It maintains:
//   - bufs: serialized value liveness encodings that will be written to the
//     sstable.
//   - refState: a slice of blobRefValueLivenessState. This tracks the
//     in-progress value liveness for each blob value block for our sstable's
//     blob references. The index of the slice corresponds to the blob.ReferenceID.
type blobRefValueLivenessWriter struct {
	// INVARIANT: len(bufs) == len(refState).
	bufs     [][]byte
	refState []blobRefValueLivenessState
}

// init initializes the writer's state.
func (w *blobRefValueLivenessWriter) init() {
	w.bufs = w.bufs[:0]
	w.refState = w.refState[:0]
}

// addLiveValue adds a live value to the state maintained by refID. If the
// current blockID for this in-progress state is different from the provided
// blockID, a new state is created and the old one is preserved to the buffer
// at w.bufs[refID].
func (w *blobRefValueLivenessWriter) addLiveValue(
	refID blob.ReferenceID, blockID blob.BlockID, valueID blob.BlockValueID, valueSize uint64,
) {
	state := &w.refState[refID]
	if state.blockID != blockID {
		w.bufs[refID] = state.finishOutput(w.bufs[refID])
		state.init(refID, blockID)
	}
	state.valuesSize += valueSize
	state.bitmap.Set(int(valueID))
}

// maybeAddNewState adds a new state for the provided refID if one does
// not already exist. It assumes that any new blob.ReferenceIDs are visited in
// monotonically increasing order.
//
// INVARIANT: len(w.refState) == len(w.bufs).
func (w *blobRefValueLivenessWriter) maybeAddNewState(
	refID blob.ReferenceID, blockID blob.BlockID,
) error {
	// Compute the minimum expected length of the state slice in order for our
	// refID to be indexable.
	minLen := int(refID) + 1

	// We already have a state for this reference. No need to grow.
	if len(w.refState) >= minLen {
		return nil
	}

	// Check if we have jumped ahead more than one reference.
	if invariants.Enabled && len(w.refState) < minLen && len(w.refState)+1 != minLen {
		return base.AssertionFailedf("jump from greatest reference ID %d to new reference "+
			"ID %d greater than 1", len(w.refState)-1, refID)
	}

	// We have a new reference, grow the state slice and buffer.
	w.refState = append(w.refState, blobRefValueLivenessState{
		refID:   refID,
		blockID: blockID,
	})
	w.bufs = append(w.bufs, []byte{})

	if invariants.Enabled && len(w.refState) != len(w.bufs) {
		return base.AssertionFailedf("len(refState) != len(bufs): %d != %d", len(w.refState), len(w.bufs))
	}
	return nil
}

// finishOutput finishes any in-progress state to their respective buffer.
func (w *blobRefValueLivenessWriter) finishOutput() {
	// N.B. `i` is equivalent to blob.ReferenceID.
	for i, state := range w.refState {
		w.bufs[i] = state.finishOutput(w.bufs[i])
	}
}
