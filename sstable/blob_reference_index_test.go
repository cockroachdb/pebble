// Copyright 2025 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package sstable

import (
	"testing"

	"github.com/cockroachdb/pebble/sstable/blob"
	"github.com/stretchr/testify/require"
)

// TestBlobRefValueLivenessWriter tests functions around the
// blobRefValueLivenessWriter.
func TestBlobRefValueLivenessWriter(t *testing.T) {
	t.Run("basic", func(t *testing.T) {
		w := &blobRefValueLivenessWriter{}
		w.init()
		refID := blob.ReferenceID(0)
		blockID := blob.BlockID(0)

		require.NoError(t, w.addLiveValue(refID, blockID, 0 /* valueID */, 24 /* valueSize */))
		require.NoError(t, w.addLiveValue(refID, blockID, 1 /* valueID */, 10 /* valueSize */))
		require.NoError(t, w.addLiveValue(refID, blockID, 2 /* valueID */, 14 /* valueSize */))
		// Add a gap between valueIDs so that we get some dead values.
		require.NoError(t, w.addLiveValue(refID, blockID, 5 /* valueID */, 12 /* valueSize */))

		// Add values to a different block.
		blockID++
		require.NoError(t, w.addLiveValue(refID, blockID, 2 /* valueID */, 20 /* valueSize */))

		w.finishOutput()

		// Verify first block (refID=0, blockID=0).
		encodings := DecodeBlobRefLivenessEncoding(w.bufs[0])
		firstBlock := encodings[0]
		require.Equal(t, 0, int(firstBlock.BlockID))
		require.Equal(t, 60, firstBlock.ValuesSize)
		// We only have 1 byte worth of value liveness encoding.
		require.Equal(t, 1, firstBlock.BitmapSize)
		// Verify bitmap: 111001 (27 in hex).
		require.Equal(t, uint8(0x27), firstBlock.Bitmap[0])

		// Verify second block (refID=0, blockID=1).
		secondBlock := encodings[1]
		require.Equal(t, 1, int(secondBlock.BlockID))
		require.Equal(t, 20, secondBlock.ValuesSize)
		// We only have 1 byte worth of value liveness encoding.
		require.Equal(t, 1, secondBlock.BitmapSize)
		// Verify bitmap: 001 (4 in hex).
		require.Equal(t, uint8(0x4), secondBlock.Bitmap[0])
	})

	t.Run("all-ones", func(t *testing.T) {
		w := &blobRefValueLivenessWriter{}
		w.init()
		refID := blob.ReferenceID(0)
		blockID := blob.BlockID(0)

		// Add only live values.
		require.NoError(t, w.addLiveValue(refID, blockID, 0 /* valueID */, 100 /* valueSize */))
		require.NoError(t, w.addLiveValue(refID, blockID, 1 /* valueID */, 200 /* valueSize */))
		require.NoError(t, w.addLiveValue(refID, blockID, 2 /* valueID */, 300 /* valueSize */))

		w.finishOutput()

		encodings := DecodeBlobRefLivenessEncoding(w.bufs[0])
		firstBlock := encodings[0]
		require.Equal(t, 0, int(firstBlock.BlockID))
		require.Equal(t, 600, firstBlock.ValuesSize)
		// We only have 1 byte worth of value liveness encoding.
		require.Equal(t, 1, firstBlock.BitmapSize)
		// Verify bitmap: 111 (7 in hex).
		require.Equal(t, uint8(0x7), firstBlock.Bitmap[0])
	})
}
