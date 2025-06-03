// Copyright 2025 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package sstable

import (
	"encoding/binary"
	"testing"

	"github.com/cockroachdb/pebble/sstable/blob"
	"github.com/stretchr/testify/require"
)

// blobRefLivenessEncoding represents the decoded form of a blob reference
// liveness encoding. The encoding format is:
//
//	<block ID> <values size> <len of bitmap> [<bitmap>]
type blobRefLivenessEncoding struct {
	blockID    int
	valuesSize int
	bitmapSize int
	bitmap     []byte
}

// decodeBlobRefLivenessEncoding decodes a sequence of blob reference liveness encodings
// from the provided buffer. Each encoding has the format:
// <block ID> <values size> <n bytes of bitmap> [<bitmap>]
func decodeBlobRefLivenessEncoding(buf []byte) []blobRefLivenessEncoding {
	var encodings []blobRefLivenessEncoding
	for len(buf) > 0 {
		var enc blobRefLivenessEncoding
		var n int

		blockIDVal, n := binary.Uvarint(buf)
		buf = buf[n:]
		enc.blockID = int(blockIDVal)

		valuesSizeVal, n := binary.Uvarint(buf)
		buf = buf[n:]
		enc.valuesSize = int(valuesSizeVal)

		bitmapSizeVal, n := binary.Uvarint(buf)
		buf = buf[n:]
		enc.bitmapSize = int(bitmapSizeVal)

		// The bitmap takes up the remaining bitmapSize bytes for this encoding.
		enc.bitmap = buf[:enc.bitmapSize]
		buf = buf[enc.bitmapSize:]

		encodings = append(encodings, enc)
	}
	return encodings
}

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
		encodings := decodeBlobRefLivenessEncoding(w.bufs[0])
		firstBlock := encodings[0]
		require.Equal(t, 0, firstBlock.blockID)
		require.Equal(t, 60, firstBlock.valuesSize)
		// We only have 1 byte worth of value liveness encoding.
		require.Equal(t, 1, firstBlock.bitmapSize)
		// Verify bitmap: 111001 (27 in hex).
		require.Equal(t, uint8(0x27), firstBlock.bitmap[0])

		// Verify second block (refID=0, blockID=1).
		secondBlock := encodings[1]
		require.Equal(t, 1, secondBlock.blockID)
		require.Equal(t, 20, secondBlock.valuesSize)
		// We only have 1 byte worth of value liveness encoding.
		require.Equal(t, 1, secondBlock.bitmapSize)
		// Verify bitmap: 001 (4 in hex).
		require.Equal(t, uint8(0x4), secondBlock.bitmap[0])
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

		encodings := decodeBlobRefLivenessEncoding(w.bufs[0])
		firstBlock := encodings[0]
		require.Equal(t, 0, firstBlock.blockID)
		require.Equal(t, 600, firstBlock.valuesSize)
		// We only have 1 byte worth of value liveness encoding.
		require.Equal(t, 1, firstBlock.bitmapSize)
		// Verify bitmap: 111 (7 in hex).
		require.Equal(t, uint8(0x7), firstBlock.bitmap[0])
	})
}
