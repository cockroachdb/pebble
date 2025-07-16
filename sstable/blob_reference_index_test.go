// Copyright 2025 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package sstable

import (
	"bytes"
	"fmt"
	"iter"
	"maps"
	"math/rand/v2"
	"strings"
	"testing"
	"time"

	"github.com/cockroachdb/datadriven"
	"github.com/cockroachdb/pebble/internal/testutils"
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

		encodings := maps.Collect(w.finish())
		require.Len(t, encodings, 1)
		blocks := DecodeBlobRefLivenessEncoding(encodings[0])
		require.Len(t, blocks, 2)

		// Verify first block (refID=0, blockID=0).
		require.Equal(t, blob.BlockID(0), blocks[0].BlockID)
		require.Equal(t, 60, blocks[0].ValuesSize)
		// We only have 1 byte worth of value liveness encoding.
		require.Equal(t, 1, blocks[0].BitmapSize)
		// Verify bitmap: 111001 (27 in hex).
		require.Equal(t, uint8(0x27), blocks[0].Bitmap[0])

		// Verify second block (refID=0, blockID=1).
		require.Equal(t, blob.BlockID(1), blocks[1].BlockID)
		require.Equal(t, 20, blocks[1].ValuesSize)
		// We only have 1 byte worth of value liveness encoding.
		require.Equal(t, 1, blocks[1].BitmapSize)
		// Verify bitmap: 001 (4 in hex).
		require.Equal(t, uint8(0x4), blocks[1].Bitmap[0])
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

		encodings := maps.Collect(w.finish())
		require.Len(t, encodings, 1)
		blocks := DecodeBlobRefLivenessEncoding(encodings[0])
		require.Len(t, blocks, 1)

		require.Equal(t, blob.BlockID(0), blocks[0].BlockID)
		require.Equal(t, 600, blocks[0].ValuesSize)
		// We only have 1 byte worth of value liveness encoding.
		require.Equal(t, 1, blocks[0].BitmapSize)
		// Verify bitmap: 111 (7 in hex).
		require.Equal(t, uint8(0x7), blocks[0].Bitmap[0])
	})
}

func TestBlobRefLivenessEncoding_Randomized(t *testing.T) {
	const valueSize = uint64(10)
	prng := rand.New(rand.NewPCG(uint64(time.Now().UnixNano()), 0))
	w := blobRefValueLivenessWriter{}

	collectSlice := func(i iter.Seq2[blob.ReferenceID, []byte]) [][]byte {
		var s [][]byte
		for _, enc := range i {
			s = append(s, enc)
		}
		return s
	}

	for range 20 {
		w.init()
		numRefs := testutils.RandIntInRange(prng, 1, 10)
		for refID := range numRefs {
			numBlocks := testutils.RandIntInRange(prng, 1, 6)
			currentBlockID := blob.BlockID(testutils.RandIntInRange(prng, 0, 4))

			for range numBlocks {
				// Generate blockIDs that are increasing -- with occasional
				// duplicates. Allow a 70% chance to repeat the previous block
				// ID.
				if prng.Float64() < 0.3 {
					currentBlockID += blob.BlockID(testutils.RandIntInRange(prng, 1, 4))
				}
				numValues := testutils.RandIntInRange(prng, 10, 101)
				currentValueID := blob.BlockValueID(testutils.RandIntInRange(prng, 0, 4))
				for range numValues {
					require.NoError(t, w.addLiveValue(
						blob.ReferenceID(refID),
						currentBlockID,
						currentValueID,
						valueSize,
					))
					currentValueID += blob.BlockValueID(testutils.RandIntInRange(prng, 1, 4))
				}
			}
		}
		encoded := collectSlice(w.finish())

		// Test the encoding/decoding roundtrip to ensure idempotence.
		// Reinitialize the writer before reconstructing values.
		w.init()
		for refID, blockEnc := range encoded {
			for _, block := range DecodeBlobRefLivenessEncoding(blockEnc) {
				// Reconstruct the live values from the bitmap and add them to
				// the writer.
				for valueID := range IterSetBitsInRunLengthBitmap(block.Bitmap) {
					require.NoError(t, w.addLiveValue(
						blob.ReferenceID(refID),
						block.BlockID,
						blob.BlockValueID(valueID),
						valueSize,
					))
				}
			}
		}

		reencoded := collectSlice(w.finish())
		require.Equal(t, encoded, reencoded)
	}
}

func TestValueLivenessBlock(t *testing.T) {
	var decoder ReferenceLivenessBlockDecoder
	var buf bytes.Buffer
	datadriven.RunTest(t, "testdata/value_liveness_block", func(t *testing.T, d *datadriven.TestData) string {
		buf.Reset()
		switch d.Cmd {
		case "build":
			var w referenceLivenessBlockEncoder
			w.Init()
			for i, line := range strings.Split(d.Input, "\n") {
				fields := strings.Fields(line)
				value := fields[0]
				w.AddReferenceLiveness(i, []byte(value))
			}

			data := w.Finish()
			decoder.Init(data)
			fmt.Fprint(&buf, decoder.DebugString())
			return buf.String()
		default:
			return fmt.Sprintf("unknown command: %s", d.Cmd)
		}
	})
}
