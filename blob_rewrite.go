// Copyright 2025 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package pebble

import (
	"container/heap"
	"context"
	"fmt"
	"slices"

	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/pebble/internal/base"
	"github.com/cockroachdb/pebble/internal/manifest"
	"github.com/cockroachdb/pebble/sstable"
	"github.com/cockroachdb/pebble/sstable/blob"
	"github.com/cockroachdb/pebble/sstable/block"
	"github.com/cockroachdb/pebble/sstable/colblk"
)

// blockHeap is a min-heap of blob reference liveness encodings, ordered by
// blockID. We use this to help us determine the overall liveness of values in
// each blob block by combining the blob reference liveness encodings of all
// referencing sstables for a particular blockID.
type blockHeap []*sstable.BlobRefLivenessEncoding

// Len implements sort.Interface.
func (h blockHeap) Len() int { return len(h) }

// Less implements sort.Interface.
func (h blockHeap) Less(i, j int) bool { return h[i].BlockID < h[j].BlockID }

// Swap implements sort.Interface.
func (h blockHeap) Swap(i, j int) {
	h[i], h[j] = h[j], h[i]
}

// Push implements heap.Interface.
func (h *blockHeap) Push(x any) {
	blobEnc := x.(*sstable.BlobRefLivenessEncoding)
	*h = append(*h, blobEnc)
}

// Pop implements heap.Interface.
func (h *blockHeap) Pop() any {
	old := *h
	n := len(old)
	item := old[n-1]
	old[n-1] = nil
	*h = old[0 : n-1]
	return item
}

// accumulatedBlockData holds the accumulated liveness data for a block.
type accumulatedBlockData struct {
	valuesSize   int
	liveValueIDs []int
}

// blobFileMapping implements blob.FileMapping to always map to the input blob
// file.
type blobFileMapping struct {
	fileNum base.DiskFileNum
}

func (m *blobFileMapping) Lookup(fileID base.BlobFileID) (base.DiskFileNum, bool) {
	return m.fileNum, true
}

// blobFileRewriter is responsible for rewriting blob files by combining and
// processing blob reference liveness encodings from multiple SSTables. It
// maintains state for writing to an output blob file.
type blobFileRewriter struct {
	fc       *fileCacheHandle
	env      block.ReadEnv
	sstables []*manifest.TableMetadata

	inputBlob    manifest.BlobFileMetadata
	valueFetcher blob.ValueFetcher
	fileMapping  *blobFileMapping

	// Current blob writer state
	writer *blob.FileWriter
}

func newBlobFileRewriter(
	fc *fileCacheHandle, sstables []*manifest.TableMetadata, inputBlob manifest.BlobFileMetadata,
) *blobFileRewriter {
	fileMapping := &blobFileMapping{fileNum: inputBlob.Physical.FileNum}
	return &blobFileRewriter{
		fc:          fc,
		sstables:    sstables,
		inputBlob:   inputBlob,
		fileMapping: fileMapping,
	}
}

// generateHeap populates and returns a blockHeap with the blob reference
// liveness encodings for each referencing sstable, rw.inputSSTables.
func (rw *blobFileRewriter) generateHeap() (*blockHeap, error) {
	ctx := context.TODO()
	blkHeap := &blockHeap{}
	heap.Init(blkHeap)

	var decoder colblk.ReferenceLivenessBlockDecoder
	// For each sstable that references the input blob file, push its
	// sstable.BlobLivenessEncoding on to the heap.
	for _, sst := range rw.sstables {
		for i, ref := range sst.BlobReferences {
			if ref.FileID == base.BlobFileID(rw.inputBlob.Physical.FileNum) {
				reader, _, err := rw.fc.openFile(ctx, sst.TableBacking.DiskFileNum, base.FileTypeTable)
				if err != nil {
					return nil, err
				}
				h, err := reader.(*sstable.Reader).ReadBlobRefIndexBlock(ctx, rw.env)
				if err != nil {
					return nil, err
				}
				blockData := append([]byte(nil), h.BlockData()...)
				h.Release()
				decoder.Init(blockData)
				bitmapEncodings := decoder.LivenessAtReference(i)
				for _, enc := range sstable.DecodeBlobRefLivenessEncoding(bitmapEncodings) {
					fmt.Println("pushing encoding on the heap")
					heap.Push(blkHeap, enc)
				}
			}
		}
	}
	return blkHeap, nil
}

// copyBlockValues copies the live values from the given block to the output blob file.
func (rw *blobFileRewriter) copyBlockValues(
	ctx context.Context, blockID blob.BlockID, liveValueIDs []int,
) error {
	slices.Sort(liveValueIDs)
	for _, valueID := range liveValueIDs {
		// Create a handle suffix for the value
		handleSuffix := blob.HandleSuffix{
			BlockID: blockID,
			ValueID: blob.BlockValueID(valueID),
		}
		handleSuffixBytes := make([]byte, blob.MaxInlineHandleLength)
		n := handleSuffix.Encode(handleSuffixBytes)

		// Fetch the value using the ValueFetcher
		value, _, err := rw.valueFetcher.Fetch(ctx, handleSuffixBytes[:n], base.BlobFileID(rw.inputBlob.Physical.FileNum), 0, nil)
		if err != nil {
			return err
		}
		rw.writer.AddValue(value)
	}
	return nil
}

func (rw *blobFileRewriter) Rewrite() error {
	ctx := context.TODO()

	// Initialize the ValueFetcher
	rw.valueFetcher.Init(rw.fileMapping, rw.fc, rw.env)
	defer func() { _ = rw.valueFetcher.Close() }()

	blkHeap, err := rw.generateHeap()
	if err != nil {
		return err
	}

	// Begin constructing our output blob file. We maintain a map of blockID
	// to accumulated liveness data across all referencing sstables.
	encodingsAcc := make(map[blob.BlockID]*accumulatedBlockData)
	var lastBlockID blob.BlockID
	for blkHeap.Len() > 0 {
		currBlock := heap.Pop(blkHeap).(*sstable.BlobRefLivenessEncoding)

		// If we haven't seen this block before in our accumulator, write the
		// last accumulated block to the blob file and initialize the current
		// block in our accumulator map.
		if _, exists := encodingsAcc[currBlock.BlockID]; !exists {
			toWrite, ok := encodingsAcc[lastBlockID]
			if !ok {
				return errors.AssertionFailedf("block %d not found in accumulator", lastBlockID)
			}
			// Add virtual block mappings for all blocks between the last block
			// we encountered and the current block.
			for blockID := lastBlockID; blockID < currBlock.BlockID; blockID++ {
				rw.writer.BeginNewVirtualBlock(blockID)
			}
			if err := rw.copyBlockValues(ctx, currBlock.BlockID, toWrite.liveValueIDs); err != nil {
				return err
			}
			encodingsAcc[currBlock.BlockID] = &accumulatedBlockData{
				valuesSize:   currBlock.ValuesSize,
				liveValueIDs: slices.Collect(sstable.IterSetBitsInRunLengthBitmap(currBlock.Bitmap)),
			}
			lastBlockID = currBlock.BlockID
			continue
		}

		// Update the accumulated encoding for this block.
		accEncoding := encodingsAcc[currBlock.BlockID]
		accEncoding.valuesSize += currBlock.ValuesSize
		accEncoding.liveValueIDs = slices.AppendSeq(accEncoding.liveValueIDs, sstable.IterSetBitsInRunLengthBitmap(currBlock.Bitmap))
		lastBlockID = currBlock.BlockID
	}

	// Copy the last accumulated block.
	if lastAcc, ok := encodingsAcc[lastBlockID]; ok {
		if err := rw.copyBlockValues(ctx, lastBlockID, lastAcc.liveValueIDs); err != nil {
			return err
		}
	}

	_, err = rw.writer.Close()
	if err != nil {
		return err
	}
	return nil
}
