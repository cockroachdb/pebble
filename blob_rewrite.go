// Copyright 2025 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package pebble

import (
	"container/heap"
	"context"
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

// accumulatedBlockData holds the accumulated liveness data for blockID.
type accumulatedBlockData struct {
	blockID      blob.BlockID
	valuesSize   int
	liveValueIDs []int
}

// blobFileMapping implements blob.FileMapping to always map to the input blob
// file.
type blobFileMapping struct {
	fileNum base.DiskFileNum
}

// Assert that (*blobFileMapping) implements blob.FileMapping.
var _ blob.FileMapping = (*blobFileMapping)(nil)

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
	fileMapping  blobFileMapping
	blkHeap      blockHeap

	// Current blob writer state.
	writer *blob.FileWriter
}

func newBlobFileRewriter(
	fc *fileCacheHandle,
	env block.ReadEnv,
	writer *blob.FileWriter,
	sstables []*manifest.TableMetadata,
	inputBlob manifest.BlobFileMetadata,
) *blobFileRewriter {
	return &blobFileRewriter{
		fc:          fc,
		env:         env,
		writer:      writer,
		sstables:    sstables,
		inputBlob:   inputBlob,
		fileMapping: blobFileMapping{fileNum: inputBlob.Physical.FileNum},
		blkHeap:     blockHeap{},
	}
}

// generateHeap populates rw.blkHeap with the blob reference liveness encodings
// for each referencing sstable, rw.sstables.
func (rw *blobFileRewriter) generateHeap() error {
	ctx := context.TODO()
	heap.Init(&rw.blkHeap)

	var decoder colblk.ReferenceLivenessBlockDecoder
	// For each sstable that references the input blob file, push its
	// sstable.BlobLivenessEncoding on to the heap.
	for _, sst := range rw.sstables {
		// Validate that the sstable contains a reference to the input blob
		// file.
		refID, ok := sst.BlobReferences.IDByBlobFileID(rw.inputBlob.FileID)
		if !ok {
			return errors.AssertionFailedf("table %s doesn't contain a reference to blob file %s",
				sst.TableNum, rw.inputBlob.FileID)
		}
		err := rw.fc.withReader(ctx, rw.env, sst, func(r *sstable.Reader, _ sstable.ReadEnv) error {
			h, err := r.ReadBlobRefIndexBlock(ctx, rw.env)
			if err != nil {
				return errors.CombineErrors(err, r.Close())
			}
			decoder.Init(h.BlockData())
			bitmapEncodings := slices.Clone(decoder.LivenessAtReference(int(refID)))
			h.Release()
			// TODO(annie): We should instead maintain 1 heap item per sstable
			// instead of 1 heap item per sstable block ref to reduce the heap
			// comparisons to O(sstables).
			for _, enc := range sstable.DecodeBlobRefLivenessEncoding(bitmapEncodings) {
				heap.Push(&rw.blkHeap, &enc)
			}
			return r.Close()
		})
		if err != nil {
			return err
		}
	}
	return nil
}

// copyBlockValues copies the live values from the given block to the output
// blob file, flushing the current block before if necessary. If a flush is
// performed, it returns true to signal the callee that we have started on a
// new block.
func (rw *blobFileRewriter) copyBlockValues(
	ctx context.Context, toWrite *accumulatedBlockData, currentBlockSize int,
) (bool, error) {
	shouldFlush := rw.writer.ShouldFlush(currentBlockSize, currentBlockSize+toWrite.valuesSize)
	if shouldFlush {
		rw.writer.ForceFlush()
	}
	slices.Sort(toWrite.liveValueIDs)
	for _, valueID := range toWrite.liveValueIDs {
		value, _, err := rw.valueFetcher.Fetch(ctx, rw.inputBlob.FileID, toWrite.blockID, blob.BlockValueID(valueID))
		if err != nil {
			return shouldFlush, err
		}
		rw.writer.AddValue(value)
	}
	return shouldFlush, nil
}

func (rw *blobFileRewriter) Rewrite() error {
	ctx := context.TODO()

	rw.valueFetcher.Init(&rw.fileMapping, rw.fc, rw.env)
	defer func() { _ = rw.valueFetcher.Close() }()

	err := rw.generateHeap()
	if err != nil {
		return err
	}

	// Begin constructing our output blob file. We maintain a map of blockID
	// to accumulated liveness data across all referencing sstables.
	var lastAccEncoding *accumulatedBlockData
	blockSize := 0
	for rw.blkHeap.Len() > 0 {
		currBlock := heap.Pop(&rw.blkHeap).(*sstable.BlobRefLivenessEncoding)

		// Initialize the last accumulated block if nil.
		if lastAccEncoding == nil {
			lastAccEncoding = &accumulatedBlockData{
				blockID:      currBlock.BlockID,
				valuesSize:   currBlock.ValuesSize,
				liveValueIDs: slices.Collect(sstable.IterSetBitsInRunLengthBitmap(currBlock.Bitmap)),
			}
		}
		// If we are encountering a new block, write the last accumulated block to
		// the blob file.
		if lastAccEncoding.blockID != currBlock.BlockID {
			// Add virtual block mappings for all blocks between the last block
			// we encountered and the current block.
			for blockID := lastAccEncoding.blockID; blockID < currBlock.BlockID; blockID++ {
				rw.writer.BeginNewVirtualBlock(blockID)
			}
			// Write the last accumulated block's values to the blob file.
			if flushed, err := rw.copyBlockValues(ctx, lastAccEncoding, blockSize); err != nil {
				return err
			} else if flushed {
				blockSize = 0
			} else {
				blockSize += lastAccEncoding.valuesSize
			}
		}

		// Update the accumulated encoding for this block.
		lastAccEncoding.valuesSize += currBlock.ValuesSize
		lastAccEncoding.liveValueIDs = slices.AppendSeq(lastAccEncoding.liveValueIDs,
			sstable.IterSetBitsInRunLengthBitmap(currBlock.Bitmap))
	}

	// Copy the last accumulated block.
	rw.writer.BeginNewVirtualBlock(lastAccEncoding.blockID)
	if _, err := rw.copyBlockValues(ctx, lastAccEncoding, blockSize); err != nil {
		return err
	}

	_, err = rw.writer.Close()
	if err != nil {
		return err
	}
	return nil
}
