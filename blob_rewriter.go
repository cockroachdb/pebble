// Copyright 2025 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.
package pebble

import (
	"container/heap"
	"context"

	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/pebble/internal/base"
	"github.com/cockroachdb/pebble/internal/manifest"
	"github.com/cockroachdb/pebble/objstorage"
	"github.com/cockroachdb/pebble/sstable"
	"github.com/cockroachdb/pebble/sstable/blob"
	"github.com/cockroachdb/pebble/sstable/block"
	"github.com/cockroachdb/pebble/sstable/colblk"
	"github.com/cockroachdb/pebble/sstable/valblk"
	"github.com/cockroachdb/pebble/vfs"
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
	blobEnc, ok := x.(*sstable.BlobRefLivenessEncoding)
	if !ok {
		panic(errors.AssertionFailedf("invalid type in heap"))
	}
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

// blobFileRewriter is responsible for rewriting blob files by combining and
// processing blob reference liveness encodings from multiple SSTables. It
// maintains state for writing to an output blob file.
type blobFileRewriter struct {
	// fs is the filesystem to read the input SSTables from.
	fs       vfs.FS
	sstables []*manifest.TableMetadata

	opts     sstable.ReaderOptions
	blobOpts blob.FileReaderOptions
	decoder  colblk.ReferenceLivenessBlockDecoder
	// Current blob writer state
	writer     *blob.FileWriter
	writerOpts blob.FileWriterOptions

	inputBlob  base.DiskFileNum
	readable   objstorage.Readable
	readHandle objstorage.ReadHandle
}

func NewBlobFileRewriter(
	fs vfs.FS,
	sstables []*manifest.TableMetadata,
	opts sstable.ReaderOptions,
	blobOpts blob.FileReaderOptions,
	inputBlob base.DiskFileNum,
) *blobFileRewriter {
	return &blobFileRewriter{
		fs:        fs,
		sstables:  sstables,
		opts:      opts,
		blobOpts:  blobOpts,
		inputBlob: inputBlob,
	}
}

// getBlobValueBlock returns a value block buffer for the given blockID.
func (rw *blobFileRewriter) getBlobValueBlock(ctx context.Context, blockID int) ([]byte, error) {
	reader, err := blob.NewFileReader(ctx, rw.readable, rw.blobOpts)
	if err != nil {
		return nil, err
	}
	defer reader.Close()
	indexBlock, err := reader.ReadIndexBlock(ctx, block.NoReadEnv, rw.readHandle)
	if err != nil {
		return nil, err
	}
	// TODO(before merge): is this how we want to get the value block? what will be
	// the index handle?
	var vbih valblk.IndexHandle
	blockHandle, err := valblk.DecodeBlockHandleFromIndex(indexBlock.BlockData(), uint32(blockID), vbih)
	if err != nil {
		return nil, err
	}
	block, err := reader.ReadValueBlock(ctx, block.NoReadEnv, rw.readHandle, blockHandle)
	if err != nil {
		return nil, err
	}
	return block.BlockData(), nil
}

// generateHeap populates and returns a blockHeap with the blob reference
// liveness encodings for each referencing sstable, rw.inputSSTables.
func (rw *blobFileRewriter) generateHeap() (*blockHeap, error) {
	ctx := context.TODO()
	blkHeap := &blockHeap{}
	heap.Init(blkHeap)

	// For each sstable that references the input blob file, push its
	// sstable.BlobLivenessEncoding on to the heap.
	for _, sst := range rw.sstables {
		for i, ref := range sst.BlobReferences {
			if ref.FileID == base.BlobFileID(rw.inputBlob) {
				f, err := rw.fs.Open(sst.TableBacking.DiskFileNum.String())
				if err != nil {
					return nil, err
				}
				readable, err := sstable.NewSimpleReadable(f)
				if err != nil {
					return nil, err
				}
				reader, err := sstable.NewReader(ctx, readable, rw.opts)
				if err != nil {
					return nil, err
				}
				h, err := reader.ReadBlobRefIndexBlock(ctx)
				if err != nil {
					return nil, err
				}
				rw.decoder.Init(h.BlockData())
				bitmapEncoding := rw.decoder.LivenessAtReference(i)
				livenessEncoding := sstable.DecodeBlobRefLivenessEncoding(bitmapEncoding)

				heap.Push(blkHeap, livenessEncoding)
			}
		}
	}
	return blkHeap, nil
}

func (rw *blobFileRewriter) Rewrite() error {
	blkHeap, err := rw.generateHeap()
	if err != nil {
		return err
	}

	// Begin constructing our output blob file. We maintain a map of blockID
	// to accumulated bitmap encodings across all referencing sstables.
	encodingsAcc := make(map[int]*sstable.BlobRefLivenessEncoding)
	var combinedEncoder sstable.BitmapRunLengthEncoder
	combinedEncoder.Init()
	blockSize := 0
	var lastBlockID int
	for blkHeap.Len() > 0 {
		currBlock, ok := heap.Pop(blkHeap).(*sstable.BlobRefLivenessEncoding)
		if !ok {
			return errors.AssertionFailedf("invalid type in heap")
		}

		// If we haven't seen this block before in our accumulator, write the
		// last accumulated block to the blob file and initialize the current
		// block in our accumulator map.
		if _, exists := encodingsAcc[currBlock.BlockID]; !exists {
			toWrite, ok := encodingsAcc[lastBlockID]
			if !ok {
				return errors.AssertionFailedf("block %d not found in accumulator", lastBlockID)
			}
			// Check if adding this block would exceed the block size limit,
			// triggering a flush.
			if rw.writerOpts.FlushGovernor.ShouldFlush(blockSize, toWrite.ValuesSize) {
				rw.writer.BeginNewVirtualBlock(blob.BlockID(toWrite.BlockID))
				blockSize = 0
			}
			var idxForValsToKeep []int
			for i := range sstable.IterSetBitsInRunLengthBitmap(toWrite.Bitmap) {
				idxForValsToKeep = append(idxForValsToKeep, i)
			}
			blockData, err := rw.getBlobValueBlock(context.TODO(), currBlock.BlockID)
			if err != nil {
				return err
			}
			var decoder blob.BlobValueBlockDecoder
			decoder.Init(blockData)
			for _, idx := range idxForValsToKeep {
				rw.writer.AddValue(decoder.ValueAt(idx))
			}
			blockSize += toWrite.ValuesSize
			encodingsAcc[currBlock.BlockID] = currBlock
			lastBlockID = currBlock.BlockID
			continue
		}

		// Update the accumulated encoding for this block.
		accEncoding := encodingsAcc[currBlock.BlockID]
		accEncoding.ValuesSize = max(accEncoding.ValuesSize, currBlock.ValuesSize)

		// Combine bitmaps
		for i := range sstable.IterSetBitsInRunLengthBitmap(accEncoding.Bitmap) {
			combinedEncoder.Set(i)
		}
		for i := range sstable.IterSetBitsInRunLengthBitmap(currBlock.Bitmap) {
			combinedEncoder.Set(i)
		}
		accEncoding.Bitmap = combinedEncoder.FinishAndAppend(nil)
		encodingsAcc[currBlock.BlockID] = accEncoding
		combinedEncoder.Init()
		lastBlockID = currBlock.BlockID
	}
	_, err = rw.writer.Close()
	if err != nil {
		return err
	}
	return nil
}
