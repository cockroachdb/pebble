// Copyright 2025 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package blob

import (
	"context"
	"slices"

	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/pebble/v2/internal/base"
	"github.com/cockroachdb/pebble/v2/objstorage"
	"github.com/cockroachdb/pebble/v2/sstable/block"
)

// A FileRewriter copies values from an input blob file, outputting a new blob
// file containing a subset of the original blob file's values. The original
// Handles used to access values in the original blob file will continue to work
// with the new blob file, as long as the value was copied during rewrite.
type FileRewriter struct {
	fileID base.BlobFileID
	w      *FileWriter
	f      ValueFetcher
}

// NewFileRewriter creates a new FileRewriter that will copy values from the
// input blob file to the output blob file.
func NewFileRewriter(
	fileID base.BlobFileID,
	inputFileNum base.DiskFileNum,
	rp ReaderProvider,
	readEnv block.ReadEnv,
	outputFileNum base.DiskFileNum,
	w objstorage.Writable,
	opts FileWriterOptions,
) *FileRewriter {
	rw := &FileRewriter{
		fileID: fileID,
		w:      NewFileWriter(outputFileNum, w, opts),
	}
	rw.f.Init(inputFileMapping(inputFileNum), rp, readEnv)
	return rw
}

// CopyBlock copies the values for the given blockID to the output blob file.
// CopyBlock must be called with ascending blockIDs. The totalValueSize must be
// the size of all the values indicated by valueIDs.
func (rw *FileRewriter) CopyBlock(
	ctx context.Context, blockID BlockID, totalValueSize int, valueIDs []int,
) error {
	slices.Sort(valueIDs)

	// Consider whether we should flush the current physical block. We know
	// we'll need to add totalValueSize worth of value data, and can make a
	// decision up front. All values from the same original blockID must be
	// located in the same physical block.
	valuesInBlock := rw.w.valuesEncoder.Count()
	if valuesInBlock > 0 {
		currentBlockSize := rw.w.valuesEncoder.size() + block.TrailerLen
		if rw.w.flushGov.ShouldFlush(currentBlockSize, currentBlockSize+totalValueSize) {
			rw.w.flush()
		}
	}

	// Record the mapping from the virtual block ID to the current physical
	// block and offset within the block.
	rw.w.beginNewVirtualBlock(blockID)

	previousValueID := -1
	for _, valueID := range valueIDs {
		// Subsequent logic depends on the valueIDs being unique.
		// TODO(jackson): This is a workaround because we don't have per-sstable
		// liveness data. See https://github.com/cockroachdb/pebble/v2/issues/4915.
		// If we had per-sstable liveness data, we should be able to make this
		// an assertion failure.
		if previousValueID == valueID {
			continue
		}
		// If there is a gap in the referenced Value IDs within this block, we
		// need to represent this sparseness as empty values within the block.
		// We can represent sparseness at the tail of a block or between blocks
		// more compactly, but not sparseless at the beginning of a virtual
		// block. See the doc.go comment for more details on sparseness.
		for missingValueID := previousValueID + 1; missingValueID < valueID; missingValueID++ {
			rw.w.stats.ValueCount++
			rw.w.valuesEncoder.AddValue(nil)
		}

		// Retrieve the value and copy it to the output blob file.
		value, _, err := rw.f.Fetch(ctx, rw.fileID, blockID, BlockValueID(valueID))
		if err != nil {
			return err
		}
		// We don't know the value size, but we know it must not be empty.
		if len(value) == 0 {
			return errors.AssertionFailedf("value is empty")
		}
		rw.w.stats.ValueCount++
		rw.w.stats.UncompressedValueBytes += uint64(len(value))
		rw.w.valuesEncoder.AddValue(value)
		previousValueID = valueID
	}
	return nil
}

// Close finishes writing the output blob file and releases resources.
func (rw *FileRewriter) Close() (FileWriterStats, error) {
	stats, err := rw.w.Close()
	return stats, errors.CombineErrors(err, rw.f.Close())
}

// inputFileMapping implements blob.FileMapping and always maps to itself.
type inputFileMapping base.DiskFileNum

// Assert that (*inputFileMapping) implements blob.FileMapping.
var _ FileMapping = inputFileMapping(0)

func (m inputFileMapping) Lookup(fileID base.BlobFileID) (base.DiskFileNum, bool) {
	return base.DiskFileNum(m), true
}
