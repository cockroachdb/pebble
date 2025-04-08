// Copyright 2024 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package sstable

import (
	"context"

	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/pebble/internal/base"
	"github.com/cockroachdb/pebble/internal/bytealloc"
	"github.com/cockroachdb/pebble/objstorage"
	"github.com/cockroachdb/pebble/objstorage/objstorageprovider"
	"github.com/cockroachdb/pebble/sstable/block"
)

// CopySpan produces a copy of a approximate subset of an input sstable.
//
// The produced sstable contains all keys from the input sstable in the span
// [start, end), as well as potentially some additional keys from the original
// file that were adjacent to but outside that span.
//
// CopySpan differs from simply seeking a reader to start and iterating until
// the end passing the results to a writer in that it does not write the new
// sstable from scratch, key-by-key, recompressing each key into new blocks and
// computing new filters and properties. Instead, it finds data _blocks_ that
// intersect the requested span and copies those, whole, to the new file,
// avoiding all decompression and recompression work. It then copies the
// original bloom filter - this filter is valid for the subset of data as well,
// just with potentially a higher false positive rate compared to one that would
// be computed just from the keys in it.
//
// The resulting sstable will have no block properties.
//
// The function might return ErrEmptySpan if there are no blocks that could
// include keys in the given range. See ErrEmptySpan for more details.
//
// Closes input and finishes or aborts output in all cases, including on errors.
//
// Note that CopySpan is not aware of any suffix or prefix replacement; the
// caller must account for those when specifying the bounds.
func CopySpan(
	ctx context.Context,
	input objstorage.Readable,
	r *Reader,
	rOpts ReaderOptions,
	output objstorage.Writable,
	o WriterOptions,
	start, end InternalKey,
) (size uint64, _ error) {
	defer func() { _ = input.Close() }()

	if r.Properties.NumValueBlocks > 0 || r.Properties.NumRangeKeys() > 0 || r.Properties.NumRangeDeletions > 0 {
		return copyWholeFileBecauseOfUnsupportedFeature(ctx, input, output) // Finishes/Aborts output.
	}

	// If our input has not filters, our output cannot have filters either.
	if r.tableFilter == nil {
		o.FilterPolicy = nil
	}
	o.TableFormat = r.tableFormat
	// We don't want the writer to attempt to write out block property data in
	// index blocks. This data won't be valid since we're not passing the actual
	// key data through the writer. We also remove the table-level properties
	// below.
	//
	// TODO(dt,radu): Figure out how to populate the prop collector state with
	// block props from the original sst.
	o.BlockPropertyCollectors = nil
	o.disableObsoleteCollector = true
	w := NewRawWriter(output, o)

	defer func() {
		if w != nil {
			_ = w.Close()
		}
	}()

	if r.Properties.NumValueBlocks > 0 || r.Properties.NumRangeKeys() > 0 || r.Properties.NumRangeDeletions > 0 {
		// We just checked for these conditions above.
		return 0, base.AssertionFailedf("cannot CopySpan sstables with value blocks or range keys")
	}

	var preallocRH objstorageprovider.PreallocatedReadHandle
	// ReadBeforeForIndexAndFilter attempts to read the top-level index, filter
	// and lower-level index blocks with one read.
	rh := r.blockReader.UsePreallocatedReadHandle(
		objstorage.ReadBeforeForIndexAndFilter, &preallocRH)
	defer func() { _ = rh.Close() }()
	rh.SetupForCompaction()
	indexH, err := r.readTopLevelIndexBlock(ctx, block.NoReadEnv, rh)
	if err != nil {
		return 0, err
	}
	defer indexH.Release()

	// Set the filter block to be copied over if it exists. It will return false
	// positives for keys in blocks of the original file that we don't copy, but
	// filters can always have false positives, so this is fine.
	if r.tableFilter != nil {
		filterBlock, err := r.readFilterBlock(ctx, block.NoReadEnv, rh, r.filterBH)
		if err != nil {
			return 0, errors.Wrap(err, "reading filter")
		}
		filterBytes := append([]byte{}, filterBlock.BlockData()...)
		filterBlock.Release()
		if err := w.copyFilter(filterBytes, r.Properties.FilterPolicyName); err != nil {
			return 0, errors.Wrap(err, "copying filter")
		}
	}

	// Copy all the props from the source file; we can't compute our own for many
	// that depend on seeing every key, such as total count or size so we copy the
	// original props instead. This will result in over-counts but that is safer
	// than under-counts.
	w.copyProperties(r.Properties)

	// Find the blocks that intersect our span.
	blocks, err := intersectingIndexEntries(ctx, r, rh, indexH, start, end)
	if err != nil {
		return 0, err
	}

	// In theory an empty SST is fine, but #3409 means they are not. We could make
	// a non-empty sst by copying something outside the span, but #3907 means that
	// the empty virtual span would still be a problem, so don't bother.
	if len(blocks) < 1 {
		return 0, ErrEmptySpan
	}

	// Copy all blocks byte-for-byte without doing any per-key processing.
	var blocksNotInCache []indexEntry

	for i := range blocks {
		cv := r.blockReader.GetFromCache(blocks[i].bh.Handle)
		if cv == nil {
			// Cache miss. Add this block to the list of blocks that are not in cache.
			blocksNotInCache = blocks[i-len(blocksNotInCache) : i+1]
			continue
		}

		// Cache hit.
		rh.RecordCacheHit(ctx, int64(blocks[i].bh.Offset), int64(blocks[i].bh.Length+block.TrailerLen))
		if len(blocksNotInCache) > 0 {
			// We have some blocks that were not in cache preceding this block.
			// Copy them using objstorage.Copy.
			if err := w.copyDataBlocks(ctx, blocksNotInCache, rh); err != nil {
				cv.Release()
				return 0, err
			}
			blocksNotInCache = nil
		}

		err := w.addDataBlock(block.CacheBufferHandle(cv).BlockData(), blocks[i].sep, blocks[i].bh)
		cv.Release()
		if err != nil {
			return 0, err
		}
	}

	if len(blocksNotInCache) > 0 {
		// We have some remaining blocks that were not in cache. Copy them
		// using objstorage.Copy.
		if err := w.copyDataBlocks(ctx, blocksNotInCache, rh); err != nil {
			return 0, err
		}
		blocksNotInCache = nil
	}

	// TODO(dt): Copy range keys (the fact there are none is checked above).
	// TODO(dt): Copy valblocks keys (the fact there are none is checked above).

	if err := w.Close(); err != nil {
		w = nil
		return 0, err
	}
	meta, err := w.Metadata()
	if err != nil {
		return 0, err
	}
	wrote := meta.Size
	w = nil
	return wrote, nil
}

// ErrEmptySpan is returned by CopySpan if the input sstable has no keys in the
// requested span.
//
// Note that CopySpan's determination of block overlap is best effort - we may
// copy a block that doesn't actually contain any keys in the span, in which
// case we won't generate this error. We currently only generate this error when
// the span start is beyond all keys in the physical sstable.
var ErrEmptySpan = errors.New("cannot copy empty span")

// indexEntry captures the two components of an sst index entry: the key and the
// decoded block handle value.
type indexEntry struct {
	sep []byte
	bh  block.HandleWithProperties
}

// intersectingIndexEntries returns the entries from the index with separator
// keys contained by [start, end), i.e. the subset of the sst's index that
// intersects the provided span.
func intersectingIndexEntries(
	ctx context.Context,
	r *Reader,
	rh objstorage.ReadHandle,
	indexH block.BufferHandle,
	start, end InternalKey,
) ([]indexEntry, error) {
	top := r.tableFormat.newIndexIter()
	err := top.Init(r.Comparer, indexH.BlockData(), NoTransforms)
	if err != nil {
		return nil, err
	}
	defer func() { _ = top.Close() }()

	var alloc bytealloc.A
	res := make([]indexEntry, 0, r.Properties.NumDataBlocks)
	for valid := top.SeekGE(start.UserKey); valid; valid = top.Next() {
		bh, err := top.BlockHandleWithProperties()
		if err != nil {
			return nil, err
		}
		if r.Properties.IndexType != twoLevelIndex {
			entry := indexEntry{bh: bh, sep: top.Separator()}
			alloc, entry.bh.Props = alloc.Copy(entry.bh.Props)
			alloc, entry.sep = alloc.Copy(entry.sep)
			res = append(res, entry)
		} else {
			err := func() error {
				subBlk, err := r.readIndexBlock(ctx, block.NoReadEnv, rh, bh.Handle)
				if err != nil {
					return err
				}
				defer subBlk.Release()

				sub := r.tableFormat.newIndexIter()
				err = sub.Init(r.Comparer, subBlk.BlockData(), NoTransforms)
				if err != nil {
					return err
				}
				defer func() { _ = sub.Close() }()

				for valid := sub.SeekGE(start.UserKey); valid; valid = sub.Next() {
					bh, err := sub.BlockHandleWithProperties()
					if err != nil {
						return err
					}
					entry := indexEntry{bh: bh, sep: sub.Separator()}
					alloc, entry.bh.Props = alloc.Copy(entry.bh.Props)
					alloc, entry.sep = alloc.Copy(entry.sep)
					res = append(res, entry)
					if r.Comparer.Compare(end.UserKey, entry.sep) <= 0 {
						break
					}
				}
				return nil
			}()
			if err != nil {
				return nil, err
			}
		}
		if top.SeparatorGT(end.UserKey, true /* inclusively */) {
			break
		}
	}
	return res, nil
}

// copyWholeFileBecauseOfUnsupportedFeature is a thin wrapper around Copy that
// exists to ensure it is visible in profiles/stack traces if we are looking at
// cluster copying more than expected.
//
// Finishes or Aborts output; does *not* Close input.
func copyWholeFileBecauseOfUnsupportedFeature(
	ctx context.Context, input objstorage.Readable, output objstorage.Writable,
) (size uint64, _ error) {
	length := uint64(input.Size())
	rh := input.NewReadHandle(objstorage.NoReadBefore)
	rh.SetupForCompaction()
	if err := objstorage.Copy(ctx, rh, output, 0, length); err != nil {
		output.Abort()
		return 0, err
	}
	return length, output.Finish()
}
