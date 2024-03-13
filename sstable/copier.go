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
)

// CopySpan produces a copy of a approximate subset of an input sstable.
//
// The produced sstable contains all keys from the input sstable in the span
// [start, end), as well as potentially some additional keys from the original
// file that were adjacent to but outside that span.
//
// CopyAtLeastSpan differs from simply seeking a reader to start and iterating a
// until end passing the results to a writer in that it does not write the new
// sstable from scratch, key-by-key, recompressing each key nto new blocks and
// computing new filters and properties. Instead finds _block_ that intersect
// the requested span and copies those, whole, to the new file, avoiding all
// decompression and recompression work. It then copies the original filter as
// these can serve as a filter for the subset as well, just with potentially a
// higher false positive rate than one computed just from the they keys in it.
//
// Closes input and finishes or aborts output, including on non-nil errors.
func CopySpan(
	ctx context.Context,
	input objstorage.Readable,
	rOpts ReaderOptions,
	output objstorage.Writable,
	o WriterOptions,
	start, end InternalKey,
) (uint64, error) {
	r, err := NewReader(input, rOpts)
	if err != nil {
		input.Close()
		output.Abort()
		return 0, err
	}
	defer r.Close() // r.Close now owns calling input.Close().

	if r.Properties.NumValueBlocks > 0 || r.Properties.NumRangeKeys() > 0 {
		return 0, errors.New("sstable value blocks or range keys")
	}

	// If our input has not filters, our output cannot have filters either.
	if r.filterBH.Offset == 0 {
		o.FilterPolicy = nil
	}

	w := NewWriter(output, o)
	defer func() {
		if w != nil {
			// set w.err to any non-nil error just so it aborts instead of finishing.
			w.err = base.ErrNotFound
			// w.Close now owns calling output.Abort() or output.Finish().
			w.Close()
		}
	}()

	if w.filter != nil {
		if err := checkWriterFilterMatchesReader(r, w); err != nil {
			return 0, err
		}
	}

	blocks, err := intersectingIndex(ctx, r, start, end)
	if err != nil {
		return 0, err
	}
	if len(blocks) > 0 {
		offset := blocks[0].bh.Offset
		length := blocks[len(blocks)-1].bh.Offset + blocks[len(blocks)-1].bh.Length + blockTrailerLen - offset

		if err := objstorage.Copy(ctx, r.readable, w.writable, offset, length); err != nil {
			return 0, err
		}
		w.meta.Size += length

		for i := range blocks {
			blocks[i].bh.Offset -= offset
			var next InternalKey
			if i+1 < len(blocks) {
				next = blocks[i+1].key
			}
			if err := w.addIndexEntrySync(blocks[i].key, next, blocks[i].bh, w.dataBlockBuf.tmp[:]); err != nil {
				return 0, err
			}
		}

		// TODO(dt): range keys.

		// TODO(dt): value blocks.

		// NB: Some of these -- such as total keys -- will be over-counts since they
		// include the blocks we didn't copy, but over-estimating is safer than under,
		// e.g. if we tried to scale based on fraction of blocks copied.
		w.props = r.Properties

		// Copy over the filter block if it exists.
		if w.filter != nil && r.filterBH.Length > 0 {
			filterBlock, _, err := readBlockBuf(r, r.filterBH, nil)
			if err != nil {
				return 0, errors.Wrap(err, "reading filter")
			}
			w.filter = copyFilterWriter{
				origPolicyName: w.filter.policyName(), origMetaName: w.filter.metaName(), data: filterBlock,
			}
		}
	}

	if err := w.Close(); err != nil {
		w = nil
		return 0, err
	}
	wrote := w.meta.Size
	w = nil
	return wrote, err
}

type indexEntry struct {
	bh  BlockHandleWithProperties
	key InternalKey
}

func intersectingIndex(ctx context.Context, r *Reader, start, end InternalKey) ([]indexEntry, error) {
	indexH, err := r.readIndex(ctx, nil, nil)
	if err != nil {
		return nil, err
	}
	defer indexH.Release()
	top, err := newBlockIter(r.Compare, r.Split, indexH.Get(), NoTransforms)
	if err != nil {
		return nil, err
	}
	defer top.Close()

	var alloc bytealloc.A
	res := make([]indexEntry, 0, r.Properties.NumDataBlocks)
	for key, value := top.SeekGE(start.UserKey, base.SeekGEFlagsNone); key != nil; key, value = top.Next() {
		bh, err := decodeBlockHandleWithProperties(value.InPlaceValue())
		if err != nil {
			return nil, err
		}
		if r.Properties.IndexType != twoLevelIndex {
			entry := indexEntry{bh: bh, key: *key}
			alloc, entry.bh.Props = alloc.Copy(entry.bh.Props)
			alloc, entry.key.UserKey = alloc.Copy(entry.key.UserKey)
			res = append(res, entry)
		} else {
			subBlk, err := r.readBlock(ctx, bh.BlockHandle, nil, nil, nil, nil, nil)
			if err != nil {
				return nil, err
			}
			defer subBlk.Release() // in-loop, but it is a short loop.

			sub, err := newBlockIter(r.Compare, r.Split, subBlk.Get(), NoTransforms)
			if err != nil {
				return nil, err
			}
			defer sub.Close() // in-loop, but it is a short loop.

			for key, value := sub.SeekGE(start.UserKey, base.SeekGEFlagsNone); key != nil; key, value = sub.Next() {
				bh, err := decodeBlockHandleWithProperties(value.InPlaceValue())
				if err != nil {
					return nil, err
				}
				entry := indexEntry{bh: bh, key: *key}
				alloc, entry.bh.Props = alloc.Copy(entry.bh.Props)
				alloc, entry.key.UserKey = alloc.Copy(entry.key.UserKey)
				res = append(res, entry)
				if base.InternalCompare(r.Compare, end, *key) < 1 {
					break
				}
			}
			if err := sub.Error(); err != nil {
				return nil, err
			}
		}
		if base.InternalCompare(r.Compare, end, *key) < 1 {
			break
		}
	}
	return res, top.Error()
}
