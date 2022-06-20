// Copyright 2021 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package pebble

import (
	"github.com/cockroachdb/pebble/internal/keyspan"
	"github.com/cockroachdb/pebble/internal/manifest"
)

func (d *DB) newRangeKeyIter(
	it *Iterator, seqNum, batchSeqNum uint64, batch *Batch, readState *readState,
) keyspan.FragmentIterator {
	it.rangeKey.rangeKeyIter = it.rangeKey.iterConfig.Init(d.cmp, seqNum)

	// If there's an indexed batch with range keys, include it.
	if batch != nil {
		if batch.index == nil {
			it.rangeKey.iterConfig.AddLevel(newErrorKeyspanIter(ErrNotIndexed))
		} else {
			// Only include the batch's range key iterator if it has any keys.
			// NB: This can force reconstruction of the rangekey iterator stack
			// in SetOptions if subsequently range keys are added. See
			// SetOptions.
			if batch.countRangeKeys > 0 {
				batch.initRangeKeyIter(&it.opts, &it.batchRangeKeyIter, batchSeqNum)
				it.rangeKey.iterConfig.AddLevel(&it.batchRangeKeyIter)
			}
		}
	}

	// Next are the flushables: memtables and large batches.
	for i := len(readState.memtables) - 1; i >= 0; i-- {
		mem := readState.memtables[i]
		// We only need to read from memtables which contain sequence numbers older
		// than seqNum.
		if logSeqNum := mem.logSeqNum; logSeqNum >= seqNum {
			continue
		}
		if rki := mem.newRangeKeyIter(&it.opts); rki != nil {
			it.rangeKey.iterConfig.AddLevel(rki)
		}
	}

	current := readState.current
	// Next are the file levels: L0 sub-levels followed by lower levels.
	//
	// Add file-specific iterators for L0 files containing range keys. This is less
	// efficient than using levelIters for sublevels of L0 files containing
	// range keys, but range keys are expected to be sparse anyway, reducing the
	// cost benefit of maintaining a separate L0Sublevels instance for range key
	// files and then using it here.
	iter := current.RangeKeyLevels[0].Iter()
	for f := iter.First(); f != nil; f = iter.Next() {
		spanIterOpts := &keyspan.SpanIterOptions{RangeKeyFilters: it.opts.RangeKeyFilters}
		spanIter, err := d.tableNewRangeKeyIter(f, spanIterOpts)
		if err != nil {
			it.rangeKey.iterConfig.AddLevel(&errorKeyspanIter{err: err})
			continue
		}
		it.rangeKey.iterConfig.AddLevel(spanIter)
	}

	// Add level iterators for the non-empty non-L0 levels.
	for level := 1; level < len(current.RangeKeyLevels); level++ {
		if current.RangeKeyLevels[level].Empty() {
			continue
		}
		li := it.rangeKey.iterConfig.NewLevelIter()
		spanIterOpts := keyspan.SpanIterOptions{RangeKeyFilters: it.opts.RangeKeyFilters}

		li.Init(spanIterOpts, it.cmp, d.tableNewRangeKeyIter, current.RangeKeyLevels[level].Iter(),
			manifest.Level(level), d.opts.Logger, manifest.KeyTypeRange)
		it.rangeKey.iterConfig.AddLevel(li)
	}
	return it.rangeKey.rangeKeyIter
}
