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
	// TODO(bilal): Roll the LevelIter allocation into it.rangeKey.iterConfig.
	levelIters := make([]keyspan.LevelIter, 0)
	// Next are the file levels: L0 sub-levels followed by lower levels.
	addLevelIterForFiles := func(files manifest.LevelIterator, level manifest.Level) {
		rangeIter := files.Filter(manifest.KeyTypeRange)
		if rangeIter.First() == nil {
			// No files with range keys.
			return
		}
		levelIters = append(levelIters, keyspan.LevelIter{})
		li := &levelIters[len(levelIters)-1]
		spanIterOpts := keyspan.SpanIterOptions{RangeKeyFilters: it.opts.RangeKeyFilters}

		li.Init(spanIterOpts, it.cmp, d.tableNewRangeKeyIter, files, level, d.opts.Logger, manifest.KeyTypeRange)
		it.rangeKey.iterConfig.AddLevel(li)
	}

	// Add level iterators for the L0 sublevels, iterating from newest to
	// oldest.
	for i := len(current.L0SublevelFiles) - 1; i >= 0; i-- {
		addLevelIterForFiles(current.L0SublevelFiles[i].Iter(), manifest.L0Sublevel(i))
	}

	// Add level iterators for the non-empty non-L0 levels.
	for level := 1; level < len(current.Levels); level++ {
		if current.Levels[level].Empty() {
			continue
		}
		addLevelIterForFiles(current.Levels[level].Iter(), manifest.Level(level))
	}
	return it.rangeKey.rangeKeyIter
}
