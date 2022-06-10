// Copyright 2021 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package pebble

import (
	"sync"

	"github.com/cockroachdb/pebble/internal/arenaskl"
	"github.com/cockroachdb/pebble/internal/keyspan"
	"github.com/cockroachdb/pebble/internal/manifest"
	"github.com/cockroachdb/pebble/internal/rangekey"
)

const rangeKeyArenaSize = 1 << 20

// RangeKeysArena is an in-memory arena in which range keys are stored.
//
// This is a temporary type that will eventually be removed.
//
// TODO(bilal): This type should mostly be unused now. Clean up the last few
// uses and remove it.
type RangeKeysArena struct {
	once      sync.Once
	skl       arenaskl.Skiplist
	arena     *arenaskl.Arena
	fragCache keySpanCache
}

func (d *DB) maybeInitializeRangeKeys() {
	// Lazily construct the global range key arena, so that tests that
	// don't use range keys don't need to allocate this long-lived
	// buffer.
	d.rangeKeys.once.Do(func() {
		arenaBuf := make([]byte, rangeKeyArenaSize)
		d.rangeKeys.arena = arenaskl.NewArena(arenaBuf)
		d.rangeKeys.skl.Reset(d.rangeKeys.arena, d.cmp)
		d.rangeKeys.fragCache = keySpanCache{
			cmp:           d.cmp,
			formatKey:     d.opts.Comparer.FormatKey,
			skl:           &d.rangeKeys.skl,
			constructSpan: rangekey.Decode,
		}
	})
}

func (d *DB) newRangeKeyIter(
	it *Iterator, seqNum, batchSeqNum uint64, batch *Batch, readState *readState,
) keyspan.FragmentIterator {
	d.maybeInitializeRangeKeys()
	it.rangeKey.rangeKeyIter = it.rangeKey.iterConfig.Init(d.cmp, seqNum)

	// If there's an indexed batch with range keys, include it.
	if batch != nil {
		if batch.index == nil {
			it.rangeKey.iterConfig.AddLevel(newErrorKeyspanIter(ErrNotIndexed))
		} else {
			batch.initRangeKeyIter(&it.opts, &it.batchRangeKeyIter, batchSeqNum)
			it.rangeKey.iterConfig.AddLevel(&it.batchRangeKeyIter)
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
