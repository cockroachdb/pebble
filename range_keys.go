// Copyright 2021 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package pebble

import (
	"sync"

	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/pebble/internal/arenaskl"
	"github.com/cockroachdb/pebble/internal/base"
	"github.com/cockroachdb/pebble/internal/keyspan"
	"github.com/cockroachdb/pebble/internal/rangekey"
)

const rangeKeyArenaSize = 1 << 20

// RangeKeysArena is an in-memory arena in which range keys are stored.
//
// This is a temporary type that will eventually be removed.
type RangeKeysArena struct {
	once      sync.Once
	skl       arenaskl.Skiplist
	arena     *arenaskl.Arena
	fragCache keySpanCache
}

// applyFlushedRangeKeys is a temporary hack to support in-memory only range
// keys.  We use much of the same code that we will use for the memtable, but we
// use a separate arena that exists beyond the lifetime of any individual
// memtable.  For as long as we're relying on this hack, a single *pebble.DB may
// only store as many range keys as fit in this arena.
//
// TODO(jackson): Remove applyFlushedRangeKeys when range keys are persisted.
func (d *DB) applyFlushedRangeKeys(flushable []*flushableEntry) error {
	var added uint32
	for i := 0; i < len(flushable); i++ {
		iter := flushable[i].newRangeKeyIter(nil)
		if iter == nil {
			continue
		}
		d.maybeInitializeRangeKeys()

		for s := iter.First(); s.Valid(); s = iter.Next() {
			// flushable.newRangeKeyIter provides a FragmentIterator, which
			// iterates over parsed keyspan.Spans.
			//
			// While we're faking a flush, we just want to write the original
			// key into the global arena, so we need to re-encode the span's
			// keys into internal key-value pairs.
			//
			// This awkward recombination will be removed when flushes implement
			// range-key logic, coalescing range keys and constructing internal
			// keys.
			err := rangekey.Encode(s, func(k base.InternalKey, v []byte) error {
				err := d.rangeKeys.skl.Add(k, v)
				switch {
				case err == nil:
					added++
					return nil
				case errors.Is(err, arenaskl.ErrRecordExists):
					// It's possible that we'll try to add a key to the arena twice
					// during metamorphic tests that reset the synced state. Ignore.
					// When range keys are actually flushed to stable storage, this
					// will go away.
					return nil
				default:
					// err != nil
					return err
				}
			})
			if err != nil {
				return err
			}
		}
	}
	if added > 0 {
		d.rangeKeys.fragCache.invalidate(added)
	}
	return nil
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
	it *iteratorRangeKeyState, seqNum uint64, batch *Batch, readState *readState, opts *IterOptions,
) keyspan.FragmentIterator {
	d.maybeInitializeRangeKeys()

	// TODO(jackson): Preallocate iters, mergingIter, rangeKeyIter in a
	// structure analogous to iterAlloc.
	var iters []keyspan.FragmentIterator

	// If there's an indexed batch with range keys, include it.
	if batch != nil {
		if rki := batch.newRangeKeyIter(opts); rki != nil {
			iters = append(iters, rki)
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
		if rki := mem.newRangeKeyIter(opts); rki != nil {
			iters = append(iters, rki)
		}
	}

	// For now while range keys are not fully integrated into Pebble, all range
	// keys ever written to the DB are persisted in the d.rangeKeys arena.
	frags := d.rangeKeys.fragCache.get()
	if len(frags) > 0 {
		iters = append(iters, keyspan.NewIter(d.cmp, frags))
	}
	it.rangeKeyIter = rangekey.InitUserIteration(
		d.cmp, seqNum, &it.alloc.merging, &it.alloc.defraging, iters...,
	)
	return it.rangeKeyIter
}
