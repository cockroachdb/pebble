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

// applyBatchRangeKeys is a temporary hack to support in-memory only range keys.
// We use much of the same code that we will use for the memtable, but we use a
// separate arena that exists beyond the lifetime of any individual memtable.
// For as long as we're relying on this hack, a single *pebble.DB may only store
// as many range keys as fit in this arena.
func (d *DB) applyBatchRangeKeys(b *Batch) error {
	d.maybeInitializeRangeKeys()

	seqNum := b.SeqNum()
	startSeqNum := seqNum
	for r := b.Reader(); ; seqNum++ {
		kind, ukey, value, ok := r.Next()
		if !ok {
			break
		}
		switch kind {
		case InternalKeyKindLogData:
			// Don't increment seqNum for LogData, since these are not applied
			// to the memtable.
			seqNum--
			continue
		case InternalKeyKindRangeKeySet, InternalKeyKindRangeKeyUnset, InternalKeyKindRangeKeyDelete:
			ikey := base.MakeInternalKey(ukey, seqNum, kind)
			if err := d.rangeKeys.skl.Add(ikey, value); err != nil {
				return err
			}
		default:
			// This method only applies range keys; ignore all other key kinds.
		}
	}
	if seqNum != startSeqNum+uint64(b.Count()) {
		return base.CorruptionErrorf("pebble: inconsistent batch count: %d vs %d",
			errors.Safe(seqNum), errors.Safe(startSeqNum+uint64(b.Count())))
	}
	d.rangeKeys.fragCache.invalidate(uint32(b.countRangeKeys))
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
			cmp:       d.cmp,
			formatKey: d.opts.Comparer.FormatKey,
			skl:       &d.rangeKeys.skl,
			splitValue: func(kind base.InternalKeyKind, rawValue []byte) (endKey []byte, value []byte, err error) {
				var ok bool
				endKey, value, ok = rangekey.DecodeEndKey(kind, rawValue)
				if !ok {
					err = base.CorruptionErrorf("unable to decode end key for key of kind %s", kind)
				}
				// NB: This value encodes a series of (suffix,value) tuples.
				return endKey, value, err
			},
		}
	})
}

func (d *DB) newRangeKeyIter(
	seqNum uint64, batch *Batch, readState *readState, opts *IterOptions,
) *rangekey.Iter {
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

	iter := &rangekey.Iter{}
	miter := &rangekey.MergingIter{}
	miter.Init(d.cmp, iters...)
	iter.Init(d.cmp, d.opts.Comparer.FormatKey, seqNum, miter)
	return iter
}
