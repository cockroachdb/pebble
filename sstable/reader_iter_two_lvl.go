// Copyright 2011 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package sstable

import (
	"bytes"
	"context"
	"fmt"

	"github.com/cockroachdb/pebble/internal/base"
	"github.com/cockroachdb/pebble/internal/invariants"
	"github.com/cockroachdb/pebble/objstorage"
	"github.com/cockroachdb/pebble/objstorage/objstorageprovider"
	"github.com/cockroachdb/pebble/objstorage/objstorageprovider/objiotracing"
	"github.com/cockroachdb/pebble/sstable/block"
)

type twoLevelIterator struct {
	singleLevelIterator
	topLevelIndex blockIter
}

// twoLevelIterator implements the base.InternalIterator interface.
var _ base.InternalIterator = (*twoLevelIterator)(nil)

// loadIndex loads the index block at the current top level index position and
// leaves i.index unpositioned. If unsuccessful, it gets i.err to any error
// encountered, which may be nil if we have simply exhausted the entire table.
// This is used for two level indexes.
func (i *twoLevelIterator) loadIndex(dir int8) loadBlockResult {
	// Ensure the index data block iterators are invalidated even if loading of
	// the index fails.
	i.data.invalidate()
	i.index.invalidate()
	if !i.topLevelIndex.valid() {
		i.index.offset = 0
		i.index.restarts = 0
		return loadBlockFailed
	}
	v := i.topLevelIndex.value()
	bhp, err := decodeBlockHandleWithProperties(v.InPlaceValue())
	if err != nil {
		i.err = base.CorruptionErrorf("pebble/table: corrupt top level index entry")
		return loadBlockFailed
	}
	if i.bpfs != nil {
		intersects, err := i.bpfs.intersects(bhp.Props)
		if err != nil {
			i.err = errCorruptIndexEntry(err)
			return loadBlockFailed
		}
		if intersects == blockMaybeExcluded {
			intersects = i.resolveMaybeExcluded(dir)
		}
		if intersects == blockExcluded {
			return loadBlockIrrelevant
		}
		// blockIntersects
	}
	ctx := objiotracing.WithBlockType(i.ctx, objiotracing.MetadataBlock)
	indexBlock, err := i.reader.readBlock(
		ctx, bhp.BlockHandle, nil /* transform */, i.indexFilterRH, i.stats, &i.iterStats, i.bufferPool)
	if err != nil {
		i.err = err
		return loadBlockFailed
	}
	if i.err = i.index.initHandle(i.cmp, i.reader.Split, indexBlock, i.transforms); i.err == nil {
		return loadBlockOK
	}
	return loadBlockFailed
}

// resolveMaybeExcluded is invoked when the block-property filterer has found
// that an index block is excluded according to its properties but only if its
// bounds fall within the filter's current bounds. This function consults the
// apprioriate bound, depending on the iteration direction, and returns either
// `blockIntersects` or `blockExcluded`.
func (i *twoLevelIterator) resolveMaybeExcluded(dir int8) intersectsResult {
	// This iterator is configured with a bound-limited block property filter.
	// The bpf determined this entire index block could be excluded from
	// iteration based on the property encoded in the block handle. However, we
	// still need to determine if the index block is wholly contained within the
	// filter's key bounds.
	//
	// External guarantees ensure all its data blocks' keys are ≥ the filter's
	// lower bound during forward iteration, and that all its data blocks' keys
	// are < the filter's upper bound during backward iteration. We only need to
	// determine if the opposite bound is also met.
	//
	// The index separator in topLevelIndex.Key() provides an inclusive
	// upper-bound for the index block's keys, guaranteeing that all its keys
	// are ≤ topLevelIndex.Key(). For forward iteration, this is all we need.
	if dir > 0 {
		// Forward iteration.
		if i.bpfs.boundLimitedFilter.KeyIsWithinUpperBound(i.topLevelIndex.Key().UserKey) {
			return blockExcluded
		}
		return blockIntersects
	}

	// Reverse iteration.
	//
	// Because we're iterating in the reverse direction, we don't yet have
	// enough context available to determine if the block is wholly contained
	// within its bounds. This case arises only during backward iteration,
	// because of the way the index is structured.
	//
	// Consider a bound-limited bpf limited to the bounds [b,d), loading the
	// block with separator `c`. During reverse iteration, the guarantee that
	// all the block's keys are < `d` is externally provided, but no guarantee
	// is made on the bpf's lower bound. The separator `c` only provides an
	// inclusive upper bound on the block's keys, indicating that the
	// corresponding block handle points to a block containing only keys ≤ `c`.
	//
	// To establish a lower bound, we step the top-level index backwards to read
	// the previous block's separator, which provides an inclusive lower bound
	// on the original index block's keys. Afterwards, we step forward to
	// restore our top-level index position.
	if peekKey := i.topLevelIndex.Prev(); peekKey == nil {
		// The original block points to the first index block of this table. If
		// we knew the lower bound for the entire table, it could provide a
		// lower bound, but the code refactoring necessary to read it doesn't
		// seem worth the payoff. We fall through to loading the block.
	} else if i.bpfs.boundLimitedFilter.KeyIsWithinLowerBound(peekKey.K.UserKey) {
		// The lower-bound on the original index block falls within the filter's
		// bounds, and we can skip the block (after restoring our current
		// top-level index position).
		_ = i.topLevelIndex.Next()
		return blockExcluded
	}
	_ = i.topLevelIndex.Next()
	return blockIntersects
}

// Note that lower, upper passed into init has nothing to do with virtual sstable
// bounds. If the virtualState passed in is not nil, then virtual sstable bounds
// will be enforced.
func (i *twoLevelIterator) init(
	ctx context.Context,
	r *Reader,
	v *virtualState,
	transforms IterTransforms,
	lower, upper []byte,
	filterer *BlockPropertiesFilterer,
	useFilter bool,
	stats *base.InternalIteratorStats,
	categoryAndQoS CategoryAndQoS,
	statsCollector *CategoryStatsCollector,
	rp ReaderProvider,
	bufferPool *block.BufferPool,
) error {
	if r.err != nil {
		return r.err
	}
	i.iterStats.init(categoryAndQoS, statsCollector)
	i.indexFilterRH = objstorageprovider.UsePreallocatedReadHandle(
		ctx, r.readable, objstorage.ReadBeforeForIndexAndFilter, &i.indexFilterRHPrealloc)
	topLevelIndexH, err := r.readIndex(ctx, i.indexFilterRH, stats, &i.iterStats)
	if err != nil {
		return err
	}
	if v != nil {
		i.vState = v
		// Note that upper is exclusive here.
		i.endKeyInclusive, lower, upper = v.constrainBounds(lower, upper, false /* endInclusive */)
	}

	i.inPool = false
	i.ctx = ctx
	i.lower = lower
	i.upper = upper
	i.bpfs = filterer
	i.useFilter = useFilter
	i.reader = r
	i.cmp = r.Compare
	i.stats = stats
	i.transforms = transforms
	i.bufferPool = bufferPool
	err = i.topLevelIndex.initHandle(i.cmp, i.reader.Split, topLevelIndexH, transforms)
	if err != nil {
		// blockIter.Close releases topLevelIndexH and always returns a nil error
		_ = i.topLevelIndex.Close()
		return err
	}
	i.dataRH = objstorageprovider.UsePreallocatedReadHandle(
		ctx, r.readable, objstorage.NoReadBefore, &i.dataRHPrealloc)
	if r.tableFormat >= TableFormatPebblev3 {
		if r.Properties.NumValueBlocks > 0 {
			i.vbReader = &valueBlockReader{
				bpOpen: i,
				rp:     rp,
				vbih:   r.valueBIH,
				stats:  stats,
			}
			i.data.lazyValueHandling.vbr = i.vbReader
			i.vbRH = objstorageprovider.UsePreallocatedReadHandle(
				ctx, r.readable, objstorage.NoReadBefore, &i.vbRHPrealloc)
		}
		i.data.lazyValueHandling.hasValuePrefix = true
	}
	return nil
}

func (i *twoLevelIterator) String() string {
	if i.vState != nil {
		return i.vState.fileNum.String()
	}
	return i.reader.fileNum.String()
}

// SeekGE implements internalIterator.SeekGE, as documented in the pebble
// package. Note that SeekGE only checks the upper bound. It is up to the
// caller to ensure that key is greater than or equal to the lower bound.
func (i *twoLevelIterator) SeekGE(key []byte, flags base.SeekGEFlags) *base.InternalKV {
	if i.vState != nil {
		// Callers of SeekGE don't know about virtual sstable bounds, so we may
		// have to internally restrict the bounds.
		//
		// TODO(bananabrick): We can optimize away this check for the level iter
		// if necessary.
		if i.cmp(key, i.lower) < 0 {
			key = i.lower
		}
	}

	err := i.err
	i.err = nil // clear cached iteration error

	// The twoLevelIterator could be already exhausted. Utilize that when
	// trySeekUsingNext is true. See the comment about data-exhausted, PGDE, and
	// bounds-exhausted near the top of the file.
	if flags.TrySeekUsingNext() &&
		(i.exhaustedBounds == +1 || (i.data.isDataInvalidated() && i.index.isDataInvalidated())) &&
		err == nil {
		// Already exhausted, so return nil.
		return nil
	}

	// SeekGE performs various step-instead-of-seeking optimizations: eg enabled
	// by trySeekUsingNext, or by monotonically increasing bounds (i.boundsCmp).

	// We fall into the slow path if i.index.isDataInvalidated() even if the
	// top-level iterator is already positioned correctly and all other
	// conditions are met. An alternative structure could reuse topLevelIndex's
	// current position and reload the index block to which it points. Arguably,
	// an index block load is expensive and the index block may still be earlier
	// than the index block containing the sought key, resulting in a wasteful
	// block load.

	var dontSeekWithinSingleLevelIter bool
	if i.topLevelIndex.isDataInvalidated() || !i.topLevelIndex.valid() || i.index.isDataInvalidated() || err != nil ||
		(i.boundsCmp <= 0 && !flags.TrySeekUsingNext()) || i.cmp(key, i.topLevelIndex.Key().UserKey) > 0 {
		// Slow-path: need to position the topLevelIndex.

		// The previous exhausted state of singleLevelIterator is no longer
		// relevant, since we may be moving to a different index block.
		i.exhaustedBounds = 0
		flags = flags.DisableTrySeekUsingNext()
		var ikv *base.InternalKV
		if ikv = i.topLevelIndex.SeekGE(key, flags); ikv == nil {
			i.data.invalidate()
			i.index.invalidate()
			return nil
		}

		result := i.loadIndex(+1)
		if result == loadBlockFailed {
			i.boundsCmp = 0
			return nil
		}
		if result == loadBlockIrrelevant {
			// Enforce the upper bound here since don't want to bother moving
			// to the next entry in the top level index if upper bound is
			// already exceeded. Note that the next entry starts with keys >=
			// ikey.InternalKey.UserKey since even though this is the block separator, the
			// same user key can span multiple index blocks. If upper is
			// exclusive we use >= below, else we use >.
			if i.upper != nil {
				cmp := i.cmp(ikv.K.UserKey, i.upper)
				if (!i.endKeyInclusive && cmp >= 0) || cmp > 0 {
					i.exhaustedBounds = +1
				}
			}
			// Fall through to skipForward.
			dontSeekWithinSingleLevelIter = true
			// Clear boundsCmp.
			//
			// In the typical cases where dontSeekWithinSingleLevelIter=false,
			// the singleLevelIterator.SeekGE call will clear boundsCmp.
			// However, in this case where dontSeekWithinSingleLevelIter=true,
			// we never seek on the single-level iterator. This call will fall
			// through to skipForward, which may improperly leave boundsCmp=+1
			// unless we clear it here.
			i.boundsCmp = 0
		}
	} else {
		// INVARIANT: err == nil.
		//
		// Else fast-path: There are two possible cases, from
		// (i.boundsCmp > 0 || flags.TrySeekUsingNext()):
		//
		// 1) The bounds have moved forward (i.boundsCmp > 0) and this SeekGE is
		// respecting the lower bound (guaranteed by Iterator). We know that the
		// iterator must already be positioned within or just outside the previous
		// bounds. Therefore, the topLevelIndex iter cannot be positioned at an
		// entry ahead of the seek position (though it can be positioned behind).
		// The !i.cmp(key, i.topLevelIndex.Key().UserKey) > 0 confirms that it is
		// not behind. Since it is not ahead and not behind it must be at the
		// right position.
		//
		// 2) This SeekGE will land on a key that is greater than the key we are
		// currently at (guaranteed by trySeekUsingNext), but since i.cmp(key,
		// i.topLevelIndex.Key().UserKey) <= 0, we are at the correct lower level
		// index block. No need to reset the state of singleLevelIterator.
		//
		// Note that cases 1 and 2 never overlap, and one of them must be true.
		// This invariant checking is important enough that we do not gate it
		// behind invariants.Enabled.
		if i.boundsCmp > 0 == flags.TrySeekUsingNext() {
			panic(fmt.Sprintf("inconsistency in optimization case 1 %t and case 2 %t",
				i.boundsCmp > 0, flags.TrySeekUsingNext()))
		}

		if !flags.TrySeekUsingNext() {
			// Case 1. Bounds have changed so the previous exhausted bounds state is
			// irrelevant.
			// WARNING-data-exhausted: this is safe to do only because the monotonic
			// bounds optimizations only work when !data-exhausted. If they also
			// worked with data-exhausted, we have made it unclear whether
			// data-exhausted is actually true. See the comment at the top of the
			// file.
			i.exhaustedBounds = 0
		}
		// Else flags.TrySeekUsingNext(). The i.exhaustedBounds is important to
		// preserve for singleLevelIterator, and twoLevelIterator.skipForward. See
		// bug https://github.com/cockroachdb/pebble/issues/2036.
	}

	if !dontSeekWithinSingleLevelIter {
		// Note that while trySeekUsingNext could be false here, singleLevelIterator
		// could do its own boundsCmp-based optimization to seek using next.
		if ikv := i.singleLevelIterator.SeekGE(key, flags); ikv != nil {
			return ikv
		}
	}
	return i.skipForward()
}

// SeekPrefixGE implements internalIterator.SeekPrefixGE, as documented in the
// pebble package. Note that SeekPrefixGE only checks the upper bound. It is up
// to the caller to ensure that key is greater than or equal to the lower bound.
func (i *twoLevelIterator) SeekPrefixGE(
	prefix, key []byte, flags base.SeekGEFlags,
) *base.InternalKV {
	if i.vState != nil {
		// Callers of SeekGE don't know about virtual sstable bounds, so we may
		// have to internally restrict the bounds.
		//
		// TODO(bananabrick): We can optimize away this check for the level iter
		// if necessary.
		if i.cmp(key, i.lower) < 0 {
			key = i.lower
		}
	}

	// NOTE: prefix is only used for bloom filter checking and not later work in
	// this method. Hence, we can use the existing iterator position if the last
	// SeekPrefixGE did not fail bloom filter matching.

	err := i.err
	i.err = nil // clear cached iteration error

	// The twoLevelIterator could be already exhausted. Utilize that when
	// trySeekUsingNext is true. See the comment about data-exhausted, PGDE, and
	// bounds-exhausted near the top of the file.
	filterUsedAndDidNotMatch :=
		i.reader.tableFilter != nil && i.useFilter && !i.lastBloomFilterMatched
	if flags.TrySeekUsingNext() && !filterUsedAndDidNotMatch &&
		(i.exhaustedBounds == +1 || (i.data.isDataInvalidated() && i.index.isDataInvalidated())) &&
		err == nil {
		// Already exhausted, so return nil.
		return nil
	}

	// Check prefix bloom filter.
	if i.reader.tableFilter != nil && i.useFilter {
		if !i.lastBloomFilterMatched {
			// Iterator is not positioned based on last seek.
			flags = flags.DisableTrySeekUsingNext()
		}
		i.lastBloomFilterMatched = false
		var mayContain bool
		mayContain, i.err = i.bloomFilterMayContain(prefix)
		if i.err != nil || !mayContain {
			// In the i.err == nil case, this invalidation may not be necessary for
			// correctness, and may be a place to optimize later by reusing the
			// already loaded block. It was necessary in earlier versions of the code
			// since the caller was allowed to call Next when SeekPrefixGE returned
			// nil. This is no longer allowed.
			i.data.invalidate()
			return nil
		}
		i.lastBloomFilterMatched = true
	}

	// Bloom filter matches.

	// SeekPrefixGE performs various step-instead-of-seeking optimizations: eg
	// enabled by trySeekUsingNext, or by monotonically increasing bounds
	// (i.boundsCmp).

	// We fall into the slow path if i.index.isDataInvalidated() even if the
	// top-level iterator is already positioned correctly and all other
	// conditions are met. An alternative structure could reuse topLevelIndex's
	// current position and reload the index block to which it points. Arguably,
	// an index block load is expensive and the index block may still be earlier
	// than the index block containing the sought key, resulting in a wasteful
	// block load.

	var dontSeekWithinSingleLevelIter bool
	if i.topLevelIndex.isDataInvalidated() || !i.topLevelIndex.valid() || i.index.isDataInvalidated() || err != nil ||
		(i.boundsCmp <= 0 && !flags.TrySeekUsingNext()) || i.cmp(key, i.topLevelIndex.Key().UserKey) > 0 {
		// Slow-path: need to position the topLevelIndex.

		// The previous exhausted state of singleLevelIterator is no longer
		// relevant, since we may be moving to a different index block.
		i.exhaustedBounds = 0
		flags = flags.DisableTrySeekUsingNext()
		var ikv *base.InternalKV
		if ikv = i.topLevelIndex.SeekGE(key, flags); ikv == nil {
			i.data.invalidate()
			i.index.invalidate()
			return nil
		}

		result := i.loadIndex(+1)
		if result == loadBlockFailed {
			i.boundsCmp = 0
			return nil
		}
		if result == loadBlockIrrelevant {
			// Enforce the upper bound here since don't want to bother moving
			// to the next entry in the top level index if upper bound is
			// already exceeded. Note that the next entry starts with keys >=
			// ikey.InternalKey.UserKey since even though this is the block separator, the
			// same user key can span multiple index blocks. If upper is
			// exclusive we use >= below, else we use >.
			if i.upper != nil {
				cmp := i.cmp(ikv.K.UserKey, i.upper)
				if (!i.endKeyInclusive && cmp >= 0) || cmp > 0 {
					i.exhaustedBounds = +1
				}
			}
			// Fall through to skipForward.
			dontSeekWithinSingleLevelIter = true
			// Clear boundsCmp.
			//
			// In the typical cases where dontSeekWithinSingleLevelIter=false,
			// the singleLevelIterator.SeekPrefixGE call will clear boundsCmp.
			// However, in this case where dontSeekWithinSingleLevelIter=true,
			// we never seek on the single-level iterator. This call will fall
			// through to skipForward, which may improperly leave boundsCmp=+1
			// unless we clear it here.
			i.boundsCmp = 0
		}
	} else {
		// INVARIANT: err == nil.
		//
		// Else fast-path: There are two possible cases, from
		// (i.boundsCmp > 0 || flags.TrySeekUsingNext()):
		//
		// 1) The bounds have moved forward (i.boundsCmp > 0) and this
		// SeekPrefixGE is respecting the lower bound (guaranteed by Iterator). We
		// know that the iterator must already be positioned within or just
		// outside the previous bounds. Therefore, the topLevelIndex iter cannot
		// be positioned at an entry ahead of the seek position (though it can be
		// positioned behind). The !i.cmp(key, i.topLevelIndex.Key().UserKey) > 0
		// confirms that it is not behind. Since it is not ahead and not behind it
		// must be at the right position.
		//
		// 2) This SeekPrefixGE will land on a key that is greater than the key we
		// are currently at (guaranteed by trySeekUsingNext), but since i.cmp(key,
		// i.topLevelIndex.Key().UserKey) <= 0, we are at the correct lower level
		// index block. No need to reset the state of singleLevelIterator.
		//
		// Note that cases 1 and 2 never overlap, and one of them must be true.
		// This invariant checking is important enough that we do not gate it
		// behind invariants.Enabled.
		if i.boundsCmp > 0 == flags.TrySeekUsingNext() {
			panic(fmt.Sprintf("inconsistency in optimization case 1 %t and case 2 %t",
				i.boundsCmp > 0, flags.TrySeekUsingNext()))
		}

		if !flags.TrySeekUsingNext() {
			// Case 1. Bounds have changed so the previous exhausted bounds state is
			// irrelevant.
			// WARNING-data-exhausted: this is safe to do only because the monotonic
			// bounds optimizations only work when !data-exhausted. If they also
			// worked with data-exhausted, we have made it unclear whether
			// data-exhausted is actually true. See the comment at the top of the
			// file.
			i.exhaustedBounds = 0
		}
		// Else flags.TrySeekUsingNext(). The i.exhaustedBounds is important to
		// preserve for singleLevelIterator, and twoLevelIterator.skipForward. See
		// bug https://github.com/cockroachdb/pebble/issues/2036.
	}

	if !dontSeekWithinSingleLevelIter {
		if ikv := i.singleLevelIterator.seekPrefixGE(
			prefix, key, flags, false /* checkFilter */); ikv != nil {
			return ikv
		}
	}
	// NB: skipForward checks whether exhaustedBounds is already +1.
	return i.skipForward()
}

// virtualLast should only be called if i.vReader != nil.
func (i *twoLevelIterator) virtualLast() *base.InternalKV {
	if i.vState == nil {
		panic("pebble: invalid call to virtualLast")
	}
	if !i.endKeyInclusive {
		// Trivial case.
		return i.SeekLT(i.upper, base.SeekLTFlagsNone)
	}
	return i.virtualLastSeekLE()
}

// virtualLastSeekLE implements a SeekLE() that can be used as part
// of reverse-iteration calls such as a Last() on a virtual sstable. Does a
// SeekLE on the upper bound of the file/iterator.
func (i *twoLevelIterator) virtualLastSeekLE() *base.InternalKV {
	// Callers of SeekLE don't know about virtual sstable bounds, so we may
	// have to internally restrict the bounds.
	//
	// TODO(bananabrick): We can optimize this check away for the level iter
	// if necessary.
	if !i.endKeyInclusive {
		panic("unexpected virtualLastSeekLE with exclusive upper bounds")
	}
	key := i.upper
	// Need to position the topLevelIndex.
	//
	// The previous exhausted state of singleLevelIterator is no longer
	// relevant, since we may be moving to a different index block.
	i.exhaustedBounds = 0
	// Seek optimization only applies until iterator is first positioned with a
	// SeekGE or SeekLT after SetBounds.
	i.boundsCmp = 0
	ikv := i.topLevelIndex.SeekGE(key, base.SeekGEFlagsNone)
	// We can have multiple internal keys with the same user key as the seek
	// key. In that case, we want the last (greatest) internal key.
	for ikv != nil && bytes.Equal(ikv.K.UserKey, key) {
		ikv = i.topLevelIndex.Next()
	}
	if ikv == nil {
		return i.skipBackward()
	}
	result := i.loadIndex(-1)
	if result == loadBlockFailed {
		i.boundsCmp = 0
		return nil
	}
	if result == loadBlockIrrelevant {
		// Load the previous block.
		return i.skipBackward()
	}
	if ikv := i.singleLevelIterator.virtualLastSeekLE(); ikv != nil {
		return ikv
	}
	return i.skipBackward()
}

// SeekLT implements internalIterator.SeekLT, as documented in the pebble
// package. Note that SeekLT only checks the lower bound. It is up to the
// caller to ensure that key is less than the upper bound.
func (i *twoLevelIterator) SeekLT(key []byte, flags base.SeekLTFlags) *base.InternalKV {
	if i.vState != nil {
		// Might have to fix upper bound since virtual sstable bounds are not
		// known to callers of SeekLT.
		//
		// TODO(bananabrick): We can optimize away this check for the level iter
		// if necessary.
		cmp := i.cmp(key, i.upper)
		// key == i.upper is fine. We'll do the right thing and return the
		// first internal key with user key < key.
		if cmp > 0 {
			return i.virtualLast()
		}
	}

	i.exhaustedBounds = 0
	i.err = nil // clear cached iteration error
	// Seek optimization only applies until iterator is first positioned after SetBounds.
	i.boundsCmp = 0

	var result loadBlockResult
	var ikv *base.InternalKV
	// NB: Unlike SeekGE, we don't have a fast-path here since we don't know
	// whether the topLevelIndex is positioned after the position that would
	// be returned by doing i.topLevelIndex.SeekGE(). To know this we would
	// need to know the index key preceding the current one.
	// NB: If a bound-limited block property filter is configured, it's
	// externally ensured that the filter is disabled (through returning
	// Intersects=false irrespective of the block props provided) during seeks.
	if ikv = i.topLevelIndex.SeekGE(key, base.SeekGEFlagsNone); ikv == nil {
		if ikv = i.topLevelIndex.Last(); ikv == nil {
			i.data.invalidate()
			i.index.invalidate()
			return nil
		}

		result = i.loadIndex(-1)
		if result == loadBlockFailed {
			return nil
		}
		if result == loadBlockOK {
			if ikv := i.singleLevelIterator.lastInternal(); ikv != nil {
				return i.maybeVerifyKey(ikv)
			}
			// Fall through to skipBackward since the singleLevelIterator did
			// not have any blocks that satisfy the block interval
			// constraints, or the lower bound was reached.
		}
		// Else loadBlockIrrelevant, so fall through.
	} else {
		result = i.loadIndex(-1)
		if result == loadBlockFailed {
			return nil
		}
		if result == loadBlockOK {
			if ikv := i.singleLevelIterator.SeekLT(key, flags); ikv != nil {
				return i.maybeVerifyKey(ikv)
			}
			// Fall through to skipBackward since the singleLevelIterator did
			// not have any blocks that satisfy the block interval
			// constraint, or the lower bound was reached.
		}
		// Else loadBlockIrrelevant, so fall through.
	}
	if result == loadBlockIrrelevant {
		// Enforce the lower bound here since don't want to bother moving to
		// the previous entry in the top level index if lower bound is already
		// exceeded. Note that the previous entry starts with keys <=
		// ikey.InternalKey.UserKey since even though this is the current block's
		// separator, the same user key can span multiple index blocks.
		if i.lower != nil && i.cmp(ikv.K.UserKey, i.lower) < 0 {
			i.exhaustedBounds = -1
		}
	}
	// NB: skipBackward checks whether exhaustedBounds is already -1.
	return i.skipBackward()
}

// First implements internalIterator.First, as documented in the pebble
// package. Note that First only checks the upper bound. It is up to the caller
// to ensure that key is greater than or equal to the lower bound (e.g. via a
// call to SeekGE(lower)).
func (i *twoLevelIterator) First() *base.InternalKV {
	// If we have a lower bound, use SeekGE. Note that in general this is not
	// supported usage, except when the lower bound is there because the table is
	// virtual.
	if i.lower != nil {
		return i.SeekGE(i.lower, base.SeekGEFlagsNone)
	}
	i.exhaustedBounds = 0
	i.err = nil // clear cached iteration error
	// Seek optimization only applies until iterator is first positioned after SetBounds.
	i.boundsCmp = 0

	var ikv *base.InternalKV
	if ikv = i.topLevelIndex.First(); ikv == nil {
		return nil
	}

	result := i.loadIndex(+1)
	if result == loadBlockFailed {
		return nil
	}
	if result == loadBlockOK {
		if ikv := i.singleLevelIterator.First(); ikv != nil {
			return ikv
		}
		// Else fall through to skipForward.
	} else {
		// result == loadBlockIrrelevant. Enforce the upper bound here since
		// don't want to bother moving to the next entry in the top level
		// index if upper bound is already exceeded. Note that the next entry
		// starts with keys >= ikv.InternalKey.UserKey since even though this is the
		// block separator, the same user key can span multiple index blocks.
		// If upper is exclusive we use >= below, else we use >.
		if i.upper != nil {
			cmp := i.cmp(ikv.K.UserKey, i.upper)
			if (!i.endKeyInclusive && cmp >= 0) || cmp > 0 {
				i.exhaustedBounds = +1
			}
		}
	}
	// NB: skipForward checks whether exhaustedBounds is already +1.
	return i.skipForward()
}

// Last implements internalIterator.Last, as documented in the pebble
// package. Note that Last only checks the lower bound. It is up to the caller
// to ensure that key is less than the upper bound (e.g. via a call to
// SeekLT(upper))
func (i *twoLevelIterator) Last() *base.InternalKV {
	if i.vState != nil {
		if i.endKeyInclusive {
			return i.virtualLast()
		}
		return i.SeekLT(i.upper, base.SeekLTFlagsNone)
	}

	if i.upper != nil {
		panic("twoLevelIterator.Last() used despite upper bound")
	}
	i.exhaustedBounds = 0
	i.err = nil // clear cached iteration error
	// Seek optimization only applies until iterator is first positioned after SetBounds.
	i.boundsCmp = 0

	var ikv *base.InternalKV
	if ikv = i.topLevelIndex.Last(); ikv == nil {
		return nil
	}

	result := i.loadIndex(-1)
	if result == loadBlockFailed {
		return nil
	}
	if result == loadBlockOK {
		if ikv := i.singleLevelIterator.Last(); ikv != nil {
			return ikv
		}
		// Else fall through to skipBackward.
	} else {
		// result == loadBlockIrrelevant. Enforce the lower bound here since
		// don't want to bother moving to the previous entry in the top level
		// index if lower bound is already exceeded. Note that the previous
		// entry starts with keys <= ikv.InternalKey.UserKey since even though
		// this is the current block's separator, the same user key can span
		// multiple index blocks.
		if i.lower != nil && i.cmp(ikv.K.UserKey, i.lower) < 0 {
			i.exhaustedBounds = -1
		}
	}
	// NB: skipBackward checks whether exhaustedBounds is already -1.
	return i.skipBackward()
}

// Next implements internalIterator.Next, as documented in the pebble
// package.
// Note: twoLevelCompactionIterator.Next mirrors the implementation of
// twoLevelIterator.Next due to performance. Keep the two in sync.
func (i *twoLevelIterator) Next() *base.InternalKV {
	// Seek optimization only applies until iterator is first positioned after SetBounds.
	i.boundsCmp = 0
	if i.err != nil {
		// TODO(jackson): Can this case be turned into a panic? Once an error is
		// encountered, the iterator must be re-seeked.
		return nil
	}
	if ikv := i.singleLevelIterator.Next(); ikv != nil {
		return ikv
	}
	return i.skipForward()
}

// NextPrefix implements (base.InternalIterator).NextPrefix.
func (i *twoLevelIterator) NextPrefix(succKey []byte) *base.InternalKV {
	if i.exhaustedBounds == +1 {
		panic("Next called even though exhausted upper bound")
	}
	// Seek optimization only applies until iterator is first positioned after SetBounds.
	i.boundsCmp = 0
	if i.err != nil {
		// TODO(jackson): Can this case be turned into a panic? Once an error is
		// encountered, the iterator must be re-seeked.
		return nil
	}
	if ikv := i.singleLevelIterator.NextPrefix(succKey); ikv != nil {
		return ikv
	}
	// key == nil
	if i.err != nil {
		return nil
	}

	// Did not find prefix in the existing second-level index block. This is the
	// slow-path where we seek the iterator.
	var ikv *base.InternalKV
	if ikv = i.topLevelIndex.SeekGE(succKey, base.SeekGEFlagsNone); ikv == nil {
		i.data.invalidate()
		i.index.invalidate()
		return nil
	}
	result := i.loadIndex(+1)
	if result == loadBlockFailed {
		return nil
	}
	if result == loadBlockIrrelevant {
		// Enforce the upper bound here since don't want to bother moving to the
		// next entry in the top level index if upper bound is already exceeded.
		// Note that the next entry starts with keys >= ikv.InternalKey.UserKey
		// since even though this is the block separator, the same user key can
		// span multiple index blocks. If upper is exclusive we use >= below,
		// else we use >.
		if i.upper != nil {
			cmp := i.cmp(ikv.K.UserKey, i.upper)
			if (!i.endKeyInclusive && cmp >= 0) || cmp > 0 {
				i.exhaustedBounds = +1
			}
		}
	} else if kv := i.singleLevelIterator.SeekGE(succKey, base.SeekGEFlagsNone); kv != nil {
		return i.maybeVerifyKey(kv)
	}
	return i.skipForward()
}

// Prev implements internalIterator.Prev, as documented in the pebble
// package.
func (i *twoLevelIterator) Prev() *base.InternalKV {
	// Seek optimization only applies until iterator is first positioned after SetBounds.
	i.boundsCmp = 0
	if i.err != nil {
		return nil
	}
	if kv := i.singleLevelIterator.Prev(); kv != nil {
		return kv
	}
	return i.skipBackward()
}

func (i *twoLevelIterator) skipForward() *base.InternalKV {
	for {
		if i.err != nil || i.exhaustedBounds > 0 {
			return nil
		}

		// It is possible that skipBackward went too far and the virtual table lower
		// bound is after the first key in the block we are about to load, in which
		// case we must use SeekGE below. The keys in the block we are about to load
		// start right after the topLevelIndex key (before we Next).
		//
		// An example of how this can happen:
		//
		//   Second-level index block 1 - contains keys a@1, c@1
		//   Second-level index block 2 - contains keys e@1, g@1
		//   Second-level index block 3 - contains keys i@2, k@2
		//
		//   The virtual table lower bound is f. We have a range key masking filter
		//   that filters keys with @1 suffix. We are positioned inside block 3 then
		//   we Prev(). Block 2 is entirely filtered out, which makes us move to
		//   block 1. Now the range key masking filter gets an update (via
		//   SpanChanged) and it no longer filters out any keys. At this point if a
		//   Next happens, we will load block 2 but it would not be legal to return
		//   "e@1" which is outside the virtual bounds.
		//
		//   The core of the problem is that skipBackward doesn't know it can stop
		//   at block 2, because it doesn't know what keys are at the start of that
		//   block. This is why we don't have this problem in the opposite
		//   direction: skipForward will never go beyond the last relevant block
		//   because it looks at the separator key which is an upper bound for the
		//   block.
		//
		// Note that this is only a problem with virtual tables; we make no
		// guarantees wrt an iterator lower bound when we iterate forward. But we
		// must never return keys that are not inside the virtual table.
		useSeek := i.vState != nil &&
			(!i.topLevelIndex.valid() || base.InternalCompare(i.cmp, i.topLevelIndex.ikv.K, i.vState.lower) < 0)

		i.exhaustedBounds = 0
		topLevelKey := i.topLevelIndex.Next()
		if topLevelKey == nil {
			i.data.invalidate()
			i.index.invalidate()
			return nil
		}
		result := i.loadIndex(+1)
		if result == loadBlockFailed {
			return nil
		}
		if result == loadBlockOK {
			var ikv *base.InternalKV
			if useSeek {
				ikv = i.singleLevelIterator.SeekGE(i.lower, base.SeekGEFlagsNone)
			} else {
				ikv = i.singleLevelIterator.firstInternal()
			}
			if ikv != nil {
				return i.maybeVerifyKey(ikv)
			}
			// Next iteration will return if singleLevelIterator set
			// exhaustedBounds = +1.
		} else {
			// result == loadBlockIrrelevant. Enforce the upper bound here since
			// don't want to bother moving to the next entry in the top level
			// index if upper bound is already exceeded. Note that the next
			// entry starts with keys >= ikv.InternalKey.UserKey since even
			// though this is the block separator, the same user key can span
			// multiple index blocks. If upper is exclusive we use >= below,
			// else we use >.
			if i.upper != nil {
				cmp := i.cmp(topLevelKey.K.UserKey, i.upper)
				if (!i.endKeyInclusive && cmp >= 0) || cmp > 0 {
					i.exhaustedBounds = +1
					// Next iteration will return.
				}
			}
		}
	}
}

func (i *twoLevelIterator) skipBackward() *base.InternalKV {
	for {
		if i.err != nil || i.exhaustedBounds < 0 {
			return nil
		}
		i.exhaustedBounds = 0
		topLevelKey := i.topLevelIndex.Prev()
		if topLevelKey == nil {
			i.data.invalidate()
			i.index.invalidate()
			return nil
		}
		result := i.loadIndex(-1)
		if result == loadBlockFailed {
			return nil
		}
		if result == loadBlockOK {
			ikv := i.singleLevelIterator.lastInternal()
			if ikv != nil {
				return i.maybeVerifyKey(ikv)
			}

			// Next iteration will return if singleLevelIterator set
			// exhaustedBounds = -1.
		} else {
			// result == loadBlockIrrelevant. Enforce the lower bound here since
			// don't want to bother moving to the previous entry in the top
			// level index if lower bound is already exceeded. Note that the
			// previous entry starts with keys <= ikv.InternalKey.UserKey since
			// even though this is the current block's separator, the same user
			// key can span multiple index blocks.
			if i.lower != nil && i.cmp(topLevelKey.K.UserKey, i.lower) < 0 {
				i.exhaustedBounds = -1
				// Next iteration will return.
			}
		}
	}
}

// Close implements internalIterator.Close, as documented in the pebble
// package.
func (i *twoLevelIterator) Close() error {
	if invariants.Enabled && i.inPool {
		panic("Close called on interator in pool")
	}
	i.iterStats.close()
	var err error
	if i.closeHook != nil {
		err = firstError(err, i.closeHook(i))
	}
	err = firstError(err, i.data.Close())
	err = firstError(err, i.index.Close())
	err = firstError(err, i.topLevelIndex.Close())
	if i.indexFilterRH != nil {
		err = firstError(err, i.indexFilterRH.Close())
		i.indexFilterRH = nil
	}
	if i.dataRH != nil {
		err = firstError(err, i.dataRH.Close())
		i.dataRH = nil
	}
	err = firstError(err, i.err)
	if i.bpfs != nil {
		releaseBlockPropertiesFilterer(i.bpfs)
	}
	if i.vbReader != nil {
		i.vbReader.close()
	}
	if i.vbRH != nil {
		err = firstError(err, i.vbRH.Close())
		i.vbRH = nil
	}
	*i = twoLevelIterator{
		singleLevelIterator: i.singleLevelIterator.resetForReuse(),
		topLevelIndex:       i.topLevelIndex.resetForReuse(),
	}
	twoLevelIterPool.Put(i)
	return err
}

// Note: twoLevelCompactionIterator and compactionIterator are very similar but
// were separated due to performance.
type twoLevelCompactionIterator struct {
	*twoLevelIterator
}

// twoLevelCompactionIterator implements the base.InternalIterator interface.
var _ base.InternalIterator = (*twoLevelCompactionIterator)(nil)

func (i *twoLevelCompactionIterator) Close() error {
	return i.twoLevelIterator.Close()
}

func (i *twoLevelCompactionIterator) SeekGE(key []byte, flags base.SeekGEFlags) *base.InternalKV {
	panic("pebble: SeekGE unimplemented")
}

func (i *twoLevelCompactionIterator) SeekPrefixGE(
	prefix, key []byte, flags base.SeekGEFlags,
) *base.InternalKV {
	panic("pebble: SeekPrefixGE unimplemented")
}

func (i *twoLevelCompactionIterator) SeekLT(key []byte, flags base.SeekLTFlags) *base.InternalKV {
	panic("pebble: SeekLT unimplemented")
}

func (i *twoLevelCompactionIterator) First() *base.InternalKV {
	i.err = nil // clear cached iteration error
	return i.skipForward(i.twoLevelIterator.First())
}

func (i *twoLevelCompactionIterator) Last() *base.InternalKV {
	panic("pebble: Last unimplemented")
}

// Note: twoLevelCompactionIterator.Next mirrors the implementation of
// twoLevelIterator.Next due to performance. Keep the two in sync.
func (i *twoLevelCompactionIterator) Next() *base.InternalKV {
	if i.err != nil {
		return nil
	}
	return i.skipForward(i.singleLevelIterator.Next())
}

func (i *twoLevelCompactionIterator) NextPrefix(succKey []byte) *base.InternalKV {
	panic("pebble: NextPrefix unimplemented")
}

func (i *twoLevelCompactionIterator) Prev() *base.InternalKV {
	panic("pebble: Prev unimplemented")
}

func (i *twoLevelCompactionIterator) String() string {
	if i.vState != nil {
		return i.vState.fileNum.String()
	}
	return i.reader.fileNum.String()
}

func (i *twoLevelCompactionIterator) skipForward(kv *base.InternalKV) *base.InternalKV {
	if kv == nil {
		for {
			if key := i.topLevelIndex.Next(); key == nil {
				break
			}
			result := i.loadIndex(+1)
			if result != loadBlockOK {
				if i.err != nil {
					break
				}
				switch result {
				case loadBlockFailed:
					// We checked that i.index was at a valid entry, so
					// loadBlockFailed could not have happened due to to i.index
					// being exhausted, and must be due to an error.
					panic("loadBlock should not have failed with no error")
				case loadBlockIrrelevant:
					panic("compactionIter should not be using block intervals for skipping")
				default:
					panic(fmt.Sprintf("unexpected case %d", result))
				}
			}
			// result == loadBlockOK
			if kv = i.singleLevelIterator.First(); kv != nil {
				break
			}
		}
	}

	// We have an upper bound when the table is virtual.
	if i.upper != nil && kv != nil {
		cmp := i.cmp(kv.K.UserKey, i.upper)
		if cmp > 0 || (!i.endKeyInclusive && cmp == 0) {
			return nil
		}
	}

	return kv
}
