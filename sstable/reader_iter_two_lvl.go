// Copyright 2011 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package sstable

import (
	"bytes"
	"context"
	"fmt"
	"sync"

	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/pebble/internal/base"
	"github.com/cockroachdb/pebble/internal/invariants"
	"github.com/cockroachdb/pebble/internal/treeprinter"
	"github.com/cockroachdb/pebble/objstorage"
	"github.com/cockroachdb/pebble/sstable/block"
	"github.com/cockroachdb/pebble/sstable/valblk"
)

type twoLevelIterator[I any, PI indexBlockIterator[I], D any, PD dataBlockIterator[D]] struct {
	secondLevel   singleLevelIterator[I, PI, D, PD]
	topLevelIndex I
	// pool is the pool from which the iterator was allocated and to which the
	// iterator should be returned on Close. Because the iterator is
	// parameterized by the type of the data block iterator, pools must be
	// specific to the type of the data block iterator.
	pool *sync.Pool

	// useFilterBlock controls whether we consult the bloom filter in the
	// twoLevelIterator code. Note that secondLevel.useFilterBlock is always
	// false - any filtering happens at the top level.
	useFilterBlock         bool
	lastBloomFilterMatched bool
}

var _ Iterator = (*twoLevelIteratorRowBlocks)(nil)

// loadSecondLevelIndexBlock loads the index block at the current top level
// index position and leaves i.index unpositioned. If unsuccessful, it gets
// i.secondLevel.err to any error encountered, which may be nil if we have
// simply exhausted the entire table. This is used for two level indexes.
func (i *twoLevelIterator[I, PI, D, PD]) loadSecondLevelIndexBlock(dir int8) loadBlockResult {
	// Ensure the index data block iterators are invalidated even if loading of
	// the index fails.
	PD(&i.secondLevel.data).Invalidate()
	PI(&i.secondLevel.index).Invalidate()
	if !PI(&i.topLevelIndex).Valid() {
		return loadBlockFailed
	}
	bhp, err := PI(&i.topLevelIndex).BlockHandleWithProperties()
	if err != nil {
		i.secondLevel.err = base.CorruptionErrorf("pebble/table: corrupt top level index entry (%v)", err)
		return loadBlockFailed
	}
	if i.secondLevel.bpfs != nil {
		intersects, err := i.secondLevel.bpfs.intersects(bhp.Props)
		if err != nil {
			i.secondLevel.err = errCorruptIndexEntry(err)
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
	indexBlock, err := i.secondLevel.reader.readIndexBlock(i.secondLevel.ctx, i.secondLevel.readBlockEnv, i.secondLevel.indexFilterRH, bhp.Handle)
	if err != nil {
		i.secondLevel.err = err
		return loadBlockFailed
	}
	err = PI(&i.secondLevel.index).InitHandle(i.secondLevel.reader.Comparer, indexBlock, i.secondLevel.transforms)
	if err != nil {
		PI(&i.secondLevel.index).Invalidate()
		i.secondLevel.err = err
		return loadBlockFailed
	}
	return loadBlockOK
}

// resolveMaybeExcluded is invoked when the block-property filterer has found
// that an index block is excluded according to its properties but only if its
// bounds fall within the filter's current bounds. This function consults the
// appropriate bound, depending on the iteration direction, and returns either
// `blockIntersects` or `blockExcluded`.
func (i *twoLevelIterator[I, PI, D, PD]) resolveMaybeExcluded(dir int8) intersectsResult {
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
	// The index separator in topLevelIndex.Separator() provides an inclusive
	// upper-bound for the index block's keys, guaranteeing that all its keys
	// are ≤ topLevelIndex.Separator(). For forward iteration, this is all we
	// need.
	if dir > 0 {
		// Forward iteration.
		if i.secondLevel.bpfs.boundLimitedFilter.KeyIsWithinUpperBound(PI(&i.topLevelIndex).Separator()) {
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
	if !PI(&i.topLevelIndex).Prev() {
		// The original block points to the first index block of this table. If
		// we knew the lower bound for the entire table, it could provide a
		// lower bound, but the code refactoring necessary to read it doesn't
		// seem worth the payoff. We fall through to loading the block.
	} else if i.secondLevel.bpfs.boundLimitedFilter.KeyIsWithinLowerBound(PI(&i.topLevelIndex).Separator()) {
		// The lower-bound on the original index block falls within the filter's
		// bounds, and we can skip the block (after restoring our current
		// top-level index position).
		_ = PI(&i.topLevelIndex).Next()
		return blockExcluded
	}
	_ = PI(&i.topLevelIndex).Next()
	return blockIntersects
}

// newColumnBlockTwoLevelIterator reads the top-level index block and creates and
// initializes a two-level iterator over an sstable with column-oriented data
// blocks.
//
// Note that lower, upper are iterator bounds and are separate from virtual
// sstable bounds. If the virtualState passed in is not nil, then virtual
// sstable bounds will be enforced.
func newColumnBlockTwoLevelIterator(
	ctx context.Context,
	r *Reader,
	v *virtualState,
	transforms IterTransforms,
	lower, upper []byte,
	filterer *BlockPropertiesFilterer,
	filterBlockSizeLimit FilterBlockSizeLimit,
	env block.ReadEnv,
	rp valblk.ReaderProvider,
) (*twoLevelIteratorColumnBlocks, error) {
	if r.err != nil {
		return nil, r.err
	}
	if !r.tableFormat.BlockColumnar() {
		panic(errors.AssertionFailedf("table format %d should not use columnar block format", r.tableFormat))
	}
	i := twoLevelIterColumnBlockPool.Get().(*twoLevelIteratorColumnBlocks)
	i.secondLevel.init(ctx, r, v, transforms, lower, upper, filterer,
		false, // Disable the use of the filter block in the second level.
		env)
	var getLazyValuer block.GetInternalValueForPrefixAndValueHandler
	if r.Properties.NumValueBlocks > 0 {
		// NB: we cannot avoid this ~248 byte allocation, since valueBlockReader
		// can outlive the singleLevelIterator due to be being embedded in a
		// LazyValue. This consumes ~2% in microbenchmark CPU profiles, but we
		// should only optimize this if it shows up as significant in end-to-end
		// CockroachDB benchmarks, since it is tricky to do so. One possibility
		// is that if many sstable iterators only get positioned at latest
		// versions of keys, and therefore never expose a LazyValue that is
		// separated to their callers, they can put this valueBlockReader into a
		// sync.Pool.
		i.secondLevel.vbReader = valblk.MakeReader(&i.secondLevel, rp, r.valueBIH, env.Stats)
		getLazyValuer = &i.secondLevel.vbReader
		i.secondLevel.vbRH = r.blockReader.UsePreallocatedReadHandle(
			objstorage.NoReadBefore, &i.secondLevel.vbRHPrealloc)
	}
	i.secondLevel.data.InitOnce(r.keySchema, r.Comparer, getLazyValuer)
	i.useFilterBlock = shouldUseFilterBlock(r, filterBlockSizeLimit)
	topLevelIndexH, err := r.readTopLevelIndexBlock(ctx, i.secondLevel.readBlockEnv, i.secondLevel.indexFilterRH)
	if err == nil {
		err = i.topLevelIndex.InitHandle(r.Comparer, topLevelIndexH, transforms)
	}
	if err != nil {
		_ = i.Close()
		return nil, err
	}
	return i, nil
}

// newRowBlockTwoLevelIterator reads the top-level index block and creates and
// initializes a two-level iterator over an sstable with row-oriented data
// blocks.
//
// Note that lower, upper are iterator bounds and are separate from virtual
// sstable bounds. If the virtualState passed in is not nil, then virtual
// sstable bounds will be enforced.
func newRowBlockTwoLevelIterator(
	ctx context.Context,
	r *Reader,
	v *virtualState,
	transforms IterTransforms,
	lower, upper []byte,
	filterer *BlockPropertiesFilterer,
	filterBlockSizeLimit FilterBlockSizeLimit,
	env block.ReadEnv,
	rp valblk.ReaderProvider,
) (*twoLevelIteratorRowBlocks, error) {
	if r.err != nil {
		return nil, r.err
	}
	if r.tableFormat.BlockColumnar() {
		panic(errors.AssertionFailedf("table format %s uses block columnar format", r.tableFormat))
	}
	i := twoLevelIterRowBlockPool.Get().(*twoLevelIteratorRowBlocks)
	i.secondLevel.init(ctx, r, v, transforms, lower, upper, filterer,
		false, // Disable the use of the filter block in the second level.
		env)
	if r.tableFormat >= TableFormatPebblev3 {
		if r.Properties.NumValueBlocks > 0 {
			// NB: we cannot avoid this ~248 byte allocation, since valueBlockReader
			// can outlive the singleLevelIterator due to be being embedded in a
			// LazyValue. This consumes ~2% in microbenchmark CPU profiles, but we
			// should only optimize this if it shows up as significant in end-to-end
			// CockroachDB benchmarks, since it is tricky to do so. One possibility
			// is that if many sstable iterators only get positioned at latest
			// versions of keys, and therefore never expose a LazyValue that is
			// separated to their callers, they can put this valueBlockReader into a
			// sync.Pool.
			i.secondLevel.vbReader = valblk.MakeReader(&i.secondLevel, rp, r.valueBIH, env.Stats)
			i.secondLevel.data.SetGetLazyValuer(&i.secondLevel.vbReader)
			i.secondLevel.vbRH = r.blockReader.UsePreallocatedReadHandle(
				objstorage.NoReadBefore, &i.secondLevel.vbRHPrealloc)
		}
		i.secondLevel.data.SetHasValuePrefix(true)
	}

	i.useFilterBlock = shouldUseFilterBlock(r, filterBlockSizeLimit)

	topLevelIndexH, err := r.readTopLevelIndexBlock(ctx, i.secondLevel.readBlockEnv, i.secondLevel.indexFilterRH)
	if err == nil {
		err = i.topLevelIndex.InitHandle(r.Comparer, topLevelIndexH, transforms)
	}
	if err != nil {
		_ = i.Close()
		return nil, err
	}
	return i, nil
}

func (i *twoLevelIterator[I, PI, D, PD]) String() string {
	return i.secondLevel.String()
}

// DebugTree is part of the InternalIterator interface.
func (i *twoLevelIterator[I, PI, D, PD]) DebugTree(tp treeprinter.Node) {
	tp.Childf("%T(%p) fileNum=%s", i, i, i.String())
}

// SeekGE implements internalIterator.SeekGE, as documented in the pebble
// package. Note that SeekGE only checks the upper bound. It is up to the
// caller to ensure that key is greater than or equal to the lower bound.
func (i *twoLevelIterator[I, PI, D, PD]) SeekGE(
	key []byte, flags base.SeekGEFlags,
) *base.InternalKV {
	if i.secondLevel.vState != nil {
		// Callers of SeekGE don't know about virtual sstable bounds, so we may
		// have to internally restrict the bounds.
		//
		// TODO(bananabrick): We can optimize away this check for the level iter
		// if necessary.
		if i.secondLevel.cmp(key, i.secondLevel.lower) < 0 {
			key = i.secondLevel.lower
		}
	}

	err := i.secondLevel.err
	i.secondLevel.err = nil // clear cached iteration error

	// The twoLevelIterator could be already exhausted. Utilize that when
	// trySeekUsingNext is true. See the comment about data-exhausted, PGDE, and
	// bounds-exhausted near the top of the file.
	if flags.TrySeekUsingNext() &&
		(i.secondLevel.exhaustedBounds == +1 || (PD(&i.secondLevel.data).IsDataInvalidated() && PI(&i.secondLevel.index).IsDataInvalidated())) &&
		err == nil {
		// Already exhausted, so return nil.
		return nil
	}

	// SeekGE performs various step-instead-of-seeking optimizations: eg enabled
	// by trySeekUsingNext, or by monotonically increasing bounds (i.boundsCmp).

	// We fall into the slow path if i.index.IsDataInvalidated() even if the
	// top-level iterator is already positioned correctly and all other
	// conditions are met. An alternative structure could reuse topLevelIndex's
	// current position and reload the index block to which it points. Arguably,
	// an index block load is expensive and the index block may still be earlier
	// than the index block containing the sought key, resulting in a wasteful
	// block load.

	var dontSeekWithinSingleLevelIter bool
	if PI(&i.topLevelIndex).IsDataInvalidated() || !PI(&i.topLevelIndex).Valid() || PI(&i.secondLevel.index).IsDataInvalidated() || err != nil ||
		(i.secondLevel.boundsCmp <= 0 && !flags.TrySeekUsingNext()) || PI(&i.topLevelIndex).SeparatorLT(key) {
		// Slow-path: need to position the topLevelIndex.

		// The previous exhausted state of singleLevelIterator is no longer
		// relevant, since we may be moving to a different index block.
		i.secondLevel.exhaustedBounds = 0
		flags = flags.DisableTrySeekUsingNext()
		if !PI(&i.topLevelIndex).SeekGE(key) {
			PD(&i.secondLevel.data).Invalidate()
			PI(&i.secondLevel.index).Invalidate()
			return nil
		}

		result := i.loadSecondLevelIndexBlock(+1)
		if result == loadBlockFailed {
			i.secondLevel.boundsCmp = 0
			return nil
		}
		if result == loadBlockIrrelevant {
			// Enforce the upper bound here since don't want to bother moving to
			// the next entry in the top level index if upper bound is already
			// exceeded. Note that the next entry starts with keys >=
			// topLevelIndex.Separator() since even though this is the block
			// separator, the same user key can span multiple index blocks. If
			// upper is exclusive we pass orEqual=true below, else we require
			// the separator to be strictly greater than upper.
			if i.secondLevel.upper != nil && PI(&i.topLevelIndex).SeparatorGT(
				i.secondLevel.upper, !i.secondLevel.endKeyInclusive) {
				i.secondLevel.exhaustedBounds = +1
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
			i.secondLevel.boundsCmp = 0
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
		// The !i.cmp(key, i.topLevelIndex.Separator) > 0 confirms that it is
		// not behind. Since it is not ahead and not behind it must be at the
		// right position.
		//
		// 2) This SeekGE will land on a key that is greater than the key we are
		// currently at (guaranteed by trySeekUsingNext), but since i.cmp(key,
		// i.topLevelIndex.Separator()) <= 0, we are at the correct lower level
		// index block. No need to reset the state of singleLevelIterator.
		//
		// Note that cases 1 and 2 never overlap, and one of them must be true.
		// This invariant checking is important enough that we do not gate it
		// behind invariants.Enabled.
		if i.secondLevel.boundsCmp > 0 == flags.TrySeekUsingNext() {
			panic(fmt.Sprintf("inconsistency in optimization case 1 %t and case 2 %t",
				i.secondLevel.boundsCmp > 0, flags.TrySeekUsingNext()))
		}

		if !flags.TrySeekUsingNext() {
			// Case 1. Bounds have changed so the previous exhausted bounds state is
			// irrelevant.
			// WARNING-data-exhausted: this is safe to do only because the monotonic
			// bounds optimizations only work when !data-exhausted. If they also
			// worked with data-exhausted, we have made it unclear whether
			// data-exhausted is actually true. See the comment at the top of the
			// file.
			i.secondLevel.exhaustedBounds = 0
		}
		// Else flags.TrySeekUsingNext(). The i.exhaustedBounds is important to
		// preserve for singleLevelIterator, and twoLevelIterator.skipForward. See
		// bug https://github.com/cockroachdb/pebble/issues/2036.
	}

	if !dontSeekWithinSingleLevelIter {
		// Note that while trySeekUsingNext could be false here, singleLevelIterator
		// could do its own boundsCmp-based optimization to seek using next.
		if ikv := i.secondLevel.SeekGE(key, flags); ikv != nil {
			return ikv
		}
	}
	return i.skipForward()
}

// SeekPrefixGE implements internalIterator.SeekPrefixGE, as documented in the
// pebble package. Note that SeekPrefixGE only checks the upper bound. It is up
// to the caller to ensure that key is greater than or equal to the lower bound.
func (i *twoLevelIterator[I, PI, D, PD]) SeekPrefixGE(
	prefix, key []byte, flags base.SeekGEFlags,
) *base.InternalKV {
	if i.secondLevel.vState != nil {
		// Callers of SeekGE don't know about virtual sstable bounds, so we may
		// have to internally restrict the bounds.
		//
		// TODO(bananabrick): We can optimize away this check for the level iter
		// if necessary.
		if i.secondLevel.cmp(key, i.secondLevel.lower) < 0 {
			key = i.secondLevel.lower
		}
	}

	// NOTE: prefix is only used for bloom filter checking and not later work in
	// this method. Hence, we can use the existing iterator position if the last
	// SeekPrefixGE did not fail bloom filter matching.

	err := i.secondLevel.err
	i.secondLevel.err = nil // clear cached iteration error

	// The twoLevelIterator could be already exhausted. Utilize that when
	// trySeekUsingNext is true. See the comment about data-exhausted, PGDE, and
	// bounds-exhausted near the top of the file.
	filterUsedAndDidNotMatch := i.useFilterBlock && !i.lastBloomFilterMatched
	if flags.TrySeekUsingNext() && !filterUsedAndDidNotMatch &&
		(i.secondLevel.exhaustedBounds == +1 || (PD(&i.secondLevel.data).IsDataInvalidated() && PI(&i.secondLevel.index).IsDataInvalidated())) &&
		err == nil {
		// Already exhausted, so return nil.
		return nil
	}

	// Check prefix bloom filter.
	if i.useFilterBlock {
		if !i.lastBloomFilterMatched {
			// Iterator is not positioned based on last seek.
			flags = flags.DisableTrySeekUsingNext()
		}
		i.lastBloomFilterMatched = false
		var mayContain bool
		mayContain, i.secondLevel.err = i.secondLevel.bloomFilterMayContain(prefix)
		if i.secondLevel.err != nil || !mayContain {
			// In the i.secondLevel.err == nil case, this invalidation may not be necessary for
			// correctness, and may be a place to optimize later by reusing the
			// already loaded block. It was necessary in earlier versions of the code
			// since the caller was allowed to call Next when SeekPrefixGE returned
			// nil. This is no longer allowed.
			PD(&i.secondLevel.data).Invalidate()
			return nil
		}
		i.lastBloomFilterMatched = true
	}

	// Bloom filter matches.

	// SeekPrefixGE performs various step-instead-of-seeking optimizations: eg
	// enabled by trySeekUsingNext, or by monotonically increasing bounds
	// (i.boundsCmp).

	// We fall into the slow path if i.index.IsDataInvalidated() even if the
	// top-level iterator is already positioned correctly and all other
	// conditions are met. An alternative structure could reuse topLevelIndex's
	// current position and reload the index block to which it points. Arguably,
	// an index block load is expensive and the index block may still be earlier
	// than the index block containing the sought key, resulting in a wasteful
	// block load.

	var dontSeekWithinSingleLevelIter bool
	if PI(&i.topLevelIndex).IsDataInvalidated() || !PI(&i.topLevelIndex).Valid() || PI(&i.secondLevel.index).IsDataInvalidated() || err != nil ||
		(i.secondLevel.boundsCmp <= 0 && !flags.TrySeekUsingNext()) || PI(&i.topLevelIndex).SeparatorLT(key) {
		// Slow-path: need to position the topLevelIndex.

		// The previous exhausted state of singleLevelIterator is no longer
		// relevant, since we may be moving to a different index block.
		i.secondLevel.exhaustedBounds = 0
		flags = flags.DisableTrySeekUsingNext()
		if !PI(&i.topLevelIndex).SeekGE(key) {
			PD(&i.secondLevel.data).Invalidate()
			PI(&i.secondLevel.index).Invalidate()
			return nil
		}

		result := i.loadSecondLevelIndexBlock(+1)
		if result == loadBlockFailed {
			i.secondLevel.boundsCmp = 0
			return nil
		}
		if result == loadBlockIrrelevant {
			// Enforce the upper bound here since don't want to bother moving to
			// the next entry in the top level index if upper bound is already
			// exceeded. Note that the next entry starts with keys >=
			// topLevelIndex.Separator() since even though this is the block
			// separator, the same user key can span multiple index blocks. If
			// upper is exclusive we pass orEqual=true below, else we require
			// the separator to be strictly greater than upper.
			if i.secondLevel.upper != nil && PI(&i.topLevelIndex).SeparatorGT(
				i.secondLevel.upper, !i.secondLevel.endKeyInclusive) {
				i.secondLevel.exhaustedBounds = +1
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
			i.secondLevel.boundsCmp = 0
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
		// positioned behind). The !i.cmp(key, i.topLevelIndex.Separator()) > 0
		// confirms that it is not behind. Since it is not ahead and not behind it
		// must be at the right position.
		//
		// 2) This SeekPrefixGE will land on a key that is greater than the key we
		// are currently at (guaranteed by trySeekUsingNext), but since i.cmp(key,
		// i.topLevelIndex.Separator()) <= 0, we are at the correct lower level
		// index block. No need to reset the state of singleLevelIterator.
		//
		// Note that cases 1 and 2 never overlap, and one of them must be true.
		// This invariant checking is important enough that we do not gate it
		// behind invariants.Enabled.
		if i.secondLevel.boundsCmp > 0 == flags.TrySeekUsingNext() {
			panic(fmt.Sprintf("inconsistency in optimization case 1 %t and case 2 %t",
				i.secondLevel.boundsCmp > 0, flags.TrySeekUsingNext()))
		}

		if !flags.TrySeekUsingNext() {
			// Case 1. Bounds have changed so the previous exhausted bounds state is
			// irrelevant.
			// WARNING-data-exhausted: this is safe to do only because the monotonic
			// bounds optimizations only work when !data-exhausted. If they also
			// worked with data-exhausted, we have made it unclear whether
			// data-exhausted is actually true. See the comment at the top of the
			// file.
			i.secondLevel.exhaustedBounds = 0
		}
		// Else flags.TrySeekUsingNext(). The i.exhaustedBounds is important to
		// preserve for singleLevelIterator, and twoLevelIterator.skipForward. See
		// bug https://github.com/cockroachdb/pebble/issues/2036.
	}

	if !dontSeekWithinSingleLevelIter {
		if ikv := i.secondLevel.seekPrefixGE(prefix, key, flags); ikv != nil {
			return ikv
		}
	}
	// NB: skipForward checks whether exhaustedBounds is already +1.
	return i.skipForward()
}

// virtualLast should only be called if i.vReader != nil.
func (i *twoLevelIterator[I, PI, D, PD]) virtualLast() *base.InternalKV {
	if i.secondLevel.vState == nil {
		panic("pebble: invalid call to virtualLast")
	}
	if !i.secondLevel.endKeyInclusive {
		// Trivial case.
		return i.SeekLT(i.secondLevel.upper, base.SeekLTFlagsNone)
	}
	return i.virtualLastSeekLE()
}

// virtualLastSeekLE implements a SeekLE() that can be used as part
// of reverse-iteration calls such as a Last() on a virtual sstable. Does a
// SeekLE on the upper bound of the file/iterator.
func (i *twoLevelIterator[I, PI, D, PD]) virtualLastSeekLE() *base.InternalKV {
	// Callers of SeekLE don't know about virtual sstable bounds, so we may
	// have to internally restrict the bounds.
	//
	// TODO(bananabrick): We can optimize this check away for the level iter
	// if necessary.
	if !i.secondLevel.endKeyInclusive {
		panic("unexpected virtualLastSeekLE with exclusive upper bounds")
	}
	key := i.secondLevel.upper
	// Need to position the topLevelIndex.
	//
	// The previous exhausted state of singleLevelIterator is no longer
	// relevant, since we may be moving to a different index block.
	i.secondLevel.exhaustedBounds = 0
	// Seek optimization only applies until iterator is first positioned with a
	// SeekGE or SeekLT after SetBounds.
	i.secondLevel.boundsCmp = 0
	topLevelOk := PI(&i.topLevelIndex).SeekGE(key)
	// We can have multiple internal keys with the same user key as the seek
	// key. In that case, we want the last (greatest) internal key.
	for topLevelOk && bytes.Equal(PI(&i.topLevelIndex).Separator(), key) {
		topLevelOk = PI(&i.topLevelIndex).Next()
	}
	if !topLevelOk {
		return i.skipBackward()
	}
	result := i.loadSecondLevelIndexBlock(-1)
	if result == loadBlockFailed {
		i.secondLevel.boundsCmp = 0
		return nil
	}
	if result == loadBlockIrrelevant {
		// Load the previous block.
		return i.skipBackward()
	}
	if ikv := i.secondLevel.virtualLastSeekLE(); ikv != nil {
		return ikv
	}
	return i.skipBackward()
}

// SeekLT implements internalIterator.SeekLT, as documented in the pebble
// package. Note that SeekLT only checks the lower bound. It is up to the
// caller to ensure that key is less than the upper bound.
func (i *twoLevelIterator[I, PI, D, PD]) SeekLT(
	key []byte, flags base.SeekLTFlags,
) *base.InternalKV {
	if i.secondLevel.vState != nil {
		// Might have to fix upper bound since virtual sstable bounds are not
		// known to callers of SeekLT.
		//
		// TODO(bananabrick): We can optimize away this check for the level iter
		// if necessary.
		cmp := i.secondLevel.cmp(key, i.secondLevel.upper)
		// key == i.secondLevel.upper is fine. We'll do the right thing and return the
		// first internal key with user key < key.
		if cmp > 0 {
			return i.virtualLast()
		}
	}

	i.secondLevel.exhaustedBounds = 0
	i.secondLevel.err = nil // clear cached iteration error
	// Seek optimization only applies until iterator is first positioned after SetBounds.
	i.secondLevel.boundsCmp = 0

	var result loadBlockResult
	// NB: Unlike SeekGE, we don't have a fast-path here since we don't know
	// whether the topLevelIndex is positioned after the position that would
	// be returned by doing i.topLevelIndex.SeekGE(). To know this we would
	// need to know the index key preceding the current one.
	// NB: If a bound-limited block property filter is configured, it's
	// externally ensured that the filter is disabled (through returning
	// Intersects=false irrespective of the block props provided) during seeks.
	if !PI(&i.topLevelIndex).SeekGE(key) {
		if !PI(&i.topLevelIndex).Last() {
			PD(&i.secondLevel.data).Invalidate()
			PI(&i.secondLevel.index).Invalidate()
			return nil
		}

		result = i.loadSecondLevelIndexBlock(-1)
		if result == loadBlockFailed {
			return nil
		}
		if result == loadBlockOK {
			if ikv := i.secondLevel.lastInternal(); ikv != nil {
				return i.secondLevel.maybeVerifyKey(ikv)
			}
			// Fall through to skipBackward since the singleLevelIterator did
			// not have any blocks that satisfy the block interval
			// constraints, or the lower bound was reached.
		}
		// Else loadBlockIrrelevant, so fall through.
	} else {
		result = i.loadSecondLevelIndexBlock(-1)
		if result == loadBlockFailed {
			return nil
		}
		if result == loadBlockOK {
			if ikv := i.secondLevel.SeekLT(key, flags); ikv != nil {
				return i.secondLevel.maybeVerifyKey(ikv)
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
		if i.secondLevel.lower != nil && PI(&i.topLevelIndex).SeparatorLT(i.secondLevel.lower) {
			i.secondLevel.exhaustedBounds = -1
		}
	}
	// NB: skipBackward checks whether exhaustedBounds is already -1.
	return i.skipBackward()
}

// First implements internalIterator.First, as documented in the pebble
// package. Note that First only checks the upper bound. It is up to the caller
// to ensure that key is greater than or equal to the lower bound (e.g. via a
// call to SeekGE(lower)).
func (i *twoLevelIterator[I, PI, D, PD]) First() *base.InternalKV {
	// If we have a lower bound, use SeekGE. Note that in general this is not
	// supported usage, except when the lower bound is there because the table is
	// virtual.
	if i.secondLevel.lower != nil {
		return i.SeekGE(i.secondLevel.lower, base.SeekGEFlagsNone)
	}
	i.secondLevel.exhaustedBounds = 0
	i.secondLevel.err = nil // clear cached iteration error
	// Seek optimization only applies until iterator is first positioned after SetBounds.
	i.secondLevel.boundsCmp = 0

	if !PI(&i.topLevelIndex).First() {
		return nil
	}
	result := i.loadSecondLevelIndexBlock(+1)
	if result == loadBlockFailed {
		return nil
	}
	if result == loadBlockOK {
		if ikv := i.secondLevel.First(); ikv != nil {
			return ikv
		}
		// Else fall through to skipForward.
	} else {
		// result == loadBlockIrrelevant. Enforce the upper bound here since
		// don't want to bother moving to the next entry in the top level index
		// if upper bound is already exceeded. Note that the next entry starts
		// with keys >= topLevelIndex.Separator() since even though this is the
		// block separator, the same user key can span multiple index blocks.
		// If upper is exclusive we pass orEqual=true below, else we require the
		// separator to be strictly greater than upper.
		if i.secondLevel.upper != nil && PI(&i.topLevelIndex).SeparatorGT(
			i.secondLevel.upper, !i.secondLevel.endKeyInclusive) {
			i.secondLevel.exhaustedBounds = +1
		}
	}
	// NB: skipForward checks whether exhaustedBounds is already +1.
	return i.skipForward()
}

// Last implements internalIterator.Last, as documented in the pebble
// package. Note that Last only checks the lower bound. It is up to the caller
// to ensure that key is less than the upper bound (e.g. via a call to
// SeekLT(upper))
func (i *twoLevelIterator[I, PI, D, PD]) Last() *base.InternalKV {
	if i.secondLevel.vState != nil {
		if i.secondLevel.endKeyInclusive {
			return i.virtualLast()
		}
		return i.SeekLT(i.secondLevel.upper, base.SeekLTFlagsNone)
	}

	if i.secondLevel.upper != nil {
		panic("twoLevelIterator.Last() used despite upper bound")
	}
	i.secondLevel.exhaustedBounds = 0
	i.secondLevel.err = nil // clear cached iteration error
	// Seek optimization only applies until iterator is first positioned after SetBounds.
	i.secondLevel.boundsCmp = 0

	if !PI(&i.topLevelIndex).Last() {
		return nil
	}
	result := i.loadSecondLevelIndexBlock(-1)
	if result == loadBlockFailed {
		return nil
	}
	if result == loadBlockOK {
		if ikv := i.secondLevel.Last(); ikv != nil {
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
		if i.secondLevel.lower != nil && PI(&i.topLevelIndex).SeparatorLT(i.secondLevel.lower) {
			i.secondLevel.exhaustedBounds = -1
		}
	}
	// NB: skipBackward checks whether exhaustedBounds is already -1.
	return i.skipBackward()
}

// Next implements internalIterator.Next, as documented in the pebble
// package.
// Note: twoLevelCompactionIterator.Next mirrors the implementation of
// twoLevelIterator.Next due to performance. Keep the two in sync.
func (i *twoLevelIterator[I, PI, D, PD]) Next() *base.InternalKV {
	// Seek optimization only applies until iterator is first positioned after SetBounds.
	i.secondLevel.boundsCmp = 0
	if i.secondLevel.err != nil {
		// TODO(jackson): Can this case be turned into a panic? Once an error is
		// encountered, the iterator must be re-seeked.
		return nil
	}
	if ikv := i.secondLevel.Next(); ikv != nil {
		return ikv
	}
	return i.skipForward()
}

// NextPrefix implements (base.InternalIterator).NextPrefix.
func (i *twoLevelIterator[I, PI, D, PD]) NextPrefix(succKey []byte) *base.InternalKV {
	if i.secondLevel.exhaustedBounds == +1 {
		panic("Next called even though exhausted upper bound")
	}
	// Seek optimization only applies until iterator is first positioned after SetBounds.
	i.secondLevel.boundsCmp = 0
	if i.secondLevel.err != nil {
		// TODO(jackson): Can this case be turned into a panic? Once an error is
		// encountered, the iterator must be re-seeked.
		return nil
	}
	if ikv := i.secondLevel.NextPrefix(succKey); ikv != nil {
		return ikv
	}
	// ikv == nil
	if i.secondLevel.err != nil {
		return nil
	}

	// Did not find prefix in the existing second-level index block. This is the
	// slow-path where we seek the iterator.
	if !PI(&i.topLevelIndex).SeekGE(succKey) {
		PD(&i.secondLevel.data).Invalidate()
		PI(&i.secondLevel.index).Invalidate()
		return nil
	}
	result := i.loadSecondLevelIndexBlock(+1)
	if result == loadBlockFailed {
		return nil
	}
	if result == loadBlockIrrelevant {
		// Enforce the upper bound here since don't want to bother moving to the
		// next entry in the top level index if upper bound is already exceeded.
		// Note that the next entry starts with keys >=
		// topLevelIndex.Separator() since even though this is the block
		// separator, the same user key can span multiple index blocks. If upper
		// is exclusive we pass orEqual=true below, else we require the
		// separator to be strictly greater than upper.
		if i.secondLevel.upper != nil && PI(&i.topLevelIndex).SeparatorGT(
			i.secondLevel.upper, !i.secondLevel.endKeyInclusive) {
			i.secondLevel.exhaustedBounds = +1
		}
	} else if kv := i.secondLevel.SeekGE(succKey, base.SeekGEFlagsNone); kv != nil {
		return i.secondLevel.maybeVerifyKey(kv)
	}
	return i.skipForward()
}

// Prev implements internalIterator.Prev, as documented in the pebble
// package.
func (i *twoLevelIterator[I, PI, D, PD]) Prev() *base.InternalKV {
	// Seek optimization only applies until iterator is first positioned after SetBounds.
	i.secondLevel.boundsCmp = 0
	if i.secondLevel.err != nil {
		return nil
	}
	if kv := i.secondLevel.Prev(); kv != nil {
		return kv
	}
	return i.skipBackward()
}

func (i *twoLevelIterator[I, PI, D, PD]) skipForward() *base.InternalKV {
	for {
		if i.secondLevel.err != nil || i.secondLevel.exhaustedBounds > 0 {
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
		useSeek := i.secondLevel.vState != nil && (!PI(&i.topLevelIndex).Valid() ||
			PI(&i.topLevelIndex).SeparatorLT(i.secondLevel.vState.lower.UserKey))

		i.secondLevel.exhaustedBounds = 0
		if !PI(&i.topLevelIndex).Next() {
			PD(&i.secondLevel.data).Invalidate()
			PI(&i.secondLevel.index).Invalidate()
			return nil
		}
		result := i.loadSecondLevelIndexBlock(+1)
		if result == loadBlockFailed {
			return nil
		}
		if result == loadBlockOK {
			var ikv *base.InternalKV
			if useSeek {
				ikv = i.secondLevel.SeekGE(i.secondLevel.lower, base.SeekGEFlagsNone)
			} else {
				ikv = i.secondLevel.firstInternal()
			}
			if ikv != nil {
				return i.secondLevel.maybeVerifyKey(ikv)
			}
			// Next iteration will return if singleLevelIterator set
			// exhaustedBounds = +1.
		} else {
			// result == loadBlockIrrelevant. Enforce the upper bound here since
			// don't want to bother moving to the next entry in the top level
			// index if upper bound is already exceeded. Note that the next
			// entry starts with keys >= i.topLevelIndex.Separator() since even
			// though this is the block separator, the same user key can span
			// multiple index blocks. If upper is exclusive we pass orEqual=true
			// below, else we require the separator to be strictly greater than
			// upper.
			if i.secondLevel.upper != nil && PI(&i.topLevelIndex).SeparatorGT(
				i.secondLevel.upper, !i.secondLevel.endKeyInclusive) {
				i.secondLevel.exhaustedBounds = +1
				// Next iteration will return.
			}
		}
	}
}

func (i *twoLevelIterator[I, PI, D, PD]) skipBackward() *base.InternalKV {
	for {
		if i.secondLevel.err != nil || i.secondLevel.exhaustedBounds < 0 {
			return nil
		}
		i.secondLevel.exhaustedBounds = 0
		if !PI(&i.topLevelIndex).Prev() {
			PD(&i.secondLevel.data).Invalidate()
			PI(&i.secondLevel.index).Invalidate()
			return nil
		}
		result := i.loadSecondLevelIndexBlock(-1)
		if result == loadBlockFailed {
			return nil
		}
		if result == loadBlockOK {
			ikv := i.secondLevel.lastInternal()
			if ikv != nil {
				return i.secondLevel.maybeVerifyKey(ikv)
			}

			// Next iteration will return if singleLevelIterator set
			// exhaustedBounds = -1.
		} else {
			// result == loadBlockIrrelevant. Enforce the lower bound here since
			// don't want to bother moving to the previous entry in the top
			// level index if lower bound is already exceeded. Note that the
			// previous entry starts with keys <= i.topLevelIndex.Separator() since
			// even though this is the current block's separator, the same user
			// key can span multiple index blocks.
			if i.secondLevel.lower != nil && PI(&i.topLevelIndex).SeparatorLT(i.secondLevel.lower) {
				i.secondLevel.exhaustedBounds = -1
				// Next iteration will return.
			}
		}
	}
}

func (i *twoLevelIterator[I, PI, D, PD]) Error() error {
	return i.secondLevel.Error()
}

func (i *twoLevelIterator[I, PI, D, PD]) SetBounds(lower, upper []byte) {
	i.secondLevel.SetBounds(lower, upper)
}

func (i *twoLevelIterator[I, PI, D, PD]) SetContext(ctx context.Context) {
	i.secondLevel.SetContext(ctx)
}

func (i *twoLevelIterator[I, PI, D, PD]) SetCloseHook(fn func()) {
	i.secondLevel.SetCloseHook(fn)
}

func (i *twoLevelIterator[I, PI, D, PD]) SetupForCompaction() {
	i.secondLevel.SetupForCompaction()
}

// Close implements internalIterator.Close, as documented in the pebble
// package.
func (i *twoLevelIterator[I, PI, D, PD]) Close() error {
	if invariants.Enabled && i.secondLevel.pool != nil {
		panic("twoLevelIterator's singleLevelIterator has its own non-nil pool")
	}
	pool := i.pool
	err := i.secondLevel.closeInternal()
	i.secondLevel.resetForReuse()
	err = firstError(err, PI(&i.topLevelIndex).Close())
	i.useFilterBlock = false
	i.lastBloomFilterMatched = false
	if pool != nil {
		pool.Put(i)
	}
	return err
}
