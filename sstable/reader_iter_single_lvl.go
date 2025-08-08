// Copyright 2011 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package sstable

import (
	"bytes"
	"context"
	"fmt"
	"sync"
	"unsafe"

	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/pebble/internal/base"
	"github.com/cockroachdb/pebble/internal/invariants"
	"github.com/cockroachdb/pebble/internal/treeprinter"
	"github.com/cockroachdb/pebble/objstorage"
	"github.com/cockroachdb/pebble/objstorage/objstorageprovider"
	"github.com/cockroachdb/pebble/sstable/block"
	"github.com/cockroachdb/pebble/sstable/valblk"
)

// singleLevelIterator iterates over an entire table of data. To seek for a given
// key, it first looks in the index for the block that contains that key, and then
// looks inside that block.
//
// singleLevelIterator is parameterized by the type of the data block iterator
// and index block iterator.  The type parameters are designed to allow the
// singleLevelIterator to embed the data block and index block iterator structs
// within itself, avoiding an extra allocation and pointer indirection. The
// complication comes from the fact that we want to implement the interfaces on
// pointer receivers but embed the non-pointer types within the struct. The D
// and I type parameters are the non-pointer data and index block iterator
// types, and the PD and PI type parameters are the *D and *I types that
// actually implement the blockiter.Data and blockiter.Index constraints.
//
// Unfortunately, uses of the [data] and [index] fields must explicitly cast
// &data/&index to the PD/PI type in order to access its interface methods. This
// pattern is taken from the Go generics proposal:
// https://go.googlesource.com/proposal/+/refs/heads/master/design/43651-type-parameters.md#pointer-method-example
type singleLevelIterator[I any, PI indexBlockIterator[I], D any, PD dataBlockIterator[D]] struct {
	ctx context.Context
	cmp Compare
	// Global lower/upper bound for the iterator.
	lower []byte
	upper []byte
	bpfs  *BlockPropertiesFilterer
	// Per-block lower/upper bound. Nil if the bound does not apply to the block
	// because we determined the block lies completely within the bound.
	blockLower []byte
	blockUpper []byte
	reader     *Reader
	// endKeyInclusive is set to force the iterator to treat the upper field as
	// inclusive while iterating instead of exclusive.
	endKeyInclusive       bool
	indexFilterRH         objstorage.ReadHandle
	indexFilterRHPrealloc objstorageprovider.PreallocatedReadHandle
	dataRH                objstorage.ReadHandle
	dataRHPrealloc        objstorageprovider.PreallocatedReadHandle
	// dataBH refers to the last data block that the iterator considered
	// loading. It may not actually have loaded the block, due to an error or
	// because it was considered irrelevant.
	dataBH                   block.Handle
	internalValueConstructor defaultInternalValueConstructor
	// vbRH is the read handle for value blocks, which are in a different
	// part of the sstable than data blocks.
	vbRH         objstorage.ReadHandle
	vbRHPrealloc objstorageprovider.PreallocatedReadHandle
	err          error
	closeHook    func()

	readEnv ReadEnv

	// boundsCmp and positionedUsingLatestBounds are for optimizing iteration
	// that uses multiple adjacent bounds. The seek after setting a new bound
	// can use the fact that the iterator is either within the previous bounds
	// or exactly one key before or after the bounds. If the new bounds is
	// after/before the previous bounds, and we are already positioned at a
	// block that is relevant for the new bounds, we can try to first position
	// using Next/Prev (repeatedly) instead of doing a more expensive seek.
	//
	// When there are wide files at higher levels that match the bounds
	// but don't have any data for the bound, we will already be
	// positioned at the key beyond the bounds and won't need to do much
	// work -- given that most data is in L6, such files are likely to
	// dominate the performance of the mergingIter, and may be the main
	// benefit of this performance optimization (of course it also helps
	// when the file that has the data has successive seeks that stay in
	// the same block).
	//
	// Specifically, boundsCmp captures the relationship between the previous
	// and current bounds, if the iterator had been positioned after setting
	// the previous bounds. If it was not positioned, i.e., Seek/First/Last
	// were not called, we don't know where it is positioned and cannot
	// optimize.
	//
	// Example: Bounds moving forward, and iterator exhausted in forward direction.
	//      bounds = [f, h), ^ shows block iterator position
	//  file contents [ a  b  c  d  e  f  g  h  i  j  k ]
	//                                       ^
	//  new bounds = [j, k). Since positionedUsingLatestBounds=true, boundsCmp is
	//  set to +1. SeekGE(j) can use next (the optimization also requires that j
	//  is within the block, but that is not for correctness, but to limit the
	//  optimization to when it will actually be an optimization).
	//
	// Example: Bounds moving forward.
	//      bounds = [f, h), ^ shows block iterator position
	//  file contents [ a  b  c  d  e  f  g  h  i  j  k ]
	//                                 ^
	//  new bounds = [j, k). Since positionedUsingLatestBounds=true, boundsCmp is
	//  set to +1. SeekGE(j) can use next.
	//
	// Example: Bounds moving forward, but iterator not positioned using previous
	//  bounds.
	//      bounds = [f, h), ^ shows block iterator position
	//  file contents [ a  b  c  d  e  f  g  h  i  j  k ]
	//                                             ^
	//  new bounds = [i, j). Iterator is at j since it was never positioned using
	//  [f, h). So positionedUsingLatestBounds=false, and boundsCmp is set to 0.
	//  SeekGE(i) will not use next.
	//
	// Example: Bounds moving forward and sparse file
	//      bounds = [f, h), ^ shows block iterator position
	//  file contents [ a z ]
	//                    ^
	//  new bounds = [j, k). Since positionedUsingLatestBounds=true, boundsCmp is
	//  set to +1. SeekGE(j) notices that the iterator is already past j and does
	//  not need to do anything.
	//
	// Similar examples can be constructed for backward iteration.
	//
	// This notion of exactly one key before or after the bounds is not quite
	// true when block properties are used to ignore blocks. In that case we
	// can't stop precisely at the first block that is past the bounds since
	// we are using the index entries to enforce the bounds.
	//
	// e.g. 3 blocks with keys [b, c]  [f, g], [i, j, k] with index entries d,
	// h, l. And let the lower bound be k, and we are reverse iterating. If
	// the block [i, j, k] is ignored due to the block interval annotations we
	// do need to move the index to block [f, g] since the index entry for the
	// [i, j, k] block is l which is not less than the lower bound of k. So we
	// have passed the entries i, j.
	//
	// This behavior is harmless since the block property filters are fixed
	// for the lifetime of the iterator so i, j are irrelevant. In addition,
	// the current code will not load the [f, g] block, so the seek
	// optimization that attempts to use Next/Prev do not apply anyway.
	boundsCmp                   int
	positionedUsingLatestBounds bool

	// exhaustedBounds represents whether the iterator is exhausted for
	// iteration by reaching the upper or lower bound. +1 when exhausted
	// the upper bound, -1 when exhausted the lower bound, and 0 when
	// neither. exhaustedBounds is also used for the TrySeekUsingNext
	// optimization in twoLevelIterator and singleLevelIterator. Care should be
	// taken in setting this in twoLevelIterator before calling into
	// singleLevelIterator, given that these two iterators share this field.
	exhaustedBounds int8

	// useFilterBlock controls whether the bloom filter block in this sstable, if
	// present, should be used for prefix seeks or not. In some cases it is
	// beneficial to skip a filter block even if it exists (eg. if probability of
	// a match is high).
	useFilterBlock         bool
	lastBloomFilterMatched bool

	transforms IterTransforms

	// All fields above this field are cleared when resetting the iterator for reuse.
	clearForResetBoundary struct{}

	index I
	data  D
	// inPool is set to true before putting the iterator in the reusable pool;
	// used to detect double-close.
	inPool bool
	// pool is the pool from which the iterator was allocated and to which the
	// iterator should be returned on Close. Because the iterator is
	// parameterized by the type of the data block iterator, pools must be
	// specific to the type of the data block iterator.
	//
	// If the iterator is embedded within a twoLevelIterator, pool is nil and
	// the twoLevelIterator.pool field may be non-nil.
	pool *sync.Pool

	// NOTE: any new fields should be added above the clearForResetBoundary field,
	// unless they need to be retained when resetting the iterator.
}

// singleLevelIterator implements the base.InternalIterator interface.
var _ base.InternalIterator = (*singleLevelIteratorRowBlocks)(nil)

// newColumnBlockSingleLevelIterator reads the index block and creates and
// initializes a singleLevelIterator over an sstable with column-oriented data
// blocks.
//
// Note that lower, upper are iterator bounds and are separate from virtual
// sstable bounds. If the virtualState passed in is not nil, then virtual
// sstable bounds will be enforced.
func newColumnBlockSingleLevelIterator(
	ctx context.Context, r *Reader, opts IterOptions,
) (*singleLevelIteratorColumnBlocks, error) {
	if r.err != nil {
		return nil, r.err
	}
	if !r.tableFormat.BlockColumnar() {
		panic(errors.AssertionFailedf("table format %d should not use columnar block format", r.tableFormat))
	}
	i := singleLevelIterColumnBlockPool.Get().(*singleLevelIteratorColumnBlocks)
	i.init(ctx, r, opts)
	if r.Attributes.Has(AttributeValueBlocks) {
		i.internalValueConstructor.vbReader = valblk.MakeReader(
			i, opts.ReaderProvider, r.valueBIH, opts.Env.Block.Stats, opts.Env.Block.IterStats)
		i.vbRH = r.blockReader.UsePreallocatedReadHandle(objstorage.NoReadBefore, &i.vbRHPrealloc)
	}
	i.data.InitOnce(r.keySchema, r.Comparer, &i.internalValueConstructor)
	indexH, err := r.readTopLevelIndexBlock(ctx, i.readEnv.Block, i.indexFilterRH)
	if err == nil {
		err = i.index.InitHandle(r.Comparer, indexH, opts.Transforms)
	}
	if err != nil {
		_ = i.Close()
		return nil, err
	}
	return i, nil
}

// newRowBlockSingleLevelIterator reads the index block and creates and
// initializes a singleLevelIterator over an sstable with row-oriented data
// blocks.
//
// Note that lower, upper are iterator bounds and are separate from virtual
// sstable bounds. If the virtualState passed in is not nil, then virtual
// sstable bounds will be enforced.
func newRowBlockSingleLevelIterator(
	ctx context.Context, r *Reader, opts IterOptions,
) (*singleLevelIteratorRowBlocks, error) {
	if r.err != nil {
		return nil, r.err
	}
	if r.tableFormat.BlockColumnar() {
		panic(errors.AssertionFailedf("table format %s uses block columnar format", r.tableFormat))
	}
	i := singleLevelIterRowBlockPool.Get().(*singleLevelIteratorRowBlocks)
	i.init(ctx, r, opts)
	if r.tableFormat >= TableFormatPebblev3 {
		if r.Attributes.Has(AttributeValueBlocks) {
			i.internalValueConstructor.vbReader = valblk.MakeReader(
				i, opts.ReaderProvider, r.valueBIH, opts.Env.Block.Stats, opts.Env.Block.IterStats)
			// We can set the GetLazyValuer directly to the vbReader because
			// rowblk sstables never contain blob value handles.
			(&i.data).SetGetLazyValuer(&i.internalValueConstructor.vbReader)
			i.vbRH = r.blockReader.UsePreallocatedReadHandle(objstorage.NoReadBefore, &i.vbRHPrealloc)
		}
		i.data.SetHasValuePrefix(true)
	}

	indexH, err := r.readTopLevelIndexBlock(ctx, i.readEnv.Block, i.indexFilterRH)
	if err == nil {
		err = i.index.InitHandle(r.Comparer, indexH, opts.Transforms)
	}
	if err != nil {
		_ = i.Close()
		return nil, err
	}
	return i, nil
}

// init initializes the singleLevelIterator struct. It does not read the index.
func (i *singleLevelIterator[I, PI, D, PD]) init(ctx context.Context, r *Reader, opts IterOptions) {
	i.inPool = false
	i.ctx = ctx
	i.lower = opts.Lower
	i.upper = opts.Upper
	i.bpfs = opts.Filterer
	i.useFilterBlock = shouldUseFilterBlock(r, opts.FilterBlockSizeLimit)
	i.reader = r
	i.cmp = r.Comparer.Compare
	i.transforms = opts.Transforms
	i.readEnv = opts.Env
	i.internalValueConstructor.blobContext = opts.BlobContext
	i.internalValueConstructor.env = &i.readEnv.Block
	if opts.Env.Virtual != nil {
		i.endKeyInclusive, i.lower, i.upper = opts.Env.Virtual.ConstrainBounds(opts.Lower, opts.Upper, false /* endInclusive */, r.Comparer.Compare)
	}

	i.indexFilterRH = r.blockReader.UsePreallocatedReadHandle(
		objstorage.ReadBeforeForIndexAndFilter, &i.indexFilterRHPrealloc)
	i.dataRH = r.blockReader.UsePreallocatedReadHandle(
		objstorage.NoReadBefore, &i.dataRHPrealloc)
}

// Helper function to check if keys returned from iterator are within virtual bounds.
func (i *singleLevelIterator[I, PI, D, PD]) maybeVerifyKey(kv *base.InternalKV) *base.InternalKV {
	if invariants.Enabled && kv != nil && i.readEnv.Virtual != nil {
		key := kv.K.UserKey
		v := i.readEnv.Virtual
		lc := i.cmp(key, v.Lower.UserKey)
		uc := i.cmp(key, v.Upper.UserKey)
		if lc < 0 || uc > 0 || (uc == 0 && v.Upper.IsExclusiveSentinel()) {
			panic(fmt.Sprintf("key %q out of singleLeveliterator virtual bounds %s %s", key, v.Lower.UserKey, v.Upper.UserKey))
		}
	}
	return kv
}

// SetupForCompaction sets up the singleLevelIterator for use with compactionIter.
// Currently, it skips readahead ramp-up. It should be called after init is called.
func (i *singleLevelIterator[I, PI, D, PD]) SetupForCompaction() {
	i.dataRH.SetupForCompaction()
	if i.vbRH != nil {
		i.vbRH.SetupForCompaction()
	}
}

const clearLen = unsafe.Offsetof(singleLevelIteratorRowBlocks{}.clearForResetBoundary)

// Assert that clearLen is consistent betwen the row and columnar implementations.
const clearLenColBlocks = unsafe.Offsetof(singleLevelIteratorColumnBlocks{}.clearForResetBoundary)
const _ uintptr = clearLen - clearLenColBlocks
const _ uintptr = clearLenColBlocks - clearLen

func (i *singleLevelIterator[I, PI, D, PD]) resetForReuse() {
	*(*[clearLen]byte)(unsafe.Pointer(i)) = [clearLen]byte{}
	i.inPool = true
}

func (i *singleLevelIterator[I, PI, D, PD]) initBounds() {
	// Trim the iteration bounds for the current block. We don't have to check
	// the bounds on each iteration if the block is entirely contained within the
	// iteration bounds.
	i.blockLower = i.lower
	if i.blockLower != nil {
		kv := PD(&i.data).First()
		// TODO(radu): this should be <= 0
		if kv != nil && i.cmp(i.blockLower, kv.K.UserKey) < 0 {
			// The lower-bound is less than the first key in the block. No need
			// to check the lower-bound again for this block.
			i.blockLower = nil
		}
	}
	i.blockUpper = i.upper
	// TODO(radu): this should be >= 0 if blockUpper is inclusive.
	if i.blockUpper != nil && PI(&i.index).SeparatorLT(i.blockUpper) {
		// The upper-bound is greater than the index key which itself is greater
		// than or equal to every key in the block. No need to check the
		// upper-bound again for this block. Even if blockUpper is inclusive
		// because of upper being inclusive, we can still safely set blockUpper
		// to nil here.
		i.blockUpper = nil
	}
}

func (i *singleLevelIterator[I, PI, D, PD]) initBoundsForAlreadyLoadedBlock() {
	// TODO(radu): determine automatically if we need to call First or not and
	// unify this function with initBounds().
	i.blockLower = i.lower
	if i.blockLower != nil && PD(&i.data).IsLowerBound(i.blockLower) {
		// The lower-bound is less than the first key in the block. No need
		// to check the lower-bound again for this block.
		i.blockLower = nil
	}
	i.blockUpper = i.upper
	// TODO(radu): this should be >= 0 if blockUpper is inclusive.
	if i.blockUpper != nil && PI(&i.index).SeparatorLT(i.blockUpper) {
		// The upper-bound is greater than the index key which itself is greater
		// than or equal to every key in the block. No need to check the
		// upper-bound again for this block.
		i.blockUpper = nil
	}
}

// Deterministic disabling (in testing mode) of the bounds-based optimization
// that avoids seeking. Uses the iterator pointer, since we want diversity in
// iterator behavior for the same SetBounds call. Used for tests.
func testingDisableBoundsOpt(bound []byte, ptr uintptr) bool {
	if !invariants.Enabled || ensureBoundsOptDeterminism {
		return false
	}
	// Fibonacci hash https://probablydance.com/2018/06/16/fibonacci-hashing-the-optimization-that-the-world-forgot-or-a-better-alternative-to-integer-modulo/
	simpleHash := (11400714819323198485 * uint64(ptr)) >> 63
	return bound[len(bound)-1]&byte(1) == 0 && simpleHash == 0
}

// ensureBoundsOptDeterminism provides a facility for disabling of the bounds
// optimizations performed by disableBoundsOpt for tests that require
// deterministic iterator behavior. Some unit tests examine internal iterator
// state and require this behavior to be deterministic.
var ensureBoundsOptDeterminism bool

// SetBoundsWithSyntheticPrefix indicates whether this iterator requires keys
// passed to its SetBounds() method by a prefix rewriting wrapper to be *not*
// rewritten to be in terms of this iterator's content, but instead be passed
// as-is, i.e. with the synthetic prefix still on them.
//
// This allows an optimization when this iterator is passing these bounds on to
// a vState to additionally constrain them. In said vState, passed bounds are
// combined with the vState bounds which are in terms of the rewritten prefix.
// If the caller rewrote bounds to be in terms of content prefix and SetBounds
// passed those to vState, the vState would need to *un*rewrite them back to the
// synthetic prefix in order to combine them with the vState bounds. Thus, if
// this iterator knows bounds will be passed to vState, it can signal that it
// they should be passed without being rewritten to skip converting to and fro.
func (i singleLevelIterator[I, PI, P, PD]) SetBoundsWithSyntheticPrefix() bool {
	return i.readEnv.Virtual != nil
}

// SetBounds implements internalIterator.SetBounds, as documented in the pebble
// package. Note that the upper field is exclusive.
func (i *singleLevelIterator[I, PI, P, PD]) SetBounds(lower, upper []byte) {
	i.boundsCmp = 0
	if i.readEnv.Virtual != nil {
		// If the reader is constructed for a virtual sstable, then we must
		// constrain the bounds of the reader. For physical sstables, the bounds
		// can be wider than the actual sstable's bounds because we won't
		// accidentally expose additional keys as there are no additional keys.
		i.endKeyInclusive, lower, upper = i.readEnv.Virtual.ConstrainBounds(
			lower, upper, false, i.reader.Comparer.Compare,
		)
	} else {
		// TODO(bananabrick): Figure out the logic here to enable the boundsCmp
		// optimization for virtual sstables.
		if i.positionedUsingLatestBounds {
			if i.upper != nil && lower != nil && i.cmp(i.upper, lower) <= 0 {
				i.boundsCmp = +1
				if testingDisableBoundsOpt(lower, uintptr(unsafe.Pointer(i))) {
					i.boundsCmp = 0
				}
			} else if i.lower != nil && upper != nil && i.cmp(upper, i.lower) <= 0 {
				i.boundsCmp = -1
				if testingDisableBoundsOpt(upper, uintptr(unsafe.Pointer(i))) {
					i.boundsCmp = 0
				}
			}
		}
	}

	i.positionedUsingLatestBounds = false
	i.lower = lower
	i.upper = upper
	i.blockLower = nil
	i.blockUpper = nil
}

func (i *singleLevelIterator[I, PI, P, PD]) SetContext(ctx context.Context) {
	i.ctx = ctx
}

// loadDataBlock loads the block at the current index position and leaves i.data
// unpositioned. If unsuccessful, it sets i.err to any error encountered, which
// may be nil if we have simply exhausted the entire table.
func (i *singleLevelIterator[I, PI, P, PD]) loadDataBlock(dir int8) loadBlockResult {
	if !PI(&i.index).Valid() {
		// Ensure the data block iterator is invalidated even if loading of the block
		// fails.
		PD(&i.data).Invalidate()
		return loadBlockFailed
	}
	// Load the next block.
	bhp, err := PI(&i.index).BlockHandleWithProperties()
	if i.dataBH == bhp.Handle && PD(&i.data).Valid() {
		// We're already at the data block we want to load. Reset bounds in case
		// they changed since the last seek, but don't reload the block from cache
		// or disk.
		//
		// It's safe to leave i.data in its original state here, as all callers to
		// loadDataBlock make an absolute positioning call (i.e. a seek, first, or last)
		// to `i.data` right after loadDataBlock returns loadBlockOK.
		i.initBounds()
		return loadBlockOK
	}
	// Ensure the data block iterator is invalidated even if loading of the block
	// fails.
	PD(&i.data).Invalidate()
	i.dataBH = bhp.Handle
	if err != nil {
		i.err = errCorruptIndexEntry(err)
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
	block, err := i.reader.readDataBlock(i.ctx, i.readEnv.Block, i.dataRH, i.dataBH)
	if err != nil {
		i.err = err
		return loadBlockFailed
	}
	i.err = PD(&i.data).InitHandle(i.reader.Comparer, block, i.transforms)
	if i.err != nil {
		// The block is partially loaded, and we don't want it to appear valid.
		PD(&i.data).Invalidate()
		return loadBlockFailed
	}
	i.initBounds()
	return loadBlockOK
}

// ReadValueBlock implements the valblk.BlockProviderWhenOpen interface for use
// by the valblk.IteratorBlockReader.
//
// TODO(sumeer): why does this have stats and catStats parameters, given this
// iter gave them to the valblk.Reader constructor? Remove.
func (i *singleLevelIterator[I, PI, D, PD]) ReadValueBlock(
	bh block.Handle, stats *base.InternalIteratorStats, catStats *block.CategoryStatsShard,
) (block.BufferHandle, error) {
	env := i.readEnv.Block
	env.Stats = stats
	env.IterStats = catStats
	return i.reader.readValueBlock(i.ctx, env, i.vbRH, bh)
}

// resolveMaybeExcluded is invoked when the block-property filterer has found
// that a block is excluded according to its properties but only if its bounds
// fall within the filter's current bounds.  This function consults the
// apprioriate bound, depending on the iteration direction, and returns either
// `blockIntersects` or `blockExcluded`.
func (i *singleLevelIterator[I, PI, D, PD]) resolveMaybeExcluded(dir int8) intersectsResult {
	// TODO(jackson): We could first try comparing to top-level index block's
	// key, and if within bounds avoid per-data block key comparisons.

	// This iterator is configured with a bound-limited block property
	// filter. The bpf determined this block could be excluded from
	// iteration based on the property encoded in the block handle.
	// However, we still need to determine if the block is wholly
	// contained within the filter's key bounds.
	//
	// External guarantees ensure all the block's keys are ≥ the
	// filter's lower bound during forward iteration, and that all the
	// block's keys are < the filter's upper bound during backward
	// iteration. We only need to determine if the opposite bound is
	// also met.
	//
	// The index separator in index.Key() provides an inclusive
	// upper-bound for the data block's keys, guaranteeing that all its
	// keys are ≤ index.Key(). For forward iteration, this is all we
	// need.
	if dir > 0 {
		// Forward iteration.
		if i.bpfs.boundLimitedFilter.KeyIsWithinUpperBound(PI(&i.index).Separator()) {
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
	// To establish a lower bound, we step the index backwards to read the
	// previous block's separator, which provides an inclusive lower bound on
	// the original block's keys. Afterwards, we step forward to restore our
	// index position.
	if !PI(&i.index).Prev() {
		// The original block points to the first block of this index block. If
		// there's a two-level index, it could potentially provide a lower
		// bound, but the code refactoring necessary to read it doesn't seem
		// worth the payoff. We fall through to loading the block.
	} else if i.bpfs.boundLimitedFilter.KeyIsWithinLowerBound(PI(&i.index).Separator()) {
		// The lower-bound on the original block falls within the filter's
		// bounds, and we can skip the block (after restoring our current index
		// position).
		_ = PI(&i.index).Next()
		return blockExcluded
	}
	_ = PI(&i.index).Next()
	return blockIntersects
}

// The number of times to call Next/Prev in a block before giving up and seeking.
// The value of 4 is arbitrary.
// TODO(sumeer): experiment with dynamic adjustment based on the history of
// seeks for a particular iterator.
const numStepsBeforeSeek = 4

func (i *singleLevelIterator[I, PI, D, PD]) trySeekGEUsingNextWithinBlock(
	key []byte,
) (kv *base.InternalKV, done bool) {
	kv = PD(&i.data).KV()
	for j := 0; j < numStepsBeforeSeek; j++ {
		curKeyCmp := i.cmp(kv.K.UserKey, key)
		if curKeyCmp >= 0 {
			if i.blockUpper != nil {
				cmp := i.cmp(kv.K.UserKey, i.blockUpper)
				if (!i.endKeyInclusive && cmp >= 0) || cmp > 0 {
					i.exhaustedBounds = +1
					return nil, true
				}
			}
			return kv, true
		}
		kv = PD(&i.data).Next()
		if kv == nil {
			break
		}
	}
	return kv, false
}

func (i *singleLevelIterator[I, PI, D, PD]) trySeekLTUsingPrevWithinBlock(
	key []byte,
) (kv *base.InternalKV, done bool) {
	kv = PD(&i.data).KV()
	for j := 0; j < numStepsBeforeSeek; j++ {
		curKeyCmp := i.cmp(kv.K.UserKey, key)
		if curKeyCmp < 0 {
			if i.blockLower != nil && i.cmp(kv.K.UserKey, i.blockLower) < 0 {
				i.exhaustedBounds = -1
				return nil, true
			}
			return kv, true
		}
		kv = PD(&i.data).Prev()
		if kv == nil {
			break
		}
	}
	return kv, false
}

// SeekGE implements internalIterator.SeekGE, as documented in the pebble
// package. Note that SeekGE only checks the upper bound. It is up to the
// caller to ensure that key is greater than or equal to the lower bound.
func (i *singleLevelIterator[I, PI, D, PD]) SeekGE(
	key []byte, flags base.SeekGEFlags,
) *base.InternalKV {
	if i.readEnv.Virtual != nil {
		// Callers of SeekGE don't know about virtual sstable bounds, so we may
		// have to internally restrict the bounds.
		//
		// TODO(bananabrick): We can optimize this check away for the level iter
		// if necessary.
		if i.cmp(key, i.lower) < 0 {
			key = i.lower
		}
	}

	if flags.TrySeekUsingNext() {
		// The i.exhaustedBounds comparison indicates that the upper bound was
		// reached. The i.data.isDataInvalidated() indicates that the sstable was
		// exhausted.
		if (i.exhaustedBounds == +1 || PD(&i.data).IsDataInvalidated()) && i.err == nil {
			// Already exhausted, so return nil.
			return nil
		}
		if i.err != nil {
			// The current iterator position cannot be used.
			flags = flags.DisableTrySeekUsingNext()
		}
		// INVARIANT: flags.TrySeekUsingNext() => i.err == nil &&
		// !i.exhaustedBounds==+1 && !i.data.isDataInvalidated(). That is,
		// data-exhausted and bounds-exhausted, as defined earlier, are both
		// false. Ths makes it safe to clear out i.exhaustedBounds and i.err
		// before calling into seekGEHelper.
	}

	i.exhaustedBounds = 0
	i.err = nil // clear cached iteration error
	boundsCmp := i.boundsCmp
	// Seek optimization only applies until iterator is first positioned after SetBounds.
	i.boundsCmp = 0
	i.positionedUsingLatestBounds = true
	return i.seekGEHelper(key, boundsCmp, flags)
}

// seekGEHelper contains the common functionality for SeekGE and SeekPrefixGE.
func (i *singleLevelIterator[I, PI, D, PD]) seekGEHelper(
	key []byte, boundsCmp int, flags base.SeekGEFlags,
) *base.InternalKV {
	// Invariant: trySeekUsingNext => !i.data.isDataInvalidated() && i.exhaustedBounds != +1

	// SeekGE performs various step-instead-of-seeking optimizations: eg enabled
	// by trySeekUsingNext, or by monotonically increasing bounds (i.boundsCmp).

	var dontSeekWithinBlock bool
	if !PD(&i.data).IsDataInvalidated() && PD(&i.data).Valid() && PI(&i.index).Valid() &&
		boundsCmp > 0 && PI(&i.index).SeparatorGT(key, true /* orEqual */) {
		// Fast-path: The bounds have moved forward and this SeekGE is
		// respecting the lower bound (guaranteed by Iterator). We know that the
		// iterator must already be positioned within or just outside the
		// previous bounds. Therefore it cannot be positioned at a block (or the
		// position within that block) that is ahead of the seek position.
		// However it can be positioned at an earlier block. This fast-path to
		// use Next() on the block is only applied when we are already at the
		// block that the slow-path (the else-clause) would load -- this is the
		// motivation for the IsSeparatorUpperBound(key, true) predicate.
		i.initBoundsForAlreadyLoadedBlock()
		kv, done := i.trySeekGEUsingNextWithinBlock(key)
		if done {
			return kv
		}
		if kv == nil {
			// Done with this block.
			dontSeekWithinBlock = true
		}
	} else {
		// Cannot use bounds monotonicity. But may be able to optimize if
		// caller claimed externally known invariant represented by
		// flags.TrySeekUsingNext().
		if flags.TrySeekUsingNext() {
			// seekPrefixGE or SeekGE has already ensured
			// !i.data.isDataInvalidated() && i.exhaustedBounds != +1
			curr := PD(&i.data).KV()
			less := i.cmp(curr.K.UserKey, key) < 0
			// We could be more sophisticated and confirm that the seek
			// position is within the current block before applying this
			// optimization. But there may be some benefit even if it is in
			// the next block, since we can avoid seeking i.index.
			for j := 0; less && j < numStepsBeforeSeek; j++ {
				curr = i.Next()
				if curr == nil {
					return nil
				}
				less = i.cmp(curr.K.UserKey, key) < 0
			}
			if !less {
				if i.blockUpper != nil {
					cmp := i.cmp(curr.K.UserKey, i.blockUpper)
					if (!i.endKeyInclusive && cmp >= 0) || cmp > 0 {
						i.exhaustedBounds = +1
						return nil
					}
				}
				return curr
			}
		}

		// Slow-path.

		if !PI(&i.index).SeekGE(key) {
			// The target key is greater than any key in the index block.
			// Invalidate the block iterator so that a subsequent call to Prev()
			// will return the last key in the table.
			PD(&i.data).Invalidate()
			return nil
		}
		result := i.loadDataBlock(+1)
		if result == loadBlockFailed {
			return nil
		}
		if result == loadBlockIrrelevant {
			// Enforce the upper bound here since don't want to bother moving to
			// the next block if upper bound is already exceeded. Note that the
			// next block may start with keys >= index.Separator() since even
			// though this is the block separator, the same user key can span
			// multiple blocks. If upper is exclusive we pass orEqual=true
			// below, else we require the separator to be strictly greater than
			// upper.
			if i.upper != nil && PI(&i.index).SeparatorGT(i.upper, !i.endKeyInclusive) {
				i.exhaustedBounds = +1
				return nil
			}
			// Want to skip to the next block.
			dontSeekWithinBlock = true
		}
	}
	if !dontSeekWithinBlock {
		if ikv := PD(&i.data).SeekGE(key, flags.DisableTrySeekUsingNext()); ikv != nil {
			if i.blockUpper != nil {
				cmp := i.cmp(ikv.K.UserKey, i.blockUpper)
				if (!i.endKeyInclusive && cmp >= 0) || cmp > 0 {
					i.exhaustedBounds = +1
					return nil
				}
			}
			return ikv
		}
	}
	return i.skipForward()
}

// SeekPrefixGE implements internalIterator.SeekPrefixGE, as documented in the
// pebble package. Note that SeekPrefixGE only checks the upper bound. It is up
// to the caller to ensure that key is greater than or equal to the lower bound.
func (i *singleLevelIterator[I, PI, D, PD]) SeekPrefixGE(
	prefix, key []byte, flags base.SeekGEFlags,
) *base.InternalKV {
	if i.readEnv.Virtual != nil {
		// Callers of SeekPrefixGE aren't aware of virtual sstable bounds, so
		// we may have to internally restrict the bounds.
		//
		// TODO(bananabrick): We can optimize away this check for the level iter
		// if necessary.
		if i.cmp(key, i.lower) < 0 {
			key = i.lower
		}
	}
	return i.seekPrefixGE(prefix, key, flags)
}

func (i *singleLevelIterator[I, PI, D, PD]) seekPrefixGE(
	prefix, key []byte, flags base.SeekGEFlags,
) (kv *base.InternalKV) {
	// NOTE: prefix is only used for bloom filter checking and not later work in
	// this method. Hence, we can use the existing iterator position if the last
	// SeekPrefixGE did not fail bloom filter matching.

	err := i.err
	i.err = nil // clear cached iteration error
	if i.useFilterBlock {
		if !i.lastBloomFilterMatched {
			// Iterator is not positioned based on last seek.
			flags = flags.DisableTrySeekUsingNext()
		}
		i.lastBloomFilterMatched = false
		// Check prefix bloom filter.
		var mayContain bool
		mayContain, i.err = i.bloomFilterMayContain(prefix)
		if i.err != nil || !mayContain {
			// In the i.err == nil case, this invalidation may not be necessary for
			// correctness, and may be a place to optimize later by reusing the
			// already loaded block. It was necessary in earlier versions of the code
			// since the caller was allowed to call Next when SeekPrefixGE returned
			// nil. This is no longer allowed.
			PD(&i.data).Invalidate()
			return nil
		}
		i.lastBloomFilterMatched = true
	}
	if flags.TrySeekUsingNext() {
		// The i.exhaustedBounds comparison indicates that the upper bound was
		// reached. The i.data.isDataInvalidated() indicates that the sstable was
		// exhausted.
		if (i.exhaustedBounds == +1 || PD(&i.data).IsDataInvalidated()) && err == nil {
			// Already exhausted, so return nil.
			return nil
		}
		if err != nil {
			// The current iterator position cannot be used.
			flags = flags.DisableTrySeekUsingNext()
		}
		// INVARIANT: flags.TrySeekUsingNext() => err == nil &&
		// !i.exhaustedBounds==+1 && !i.data.isDataInvalidated(). That is,
		// data-exhausted and bounds-exhausted, as defined earlier, are both
		// false. Ths makes it safe to clear out i.exhaustedBounds and i.err
		// before calling into seekGEHelper.
	}
	// Bloom filter matches, or skipped, so this method will position the
	// iterator.
	i.exhaustedBounds = 0
	boundsCmp := i.boundsCmp
	// Seek optimization only applies until iterator is first positioned after SetBounds.
	i.boundsCmp = 0
	i.positionedUsingLatestBounds = true
	return i.maybeVerifyKey(i.seekGEHelper(key, boundsCmp, flags))
}

// shouldUseFilterBlock returns whether we should use the filter block, based on
// its length and the size limit.
func shouldUseFilterBlock(reader *Reader, filterBlockSizeLimit FilterBlockSizeLimit) bool {
	return reader.tableFilter != nil && reader.filterBH.Length <= uint64(filterBlockSizeLimit)
}

func (i *singleLevelIterator[I, PI, D, PD]) bloomFilterMayContain(prefix []byte) (bool, error) {
	// Check prefix bloom filter.
	prefixToCheck := prefix
	if i.transforms.HasSyntheticPrefix() {
		// We have to remove the synthetic prefix.
		var ok bool
		prefixToCheck, ok = bytes.CutPrefix(prefix, i.transforms.SyntheticPrefix())
		if !ok {
			// This prefix will not be found inside this table.
			return false, nil
		}
	}

	dataH, err := i.reader.readFilterBlock(i.ctx, i.readEnv.Block, i.indexFilterRH, i.reader.filterBH)
	if err != nil {
		return false, err
	}
	defer dataH.Release()
	return i.reader.tableFilter.mayContain(dataH.BlockData(), prefixToCheck), nil
}

// virtualLast should only be called if i.readBlockEnv.Virtual != nil
func (i *singleLevelIterator[I, PI, D, PD]) virtualLast() *base.InternalKV {
	if i.readEnv.Virtual == nil {
		panic("pebble: invalid call to virtualLast")
	}

	if !i.endKeyInclusive {
		// Trivial case.
		return i.SeekLT(i.upper, base.SeekLTFlagsNone)
	}
	return i.virtualLastSeekLE()
}

// virtualLastSeekLE is called by virtualLast to do a SeekLE as part of a
// virtualLast. Consider generalizing this into a SeekLE() if there are other
// uses of this method in the future. Does a SeekLE on the upper bound of the
// file/iterator.
func (i *singleLevelIterator[I, PI, D, PD]) virtualLastSeekLE() *base.InternalKV {
	// Callers of SeekLE don't know about virtual sstable bounds, so we may
	// have to internally restrict the bounds.
	//
	// TODO(bananabrick): We can optimize this check away for the level iter
	// if necessary.
	if !i.endKeyInclusive {
		panic("unexpected virtualLastSeekLE with exclusive upper bounds")
	}
	key := i.upper

	i.exhaustedBounds = 0
	i.err = nil // clear cached iteration error
	// Seek optimization only applies until iterator is first positioned with a
	// SeekGE or SeekLT after SetBounds.
	i.boundsCmp = 0
	i.positionedUsingLatestBounds = true

	indexOk := PI(&i.index).SeekGE(key)
	// We can have multiple internal keys with the same user key as the seek
	// key. In that case, we want the last (greatest) internal key.
	//
	// INVARIANT: One of two cases:
	// A. !indexOk. There is no data block with index key >= key. So all keys
	//    in the last data block are < key.
	// B. i.index.Separator() >= key. This data block may have some keys > key.
	//
	// Subcases of B:
	//   B1. Separator() == key. This is when loop iteration happens.
	//       Since Separator() >= largest data key in the block, the largest data
	//       key in this block is <= key.
	//   B2. Separator() > key. Loop iteration will not happen.
	//
	// NB: We can avoid this Next()ing if we just implement a blockIter.SeekLE().
	// This might be challenging to do correctly, so impose regular operations
	// for now.
	// TODO(jackson): Consider implementing SeekLE since it's easier to do in
	// colblk.
	for indexOk && bytes.Equal(PI(&i.index).Separator(), key) {
		indexOk = PI(&i.index).Next()
	}
	if !indexOk {
		// Cases A or B1 where B1 exhausted all blocks. In both cases the last block
		// has all keys <= key. skipBackward enforces the lower bound.
		return i.skipBackward()
	}
	// Case B. We are here because we were originally in case B2, or we were in B1
	// and we arrived at a block where ikey.UserKey > key. Either way, ikey.UserKey
	// > key. So there could be keys in the block > key. But the block preceding
	// this block cannot have any keys > key, otherwise it would have been the
	// result of the original index.SeekGE.
	result := i.loadDataBlock(-1)
	if result == loadBlockFailed {
		return nil
	}
	if result == loadBlockIrrelevant {
		// Want to skip to the previous block.
		return i.skipBackward()
	}
	ikv := PD(&i.data).SeekGE(key, base.SeekGEFlagsNone)
	// Go to the last user key that matches key, and then Prev() on the data
	// block.
	for ikv != nil && bytes.Equal(ikv.K.UserKey, key) {
		ikv = PD(&i.data).Next()
	}
	ikv = PD(&i.data).Prev()
	if ikv != nil {
		// Enforce the lower bound here, as we could have gone past it. This happens
		// if keys between `i.blockLower` and `key` are obsolete, for instance. Even
		// though i.blockLower (which is either nil or equal to i.lower) is <= key,
		// all internal keys in the user key interval [i.blockLower, key] could be
		// obsolete (due to a RANGEDEL which will not be observed here). And
		// i.data.Prev will skip all these obsolete keys, and could land on a key
		// below the lower bound, requiring the lower bound check.
		if i.blockLower != nil && i.cmp(ikv.K.UserKey, i.blockLower) < 0 {
			i.exhaustedBounds = -1
			return nil
		}
		return ikv
	}
	return i.skipBackward()
}

// SeekLT implements internalIterator.SeekLT, as documented in the pebble
// package. Note that SeekLT only checks the lower bound. It is up to the
// caller to ensure that key is less than or equal to the upper bound.
func (i *singleLevelIterator[I, PI, D, PD]) SeekLT(
	key []byte, flags base.SeekLTFlags,
) *base.InternalKV {
	if i.readEnv.Virtual != nil {
		// Might have to fix upper bound since virtual sstable bounds are not
		// known to callers of SeekLT.
		//
		// TODO(bananabrick): We can optimize away this check for the level iter
		// if necessary.
		cmp := i.cmp(key, i.upper)
		// key == i.upper is fine. We'll do the right thing and return the
		// first internal key with user key < key.
		if cmp > 0 {
			// Return the last key in the virtual sstable.
			return i.maybeVerifyKey(i.virtualLast())
		}
	}

	i.exhaustedBounds = 0
	i.err = nil // clear cached iteration error
	boundsCmp := i.boundsCmp
	// Seek optimization only applies until iterator is first positioned after SetBounds.
	i.boundsCmp = 0

	// Seeking operations perform various step-instead-of-seeking optimizations:
	// eg by considering monotonically increasing bounds (i.boundsCmp).

	i.positionedUsingLatestBounds = true

	var dontSeekWithinBlock bool
	if !PD(&i.data).IsDataInvalidated() && PD(&i.data).Valid() && PI(&i.index).Valid() &&
		boundsCmp < 0 && !PD(&i.data).IsLowerBound(key) {
		// Fast-path: The bounds have moved backward, and this SeekLT is
		// respecting the upper bound (guaranteed by Iterator). We know that
		// the iterator must already be positioned within or just outside the
		// previous bounds. Therefore it cannot be positioned at a block (or
		// the position within that block) that is behind the seek position.
		// However it can be positioned at a later block. This fast-path to
		// use Prev() on the block is only applied when we are already at the
		// block that can satisfy this seek -- this is the motivation for the
		// the i.cmp(i.data.firstKey.UserKey, key) < 0 predicate.
		i.initBoundsForAlreadyLoadedBlock()
		ikv, done := i.trySeekLTUsingPrevWithinBlock(key)
		if done {
			return ikv
		}
		if ikv == nil {
			// Done with this block.
			dontSeekWithinBlock = true
		}
	} else {
		// Slow-path.

		// NB: If a bound-limited block property filter is configured, it's
		// externally ensured that the filter is disabled (through returning
		// Intersects=false irrespective of the block props provided) during
		// seeks.
		if !PI(&i.index).SeekGE(key) {
			if !PI(&i.index).Last() {
				return nil
			}
		}
		// INVARIANT: ikey != nil.
		result := i.loadDataBlock(-1)
		if result == loadBlockFailed {
			return nil
		}
		if result == loadBlockIrrelevant {
			// Enforce the lower bound here since don't want to bother moving
			// to the previous block if lower bound is already exceeded. Note
			// that the previous block starts with keys <= ikey.UserKey since
			// even though this is the current block's separator, the same
			// user key can span multiple blocks.
			if i.lower != nil && PI(&i.index).SeparatorLT(i.lower) {
				i.exhaustedBounds = -1
				return nil
			}
			// Want to skip to the previous block.
			dontSeekWithinBlock = true
		}
	}
	if !dontSeekWithinBlock {
		if ikv := PD(&i.data).SeekLT(key, flags); ikv != nil {
			if i.blockLower != nil && i.cmp(ikv.K.UserKey, i.blockLower) < 0 {
				i.exhaustedBounds = -1
				return nil
			}
			return ikv
		}
	}
	// The index contains separator keys which may lie between
	// user-keys. Consider the user-keys:
	//
	//   complete
	// ---- new block ---
	//   complexion
	//
	// If these two keys end one block and start the next, the index key may
	// be chosen as "compleu". The SeekGE in the index block will then point
	// us to the block containing "complexion". If this happens, we want the
	// last key from the previous data block.
	return i.maybeVerifyKey(i.skipBackward())
}

// First implements internalIterator.First, as documented in the pebble
// package. Note that First only checks the upper bound. It is up to the caller
// to ensure that key is greater than or equal to the lower bound (e.g. via a
// call to SeekGE(lower)).
func (i *singleLevelIterator[I, PI, D, PD]) First() *base.InternalKV {
	// If we have a lower bound, use SeekGE. Note that in general this is not
	// supported usage, except when the lower bound is there because the table is
	// virtual.
	if i.lower != nil {
		return i.SeekGE(i.lower, base.SeekGEFlagsNone)
	}

	i.positionedUsingLatestBounds = true

	return i.firstInternal()
}

// firstInternal is a helper used for absolute positioning in a single-level
// index file, or for positioning in the second-level index in a two-level
// index file. For the latter, one cannot make any claims about absolute
// positioning.
func (i *singleLevelIterator[I, PI, D, PD]) firstInternal() *base.InternalKV {
	i.exhaustedBounds = 0
	i.err = nil // clear cached iteration error
	// Seek optimization only applies until iterator is first positioned after SetBounds.
	i.boundsCmp = 0

	if !PI(&i.index).First() {
		PD(&i.data).Invalidate()
		return nil
	}
	result := i.loadDataBlock(+1)
	if result == loadBlockFailed {
		return nil
	}
	if result == loadBlockOK {
		if kv := PD(&i.data).First(); kv != nil {
			if i.blockUpper != nil {
				cmp := i.cmp(kv.K.UserKey, i.blockUpper)
				if (!i.endKeyInclusive && cmp >= 0) || cmp > 0 {
					i.exhaustedBounds = +1
					return nil
				}
			}
			return kv
		}
		// Else fall through to skipForward.
	} else {
		// result == loadBlockIrrelevant. Enforce the upper bound here since
		// don't want to bother moving to the next block if upper bound is
		// already exceeded. Note that the next block may start with keys >=
		// index.Separator() since even though this is the block separator, the
		// same user key can span multiple blocks. If upper is exclusive we pass
		// orEqual=true below, else we require the separator to be strictly
		// greater than upper.
		if i.upper != nil && PI(&i.index).SeparatorGT(i.upper, !i.endKeyInclusive) {
			i.exhaustedBounds = +1
			return nil
		}
		// Else fall through to skipForward.
	}

	return i.skipForward()
}

// Last implements internalIterator.Last, as documented in the pebble
// package. Note that Last only checks the lower bound. It is up to the caller
// to ensure that key is less than the upper bound (e.g. via a call to
// SeekLT(upper))
func (i *singleLevelIterator[I, PI, D, PD]) Last() *base.InternalKV {
	if i.readEnv.Virtual != nil {
		return i.maybeVerifyKey(i.virtualLast())
	}

	if i.upper != nil {
		panic("singleLevelIterator.Last() used despite upper bound")
	}
	i.positionedUsingLatestBounds = true
	return i.lastInternal()
}

// lastInternal is a helper used for absolute positioning in a single-level
// index file, or for positioning in the second-level index in a two-level
// index file. For the latter, one cannot make any claims about absolute
// positioning.
func (i *singleLevelIterator[I, PI, D, PD]) lastInternal() *base.InternalKV {
	i.exhaustedBounds = 0
	i.err = nil // clear cached iteration error
	// Seek optimization only applies until iterator is first positioned after SetBounds.
	i.boundsCmp = 0

	if !PI(&i.index).Last() {
		PD(&i.data).Invalidate()
		return nil
	}
	result := i.loadDataBlock(-1)
	if result == loadBlockFailed {
		return nil
	}
	if result == loadBlockOK {
		if ikv := PD(&i.data).Last(); ikv != nil {
			if i.blockLower != nil && i.cmp(ikv.K.UserKey, i.blockLower) < 0 {
				i.exhaustedBounds = -1
				return nil
			}
			return ikv
		}
		// Else fall through to skipBackward.
	} else {
		// result == loadBlockIrrelevant. Enforce the lower bound here since
		// don't want to bother moving to the previous block if lower bound is
		// already exceeded. Note that the previous block starts with keys <=
		// key.UserKey since even though this is the current block's
		// separator, the same user key can span multiple blocks.
		if i.lower != nil && PI(&i.index).SeparatorLT(i.lower) {
			i.exhaustedBounds = -1
			return nil
		}
	}

	return i.skipBackward()
}

// Next implements internalIterator.Next, as documented in the pebble
// package.
// Note: compactionIterator.Next mirrors the implementation of Iterator.Next
// due to performance. Keep the two in sync.
func (i *singleLevelIterator[I, PI, D, PD]) Next() *base.InternalKV {
	if i.exhaustedBounds == +1 {
		panic("Next called even though exhausted upper bound")
	}
	i.exhaustedBounds = 0
	// Seek optimization only applies until iterator is first positioned after SetBounds.
	i.boundsCmp = 0

	if i.err != nil {
		// TODO(jackson): Can this case be turned into a panic? Once an error is
		// encountered, the iterator must be re-seeked.
		return nil
	}
	if kv := PD(&i.data).Next(); kv != nil {
		if i.blockUpper != nil {
			cmp := i.cmp(kv.K.UserKey, i.blockUpper)
			if (!i.endKeyInclusive && cmp >= 0) || cmp > 0 {
				i.exhaustedBounds = +1
				return nil
			}
		}
		return kv
	}
	return i.skipForward()
}

// NextPrefix implements (base.InternalIterator).NextPrefix.
func (i *singleLevelIterator[I, PI, D, PD]) NextPrefix(succKey []byte) *base.InternalKV {
	if i.exhaustedBounds == +1 {
		panic("NextPrefix called even though exhausted upper bound")
	}
	i.exhaustedBounds = 0
	// Seek optimization only applies until iterator is first positioned after SetBounds.
	i.boundsCmp = 0
	if i.err != nil {
		// TODO(jackson): Can this case be turned into a panic? Once an error is
		// encountered, the iterator must be re-seeked.
		return nil
	}
	if kv := PD(&i.data).NextPrefix(succKey); kv != nil {
		if i.blockUpper != nil {
			cmp := i.cmp(kv.K.UserKey, i.blockUpper)
			if (!i.endKeyInclusive && cmp >= 0) || cmp > 0 {
				i.exhaustedBounds = +1
				return nil
			}
		}
		return kv
	}
	// Did not find prefix in the existing data block. This is the slow-path
	// where we effectively seek the iterator.
	// The key is likely to be in the next data block, so try one step.
	if !PI(&i.index).Next() {
		// The target key is greater than any key in the index block.
		// Invalidate the block iterator so that a subsequent call to Prev()
		// will return the last key in the table.
		PD(&i.data).Invalidate()
		return nil
	}
	if PI(&i.index).SeparatorLT(succKey) {
		// Not in the next data block, so seek the index.
		if !PI(&i.index).SeekGE(succKey) {
			// The target key is greater than any key in the index block.
			// Invalidate the block iterator so that a subsequent call to Prev()
			// will return the last key in the table.
			PD(&i.data).Invalidate()
			return nil
		}
	}
	result := i.loadDataBlock(+1)
	if result == loadBlockFailed {
		return nil
	}
	if result == loadBlockIrrelevant {
		// Enforce the upper bound here since don't want to bother moving to the
		// next block if upper bound is already exceeded. Note that the next
		// block may start with keys >= index.Separator() since even though this
		// is the block separator, the same user key can span multiple blocks.
		// If upper is exclusive we pass orEqual=true below, else we require
		// the separator to be strictly greater than upper.
		if i.upper != nil && PI(&i.index).SeparatorGT(i.upper, !i.endKeyInclusive) {
			i.exhaustedBounds = +1
			return nil
		}
	} else if kv := PD(&i.data).SeekGE(succKey, base.SeekGEFlagsNone); kv != nil {
		if i.blockUpper != nil {
			cmp := i.cmp(kv.K.UserKey, i.blockUpper)
			if (!i.endKeyInclusive && cmp >= 0) || cmp > 0 {
				i.exhaustedBounds = +1
				return nil
			}
		}
		return i.maybeVerifyKey(kv)
	}

	return i.skipForward()
}

// Prev implements internalIterator.Prev, as documented in the pebble
// package.
func (i *singleLevelIterator[I, PI, D, PD]) Prev() *base.InternalKV {
	if i.exhaustedBounds == -1 {
		panic("Prev called even though exhausted lower bound")
	}
	i.exhaustedBounds = 0
	// Seek optimization only applies until iterator is first positioned after SetBounds.
	i.boundsCmp = 0

	if i.err != nil {
		return nil
	}
	if kv := PD(&i.data).Prev(); kv != nil {
		if i.blockLower != nil && i.cmp(kv.K.UserKey, i.blockLower) < 0 {
			i.exhaustedBounds = -1
			return nil
		}
		return kv
	}
	return i.skipBackward()
}

func (i *singleLevelIterator[I, PI, D, PD]) skipForward() *base.InternalKV {
	for {
		if !PI(&i.index).Next() {
			PD(&i.data).Invalidate()
			break
		}
		result := i.loadDataBlock(+1)
		if result != loadBlockOK {
			if i.err != nil {
				break
			}
			if result == loadBlockFailed {
				// We checked that i.index was at a valid entry, so
				// loadBlockFailed could not have happened due to i.index
				// being exhausted, and must be due to an error.
				panic("loadDataBlock should not have failed with no error")
			}
			// result == loadBlockIrrelevant. Enforce the upper bound here since
			// don't want to bother moving to the next block if upper bound is
			// already exceeded. Note that the next block may start with keys >=
			// index.Separator() since even though this is the block separator,
			// the same user key can span multiple blocks. If upper is exclusive
			// we pass orEqual=true below, else we require the separator to be
			// strictly greater than upper.
			if i.upper != nil && PI(&i.index).SeparatorGT(i.upper, !i.endKeyInclusive) {
				i.exhaustedBounds = +1
				return nil
			}
			continue
		}
		var kv *base.InternalKV
		// It is possible that skipBackward went too far and the virtual table lower
		// bound is after the first key in the block we are about to load, in which
		// case we must use SeekGE.
		//
		// An example of how this can happen:
		//
		//   Data block 1 - contains keys a@1, c@1
		//   Data block 2 - contains keys e@1, g@1
		//   Data block 3 - contains keys i@2, k@2
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
		if i.readEnv.Virtual != nil && i.blockLower != nil {
			kv = PD(&i.data).SeekGE(i.lower, base.SeekGEFlagsNone)
		} else {
			kv = PD(&i.data).First()
		}
		if kv != nil {
			if i.blockUpper != nil {
				cmp := i.cmp(kv.K.UserKey, i.blockUpper)
				if (!i.endKeyInclusive && cmp >= 0) || cmp > 0 {
					i.exhaustedBounds = +1
					return nil
				}
			}
			return i.maybeVerifyKey(kv)
		}
	}
	return nil
}

func (i *singleLevelIterator[I, PI, D, PD]) skipBackward() *base.InternalKV {
	for {
		if !PI(&i.index).Prev() {
			PD(&i.data).Invalidate()
			break
		}
		result := i.loadDataBlock(-1)
		if result != loadBlockOK {
			if i.err != nil {
				break
			}
			if result == loadBlockFailed {
				// We checked that i.index was at a valid entry, so
				// loadBlockFailed could not have happened due to to i.index
				// being exhausted, and must be due to an error.
				panic("loadDataBlock should not have failed with no error")
			}
			// result == loadBlockIrrelevant. Enforce the lower bound here
			// since don't want to bother moving to the previous block if lower
			// bound is already exceeded. Note that the previous block starts with
			// keys <= key.UserKey since even though this is the current block's
			// separator, the same user key can span multiple blocks.
			if i.lower != nil && PI(&i.index).SeparatorLT(i.lower) {
				i.exhaustedBounds = -1
				return nil
			}
			continue
		}
		kv := PD(&i.data).Last()
		if kv == nil {
			// The block iter could have hid some obsolete points, so it isn't
			// safe to assume that there are no keys if we keep skipping backwards.
			// Check the previous block, but check the lower bound before doing
			// that.
			if i.lower != nil && PI(&i.index).SeparatorLT(i.lower) {
				i.exhaustedBounds = -1
				return nil
			}
			continue
		}
		if i.blockLower != nil && i.cmp(kv.K.UserKey, i.blockLower) < 0 {
			i.exhaustedBounds = -1
			return nil
		}
		return i.maybeVerifyKey(kv)
	}
	return nil
}

// Error implements internalIterator.Error, as documented in the pebble
// package.
func (i *singleLevelIterator[I, PI, D, PD]) Error() error {
	if err := PD(&i.data).Error(); err != nil {
		return err
	}
	return i.err
}

// SetCloseHook sets a function that will be called when the iterator is closed.
// This is used by the file cache to release the reference count on the open
// sstable.Reader when the iterator is closed.
func (i *singleLevelIterator[I, PI, D, PD]) SetCloseHook(fn func()) {
	i.closeHook = fn
}

func firstError(err0, err1 error) error {
	if err0 != nil {
		return err0
	}
	return err1
}

// Close implements internalIterator.Close, as documented in the pebble
// package.
func (i *singleLevelIterator[I, PI, D, PD]) Close() error {
	err := i.closeInternal()
	pool := i.pool
	i.resetForReuse()
	if pool != nil {
		pool.Put(i)
	}
	return err
}

func (i *singleLevelIterator[I, PI, D, PD]) closeInternal() error {
	if invariants.Enabled && i.inPool {
		panic("Close called on interator in pool")
	}

	if i.closeHook != nil {
		i.closeHook()
	}
	var err error
	err = firstError(err, PD(&i.data).Close())
	err = firstError(err, PI(&i.index).Close())
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
	i.internalValueConstructor.vbReader.Close()
	if i.vbRH != nil {
		err = firstError(err, i.vbRH.Close())
		i.vbRH = nil
	}
	return err
}

func (i *singleLevelIterator[I, PI, D, PD]) String() string {
	if i.readEnv.Virtual != nil {
		return i.readEnv.Virtual.FileNum.String()
	}
	return i.reader.blockReader.FileNum().String()
}

// DebugTree is part of the InternalIterator interface.
func (i *singleLevelIterator[I, PI, D, PD]) DebugTree(tp treeprinter.Node) {
	tp.Childf("%T(%p) fileNum=%s", i, i, i.String())
}
