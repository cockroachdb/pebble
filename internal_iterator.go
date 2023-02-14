// Copyright 2023 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package pebble

import (
	"github.com/cockroachdb/pebble/internal/base"
	"github.com/cockroachdb/pebble/internal/keyspan"
	"github.com/cockroachdb/pebble/internal/manifest"
)

// InternalIterator exports an externally-usable Iterator that returns all
// internal keys instead of collapsing them by user keys. For instance, an
// InternalKeyKindDelete would be returned as an InternalKeyKindDelete instead of
// the iterator skipping over to the next key. Useful if an external user of
// Pebble needs to observe and rebuild Pebble's history of internal keys, such as
// in node-to-node replication.
type InternalIterator struct {
	opts            IterOptions
	comparer        *base.Comparer
	iter            internalIterator
	readState       *readState
	rangeKey        *iteratorRangeKeyState
	pointKeyIter    keyspan.InterleavingIter
	iterKey         *InternalKey
	iterValue       LazyValue
	alloc           *iterAlloc
	newIters        tableNewIters
	newIterRangeKey keyspan.TableNewSpanIter
	seqNum          uint64
	lazyValueBuf    []byte

	// boundsBuf holds two buffers used to store the lower and upper bounds.
	// Whenever the InternalIterator's bounds change, the new bounds are copied
	// into boundsBuf[boundsBufIdx]. The two bounds share a slice to reduce
	// allocations. opts.LowerBound and opts.UpperBound point into this slice.
	boundsBuf    [2][]byte
	boundsBufIdx int
}

// constructPointIter constructs a merging iterator and sets i.iter to it.
func (i *InternalIterator) constructPointIter(memtables flushableList, buf *iterAlloc) {
	// Merging levels and levels from iterAlloc.
	mlevels := buf.mlevels[:0]
	levels := buf.levels[:0]

	// We compute the number of levels needed ahead of time and reallocate a slice if
	// the array from the iterAlloc isn't large enough. Doing this allocation once
	// should improve the performance.
	numMergingLevels := len(memtables)
	numLevelIters := 0

	current := i.readState.current
	numMergingLevels += len(current.L0SublevelFiles)
	numLevelIters += len(current.L0SublevelFiles)
	for level := 1; level < len(current.Levels); level++ {
		if current.Levels[level].Empty() {
			continue
		}
		numMergingLevels++
		numLevelIters++
	}

	if numMergingLevels > cap(mlevels) {
		mlevels = make([]mergingIterLevel, 0, numMergingLevels)
	}
	if numLevelIters > cap(levels) {
		levels = make([]levelIter, 0, numLevelIters)
	}
	// TODO(bilal): Push these into the iterAlloc buf.
	var rangeDelMiter keyspan.MergingIter
	rangeDelIters := make([]keyspan.FragmentIterator, 0, numMergingLevels)
	rangeDelLevels := make([]keyspan.LevelIter, 0, numLevelIters)

	// Next are the memtables.
	for j := len(memtables) - 1; j >= 0; j-- {
		mem := memtables[j]
		mlevels = append(mlevels, mergingIterLevel{
			iter: mem.newIter(&i.opts),
		})
		if rdi := mem.newRangeDelIter(&i.opts); rdi != nil {
			rangeDelIters = append(rangeDelIters, rdi)
		}
	}

	// Next are the file levels: L0 sub-levels followed by lower levels.
	mlevelsIndex := len(mlevels)
	levelsIndex := len(levels)
	mlevels = mlevels[:numMergingLevels]
	levels = levels[:numLevelIters]
	rangeDelLevels = rangeDelLevels[:numLevelIters]
	addLevelIterForFiles := func(files manifest.LevelIterator, level manifest.Level) {
		li := &levels[levelsIndex]
		rli := &rangeDelLevels[levelsIndex]

		li.init(i.opts, i.comparer.Compare, i.comparer.Split, i.newIters, files, level, internalIterOpts{})
		li.initBoundaryContext(&mlevels[mlevelsIndex].levelIterBoundaryContext)
		mlevels[mlevelsIndex].iter = li
		rli.Init(keyspan.SpanIterOptions{RangeKeyFilters: i.opts.RangeKeyFilters},
			i.comparer.Compare, tableNewRangeDelIter(i.newIters), files, level, manifest.KeyTypePoint)
		rangeDelIters = append(rangeDelIters, rli)

		levelsIndex++
		mlevelsIndex++
	}

	// Add level iterators for the L0 sublevels, iterating from newest to
	// oldest.
	for i := len(current.L0SublevelFiles) - 1; i >= 0; i-- {
		addLevelIterForFiles(current.L0SublevelFiles[i].Iter(), manifest.L0Sublevel(i))
	}

	// Add level iterators for the non-empty non-L0 levels.
	for level := 1; level < numLevels; level++ {
		if current.Levels[level].Empty() {
			continue
		}
		addLevelIterForFiles(current.Levels[level].Iter(), manifest.Level(level))
	}
	buf.merging.init(&i.opts, &InternalIteratorStats{}, i.comparer.Compare, i.comparer.Split, mlevels...)
	buf.merging.snapshot = i.seqNum
	rangeDelMiter.Init(i.comparer.Compare, keyspan.VisibleTransform(i.seqNum), new(keyspan.MergingBuffers), rangeDelIters...)
	i.pointKeyIter.Init(i.comparer, &buf.merging, &rangeDelMiter, nil /* mask */, i.opts.LowerBound, i.opts.UpperBound)
	i.iter = &i.pointKeyIter
}

// constructRangeKeyIter constructs the range-key iterator stack, populating
// i.rangeKey.rangeKeyIter with the resulting iterator. This is similar to
// Iterator.constructRangeKeyIter, except it doesn't handle batches and ensures
// iterConfig does *not* elide unsets/deletes.
func (i *InternalIterator) constructRangeKeyIter() {
	// We want the bounded iter from iterConfig, but not the collapsing of
	// RangeKeyUnsets and RangeKeyDels.
	i.rangeKey.rangeKeyIter = i.rangeKey.iterConfig.Init(
		i.comparer, i.seqNum, i.opts.LowerBound, i.opts.UpperBound,
		nil /* hasPrefix */, nil /* prefix */, false, /* onlySets */
		&i.rangeKey.rangeKeyBuffers.internal)

	// Next are the flushables: memtables and large batches.
	for j := len(i.readState.memtables) - 1; j >= 0; j-- {
		mem := i.readState.memtables[j]
		// We only need to read from memtables which contain sequence numbers older
		// than seqNum.
		if logSeqNum := mem.logSeqNum; logSeqNum >= i.seqNum {
			continue
		}
		if rki := mem.newRangeKeyIter(&i.opts); rki != nil {
			i.rangeKey.iterConfig.AddLevel(rki)
		}
	}

	current := i.readState.current
	// Next are the file levels: L0 sub-levels followed by lower levels.
	//
	// Add file-specific iterators for L0 files containing range keys. This is less
	// efficient than using levelIters for sublevels of L0 files containing
	// range keys, but range keys are expected to be sparse anyway, reducing the
	// cost benefit of maintaining a separate L0Sublevels instance for range key
	// files and then using it here.
	//
	// NB: We iterate L0's files in reverse order. They're sorted by
	// LargestSeqNum ascending, and we need to add them to the merging iterator
	// in LargestSeqNum descending to preserve the merging iterator's invariants
	// around Key Trailer order.
	iter := current.RangeKeyLevels[0].Iter()
	for f := iter.Last(); f != nil; f = iter.Prev() {
		spanIterOpts := &keyspan.SpanIterOptions{RangeKeyFilters: i.opts.RangeKeyFilters}
		spanIter, err := i.newIterRangeKey(f, spanIterOpts)
		if err != nil {
			i.rangeKey.iterConfig.AddLevel(&errorKeyspanIter{err: err})
			continue
		}
		i.rangeKey.iterConfig.AddLevel(spanIter)
	}

	// Add level iterators for the non-empty non-L0 levels.
	for level := 1; level < len(current.RangeKeyLevels); level++ {
		if current.RangeKeyLevels[level].Empty() {
			continue
		}
		li := i.rangeKey.iterConfig.NewLevelIter()
		spanIterOpts := keyspan.SpanIterOptions{RangeKeyFilters: i.opts.RangeKeyFilters}
		li.Init(spanIterOpts, i.comparer.Compare, i.newIterRangeKey, current.RangeKeyLevels[level].Iter(),
			manifest.Level(level), manifest.KeyTypeRange)
		i.rangeKey.iterConfig.AddLevel(li)
	}
}

// SeekGE seeks this iterator to the first key that's greater than or equal
// to the specified user key.
func (i *InternalIterator) SeekGE(key []byte) bool {
	i.iterKey, i.iterValue = i.iter.SeekGE(key, base.SeekGEFlagsNone)
	return i.iterKey != nil
}

// UnsafeKey returns the unsafe InternalKey at the current position. The value
// is nil if the iterator is invalid or exhausted.
func (i *InternalIterator) UnsafeKey() *InternalKey {
	return i.iterKey
}

// LazyValue returns a value pointer to the value at the current iterator
// position. Behaviour undefined if UnsafeKey() returns a Range key or Rangedel
// kind key.
func (i *InternalIterator) LazyValue() LazyValue {
	return i.iterValue
}

// UnsafeRangeDel returns a range key span. Behaviour undefined if UnsafeKey returns
// a non-rangedel kind.
func (i *InternalIterator) UnsafeRangeDel() *keyspan.Span {
	return i.pointKeyIter.Span()
}

// UnsafeSpan returns a range key span. Behaviour undefined if UnsafeKey returns
// a non-rangekey type.
func (i *InternalIterator) UnsafeSpan() *keyspan.Span {
	return i.rangeKey.iiter.Span()
}

// ValueAndErr returns the value at the current iterator position, lazy-loading
// it if necessary. Behaviour undefined if UnsafeKey() returns a RangeKey or
// RangeDel kind key.
func (i *InternalIterator) ValueAndErr() ([]byte, error) {
	val, callerOwned, err := i.iterValue.Value(i.lazyValueBuf)
	if err != nil {
		return nil, err
	}
	if callerOwned {
		i.lazyValueBuf = val[:0]
	}
	return val, nil
}

// First moves the iterator to the first key in the iterator. To obey the
// internalIterator contract, it could translate this to a SeekGE at the
// lower bound.
func (i *InternalIterator) First() bool {
	if i.opts.LowerBound != nil {
		return i.SeekGE(i.opts.LowerBound)
	}
	i.iterKey, i.iterValue = i.iter.First()
	return i.iterKey != nil
}

// Next advances the iterator in the forward direction, and returns the
// iterator's new validity state.
func (i *InternalIterator) Next() bool {
	i.iterKey, i.iterValue = i.iter.Next()
	return i.iterKey != nil
}

// Prev advances the iterator in the reverse direction, and returns the
// iterator's new validity state.
func (i *InternalIterator) Prev() bool {
	i.iterKey, i.iterValue = i.iter.Prev()
	return i.iterKey != nil
}

// Valid returns whether the iterator is valid at its current position.
func (i *InternalIterator) Valid() bool {
	return i.iterKey != nil
}

// Error returns an error from the internal iterator, if there's any.
func (i *InternalIterator) Error() error {
	return i.iter.Error()
}

// Close closes this iterator, and releases any pooled objects.
func (i *InternalIterator) Close() error {
	if err := i.iter.Close(); err != nil {
		return err
	}
	i.readState.unref()
	if i.rangeKey != nil {
		i.rangeKey.PrepareForReuse()
		*i.rangeKey = iteratorRangeKeyState{
			rangeKeyBuffers: i.rangeKey.rangeKeyBuffers,
		}
		iterRangeKeyStateAllocPool.Put(i.rangeKey)
		i.rangeKey = nil
	}
	if alloc := i.alloc; alloc != nil {
		for j := range i.boundsBuf {
			if cap(i.boundsBuf[j]) >= maxKeyBufCacheSize {
				alloc.boundsBuf[j] = nil
			} else {
				alloc.boundsBuf[j] = i.boundsBuf[j]
			}
		}
		*alloc = iterAlloc{
			keyBuf:              alloc.keyBuf[:0],
			boundsBuf:           alloc.boundsBuf,
			prefixOrFullSeekKey: alloc.prefixOrFullSeekKey[:0],
		}
		iterAllocPool.Put(alloc)
		i.alloc = nil
	}
	return nil
}

func (i *InternalIterator) initializeBoundBufs(lower, upper []byte) {
	buf := i.boundsBuf[i.boundsBufIdx][:0]
	if lower != nil {
		buf = append(buf, lower...)
		i.opts.LowerBound = buf
	} else {
		i.opts.LowerBound = nil
	}
	if upper != nil {
		buf = append(buf, upper...)
		i.opts.UpperBound = buf[len(buf)-len(upper):]
	} else {
		i.opts.UpperBound = nil
	}
	i.boundsBuf[i.boundsBufIdx] = buf
	i.boundsBufIdx = 1 - i.boundsBufIdx
}

// SetBounds sets the lower and upper bounds for the iterator. Once SetBounds
// returns, the caller is free to mutate the provided slices.
func (i *InternalIterator) SetBounds(lower, upper []byte) {
	// initializeBoundBufs sets i.opts.{Lower,Upper}Bound to copies of lower,upper.
	i.initializeBoundBufs(lower, upper)
	i.iter.SetBounds(i.opts.LowerBound, i.opts.UpperBound)
	if i.rangeKey != nil {
		i.rangeKey.iterConfig.SetBounds(i.opts.LowerBound, i.opts.UpperBound)
	}
}
