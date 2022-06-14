// Copyright 2021 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package pebble

import (
	"github.com/cockroachdb/pebble/internal/base"
	"github.com/cockroachdb/pebble/internal/invariants"
	"github.com/cockroachdb/pebble/internal/keyspan"
	"github.com/cockroachdb/pebble/internal/manifest"
)

// constructRangeKeyIter constructs the range-key iterator stack, populating
// i.rangeKey.rangeKeyIter with the resulting iterator.
func (i *Iterator) constructRangeKeyIter() {
	i.rangeKey.rangeKeyIter = i.rangeKey.iterConfig.Init(i.cmp, i.seqNum)

	// If there's an indexed batch with range keys, include it.
	if i.batch != nil {
		if i.batch.index == nil {
			i.rangeKey.iterConfig.AddLevel(newErrorKeyspanIter(ErrNotIndexed))
		} else {
			// Only include the batch's range key iterator if it has any keys.
			// NB: This can force reconstruction of the rangekey iterator stack
			// in SetOptions if subsequently range keys are added. See
			// SetOptions.
			if i.batch.countRangeKeys > 0 {
				i.batch.initRangeKeyIter(&i.opts, &i.batchRangeKeyIter, i.batchSeqNum)
				i.rangeKey.iterConfig.AddLevel(&i.batchRangeKeyIter)
			}
		}
	}

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
	iter := current.RangeKeyLevels[0].Iter()
	for f := iter.First(); f != nil; f = iter.Next() {
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
		li.Init(spanIterOpts, i.cmp, i.newIterRangeKey, current.RangeKeyLevels[level].Iter(),
			manifest.Level(level), i.opts.logger, manifest.KeyTypeRange)
		i.rangeKey.iterConfig.AddLevel(li)
	}
}

// lazyCombinedIter implements the internalIterator interface, wrapping a
// pointIter. It requires the pointIter's the levelIters be configured with
// pointers to its combinedIterState. When the levelIter observes a file
// containing a range key, the lazyCombinedIter constructs the combined
// range+point key iterator stack and switches to it.
type lazyCombinedIter struct {
	// parent holds a pointer to the root *pebble.Iterator containing this
	// iterator. It's used to mutate the internalIterator in use when switching
	// to combined iteration.
	parent            *Iterator
	pointIter         internalIteratorWithStats
	combinedIterState combinedIterState
}

// combinedIterState encapsulates the current state of combined iteration.
// Various low-level iterators (mergingIter, leveliter) hold pointers to the
// *pebble.Iterator's combinedIterState. This allows them to check whether or
// not they must monitor for files containing range keys (!initialized), or not.
//
// When !initialized, low-level iterators watch for files containing range keys.
// When one is discovered, they set triggered=true and key to the smallest
// (forward direction) or largest (reverse direction) range key that's been
// observed.
type combinedIterState struct {
	// key holds the smallest (forward direction) or largest (backward
	// direction) user key from a range key bound discovered during the iterator
	// operation that triggered the switch to combined iteration.
	//
	// Slices stored here must be stable. This is possible because callers pass
	// a Smallest/Largest bound from a fileMetadata, which are immutable. A key
	// slice's bytes must not be overwritten.
	key         []byte
	triggered   bool
	initialized bool
}

// Assert that *lazyCombinedIter implements internalIterator.
var _ internalIterator = (*lazyCombinedIter)(nil)

// initCombinedIteration is invoked after a pointIter positioning operation
// resulted in i.combinedIterState.triggered=true.
//
// The `dir` parameter is `+1` or `-1` indicating forward iteration or backward
// iteration respectively.
//
// The `pointKey` and `pointValue` parameters provide the new point key-value
// pair that the iterator was just positioned to. The combined iterator should
// be seeded with this point key-value pair and return the smaller (forward
// iteration) or largest (backward iteration) of the two.
//
// The `seekKey` parameter is non-nil only if the iterator operation that
// triggered the switch to combined iteration was a SeekGE, SeekPrefixGE or
// SeekLT. It provides the seek key supplied and is used to seek the range-key
// iterator using the same key. This is necessary for SeekGE/SeekPrefixGE
// operations that land in the middle of a range key and must truncate to the
// user-provided seek key.
func (i *lazyCombinedIter) initCombinedIteration(
	dir int8, pointKey *InternalKey, pointValue []byte, seekKey []byte,
) (*InternalKey, []byte) {
	// Invariant: i.parent.rangeKey is nil.
	// Invariant: !i.combinedIterState.initialized.
	if invariants.Enabled {
		if i.combinedIterState.initialized {
			panic("pebble: combined iterator already initialized")
		}
		if i.parent.rangeKey != nil {
			panic("pebble: iterator already has a range-key iterator stack")
		}
	}

	// An operation on the point iterator observed a file containing range keys,
	// so we must switch to combined interleaving iteration. First, construct
	// the range key iterator stack. It must not exist, otherwise we'd already
	// be performing combined iteration.
	i.parent.rangeKey = iterRangeKeyStateAllocPool.Get().(*iteratorRangeKeyState)
	i.parent.rangeKey.init(i.parent.cmp, i.parent.split, &i.parent.opts)
	i.parent.constructRangeKeyIter()

	// Initialize the Iterator's interleaving iterator.
	i.parent.rangeKey.iiter.Init(
		i.parent.cmp, i.parent.pointIter, i.parent.rangeKey.rangeKeyIter,
		i.parent.rangeKey, i.parent.opts.LowerBound, i.parent.opts.UpperBound)

	// We need to determine the key to seek the range key iterator to. If
	// seekKey is not nil, the user-initiated operation that triggered the
	// switch to combined iteration was itself a seek, and we can use that key.
	// Otherwise, a First/Last or relative positioning operation triggered the
	// switch to combined iteration.
	//
	// The levelIter that observed a file containing range keys populated
	// combinedIterState.key with the smallest (forward) or largest (backward)
	// range key it observed. If multiple levelIters observed files with range
	// keys during the same operation on the mergingIter, combinedIterState.key
	// is the smallest [during forward iteration; largest in reverse iteration]
	// such key.
	if seekKey == nil {
		// Use the levelIter-populated key.
		seekKey = i.combinedIterState.key

		// We may need to adjust the levelIter-populated seek key to the
		// surfaced point key. If the key observed is beyond [in the iteration
		// direction] the current point key, there may still exist a range key
		// at an earlier key. Consider the following example:
		//
		//   L5:  000003:[bar.DEL.5, foo.RANGEKEYSET.9]
		//   L6:  000001:[bar.SET.2] 000002:[bax.RANGEKEYSET.8]
		//
		// A call to First() seeks the levels to files L5.000003 and L6.000001.
		// The L5 levelIter observes that L5.000003 contains the range key with
		// start key `foo`, and triggers a switch to combined iteration, setting
		// `combinedIterState.key` = `foo`.
		//
		// The L6 levelIter did not observe the true first range key
		// (bax.RANGEKEYSET.8), because it appears in a later sstable. When the
		// combined iterator is initialized, the range key iterator must be
		// seeked to a key that will find `bax`. To accomplish this, we seek the
		// key instead to `bar`. It is guaranteed that no range key exists
		// earlier than `bar`, otherwise a levelIter would've observed it and
		// set `combinedIterState.key` to its start key.
		if pointKey != nil {
			if dir == +1 && i.parent.cmp(i.combinedIterState.key, pointKey.UserKey) > 0 {
				seekKey = pointKey.UserKey
			} else if dir == -1 && i.parent.cmp(seekKey, pointKey.UserKey) < 0 {
				seekKey = pointKey.UserKey
			}
		}
	}

	// Set the parent's primary iterator to point to the combined, interleaving
	// iterator that's now initialized with our current state.
	i.parent.iter = &i.parent.rangeKey.iiter
	i.combinedIterState.initialized = true
	i.combinedIterState.key = nil

	// All future iterator operations will go directly through the combined
	// iterator.
	//
	// Initialize the interleaving iterator. We pass the point key-value pair so
	// that the interleaving iterator knows where the point iterator is
	// positioned. Additionally, we pass the seek key to which the range-key
	// iterator should be seeked in order to initialize its position.
	//
	// In the forward direction (invert for backwards), the seek key is a key
	// guaranteed to find the smallest range key that's greater than the last
	// key the iterator returned. The range key may be less than pointKey, in
	// which case the range key will be interleaved next instead of the point
	// key.
	if dir == +1 {
		return i.parent.rangeKey.iiter.InitSeekGE(seekKey, pointKey, pointValue)
	}
	return i.parent.rangeKey.iiter.InitSeekLT(seekKey, pointKey, pointValue)
}

func (i *lazyCombinedIter) SeekGE(key []byte, flags base.SeekGEFlags) (*InternalKey, []byte) {
	if i.combinedIterState.initialized {
		return i.parent.rangeKey.iiter.SeekGE(key, flags)
	}
	k, v := i.pointIter.SeekGE(key, flags)
	if i.combinedIterState.triggered {
		return i.initCombinedIteration(+1, k, v, key)
	}
	return k, v
}

func (i *lazyCombinedIter) SeekPrefixGE(
	prefix, key []byte, flags base.SeekGEFlags,
) (*InternalKey, []byte) {
	if i.combinedIterState.initialized {
		return i.parent.rangeKey.iiter.SeekPrefixGE(prefix, key, flags)
	}
	k, v := i.pointIter.SeekPrefixGE(prefix, key, flags)
	if i.combinedIterState.triggered {
		return i.initCombinedIteration(+1, k, v, key)
	}
	return k, v
}

func (i *lazyCombinedIter) SeekLT(key []byte, flags base.SeekLTFlags) (*InternalKey, []byte) {
	if i.combinedIterState.initialized {
		return i.parent.rangeKey.iiter.SeekLT(key, flags)
	}
	k, v := i.pointIter.SeekLT(key, flags)
	if i.combinedIterState.triggered {
		return i.initCombinedIteration(-1, k, v, key)
	}
	return k, v
}

func (i *lazyCombinedIter) First() (*InternalKey, []byte) {
	if i.combinedIterState.initialized {
		return i.parent.rangeKey.iiter.First()
	}
	k, v := i.pointIter.First()
	if i.combinedIterState.triggered {
		return i.initCombinedIteration(+1, k, v, nil)
	}
	return k, v
}

func (i *lazyCombinedIter) Last() (*InternalKey, []byte) {
	if i.combinedIterState.initialized {
		return i.parent.rangeKey.iiter.Last()
	}
	k, v := i.pointIter.Last()
	if i.combinedIterState.triggered {
		return i.initCombinedIteration(-1, k, v, nil)
	}
	return k, v
}

func (i *lazyCombinedIter) Next() (*InternalKey, []byte) {
	if i.combinedIterState.initialized {
		return i.parent.rangeKey.iiter.Next()
	}
	k, v := i.pointIter.Next()
	if i.combinedIterState.triggered {
		return i.initCombinedIteration(+1, k, v, nil)
	}
	return k, v
}

func (i *lazyCombinedIter) Prev() (*InternalKey, []byte) {
	if i.combinedIterState.initialized {
		return i.parent.rangeKey.iiter.Prev()
	}
	k, v := i.pointIter.Prev()
	if i.combinedIterState.triggered {
		return i.initCombinedIteration(-1, k, v, nil)
	}
	return k, v
}

func (i *lazyCombinedIter) Error() error {
	if i.combinedIterState.initialized {
		return i.parent.rangeKey.iiter.Error()
	}
	return i.pointIter.Error()
}

func (i *lazyCombinedIter) Close() error {
	if i.combinedIterState.initialized {
		return i.parent.rangeKey.iiter.Close()
	}
	return i.pointIter.Close()
}

func (i *lazyCombinedIter) SetBounds(lower, upper []byte) {
	if i.combinedIterState.initialized {
		i.parent.rangeKey.iiter.SetBounds(lower, upper)
		return
	}
	i.pointIter.SetBounds(lower, upper)
}

func (i *lazyCombinedIter) String() string {
	if i.combinedIterState.initialized {
		return i.parent.rangeKey.iiter.String()
	}
	return i.pointIter.String()
}

func (i *lazyCombinedIter) Stats() InternalIteratorStats {
	return i.pointIter.Stats()
}

func (i *lazyCombinedIter) ResetStats() {
	i.pointIter.ResetStats()
}
