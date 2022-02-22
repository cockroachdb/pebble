// Copyright 2022 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package rangekey

import (
	"bytes"
	"fmt"

	"github.com/cockroachdb/pebble/internal/base"
	"github.com/cockroachdb/pebble/internal/invariants"
)

// bufferReuseMaxCapacity is the maximum capacity of a DefragmentingIter buffer
// that DefragmentingIter will reuse. Buffers greater than this will be
// discarded and reallocated as necessary.
const bufferReuseMaxCapacity = 10 << 10 // 10 KB

// DefragmentMethod configures the type of defragmentation performed by the
// DefragmentingIter.
type DefragmentMethod int8

const (
	// DefragmentInternal configures a DefragmentingIter to defragment spans of
	// range keys only if they have the same internal state. Put another away,
	// if two range key spans would synethsize into identical internal keys
	// (RANGEKEYSET, RANGEKEYDEL, RANGEKEYUNSET), not considering their
	// different bounds, then they're eligible for defragmenting.
	//
	// This defragmenting method is intended for compactions that may see
	// internal range keys fragments that may now be joined, because the state
	// that required their fragmentation has been dropped.
	DefragmentInternal DefragmentMethod = iota
	// DefragmentLogical configures a DefragmentingIter to defragment spans of
	// range keys if their external, user-visible state is identical.
	// specifically, this defragmenting method ignores all state besides the set
	// suffix-value tuples. it's intended for use during user iteration, when
	// the wrapped range key iterator is merging range keys across all levels of
	// the lsm.
	//
	// Consider the fragments:
	//     a.RANGEKEYSET.5: c [(@5=foo)]  \
	//     a.RANGEKEYUNSET.5:c [@4]        - CoalescedSpan{@5=foo, unset(@4)}
	//     a.RANGEKEYSET.3: c [(@4=foo)]  /
	//     c.RANGEKEYSET.4: c [(@5=foo)]   - CoalescedSpan{@5=foo}
	//
	// DefragmentLogical will merge these two coalesced spans because they're
	// abutting. It ignores unsets and deletes, whose existence during iteration
	// varies depending on compactions.
	DefragmentLogical
)

// iterPos is an enum indicating the position of the defragmenting iter's
// wrapped iter. The defragmenting iter must look ahead or behind when
// defragmenting forward or backwards respectively, and this enum records that
// current position.
type iterPos int8

const (
	iterPosPrev iterPos = -1
	iterPosCurr iterPos = 0
	iterPosNext iterPos = +1
)

// DefragmentingIter wraps a range key iterator, defragmenting physical
// fragmentation during iteration.
//
// During flushes and compactions, range keys may be split at sstable
// boundaries. This fragmentation can produce internal range key bounds that do
// not match any of the bounds ever supplied to a user range-key operation. This
// physical fragmentation is necessary to avoid excessively wide sstables.
//
// The defragmenting iterator undoes this physical fragmentation, joining spans
// with abutting bounds and equal state. The defragmenting iterator supports two
// separate methods of determining what is "equal state" for a span. The user
// may configure which of the methods of defragmentation they desire at Init
// through passing a DefragmentMethod enum value:
//
// DefragmentLogical is intended for use during user iteration, joining adjacent
// CoalescedSpans if the user-observable logical state is identical.
// Specifically, DefragmentLogical defragments abutting spans if the set of
// range key suffix-value tuples are identical. This may be used to hide
// physical fragmentation from the end user during iteration, ensuring the only
// fragmentation observable is the fragmentation introduced by the user's own
// operations.
//
// DefragmentInternal is intended for use during compactions, joining adjacent
// CoalescedSpans if the internal state is identical between two spans. Range
// keys covering a span [a, c) may be split into two sstables, as two spans
// [a,b) and [b,c). These range keys' sstables may eventually both be inputs to
// the same compaction, and a DefragmentingIter configured with
// DefragmentInternal may join the two spans back together before outputting to
// a single sstable.
//
// Seeking (SeekGE, SeekLT) poses an obstacle to defragmentation. A seek may
// land on a physical fragment in the middle of several fragments that must be
// defragmented. A seek first degfragments in the opposite direction of
// iteration to find the beginning of the defragmented span, and then
// defragments in the iteration direction, ensuring it's found a whole
// defragmented span.
type DefragmentingIter struct {
	cmp      base.Compare
	iter     Iterator
	iterSpan *CoalescedSpan
	iterPos  iterPos
	method   DefragmentMethod

	// curr holds the range key span at the current iterator position. currBuf
	// is a buffer that may be used when copying keys for curr. currBuf is
	// cleared between positioning methods.
	//
	// keyBuf is a buffer specifically for the defragmented start key when
	// defragmenting backwards or the defragmented end key when defragmenting
	// forwards. These bounds are overwritten repeatedly during defragmentation,
	// and the defragmentation routines overwrite keyBuf repeatedly to store
	// these extended bounds.
	curr    CoalescedSpan
	currBuf []byte
	keyBuf  []byte

	// equalState is a comparison function for two coalesced range key spans.
	// The value of equalState is dependent on the method of defragmenting being
	// performed: internal or logical. The implementations of this function that
	// may be used are equalRangeKeyInternal and equalRangeKeyLogical.
	//
	// equalState does not check for adjacency for the two spans.
	equalState func(cmp base.Compare, a, b *CoalescedSpan) bool
}

// Assert that *DefragmentingIter implements the rangekey.Iterator interface.
var _ Iterator = (*DefragmentingIter)(nil)

// Init initializes the defragmenting iter using the provided defragmentation
// method.
func (i *DefragmentingIter) Init(
	cmp base.Compare, iter Iterator, defragmentMethod DefragmentMethod,
) {
	*i = DefragmentingIter{cmp: cmp, iter: iter, method: defragmentMethod}
	switch i.method {
	case DefragmentInternal:
		i.equalState = equalRangeKeyInternal
	case DefragmentLogical:
		i.equalState = equalRangeKeyLogical
	default:
		panic(fmt.Sprintf("pebble: unrecognized defragment method: %d", defragmentMethod))
	}
}

// Valid returns true if the iterator is currently positioned over a span.
func (i *DefragmentingIter) Valid() bool {
	return i.iter.Valid()
}

// Clone clones the iterator, returning an independent iterator over the same
// state. This method is temporary and may be deleted once range keys' state is
// properly reflected in readState.
func (i *DefragmentingIter) Clone() Iterator {
	// TODO(jackson): Delete Clone() when range-key state is incorporated into
	// readState.
	c := &DefragmentingIter{}
	c.Init(i.cmp, i.iter.Clone(), i.method)
	return c
}

// Error returns any accumulated error.
func (i *DefragmentingIter) Error() error {
	return i.iter.Error()
}

// Close closes the underlying iterators.
func (i *DefragmentingIter) Close() error {
	return i.iter.Close()
}

// Current returns the span at the iterator's current position.
func (i *DefragmentingIter) Current() *CoalescedSpan {
	return &i.curr
}

// SeekGE seeks the iterator to the first span covering a key greater than or
// equal to key and returns it.
func (i *DefragmentingIter) SeekGE(key []byte) *CoalescedSpan {
	i.iterSpan = i.iter.SeekGE(key)
	// Defragment backward to find the beginning of the defragmented span.
	i.defragmentBackward()
	// Next once back onto the span.
	i.iterSpan = i.iter.Next()
	// Defragment the full span from its beginning.
	return i.defragmentForward()
}

// SeekLT seeks the iterator to the first span covering a key less than key and
// returns it.
func (i *DefragmentingIter) SeekLT(key []byte) *CoalescedSpan {
	i.iterSpan = i.iter.SeekLT(key)
	// Defragment forward to find the end of the defragmented span.
	i.defragmentForward()
	// Prev once back onto the span.
	i.iterSpan = i.iter.Prev()
	// Defragment the full span from its end.
	return i.defragmentBackward()
}

// First seeks the iterator to the first span and returns it.
func (i *DefragmentingIter) First() *CoalescedSpan {
	i.iterSpan = i.iter.First()
	return i.defragmentForward()
}

// Last seeks the iterator to the last span and returns it.
func (i *DefragmentingIter) Last() *CoalescedSpan {
	i.iterSpan = i.iter.Last()
	return i.defragmentBackward()
}

// Next advances to the next span and returns it.
func (i *DefragmentingIter) Next() *CoalescedSpan {
	switch i.iterPos {
	case iterPosPrev:
		// Switching diections; The iterator is currently positioned over the
		// last fragment of the previous set of fragments. In the below diagram,
		// the iterator is positioned over the last span that contributes to
		// the defragmented x position. We want to be positioned over the first
		// span that contributes to the z position.
		//
		//   x x x y y y z z z
		//       ^       ^
		//      old     new
		//
		// Next once to move onto y, defragment forward to land on the first z
		// position.
		i.iterSpan = i.iter.Next()
		if i.iterSpan == nil {
			panic("pebble: invariant violation: no next span while switching directions")
		}
		// We're now positioned on the first span that was defragmented into the
		// current iterator position. Skip over the rest of the current iterator
		// position's constitutent fragments. In the above example, this would
		// land on the first 'z'.
		i.defragmentForward()

		// Now that we're positioned over the first of the next set of
		// fragments, defragment forward.
		return i.defragmentForward()
	case iterPosCurr:
		// iterPosCurr is only used when the iter is exhausted.
		if invariants.Enabled && i.iterSpan != nil {
			panic("pebble: invariant violation: iterPosCurr with iterSpan set")
		}

		i.iterSpan = i.iter.Next()
		return i.defragmentForward()
	case iterPosNext:
		// Already at the next span.
		return i.defragmentForward()
	default:
		panic("unreachable")
	}
}

// Prev steps back to the previous span and returns it.
func (i *DefragmentingIter) Prev() *CoalescedSpan {
	switch i.iterPos {
	case iterPosPrev:
		// Already at the previous span.
		return i.defragmentBackward()
	case iterPosCurr:
		// iterPosCurr is only used when the iter is exhausted.
		if invariants.Enabled && i.iterSpan != nil {
			panic("pebble: invariant violation: iterPosCurr with iterSpan set")
		}

		i.iterSpan = i.iter.Prev()
		return i.defragmentBackward()
	case iterPosNext:
		// Switching diections; The iterator is currently positioned over the
		// first fragment of the next set of fragments. In the below diagram,
		// the iterator is positioned over the first span that contributes to
		// the defragmented z position. We want to be positioned over the last
		// span that contributes to the x position.
		//
		//   x x x y y y z z z
		//       ^       ^
		//      new     old
		//
		// Prev once to move onto y, defragment backward to land on the last x
		// position.
		i.iterSpan = i.iter.Prev()
		if i.iterSpan == nil {
			panic("pebble: invariant violation: no previous span while switching directions")
		}
		// We're now positioned on the last span that was defragmented into the
		// current iterator position. Skip over the rest of the current iterator
		// position's constitutent fragments. In the above example, this would
		// land on the last 'x'.
		i.defragmentBackward()

		// Now that we're positioned over the last of the prev set of
		// fragments, defragment backward.
		return i.defragmentBackward()
	default:
		panic("unreachable")
	}
}

// defragmentForward defragments spans in the forward direction, starting from
// i.iter's current position.
func (i *DefragmentingIter) defragmentForward() *CoalescedSpan {
	if i.iterSpan == nil {
		i.iterPos = iterPosCurr
		return nil
	}
	i.saveCurrent(i.iterSpan)

	i.iterPos = iterPosNext
	i.iterSpan = i.iter.Next()
	for i.iterSpan != nil {
		if !i.equalState(i.cmp, i.iterSpan, &i.curr) {
			// Not a continuation.
			break
		}
		if i.cmp(i.curr.End, i.iterSpan.Start) != 0 {
			// Not a continuation.
			break
		}
		i.keyBuf = append(i.keyBuf[:0], i.iterSpan.End...)
		i.curr.End = i.keyBuf
		i.iterSpan = i.iter.Next()
	}
	return &i.curr
}

// defragmentBackward defragments spans in the backward direction, starting from
// i.iter's current position.
func (i *DefragmentingIter) defragmentBackward() *CoalescedSpan {
	if i.iterSpan == nil {
		i.iterPos = iterPosCurr
		return nil
	}
	i.saveCurrent(i.iterSpan)

	i.iterPos = iterPosPrev
	i.iterSpan = i.iter.Prev()
	for i.iterSpan != nil {
		if !i.equalState(i.cmp, i.iterSpan, &i.curr) {
			// Not a continuation.
			break
		}
		if i.cmp(i.curr.Start, i.iterSpan.End) != 0 {
			// Not a continuation.
			break
		}
		i.keyBuf = append(i.keyBuf[:0], i.iterSpan.Start...)
		i.curr.Start = i.keyBuf
		i.iterSpan = i.iter.Prev()
	}
	return &i.curr
}

func (i *DefragmentingIter) saveCurrent(cs *CoalescedSpan) {
	i.currBuf = i.currBuf[:0]
	i.keyBuf = i.keyBuf[:0]
	if cap(i.currBuf) > bufferReuseMaxCapacity {
		i.currBuf = nil
	}
	if cap(i.keyBuf) > bufferReuseMaxCapacity {
		i.keyBuf = nil
	}
	i.curr = CoalescedSpan{
		LargestSeqNum: cs.LargestSeqNum,
		Start:         i.saveBytes(cs.Start),
		End:           i.saveBytes(cs.End),
		Delete:        cs.Delete,
	}
	// When defragmenting based on logical range key state (eg, disregarding
	// internal unsets, deletes), clear any fields not used for equality
	// comparisons. This isn't strictly necessary, but it's deceiving to return
	// the fields set to the user for the defragmented bounds
	// [i.curr.Start, i.curr.End) because these fields are /not/ guaranteed to
	// be accurate for the entire bounds.
	if i.method == DefragmentLogical {
		i.curr.Delete = false
		i.curr.LargestSeqNum = 0
	}
	for j := range cs.Items {
		// Similar to above, skip unsets that aren't relevant during logical
		// defragmentation.
		if i.method == DefragmentLogical && cs.Items[j].Unset {
			continue
		}
		i.curr.Items = append(i.curr.Items, SuffixItem{
			Unset:  cs.Items[j].Unset,
			Suffix: i.saveBytes(cs.Items[j].Suffix),
			Value:  i.saveBytes(cs.Items[j].Value),
		})
	}
}

func (i *DefragmentingIter) saveBytes(b []byte) []byte {
	if b == nil {
		return nil
	}
	ret := append(i.currBuf, b...)
	i.currBuf = ret[len(ret):]
	return ret
}

func equalRangeKeyInternal(cmp base.Compare, a, b *CoalescedSpan) bool {
	if a.LargestSeqNum != b.LargestSeqNum || a.Delete != b.Delete || len(a.Items) != len(b.Items) {
		return false
	}
	for i := 0; i < len(a.Items); i++ {
		if a.Items[i].Unset != b.Items[i].Unset ||
			cmp(a.Items[i].Suffix, b.Items[i].Suffix) != 0 ||
			!bytes.Equal(a.Items[i].Value, b.Items[i].Value) {
			return false
		}
	}
	return true
}

func equalRangeKeyLogical(cmp base.Compare, a, b *CoalescedSpan) bool {
	ai, bi := 0, 0
	for ai < len(a.Items) || bi < len(b.Items) {
		// Skip any unset suffix items.
		switch {
		case ai < len(a.Items) && a.Items[ai].Unset:
			ai++
			continue
		case bi < len(b.Items) && b.Items[bi].Unset:
			bi++
			continue
		}

		// If either is exhausted and the other has a non-Unset suffix item,
		// then they must not be equal.
		if ai >= len(a.Items) || bi >= len(b.Items) {
			return false
		}
		// Since everything is sorted, the sets must be exactly equal in suffix
		// and value if the set of keys are equal.
		if cmp(a.Items[ai].Suffix, b.Items[bi].Suffix) != 0 ||
			!bytes.Equal(a.Items[ai].Value, b.Items[bi].Value) {
			return false
		}
		ai, bi = ai+1, bi+1
	}
	return true
}
