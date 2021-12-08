// Copyright 2021 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package rangekey

import (
	"fmt"

	"github.com/cockroachdb/pebble/internal/base"
)

// TODO(jackson): The interleaving iterator has various invariants that it
// asserts. We should eventually gate these behind `invariants.Enabled`.

// InterleavingIter combines an iterator over point keys with an iterator over
// range keys. After any of the iterator's methods returns a key, a caller may
// call RangeKey to retrieve the state of range keys covering that key.
//
// Additionally, InterleavingIter stops at range-key start boundaries. Start
// boundaries are surfaced as a synthetic range key marker: an InternalKey with
// the boundary as the user key, the infinite sequence number and the
// RangeKeySet key kind. Because these synthetic keys have the infinite sequence
// number, they're interleaved before any point keys with same user key when
// iterating forward.
//
// InterleavingIter never truncates the returned range keys' bounds. For
// example, a SeekGE("d") may return a synthetic range-key marker
// `a#72057594037927935,21` if there exists a range key that begins at `a` and
// covers the key `d`. The caller is expected to truncate bounds before
// returning them to the user. Similarly, returned range keys' bounds are not
// truncated to the bounds set through SetBounds. InterleavingIter will
// propagate SetBounds calls to its underlying point iterator.
type InterleavingIter struct {
	pointIter    base.InternalIterator
	rangeKeyIter *Iter

	pointKey *base.InternalKey
	pointVal []byte
	rangeKey *CoalescedSpan
	// rangeKeyMarker is holds synthetic RangeKeySet keys returned when the
	// iterator passes over a range key's start bound.
	rangeKeyMarker base.InternalKey

	// Keeping all of the bools together reduces the sizeof of the struct.

	// pointKeyInterleaved indicates whether the current point key has been
	// interleaved in the current direction.
	pointKeyInterleaved bool
	// rangeKeyCoversKey indicates whether the current rangeKey covers the
	// last-returned key. If false, RangeKey() returns nil.
	rangeKeyCoversKey bool
	// rangeKeyInterleaved indicates whether or not the current rangeKey has
	// been interleaved at its start key in the current direction.
	//
	// When iterating in the forward direction, a range-key marker is
	// interleaved when first passing over the start key. The rangeKey field
	// isn't updated until the iterator moves beyond the end key. This field is
	// used to remember that the range key has already been interleaved and
	// shouldn't be interleaved again.
	rangeKeyInterleaved bool
	// dir indicates the direction of iteration: forward (+1) or backward (-1)
	dir int8
}

// Assert that InterleavingIter implements the InternalIterator interface.
var _ base.InternalIterator = &InterleavingIter{}

// Init initializes the InterleavingIter to interleave point keys from pointIter
// with range keys from rangeKeyIter.
func (i *InterleavingIter) Init(pointIter base.InternalIterator, rangeKeyIter *Iter) {
	*i = InterleavingIter{
		pointIter:    pointIter,
		rangeKeyIter: rangeKeyIter,
	}
}

func (i *InterleavingIter) cmp(a, b []byte) int {
	return i.rangeKeyIter.coalescer.items.cmp(a, b)
}

// RangeKey returns the range key covering the last key returned if any.
func (i *InterleavingIter) RangeKey() *CoalescedSpan {
	if !i.rangeKeyCoversKey {
		return nil
	}
	return i.rangeKey
}

// SeekGE implements (base.InternalIterator).SeekGE.
func (i *InterleavingIter) SeekGE(key []byte) (*base.InternalKey, []byte) {
	i.pointKey, i.pointVal = i.pointIter.SeekGE(key)
	i.pointKeyInterleaved = false
	i.rangeKey = i.rangeKeyIter.SeekGE(key)
	i.rangeKeyInterleaved = false
	if i.rangeKey != nil && !i.rangeKey.Sets() {
		i.nextRangeKey()
	}
	i.dir = +1
	return i.mergeForward()
}

// SeekPrefixGE implements (base.InternalIterator).SeekPrefixGE.
func (i *InterleavingIter) SeekPrefixGE(prefix, key []byte, trySeekUsingNext bool) (*base.InternalKey, []byte) {
	// NB: If i.pointIter and i.rangeKeyIter are backed by levelIters, it's up
	// to the levelIter implementation to ensure that SeekPrefixGE on the point
	// iterator loads files that do not contain `prefix` but may contain a range
	// key covering `prefix`.
	i.pointKey, i.pointVal = i.pointIter.SeekPrefixGE(prefix, key, trySeekUsingNext)
	i.pointKeyInterleaved = false
	i.rangeKey = i.rangeKeyIter.SeekGE(key)
	i.rangeKeyInterleaved = false
	if i.rangeKey != nil && !i.rangeKey.Sets() {
		i.nextRangeKey()
	}
	i.dir = +1
	return i.mergeForward()
}

// SeekLT implements (base.InternalIterator).SeekLT.
func (i *InterleavingIter) SeekLT(key []byte) (*base.InternalKey, []byte) {
	i.pointKey, i.pointVal = i.pointIter.SeekLT(key)
	i.pointKeyInterleaved = false
	i.rangeKey = i.rangeKeyIter.SeekLT(key)
	i.rangeKeyInterleaved = false
	if i.rangeKey != nil && !i.rangeKey.Sets() {
		i.prevRangeKey()
	}
	i.dir = -1
	return i.mergeBackward()
}

// First implements (base.InternalIterator).First.
func (i *InterleavingIter) First() (*base.InternalKey, []byte) {
	i.pointKey, i.pointVal = i.pointIter.First()
	i.pointKeyInterleaved = false
	i.rangeKey = i.rangeKeyIter.First()
	i.rangeKeyInterleaved = false
	if i.rangeKey != nil && !i.rangeKey.Sets() {
		i.nextRangeKey()
	}
	i.dir = +1
	return i.mergeForward()
}

// Last implements (base.InternalIterator).Last.
func (i *InterleavingIter) Last() (*base.InternalKey, []byte) {
	i.pointKey, i.pointVal = i.pointIter.Last()
	i.pointKeyInterleaved = false
	i.rangeKey = i.rangeKeyIter.Last()
	i.rangeKeyInterleaved = false
	if i.rangeKey != nil && !i.rangeKey.Sets() {
		i.prevRangeKey()
	}
	i.dir = -1
	return i.mergeBackward()
}

// Next implements (base.InternalIterator).Next.
func (i *InterleavingIter) Next() (*base.InternalKey, []byte) {
	if i.dir == -1 {
		// Switching directions.
		i.dir = +1
		// Exactly one of {pointKey,rangeKey}Interleaved must be true if we were
		// previously going in the backward direction.
		if i.rangeKeyInterleaved == i.pointKeyInterleaved {
			panic(fmt.Sprintf("pebble: invariant violation: interleaving (point %t, range %t)",
				i.pointKeyInterleaved, i.rangeKeyInterleaved))
		}

		// The existing point key is either the last point key we returned, or
		// the upcoming point key in the backward direction. Move to the next.
		i.pointKey, i.pointVal = i.pointIter.Next()
		i.pointKeyInterleaved = false

		// Regardless of which one was last returned, we mark the range key as
		// interleaved when switching to forward iteration, justified below.
		//
		// If the point key is the last returned:
		//   pointIter    :         ... (y)   z ...
		//   rangeKeyIter : ... ([x -               )) ...
		//                               ^
		// The range key's start must be ≤ the point key, otherwise we'd have
		// interleaved the range key's start. From a forward-iteration
		// perspective, the range key's start is in the past and should be
		// considered already-interleaved.
		//
		// If the range key is the last returned:
		//   pointIter    : ... (x)       z ...
		//   rangeKeyIter :     ... ([y -        )) ...
		//                            ^
		// i.rangeKey.Start is the key we last returned during backward
		// iteration. From the perspective of forward-iteration, it's the start
		// key has already been visited.
		i.rangeKeyInterleaved = true
	}

	// Refresh the point key if the current point key has already been
	// interleaved.
	if i.pointKeyInterleaved {
		i.pointKey, i.pointVal = i.pointIter.Next()
		i.pointKeyInterleaved = false
	}
	// If we already interleaved the current range key, and the point key is ≥
	// the range key's end, move to the next range key.
	if i.rangeKeyInterleaved && i.pointKey != nil && i.rangeKey != nil {
		if i.cmp(i.pointKey.UserKey, i.rangeKey.End) >= 0 {
			i.nextRangeKey()
		}
	}
	return i.mergeForward()
}

// Prev implements (base.InternalIterator).Prev.
func (i *InterleavingIter) Prev() (*base.InternalKey, []byte) {
	// INVARIANT: During reverse iteration, exactly one of
	// {pointKey,rangeKey}Interleaved must be true after each reverse-iteration
	// method completes.
	if i.dir == -1 && i.rangeKeyInterleaved == i.pointKeyInterleaved {
		panic(fmt.Sprintf("pebble: invariant violation: interleaving (point %t, range %t)",
			i.pointKeyInterleaved, i.rangeKeyInterleaved))
	}

	if i.dir == +1 {
		// Switching directions.
		i.dir = -1

		// Since we were previously iterating forward, the current range-key
		// start key may be less than, equal, or greater than the current
		// iterator position. If it's marked as having already been interleaved,
		// it must be ≤ the current iterator position and is still relevant for
		// reverse iteration. If it hasn't been interleaved, then the range
		// key's start is greater than the current iterator position, and the
		// range key isn't relevant.
		if !i.rangeKeyInterleaved {
			i.prevRangeKey()
		}
		i.rangeKeyInterleaved = false

		// The existing point key is either the last point key we returned, or
		// the next point key in the forward direction. Move it backwards.
		i.pointKey, i.pointVal = i.pointIter.Prev()
		i.pointKeyInterleaved = false
	}

	// Refresh the point key if the current point key has already been
	// interleaved.
	if i.pointKeyInterleaved {
		i.pointKey, i.pointVal = i.pointIter.Prev()
		i.pointKeyInterleaved = false
	}
	if i.rangeKeyInterleaved {
		i.prevRangeKey()
	}
	return i.mergeBackward()
}

func (i *InterleavingIter) mergeForward() (*base.InternalKey, []byte) {
	// INVARAINT: rangeKey == nil || rangeKey.Sets()
	// Intuitively, the caller must've already skipped over range key spans that
	// don't include any unshadowed RangeKeySets.
	if i.rangeKey != nil && !i.rangeKey.Sets() {
		panic("pebble: invariant violation: range-key span without visible Sets")
	}

	switch {
	case i.rangeKey == nil:
		// If we're out of range keys, just return the point key.
		i.pointKeyInterleaved = true
		i.rangeKeyCoversKey = false
		return i.pointKey, i.pointVal
	case i.pointKey == nil:
		// If we're out of point keys, we need to return a range key marker. If
		// the current range key has already been interleaved, advance it. Since
		// there are no more point keys, we don't need to worry about advancing
		// past the current point key.
		if i.rangeKeyInterleaved {
			i.nextRangeKey()
		}
		return i.syntheticRangeKeyMarker()
	default:
		// Check if range key hasn't been interleaved and if it starts before
		// the point key, in which case we need to interleave it.
		c := i.cmp(i.pointKey.UserKey, i.rangeKey.Start)
		if !i.rangeKeyInterleaved && c >= 0 {
			return i.syntheticRangeKeyMarker()
		}
		// We have a range key, but it's not time to interleave it: Either its
		// Start is in the future, OR we've already interleaved it. We're
		// returning a point key, but we need to determine if the point key is
		// covered by the current range key.
		i.rangeKeyCoversKey = c >= 0
		i.pointKeyInterleaved = true
		return i.pointKey, i.pointVal
	}
}

func (i *InterleavingIter) nextRangeKey() {
	// Next until we find a range key that includes sets, eliding any range key
	// spans that only contain RangeKeyUnsets or RangeKeyDeletes.
	i.rangeKey = i.rangeKeyIter.Next()
	i.rangeKeyInterleaved = false
	for i.rangeKey != nil && !i.rangeKey.Sets() {
		i.rangeKey = i.rangeKeyIter.Next()
		i.rangeKeyInterleaved = false
	}
}

func (i *InterleavingIter) mergeBackward() (*base.InternalKey, []byte) {
	// INVARAINT: rangeKey == nil || rangeKey.Sets()
	// Intuitively, the caller must've already skipped over range key spans that
	// don't include any unshadowed RangeKeySets.
	if i.rangeKey != nil && !i.rangeKey.Sets() {
		panic("pebble: invariant violation: range-key span without visible Sets")
	}

	switch {
	case i.rangeKey == nil:
		// If we're out of range keys, just return the point key.
		i.pointKeyInterleaved = true
		i.rangeKeyCoversKey = false
		return i.pointKey, i.pointVal
	case i.pointKey == nil:
		// If we're out of point keys, we need to return a range key marker.
		return i.syntheticRangeKeyMarker()
	default:
		// If the range key's start key is greater than the point key, return a
		// marker for the range key.
		if i.cmp(i.rangeKey.Start, i.pointKey.UserKey) > 0 {
			if i.rangeKeyInterleaved {
				panic("pebble: range key unexpectedly already interleaved")
			}
			return i.syntheticRangeKeyMarker()
		}

		// We have a range key but it has not been interleaved and starts after
		// the current point key. Return the point key.
		i.rangeKeyCoversKey = i.cmp(i.pointKey.UserKey, i.rangeKey.End) < 0
		i.pointKeyInterleaved = true
		return i.pointKey, i.pointVal
	}
}

func (i *InterleavingIter) prevRangeKey() {
	// Prev until we find a range key that includes sets, eliding any range key
	// spans that only contain RangeKeyUnsets or RangeKeyDeletes.
	for {
		i.rangeKey = i.rangeKeyIter.Prev()
		i.rangeKeyInterleaved = false
		if i.rangeKey == nil || i.rangeKey.Sets() {
			break
		}
	}
}

func (i *InterleavingIter) syntheticRangeKeyMarker() (*base.InternalKey, []byte) {
	i.rangeKeyMarker.UserKey = i.rangeKey.Start
	i.rangeKeyMarker.Trailer = base.InternalKeyBoundaryRangeKey
	i.rangeKeyInterleaved = true
	i.rangeKeyCoversKey = true
	return &i.rangeKeyMarker, nil
}

// SetBounds implements (base.InternalIterator).SetBounds.
func (i *InterleavingIter) SetBounds(lower, upper []byte) {
	i.pointIter.SetBounds(lower, upper)
}

// Error implements (base.InternalIterator).Error.
func (i *InterleavingIter) Error() error {
	return i.pointIter.Error()
}

// Close implements (base.InternalIterator).Close.
func (i *InterleavingIter) Close() error {
	return i.pointIter.Close()
}

// String implements (base.InternalIterator).String.
func (i *InterleavingIter) String() string {
	return fmt.Sprintf("range-key-interleaving(%q)", i.pointIter.String())
}
