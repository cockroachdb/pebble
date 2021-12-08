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
// call RangeKey to retrieve the state of range keys covering the key's user
// key.
//
// A range key is considered to 'cover' a returned key if the range key's
// [start, end) bounds include the key's user key. A range key with a lower
// sequence number may cover a point key at a higher sequence number. This is
// allowed because Pebble evaluates point keys' sequence numbers with respect to
// other point keys, and range keys' sequence numbers with respect to other
// range keys.
//
// In addition to tracking the current covering range key, InterleavingIter
// returns a special InternalKey at range-key start boundaries. Start boundaries
// are surfaced as a synthetic range key marker: an InternalKey with the
// boundary as the user key, the infinite sequence number and the RangeKeySet
// key kind. These synthetic keys have the infinite sequence number, so they're
// interleaved before any point keys with the same user key when iterating
// forward and after when iterating backward.
//
// Interleaving the synthetic start key boundaries at the maximum sequence
// number provides an opportunity for the higher-level, public Iterator to pause
// at the start key, even if the point key at the same user key is deleted.
//
// InterleavingIter never truncates the returned range keys' bounds. For
// example, a SeekGE("d") may return a synthetic range-key marker
// `a#72057594037927935,21` if there exists a range key that begins at `a` and
// covers the key `d`. The caller is expected to truncate bounds before
// returning them to the user. Similarly, returned range keys' bounds are not
// truncated to the bounds set through SetBounds. InterleavingIter propagates
// SetBounds calls to its underlying point iterator.
//
// InterleavedIter elides range keys that do not contain any visible range keys
// (eg, internal range key spans that contain only Unsets and Deletes).
type InterleavingIter struct {
	pointIter    base.InternalIterator
	rangeKeyIter *Iter

	// lower and upper are the bounds set through SetBounds.
	lower, upper []byte
	// keyBuf is used to copy SeekGE or SeekPrefixGE arguments when they're used
	// to truncate a range key. These byte slices backing search keys can come
	// directly from the end user, in which case we can't retain them and expect
	// them to remain unchanged.
	keyBuf        []byte
	pointKey      *base.InternalKey
	pointVal      []byte
	rangeKey      *CoalescedSpan
	rangeKeyStart []byte
	rangeKeyEnd   []byte
	rangeKeyItems []SuffixItem
	// rangeKeyMarker holds the synthetic RangeKeySet key that is returned when
	// the iterator passes over a range key's start bound.
	rangeKeyMarker base.InternalKey

	// Keeping all of the bools together reduces the sizeof the struct.

	// rangeKeyCoversKey indicates whether the current rangeKey covers the
	// last-returned key. If false, RangeKey() returns nil.
	rangeKeyCoversKey bool
	// pointKeyInterleaved indicates whether the current point key has been
	// interleaved in the current direction.
	pointKeyInterleaved bool
	// rangeKeyInterleaved indicates whether or not the current rangeKey has
	// been interleaved at its start key in the current direction. A range key
	// marker is interleaved when first passing over the start key.
	//
	// When iterating in the forward direction, the range key start is
	// interleaved when the range key first begin to cover the current iterator
	// position. The rangeKey field isn't updated again until the iterator moves
	// beyond the end key. This field is used to remember that the range key has
	// already been interleaved and shouldn't be interleaved again.
	//
	// When iterating in the reverse direction, the range key start is
	// interleaved immediately before the iterator will move to a key no longer
	// be covered by the range key. This field is used to remember to Prev to
	// the previous range key on a subsequent Prev.
	rangeKeyInterleaved bool
	// rangeKeyTruncated is set to true when the current range key is
	// interleaved in a truncated form by a SeekGE or SeekPrefixGE.
	rangeKeyTruncated bool
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

// SeekGE implements (base.InternalIterator).SeekGE.
//
// NB: In accordance with the base.InternalIterator contract:
//   i.lower ≤ key
func (i *InterleavingIter) SeekGE(key []byte) (*base.InternalKey, []byte) {
	i.pointKey, i.pointVal = i.pointIter.SeekGE(key)
	i.pointKeyInterleaved = false
	i.nextRangeKey(i.rangeKeyIter.SeekGE(key))
	i.dir = +1
	return i.mergeForward(key)
}

// SeekPrefixGE implements (base.InternalIterator).SeekPrefixGE.
//
// NB: In accordance with the base.InternalIterator contract:
//   i.lower ≤ key
func (i *InterleavingIter) SeekPrefixGE(prefix, key []byte, trySeekUsingNext bool) (*base.InternalKey, []byte) {
	i.pointKey, i.pointVal = i.pointIter.SeekPrefixGE(prefix, key, trySeekUsingNext)
	i.pointKeyInterleaved = false
	i.nextRangeKey(i.rangeKeyIter.SeekGE(key))
	i.dir = +1
	return i.mergeForward(key)
}

// SeekLT implements (base.InternalIterator).SeekLT.
func (i *InterleavingIter) SeekLT(key []byte) (*base.InternalKey, []byte) {
	i.pointKey, i.pointVal = i.pointIter.SeekLT(key)
	i.pointKeyInterleaved = false
	i.prevRangeKey(i.rangeKeyIter.SeekLT(key))
	i.dir = -1
	return i.mergeBackward()
}

// First implements (base.InternalIterator).First.
func (i *InterleavingIter) First() (*base.InternalKey, []byte) {
	i.pointKey, i.pointVal = i.pointIter.First()
	i.pointKeyInterleaved = false
	i.nextRangeKey(i.rangeKeyIter.First())
	i.dir = +1
	return i.mergeForward(i.lower)
}

// Last implements (base.InternalIterator).Last.
func (i *InterleavingIter) Last() (*base.InternalKey, []byte) {
	i.pointKey, i.pointVal = i.pointIter.Last()
	i.pointKeyInterleaved = false
	i.prevRangeKey(i.rangeKeyIter.Last())
	i.dir = -1
	return i.mergeBackward()
}

// Next implements (base.InternalIterator).Next.
func (i *InterleavingIter) Next() (*base.InternalKey, []byte) {
	if i.dir == -1 {
		// Switching directions.
		i.dir = +1

		// The existing point key (denoted below with *) is either the last
		// key we returned (the current iterator position):
		//   points:    x     (y*)    z
		// or the upcoming point key in the backward direction if we just
		// returned a range key start boundary:
		//   points:    x*            z
		//   ranges:        ([y-?))
		// direction. Either way, we must move to the next point key.
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
		// i.rangeKey.Start is the key we last returned during reverse
		// iteration. From the perspective of forward-iteration, its start key
		// has already been visited.
		i.rangeKeyInterleaved = true
		i.rangeKeyTruncated = false
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
			i.nextRangeKey(i.rangeKeyIter.Next())
		}
	}
	return i.mergeForward(i.lower)
}

// Prev implements (base.InternalIterator).Prev.
func (i *InterleavingIter) Prev() (*base.InternalKey, []byte) {
	if i.dir == +1 {
		// Switching directions.
		i.dir = -1

		if !i.rangeKeyInterleaved {
			// If the current range key has not been interleaved, then the range
			// key's start is greater than the current iterator position (denoted in
			// parenthesis), and the current range key is ahead of our iterator
			// position. Move it to the previous range key:
			//  points:    (x*)
			//  ranges:          [y-z)*
			i.prevRangeKey(i.rangeKeyIter.Prev())
		} else {
			// If the current range key has been interleaved, its start key must
			// be less than or equal to the current iterator position. If it's
			// less, the current range key is relevant and we should not move
			// the iterator.
			//
			// If it's equal to the current iterator position, we last returned
			// the range key. We should prev past it so we don't return it
			// again, with an exception. Consider range key [a, z) and this
			// sequence of iterator calls:
			//
			//   SeekGE('c') = c.RANGEKEYSET#72057594037927935
			//   Prev()      = a.RANGEKEYSET#72057594037927935
			//
			// If the current range key was last surfaced truncated due to a
			// SeekGE or SeekPrefixGE call, then it's still relevant in the
			// reverse direction.
			if !i.pointKeyInterleaved && !i.rangeKeyTruncated {
				i.prevRangeKey(i.rangeKeyIter.Prev())
			} else {
				// Don't move, but re-save the range key because it should no
				// longer be considered truncated or interlaved.
				i.saveRangeKey(i.rangeKey)
			}
		}

		// The existing point key (denoted below with *) is either the last
		// key we returned (the current iterator position):
		//   points:    x     (y*)    z
		// or the upcoming point key in the forward direction if we just
		// returned a range key start boundary:
		//   points:    x             z*
		//   ranges:        ([y-?))
		// direction. Either way, we must move the point iterator backwards.
		i.pointKey, i.pointVal = i.pointIter.Prev()
		i.pointKeyInterleaved = false
	}

	// Refresh the point key if we just returned the current point key.
	if i.pointKeyInterleaved {
		i.pointKey, i.pointVal = i.pointIter.Prev()
		i.pointKeyInterleaved = false
	}
	// Refresh the range key if we just returned the range key start boundary.
	if i.rangeKeyInterleaved {
		i.prevRangeKey(i.rangeKeyIter.Prev())
	}
	return i.mergeBackward()
}

func (i *InterleavingIter) mergeForward(lowerBound []byte) (*base.InternalKey, []byte) {
	// INVARIANT: rangeKey == nil || rangeKey.HasSets()
	// Intuitively, the caller must've already skipped over range key spans that
	// don't include any unshadowed RangeKeySets.
	if i.rangeKey != nil && !i.rangeKey.HasSets() {
		panic("pebble: invariant violation: range-key span without visible Sets")
	}
	// INVARIANT: !rangeKeyInterleaved || pointKey < rangeKey.End
	// The caller is responsible for advancing this range key if it's already
	// been interleaved and the point key ends before it.
	if i.rangeKey != nil && i.pointKey != nil && i.rangeKeyInterleaved &&
		i.cmp(i.pointKey.UserKey, i.rangeKey.End) >= 0 {
		panic("pebble: invariant violation: range key interleaved, but point key > range key end")
	}

	switch {
	case i.rangeKey == nil:
		// If we're out of range keys, just return the point key.
		i.rangeKeyCoversKey = false
		return i.yieldPointKey()
	case i.pointKey == nil:
		// If we're out of point keys, we need to return a range key marker. If
		// the current range key has already been interleaved, advance it. Since
		// there are no more point keys, we don't need to worry about advancing
		// past the current point key.
		if i.rangeKeyInterleaved {
			i.nextRangeKey(i.rangeKeyIter.Next())
			if i.rangeKey == nil {
				return i.yieldPointKey()
			}
		}
		return i.yieldSyntheticRangeKeyMarker(lowerBound)
	default:
		if i.cmp(i.pointKey.UserKey, i.rangeKey.Start) >= 0 {
			// The range key starts before the point key. If we haven't
			// interleaved it, we should.
			if !i.rangeKeyInterleaved {
				return i.yieldSyntheticRangeKeyMarker(lowerBound)
			}

			// Otherwise, the range key is already interleaved and we need to
			// return the point key. The current range key necessarily must
			// cover the point key:
			//
			// Since the range key's start is less than or equal to the point
			// key, the only way for this range key to not cover the point would
			// be if the range key's end is less than or equal to the point.
			// (For example range key = [a, b), point key = c).
			//
			// However, the invariant at the beginning of the function
			// guarantees that if:
			//  * we have both a point key and a range key
			//  * and the range key has already been interleaved
			//  * then the point key must be less than the range key's end
			i.rangeKeyCoversKey = true
			return i.yieldPointKey()
		}
		i.rangeKeyCoversKey = false
		return i.yieldPointKey()
	}
}

func (i *InterleavingIter) mergeBackward() (*base.InternalKey, []byte) {
	// INVARAINT: rangeKey == nil || rangeKey.HasSets()
	// Intuitively, the caller must've already skipped over range key spans that
	// don't include any unshadowed RangeKeySets.
	if i.rangeKey != nil && !i.rangeKey.HasSets() {
		panic("pebble: invariant violation: range-key span without visible Sets")
	}

	switch {
	case i.rangeKey == nil:
		// If we're out of range keys, just return the point key.
		i.rangeKeyCoversKey = false
		return i.yieldPointKey()
	case i.pointKey == nil:
		// If we're out of point keys, we need to return a range key marker.
		return i.yieldSyntheticRangeKeyMarker(i.lower)
	default:
		// If the range key's start key is greater than the point key, return a
		// marker for the range key.
		if i.cmp(i.rangeKey.Start, i.pointKey.UserKey) > 0 {
			if i.rangeKeyInterleaved {
				panic("pebble: range key unexpectedly already interleaved")
			}
			return i.yieldSyntheticRangeKeyMarker(i.lower)
		}

		// We have a range key but it has not been interleaved and starts after
		// the current point key. Return the point key.
		i.rangeKeyCoversKey = i.cmp(i.pointKey.UserKey, i.rangeKey.End) < 0
		return i.yieldPointKey()
	}
}

func (i *InterleavingIter) nextRangeKey(k *CoalescedSpan) {
	// Next until we find a range key that includes sets, eliding any range key
	// spans that only contain RangeKeyUnsets or RangeKeyDeletes.
	for k != nil {
		// Check the upper bound if we have one.
		if i.upper != nil && i.cmp(k.Start, i.upper) >= 0 {
			k = nil
			break
		}
		if k.HasSets() {
			break
		}
		k = i.rangeKeyIter.Next()
	}
	i.saveRangeKey(k)
}

func (i *InterleavingIter) prevRangeKey(k *CoalescedSpan) {
	// Prev until we find a range key that includes sets, eliding any range key
	// spans that only contain RangeKeyUnsets or RangeKeyDeletes.
	for k != nil {
		// Check the lower bound if we have one.
		if i.lower != nil && i.cmp(k.End, i.lower) <= 0 {
			k = nil
			break
		}
		if k.HasSets() {
			break
		}
		k = i.rangeKeyIter.Prev()
	}
	i.saveRangeKey(k)
}

func (i *InterleavingIter) yieldPointKey() (*base.InternalKey, []byte) {
	i.pointKeyInterleaved = true
	return i.verify(i.pointKey, i.pointVal)
}

func (i *InterleavingIter) yieldSyntheticRangeKeyMarker(lowerBound []byte) (*base.InternalKey, []byte) {
	i.rangeKeyMarker.UserKey = i.rangeKey.Start
	i.rangeKeyMarker.Trailer = base.InternalKeyBoundaryRangeKey
	i.rangeKeyInterleaved = true
	i.rangeKeyCoversKey = true

	// Truncate the range key we return to our lower bound if we have one.  Note
	// that the lowerBound argument may come from a SetBounds` call or it could
	// come from a SeekGE or SeekPrefixGE search key.
	if lowerBound != nil && i.cmp(lowerBound, i.rangeKey.Start) > 0 {
		// If the lowerBound argument is the lower bound set by SetBounds,
		// Pebble owns the slice's memory and there's no need to make a copy of
		// the lower bound.
		//
		// Otherwise, the lowerBound argument came from a SeekGE or SeekPrefixGE
		// call, and it may be backed by a user-provided byte slice.
		if len(lowerBound) > 0 && len(i.lower) > 0 && &lowerBound[0] == &i.lower[0] {
			i.rangeKeyMarker.UserKey = lowerBound
		} else {
			i.keyBuf = append(i.keyBuf[:0], lowerBound...)
			i.rangeKeyMarker.UserKey = i.keyBuf
			i.rangeKeyTruncated = true
		}
	}
	return i.verify(&i.rangeKeyMarker, nil)
}

func (i *InterleavingIter) verify(k *base.InternalKey, v []byte) (*base.InternalKey, []byte) {
	if !i.rangeKeyInterleaved && !i.pointKeyInterleaved {
		panic("pebble: invariant violation: both keys marked as noninterleaved")
	}
	if i.dir == -1 && i.rangeKeyInterleaved == i.pointKeyInterleaved {
		// INVARIANT: During reverse iteration, either the range key must have
		// been interleaved OR the current point key value is being returned. It
		// cannot be both.
		//
		// This invariant holds because in reverse iteration the start key of the
		// range behaves like a point. Once the start key is interleaved, we move
		// the range key iterator to the previous range key.
		panic(fmt.Sprintf("pebble: invariant violation: interleaving (point %t, range %t)",
			i.pointKeyInterleaved, i.rangeKeyInterleaved))
	}
	return k, v
}

func (i *InterleavingIter) saveRangeKey(rk *CoalescedSpan) {
	i.rangeKeyInterleaved = false
	i.rangeKeyTruncated = false
	i.rangeKey = rk
	if rk == nil {
		i.rangeKeyItems = nil
		i.rangeKeyStart = nil
		i.rangeKeyEnd = nil
		return
	}
	i.rangeKeyItems = rk.Items
	i.rangeKeyStart = rk.Start
	i.rangeKeyEnd = rk.End
	// TODO(jackson): Currently, these key comparisons to truncate bounds happen
	// whenever the range key iterator is repositioned. We could perform this
	// lazily, and only do it first the time the user actually asks for this
	// range key's bounds in RangeKeyBounds. This would reduce work in the case
	// where there's no range key covering the point, but there does exist a
	// range key(s) in the rangeKeyIter.
	if i.lower != nil && i.cmp(i.rangeKeyStart, i.lower) < 0 {
		i.rangeKeyStart = i.lower
	}
	if i.upper != nil && i.cmp(i.upper, i.rangeKeyEnd) < 0 {
		i.rangeKeyEnd = i.upper
	}
}

// HasRangeKey returns whether there exists a range key covering the last
// key returned.
func (i *InterleavingIter) HasRangeKey() bool {
	return i.rangeKeyCoversKey
}

// RangeKeyBounds returns the covering range key's bounds.
func (i *InterleavingIter) RangeKeyBounds() (start, end []byte) {
	return i.rangeKeyStart, i.rangeKeyEnd
}

// RangeKeys returns the SuffixItems contained by the current range key. The
// returned items are sorted and consistent (eg, no more than one entry per
// Suffix). The returned items may include Unsets, which the caller may need to
// ignore.
func (i *InterleavingIter) RangeKeys() []SuffixItem {
	return i.rangeKeyItems
}

// SetBounds implements (base.InternalIterator).SetBounds.
func (i *InterleavingIter) SetBounds(lower, upper []byte) {
	i.lower, i.upper = lower, upper
	i.pointIter.SetBounds(lower, upper)
}

// Error implements (base.InternalIterator).Error.
func (i *InterleavingIter) Error() error {
	return firstError(i.pointIter.Error(), i.rangeKeyIter.Error())
}

// Close implements (base.InternalIterator).Close.
func (i *InterleavingIter) Close() error {
	return i.pointIter.Close()
}

// String implements (base.InternalIterator).String.
func (i *InterleavingIter) String() string {
	return fmt.Sprintf("range-key-interleaving(%q)", i.pointIter.String())
}

func firstError(err0, err1 error) error {
	if err0 != nil {
		return err0
	}
	return err1
}
