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
// call HasRangeKey, RangeKeyBounds and RangeKeys to retrieve range keys
// covering the key.
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
// key kind. These synthetic keys have the infinite sequence number, so that
// they're interleaved before any point keys with the same user key when
// iterating forward and after when iterating backward.
//
// Interleaving the synthetic start key boundaries at the maximum sequence
// number provides an opportunity for the higher-level, public Iterator to pause
// at the start key, even if the point key at the same user key is deleted.
//
// When returning a synthetic range-key marker, InterleavingIter will truncate
// the range key's start bound to the SeekGE or SeekPrefixGE search key. For
// example, a SeekGE("d") that finds a range key [a, z) may return a synthetic
// range-key marker `d#72057594037927935,21`.
//
// If bounds have been applied to the iterator through SetBounds,
// InterleavingIter will truncate the bounds returned through RangeKeyBounds to
// the set bounds. The bounds returned through RangeKeyBounds are not truncated
// by a SeekGE or SeekPrefixGE search key (eg, in the earlier example after
// returning the synthetic range-key marker `d#72057594037927935,21`,
// RangeKeyBounds would still return `a` and `z`.
//
// InterleavedIter elides range keys that do not contain any visible range keys
// (eg, internal range key spans that contain only Unsets and Deletes).
//
// Masking
//
// An InterleavingIter may be configured to treat range keys as masks. If a
// non-nil maskingThresholdSuffix is passed to Init, masking is enabled. Masking
// hides point keys, transparently skipping over the keys. Whether or not a
// point key is masked is determined by comparing the point key's suffix, the
// overlapping range keys' suffixes, and the maskingTreshresholdSuffix. When
// configured with a range-key masking threshold _t_, and there exists a range key
// with suffix _r_ covering a point key with suffix _p_, and
//
//     _t_ ≤ _r_ < _p_
//
// then the point key is elided. Consider the following rendering, where
// suffixes with higher integers sort before suffixes with lower integers:
//
//          ^
//       @9 |        •―――――――――――――――○ [e,m)@9
//     s  8 |                      • l@8
//     u  7 |------------------------------------ @7 masking
//     f  6 |      [h,q)@6 •―――――――――――――――――○     threshold
//     f  5 |              • h@5
//     f  4 |                          • n@4
//     i  3 |          •―――――――――――○ [f,l)@3
//     x  2 |  • b@2
//        1 |
//        0 |___________________________________
//           a b c d e f g h i j k l m n o p q
//
// An iterator scanning the entire keyspace with the masking threshold set to @7
// will observe point keys b@2 and l@8. The range keys [h,q)@6 and [f,l)@3 serve
// as masks, because cmp(@6,@7) ≥ 0 and cmp(@3,@7) ≥ 0. The range key [e,m)@9
// does not serve as a mask, because cmp(@9,@7) < 0.
//
// Although point l@8 falls within the user key bounds of [e,m)@9, [e,m)@9 is
// non-masking due to its suffix. The point key l@8 also falls within the user
// key bounds of [h,q)@6, but since cmp(@6,@8) ≥ 0, l@8 is unmasked.
//
// All range keys, including those acting as masks, are exposed during
// iteration.
type InterleavingIter struct {
	split                  base.Split
	pointIter              base.InternalIterator
	rangeKeyIter           *Iter
	maskingThresholdSuffix []byte
	maskSuffix             []byte

	// lower and upper hold the iteration bounds set through SetBounds.
	lower, upper []byte
	// keyBuf is used to copy SeekGE or SeekPrefixGE arguments when they're used
	// to truncate a range key. The byte slices backing a SeekGE/SeekPrefixGE
	// search keys can come directly from the end user, so they're copied into
	// keyBuf to ensure key stability.
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
	// last-returned key. HasRangeKey() returns this value.
	rangeKeyCoversKey bool
	// pointKeyInterleaved indicates whether the current point key has been
	// interleaved in the current direction.
	pointKeyInterleaved bool
	// rangeKeyInterleaved indicates whether or not the current rangeKey has
	// been interleaved at its start key in the current direction. A range key
	// marker is interleaved when first passing over the start key.
	//
	// When iterating in the forward direction, the range key start is
	// interleaved when the range key first begins to cover the current iterator
	// position. The rangekey iterator isn't advanced until the
	// InterleavingIterator moves beyond the current range key's end key. This
	// field is used to remember that the range key has already been interleaved
	// and shouldn't be interleaved again.
	//
	// When iterating in the reverse direction, the range key start is
	// interleaved immediately before the iterator will move to a key no longer
	// be covered by the range key. This field behaves analagously to
	// pointKeyInterleaved and if true signals that we must Prev the range key
	// iter on the next Prev call.
	rangeKeyInterleaved bool
	// rangeKeyMarkerTruncated is set by SeekGE/SeekPrefixGE calls that truncate
	// the range key's start bound marker to the search key. It's returned to
	// false on the next repositioning of the range key iter.
	rangeKeyMarkerTruncated bool
	// dir indicates the direction of iteration: forward (+1) or backward (-1)
	dir int8
}

// Assert that *InterleavingIter implements the InternalIterator interface.
var _ base.InternalIterator = &InterleavingIter{}

// Init initializes the InterleavingIter to interleave point keys from pointIter
// with range keys from rangeKeyIter.
//
// If maskingThresholdSuffix is non-nil, masking of point keys by range keys is
// enabled. Any range keys encountered with a suffix > maskingThresholdSuffix
// will act as a mask, hiding all point keys with suffixes > the range key's
// suffix within their user key bounds.
func (i *InterleavingIter) Init(
	split base.Split,
	pointIter base.InternalIterator,
	rangeKeyIter *Iter,
	maskingThresholdSuffix []byte,
) {
	*i = InterleavingIter{
		split:                  split,
		pointIter:              pointIter,
		rangeKeyIter:           rangeKeyIter,
		maskingThresholdSuffix: maskingThresholdSuffix,
	}
}

func (i *InterleavingIter) cmp(a, b []byte) int {
	return i.rangeKeyIter.coalescer.items.cmp(a, b)
}

// SeekGE implements (base.InternalIterator).SeekGE.
//
// If there exists a range key with a start key ≤ the first matching point key,
// SeekGE will return a synthetic range key marker for the range key. If this
// range key's start key is less than key, the returned marker will be truncated
// to key. Note that this search-key truncation of the marker's key is not
// applied to RangeKeyBounds.
//
// NB: In accordance with the base.InternalIterator contract:
//   i.lower ≤ key
func (i *InterleavingIter) SeekGE(key []byte, trySeekUsingNext bool) (*base.InternalKey, []byte) {
	i.pointKey, i.pointVal = i.pointIter.SeekGE(key, trySeekUsingNext)
	i.pointKeyInterleaved = false
	i.nextRangeKey(i.rangeKeyIter.SeekGE(key))
	i.dir = +1
	return i.interleaveForward(key)
}

// SeekPrefixGE implements (base.InternalIterator).SeekPrefixGE.
//
// If there exists a range key with a start key ≤ the first matching point key,
// SeekPrefixGE will return a synthetic range key marker for the range key. If
// this range key's start key is less than key, the returned marker will be
// truncated to key. Note that this search-key truncation of the marker's key is
// not applied to RangeKeyBounds.
//
// NB: In accordance with the base.InternalIterator contract:
//   i.lower ≤ key
func (i *InterleavingIter) SeekPrefixGE(prefix, key []byte, trySeekUsingNext bool) (*base.InternalKey, []byte) {
	i.pointKey, i.pointVal = i.pointIter.SeekPrefixGE(prefix, key, trySeekUsingNext)
	i.pointKeyInterleaved = false
	i.nextRangeKey(i.rangeKeyIter.SeekGE(key))
	i.dir = +1
	return i.interleaveForward(key)
}

// SeekLT implements (base.InternalIterator).SeekLT.
func (i *InterleavingIter) SeekLT(key []byte) (*base.InternalKey, []byte) {
	i.pointKey, i.pointVal = i.pointIter.SeekLT(key)
	i.pointKeyInterleaved = false
	i.prevRangeKey(i.rangeKeyIter.SeekLT(key))
	i.dir = -1
	return i.interleaveBackward()
}

// First implements (base.InternalIterator).First.
func (i *InterleavingIter) First() (*base.InternalKey, []byte) {
	i.pointKey, i.pointVal = i.pointIter.First()
	i.pointKeyInterleaved = false
	i.nextRangeKey(i.rangeKeyIter.First())
	i.dir = +1
	return i.interleaveForward(i.lower)
}

// Last implements (base.InternalIterator).Last.
func (i *InterleavingIter) Last() (*base.InternalKey, []byte) {
	i.pointKey, i.pointVal = i.pointIter.Last()
	i.pointKeyInterleaved = false
	i.prevRangeKey(i.rangeKeyIter.Last())
	i.dir = -1
	return i.interleaveBackward()
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

		// Regardless of the current iterator state, we mark the range key as
		// interleaved when switching to forward iteration, justified below.
		//
		// If the point key is the last key returned:
		//   pointIter    :         ... (y)   z ...
		//   rangeKeyIter : ... ([x -               )) ...
		//                               ^
		// The range key's start must be ≤ the point key, otherwise we'd have
		// interleaved the range key's start. From a forward-iteration
		// perspective, the range key's start is in the past and should be
		// considered already-interleaved.
		//
		// If the range key start boundary is the last key returned:
		//   pointIter    : ... (x)       z ...
		//   rangeKeyIter :     ... ([y -        )) ...
		//                            ^
		// i.rangeKey.Start is the key we last returned during reverse
		// iteration. From the perspective of forward-iteration, its start key
		// was just visited.
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
	if i.rangeKeyInterleaved && i.pointKey != nil && i.rangeKey != nil &&
		i.cmp(i.pointKey.UserKey, i.rangeKey.End) >= 0 {
		i.nextRangeKey(i.rangeKeyIter.Next())
	}
	return i.interleaveForward(i.lower)
}

// Prev implements (base.InternalIterator).Prev.
func (i *InterleavingIter) Prev() (*base.InternalKey, []byte) {
	if i.dir == +1 {
		// Switching directions.
		i.dir = -1

		if i.rangeKeyInterleaved {
			// If the current range key has already been interleaved in the
			// forward direction, the iterator has already passed over the range
			// key's start key. The start key may have been interleaved a while
			// ago, or it might've been interleaved at the current iterator
			// position. If it was interleaved a while ago, the current range
			// key is still relevant and we should not move the range key
			// iterator.
			//
			// If it was just interleaved at the current iterator position, the
			// range key start was the last key returned to the user. We should
			// prev past it so we don't return it again, with an exception.
			// Consider range key [a, z) and this sequence of iterator calls:
			//
			//   SeekGE('c') = c.RANGEKEYSET#72057594037927935
			//   Prev()      = a.RANGEKEYSET#72057594037927935
			//
			// If the current range key was last surfaced truncated due to a
			// SeekGE or SeekPrefixGE call, then it's still relevant in the
			// reverse direction with an untruncated start key.
			//
			// We can determine whether the last key returned was a point key by
			// checking i.pointKeyInterleaved, because every Next/Prev will
			// advance the point iterator and reset pointKeyInterleaved if it
			// was.
			if i.pointKeyInterleaved || i.rangeKeyMarkerTruncated {
				// The last returned key was a point key, OR a truncated range
				// key. Don't move, but re-save the range key because it should
				// no longer be considered truncated or interleaved.
				i.saveRangeKey(i.rangeKey)
			} else {
				// The last returned key is this key's start boundary, so Prev
				// past it so we don't return it again.
				i.prevRangeKey(i.rangeKeyIter.Prev())
			}
		} else {
			// If the current range key has not been interleaved, then the range
			// key's start is greater than the current iterator position (denoted in
			// parenthesis), and the current range key is ahead of our iterator
			// position. Move it to the previous range key:
			//  points:    (x*)
			//  ranges:          [y-z)*
			i.prevRangeKey(i.rangeKeyIter.Prev())
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
	return i.interleaveBackward()
}

func (i *InterleavingIter) interleaveForward(lowerBound []byte) (*base.InternalKey, []byte) {
	// This loop determines whether a point key or a range key should be
	// interleaved on each iteration. If masking is disabled, this loop executes
	// for exactly one iteration. If masking is enabled and a masked key is
	// determined to be interleaved next, this loop continues until the
	// interleaved key is unmasked.
	for {
		// Check invariants.
		// INVARIANT: !pointKeyInterleaved
		if i.pointKeyInterleaved {
			panic("pebble: invariant violation: point key interleaved")
		}
		switch {
		case i.rangeKey == nil:
		case i.pointKey == nil:
			// INVARIANT: rangeKey.HasSets()
			if !i.rangeKey.HasSets() {
				panic("pebble: invariant violation: range-key span without visible Sets")
			}
		default:
			// INVARIANT: rangeKey.HasSets()
			if !i.rangeKey.HasSets() {
				panic("pebble: invariant violation: range-key span without visible Sets")
			}
			// INVARIANT: !rangeKeyInterleaved || pointKey < rangeKey.End
			// The caller is responsible for advancing this range key if it's
			// already been interleaved and the range key ends before the point key.
			// Absolute positioning methods will never have already interleaved the
			// range key, so only Next needs to handle the case where
			// pointKey >= rangeKey.End.
			if i.rangeKeyInterleaved && i.cmp(i.pointKey.UserKey, i.rangeKey.End) >= 0 {
				panic("pebble: invariant violation: range key interleaved, but point key >= range key end")
			}
		}

		// Interleave.
		switch {
		case i.rangeKey == nil:
			// If we're out of range keys, just return the point key.
			return i.yieldPointKey(false /* covered */)
		case i.pointKey == nil:
			if i.pointKeyInterleaved {
				panic("pebble: invariant violation: point key already interleaved")
			}
			// If we're out of point keys, we need to return a range key marker. If
			// the current range key has already been interleaved, advance it. Since
			// there are no more point keys, we don't need to worry about advancing
			// past the current point key.
			if i.rangeKeyInterleaved {
				i.nextRangeKey(i.rangeKeyIter.Next())
				if i.rangeKey == nil {
					return i.yieldNil()
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
				// => then the point key must be less than the range key's end, and
				//    the point key must be covered by the current range key.

				// The range key covers the point key. The point key might be
				// masked too if range-key masking is enabled.
				if i.maskSuffix != nil {
					pointSuffix := i.pointKey.UserKey[i.split(i.pointKey.UserKey):]
					if len(pointSuffix) > 0 && i.cmp(i.maskSuffix, pointSuffix) < 0 {
						// A range key in the current coalesced span masks this
						// point key. Skip the point key.
						i.pointKey, i.pointVal = i.pointIter.Next()
						// We may have just invalidated the invariant that
						// ensures the range key's End is > the point key, so
						// reestablish it before the next iteration.
						if i.pointKey != nil && i.cmp(i.pointKey.UserKey, i.rangeKey.End) >= 0 {
							i.nextRangeKey(i.rangeKeyIter.Next())
						}
						continue
					}
				}

				// Point key is unmasked but covered.
				return i.yieldPointKey(true /* covered */)
			}
			return i.yieldPointKey(false /* covered */)
		}
	}
}

func (i *InterleavingIter) interleaveBackward() (*base.InternalKey, []byte) {
	// This loop determines whether a point key or a range key should be
	// interleaved on each iteration. If masking is disabled, this loop executes
	// for exactly one iteration. If masking is enabled and a masked key is
	// determined to be interleaved next, this loop continues until the
	// interleaved key is unmasked.
	for {
		// Check invariants.
		// INVARIANT: !pointKeyInterleaved
		if i.pointKeyInterleaved {
			panic("pebble: invariant violation: point key interleaved")
		}
		switch {
		case i.rangeKey == nil:
		case i.pointKey == nil:
			// INVARAINT: rangeKey.HasSets()
			if i.rangeKey != nil && !i.rangeKey.HasSets() {
				panic("pebble: invariant violation: range-key span without visible Sets")
			}
		default:
			// INVARAINT: rangeKey.HasSets()
			if i.rangeKey != nil && !i.rangeKey.HasSets() {
				panic("pebble: invariant violation: range-key span without visible Sets")
			}
		}

		// Interleave.
		switch {
		case i.rangeKey == nil:
			// If we're out of range keys, just return the point key.
			return i.yieldPointKey(false /* covered */)
		case i.pointKey == nil:
			// If we're out of point keys, we need to return a range key marker.
			return i.yieldSyntheticRangeKeyMarker(i.lower)
		default:
			// If the range key's start key is greater than the point key, return a
			// marker for the range key.
			if i.cmp(i.rangeKey.Start, i.pointKey.UserKey) > 0 {
				return i.yieldSyntheticRangeKeyMarker(i.lower)
			}
			// We have a range key but it has not been interleaved and begins at
			// a key equal to or before the current point key. The point key
			// should be interleaved next, if it's not masked.
			if i.cmp(i.pointKey.UserKey, i.rangeKey.End) < 0 {
				// The range key covers the point key. The point key might be
				// masked too if range-key masking is enabled.

				// Since this point key is covered by the range key, it might be
				// masked by the range key if range-key masking is enabled.
				if i.maskSuffix != nil {
					pointSuffix := i.pointKey.UserKey[i.split(i.pointKey.UserKey):]
					if len(pointSuffix) > 0 && i.cmp(i.maskSuffix, pointSuffix) < 0 {
						// A range key in the current coalesced span masks this
						// point key. Skip the point key.
						i.pointKey, i.pointVal = i.pointIter.Prev()
						continue
					}
				}

				// Point key is unmasked but covered.
				return i.yieldPointKey(true /* covered */)
			}
			return i.yieldPointKey(false /* covered */)
		}
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

func (i *InterleavingIter) yieldNil() (*base.InternalKey, []byte) {
	i.rangeKeyCoversKey = false
	return i.verify(nil, nil)
}

func (i *InterleavingIter) yieldPointKey(covered bool) (*base.InternalKey, []byte) {
	i.pointKeyInterleaved = true
	i.rangeKeyCoversKey = covered
	return i.verify(i.pointKey, i.pointVal)
}

func (i *InterleavingIter) yieldSyntheticRangeKeyMarker(lowerBound []byte) (*base.InternalKey, []byte) {
	i.rangeKeyMarker.UserKey = i.rangeKey.Start
	i.rangeKeyMarker.Trailer = base.InternalKeyBoundaryRangeKey
	i.rangeKeyInterleaved = true
	i.rangeKeyCoversKey = true

	// Truncate the key we return to our lower bound if we have one. Note that
	// we use the lowerBound function parameter, not i.lower. The lowerBound
	// argument is guaranteed to be ≥ i.lower. It may be equal to the SetBounds
	// lower bound, or it could come from a SeekGE or SeekPrefixGE search key.
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
			i.rangeKeyMarkerTruncated = true
		}
	}
	return i.verify(&i.rangeKeyMarker, nil)
}

func (i *InterleavingIter) verify(k *base.InternalKey, v []byte) (*base.InternalKey, []byte) {
	// TODO(jackson): Wrap the entire body of this function in an
	// invariants.Enabled conditional, so that in production builds this
	// function is empty and may be inlined away.

	switch {
	case k != nil && !i.rangeKeyInterleaved && !i.pointKeyInterleaved:
		panic("pebble: invariant violation: both keys marked as noninterleaved")
	case i.dir == -1 && k != nil && i.rangeKeyInterleaved == i.pointKeyInterleaved:
		// During reverse iteration, if we're returning a key, either the range
		// key must have been interleaved OR the current point key value is
		// being returned, not both.
		//
		// This invariant holds because in reverse iteration the start key of the
		// range behaves like a point. Once the start key is interleaved, we move
		// the range key iterator to the previous range key.
		panic(fmt.Sprintf("pebble: invariant violation: interleaving (point %t, range %t)",
			i.pointKeyInterleaved, i.rangeKeyInterleaved))
	case i.dir == -1 && i.rangeKeyMarkerTruncated:
		panic("pebble: invariant violation: truncated range key in reverse iteration")
	case k != nil && i.lower != nil && i.cmp(k.UserKey, i.lower) < 0:
		panic("pebble: invariant violation: key < lower bound")
	case k != nil && i.upper != nil && i.cmp(k.UserKey, i.upper) >= 0:
		panic("pebble: invariant violation: key ≥ lower bound")
	case i.HasRangeKey() && len(i.rangeKeyItems) == 0:
		panic("pebble: invariant violation: range key with no items")
	case i.rangeKey != nil && k != nil && i.maskSuffix != nil && i.pointKeyInterleaved &&
		i.split(k.UserKey) != len(k.UserKey) &&
		i.cmp(i.maskSuffix, k.UserKey[i.split(k.UserKey):]) < 0 &&
		i.cmp(k.UserKey, i.rangeKeyStart) >= 0 && i.cmp(k.UserKey, i.rangeKeyEnd) < 0:
		panic("pebble: invariant violation: point key eligible for masking returned")
	}

	return k, v
}

func (i *InterleavingIter) saveRangeKey(rk *CoalescedSpan) {
	i.rangeKeyInterleaved = false
	i.rangeKeyMarkerTruncated = false
	i.rangeKey = rk
	i.maskSuffix = nil
	if rk == nil {
		i.rangeKeyItems = nil
		i.rangeKeyStart = nil
		i.rangeKeyEnd = nil
		return
	}
	i.maskSuffix = rk.SmallestSetSuffix(i.cmp, i.maskingThresholdSuffix)
	i.rangeKeyItems = rk.Items
	i.rangeKeyStart = rk.Start
	i.rangeKeyEnd = rk.End
	// TODO(jackson): The key comparisons below truncate bounds whenever the
	// range key iterator is repositioned. We could perform this lazily, and do
	// it the first time the user actually asks for this range key's bounds in
	// RangeKeyBounds. This would reduce work in the case where there's no range
	// key covering the point and the rangeKeyIter is non-empty.

	// NB: These truncations don't require setting `rangeKeyMarkerTruncated`:
	// That flag only applies to truncated range key markers.
	if i.lower != nil && i.cmp(i.rangeKeyStart, i.lower) < 0 {
		i.rangeKeyStart = i.lower
	}
	if i.upper != nil && i.cmp(i.upper, i.rangeKeyEnd) < 0 {
		i.rangeKeyEnd = i.upper
	}
}

// HasRangeKey returns whether there exists a range key covering the last key
// returned. A range key is considered to 'cover' a key if the key falls within
// the range key's user key bounds. HasRangeKey does not consider or compare
// suffixes.
func (i *InterleavingIter) HasRangeKey() bool {
	return i.rangeKeyCoversKey
}

// RangeKeyBounds returns the covering range key's bounds. These bounds are
// guaranteed to be truncated to the bounds set by SetBounds. Unlike the
// interleaving synthetic range key marker returned at range key start bounds
// during SeekGE and SeekPrefixGE, these bounds are never truncated to a seek
// search key.
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
