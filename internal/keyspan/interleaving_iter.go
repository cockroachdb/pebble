// Copyright 2021 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package keyspan

import (
	"fmt"

	"github.com/cockroachdb/pebble/internal/base"
)

// TODO(jackson): The interleaving iterator has various invariants that it
// asserts. We should eventually gate these behind `invariants.Enabled`.

// Hooks configure the interleaving iterator's behavior.
type Hooks struct {
	// SpanChanged is invoked by interleaving iterator whenever the Span
	// returned by InterleavaingIterator.Span changes. As the iterator passes
	// into or out of a Span, it invokes SpanChanged, passing the new Span.
	SpanChanged func(Span)
	// SkipPoint is invoked by the interleaving iterator whenever the iterator
	// encounters a point key covered by a Span. If SkipPoint returns true, the
	// interleaving iterator skips the point key without returning it. This is
	// used during range key iteration to skip over point keys 'masked' by range
	// keys.
	SkipPoint func(userKey []byte) bool
}

// InterleavingIter combines an iterator over point keys with an iterator over
// key spans.
//
// Throughout Pebble, some keys apply at single discrete points within the user
// keyspace. Other keys apply over continuous spans of the user key space.
// Internally, iterators over point keys adhere to the base.InternalIterator
// interface, and iterators over spans adhere to the keyspan.FragmentIterator
// interface. The InterleavingIterator wraps a point iterator and span iterator,
// providing access to all the elements of both iterators.
//
// The InterleavingIterator implements the point base.InternalIterator
// interface. After any of the iterator's methods return a key, a caller may
// call Span to retrieve the span covering the returned key, if any.  A span is
// considered to 'cover' a returned key if the span's [start, end) bounds
// include the key's user key.
//
// In addition to tracking the current covering span, InterleavingIter returns a
// special InternalKey at span start boundaries. Start boundaries are surfaced
// as a synthetic span marker: an InternalKey with the boundary as the user key,
// the infinite sequence number and a key kind selected from an arbitrary key
// the infinite sequence number and an arbitrary contained key's kind. Since
// which of the Span's key's kind is surfaced is undefined, the caller should
// not use the InternalKey's kind. The caller should only rely on the `Span`
// method for retrieving information about spanning keys. The interleaved
// synthetic keys have the infinite sequence number so that they're interleaved
// before any point keys with the same user key when iterating forward and after
// when iterating backward.
//
// Interleaving the synthetic start key boundaries at the maximum sequence
// number provides an opportunity for the higher-level, public Iterator to
// observe the Span, even if no live points keys exist within the boudns of the
// Span.
//
// When returning a synthetic marker key for a start boundary, InterleavingIter
// will truncate the span's start bound to the SeekGE or SeekPrefixGE search
// key. For example, a SeekGE("d") that finds a span [a, z) may return a
// synthetic span marker key `d#72057594037927935,21`.
//
// If bounds have been applied to the iterator through SetBounds,
// InterleavingIter will truncate the bounds of spans returned through Span to
// the set bounds. The bounds returned through Span are not truncated by a
// SeekGE or SeekPrefixGE search key. Consider, for example SetBounds('c', 'e'),
// with an iterator containing the Span [a,z):
//
//     First()     = `c#72057594037927935,21`        Span() = [c,e)
//     SeekGE('d') = `d#72057594037927935,21`        Span() = [c,e)
//
// InterleavedIter does not interleave synthetic markers for spans that do not
// contain any keys.
//
// Hooks
//
// InterelavingIter takes a Hooks parameter that may be used to configure the
// behavior of the iterator. See the documentation on the Hooks type.
//
// All spans containing keys are exposed during iteration.
type InterleavingIter struct {
	cmp         base.Compare
	pointIter   base.InternalIteratorWithStats
	keyspanIter FragmentIterator
	hooks       Hooks

	// lower and upper hold the iteration bounds set through SetBounds.
	lower, upper []byte
	// keyBuf is used to copy SeekGE or SeekPrefixGE arguments when they're used
	// to truncate a span. The byte slices backing a SeekGE/SeekPrefixGE search
	// keys can come directly from the end user, so they're copied into keyBuf
	// to ensure key stability.
	keyBuf   []byte
	pointKey *base.InternalKey
	pointVal []byte
	// span, spanStart and spanEnd describe the current span state.
	// span{Start,End} are a function of the span and the Iterator's bounds.
	// span{Start,End} will be nil iff span is !Valid().
	span      Span
	spanStart []byte
	spanEnd   []byte
	// spanMarker holds the synthetic key that is returned when the iterator
	// passes over a key span's start bound.
	spanMarker base.InternalKey

	// Keeping all of the bools together reduces the sizeof the struct.

	// spanCoversKey indicates whether the current span covers the last-returned
	// key.
	spanCoversKey bool
	// pointKeyInterleaved indicates whether the current point key has been
	// interleaved in the current direction.
	pointKeyInterleaved bool
	// keyspanInterleaved indicates whether or not the current span has been
	// interleaved at its start key in the current direction. A span marker is
	// interleaved when first passing over the start key.
	//
	// When iterating in the forward direction, the span start key is
	// interleaved when the span first begins to cover the current iterator
	// position. The keyspan iterator isn't advanced until the
	// InterleavingIterator moves beyond the current span's end key. This field
	// is used to remember that the span has already been interleaved and
	// shouldn't be interleaved again.
	//
	// When iterating in the reverse direction, the span start key is
	// interleaved immediately before the iterator will move to a key no longer
	// be covered by the span. This field behaves analagously to
	// pointKeyInterleaved and if true signals that we must Prev the keyspan
	// iterator on the next Prev call.
	keyspanInterleaved bool
	// spanMarkerTruncated is set by SeekGE/SeekPrefixGE calls that truncate a
	// span's start bound marker to the search key. It's returned to false on
	// the next repositioning of the keyspan iterator.
	spanMarkerTruncated bool
	// dir indicates the direction of iteration: forward (+1) or backward (-1)
	dir int8
}

// Assert that *InterleavingIter implements the InternalIterator interface.
var _ base.InternalIterator = &InterleavingIter{}

// Init initializes the InterleavingIter to interleave point keys from pointIter
// with key spans from keyspanIter.
//
// The point iterator must already have the provided bounds. Init does not
// propagate the bounds down the iterator stack.
func (i *InterleavingIter) Init(
	cmp base.Compare,
	pointIter base.InternalIteratorWithStats,
	keyspanIter FragmentIterator,
	hooks Hooks,
	lowerBound, upperBound []byte,
) {
	*i = InterleavingIter{
		cmp:         cmp,
		pointIter:   pointIter,
		keyspanIter: keyspanIter,
		hooks:       hooks,
		lower:       lowerBound,
		upper:       upperBound,
	}
}

// SeekGE implements (base.InternalIterator).SeekGE.
//
// If there exists a span with a start key ≤ the first matching point key,
// SeekGE will return a synthetic span marker key for the span. If this span's
// start key is less than key, the returned marker will be truncated to key.
// Note that this search-key truncation of the marker's key is not applied to
// the span returned by Span.
//
// NB: In accordance with the base.InternalIterator contract:
//   i.lower ≤ key
func (i *InterleavingIter) SeekGE(key []byte, trySeekUsingNext bool) (*base.InternalKey, []byte) {
	i.pointKey, i.pointVal = i.pointIter.SeekGE(key, trySeekUsingNext)
	i.pointKeyInterleaved = false
	i.keyspanSeekGE(key)
	i.dir = +1
	return i.interleaveForward(key)
}

// SeekPrefixGE implements (base.InternalIterator).SeekPrefixGE.
//
// If there exists a span with a start key ≤ the first matching point key,
// SeekPrefixGE will return a synthetic span marker key for the span. If this
// span's start key is less than key, the returned marker will be truncated to
// key. Note that this search-key truncation of the marker's key is not applied
// to the span returned by Span.
//
// NB: In accordance with the base.InternalIterator contract:
//   i.lower ≤ key
func (i *InterleavingIter) SeekPrefixGE(
	prefix, key []byte, trySeekUsingNext bool,
) (*base.InternalKey, []byte) {
	i.pointKey, i.pointVal = i.pointIter.SeekPrefixGE(prefix, key, trySeekUsingNext)
	i.pointKeyInterleaved = false
	i.keyspanSeekGE(key)
	i.dir = +1
	return i.interleaveForward(key)
}

// SeekLT implements (base.InternalIterator).SeekLT.
func (i *InterleavingIter) SeekLT(key []byte) (*base.InternalKey, []byte) {
	i.pointKey, i.pointVal = i.pointIter.SeekLT(key)
	i.pointKeyInterleaved = false
	i.keyspanSeekLT(key)
	i.dir = -1
	return i.interleaveBackward()
}

// First implements (base.InternalIterator).First.
func (i *InterleavingIter) First() (*base.InternalKey, []byte) {
	i.pointKey, i.pointVal = i.pointIter.First()
	i.pointKeyInterleaved = false
	i.saveKeyspan(i.checkForwardBound(i.keyspanIter.First()))
	i.dir = +1
	return i.interleaveForward(i.lower)
}

// Last implements (base.InternalIterator).Last.
func (i *InterleavingIter) Last() (*base.InternalKey, []byte) {
	i.pointKey, i.pointVal = i.pointIter.Last()
	i.pointKeyInterleaved = false
	i.saveKeyspan(i.checkBackwardBound(i.keyspanIter.Last()))
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
		// returned a span start boundary key:
		//   points:    x*            z
		//    spans:        ([y-?))
		// direction. Either way, we must move to the next point key.
		switch {
		case i.pointKey == nil && i.lower == nil:
			i.pointKey, i.pointVal = i.pointIter.First()
		case i.pointKey == nil && i.lower != nil:
			i.pointKey, i.pointVal = i.pointIter.SeekGE(i.lower, false)
		default:
			i.pointKey, i.pointVal = i.pointIter.Next()
		}
		i.pointKeyInterleaved = false

		if !i.span.Valid() {
			// There was no span in the reverse direction, but there may be
			// a span in the forward direction.
			i.saveKeyspan(i.checkForwardBound(i.keyspanIter.Next()))
			i.keyspanInterleaved = false
		} else {
			// Regardless of the current iterator state, we mark any existing
			// span as interleaved when switching to forward iteration,
			// justified below.
			//
			// If the point key is the last key returned:
			//   pointIter   :         ... (y)   z ...
			//   keyspanIter : ... ([x -               )) ...
			//                              ^
			// The span's start key must be ≤ the point key, otherwise we'd have
			// interleaved the span's start key. From a forward-iteration
			// perspective, the span's start key is in the past and should be
			// considered already-interleaved.
			//
			// If the span start boundary key is the last key returned:
			//   pointIter   : ... (x)       z ...
			//   keyspanIter :     ... ([y -        )) ...
			//                           ^
			// i.span.Start is the key we last returned during reverse
			// iteration. From the perspective of forward-iteration, its start
			// key was just visited.
			i.keyspanInterleaved = true
		}
	}

	// Refresh the point key if the current point key has already been
	// interleaved.
	if i.pointKeyInterleaved {
		i.pointKey, i.pointVal = i.pointIter.Next()
		i.pointKeyInterleaved = false
	}
	// If we already interleaved the current span start key, and the point key
	// is ≥ the span's end key, move to the next span.
	if i.keyspanInterleaved && i.pointKey != nil && i.span.Valid() &&
		i.cmp(i.pointKey.UserKey, i.span.End) >= 0 {
		i.saveKeyspan(i.checkForwardBound(i.keyspanIter.Next()))
	}
	return i.interleaveForward(i.lower)
}

// Prev implements (base.InternalIterator).Prev.
func (i *InterleavingIter) Prev() (*base.InternalKey, []byte) {
	if i.dir == +1 {
		// Switching directions.
		i.dir = -1

		if i.keyspanInterleaved {
			// The current span's start key has already been interleaved in the
			// forward direction. The start key may have been interleaved a
			// while ago, or it might've been interleaved at the current
			// iterator position. If it was interleaved a while ago, the current
			// span is still relevant and we should not move the keyspan
			// iterator.
			//
			// If it was just interleaved at the current iterator position, the
			// span start was the last key returned to the user. We should
			// prev past it so we don't return it again, with an exception.
			// Consider span [a, z) and this sequence of iterator calls:
			//
			//   SeekGE('c') = c.RANGEKEYSET#72057594037927935
			//   Prev()      = a.RANGEKEYSET#72057594037927935
			//
			// If the current span's start key was last surfaced truncated due
			// to a SeekGE or SeekPrefixGE call, then it's still relevant in the
			// reverse direction with an untruncated start key.
			//
			// We can determine whether the last key returned was a point key by
			// checking i.pointKeyInterleaved, because every Next/Prev will
			// advance the point iterator and reset pointKeyInterleaved if it
			// was.
			if i.pointKeyInterleaved || i.spanMarkerTruncated {
				// The last returned key was a point key, OR a truncated span
				// marker key. Don't move, but re-save the span because it
				// should no longer be considered truncated or interleaved.
				i.saveKeyspan(i.span)
			} else {
				// The last returned key is this key's start boundary, so Prev
				// past it so we don't return it again.
				i.saveKeyspan(i.checkBackwardBound(i.keyspanIter.Prev()))
			}
		} else {
			// If the current span's start key has not been interleaved, then
			// the span's start key is greater than the current iterator
			// position (denoted in parenthesis), and the current span's start
			// key is ahead of our iterator position. Move it to the previous
			// span:
			//  points:    (x*)
			//    span:          [y-z)*
			i.saveKeyspan(i.checkBackwardBound(i.keyspanIter.Prev()))
		}

		// The existing point key (denoted below with *) is either the last
		// key we returned (the current iterator position):
		//   points:    x     (y*)    z
		// or the upcoming point key in the forward direction if we just
		// returned a span start boundary key :
		//   points:    x             z*
		//    spans:        ([y-?))
		// direction. Either way, we must move the point iterator backwards.
		switch {
		case i.pointKey == nil && i.upper == nil:
			i.pointKey, i.pointVal = i.pointIter.Last()
		case i.pointKey == nil && i.upper != nil:
			i.pointKey, i.pointVal = i.pointIter.SeekLT(i.upper)
		default:
			i.pointKey, i.pointVal = i.pointIter.Prev()
		}
		i.pointKeyInterleaved = false
	}

	// Refresh the point key if we just returned the current point key.
	if i.pointKeyInterleaved {
		i.pointKey, i.pointVal = i.pointIter.Prev()
		i.pointKeyInterleaved = false
	}
	// Refresh the span if we just returned the span's start boundary key.
	if i.keyspanInterleaved {
		i.saveKeyspan(i.checkBackwardBound(i.keyspanIter.Prev()))
	}
	return i.interleaveBackward()
}

func (i *InterleavingIter) interleaveForward(lowerBound []byte) (*base.InternalKey, []byte) {
	// This loop determines whether a point key or a span marker key should be
	// interleaved on each iteration. If masking is disabled and the span is
	// nonempty, this loop executes for exactly one iteration. If masking is
	// enabled and a masked key is determined to be interleaved next, this loop
	// continues until the interleaved key is unmasked. If a span's start key
	// should be interleaved next, but the span is empty, the loop continues to
	// the next key.
	for {
		// Check invariants.
		// INVARIANT: !pointKeyInterleaved
		if i.pointKeyInterleaved {
			panic("pebble: invariant violation: point key interleaved")
		}
		switch {
		case !i.span.Valid():
		case i.pointKey == nil:
		default:
			// INVARIANT: !keyspanInterleaved || pointKey < span.End
			// The caller is responsible for advancing this span if it's already
			// been interleaved and the span ends before the point key.
			// Absolute positioning methods will never have already interleaved
			// the span's start key, so only Next needs to handle the case where
			// pointKey >= span.End.
			if i.keyspanInterleaved && i.cmp(i.pointKey.UserKey, i.span.End) >= 0 {
				panic("pebble: invariant violation: span interleaved, but point key >= span end")
			}
		}

		// Interleave.
		switch {
		case !i.span.Valid():
			// If we're out of spans, just return the point key.
			return i.yieldPointKey(false /* covered */)
		case i.pointKey == nil:
			if i.pointKeyInterleaved {
				panic("pebble: invariant violation: point key already interleaved")
			}
			// If we're out of point keys, we need to return a span marker. If
			// the current span has already been interleaved, advance it. Since
			// there are no more point keys, we don't need to worry about
			// advancing past the current point key.
			if i.keyspanInterleaved {
				i.saveKeyspan(i.checkForwardBound(i.keyspanIter.Next()))
				if !i.span.Valid() {
					return i.yieldNil()
				}
			}
			if i.span.Empty() {
				i.keyspanInterleaved = true
				continue
			}
			return i.yieldSyntheticSpanMarker(lowerBound)
		default:
			if i.cmp(i.pointKey.UserKey, i.span.Start) >= 0 {
				// The span start key lies before the point key. If we haven't
				// interleaved it, we should.
				if !i.keyspanInterleaved {
					if i.span.Empty() {
						if i.pointKey != nil && i.cmp(i.pointKey.UserKey, i.span.End) >= 0 {
							// Advance the keyspan iterator, as just flipping
							// keyspanInterleaved would likely trip up the invariant check
							// above.
							i.saveKeyspan(i.checkForwardBound(i.keyspanIter.Next()))
						} else {
							i.keyspanInterleaved = true
						}
						continue
					}
					return i.yieldSyntheticSpanMarker(lowerBound)
				}

				// Otherwise, the span's start key is already interleaved and we
				// need to return the point key. The current span necessarily
				// must cover the point key:
				//
				// Since the span's start is less than or equal to the point
				// key, the only way for this span to not cover the point would
				// be if the span's end is less than or equal to the point.
				// (For example span = [a, b), point key = c).
				//
				// However, the invariant at the beginning of the function
				// guarantees that if:
				//  * we have both a point key and a span
				//  * and the span has already been interleaved
				// => then the point key must be less than the span's end, and
				//    the point key must be covered by the current span.

				// The span covers the point key. If a SkipPoint hook is
				// configured, ask it if we should skip this point key.
				if i.hooks.SkipPoint != nil && i.hooks.SkipPoint(i.pointKey.UserKey) {
					i.pointKey, i.pointVal = i.pointIter.Next()
					// We may have just invalidated the invariant that
					// ensures the span's End is > the point key, so
					// reestablish it before the next iteration.
					if i.pointKey != nil && i.cmp(i.pointKey.UserKey, i.span.End) >= 0 {
						i.saveKeyspan(i.checkForwardBound(i.keyspanIter.Next()))
					}
					continue
				}

				// Point key is unmasked but covered.
				return i.yieldPointKey(true /* covered */)
			}
			return i.yieldPointKey(false /* covered */)
		}
	}
}

func (i *InterleavingIter) interleaveBackward() (*base.InternalKey, []byte) {
	// This loop determines whether a point key or a span's start key should be
	// interleaved on each iteration. If masking is disabled and the span is
	// nonempty, this loop executes for exactly one iteration. If masking is
	// enabled and a masked key is determined to be interleaved next, this loop
	// continues until the interleaved key is unmasked. If a span's start key
	// should be interleaved next, but the span is empty, the loop continues to
	// the next key.
	for {
		// Check invariants.
		// INVARIANT: !pointKeyInterleaved
		if i.pointKeyInterleaved {
			panic("pebble: invariant violation: point key interleaved")
		}

		// Interleave.
		switch {
		case !i.span.Valid():
			// If we're out of spans, just return the point key.
			return i.yieldPointKey(false /* covered */)
		case i.pointKey == nil:
			// If we're out of point keys, we need to return a span marker.
			if i.span.Empty() {
				i.saveKeyspan(i.checkBackwardBound(i.keyspanIter.Prev()))
				continue
			}
			return i.yieldSyntheticSpanMarker(i.lower)
		default:
			// If the span's start key is greater than the point key, return a
			// marker for the span.
			if i.cmp(i.span.Start, i.pointKey.UserKey) > 0 {
				if i.span.Empty() {
					i.saveKeyspan(i.checkBackwardBound(i.keyspanIter.Prev()))
					continue
				}
				return i.yieldSyntheticSpanMarker(i.lower)
			}
			// We have a span but it has not been interleaved and begins at a
			// key equal to or before the current point key. The point key
			// should be interleaved next, if it's not masked.
			if i.cmp(i.pointKey.UserKey, i.span.End) < 0 {
				// The span covers the point key. The point key might be masked
				// too if masking is enabled.

				// The span covers the point key. If a SkipPoint hook is
				// configured, ask it if we should skip this point key.
				if i.hooks.SkipPoint != nil && i.hooks.SkipPoint(i.pointKey.UserKey) {
					i.pointKey, i.pointVal = i.pointIter.Prev()
					continue
				}

				// Point key is unmasked but covered.
				return i.yieldPointKey(true /* covered */)
			}
			return i.yieldPointKey(false /* covered */)
		}
	}
}

// keyspanSeekGE seeks the keyspan iterator to the first span covering k ≥ key.
// Note that this differs from the FragmentIterator.SeekGE semantics, which
// seek to the first span with a start key ≥ key.
func (i *InterleavingIter) keyspanSeekGE(key []byte) {
	// Seek using SeekLT to look for a span that starts before key, with an end
	// boundary extending beyond key.
	s := i.keyspanIter.SeekLT(key)
	if !s.Valid() || i.cmp(s.End, key) <= 0 {
		// The iterator is exhausted in the reverse direction, or the span we
		// found ends before key. Next to the first key with a start ≥ key.
		s = i.keyspanIter.Next()
	}
	i.saveKeyspan(i.checkForwardBound(s))
}

// keyspanSeekLT seeks the keyspan iterator to the last span covering k < key.
// Note that this differs from the FragmentIterator.SeekLT semantics, which
// seek to the last span with a start key < key.
func (i *InterleavingIter) keyspanSeekLT(key []byte) {
	s := i.checkBackwardBound(i.keyspanIter.SeekLT(key))
	// s.Start is not guaranteed to be less than key, because of the bounds
	// enforcement. Consider the following example:
	//
	// Bounds are set to [d,e). The user performs a SeekLT(d). The
	// FragmentIterator.SeekLT lands on a span [b,f). This span has a start key
	// less than d, as expected. Above, checkBackwardBound truncates the span to
	// match the iterator's current bounds, modifying the span to [d,e), which
	// does not overlap the search space of [-∞, d).
	//
	// This problem is a consequence of the SeekLT's exclusive search key and
	// the fact that we don't perform bounds truncation at every leaf iterator.
	if i.cmp(s.Start, key) >= 0 {
		s = Span{}
	}
	i.saveKeyspan(s)
}

func (i *InterleavingIter) checkForwardBound(s Span) Span {
	// Check the upper bound if we have one.
	if s.Valid() && i.upper != nil && i.cmp(s.Start, i.upper) >= 0 {
		return Span{}
	}

	// TODO(jackson): The key comparisons below truncate bounds whenever the
	// keyspan iterator is repositioned. We could perform this lazily, and do it
	// the first time the user actually asks for this span's bounds in
	// SpanBounds. This would reduce work in the case where there's no span
	// covering the point and the keyspan iterator is non-empty.

	// NB: These truncations don't require setting `keyspanMarkerTruncated`:
	// That flag only applies to truncated span marker keys.
	if i.lower != nil && i.cmp(s.Start, i.lower) < 0 {
		s.Start = i.lower
	}
	if i.upper != nil && i.cmp(i.upper, s.End) < 0 {
		s.End = i.upper
	}
	if i.cmp(s.Start, s.End) == 0 {
		return Span{}
	}
	return s
}

func (i *InterleavingIter) checkBackwardBound(s Span) Span {
	// Check the lower bound if we have one.
	if s.Valid() && i.lower != nil && i.cmp(s.End, i.lower) <= 0 {
		return Span{}
	}

	// TODO(jackson): The key comparisons below truncate bounds whenever the
	// keyspan iterator is repositioned. We could perform this lazily, and do it
	// the first time the user actually asks for this span's bounds in
	// SpanBounds. This would reduce work in the case where there's no span
	// covering the point and the keyspan iterator is non-empty.

	// NB: These truncations don't require setting `keyspanMarkerTruncated`:
	// That flag only applies to truncated span marker keys.
	if i.lower != nil && i.cmp(s.Start, i.lower) < 0 {
		s.Start = i.lower
	}
	if i.upper != nil && i.cmp(i.upper, s.End) < 0 {
		s.End = i.upper
	}
	if i.cmp(s.Start, s.End) == 0 {
		return Span{}
	}
	return s
}

func (i *InterleavingIter) yieldNil() (*base.InternalKey, []byte) {
	i.spanCoversKey = false
	return i.verify(nil, nil)
}

func (i *InterleavingIter) yieldPointKey(covered bool) (*base.InternalKey, []byte) {
	i.pointKeyInterleaved = true
	i.spanCoversKey = covered
	return i.verify(i.pointKey, i.pointVal)
}

func (i *InterleavingIter) yieldSyntheticSpanMarker(lowerBound []byte) (*base.InternalKey, []byte) {
	i.spanMarker.UserKey = i.span.Start
	i.spanMarker.Trailer = base.MakeTrailer(base.InternalKeySeqNumMax, i.span.Keys[0].Kind())
	i.keyspanInterleaved = true
	i.spanCoversKey = true

	// Truncate the key we return to our lower bound if we have one. Note that
	// we use the lowerBound function parameter, not i.lower. The lowerBound
	// argument is guaranteed to be ≥ i.lower. It may be equal to the SetBounds
	// lower bound, or it could come from a SeekGE or SeekPrefixGE search key.
	if lowerBound != nil && i.cmp(lowerBound, i.span.Start) > 0 {
		// Truncating to the lower bound may violate the upper bound if
		// lowerBound == i.upper. For example, a SeekGE(k) uses k as a lower
		// bound for truncating a span. The span a-z will be truncated to [k,
		// z). If i.upper == k, we'd mistakenly try to return a span [k, k), an
		// invariant violation.
		if i.cmp(lowerBound, i.upper) == 0 {
			return i.yieldNil()
		}

		// If the lowerBound argument came from a SeekGE or SeekPrefixGE
		// call, and it may be backed by a user-provided byte slice that is not
		// guaranteed to be stable.
		//
		// If the lowerBound argument is the lower bound set by SetBounds,
		// Pebble owns the slice's memory. However, consider two successive
		// calls to SetBounds(). The second may overwrite the lower bound.
		// Although the external contract requires a seek after a SetBounds,
		// Pebble's tests don't always. For this reason and to simplify
		// reasoning around lifetimes, always copy the bound into keyBuf when
		// truncating.
		i.keyBuf = append(i.keyBuf[:0], lowerBound...)
		i.spanMarker.UserKey = i.keyBuf
		i.spanMarkerTruncated = true
	}
	return i.verify(&i.spanMarker, nil)
}

func (i *InterleavingIter) verify(k *base.InternalKey, v []byte) (*base.InternalKey, []byte) {
	// TODO(jackson): Wrap the entire body of this function in an
	// invariants.Enabled conditional, so that in production builds this
	// function is empty and may be inlined away.

	switch {
	case k != nil && !i.keyspanInterleaved && !i.pointKeyInterleaved:
		panic("pebble: invariant violation: both keys marked as noninterleaved")
	case i.dir == -1 && k != nil && i.keyspanInterleaved == i.pointKeyInterleaved:
		// During reverse iteration, if we're returning a key, either the span's
		// start key must have been interleaved OR the current point key value
		// is being returned, not both.
		//
		// This invariant holds because in reverse iteration the start key of the
		// span behaves like a point. Once the start key is interleaved, we move
		// the keyspan iterator to the previous span.
		panic(fmt.Sprintf("pebble: invariant violation: interleaving (point %t, span %t)",
			i.pointKeyInterleaved, i.keyspanInterleaved))
	case i.dir == -1 && i.spanMarkerTruncated:
		panic("pebble: invariant violation: truncated span key in reverse iteration")
	case k != nil && i.lower != nil && i.cmp(k.UserKey, i.lower) < 0:
		panic("pebble: invariant violation: key < lower bound")
	case k != nil && i.upper != nil && i.cmp(k.UserKey, i.upper) >= 0:
		panic("pebble: invariant violation: key ≥ upper bound")
	case i.span.Valid() && k != nil && i.hooks.SkipPoint != nil && i.pointKeyInterleaved &&
		i.cmp(k.UserKey, i.spanStart) >= 0 && i.cmp(k.UserKey, i.spanEnd) < 0 && i.hooks.SkipPoint(k.UserKey):
		panic("pebble: invariant violation: point key eligible for skipping returned")
	}

	return k, v
}

func (i *InterleavingIter) saveKeyspan(s Span) {
	i.keyspanInterleaved = false
	i.spanMarkerTruncated = false
	i.span = s
	if !s.Valid() {
		i.spanStart = nil
		i.spanEnd = nil
		if i.hooks.SpanChanged != nil {
			i.hooks.SpanChanged(s)
		}
		return
	}
	i.spanStart = s.Start
	i.spanEnd = s.End

	if i.hooks.SpanChanged != nil {
		i.hooks.SpanChanged(s)
	}
}

// Span returns the span covering the last key returned, if any. A span key is
// considered to 'cover' a key if the key falls within the span's user key
// bounds.
func (i *InterleavingIter) Span() Span {
	if !i.spanCoversKey {
		return Span{}
	}
	// Return the span with truncated bounds.
	return Span{
		Start: i.spanStart,
		End:   i.spanEnd,
		Keys:  i.span.Keys,
	}
}

// SetBounds implements (base.InternalIterator).SetBounds.
func (i *InterleavingIter) SetBounds(lower, upper []byte) {
	i.lower, i.upper = lower, upper
	i.pointIter.SetBounds(lower, upper)
}

// Error implements (base.InternalIterator).Error.
func (i *InterleavingIter) Error() error {
	return firstError(i.pointIter.Error(), i.keyspanIter.Error())
}

// Close implements (base.InternalIterator).Close.
func (i *InterleavingIter) Close() error {
	perr := i.pointIter.Close()
	rerr := i.keyspanIter.Close()
	return firstError(perr, rerr)
}

// String implements (base.InternalIterator).String.
func (i *InterleavingIter) String() string {
	return fmt.Sprintf("keyspan-interleaving(%q)", i.pointIter.String())
}

var _ base.InternalIteratorWithStats = &InterleavingIter{}

// Stats implements InternalIteratorWithStats.
func (i *InterleavingIter) Stats() base.InternalIteratorStats {
	return i.pointIter.Stats()
}

// ResetStats implements InternalIteratorWithStats.
func (i *InterleavingIter) ResetStats() {
	i.pointIter.ResetStats()
}

func firstError(err0, err1 error) error {
	if err0 != nil {
		return err0
	}
	return err1
}
