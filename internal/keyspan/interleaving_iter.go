// Copyright 2021 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package keyspan

import (
	"context"
	"fmt"

	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/pebble/internal/base"
	"github.com/cockroachdb/pebble/internal/invariants"
	"github.com/cockroachdb/pebble/internal/treesteps"
)

// A SpanMask may be used to configure an interleaving iterator to skip point
// keys that fall within the bounds of some spans.
type SpanMask interface {
	// SpanChanged is invoked by an interleaving iterator whenever the current
	// span changes. As the iterator passes into or out of a Span, it invokes
	// SpanChanged, passing the new Span. When the iterator passes out of a
	// span's boundaries and is no longer covered by any span, SpanChanged is
	// invoked with a nil span.
	//
	// SpanChanged is invoked before SkipPoint, and callers may use SpanChanged
	// to recalculate state used by SkipPoint for masking.
	//
	// SpanChanged may be invoked consecutively with identical spans under some
	// circumstances, such as repeatedly absolutely positioning an iterator to
	// positions covered by the same span, or while changing directions.
	SpanChanged(*Span)
	// SkipPoint is invoked by the interleaving iterator whenever the iterator
	// encounters a point key covered by a Span. If SkipPoint returns true, the
	// interleaving iterator skips the point key and all larger keys with the
	// same prefix. This is used during range key iteration to skip over point
	// keys 'masked' by range keys.
	SkipPoint(userKey []byte) bool
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
//	First()     = `c#72057594037927935,21`        Span() = [c,e)
//	SeekGE('d') = `d#72057594037927935,21`        Span() = [c,e)
//
// InterleavedIter does not interleave synthetic markers for spans that do not
// contain any keys.
//
// When InterleavingIterOpts.InterleaveEndKeys is set, in addition to
// interleaving start keys, the interleaving iterator will interleave end
// boundary keys (also at the maximumal sequence number). At these end boundary
// positions, Span() will return the span to which the end boundary belongs.
//
// # SpanMask
//
// InterelavingIter takes a SpanMask parameter that may be used to configure the
// behavior of the iterator. See the documentation on the SpanMask type.
//
// All spans containing keys are exposed during iteration.
type InterleavingIter struct {
	cmp         base.Compare
	comparer    *base.Comparer
	pointIter   base.InternalIterator
	keyspanIter FragmentIterator
	opts        InterleavingIterOpts
	// keyBuf is used to copy SeekGE or SeekPrefixGE arguments when they're used
	// to truncate a span. The byte slices backing a SeekGE/SeekPrefixGE search
	// keys can come directly from the end user, so they're copied into keyBuf
	// to ensure key stability.
	keyBuf []byte
	// nextPrefixBuf is used during SeekPrefixGE calls to store the truncated
	// upper bound of the returned spans. SeekPrefixGE truncates the returned
	// spans to an upper bound of the seeked prefix's immediate successor.
	nextPrefixBuf []byte
	pointKV       *base.InternalKV
	// err holds an iterator error from either pointIter or keyspanIter. It's
	// reset to nil on seeks. An overview of error-handling mechanics:
	//
	// Whenever either pointIter or keyspanIter is respositioned and a nil
	// key/span is returned, the code performing the positioning is responsible
	// for checking the iterator's Error() value. This happens in savePoint and
	// saveSpan[Forward,Backward].
	//
	// Once i.err is non-nil, the computation of i.pos must set i.pos =
	// posExhausted. This happens in compute[Smallest|Largest]Pos and
	// [next|prev]Pos. Setting i.pos to posExhausted ensures we'll yield nil to
	// the caller, which they'll interpret as a signal they must check Error().
	//
	// INVARIANTS:
	// i.err != nil => i.pos = posExhausted
	err error
	// prefix records the iterator's current prefix if the iterator is in prefix
	// mode. During prefix mode, Pebble will truncate spans to the next prefix.
	// If the iterator subsequently leaves prefix mode, the existing span cached
	// in i.span must be invalidated because its bounds do not reflect the
	// original span's true bounds.
	prefix []byte
	// span holds the span at the keyspanIter's current position. If the span is
	// wholly contained within the iterator bounds, this span is directly
	// returned to the iterator consumer through Span(). If either bound needed
	// to be truncated to the iterator bounds, then truncated is set to true and
	// Span() must return a pointer to truncatedSpan.
	span *Span
	// spanMarker holds the synthetic key that is returned when the iterator
	// passes over a key span's start bound.
	spanMarker base.InternalKV
	// truncated indicates whether or not the span at the current position
	// needed to be truncated. If it did, truncatedSpan holds the truncated
	// span that should be returned.
	truncatedSpan Span
	truncated     bool

	// Keeping all of the bools/uint8s together reduces the sizeof the struct.

	// pos encodes the current position of the iterator: exhausted, on the point
	// key, on a keyspan start, or on a keyspan end.
	pos interleavePos
	// withinSpan indicates whether the iterator is currently positioned within
	// the bounds of the current span (i.span). withinSpan must be updated
	// whenever the interleaving iterator's position enters or exits the bounds
	// of a span.
	withinSpan bool
	// spanMarkerTruncated is set by SeekGE/SeekPrefixGE calls that truncate a
	// span's start bound marker to the search key. It's returned to false on
	// the next repositioning of the keyspan iterator.
	spanMarkerTruncated bool
	// maskSpanChangedCalled records whether or not the last call to
	// SpanMask.SpanChanged provided the current span (i.span) or not.
	maskSpanChangedCalled bool
	// dir indicates the direction of iteration: forward (+1) or backward (-1)
	dir int8
}

// interleavePos indicates the iterator's current position. Note that both
// keyspanStart and keyspanEnd positions correspond to their user key boundaries
// with maximal sequence numbers. This means in the forward direction
// posKeyspanStart and posKeyspanEnd are always interleaved before a posPointKey
// with the same user key.
type interleavePos int8

const (
	posUninitialized interleavePos = iota
	posSeekedBeyondLowerBound
	posSeekedBeyondUpperBound
	posExhausted
	posPointKey
	posKeyspanStart
	posKeyspanEnd
)

// Assert that *InterleavingIter implements the InternalIterator interface.
var _ base.InternalIterator = &InterleavingIter{}

// InterleavingIterOpts holds options configuring the behavior of a
// InterleavingIter.
type InterleavingIterOpts struct {
	Mask                   SpanMask
	LowerBound, UpperBound []byte
	// InterleaveEndKeys configures the interleaving iterator to interleave the
	// end keys of spans (in addition to the start keys, which are always
	// interleaved).
	InterleaveEndKeys bool
}

// Init initializes the InterleavingIter to interleave point keys from pointIter
// with key spans from keyspanIter.
//
// The point iterator must already have the bounds provided on opts. Init does
// not propagate the bounds down the iterator stack.
func (i *InterleavingIter) Init(
	comparer *base.Comparer,
	pointIter base.InternalIterator,
	keyspanIter FragmentIterator,
	opts InterleavingIterOpts,
) {
	keyspanIter = MaybeAssert(keyspanIter, comparer.Compare)
	// To debug:
	// keyspanIter = InjectLogging(keyspanIter, base.DefaultLogger)
	*i = InterleavingIter{
		cmp:         comparer.Compare,
		comparer:    comparer,
		pointIter:   pointIter,
		keyspanIter: keyspanIter,
		opts:        opts,
	}
}

// InitSeekGE may be called after Init but before any positioning method.
// InitSeekGE initializes the current position of the point iterator and then
// performs a SeekGE on the keyspan iterator using the provided key. InitSeekGE
// returns whichever point or keyspan key is smaller. After InitSeekGE, the
// iterator is positioned and may be repositioned using relative positioning
// methods.
//
// This method is used specifically for lazily constructing combined iterators.
// It allows for seeding the iterator with the current position of the point
// iterator.
func (i *InterleavingIter) InitSeekGE(
	prefix, key []byte, pointKV *base.InternalKV,
) *base.InternalKV {
	i.dir = +1
	i.prefix = prefix
	i.clearMask()
	i.savePoint(pointKV)

	// NB: This keyspanSeekGE call will truncate the span to the seek key if
	// necessary. This truncation is important for cases where a switch to
	// combined iteration is made during a user-initiated SeekGE.
	i.keyspanSeekGE(key)

	// During a Seek[Prefix]GE, cascading seeks due to range deletions may
	// result in seeking to or beyond the upper bound. Such a seek exhausts the
	// iterator, but we still need to perform the seek to position the child
	// iterators appropriately.  We leave i.pos as posSeekedBeyondUpperBound so
	// that a subsequent Prev will seek the iterator appropriately for the upper
	// bound.
	//
	// NB: It's still necessary to perform the above seeks of the child
	// iterators to ensure that the invariants used by the TrySeekUsingNext
	// optimization are maintained.
	if i.opts.UpperBound != nil && i.cmp(key, i.opts.UpperBound) >= 0 {
		i.pos = posSeekedBeyondUpperBound
		i.withinSpan = false
		return nil
	}
	i.computeSmallestPos()
	return i.yieldPosition(key, i.nextPos)
}

// InitSeekLT may be called after Init but before any positioning method.
// InitSeekLT initializes the current position of the point iterator and then
// performs a SeekLT on the keyspan iterator using the provided key. InitSeekLT
// returns whichever point or keyspan key is larger. After InitSeekLT, the
// iterator is positioned and may be repositioned using relative positioning
// methods.
//
// This method is used specifically for lazily constructing combined iterators.
// It allows for seeding the iterator with the current position of the point
// iterator.
func (i *InterleavingIter) InitSeekLT(key []byte, pointKV *base.InternalKV) *base.InternalKV {
	i.dir = -1
	i.clearMask()
	i.savePoint(pointKV)
	i.keyspanSeekLT(key)

	// During a SeekLT, cascading seeks due to range deletions may result in
	// seeking to or beyond the lower bound. Such a seek exhausts the iterator,
	// but we still need to perform the seek to position the child iterators
	// appropriately. We leave i.pos as posSeekedBeyondLowerBound so that a
	// subsequent Next will seek the iterator appropriately for the lower bound.
	//
	// NB: It's still necessary to perform the above seeks of the child
	// iterators to ensure that the invariants used by the TrySeekUsingNext
	// optimization are maintained.
	if i.opts.LowerBound != nil && i.cmp(i.opts.LowerBound, key) >= 0 {
		i.pos = posSeekedBeyondLowerBound
		i.withinSpan = false
		return nil
	}
	i.computeLargestPos()
	return i.yieldPosition(i.opts.LowerBound, i.prevPos)
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
//
//	i.lower ≤ key
func (i *InterleavingIter) SeekGE(key []byte, flags base.SeekGEFlags) *base.InternalKV {
	i.err = nil
	i.dir = +1
	i.clearMask()
	i.disablePrefixMode()

	i.savePoint(i.pointIter.SeekGE(key, flags))

	// We need to seek the keyspan iterator too. If the keyspan iterator was
	// already positioned at a span, we might be able to avoid the seek if the
	// seek key falls within the existing span's bounds.
	if i.span != nil && i.cmp(key, i.span.End) < 0 && i.cmp(key, i.span.Start) >= 0 {
		// We're seeking within the existing span's bounds. We still might need
		// truncate the span to the iterator's bounds.
		i.saveSpan(i.span, nil)
		i.enforceBoundsForward()
	} else {
		i.keyspanSeekGE(key)
	}

	// During a Seek[Prefix]GE, cascading seeks due to range deletions may
	// result in seeking to or beyond the upper bound. Such a seek exhausts the
	// iterator, but we still need to perform the seek to position the child
	// iterators appropriately.  We leave i.pos as posSeekedBeyondUpperBound so
	// that a subsequent Prev will seek the iterator appropriately for the upper
	// bound.
	//
	// NB: It's still necessary to perform the above seeks of the child
	// iterators to ensure that the invariants used by the TrySeekUsingNext
	// optimization are maintained.
	if i.opts.UpperBound != nil && i.cmp(key, i.opts.UpperBound) >= 0 {
		i.pos = posSeekedBeyondUpperBound
		i.withinSpan = false
		return nil
	}

	i.computeSmallestPos()
	return i.yieldPosition(key, i.nextPos)
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
//
//	i.lower ≤ key
func (i *InterleavingIter) SeekPrefixGE(
	prefix, key []byte, flags base.SeekGEFlags,
) *base.InternalKV {
	i.err = nil
	i.dir = +1
	i.clearMask()
	i.prefix = prefix

	i.savePoint(i.pointIter.SeekPrefixGE(prefix, key, flags))

	// We need to seek the keyspan iterator too. If the keyspan iterator was
	// already positioned at a span, we might be able to avoid the seek if the
	// entire seek prefix key falls within the existing span's bounds.
	//
	// During a SeekPrefixGE, Pebble defragments range keys within the bounds of
	// the prefix. For example, a SeekPrefixGE('c', 'c@8') must defragment the
	// any overlapping range keys within the bounds of [c,c\00).
	//
	// If range keys are fragmented within a prefix (eg, because a version
	// within a prefix was chosen as an sstable boundary), then it's possible
	// the seek key falls into the current i.span, but the current i.span does
	// not wholly cover the seek prefix.
	//
	// For example, a SeekPrefixGE('d@5') may only defragment a range key to
	// the bounds of [c@2,e). A subsequent SeekPrefixGE('c@0') must re-seek the
	// keyspan iterator, because although 'c@0' is contained within [c@2,e), the
	// full span of the prefix is not.
	//
	// Similarly, a SeekPrefixGE('a@3') may only defragment a range key to the
	// bounds [a,c@8). A subsequent SeekPrefixGE('c@10') must re-seek the
	// keyspan iterator, because although 'c@10' is contained within [a,c@8),
	// the full span of the prefix is not.
	seekKeyspanIter := true
	if i.span != nil && i.cmp(prefix, i.span.Start) >= 0 {
		if ei := i.comparer.Split(i.span.End); i.cmp(prefix, i.span.End[:ei]) < 0 {
			// We're seeking within the existing span's bounds. We still might need
			// truncate the span to the iterator's bounds.
			i.saveSpan(i.span, nil)
			i.enforceBoundsForward()
			seekKeyspanIter = false
		}
	}
	if seekKeyspanIter {
		i.keyspanSeekGE(key)
	}

	// During a Seek[Prefix]GE, cascading seeks due to range deletions may
	// result in seeking to or beyond the upper bound. Such a seek exhausts the
	// iterator, but we still need to perform the seek to position the child
	// iterators appropriately.  We leave i.pos as posSeekedBeyondUpperBound so
	// that a subsequent Prev will seek the iterator appropriately for the upper
	// bound.
	//
	// NB: It's still necessary to perform the above seeks of the child
	// iterators to ensure that the invariants used by the TrySeekUsingNext
	// optimization are maintained.
	if i.opts.UpperBound != nil && i.cmp(key, i.opts.UpperBound) >= 0 {
		i.pos = posSeekedBeyondUpperBound
		i.withinSpan = false
		return nil
	}
	i.computeSmallestPos()
	return i.yieldPosition(key, i.nextPos)
}

// SeekLT implements (base.InternalIterator).SeekLT.
func (i *InterleavingIter) SeekLT(key []byte, flags base.SeekLTFlags) *base.InternalKV {
	i.err = nil
	i.dir = -1
	i.clearMask()
	i.disablePrefixMode()

	i.savePoint(i.pointIter.SeekLT(key, flags))

	// We need to seek the keyspan iterator too. If the keyspan iterator was
	// already positioned at a span, we might be able to avoid the seek if the
	// seek key falls within the existing span's bounds.
	if i.span != nil && i.cmp(key, i.span.Start) > 0 && i.cmp(key, i.span.End) < 0 {
		// We're seeking within the existing span's bounds. We still might need
		// truncate the span to the iterator's bounds.
		i.saveSpan(i.span, nil)
		i.enforceBoundsBackward()
	} else {
		i.keyspanSeekLT(key)
	}

	// During a SeekLT, cascading seeks due to range deletions may result in
	// seeking to or beyond the lower bound. Such a seek exhausts the iterator,
	// but we still need to perform the seek to position the child iterators
	// appropriately. We leave i.pos as posSeekedBeyondLowerBound so that a
	// subsequent Next will seek the iterator appropriately for the lower bound.
	//
	// NB: It's still necessary to perform the above seeks of the child
	// iterators to ensure that the invariants used by the TrySeekUsingNext
	// optimization are maintained.
	if i.opts.LowerBound != nil && i.cmp(i.opts.LowerBound, key) >= 0 {
		i.pos = posSeekedBeyondLowerBound
		i.withinSpan = false
		return nil
	}
	i.computeLargestPos()
	return i.yieldPosition(i.opts.LowerBound, i.prevPos)
}

// First implements (base.InternalIterator).First.
func (i *InterleavingIter) First() *base.InternalKV {
	i.err = nil
	i.clearMask()
	i.disablePrefixMode()
	i.savePoint(i.pointIter.First())
	i.saveSpan(i.keyspanIter.First())
	i.enforceBoundsForward()
	i.dir = +1
	i.computeSmallestPos()
	return i.yieldPosition(i.opts.LowerBound, i.nextPos)
}

// Last implements (base.InternalIterator).Last.
func (i *InterleavingIter) Last() *base.InternalKV {
	i.err = nil
	i.clearMask()
	i.disablePrefixMode()
	i.savePoint(i.pointIter.Last())
	i.saveSpan(i.keyspanIter.Last())
	i.enforceBoundsBackward()
	i.dir = -1
	i.computeLargestPos()
	return i.yieldPosition(i.opts.LowerBound, i.prevPos)
}

// Next implements (base.InternalIterator).Next.
func (i *InterleavingIter) Next() *base.InternalKV {
	if i.dir == -1 {
		if i.pos == posSeekedBeyondLowerBound {
			return i.SeekGE(i.opts.LowerBound, base.SeekGEFlagsNone)
		}

		// Switching directions.
		i.dir = +1
		if i.opts.Mask != nil {
			// Clear the mask while we reposition the point iterator. While
			// switching directions, we may move the point iterator outside of
			// i.span's bounds.
			i.clearMask()
		}

		// When switching directions, iterator state corresponding to the
		// current iterator position (as indicated by i.pos) is already correct.
		// However any state that has yet to be interleaved describes a position
		// behind the current iterator position and needs to be updated to
		// describe the position ahead of the current iterator position.
		switch i.pos {
		case posExhausted:
			// Nothing to do. The below nextPos call will move both the point
			// key and span to their next positions and return
			// MIN(point,s.Start).
		case posPointKey:
			// If we're currently on a point key, the below nextPos will
			// correctly Next the point key iterator to the next point key.
			// Do we need to move the span forwards? If the current span lies
			// entirely behind the current key (!i.withinSpan), then we
			// need to move it to the first span in the forward direction.
			if !i.withinSpan {
				i.saveSpan(i.keyspanIter.Next())
				i.enforceBoundsForward()
			}
		case posKeyspanStart:
			i.withinSpan = true
			// Since we're positioned on a Span, the pointIter is positioned
			// entirely behind the current iterator position. Reposition it
			// ahead of the current iterator position.
			i.switchPointIteratorIntoForward()
		case posKeyspanEnd:
			// Since we're positioned on a Span, the pointIter is positioned
			// entirely behind of the current iterator position. Reposition it
			// ahead the current iterator position.
			i.switchPointIteratorIntoForward()
		}
		// Fallthrough to calling i.nextPos.
	}
	i.nextPos()
	return i.yieldPosition(i.opts.LowerBound, i.nextPos)
}

// NextPrefix implements (base.InternalIterator).NextPrefix.
//
// Calling NextPrefix while positioned at a span boundary is prohibited.
func (i *InterleavingIter) NextPrefix(succKey []byte) *base.InternalKV {
	if i.dir == -1 {
		panic(errors.AssertionFailedf("pebble: cannot switch directions with NextPrefix"))
	}

	switch i.pos {
	case posExhausted:
		return nil
	case posPointKey:
		i.savePoint(i.pointIter.NextPrefix(succKey))
		if i.err != nil {
			i.pos = posExhausted
		} else if i.withinSpan {
			if i.pointKV == nil || i.cmp(i.span.End, i.pointKV.K.UserKey) <= 0 {
				i.pos = posKeyspanEnd
			} else {
				i.pos = posPointKey
			}
		} else {
			i.computeSmallestPos()
		}
	case posKeyspanStart, posKeyspanEnd:
		panic(errors.AssertionFailedf("pebble: NextPrefix called while positioned on a span boundary"))
	}
	return i.yieldPosition(i.opts.LowerBound, i.nextPos)
}

// Prev implements (base.InternalIterator).Prev.
func (i *InterleavingIter) Prev() *base.InternalKV {
	if i.dir == +1 {
		if i.pos == posSeekedBeyondUpperBound {
			return i.SeekLT(i.opts.UpperBound, base.SeekLTFlagsNone)
		}

		// Switching directions.
		i.dir = -1

		if i.opts.Mask != nil {
			// Clear the mask while we reposition the point iterator. While
			// switching directions, we may move the point iterator outside of
			// i.span's bounds.
			i.clearMask()
		}

		// When switching directions, iterator state corresponding to the
		// current iterator position (as indicated by i.pos) is already correct.
		// However any state that has yet to be interleaved describes a position
		// ahead of the current iterator position and needs to be updated to
		// describe the position behind the current iterator position.
		switch i.pos {
		case posExhausted:
			// Nothing to do. The below prevPos call will move both the point
			// key and span to previous positions and return MAX(point, s.End).
		case posPointKey:
			// If we're currently on a point key, the point iterator is in the
			// right place and the call to prevPos will correctly Prev the point
			// key iterator to the previous point key. Do we need to move the
			// span backwards? If the current span lies entirely ahead of the
			// current key (!i.withinSpan), then we need to move it to the first
			// span in the reverse direction.
			if !i.withinSpan {
				i.saveSpan(i.keyspanIter.Prev())
				i.enforceBoundsBackward()
			}
		case posKeyspanStart:
			// Since we're positioned on a Span, the pointIter is positioned
			// entirely ahead of the current iterator position. Reposition it
			// behind the current iterator position.
			i.switchPointIteratorIntoReverse()
			// Without considering truncation of spans to seek keys, the keyspan
			// iterator is already in the right place. But consider span [a, z)
			// and this sequence of iterator calls:
			//
			//   SeekGE('c') = c.RANGEKEYSET#72057594037927935
			//   Prev()      = a.RANGEKEYSET#72057594037927935
			//
			// If the current span's start key was last surfaced truncated due
			// to a SeekGE or SeekPrefixGE call, then it's still relevant in the
			// reverse direction with an untruncated start key.
			if i.spanMarkerTruncated {
				// When we fallthrough to calling prevPos, we want to move to
				// MAX(point, span.Start). We cheat here by claiming we're
				// currently on the end boundary, so that we'll move on to the
				// untruncated start key if necessary.
				i.pos = posKeyspanEnd
			}
		case posKeyspanEnd:
			// Since we're positioned on a Span, the pointIter is positioned
			// entirely ahead of the current iterator position. Reposition it
			// behind the current iterator position.
			i.switchPointIteratorIntoReverse()
		}

		i.spanMarkerTruncated = false
		// Fallthrough to calling i.prevPos.
	}
	i.prevPos()
	return i.yieldPosition(i.opts.LowerBound, i.prevPos)
}

// computeSmallestPos sets i.{pos,withinSpan} to:
//
//	MIN(i.pointKey, i.span.Start)
func (i *InterleavingIter) computeSmallestPos() {
	if i.err == nil {
		if i.span != nil && (i.pointKV == nil || i.cmp(i.startKey(), i.pointKV.K.UserKey) <= 0) {
			i.withinSpan = true
			i.pos = posKeyspanStart
			return
		}
		i.withinSpan = false
		if i.pointKV != nil {
			i.pos = posPointKey
			return
		}
	}
	i.pos = posExhausted
}

// computeLargestPos sets i.{pos,withinSpan} to:
//
//	MAX(i.pointKey, i.span.End)
func (i *InterleavingIter) computeLargestPos() {
	if i.err == nil {
		if i.span != nil && (i.pointKV == nil || i.cmp(i.span.End, i.pointKV.K.UserKey) > 0) {
			i.withinSpan = true
			i.pos = posKeyspanEnd
			return
		}
		i.withinSpan = false
		if i.pointKV != nil {
			i.pos = posPointKey
			return
		}
	}
	i.pos = posExhausted
}

// nextPos advances the iterator one position in the forward direction.
func (i *InterleavingIter) nextPos() {
	if invariants.Enabled {
		defer func() {
			if i.err != nil && i.pos != posExhausted {
				panic(errors.AssertionFailedf("iterator has accumulated error but i.pos = %d", i.pos))
			}
		}()
	}
	// NB: If i.err != nil or any of the positioning methods performed in this
	// function result in i.err != nil, we must set i.pos = posExhausted. We
	// perform this check explicitly here, but if any of the branches below
	// advance either iterator, they must also check i.err and set posExhausted
	// if necessary.
	if i.err != nil {
		i.pos = posExhausted
		return
	}

	switch i.pos {
	case posExhausted:
		i.switchPointIteratorIntoForward()
		i.saveSpan(i.keyspanIter.Next())
		i.enforceBoundsForward()
		i.computeSmallestPos()
	case posPointKey:
		i.savePoint(i.pointIter.Next())
		if i.err != nil {
			i.pos = posExhausted
			return
		}
		// If we're not currently within the span, we want to chose the
		// MIN(pointKey,span.Start), which is exactly the calculation performed
		// by computeSmallestPos.
		if !i.withinSpan {
			i.computeSmallestPos()
			return
		}
		// i.withinSpan=true
		// Since we previously were within the span, we want to choose the
		// MIN(pointKey,span.End).
		switch {
		case i.span == nil:
			panic(errors.AssertionFailedf("pebble: i.withinSpan=true and i.span=nil"))
		case i.pointKV == nil:
			// Since i.withinSpan=true, we step onto the end boundary of the
			// keyspan.
			i.pos = posKeyspanEnd
		default:
			// i.withinSpan && i.pointKV != nil && i.span != nil
			if i.cmp(i.span.End, i.pointKV.K.UserKey) <= 0 {
				i.pos = posKeyspanEnd
			} else {
				i.pos = posPointKey
			}
		}
	case posKeyspanStart:
		// Either a point key or the span's end key comes next.
		if i.pointKV != nil && i.cmp(i.pointKV.K.UserKey, i.span.End) < 0 {
			i.pos = posPointKey
		} else {
			i.pos = posKeyspanEnd
		}
	case posKeyspanEnd:
		i.saveSpan(i.keyspanIter.Next())
		i.enforceBoundsForward()
		i.computeSmallestPos()
	default:
		panic(errors.AssertionFailedf("pebble: unexpected pos=%d", i.pos))
	}
}

// prevPos advances the iterator one position in the reverse direction.
func (i *InterleavingIter) prevPos() {
	if invariants.Enabled {
		defer func() {
			if i.err != nil && i.pos != posExhausted {
				panic(errors.AssertionFailedf("iterator has accumulated error but i.pos = %d", i.pos))
			}
		}()
	}
	// NB: If i.err != nil or any of the positioning methods performed in this
	// function result in i.err != nil, we must set i.pos = posExhausted. We
	// perform this check explicitly here, but if any of the branches below
	// advance either iterator, they must also check i.err and set posExhausted
	// if necessary.
	if i.err != nil {
		i.pos = posExhausted
		return
	}

	switch i.pos {
	case posExhausted:
		i.switchPointIteratorIntoReverse()
		i.saveSpan(i.keyspanIter.Prev())
		i.enforceBoundsBackward()
		i.computeLargestPos()
	case posPointKey:
		i.savePoint(i.pointIter.Prev())
		if i.err != nil {
			i.pos = posExhausted
			return
		}
		// If we're not currently covered by the span, we want to chose the
		// MAX(pointKey,span.End), which is exactly the calculation performed
		// by computeLargestPos.
		if !i.withinSpan {
			i.computeLargestPos()
			return
		}
		switch {
		case i.span == nil:
			panic(errors.AssertionFailedf("withinSpan=true, but i.span == nil"))
		case i.pointKV == nil:
			i.pos = posKeyspanStart
		default:
			// i.withinSpan && i.pointKey != nil && i.span != nil
			if i.cmp(i.span.Start, i.pointKV.K.UserKey) > 0 {
				i.pos = posKeyspanStart
			} else {
				i.pos = posPointKey
			}
		}
	case posKeyspanStart:
		i.saveSpan(i.keyspanIter.Prev())
		i.enforceBoundsBackward()
		i.computeLargestPos()
	case posKeyspanEnd:
		// Either a point key or the span's start key is previous.
		if i.pointKV != nil && i.cmp(i.pointKV.K.UserKey, i.span.Start) >= 0 {
			i.pos = posPointKey
		} else {
			i.pos = posKeyspanStart
		}
	default:
		panic(errors.AssertionFailedf("pebble: unexpected pos=%d", i.pos))
	}
}

func (i *InterleavingIter) yieldPosition(lowerBound []byte, advance func()) *base.InternalKV {
	// This loop returns the first visible position in the current iteration
	// direction. Some positions are not visible and skipped. For example, if
	// masking is enabled and the iterator is positioned over a masked point
	// key, this loop skips the position. If a span's start key should be
	// interleaved next, but the span is empty, the loop continues to the next
	// key. Currently, span end keys are also always skipped, and are used only
	// for maintaining internal state.
	for {
		switch i.pos {
		case posExhausted:
			return i.yieldNil()
		case posPointKey:
			if i.pointKV == nil {
				panic(errors.AssertionFailedf("pebble: i.pointKV is nil"))
			}

			if i.opts.Mask != nil {
				i.maybeUpdateMask()
				if i.withinSpan && i.opts.Mask.SkipPoint(i.pointKV.K.UserKey) {
					// The span covers the point key. If a SkipPoint hook is
					// configured, ask it if we should skip this point key.
					if i.prefix != nil {
						// During prefix-iteration node, once a point is masked,
						// all subsequent keys with the same prefix must also be
						// masked according to the key ordering. We can stop and
						// return nil.
						//
						// NB: The above is not just an optimization. During
						// prefix-iteration mode, the internal iterator contract
						// prohibits us from Next-ing beyond the first key
						// beyond the iteration prefix. If we didn't already
						// stop early, we would need to check if this masked
						// point is already beyond the prefix.
						return i.yieldNil()
					}
					// TODO(jackson): If we thread a base.Comparer through to
					// InterleavingIter so that we have access to
					// ImmediateSuccessor, we could use NextPrefix. We'd need to
					// tweak the SpanMask interface slightly.

					// Advance beyond the masked point key.
					advance()
					continue
				}
			}
			return i.yieldPointKey()
		case posKeyspanEnd:
			if !i.opts.InterleaveEndKeys {
				// Don't interleave end keys; just advance.
				advance()
				continue
			}
			return i.yieldSyntheticSpanEndMarker()
		case posKeyspanStart:
			// Don't interleave an empty span.
			if i.span.Empty() {
				advance()
				continue
			}
			return i.yieldSyntheticSpanStartMarker(lowerBound)
		default:
			panic(errors.AssertionFailedf("pebble: unexpected interleavePos=%d", i.pos))
		}
	}
}

// keyspanSeekGE seeks the keyspan iterator to the first span covering a key ≥ k.
func (i *InterleavingIter) keyspanSeekGE(k []byte) {
	i.saveSpan(i.keyspanIter.SeekGE(k))
	i.enforceBoundsForward()
}

// keyspanSeekLT seeks the keyspan iterator to the last span covering a key < k.
func (i *InterleavingIter) keyspanSeekLT(k []byte) {
	i.saveSpan(i.keyspanIter.SeekLT(k))
	i.enforceBoundsBackward()
}

// switchPointIteratorIntoReverse switches the direction of the point iterator
// into reverse, stepping to the previous point key. If the point iterator is
// exhausted in the forward direction and there's an upper bound present, it's
// re-seeked to ensure the iterator obeys the upper bound.
func (i *InterleavingIter) switchPointIteratorIntoReverse() {
	if i.pointKV == nil && i.opts.UpperBound != nil {
		i.savePoint(i.pointIter.SeekLT(i.opts.UpperBound, base.SeekLTFlagsNone))
		return
	}
	i.savePoint(i.pointIter.Prev())
}

// switchPointIteratorIntoForward switches the direction of the point iterator
// into the forward direction, stepping to the next point key. If the point
// iterator is exhausted in the reverse direction and there's a lower bound
// present, it's re-seeked to ensure the iterator obeys the lower bound.
func (i *InterleavingIter) switchPointIteratorIntoForward() {
	if i.pointKV == nil && i.opts.LowerBound != nil {
		i.savePoint(i.pointIter.SeekGE(i.opts.LowerBound, base.SeekGEFlagsNone))
		return
	}
	i.savePoint(i.pointIter.Next())
}

func (i *InterleavingIter) saveSpan(span *Span, err error) {
	i.span = span
	i.err = firstError(i.err, err)
	i.truncated = false
	i.truncatedSpan = Span{}
}

func (i *InterleavingIter) enforceBoundsForward() {
	i.spanMarkerTruncated = false
	i.maskSpanChangedCalled = false
	if i.span == nil {
		return
	}
	// Check the upper bound if we have one.
	if i.opts.UpperBound != nil && i.cmp(i.span.Start, i.opts.UpperBound) >= 0 {
		i.span = nil
		i.withinSpan = false
		return
	}
	i.maybeTruncateSpan()
}

func (i *InterleavingIter) enforceBoundsBackward() {
	i.spanMarkerTruncated = false
	i.maskSpanChangedCalled = false
	if i.span == nil {
		return
	}
	// Check the lower bound if we have one.
	if i.opts.LowerBound != nil && i.cmp(i.span.End, i.opts.LowerBound) <= 0 {
		i.span = nil
		i.withinSpan = false
		return
	}
	i.maybeTruncateSpan()
}

func (i *InterleavingIter) maybeTruncateSpan() {
	// NB: These truncations don't require setting `keyspanMarkerTruncated`:
	// That flag only applies to truncated span marker keys.
	if i.opts.LowerBound != nil && i.cmp(i.span.Start, i.opts.LowerBound) < 0 {
		i.truncated = true
		i.truncatedSpan = *i.span
		i.truncatedSpan.Start = i.opts.LowerBound
	}
	if i.opts.UpperBound != nil && i.cmp(i.opts.UpperBound, i.span.End) < 0 {
		if !i.truncated {
			i.truncated = true
			i.truncatedSpan = *i.span
		}
		i.truncatedSpan.End = i.opts.UpperBound
	}
	// If the iterator is in prefix iteration mode, we may also need to truncate
	// to the prefix's bounds.
	if i.prefix != nil {
		if !i.truncated {
			i.truncated = true
			i.truncatedSpan = *i.span
		}
		if i.cmp(i.prefix, i.truncatedSpan.Start) > 0 {
			i.truncatedSpan.Start = i.prefix
		}
		i.nextPrefixBuf = i.comparer.ImmediateSuccessor(i.nextPrefixBuf[:0], i.prefix)
		if i.truncated && i.cmp(i.nextPrefixBuf, i.truncatedSpan.End) < 0 {
			i.truncatedSpan.End = i.nextPrefixBuf
		}
	}
}

func (i *InterleavingIter) yieldNil() *base.InternalKV {
	i.withinSpan = false
	i.clearMask()
	return i.verify(nil)
}

func (i *InterleavingIter) yieldPointKey() *base.InternalKV {
	return i.verify(i.pointKV)
}

func (i *InterleavingIter) yieldSyntheticSpanStartMarker(lowerBound []byte) *base.InternalKV {
	i.spanMarker.K.UserKey = i.startKey()
	i.spanMarker.K.Trailer = base.MakeTrailer(base.SeqNumMax, i.span.Keys[0].Kind())

	// Truncate the key we return to our lower bound if we have one. Note that
	// we use the lowerBound function parameter, not i.lower. The lowerBound
	// argument is guaranteed to be ≥ i.lower. It may be equal to the SetBounds
	// lower bound, or it could come from a SeekGE or SeekPrefixGE search key.
	if lowerBound != nil && i.cmp(lowerBound, i.startKey()) > 0 {
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
		i.spanMarker.K.UserKey = i.keyBuf
		i.spanMarkerTruncated = true
	}
	i.maybeUpdateMask()
	return i.verify(&i.spanMarker)
}

func (i *InterleavingIter) yieldSyntheticSpanEndMarker() *base.InternalKV {
	i.spanMarker.K.UserKey = i.endKey()
	i.spanMarker.K.Trailer = base.MakeTrailer(base.SeqNumMax, i.span.Keys[0].Kind())
	return i.verify(&i.spanMarker)
}

func (i *InterleavingIter) disablePrefixMode() {
	if i.prefix != nil {
		i.prefix = nil
		// Clear the existing span. It may not hold the true end bound of the
		// underlying span.
		i.span = nil
		i.withinSpan = false
	}
}

func (i *InterleavingIter) verify(kv *base.InternalKV) *base.InternalKV {
	// Wrap the entire function body in the invariants build tag, so that
	// production builds elide this entire function.
	if invariants.Enabled {
		switch {
		case i.dir == -1 && i.spanMarkerTruncated:
			panic(errors.AssertionFailedf("pebble: invariant violation: truncated span key in reverse iteration"))
		case kv != nil && i.opts.LowerBound != nil && !kv.K.IsExclusiveSentinel() &&
			i.cmp(kv.K.UserKey, i.opts.LowerBound) < 0:
			panic(errors.AssertionFailedf("pebble: invariant violation: key (%q) < lower bound (%q)",
				kv.K.UserKey, i.opts.LowerBound))
		case kv != nil && i.opts.UpperBound != nil && !kv.K.IsExclusiveSentinel() &&
			!base.UserKeyExclusive(i.opts.UpperBound).IsUpperBoundForInternalKey(i.comparer.Compare, kv.K):
			panic(errors.AssertionFailedf("pebble: invariant violation: key (%q) ≥ upper bound (%q)",
				kv.K.UserKey, i.opts.UpperBound))
		case i.err != nil && kv != nil:
			panic(errors.WithSecondaryError(errors.AssertionFailedf(
				"pebble: invariant violation: accumulated error swallowed: %s"), i.err))
		case i.err == nil && i.pointIter.Error() != nil:
			panic(errors.WithSecondaryError(
				errors.AssertionFailedf("pebble: invariant violation: pointIter error swallowed"),
				i.pointIter.Error()))
		}
	}
	return kv
}

// updateMask updates the current mask, if a mask is configured and the mask
// hasn't been updated with the current keyspan yet.
func (i *InterleavingIter) maybeUpdateMask() {
	switch {
	case i.opts.Mask == nil, i.maskSpanChangedCalled:
		return
	case !i.withinSpan || i.span.Empty():
		i.clearMask()
	case i.truncated:
		i.opts.Mask.SpanChanged(&i.truncatedSpan)
		i.maskSpanChangedCalled = true
	default:
		i.opts.Mask.SpanChanged(i.span)
		i.maskSpanChangedCalled = true
	}
}

// clearMask clears the current mask, if a mask is configured and no mask should
// be active.
func (i *InterleavingIter) clearMask() {
	if i.opts.Mask != nil {
		i.maskSpanChangedCalled = false
		i.opts.Mask.SpanChanged(nil)
	}
}

func (i *InterleavingIter) startKey() []byte {
	if i.truncated {
		return i.truncatedSpan.Start
	}
	return i.span.Start
}

func (i *InterleavingIter) endKey() []byte {
	if i.truncated {
		return i.truncatedSpan.End
	}
	return i.span.End
}

func (i *InterleavingIter) savePoint(kv *base.InternalKV) {
	i.pointKV = kv
	if kv == nil {
		i.err = firstError(i.err, i.pointIter.Error())
	}
	if invariants.Enabled {
		if err := i.pointIter.Error(); kv != nil && err != nil {
			panic(errors.WithSecondaryError(
				base.AssertionFailedf("pebble: %T point iterator returned non-nil key %q while iter has error", i.pointIter, kv),
				err))
		}
	}
}

// Span returns the span covering the last key returned, if any. A span key is
// considered to 'cover' a key if the key falls within the span's user key
// bounds. The returned span is owned by the InterleavingIter. The caller is
// responsible for copying if stability is required.
//
// Span will never return an invalid or empty span.
func (i *InterleavingIter) Span() *Span {
	if invariants.Enabled && i.pointIter == nil {
		panic(errors.AssertionFailedf("Span() called after close"))
	}
	if !i.withinSpan || len(i.span.Keys) == 0 {
		return nil
	}
	if i.truncated {
		return &i.truncatedSpan
	}
	return i.span
}

// SetBounds implements (base.InternalIterator).SetBounds.
func (i *InterleavingIter) SetBounds(lower, upper []byte) {
	i.opts.LowerBound, i.opts.UpperBound = lower, upper
	i.pointIter.SetBounds(lower, upper)
	i.Invalidate()
}

// SetContext implements (base.InternalIterator).SetContext.
func (i *InterleavingIter) SetContext(ctx context.Context) {
	i.pointIter.SetContext(ctx)
	i.keyspanIter.SetContext(ctx)
}

// TreeStepsNode is part of the InternalIterator interface.
func (i *InterleavingIter) TreeStepsNode() treesteps.NodeInfo {
	info := treesteps.NodeInfof(i, "%T(%p)", i, i)
	info.AddChildren(i.pointIter, i.keyspanIter)
	return info
}

// Invalidate invalidates the interleaving iterator's current position, clearing
// its state. This prevents optimizations such as reusing the current span on
// seek.
func (i *InterleavingIter) Invalidate() {
	i.span = nil
	i.withinSpan = false
	i.pointKV = nil
	i.pos = posUninitialized
}

// Error implements (base.InternalIterator).Error.
func (i *InterleavingIter) Error() error {
	return i.err
}

// Close implements (base.InternalIterator).Close.
func (i *InterleavingIter) Close() error {
	err := i.pointIter.Close()
	i.pointIter = nil
	i.keyspanIter.Close()
	i.keyspanIter = nil
	return err
}

// String implements (base.InternalIterator).String.
func (i *InterleavingIter) String() string {
	return fmt.Sprintf("keyspan-interleaving(%q)", i.pointIter.String())
}

func firstError(err0, err1 error) error {
	if err0 != nil {
		return err0
	}
	return err1
}
