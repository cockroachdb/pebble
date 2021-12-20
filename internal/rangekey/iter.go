// Copyright 2021 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package rangekey

import (
	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/pebble/internal/base"
	"github.com/cockroachdb/pebble/internal/keyspan"
)

// This Iter implementation iterates over 'coalesced spans' that are not easily
// representable within the InternalIterator interface. Instead of iterating
// over internal keys, this Iter exposes CoalescedSpans that represent a set of
// overlapping fragments coalesced into a single internally consistent span.

// Iter is an iterator over a set of fragmented, coalesced spans. It wraps a
// keyspan.Iter containing fragmented keyspan.Spans with key kinds RANGEKEYSET,
// RANGEKEYUNSET and RANGEKEYDEL. The spans within the keyspan.Iter must be
// sorted by Start key, including by decreasing sequence number if user keys are
// equal and key kind if sequence numbers are equal.
//
// Iter handles 'coalescing' spans on-the-fly, including dropping key spans that
// are no longer relevant.
type Iter struct {
	iter      *keyspan.Iter
	coalescer Coalescer
	curr      CoalescedSpan
	err       error
	valid     bool
	dir       int8
}

// Init initializes an iterator over a set of fragmented, coalesced spans.
func (i *Iter) Init(cmp base.Compare, formatKey base.FormatKey, visibleSeqNum uint64, iter *keyspan.Iter) {
	*i = Iter{
		iter: iter,
	}
	i.coalescer.Init(cmp, formatKey, visibleSeqNum, func(span CoalescedSpan) {
		i.curr = span
	})
}

// Error returns any accumulated error.
func (i *Iter) Error() error {
	return i.err
}

func (i *Iter) coalesceForward(k *base.InternalKey) *CoalescedSpan {
	i.dir = +1
	i.valid = false
	// As long as we have a key and that key matches the coalescer's current
	// start key, it must be coalesced.
	// TODO(jackson): This key comparison is redundant with the one performed by
	// Coalescer.Add. Tweaking the interfaces should be able to remove the
	// comparison.
	for k != nil && (!i.valid || i.coalescer.items.cmp(k.UserKey, i.coalescer.Start()) == 0) {
		if err := i.coalescer.Add(*i.iter.Current()); err != nil {
			i.valid, i.err = false, err
			return nil
		}
		i.valid = true
		k, _ = i.iter.Next()
	}
	// NB: Finish populates i.curr with the coalesced span.
	i.coalescer.Finish()
	if !i.valid {
		return nil
	}
	return &i.curr
}

func (i *Iter) coalesceBackward(k *base.InternalKey) *CoalescedSpan {
	i.dir = -1
	i.valid = false
	// As long as we have a key and that key matches the coalescer's current
	// start key, it must be coalesced.
	// TODO(jackson): This key comparison is redundant with the one performed by
	// Coalescer.AddReverse. Tweaking the interfaces should be able to remove
	// the comparison.
	for k != nil && (!i.valid || i.coalescer.items.cmp(k.UserKey, i.coalescer.Start()) == 0) {
		if err := i.coalescer.AddReverse(*i.iter.Current()); err != nil {
			i.valid, i.err = false, err
			return nil
		}
		i.valid = true
		k, _ = i.iter.Prev()
	}
	// NB: Finish populates i.curr with the coalesced span.
	i.coalescer.Finish()
	if !i.valid {
		return nil
	}
	return &i.curr
}

// SeekGE seeks the iterator to the first span covering a key greater than or
// equal to key and returns it.
func (i *Iter) SeekGE(key []byte) *CoalescedSpan {
	k, _ := i.iter.SeekLT(key)
	if s := i.iter.Current(); s != nil && i.coalescer.items.cmp(key, s.End) < 0 {
		// We landed on a range key that begins before `key`, but extends beyond
		// it. Since we performed a SeekLT, we're on the last fragment with
		// those range key bounds and we need to coalesce backwards.
		return i.coalesceBackward(k)
	}
	// It's still possible that the next key is a range key with a start key
	// exactly equal to key. Move forward one. There's no point in checking
	// whether the next fragment actually covers the search key, because if it
	// doesn't it's still the first fragment covering a key ≥ the search key.
	k, _ = i.iter.Next()
	return i.coalesceForward(k)
}

// SeekLT seeks the iterator to the first span covering a key less than key and
// returns it.
func (i *Iter) SeekLT(key []byte) *CoalescedSpan {
	k, _ := i.iter.SeekLT(key)
	// We landed on the range key with the greatest start key that still sorts
	// before `key`.  Since we performed a SeekLT, we're on the last fragment
	// with those range key bounds and we need to coalesce backwards.
	return i.coalesceBackward(k)
}

// First seeks the iterator to the first span and returns it.
func (i *Iter) First() *CoalescedSpan {
	i.dir = +1
	k, _ := i.iter.First()
	return i.coalesceForward(k)
}

// Last seeks the iterator to the last span and returns it.
func (i *Iter) Last() *CoalescedSpan {
	i.dir = -1
	k, _ := i.iter.Last()
	return i.coalesceBackward(k)
}

// Next advances to the next span and returns it.
func (i *Iter) Next() *CoalescedSpan {
	switch {
	case i.dir == +1 && !i.iter.Valid():
		// If we were already going forward and the underlying iterator is
		// invalid, there is no next item. Don't move the iterator, just
		// invalidate the iterator's position.
		i.valid = false
		return nil
	case i.dir == -1 && !i.valid:
		// If we were previously going backward and the iterator is positioned
		// at the very beginning, we only need to Next once to position
		// ourselves over the first fragment.
		i.dir = +1
		_, _ = i.iter.Next()
	case i.dir == -1:
		// Switching directions; We're currently positioned at the last of the
		// previous set of fragments. The iterator is valid. In the example
		// below, Current is the span formed by coalescing the y fragments. The
		// ^ marks the i.iter position, over the last fragment of x.:
		//   ... x x x y y y ...
		//           ^
		// We want to return the coalesced span after y (if it exists). We move
		// the underlying iterator once to move over the first fragment of y
		// [which necessarily must exist, since i.valid], coalesce forward to
		// skip over the rest of y's fragments and land on the first fragment of
		// z, if it exists.
		i.dir = +1
		if k, _ := i.iter.Next(); k == nil {
			// Since i.valid, there must be a next fragment on the underlying
			// span.
			i.err = errors.Newf("pebble: invariant violation: next span must exist")
			return nil
		}
		// Now we're positioned over the first fragment for the most recently
		// returned coalesced span (y in the above example). The caller Next'd
		// because they want the coalesced span after that, so Next() once [on
		// this iter, not the inner iter] to skip over it.
		if x := i.Next(); x == nil {
			i.err = errors.Newf("pebble: invariant violation: next span must exist")
			return nil
		}
		// Now we're positioned over the first fragment for the correct span,
		// and may proceed as normal.
	}
	if !i.iter.Valid() {
		i.valid = false
		return nil
	}
	return i.coalesceForward(i.iter.Key())
}

// Prev steps back to the previous span and returns it.
func (i *Iter) Prev() *CoalescedSpan {
	switch {
	case i.dir == -1 && !i.iter.Valid():
		// If we were already going backward and the underlying iterator is
		// invalid, there is no previous item. Don't move the iterator, just
		// invalidate the iterator's position.
		i.valid = false
		return nil
	case i.dir == +1 && !i.valid:
		// If we were previously going forward and the iterator is positioned at
		// the very end, we only need to Prev once to position ourselves over
		// the last fragment.
		i.dir = -1
		_, _ = i.iter.Prev()
	case i.dir == +1:
		// Switching directions; We're currently positioned at the first of the
		// next set of fragments. The iterator is valid. In the example
		// below, Current is the span formed by coalescing the x fragments. The
		// ^ marks the i.iter position, over the first fragment of y.:
		//   ... x x x y y y ...
		//             ^
		// We want to return the coalesced span before x (if it exists). We move
		// the underlying iterator once to move over the last fragment of x
		// [which necessarily must exist, since i.valid], coalesce backward to
		// skip over the rest of x's fragments and land on the first fragment of
		// w, if it exists.
		i.dir = -1
		if k, _ := i.iter.Prev(); k == nil {
			i.err = errors.Newf("pebble: invariant violation: next span must exist")
			return nil
		}
		// Now we're positioned over the last fragment for the most recently
		// returned coalesced span (x in the above example). The caller Prev'd
		// because they want the coalesced span before that, so Prev() once [on
		// this iter, not the inner iter] to skip over it.
		if x := i.Prev(); x == nil {
			i.err = errors.Newf("pebble: invariant violation: next span must exist")
			return nil
		}
		// Now we're positioned over the last fragment for the correct span, and
		// may proceed as normal.
	}
	if !i.iter.Valid() {
		i.valid = false
		return nil
	}
	return i.coalesceBackward(i.iter.Key())
}

// Current returns the span at the iterator's current position, if any.
func (i *Iter) Current() *CoalescedSpan {
	if !i.valid {
		return nil
	}
	return &i.curr
}

// Valid returns true if the iterator is currently positioned over a span.
func (i *Iter) Valid() bool {
	return i.valid
}
