// Copyright 2018 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package keyspan

import "github.com/cockroachdb/pebble/internal/base"

// SeekGE seeks to the newest span that contains or is past the target key. The
// snapshot parameter controls the visibility of spans (only spans older than
// the snapshot sequence number are visible). The iterator must contain
// fragmented spans: any overlapping spans must have the same start and end key.
// The position of the iterator is undefined after calling SeekGE and may not be
// pointing at the returned span.
func SeekGE(cmp base.Compare, iter FragmentIterator, key []byte, snapshot uint64) Span {
	// NB: We use SeekLT in order to land on the proper span for a search
	// key that resides in the middle of a span. Consider the scenario:
	//
	//     a---e
	//         e---i
	//
	// The spans are indexed by their start keys `a` and `e`. If the
	// search key is `c` we want to land on the span [a,e). If we were to
	// use SeekGE then the search key `c` would land on the span [e,i) and
	// we'd have to backtrack. The one complexity here is what happens for the
	// search key `e`. In that case SeekLT will land us on the span [a,e)
	// and we'll have to move forward.
	iterKey, _ := iter.SeekLT(key)

	// Invariant: key < iter.Current().Start.UserKey

	if iterKey != nil && cmp(key, iter.End()) < 0 {
		// The current spans contains or is past the search key, but SeekLT
		// returns the oldest entry for a key, so backup until we hit the previous
		// span or an entry which is not visible.
		for savedKey := iterKey.UserKey; ; {
			iterKey, _ = iter.Prev()
			if iterKey == nil || cmp(savedKey, iter.End()) >= 0 || !iterKey.Visible(snapshot) {
				iterKey, _ = iter.Next()
				break
			}
		}
	} else {
		// The current span lies before the search key. Advance the iterator
		// to the next span which is guaranteed to lie at or past the search
		// key.
		iterKey, _ = iter.Next()
		if iterKey == nil {
			// We've run out of spans.
			return Span{}
		}
	}

	// The iter is positioned at a non-nil iterKey which is the earliest iterator
	// position that satisfies the requirement that it contains or is past the
	// target key. But it may not be visible based on the snapshot. So now we
	// only need to move forward and return the first span that is visible.
	//
	// Walk through the spans to find one the newest one that is visible
	// (i.e. has a sequence number less than the snapshot sequence number).
	for {
		if start := iterKey; start.Visible(snapshot) {
			// The span is visible at our read sequence number.
			return iter.Current()
		}
		iterKey, _ = iter.Next()
		if iterKey == nil {
			// We've run out of spans.
			return Span{}
		}
	}
}

// SeekLE seeks to the newest span that contains or is before the target
// key. The snapshot parameter controls the visibility of spans (only
// spans older than the snapshot sequence number are visible). The
// iterator must contain fragmented spans: any overlapping spans must
// have the same start and end key. The position of the iterator is undefined
// after calling SeekLE and may not be pointing at the returned span.
func SeekLE(cmp base.Compare, iter FragmentIterator, key []byte, snapshot uint64) Span {
	// NB: We use SeekLT in order to land on the proper span for a search
	// key that resides in the middle of a span. Consider the scenario:
	//
	//     a---e
	//         e---i
	//
	// The spans are indexed by their start keys `a` and `e`. If the
	// search key is `c` we want to land on the span [a,e). If we were to
	// use SeekGE then the search key `c` would land on the span [e,i) and
	// we'd have to backtrack. The one complexity here is what happens for the
	// search key `e`. In that case SeekLT will land us on the span [a,e)
	// and we'll have to move forward.
	iterKey, _ := iter.SeekLT(key)

	// Consider the following set of fragmented spans, ordered by increasing
	// key and decreasing seqnum:
	//
	//   2:   d---h
	//   1:   d---h
	//   2:         j---n
	//   1:         j---n
	//   2:             n---r
	//   1:             n---r
	//
	// The cases to consider:
	//
	// 1. search-key == "a"
	//   - The search key is fully before any span. We should return an
	//     empty span. The initial SeekLT("a") will return iterKey==nil and
	//     the next span [d,h) lies fully after the search key.
	//
	// 2. search-key == "d"
	//   - The search key is contained by the span [d,h). We want to return
	//     the newest version of the span [d,h) or the empty span if
	//     there are no visible versions. The initial SeekLT("d") will return
	//     iterKey==nil and the next span [d,h) contains the search key. We
	//     iterate forward from there returning the newest visible span for
	//     [d, h) and if there is no visible one return an empty span.
	//
	// 3. search-key == "h" or "i"
	//   - The search key lies between the spans [d,h) and [j,n). We want to
	//     return the newest visible version of the span [d,h) or the empty
	//     span if there are no visible versions. The initial SeekLT("h") or
	//     SeekLT("i") will return the span [d,h)#1. Because the end key of
	//     this span is less than or equal to the search key we have to
	//     check if the next span contains the search key. In this case it
	//     does not and we need to look backwards starting from [d, h)#1, falling
	//     into case 5.
	//
	// 4. search-key == "n"
	//   - The search key is contained by the span [n,r). We want to return
	//     the newest version of the span [n,r) or an earlier span if
	//     there are no visible versions of [n,r). The initial SeekLT("n") will
	//     return the span [j,n)#1. Because the end key of the spans
	//     [j,n) equals the search key "n" we have to see if the next span
	//     contains the search key (which it does). We iterate forward through
	//     the [n,r) spans (not beyond) and return the first visible
	//     span (since this iteration is going from newer to older). If
	//     there are no visible [n,r) spans (due to the snapshot parameter)
	//     we need to step back to [n,r) and look backwards, falling into case 5.
	//
	// 5. search-key == "p"
	//   - The search key is contained by the span [n,r). We want to return
	//     the newest visible version of the span [n,r) or an earlier
	//     span if there are no visible versions. Because the end key of the
	//     span [n,r) is greater than the search key "p", we do not have to
	//     look at the next span. We iterate backwards starting with [n,r),
	//     then [j,n) and then [d,h), returning the newest version of the first
	//     visible span.

	switch {
	case iterKey == nil:
		// Cases 1 and 2. Advance the iterator until we find a visible version, we
		// exhaust the iterator, or we hit the next span.
		for {
			iterKey, _ = iter.Next()
			if iterKey == nil || cmp(key, iterKey.UserKey) < 0 {
				// The iterator is exhausted or we've hit the next span.
				return Span{}
			}
			if start := iterKey; start.Visible(snapshot) {
				return iter.Current()
			}
		}

	default:
		// Invariant: key > iterKey.UserKey
		if cmp(key, iter.End()) >= 0 {
			// Cases 3 and 4 (forward search). The current span lies before the
			// search key. Check to see if the next span contains the search
			// key. If it doesn't, we'll backup and look for an earlier span.
			iterKey, _ = iter.Next()
			if iterKey == nil || cmp(key, iterKey.UserKey) < 0 {
				// Case 3. The next span is past our search key (or there is no next
				// span).
				iterKey, _ = iter.Prev()
			} else {
				// Case 4. Advance the iterator until we find a visible version or we hit
				// the next span.
				for {
					if start := iterKey; start.Visible(snapshot) {
						// We've found our span as we know earlier spans are
						// either not visible or lie before this span.
						return iter.Current()
					}
					iterKey, _ = iter.Next()
					if iterKey == nil || cmp(key, iterKey.UserKey) < 0 {
						// There is no next span, or the next span is past our
						// search key. Back up to the previous span. Note that we'll
						// immediately fall into the loop below which will keep on
						// iterating backwards until we find a visible span and that
						// span must contain or be before our search key.
						iterKey, _ = iter.Prev()
						break
					}
				}
			}
		}

		// Cases 3, 4, and 5 (backwards search). We're positioned at a span
		// that contains or is before the search key. Walk backward until we find a
		// visible span from this point.
		for !iterKey.Visible(snapshot) {
			iterKey, _ = iter.Prev()
			if iterKey == nil {
				// No visible spans before our search key.
				return Span{}
			}
		}

		// We're positioned at a span that contains or is before the search
		// key and is visible. Walk backwards until we find the latest version of
		// this span that is visible (i.e. has a sequence number less than the
		// snapshot sequence number).
		s := iter.Current() // current candidate to return
		for {
			iterKey, _ = iter.Prev()
			if iterKey == nil {
				// We stepped off the end of the iterator.
				break
			}
			if !iterKey.Visible(snapshot) {
				// The previous span is not visible.
				break
			}
			if cmp(s.Start.UserKey, iterKey.UserKey) != 0 {
				// The previous span is before our candidate span.
				break
			}
			// Update the candidate span's seqnum. NB: The end key is guaranteed
			// to be the same.
			s.Start.Trailer = iterKey.Trailer
		}
		return s
	}
}
