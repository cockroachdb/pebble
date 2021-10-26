// Copyright 2018 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package keyspan

import "github.com/cockroachdb/pebble/internal/base"

// Get returns the newest span that contains the target key. If no span
// contains the target key, an empty span is returned. The snapshot
// parameter controls the visibility of spans (only spans older than the
// snapshot sequence number are visible). The iterator must contain
// fragmented spans: any overlapping spans must have the same start and
// end key.
func Get(cmp base.Compare, iter base.InternalIterator, key []byte, snapshot uint64) Span {
	// NB: We use SeekLT in order to land on the proper span for a search
	// key that resides in the middle of a span. Consider the scenario:
	//
	//     a---e
	//         e---i
	//
	// The spans are indexed by their start keys `a` and `e`. If the
	// search key is `c` we want to land on the span [a,e). If we were
	// to use SeekGE then the search key `c` would land on the span
	// [e,i) and we'd have to backtrack. The one complexity here is what
	// happens for the search key `e`. In that case SeekLT will land us
	// on the span [a,e) and we'll have to move forward.
	iterKey, iterValue := iter.SeekLT(key)
	if iterKey == nil {
		iterKey, iterValue = iter.Next()
		if iterKey == nil {
			// The iterator is empty.
			return Span{}
		}
		if cmp(key, iterKey.UserKey) < 0 {
			// The search key lies before the first span.
			return Span{}
		}
	}

	// Invariant: key >= iter.Key().UserKey

	if cmp(key, iterValue) < 0 {
		// The current span contains the search key, but SeekLT returns
		// the oldest entry for a key, so backup until we hit the
		// previous span or an entry which is not visible.
		for {
			iterKey, iterValue = iter.Prev()
			if iterKey == nil || cmp(key, iterValue) >= 0 || !iterKey.Visible(snapshot) {
				iterKey, iterValue = iter.Next()
				break
			}
		}
	} else {
		// The current span lies before the search key. Advance the iterator
		// as long as the search key lies past the end of the span. See the
		// comment at the start of this function about why this is necessary.
		for {
			iterKey, iterValue = iter.Next()
			if iterKey == nil || cmp(key, iterKey.UserKey) < 0 {
				// We've run out of spans or we've moved on to a span which
				// starts after our search key.
				return Span{}
			}
			if cmp(key, iterValue) < 0 {
				break
			}
		}
	}

	for {
		if start := iterKey; start.Visible(snapshot) {
			// The span is visible at our read sequence number.
			return Span{
				Start: *start,
				End:   iterValue,
			}
		}
		iterKey, iterValue = iter.Next()
		if iterKey == nil || cmp(key, iterKey.UserKey) < 0 {
			// We've run out of spans or we've moved on to a span which
			// starts after our search key.
			return Span{}
		}
	}
}
