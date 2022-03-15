// Copyright 2019 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package keyspan

import "github.com/cockroachdb/pebble/internal/base"

// Truncate creates a new iterator where every span in the supplied iterator is
// truncated to be contained within the range [lower, upper).  If start and end
// are specified, filter out any spans that are completely outside those bounds.
func Truncate(
	cmp base.Compare, iter FragmentIterator, lower, upper []byte, start, end *base.InternalKey,
) *Iter {
	var spans []Span
	for s := iter.First(); s.Valid(); s = iter.Next() {
		// Ignore this span if it lies completely outside [start, end].
		//
		// The comparison between s.End and start is by user key only, as
		// the span is exclusive at s.End, so comparing by user keys
		// is sufficient.
		if start != nil && cmp(s.End, start.UserKey) <= 0 {
			continue
		}
		if end != nil {
			v := cmp(s.Start, end.UserKey)
			switch {
			case v > 0:
				// Wholly outside the end bound. Skip it.
				continue
			case v == 0:
				// This span begins at the same user key as `end`. Whether or
				// not any of the keys contained within the span are relevant is
				// dependent on Trailers. Any keys contained within the span
				// with trailers larger than end cover the small sliver of
				// keyspace between [k#inf, k#<end-seqnum>]. Since keys are
				// sorted descending by Trailer within the span, we need to find
				// the prefix of keys with larger trailers.
				for i := range s.Keys {
					if s.Keys[i].Trailer < end.Trailer {
						s.Keys = s.Keys[:i]
						break
					}
				}
			default:
				// Wholly within the end bound. Keep it.
			}
		}
		// Truncate the bounds to lower and upper.
		if cmp(s.Start, lower) < 0 {
			s.Start = lower
		}
		if cmp(s.End, upper) > 0 {
			s.End = upper
		}
		if !s.Empty() && cmp(s.Start, s.End) < 0 {
			spans = append(spans, s.ShallowClone())
		}
	}
	return NewIter(cmp, spans)
}
