// Copyright 2019 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package keyspan

import "github.com/cockroachdb/pebble/internal/base"

// Truncate creates a new iterator where every span in the supplied
// iterator is truncated to be contained within the range [lower, upper).
// If start and end are specified, filter out any spans that
// are completely outside those bounds.
func Truncate(
	cmp base.Compare, iter base.InternalIterator, lower, upper []byte, start, end *base.InternalKey,
) *Iter {
	var spans []Span
	for key, value := iter.First(); key != nil; key, value = iter.Next() {
		s := Span{
			Start: *key,
			End:   value,
		}
		// Ignore this span if it lies completely outside [start, end].
		// The comparison between s.End and start is by user key only, as
		// the span is exclusive at s.End, so comparing by user keys
		// is sufficient. Alternatively, the below comparison can be seen to
		// be logically equivalent to:
		// InternalKey{UserKey: s.End, SeqNum: SeqNumMax} < start
		if start != nil && cmp(s.End, start.UserKey) <= 0 {
			continue
		}
		if end != nil && base.InternalCompare(cmp, s.Start, *end) > 0 {
			continue
		}
		if cmp(s.Start.UserKey, lower) < 0 {
			s.Start.UserKey = lower
		}
		if cmp(s.End, upper) > 0 {
			s.End = upper
		}
		if cmp(s.Start.UserKey, s.End) < 0 {
			spans = append(spans, s)
		}
	}
	return NewIter(cmp, spans)
}
