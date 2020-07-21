// Copyright 2019 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package rangedel

import "github.com/cockroachdb/pebble/internal/base"

// Truncate creates a new iterator where every tombstone in the supplied
// iterator is truncated to be contained within the range [lower, upper).
// If the file's end bound is specified, filter out any range tombstones that
// are completely outside the file's end bounds.
func Truncate(cmp base.Compare, iter base.InternalIterator, lower, upper []byte, end *base.InternalKey) *Iter {
	var tombstones []Tombstone
	for key, value := iter.First(); key != nil; key, value = iter.Next() {
		t := Tombstone{
			Start: *key,
			End:   value,
		}
		if end != nil && base.InternalCompare(cmp, t.Start, *end) > 0 {
			// Range deletion tombstones are often written to sstables
			// untruncated on the end key side. However, they are still only
			// valid within a given file's bounds. The logic for writing range
			// tombstones to an output file sometimes has an incomplete view
			// of range tombstones outside the file's internal key bounds.
			continue
		}
		if cmp(t.Start.UserKey, lower) < 0 {
			t.Start.UserKey = lower
		}
		if cmp(t.End, upper) > 0 {
			t.End = upper
		}
		if cmp(t.Start.UserKey, t.End) < 0 {
			tombstones = append(tombstones, t)
		}
	}
	return NewIter(cmp, tombstones)
}
