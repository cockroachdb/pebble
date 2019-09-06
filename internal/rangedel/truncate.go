// Copyright 2019 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package rangedel

import "github.com/cockroachdb/pebble/internal/base"

// Truncate creates a new iterator where every tombstone in the supplied
// iterator is truncated to be contained within the range [lower, upper).
func Truncate(cmp base.Compare, iter iterator, lower, upper []byte) *Iter {
	var tombstones []Tombstone
	for key, value := iter.First(); key != nil; key, value = iter.Next() {
		t := Tombstone{
			Start: *key,
			End:   value,
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
