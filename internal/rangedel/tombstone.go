// Copyright 2018 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package rangedel // import "github.com/cockroachdb/pebble/internal/rangedel"

import (
	"fmt"

	"github.com/cockroachdb/pebble/internal/base"
)

// Tombstone is a range deletion tombstone. A range deletion tombstone deletes
// all of the keys in the range [start,end). Note that the start key is
// inclusive and the end key is exclusive.
type Tombstone struct {
	Start base.InternalKey
	End   []byte
}

// Empty returns true if the tombstone does not cover any keys.
func (t Tombstone) Empty() bool {
	return t.Start.Kind() != base.InternalKeyKindRangeDelete
}

// Contains returns true if the specified key resides within the range
// tombstone bounds.
func (t Tombstone) Contains(cmp base.Compare, key []byte) bool {
	return cmp(t.Start.UserKey, key) <= 0 && cmp(key, t.End) < 0
}

// Deletes returns true if the tombstone deletes keys at seqNum.
func (t Tombstone) Deletes(seqNum uint64) bool {
	return !t.Empty() && t.Start.SeqNum() > seqNum
}

func (t Tombstone) String() string {
	if t.Empty() {
		return "<empty>"
	}
	return fmt.Sprintf("%s-%s#%d", t.Start.UserKey, t.End, t.Start.SeqNum())
}
