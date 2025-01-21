// Copyright 2024 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package compact

import (
	"sort"

	"github.com/cockroachdb/pebble/v2/internal/base"
)

// Snapshots stores a list of snapshot sequence numbers, in ascending order.
//
// Snapshots are lightweight point-in-time views of the DB state. At its core,
// a snapshot is a sequence number along with a guarantee from Pebble that it
// will maintain the view of the database at that sequence number. Part of this
// guarantee is relatively straightforward to achieve. When reading from the
// database Pebble will ignore sequence numbers that are larger than the
// snapshot sequence number. The primary complexity with snapshots occurs
// during compaction: the collapsing of entries that are shadowed by newer
// entries is at odds with the guarantee that Pebble will maintain the view of
// the database at the snapshot sequence number. Rather than collapsing entries
// up to the next user key, compactionIter can only collapse entries up to the
// next snapshot boundary. That is, every snapshot boundary potentially causes
// another entry for the same user-key to be emitted. Another way to view this
// is that snapshots define stripes and entries are collapsed within stripes,
// but not across stripes. Consider the following scenario:
//
//	a.PUT.9
//	a.DEL.8
//	a.PUT.7
//	a.DEL.6
//	a.PUT.5
//
// In the absence of snapshots these entries would be collapsed to
// a.PUT.9. What if there is a snapshot at sequence number 7? The entries can
// be divided into two stripes and collapsed within the stripes:
//
//	a.PUT.9        a.PUT.9
//	a.DEL.8  --->
//	a.PUT.7
//	--             --
//	a.DEL.6  --->  a.DEL.6
//	a.PUT.5
type Snapshots []base.SeqNum

// Index returns the index of the first snapshot sequence number which is >= seq
// or len(s) if there is no such sequence number.
func (s Snapshots) Index(seq base.SeqNum) int {
	return sort.Search(len(s), func(i int) bool {
		return s[i] > seq
	})
}

// IndexAndSeqNum returns the index of the first snapshot sequence number which
// is >= seq and that sequence number, or len(s) and InternalKeySeqNumMax if
// there is no such sequence number.
func (s Snapshots) IndexAndSeqNum(seq base.SeqNum) (int, base.SeqNum) {
	index := s.Index(seq)
	if index == len(s) {
		return index, base.SeqNumMax
	}
	return index, s[index]
}
