// Copyright 2021 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package rangekey

import (
	"sort"

	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/pebble/internal/base"
	"github.com/cockroachdb/pebble/internal/invariants"
	"github.com/cockroachdb/pebble/internal/keyspan"
)

// CoalescedSpan describes the state of range keys within a user key span.
type CoalescedSpan struct {
	// LargestSeqNum holds the largest sequence number of any of the internal
	// range keys coalesced into this span.
	LargestSeqNum uint64
	// Start and End hold the inclusive start user key and exclusive end user
	// key.
	Start, End []byte
	// Items contains all of the span's Sets and Unsets, ordered by Suffix.
	Items []SuffixItem
	// Delete is true if a RANGEKEYDEL covering this user key span was coalesced
	// into this span.
	Delete bool
}

// SmallestSetSuffix returns the smallest suffix of an actively set range key
// (excluding suffixes that are unset or deleted), that's also greater than or
// equal to (as defined by cmp) than the suffixThreshold argument.
// SmallestSetSuffix returns nil if suffixThreshold is nil.
func (s *CoalescedSpan) SmallestSetSuffix(cmp base.Compare, suffixThreshold []byte) []byte {
	if suffixThreshold == nil {
		return nil
	}
	// NB: s.Items is sorted in ascending order, so return the first matching
	// suffix.
	for i := range s.Items {
		if invariants.Enabled && i > 0 && cmp(s.Items[i-1].Suffix, s.Items[i].Suffix) >= 0 {
			panic("pebble: invariant violation: coalesced span's suffixes out-of-order")
		}
		if !s.Items[i].Unset && cmp(s.Items[i].Suffix, suffixThreshold) >= 0 {
			return s.Items[i].Suffix
		}
	}
	return nil
}

// HasSets returns true if the coalesced span contains any Sets. When several
// internal range keys are coalesced, it's possible for the resulting span to
// only contain unsets or deletes. If encountered during user iteration, these
// range keys may be elided.
func (s *CoalescedSpan) HasSets() bool {
	for _, i := range s.Items {
		if !i.Unset {
			return true
		}
	}
	return false
}

// SuffixItem describes either a set or unset of a value at a particular
// suffix.
type SuffixItem struct {
	Suffix []byte
	Value  []byte
	Unset  bool
}

// TODO(jackson): Refactor Coalesce to return a keyspan.Span rather than a
// CoalescedSpan.

// Coalesce imposes range key semantics and coalesces range keys with the same
// bounds. Coalesce drops any keys shadowed by more recent sets, unsets or
// deletes. Coalesce returns a CoalescedSpan describing the still extant logical
// range keys, including the Set suffix-value pairs, the Unset suffixes and
// whether any lower-seqnumed spans should be Deleted.
//
// Coalescence has subtle behavior with respect to sequence numbers. Coalesce
// depends on a keyspan.Span's Keys being sorted in sequence number descending
// order. The first key has the largest sequence number. The returned coalesced
// span includes only the largest sequence number. All other sequence numbers
// are forgotten. When a compaction constructs output range keys from a
// coalesced span, it produces at most one RANGEKEYSET, one RANGEKEYUNSET and
// one RANGEKEYDEL. Each one of these keys adopt the largest sequence number.
//
// This has the potentially surprising effect of 'promoting' a key to a higher
// sequence number. This is okay, because:
//   - There are no other overlapping keys within the coalesced span of
//     sequence numbers (otherwise they would be in the compaction, due to
//     the LSM invariant).
//   - Range key sequence numbers are never compared to point key sequence
//     numbers. Range keys and point keys have parallel existences.
//   - Compactions only coalesce within snapshot stripes.
//
// Additionally, internal range keys at the same sequence number have subtle
// mechanics:
//   * RANGEKEYSETs shadow RANGEKEYUNSETs of the same suffix.
//   * RANGEKEYDELs only apply to keys at lower sequence numbers.
// This is required for ingestion. Ingested sstables are assigned a single
// sequence number for the file, at which all of the file's keys are visible.
// The RANGEKEYSET, RANGEKEYUNSET and RANGEKEYDEL key kinds are ordered such
// that among keys with equal sequence numbers (thus ordered by their kinds) the
// keys do not affect one another. Ingested sstables are expected to be
// consistent with respect to the set/unset suffixes: A given suffix should be
// set or unset but not both.
func Coalesce(cmp base.Compare, span keyspan.Span) (CoalescedSpan, error) {
	var cs CoalescedSpan
	var items suffixItems
	items.cmp = cmp
	items.items = make([]SuffixItem, 0, len(span.Keys))

	cs.Start = span.Start
	cs.End = span.End
	for i := 0; i < len(span.Keys) && !cs.Delete; i++ {
		k := span.Keys[i]
		if i == 0 {
			cs.LargestSeqNum = k.SeqNum()
		}
		if invariants.Enabled && k.SeqNum() > cs.LargestSeqNum {
			panic("pebble: invariant violation: span keys unordered")
		}

		// NB: Within a given sequence number, keys are ordered as:
		//   RangeKeySet > RangeKeyUnset > RangeKeyDelete
		// This is significant, because this ensures that none of the range keys
		// sharing a sequence number shadow each other.
		switch k.Kind() {
		case base.InternalKeyKindRangeKeySet:
			n := len(items.items)

			if items.get(n, k.Suffix) < n {
				// This suffix is already set or unset at a higher sequence
				// number. Skip.
				continue
			}
			items.items = append(items.items, SuffixItem{
				Suffix: k.Suffix,
				Value:  k.Value,
			})
			sort.Sort(items)
		case base.InternalKeyKindRangeKeyUnset:
			n := len(items.items)

			if items.get(n, k.Suffix) < n {
				// This suffix is already set or unset at a higher sequence
				// number. Skip.
				continue
			}
			items.items = append(items.items, SuffixItem{
				Suffix: k.Suffix,
				Unset:  true,
			})
			sort.Sort(items)
		case base.InternalKeyKindRangeKeyDelete:
			// Record that all range keys in this span have been deleted by this
			// RangeKeyDelete. There's no need to continue looping, because all
			// the remaining keys are shadowed by this one. The for loop
			// will terminate as soon as cs.Delete is set.
			cs.Delete = true
		default:
			return CoalescedSpan{}, errors.Newf("pebble: unexpected range key kind %s", k.Kind())
		}
	}
	cs.Items = items.items
	return cs, nil
}

type suffixItems struct {
	cmp   base.Compare
	items []SuffixItem
}

// get searches for suffix among the first n items in items. If the suffix is
// found, it returns the index of the item with the suffix. If the suffix is not
// found, it returns n.
func (si suffixItems) get(n int, suffix []byte) (i int) {
	// Binary search for the suffix to see if there's an existing entry with the
	// suffix. Only binary search among the first n items.  get is called while
	// appending new items with suffixes that may sort before existing items.
	// The n parameter indicates what portion of the items slice is sorted and
	// may contain relevant items.

	i = sort.Search(n, func(i int) bool {
		return si.cmp(si.items[i].Suffix, suffix) >= 0
	})
	if i < n && si.cmp(si.items[i].Suffix, suffix) == 0 {
		return i
	}
	return n
}

func (si suffixItems) Len() int           { return len(si.items) }
func (si suffixItems) Less(i, j int) bool { return si.cmp(si.items[i].Suffix, si.items[j].Suffix) < 0 }
func (si suffixItems) Swap(i, j int)      { si.items[i], si.items[j] = si.items[j], si.items[i] }
