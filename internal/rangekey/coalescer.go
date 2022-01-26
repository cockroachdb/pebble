// Copyright 2021 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package rangekey

import (
	"fmt"
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

// Coalescer imposes range key semantics and coalesces fragments with the same
// bounds. A Coalescer receives—through its Add method—a stream of internal
// range keys (sets, unsets and deletes). Among overlapping fragments (which
// must have the same bounds), fragments must be supplied in order of their
// internal keys (descending by sequence number, descending by kind). Coalescer
// drops any keys shadowed by more recent sets, unsets or deletes.
//
// A Coalescer expects to receive only fragmented range keys, such that if two
// keys overlap in user key space, they have the same bounds. The caller should
// fragment range keys before feeding them to the Coalescer by using the
// internal/keyspan.Fragmenter type. When the coalescer has seen all the
// overlapping fragments for a key span, it emits a coalesced span describing
// the still extant logical range keys, including the Set suffix-value pairs,
// the Unset suffixes and whether any lower-seqnumed spans should be Deleted.
//
// A Coalescer may also coalesce internal range keys in reverse order, through
// AddReverse. Add and AddReverse calls may be made to the same coalescer, but
// they must be separated by calls to Finish.
//
// The memory backing the CoalescedSpan.Items is only guaranteed to be unused
// until the next Add or AddReverse call. Callers should make a copy of the
// Items slice if they wish to retain it beyond the next call to Add or
// AddReverse. Coalescer does not make copies of any user keys, suffixes or
// values, and these byte slices inherit their original lifetimes: The user
// keys, suffixes and values emitted are valid as long as the original buffers
// supplied to Add or AddReverse are valid.
//
// Coalescence has subtle behavior with respect to sequence numbers. The
// coalescer expects overlapping fragments to be added in sequence number
// descending order. The first fragment has the largest sequence number. When a
// coalesced span is emitted, it includes only the largest sequence number. All
// other sequence numbers are forgotten. When a compaction constructs output
// range keys from a coalesced span, it produces at most one RANGEKEYSET, one
// RANGEKEYUNSET and one RANGEKEYDEL. Each one of these keys adopt the largest
// sequence number.
//
// This has the potentially surprising effect of 'promoting' a key to a higher
// sequence number. This is okay, because:
//   - There are no other overlapping fragments within the coalesced span of
//     sequence numbers (otherwise they would be in the compaction, due to the
//     LSM invariant).
//   - Range key sequence numbers are never compared to point key sequence
//     numbers. Range keys and point keys have parallel existences.
//   - Compactions only coalesce within snapshot stripes, calling Finish to
//     start a new coalesced span if a snapshot separates previously Added
//     fragments from the next fragment.
//
// Additionally, internal range keys at the same sequence number have subtle
// mechanics:
//   * RANGEKEYSETs shadow RANGEKEYUNSETs of the same suffix.
//   * RANGEKEYDELs only apply to keys at lower sequence numbers.
// This is required for ingestion. Ingested sstables are assigned a single
// sequence number for the file, at which all of the file's keys are visible.
// The RANGEKEYSET, RANGEKEYUNSET and RANGEKEYDEL key kinds are ordered such
// that when Add-ing the keys with equal sequence numbers in order by their
// kinds, the keys do not affect one another, where possible. Ingested sstables
// are expected to be consistent with respect to the set/unset suffixes: A
// given suffix should be set or unset but not both.
type Coalescer struct {
	emit          func(CoalescedSpan)
	formatKey     base.FormatKey
	visibleSeqNum uint64

	largestSeqNum uint64
	prevKey       base.InternalKey
	start, end    []byte
	items         suffixItems
	// delete, if set, indicates that the current pending span has seen a
	// RangeKeyDelete over the span. During forward accumulation, this causes
	// all subsequent spans to be dropped.
	delete bool
	// dir indicates the direction of accumulation (+1 or -1), or that there is
	// no accumulated state (0)
	dir int8
}

// Init initializes a coalescer.
func (c *Coalescer) Init(cmp base.Compare, formatKey base.FormatKey, visibleSeqNum uint64, emit func(CoalescedSpan)) {
	c.emit = emit
	c.formatKey = formatKey
	c.visibleSeqNum = visibleSeqNum
	c.items.cmp = cmp
}

// Start returns the currently pending span's start key.
func (c *Coalescer) Start() []byte {
	return c.start
}

// Pending returns true if there exists a pending span in the coalescer.
func (c *Coalescer) Pending() bool {
	return len(c.items.items) > 0 || c.delete
}

// Finish must be called after all spans have been added. It flushes the
// remaining pending CoalescedSpan if any. Finish may also be called at any time
// to flush the pending CoalescedSpan and reset the coalescer.
func (c *Coalescer) Finish() {
	if c.dir != 0 {
		c.flush()
	}
}

// Add adds a fragmented range key span to the coalescer. The requirement that
// spans be fragmented requires that if two spans overlap, they must have the
// same start and end bounds. Additionally, among overlapping fragments,
// fragments must be added ordered by Span.Start (descending by sequence number,
// key kind).
func (c *Coalescer) Add(s keyspan.Span) error {
	if c.dir == -1 {
		panic("pebble: cannot change directions during range key coalescence")
	}
	if !s.Start.Visible(c.visibleSeqNum) {
		return nil
	}

	// If s is the first fragment with new bounds, flush.
	if c.dir != 0 && c.items.cmp(s.Start.UserKey, c.start) != 0 {
		// TODO(jackson): Gate behind invariants.Enabled eventually.
		if c.items.cmp(s.Start.UserKey, c.start) < 0 {
			panic(fmt.Sprintf("pebble: range key key ordering invariant violated: %s < %s",
				c.formatKey(s.Start.UserKey), c.formatKey(c.start)))
		}
		if c.items.cmp(s.Start.UserKey, c.end) < 0 {
			panic(fmt.Sprintf("pebble: range key overlapping with distinct bounds: (%s,%s) <> (%s, %s)",
				c.formatKey(s.Start.UserKey), c.formatKey(s.End),
				c.formatKey(c.start), c.formatKey(c.end)))
		}

		// NB: flush sets dir to zero, so we'll initialize the new pending
		// span's information immediately below.
		c.flush()
	}

	if c.dir == 0 {
		c.dir = +1
		c.largestSeqNum = s.Start.SeqNum()
		c.start = s.Start.UserKey
		c.end = s.End
	} else {
		// TODO(jackson): Gate behind invariants.Enabled eventually.
		if base.InternalCompare(c.items.cmp, s.Start, c.prevKey) < 0 {
			panic(fmt.Sprintf("pebble: range key ordering invariant violated: %s < %s",
				s.Start.Pretty(c.formatKey), c.prevKey.Pretty(c.formatKey)))
		}
		if c.items.cmp(s.End, c.end) != 0 {
			panic(fmt.Sprintf("pebble: range keys overlapping with different end bounds: (%s,%s) <> (%s, %s)",
				c.formatKey(s.Start.UserKey), c.formatKey(s.End),
				c.formatKey(c.start), c.formatKey(c.end)))
		}
	}
	c.prevKey = s.Start

	if c.delete {
		// An earlier fragment with the same bounds deleted this span within the
		// range key space. We can skip any later range keys.
		return nil
	}

	// NB: Within a given sequence number, keys are ordered as:
	//   RangeKeySet > RangeKeyUnset > RangeKeyDelete
	// This is significant, because this ensures that none of the range keys
	// sharing a sequence number shadow each other.
	switch s.Start.Kind() {
	case base.InternalKeyKindRangeKeyUnset:
		n := len(c.items.items)

		// value represents a set of suffixes, guaranteed to not have
		// duplicates.
		value := s.Value
		for len(value) > 0 {
			suffix, rest, ok := DecodeSuffix(value)
			if !ok {
				return base.CorruptionErrorf("corrupt unset value: unable to decode suffix")
			}
			value = rest
			if c.get(n, suffix) < n {
				// This suffix is already set or unset at a higher sequence
				// number. Skip.
				continue
			}
			c.items.items = append(c.items.items, SuffixItem{
				Suffix: suffix,
				Unset:  true,
			})
		}
		sort.Sort(c.items)
	case base.InternalKeyKindRangeKeySet:
		n := len(c.items.items)

		// value represents a set of suffixes, guaranteed to not have
		// duplicates.
		value := s.Value
		for len(value) > 0 {
			sv, rest, ok := DecodeSuffixValue(value)
			if !ok {
				return base.CorruptionErrorf("corrupt set value: unable to decode suffix-value tuple")
			}
			value = rest
			if c.get(n, sv.Suffix) < n {
				// This suffix is already set or unset at a higher sequence
				// number. Skip.
				continue
			}
			c.items.items = append(c.items.items, SuffixItem{
				Suffix: sv.Suffix,
				Value:  sv.Value,
			})
		}
		sort.Sort(c.items)
	case base.InternalKeyKindRangeKeyDelete:
		// Record that all subsequent fragments with the same bounds should be
		// ignored, because they were deleted by this RangeKeyDelete.
		c.delete = true
	default:
		return errors.Newf("pebble: unexpected range key kind %s", s.Start.Kind())
	}
	return nil
}

// AddReverse adds a fragmented range key span to the coalescer. The requirement
// that spans be fragmented requires that if two spans overlap, they must have
// the same start and end bounds. Additionally, among overlapping fragments,
// fragments must be added ordered by Span.Start in reverse order (ascending by
// sequence number, key kind).
func (c *Coalescer) AddReverse(s keyspan.Span) error {
	if c.dir == +1 {
		panic("pebble: cannot change directions during range key coalescence")
	}
	if !s.Start.Visible(c.visibleSeqNum) {
		return nil
	}

	// If s is the first fragment with new bounds, flush.
	if c.dir != 0 && c.items.cmp(s.Start.UserKey, c.start) != 0 {
		// TODO(jackson): Gate behind invariants.Enabled eventually.
		if c.items.cmp(s.Start.UserKey, c.start) > 0 {
			panic(fmt.Sprintf("pebble: range key key ordering invariant violated: %s > %s",
				c.formatKey(s.Start.UserKey), c.formatKey(c.start)))
		}
		if c.items.cmp(s.End, c.start) > 0 {
			panic(fmt.Sprintf("pebble: range key overlapping with distinct bounds: (%s,%s) <> (%s, %s)",
				c.formatKey(s.Start.UserKey), c.formatKey(s.End),
				c.formatKey(c.start), c.formatKey(c.end)))
		}

		// NB: flush sets dir to zero, so we'll initialize the new pending
		// span's information immediately below.
		c.flush()
	}

	if c.dir == 0 {
		c.dir = -1
		c.start = s.Start.UserKey
		c.end = s.End
	} else {
		// TODO(jackson): Gate behind invariants.Enabled eventually.
		if base.InternalCompare(c.items.cmp, s.Start, c.prevKey) > 0 {
			panic(fmt.Sprintf("pebble: range key ordering invariant violated: %s > %s",
				s.Start.Pretty(c.formatKey), c.prevKey.Pretty(c.formatKey)))
		}
		if c.items.cmp(s.End, c.end) != 0 {
			panic(fmt.Sprintf("pebble: range keys overlapping with different end bounds: (%s,%s) <> (%s, %s)",
				c.formatKey(s.Start.UserKey), c.formatKey(s.End),
				c.formatKey(c.start), c.formatKey(c.end)))
		}
	}
	c.prevKey = s.Start
	c.largestSeqNum = s.Start.SeqNum()

	// NB: Within a given sequence number, keys are ordered as:
	//   RangeKeySet > RangeKeyUnset > RangeKeyDelete
	// This is significant, because this ensures that none of the range keys
	// sharing a sequence number shadow each other.
	switch s.Start.Kind() {
	case base.InternalKeyKindRangeKeyUnset:
		n := len(c.items.items)

		// value represents a set of suffixes, guaranteed to not have
		// duplicates.
		value := s.Value
		for len(value) > 0 {
			suffix, rest, ok := DecodeSuffix(value)
			if !ok {
				return base.CorruptionErrorf("corrupt unset value: unable to decode suffix")
			}
			value = rest
			if i := c.get(n, suffix); i < n {
				// This suffix is already set or unset at a lower sequence
				// number. Replace it.
				c.items.items[i] = SuffixItem{
					Suffix: suffix,
					Unset:  true,
				}
				continue
			}
			c.items.items = append(c.items.items, SuffixItem{
				Suffix: suffix,
				Unset:  true,
			})
		}
		sort.Sort(c.items)
	case base.InternalKeyKindRangeKeySet:
		n := len(c.items.items)

		// value represents a set of suffixes, guaranteed to not have
		// duplicates.
		value := s.Value
		for len(value) > 0 {
			sv, rest, ok := DecodeSuffixValue(value)
			if !ok {
				return base.CorruptionErrorf("corrupt set value: unable to decode suffix-value tuple")
			}
			value = rest
			if i := c.get(n, sv.Suffix); i < n {
				// This suffix is already set or unset at a lower sequence
				// number. Replace it.
				c.items.items[i] = SuffixItem{
					Suffix: sv.Suffix,
					Value:  sv.Value,
				}
				continue
			}
			c.items.items = append(c.items.items, SuffixItem{
				Suffix: sv.Suffix,
				Value:  sv.Value,
			})
		}
		sort.Sort(c.items)
	case base.InternalKeyKindRangeKeyDelete:
		// Remove all existing items, because they're all deleted.
		c.items.items = c.items.items[:0]
		c.delete = true
	default:
		return errors.Newf("pebble: unexpected range key kind %s", s.Start.Kind())
	}
	return nil
}

// get searches for suffix among the first n items in c.items. If the suffix is
// found, it returns the index of the item with the suffix. If the suffix is not
// found, it returns n.
func (c *Coalescer) get(n int, suffix []byte) (i int) {
	// Binary search for the suffix to see if there's an existing entry with the
	// suffix. Only binary search among the first n items.  get is called while
	// appending new items with suffixes that may sort before existing items.
	// The n parameter indicates what portion of the items slice is sorted and
	// may contain relevant items.
	i = sort.Search(n, func(i int) bool {
		return c.items.cmp(c.items.items[i].Suffix, suffix) >= 0
	})
	if i < len(c.items.items) &&
		c.items.cmp(c.items.items[i].Suffix, suffix) == 0 {
		return i
	}
	return n
}

func (c *Coalescer) flush() {
	c.emit(CoalescedSpan{
		LargestSeqNum: c.largestSeqNum,
		Start:         c.start,
		End:           c.end,
		Items:         c.items.items,
		Delete:        c.delete,
	})
	// Clear all state of the flushed span.
	*c = Coalescer{
		emit:          c.emit,
		formatKey:     c.formatKey,
		visibleSeqNum: c.visibleSeqNum,
		items: suffixItems{
			cmp:   c.items.cmp,
			items: c.items.items[:0],
		},
	}
}

type suffixItems struct {
	cmp   base.Compare
	items []SuffixItem
}

func (s suffixItems) Len() int           { return len(s.items) }
func (s suffixItems) Less(i, j int) bool { return s.cmp(s.items[i].Suffix, s.items[j].Suffix) < 0 }
func (s suffixItems) Swap(i, j int)      { s.items[i], s.items[j] = s.items[j], s.items[i] }
