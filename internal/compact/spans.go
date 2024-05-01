// Copyright 2024 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package compact

import (
	"github.com/cockroachdb/pebble/internal/base"
	"github.com/cockroachdb/pebble/internal/invariants"
	"github.com/cockroachdb/pebble/internal/keyspan"
	"github.com/cockroachdb/pebble/internal/rangekey"
	"github.com/cockroachdb/pebble/sstable"
)

// RangeKeySpanCompactor coalesces range keys within snapshot stripes and elides
// RangeKeyDelete and RangeKeyUnsets when possible. It is used as a container
// for at most one "compacted" span.
type RangeKeySpanCompactor struct {
	cmp       base.Compare
	equal     base.Equal
	snapshots Snapshots
	elider    rangeTombstoneElider

	span keyspan.Span
}

// MakeRangeKeySpanCompactor creates a new compactor for range key spans.
func MakeRangeKeySpanCompactor(
	cmp base.Compare, equal base.Equal, snapshots Snapshots, elision TombstoneElision,
) RangeKeySpanCompactor {
	c := RangeKeySpanCompactor{
		cmp:       cmp,
		equal:     equal,
		snapshots: snapshots,
	}
	c.elider.Init(cmp, elision)
	return c
}

// String returns the string representation of the span in the compactor, or "."
// if it is empty.
func (c *RangeKeySpanCompactor) String() string {
	if c.Empty() {
		return "."
	}
	return c.span.String()
}

// Empty returns whether the compactor contains a span.
func (c *RangeKeySpanCompactor) Empty() bool {
	return c.span.Empty()
}

// StartKey returns the start key of the span in the compactor, or nil if the
// compactor is empty.
func (c *RangeKeySpanCompactor) StartKey() []byte {
	if c.Empty() {
		return nil
	}
	return c.span.Start
}

// Encode encodes and writes out the span in the compactor (if any) to the given
// writer. The compactor will be empty after the call.
func (c *RangeKeySpanCompactor) Encode(tw *sstable.Writer) error {
	// Encode the entire span and reset it.
	if err := rangekey.Encode(&c.span, tw.AddRangeKey); err != nil {
		return err
	}
	c.span.Reset()
	return nil
}

// EncodeUpTo encodes and writes out the span in the compactor (if any) to the
// given writer. If upToKey is nil or the span in the compactor ends at or
// before upToKey, EncodeUpTo() is equivalent to Encode(). Otherwise, the span
// is split at upToKey and only the first part is encoded; the rest is left in
// the container.
func (c *RangeKeySpanCompactor) EncodeUpTo(upToKey []byte, tw *sstable.Writer) error {
	if c.Empty() {
		return nil
	}

	if upToKey == nil || c.cmp(c.span.End, upToKey) <= 0 {
		return c.Encode(tw)
	}

	if c.cmp(c.span.Start, upToKey) >= 0 {
		// The span starts at/after upToKey; nothing to encode.
		return nil
	}

	// Split the span at upToKey and encode the first part.
	splitSpan := keyspan.Span{
		Start: c.span.Start,
		End:   upToKey,
		Keys:  c.span.Keys,
	}
	if err := rangekey.Encode(&splitSpan, tw.AddRangeKey); err != nil {
		return err
	}
	c.span.Start = append(c.span.Start[:0], upToKey...)
	return nil
}

// Compact compacts the given range key span and stores the results in the
// compactor.
//
// Compaction of a span entails coalescing range keys within snapshot
// stripes, and eliding RangeKeyUnset/RangeKeyDelete in the last stripe if
// possible.
//
// The compactor must be empty when this method is called. It is possible for
// the container to be empty after the call (if all range keys in the
// span are elided).
//
// The spans that are passed to Compact calls must be ordered and
// non-overlapping.
func (c *RangeKeySpanCompactor) Compact(s *keyspan.Span) {
	if invariants.Enabled {
		if !c.Empty() {
			panic("Set called on non-empty span")
		}
		if s.KeysOrder != keyspan.ByTrailerDesc {
			panic("unexpected keys order")
		}
	}

	// snapshots are in ascending order, while s.keys are in descending seqnum
	// order. Partition s.keys by snapshot stripes, and call rangekey.Coalesce
	// on each partition.
	c.span.Keys = c.span.Keys[:0]
	x, y := len(c.snapshots)-1, 0
	usedLen := 0
	for x >= 0 {
		start := y
		for y < len(s.Keys) && !base.Visible(s.Keys[y].SeqNum(), c.snapshots[x], base.InternalKeySeqNumMax) {
			// Include y in current partition.
			y++
		}
		if y > start {
			keysDst := c.span.Keys[usedLen:cap(c.span.Keys)]
			rangekey.Coalesce(c.cmp, c.equal, s.Keys[start:y], &keysDst)
			if y == len(s.Keys) {
				// This is the last snapshot stripe. Unsets and deletes can be elided.
				keysDst = c.elideInLastStripe(s.Start, s.End, keysDst)
			}
			usedLen += len(keysDst)
			c.span.Keys = append(c.span.Keys, keysDst...)
		}
		x--
	}
	if y < len(s.Keys) {
		keysDst := c.span.Keys[usedLen:cap(c.span.Keys)]
		rangekey.Coalesce(c.cmp, c.equal, s.Keys[y:], &keysDst)
		keysDst = c.elideInLastStripe(s.Start, s.End, keysDst)
		usedLen += len(keysDst)
		c.span.Keys = append(c.span.Keys, keysDst...)
	}
	if len(c.span.Keys) > 0 {
		c.span.Start = append(c.span.Start[:0], s.Start...)
		c.span.End = append(c.span.End[:0], s.End...)
	}
}

func (c *RangeKeySpanCompactor) elideInLastStripe(
	start, end []byte, keys []keyspan.Key,
) []keyspan.Key {
	// Unsets and deletes in the last snapshot stripe can be elided.
	k := 0
	for j := range keys {
		if (keys[j].Kind() == base.InternalKeyKindRangeKeyUnset || keys[j].Kind() == base.InternalKeyKindRangeKeyDelete) &&
			c.elider.ShouldElide(start, end) {
			continue
		}
		keys[k] = keys[j]
		k++
	}
	return keys[:k]
}
