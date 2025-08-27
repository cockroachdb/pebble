// Copyright 2024 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package compact

import (
	"github.com/cockroachdb/pebble/v2/internal/base"
	"github.com/cockroachdb/pebble/v2/internal/invariants"
	"github.com/cockroachdb/pebble/v2/internal/keyspan"
	"github.com/cockroachdb/pebble/v2/internal/rangekey"
	"github.com/cockroachdb/pebble/v2/sstable"
)

// RangeDelSpanCompactor coalesces RANGEDELs within snapshot stripes and elides
// RANGEDELs in the last stripe if possible.
type RangeDelSpanCompactor struct {
	cmp       base.Compare
	equal     base.Equal
	snapshots Snapshots
	elider    rangeTombstoneElider
}

// MakeRangeDelSpanCompactor creates a new compactor for RANGEDEL spans.
func MakeRangeDelSpanCompactor(
	cmp base.Compare, equal base.Equal, snapshots Snapshots, elision TombstoneElision,
) RangeDelSpanCompactor {
	c := RangeDelSpanCompactor{
		cmp:       cmp,
		equal:     equal,
		snapshots: snapshots,
	}
	c.elider.Init(cmp, elision)
	return c
}

// Compact compacts the given range del span and stores the results in the
// given output span, reusing its slices.
//
// Compaction of a span entails coalescing RANGEDELs keys within snapshot
// stripes, and eliding RANGEDELs in the last stripe if possible.
//
// It is possible for the output span to be empty after the call (if all
// RANGEDELs in the span are elided).
//
// The spans that are passed to Compact calls must be ordered and
// non-overlapping.
func (c *RangeDelSpanCompactor) Compact(span, output *keyspan.Span) {
	if invariants.Enabled && span.KeysOrder != keyspan.ByTrailerDesc {
		panic("pebble: span's keys unexpectedly not in trailer order")
	}
	output.Reset()
	// Apply the snapshot stripe rules, keeping only the latest tombstone for
	// each snapshot stripe.
	currentIdx := -1
	for _, k := range span.Keys {
		idx := c.snapshots.Index(k.SeqNum())
		if currentIdx == idx {
			continue
		}
		if idx == 0 && c.elider.ShouldElide(span.Start, span.End) {
			// This is the last snapshot stripe and the range tombstone
			// can be elided.
			break
		}

		output.Keys = append(output.Keys, k)
		if idx == 0 {
			// This is the last snapshot stripe.
			break
		}
		currentIdx = idx
	}
	if len(output.Keys) > 0 {
		output.Start = append(output.Start, span.Start...)
		output.End = append(output.End, span.End...)
		output.KeysOrder = span.KeysOrder
	}
}

// RangeKeySpanCompactor coalesces range keys within snapshot stripes and elides
// RangeKeyDelete and RangeKeyUnsets when possible. It is used as a container
// for at most one "compacted" span.
type RangeKeySpanCompactor struct {
	cmp       base.Compare
	suffixCmp base.CompareRangeSuffixes
	snapshots Snapshots
	elider    rangeTombstoneElider
}

// MakeRangeKeySpanCompactor creates a new compactor for range key spans.
func MakeRangeKeySpanCompactor(
	cmp base.Compare,
	suffixCmp base.CompareRangeSuffixes,
	snapshots Snapshots,
	elision TombstoneElision,
) RangeKeySpanCompactor {
	c := RangeKeySpanCompactor{
		cmp:       cmp,
		suffixCmp: suffixCmp,
		snapshots: snapshots,
	}
	c.elider.Init(cmp, elision)
	return c
}

// Compact compacts the given range key span and stores the results in the
// given output span, reusing its slices.
//
// Compaction of a span entails coalescing range keys within snapshot
// stripes, and eliding RangeKeyUnset/RangeKeyDelete in the last stripe if
// possible.
//
// It is possible for the output span to be empty after the call (if all range
// keys in the span are elided).
//
// The spans that are passed to Compact calls must be ordered and
// non-overlapping.
func (c *RangeKeySpanCompactor) Compact(span, output *keyspan.Span) {
	if invariants.Enabled && span.KeysOrder != keyspan.ByTrailerDesc {
		panic("pebble: span's keys unexpectedly not in trailer order")
	}
	// snapshots are in ascending order, while s.keys are in descending seqnum
	// order. Partition s.keys by snapshot stripes, and call rangekey.Coalesce
	// on each partition.
	output.Reset()
	x, y := len(c.snapshots)-1, 0
	usedLen := 0
	for x >= 0 {
		start := y
		for y < len(span.Keys) && !base.Visible(span.Keys[y].SeqNum(), c.snapshots[x], base.SeqNumMax) {
			// Include y in current partition.
			y++
		}
		if y > start {
			keysDst := output.Keys[usedLen:cap(output.Keys)]
			rangekey.Coalesce(c.suffixCmp, span.Keys[start:y], &keysDst)
			if y == len(span.Keys) {
				// This is the last snapshot stripe. Unsets and deletes can be elided.
				keysDst = c.elideInLastStripe(span.Start, span.End, keysDst)
			}
			usedLen += len(keysDst)
			output.Keys = append(output.Keys, keysDst...)
		}
		x--
	}
	if y < len(span.Keys) {
		keysDst := output.Keys[usedLen:cap(output.Keys)]
		rangekey.Coalesce(c.suffixCmp, span.Keys[y:], &keysDst)
		keysDst = c.elideInLastStripe(span.Start, span.End, keysDst)
		usedLen += len(keysDst)
		output.Keys = append(output.Keys, keysDst...)
	}
	if len(output.Keys) > 0 {
		output.Start = append(output.Start, span.Start...)
		output.End = append(output.End, span.End...)
		output.KeysOrder = span.KeysOrder
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

// SplitAndEncodeSpan splits a span at upToKey and encodes the first part into
// the table writer, and updates the span to store the remaining part.
//
// If upToKey is nil or the span ends before upToKey, we encode the entire span
// and reset it to the empty span.
//
// Note that the span.Start slice will be reused (it will be replaced with a
// copy of upToKey, if appropriate).
//
// The span can contain either only RANGEDEL keys or only range keys.
func SplitAndEncodeSpan(
	cmp base.Compare, span *keyspan.Span, upToKey []byte, tw sstable.RawWriter,
) error {
	if span.Empty() {
		return nil
	}

	if upToKey == nil || cmp(span.End, upToKey) <= 0 {
		if err := tw.EncodeSpan(*span); err != nil {
			return err
		}
		span.Reset()
		return nil
	}

	if cmp(span.Start, upToKey) >= 0 {
		// The span starts at/after upToKey; nothing to encode.
		return nil
	}

	// Split the span at upToKey and encode the first part.
	if err := tw.EncodeSpan(keyspan.Span{
		Start: span.Start,
		End:   upToKey,
		Keys:  span.Keys,
	}); err != nil {
		return err
	}
	span.Start = append(span.Start[:0], upToKey...)
	return nil
}
