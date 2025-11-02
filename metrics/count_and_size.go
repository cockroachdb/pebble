// Copyright 2025 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package metrics

import (
	"github.com/cockroachdb/crlib/crhumanize"
	"github.com/cockroachdb/pebble/internal/invariants"
	"github.com/cockroachdb/redact"
)

// CountAndSize tracks the count and total size of a set of items.
type CountAndSize struct {
	// Count is the number of files.
	Count uint64

	// Bytes is the total size of all files.
	Bytes uint64
}

// Inc increases the count and size for a single item.
func (cs *CountAndSize) Inc(fileSize uint64) {
	cs.Count++
	cs.Bytes += fileSize
}

// Dec decreases the count and size for a single item.
func (cs *CountAndSize) Dec(fileSize uint64) {
	cs.Count = invariants.SafeSub(cs.Count, 1)
	cs.Bytes = invariants.SafeSub(cs.Bytes, fileSize)
}

// Accumulate increases the counts and sizes by the given amounts.
func (cs *CountAndSize) Accumulate(other CountAndSize) {
	cs.Count += other.Count
	cs.Bytes += other.Bytes
}

// Deduct decreases the counts and sizes by the given amounts.
func (cs *CountAndSize) Deduct(other CountAndSize) {
	cs.Count = invariants.SafeSub(cs.Count, other.Count)
	cs.Bytes = invariants.SafeSub(cs.Bytes, other.Bytes)
}

func (cs CountAndSize) IsZero() bool {
	return cs.Count == 0 && cs.Bytes == 0
}

// Sum returns the sums of the counts and sizes.
func (cs CountAndSize) Sum(other CountAndSize) CountAndSize {
	return CountAndSize{
		Count: cs.Count + other.Count,
		Bytes: cs.Bytes + other.Bytes,
	}
}

func (cs CountAndSize) String() string {
	return redact.StringWithoutMarkers(cs)
}

// SafeFormat implements redact.SafeFormatter.
func (cs CountAndSize) SafeFormat(w redact.SafePrinter, verb rune) {
	w.Printf("%s (%s)", crhumanize.Count(cs.Count, crhumanize.Compact), crhumanize.Bytes(cs.Bytes, crhumanize.Compact, crhumanize.OmitI))
}
