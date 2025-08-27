// Copyright 2024 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package compact

import (
	"strings"

	"github.com/cockroachdb/pebble/v2/internal/base"
	"github.com/cockroachdb/pebble/v2/internal/invariants"
	"github.com/cockroachdb/pebble/v2/internal/manifest"
)

// TombstoneElision is the information required to determine which tombstones
// (in the bottom snapshot stripe) can be elided. For example, when compacting
// into L6 (the lowest level), we can elide all tombstones (in the bottom
// snapshot stripe).
//
// TombstoneElision can indicate that no tombstones can be elided, or it can
// store a set of key ranges where only tombstones that do NOT overlap those key
// ranges can be elided.
//
// Note that the concept of "tombstone" applies to range keys as well:
// RangeKeyUnset and RangeKeyDelete are considered tombstones w.r.t other
// range keys and can use TombstoneElision.
type TombstoneElision struct {
	mode        tombstoneElisionMode
	inUseRanges []base.UserKeyBounds
}

type tombstoneElisionMode int8

const (
	elideNothing tombstoneElisionMode = iota
	elideNotInUse
)

// NoTombstoneElision is used when no tombstones can be elided (e.g. the entire
// compaction range is in use).
func NoTombstoneElision() TombstoneElision {
	return TombstoneElision{mode: elideNothing}
}

// ElideTombstonesOutsideOf is used when tombstones can be elided if they don't
// overlap with a set of "in use" key ranges. These ranges must be ordered and
// disjoint.
func ElideTombstonesOutsideOf(inUseRanges []base.UserKeyBounds) TombstoneElision {
	return TombstoneElision{
		mode:        elideNotInUse,
		inUseRanges: inUseRanges,
	}
}

// ElidesNothing returns true if no tombstones will be elided.
func (e TombstoneElision) ElidesNothing() bool {
	return e.mode == elideNothing
}

// ElidesEverything returns true if all tombstones (in the bottom snapshot
// stripe) can be elided.
func (e TombstoneElision) ElidesEverything() bool {
	return e.mode == elideNotInUse && len(e.inUseRanges) == 0
}

func (e TombstoneElision) String() string {
	switch {
	case e.ElidesNothing():
		return "elide nothing"
	case e.ElidesEverything():
		return "elide everything"
	default:
		var b strings.Builder
		for i, r := range e.inUseRanges {
			if i > 0 {
				b.WriteString(" ")
			}
			b.WriteString(r.String())
		}
		return b.String()
	}
}

// pointTombstoneElider is used to check if point tombstones (i.e. DEL/SINGLEDELs) can
// be elided.
type pointTombstoneElider struct {
	cmp     base.Compare
	elision TombstoneElision
	// inUseIdx is an index into elision.inUseRanges; it points to the first
	// range that ends after the last key passed to ShouldElide.
	inUseIdx int
}

func (te *pointTombstoneElider) Init(cmp base.Compare, elision TombstoneElision) {
	*te = pointTombstoneElider{
		cmp:     cmp,
		elision: elision,
	}
}

// ShouldElide returns true if a point tombstone with the given key can be
// elided. The keys in multiple invocations to ShouldElide must be supplied in
// order.
func (te *pointTombstoneElider) ShouldElide(key []byte) bool {
	if te.elision.ElidesNothing() {
		return false
	}

	inUseRanges := te.elision.inUseRanges
	if invariants.Enabled && te.inUseIdx > 0 && inUseRanges[te.inUseIdx-1].End.IsUpperBoundFor(te.cmp, key) {
		panic("ShouldElidePoint called with out-of-order key")
	}
	// Advance inUseIdx to the first in-use range that ends after key.
	for te.inUseIdx < len(te.elision.inUseRanges) && !inUseRanges[te.inUseIdx].End.IsUpperBoundFor(te.cmp, key) {
		te.inUseIdx++
	}
	// We can elide the point tombstone if this range starts after the key.
	return te.inUseIdx >= len(te.elision.inUseRanges) || te.cmp(inUseRanges[te.inUseIdx].Start, key) > 0
}

// rangeTombstoneElider is used to check if range tombstones can be elided.
//
// It can be used for RANGEDELs (in which case, the "in use" ranges reflect
// point keys); or for RANGEKEYUNSET, RANGEKEYDELETE, in which case the "in use"
// ranges reflect range keys.
type rangeTombstoneElider struct {
	cmp     base.Compare
	elision TombstoneElision
	// inUseIdx is an index into elision.inUseRanges; it points to the first
	// range that ends after the last start key passed to ShouldElide.
	inUseIdx int
}

func (te *rangeTombstoneElider) Init(cmp base.Compare, elision TombstoneElision) {
	*te = rangeTombstoneElider{
		cmp:     cmp,
		elision: elision,
	}
}

// ShouldElide returns true if the tombstone for the given end-exclusive range
// can be elided. The start keys in multiple invocations to ShouldElide must be
// supplied in order.
func (te *rangeTombstoneElider) ShouldElide(start, end []byte) bool {
	if te.elision.ElidesNothing() {
		return false
	}

	inUseRanges := te.elision.inUseRanges
	if invariants.Enabled && te.inUseIdx > 0 && inUseRanges[te.inUseIdx-1].End.IsUpperBoundFor(te.cmp, start) {
		panic("ShouldElideRange called with out-of-order key")
	}
	// Advance inUseIdx to the first in-use range that ends after start.
	for te.inUseIdx < len(te.elision.inUseRanges) && !inUseRanges[te.inUseIdx].End.IsUpperBoundFor(te.cmp, start) {
		te.inUseIdx++
	}
	// We can elide the range tombstone if this range starts after the tombstone ends.
	return te.inUseIdx >= len(te.elision.inUseRanges) || te.cmp(inUseRanges[te.inUseIdx].Start, end) >= 0
}

// SetupTombstoneElision calculates the TombstoneElision policies for a
// compaction operating on the given version and output level.
func SetupTombstoneElision(
	cmp base.Compare,
	v *manifest.Version,
	l0Organizer *manifest.L0Organizer,
	outputLevel int,
	compactionBounds base.UserKeyBounds,
) (dels, rangeKeys TombstoneElision) {
	// We want to calculate the in-use key ranges from the levels below our output
	// level, unless it is L0; L0 requires special treatment, since sstables
	// within L0 may overlap.
	startLevel := 0
	if outputLevel > 0 {
		startLevel = outputLevel + 1
	}
	// CalculateInuseKeyRanges will return a series of sorted spans. Overlapping
	// or abutting spans have already been merged.
	inUseKeyRanges := v.CalculateInuseKeyRanges(
		l0Organizer, startLevel, manifest.NumLevels-1, compactionBounds.Start, compactionBounds.End.Key,
	)
	// Check if there's a single in-use span that encompasses the entire key range
	// of the compaction. This is an optimization to avoid key comparisons against
	// the in-use ranges during the compaction when every key within the
	// compaction overlaps with an in-use span.
	if len(inUseKeyRanges) == 1 && inUseKeyRanges[0].ContainsBounds(cmp, &compactionBounds) {
		dels = NoTombstoneElision()
	} else {
		dels = ElideTombstonesOutsideOf(inUseKeyRanges)
	}
	// TODO(radu): we should calculate in-use ranges separately for point keys and for range keys.
	rangeKeys = dels
	return dels, rangeKeys
}
