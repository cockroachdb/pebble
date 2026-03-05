// Copyright 2026 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package pebble

import (
	stdcmp "cmp"
	"context"
	"slices"

	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/pebble/internal/base"
	"github.com/cockroachdb/pebble/internal/manifest"
	"github.com/cockroachdb/pebble/sstable"
)

// ApproximateMidKey returns a key that approximately bisects the estimated disk
// usage of the key range kr, along with the estimated size of the left
// subrange [kr.Start, midKey). Callers can use the returned size to decide
// whether the bisection is worthwhile (e.g. skip splitting tiny ranges).
//
// The estimation uses only SST metadata and index blocks — no data blocks are
// read. The epsilon parameter controls precision: the returned key will bisect
// the range to within epsilon bytes. Larger values require less I/O; smaller
// values read more index blocks for a tighter result.
//
// The returned key is a separator from an SST index block. It is ≥ every key
// in its data block but may not correspond to any key stored in the database.
//
// If the range cannot be bisected (e.g. no SSTs overlap it or the range is too
// small relative to epsilon), midKey is nil and lhsSize is 0.
func (d *DB) ApproximateMidKey(
	ctx context.Context, kr KeyRange, epsilon uint64,
) (midKey []byte, lhsSize uint64, _ error) {
	if err := d.closed.Load(); err != nil {
		panic(err)
	}
	if d.cmp(kr.Start, kr.End) >= 0 {
		return nil, 0, errors.New("invalid key-range specified (start >= end)")
	}

	readState := d.loadReadState()
	defer readState.unref()

	ssts := d.collectMidKeySSTs(readState.current, kr)
	if len(ssts) == 0 {
		return nil, 0, nil
	}

	totalSize, err := d.resolveEdgeSizes(ssts, kr, epsilon)
	if err != nil {
		return nil, 0, err
	}
	if totalSize <= 2*epsilon {
		// The range is too small relative to the error bar for a
		// meaningful bisection.
		return nil, 0, nil
	}

	return d.findMidKey(ctx, ssts, kr, totalSize/2, epsilon)
}

// midKeySST tracks per-SST metadata for the mid-key computation.
type midKeySST struct {
	meta       *manifest.TableMetadata
	upperBound []byte // min(meta.Largest().UserKey, kr.End)
	fullSize   uint64 // meta.Size + meta.EstimatedReferenceSize()
	contained  bool   // fully within kr

	// effectiveSize is the best estimate of this SST's data within kr.
	// For contained SSTs it equals fullSize. For partially-overlapping
	// SSTs it may be reduced by resolveEdgeSizes; until then it equals
	// fullSize (an upper bound).
	effectiveSize uint64
}

// collectMidKeySSTs returns metadata for all SSTs that overlap kr.
func (d *DB) collectMidKeySSTs(v *manifest.Version, kr KeyRange) []midKeySST {
	bounds := base.UserKeyBoundsEndExclusive(kr.Start, kr.End)
	var ssts []midKeySST

	addFile := func(m *manifest.TableMetadata) {
		fullSize := m.Size + m.EstimatedReferenceSize()
		ub := m.Largest().UserKey
		if d.cmp(ub, kr.End) >= 0 {
			ub = kr.End
		}
		ssts = append(ssts, midKeySST{
			meta:          m,
			upperBound:    ub,
			fullSize:      fullSize,
			contained:     m.ContainedWithinSpan(d.cmp, kr.Start, kr.End),
			effectiveSize: fullSize,
		})
	}

	// L0: iterate sublevels individually to avoid the transitive bound
	// expansion that Version.Overlaps performs for level 0.
	for _, ls := range v.L0SublevelFiles {
		for m := range ls.All() {
			if m.Overlaps(d.cmp, &bounds) {
				addFile(m)
			}
		}
	}
	for level := 1; level < manifest.NumLevels; level++ {
		for m := range v.Overlaps(level, bounds).All() {
			addFile(m)
		}
	}
	return ssts
}

// resolveEdgeSizes reads index blocks for partially-overlapping SSTs to
// narrow uncertainty on the total size. It resolves the largest edge SSTs
// first, stopping once uncertainty is within 2*epsilon. Returns the best
// estimate of total data size within kr.
func (d *DB) resolveEdgeSizes(ssts []midKeySST, kr KeyRange, epsilon uint64) (uint64, error) {
	var knownSize uint64      // sum of resolved SST sizes
	var uncertainty uint64    // sum of fullSize for unresolved partial SSTs
	var unresolvedEdges []int // indices into ssts

	for i := range ssts {
		if ssts[i].contained {
			knownSize += ssts[i].fullSize
		} else {
			uncertainty += ssts[i].fullSize
			unresolvedEdges = append(unresolvedEdges, i)
		}
	}

	if uncertainty > 2*epsilon {
		// Sort unresolved edges by size descending so we resolve the
		// largest contributors to uncertainty first.
		slices.SortFunc(unresolvedEdges, func(a, b int) int {
			return stdcmp.Compare(ssts[b].fullSize, ssts[a].fullSize)
		})
		for _, idx := range unresolvedEdges {
			if uncertainty <= 2*epsilon {
				break
			}
			overlapSize, err := d.fileCache.estimateSize(
				ssts[idx].meta, kr.Start, kr.End,
			)
			if err != nil {
				return 0, err
			}
			var resolvedSize uint64
			if ssts[idx].meta.Size > 0 {
				resolvedSize = uint64(float64(overlapSize) *
					float64(ssts[idx].fullSize) / float64(ssts[idx].meta.Size))
			}
			ssts[idx].effectiveSize = resolvedSize
			knownSize += resolvedSize
			uncertainty -= ssts[idx].fullSize
		}
	}

	// Best estimate. Remaining uncertainty is ≤ 2*epsilon; split the
	// difference.
	return knownSize + uncertainty/2, nil
}

// findMidKey finds a key that bisects ssts to within epsilon of target. It
// sorts SSTs by upper-bound key and walks them, accumulating effective sizes.
// If an SST boundary lands within epsilon of the target, it is returned
// directly. Otherwise, SSTs that straddle the target are refined at block
// granularity via mergeWalkStraddlers.
func (d *DB) findMidKey(
	ctx context.Context, ssts []midKeySST, kr KeyRange, target, epsilon uint64,
) ([]byte, uint64, error) {
	slices.SortFunc(ssts, func(a, b midKeySST) int {
		return d.cmp(a.upperBound, b.upperBound)
	})

	var sum uint64
	for i := range ssts {
		sz := ssts[i].effectiveSize
		if sum+sz > target+epsilon {
			// This SST overshoots. Refine at block granularity.
			midKey, blockSum, err := d.mergeWalkStraddlers(
				ctx, ssts[i:], kr, target-sum, epsilon,
			)
			if err != nil || midKey == nil {
				return nil, 0, err
			}
			return midKey, sum + blockSum, nil
		}
		sum += sz
		if sum >= target-epsilon {
			// Within epsilon of target. Use this SST's upper bound.
			ub := ssts[i].upperBound
			if d.cmp(ub, kr.Start) <= 0 || d.cmp(ub, kr.End) >= 0 {
				return nil, 0, nil
			}
			return slices.Clone(ub), sum, nil
		}
	}

	// All SSTs consumed without reaching the target. This shouldn't happen
	// (target is derived from the same SST sizes), but can due to rounding.
	return nil, 0, nil
}

// mergeWalkStraddlers finds the mid key by reading index blocks from SSTs that
// individually overshoot the target window. SSTs with effective size ≤ epsilon
// are skipped (they can be absorbed within the error budget). The remaining
// SSTs' index blocks are read and their block entries are merged in key order,
// accumulating sizes until the deficit is reached.
func (d *DB) mergeWalkStraddlers(
	ctx context.Context, ssts []midKeySST, kr KeyRange, deficit, epsilon uint64,
) ([]byte, uint64, error) {
	type straddler struct {
		entries []sstable.BlockEntry
		pos     int
	}
	var straddlers []straddler

	for i := range ssts {
		if ssts[i].effectiveSize <= epsilon {
			continue
		}
		entries, err := d.fileCache.collectBlockEntries(
			ctx, ssts[i].meta, kr.Start, kr.End,
		)
		if err != nil {
			return nil, 0, err
		}
		if len(entries) > 0 {
			straddlers = append(straddlers, straddler{entries: entries})
		}
	}

	if len(straddlers) == 0 {
		// No straddlers large enough to refine. Fall back to the first
		// SST's upper bound if it's usable.
		ub := ssts[0].upperBound
		if d.cmp(ub, kr.Start) > 0 && d.cmp(ub, kr.End) < 0 {
			return slices.Clone(ub), 0, nil
		}
		return nil, 0, nil
	}

	// Merge-walk: repeatedly advance the straddler with the smallest
	// current separator, accumulating block sizes until the deficit is
	// reached.
	var sum uint64
	for sum < deficit {
		best := -1
		for i := range straddlers {
			if straddlers[i].pos >= len(straddlers[i].entries) {
				continue
			}
			if best == -1 || d.cmp(
				straddlers[i].entries[straddlers[i].pos].Separator,
				straddlers[best].entries[straddlers[best].pos].Separator,
			) < 0 {
				best = i
			}
		}
		if best == -1 {
			break
		}

		e := &straddlers[best].entries[straddlers[best].pos]
		sum += e.Size
		if sum >= deficit {
			sep := e.Separator
			if d.cmp(sep, kr.Start) <= 0 || d.cmp(sep, kr.End) >= 0 {
				return nil, 0, nil
			}
			return slices.Clone(sep), sum, nil
		}
		straddlers[best].pos++
	}

	return nil, 0, nil
}
