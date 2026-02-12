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
// usage of the key range [start, end), along with the estimated size of the
// left subrange [start, midKey). Callers can use the returned size to decide
// whether the bisection is worthwhile (e.g. skip splitting tiny ranges).
//
// The estimation uses only SST metadata and index blocks — no data blocks are
// read. The epsilon parameter controls precision: the returned key will bisect
// the range to within epsilon bytes. Larger values require less I/O; smaller
// values read more index blocks for a tighter result.
//
// The algorithm has three phases:
//
//  1. Determine total size. SSTs fully contained in [start, end) contribute
//     their exact size. Partially-overlapping edge SSTs have unknown overlap;
//     the largest are resolved by reading their index blocks until the total
//     size uncertainty is within 2×epsilon (since target = totalSize/2, this
//     ensures the target is known to within epsilon).
//
//  2. Walk SSTs by upper bound key, accumulating sizes. If the cumulative
//     total lands within epsilon of totalSize/2, the SST boundary where
//     this happens is a good-enough mid key — no further index I/O.
//
//  3. If any SST's full size would overshoot the target ± epsilon window, it
//     must be partially consumed, requiring its index block. Every such SST
//     provably requires an index read: if its size were ≤ epsilon we could
//     have added or skipped it entirely, so the set of indexes opened is
//     minimal. Block entries from all straddling SSTs are merge-walked in
//     key order, accumulating sizes until the deficit is reached.
//
// The returned key is a separator from an SST index block. It is ≥ every key
// in its data block but may not correspond to any key stored in the database.
//
// If the range cannot be bisected (e.g. no SSTs overlap it), midKey is nil
// and lhsSize is 0.
func (d *DB) ApproximateMidKey(
	ctx context.Context, start, end []byte, epsilon uint64,
) (midKey []byte, lhsSize uint64, _ error) {
	if err := d.closed.Load(); err != nil {
		panic(err)
	}
	if d.cmp(start, end) >= 0 {
		return nil, 0, errors.New("invalid key-range specified (start >= end)")
	}

	readState := d.loadReadState()
	defer readState.unref()

	v := readState.current
	bounds := base.UserKeyBoundsEndExclusive(start, end)

	// sstInfo tracks per-SST metadata for the mid-key computation.
	type sstInfo struct {
		meta       *manifest.TableMetadata
		upperBound []byte // min(m.Largest().UserKey, end)
		fullSize   uint64 // m.Size + m.EstimatedReferenceSize()
		contained  bool   // fully within [start, end)

		// effectiveSize is the best estimate of this SST's data within
		// [start, end). For contained SSTs it equals fullSize. For
		// partially-overlapping SSTs it is set by edge resolution; until
		// then it equals fullSize (an upper bound).
		effectiveSize uint64
		edgeResolved  bool
	}

	var ssts []sstInfo

	addFile := func(m *manifest.TableMetadata) {
		fullSize := m.Size + m.EstimatedReferenceSize()
		ub := m.Largest().UserKey
		if d.cmp(ub, end) >= 0 {
			ub = end
		}
		contained := m.ContainedWithinSpan(d.cmp, start, end)
		ssts = append(ssts, sstInfo{
			meta:          m,
			upperBound:    ub,
			fullSize:      fullSize,
			contained:     contained,
			effectiveSize: fullSize,
			edgeResolved:  contained, // contained SSTs are already "resolved"
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

	if len(ssts) == 0 {
		return nil, 0, nil
	}

	// Phase 1: Resolve edge SSTs.
	//
	// For partially-overlapping SSTs the actual data within [start, end) is
	// unknown without reading their index blocks. We resolve the largest
	// ones first — each read narrows the uncertainty on totalSize. We stop
	// once uncertainty ≤ 2*epsilon: since target = totalSize/2, halving the
	// uncertainty means the target is known to within epsilon. This reads
	// the minimum number of index blocks needed to know our bisection
	// target.
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
			overlapDataSize, err := d.fileCache.estimateSize(ssts[idx].meta, start, end)
			if err != nil {
				return nil, 0, err
			}
			var resolvedSize uint64
			if ssts[idx].meta.Size > 0 {
				frac := float64(overlapDataSize) / float64(ssts[idx].meta.Size)
				resolvedSize = overlapDataSize +
					uint64(float64(ssts[idx].meta.EstimatedReferenceSize())*frac)
			}
			ssts[idx].effectiveSize = resolvedSize
			ssts[idx].edgeResolved = true
			knownSize += resolvedSize
			uncertainty -= ssts[idx].fullSize
		}
	}

	// Best estimate of total size. Remaining uncertainty (from small
	// unresolved edge SSTs) is ≤ 2*epsilon; split the difference.
	totalSize := knownSize + uncertainty/2
	target := totalSize / 2

	// Phase 2: Walk SSTs by upper bound, accumulating effective sizes.
	//
	// Every SST added to cum has all its data at keys ≤ its upper bound, so
	// cum is a correct lower bound on data to the left of that key. We add
	// SSTs greedily until one of two outcomes:
	//
	//  (a) cum lands in [target-epsilon, target+epsilon] — the last added
	//      SST's upper bound is a good-enough mid key. No further I/O.
	//
	//  (b) The next SST would push cum past target+epsilon — it must be
	//      partially consumed, requiring index-level refinement (Phase 3).
	slices.SortFunc(ssts, func(a, b sstInfo) int {
		return d.cmp(a.upperBound, b.upperBound)
	})

	var cum uint64
	walkEnd := len(ssts) // index of first SST NOT added to cum

	for i := range ssts {
		sz := ssts[i].effectiveSize
		if cum+sz > target+epsilon {
			// This SST overshoots. Phase 3 needed.
			walkEnd = i
			break
		}
		cum += sz
		if cum >= target-epsilon && cum <= target+epsilon {
			// Within epsilon of target. Use this SST's upper bound.
			ub := ssts[i].upperBound
			if d.cmp(ub, start) <= 0 || d.cmp(ub, end) >= 0 {
				return nil, 0, nil
			}
			return slices.Clone(ub), cum, nil
		}
	}

	if walkEnd == len(ssts) {
		// This should not happen: target is derived from the SST sizes we
		// just walked, so the walk should always either land within epsilon
		// or overshoot. This guard handles any rounding edge cases.
		return nil, 0, nil
	}

	// Phase 3: Merge-walk straddling SSTs.
	//
	// Every SST from walkEnd onwards whose effective size > epsilon is a
	// "straddler" — adding its full size overshoots, and skipping it
	// exceeds the error budget. Its index block must be read. This is
	// provably minimal: if a file's size were ≤ epsilon we could have added
	// or skipped it entirely within budget, so we would never have reached
	// this point for that file.
	//
	// We collect block-level entries from each straddler's index and
	// merge-walk them in key order, accumulating sizes until the deficit
	// (target - cum) is reached. The merge is over a small number of
	// iterators (at most one per LSM level ≈ 7, plus L0 sublevels) and
	// requires no I/O beyond the already-read index blocks.
	deficit := target - cum

	type straddler struct {
		entries []sstable.BlockEntry
		pos     int
	}
	var straddlers []straddler

	for i := walkEnd; i < len(ssts); i++ {
		sz := ssts[i].effectiveSize
		if sz <= epsilon {
			// Small enough to absorb within the error budget — even if
			// all its data lands on one side, the error is ≤ epsilon.
			continue
		}
		entries, err := d.fileCache.collectBlockEntries(ctx, ssts[i].meta, start)
		if err != nil {
			return nil, 0, err
		}
		if len(entries) > 0 {
			straddlers = append(straddlers, straddler{entries: entries})
		}
	}

	if len(straddlers) == 0 {
		// No straddlers large enough to refine. Fall back to the
		// boundary at walkEnd if it's usable.
		ub := ssts[walkEnd].upperBound
		if d.cmp(ub, start) > 0 && d.cmp(ub, end) < 0 {
			return slices.Clone(ub), cum, nil
		}
		return nil, 0, nil
	}

	// Merge-walk: repeatedly advance the straddler with the smallest
	// current separator, accumulating block sizes. When cumulative size
	// crosses the deficit, the current separator is the mid key.
	var cumBlocks uint64
	for cumBlocks < deficit {
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
		cumBlocks += e.Size
		if cumBlocks >= deficit {
			midKey = e.Separator
			break
		}
		straddlers[best].pos++
	}

	if midKey == nil {
		// This should not happen: deficit is derived from the same SST
		// sizes whose block entries we're walking, so cumBlocks should
		// always reach deficit. This guard handles metric mismatches
		// between full SST sizes (which include metadata) and block
		// entry sizes (which exclude it).
		return nil, 0, nil
	}
	if d.cmp(midKey, start) <= 0 || d.cmp(midKey, end) >= 0 {
		// The merge walk found a separator outside the query range,
		// which shouldn't happen but could if block entries from a
		// straddler extend past [start, end) due to virtual SST bounds
		// or clipping edge cases.
		return nil, 0, nil
	}
	return slices.Clone(midKey), cum + cumBlocks, nil
}
