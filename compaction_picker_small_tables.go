// Copyright 2026 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package pebble

import (
	"slices"

	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/pebble/internal/base"
	"github.com/cockroachdb/pebble/internal/invariants"
	"github.com/cockroachdb/pebble/internal/manifest"
	"github.com/cockroachdb/pebble/internal/problemspans"
)

// Tuning constants for small-table compactions; see pickSmallTableCompaction.
const (
	// smallTableCompactionMaxFiles bounds the number of input tables in a
	// small-table compaction (and thus the number of concurrently open readers
	// when the tables are rewritten).
	smallTableCompactionMaxFiles = 100
	// smallTableCompactionByteCapFraction is the maximum total size of a
	// consolidated run, as a fraction of the level's target file size.
	smallTableCompactionByteCapFraction = 1.0
)

// pickSmallTableCompaction looks for a run of adjacent small tables within a
// level (Lbase to L6) that can either be moved down a level as-is, or
// consolidated into a single table without touching the level below.
//
// Workloads that repeatedly flush or compact small amounts of data into key
// regions that don't overlap existing data can accumulate an unbounded number
// of small tables: score-based compactions only trigger on level size, and move
// compactions perpetuate small tables down the LSM. Consolidating the tables
// within their level bounds the file count.
//
// A run qualifies if it consists of at least K adjacent tables (K is
// Options.SmallTableCompactionMinRunLength) that satisfy:
//
//   - a byte cap: the total estimated size of the run is at most the level's
//     target file size;
//   - a size-tiered condition: the largest table in the run is at most half of
//     the run's total size. Without it, a table produced by consolidation
//     would be rewritten every time K-1 new small tables landed next to it,
//     until it reached the target file size, for a write amplification
//     proportional to the target file size divided by the size of the small
//     tables. With it, every rewrite at least doubles the table's size, so each
//     byte is rewritten a logarithmic number of times, at the cost of a
//     logarithmic (rather than constant) number of tables per key region;
//   - a single region: the run's bounds contain no boundary of a table in the
//     next level and no span policy region boundary. Small tables that
//     straddle a next-level table boundary are the deliberate product of the
//     output splitting heuristics (each is destined to merge with a distinct
//     table underneath), and consolidating them would be undone by the next
//     compaction; the writer settings (compression, value separation) are
//     chosen per span policy region.
//
// A qualifying run that overlaps no table in the next level is moved down a
// level rather than consolidated: consolidating it could produce a table that
// straddles boundaries of tables further down, which the output splitting
// heuristics deliberately avoid, and nothing else would move the tables down
// while the level's score stays low. Repeated moves bring the run down to the
// level where it lies within a single next-level table (and is consolidated),
// straddles a next-level table boundary (and is left alone; its tables are
// expected to be merged separately into the tables underneath), or to L6.
//
// Otherwise the run is consolidated. The conditions above guarantee that the
// merged table is one the output splitting heuristics would produce anyway, so
// the compaction disables output splitting and always produces exactly one
// table (see newCompaction).
//
// Levels are scanned bottom-up and the scan stops at the first level with a
// qualifying run: consolidating a level enlarges the regions available to the
// level above. Within a level, the longest qualifying run is chosen.
//
// The scan of a level does the bulk of its work only around tables that are
// small enough to be part of a run (see findSmallTableRun). A level is skipped
// altogether if its smallest table (a cached B-Tree annotation) is too large
// for any run to fit under the byte cap: moving tables down is only a means to
// consolidate them eventually, so a level without small tables is left alone.
// A conclusive scan that finds nothing is not repeated for the same version.
//
// At most one small-table compaction runs at a time, and it is the lowest
// priority compaction kind.
func (p *compactionPickerByScore) pickSmallTableCompaction(
	env compactionEnv,
) (pc *pickedTableCompaction) {
	minRunLength := p.opts.SmallTableCompactionMinRunLength()
	if minRunLength <= 0 {
		// Small-table compactions are disabled.
		return nil
	}
	// Very short runs are not worth a compaction (and a run of one table would
	// be rewritten into itself).
	minRunLength = max(minRunLength, 3)

	for _, c := range env.inProgressCompactions {
		if c.kind == compactionKindSmallTables {
			return nil
		}
	}
	if p.noSmallTableRunsForK == minRunLength {
		return nil
	}
	// If we don't find a run below, remember that so that we don't repeat the
	// scan for this version (and this K), unless some table was excluded because
	// it is compacting or overlaps a problem span. Those exclusions can be
	// lifted without a new version being installed (a compaction can fail, a
	// problem span expires by wall-clock time), so a scan that ran into them
	// must be repeated.
	conclusive := true
	defer func() {
		if pc == nil && conclusive {
			p.noSmallTableRunsForK = minRunLength
		}
	}()

	cmp := p.opts.Comparer.Compare
	var helper findSmallTableRunHelper
	for level := numLevels - 1; level >= p.baseLevel; level-- {
		if p.vers.Levels[level].Empty() {
			continue
		}
		byteCap := uint64(smallTableCompactionByteCapFraction * float64(p.opts.TargetFileSize(level, p.baseLevel)))
		// A run of at least minRunLength tables with a total size of at most
		// byteCap must contain a table of size at most byteCap/minRunLength. A
		// level without such a table has nothing to consolidate, and we don't
		// bother moving its tables down either.
		if smallest := smallestTableSizeAnnotator.LevelAnnotation(p.vers.Levels[level]); !smallest.set ||
			smallest.size > byteCap/uint64(minRunLength) {
			continue
		}
		var nextLevel manifest.LevelIterator
		if level < numLevels-1 {
			nextLevel = p.vers.Levels[level+1].Iter()
		}
		run, err := helper.findSmallTableRun(cmp,
			p.opts.Experimental.SpanPolicyFunc,
			p.vers.Levels[level].Iter(), nextLevel,
			level, env.problemSpans, minRunLength, byteCap)
		if err != nil {
			// A failing SpanPolicyFunc would also fail any compaction that we
			// pick; there is nothing useful to do now, but the failure may be
			// transient.
			conclusive = false
			return nil
		}
		conclusive = conclusive && !run.sawUnavailable
		if run.tables == nil {
			continue
		}

		outputLevel := level
		if run.nothingUnderneath && level < numLevels-1 {
			outputLevel = level + 1
		}
		pc = newPickedTableCompaction(p.opts, p.vers, p.latestVersionState.l0Organizer,
			level, outputLevel, p.baseLevel)
		pc.kind = compactionKindSmallTables
		pc.startLevel.files = p.vers.Levels[level].Find(cmp, run.tables[0]).Reslice(
			func(start, end *manifest.LevelIterator) {
				for i := 1; i < len(run.tables); i++ {
					end.Next()
				}
			})
		if invariants.Enabled && pc.startLevel.files.Len() != len(run.tables) {
			panic(errors.AssertionFailedf("small-table run has %d tables, slice has %d",
				len(run.tables), pc.startLevel.files.Len()))
		}
		// The run's tables are available for compaction, but setupInputs can
		// still reject the run when consolidating it: an in-progress compaction
		// from the level above whose input lies in a gap between the run's
		// tables involves none of them, yet outputs into the run's key range. In
		// that case we move on to the level above.
		if pc.setupInputs(p.opts, env.diskAvailBytes, env.inProgressCompactions, pc.startLevel, env.problemSpans) {
			return pc
		}
		pc = nil
		conclusive = false
	}
	return nil
}

// findSmallTableRunHelper holds the buffers used by findSmallTableRun and
// findSmallTableRunInRegion, so that they can be reused across calls. The zero
// value is ready to use.
type findSmallTableRunHelper struct {
	// tables holds the tables gathered around the small table being examined by
	// findSmallTableRun.
	tables []*manifest.TableMetadata
	// prefixSum and maxima are scratch space for findSmallTableRunInRegion.
	prefixSum []uint64
	maxima    []int
}

// smallTableRun is the result of findSmallTableRun.
type smallTableRun struct {
	// tables is the run of adjacent tables to compact, or nil if there is none.
	tables []*manifest.TableMetadata
	// nothingUnderneath is true if the run overlaps no table in the next level
	// (so it can be moved down a level as-is).
	nothingUnderneath bool
	// sawUnavailable is true if a run was rejected because one of its tables is
	// not available for compaction (it is compacting or overlaps a problem
	// span).
	sawUnavailable bool
}

// findSmallTableRun scans a level and returns the best run of adjacent tables
// for a small-table compaction: the longest run that qualifies (see
// findSmallTableRunInRegion), with ties broken by the smallest total size and
// then by position (earliest wins). A run must lie within a single region: a
// stretch of the keyspace that contains no boundary of a next-level table and
// no span policy region boundary. With respect to the next level, a region
// lies within a single next-level table or in a gap between next-level tables
// (or spans the whole level, if nextLevel is the zero iterator).
//
// A run of at least minRunLength tables totaling at most byteCap contains a
// table of size at most byteCap/minRunLength. The scan visits these small
// tables in key order and does the bulk of its work only around them:
//
//  1. Using table sizes alone, it checks whether some window of minRunLength
//     tables around the small table fits under byteCap. If not, the table
//     cannot be part of a run and the scan moves on to the next small table.
//  2. It determines the region of the small table by seeking in the next level
//     and calling spanPolicyFunc (unless the last policy retrieved still
//     covers the table). If the table straddles a region boundary, the scan
//     moves on.
//  3. It gathers the tables of the region around the small table: at most
//     minRunLength-1 tables before it, since all tables between the previous
//     small table visited and this one are larger than byteCap/minRunLength
//     and a run has fewer than minRunLength such tables; and all the tables
//     after it, up to the region boundary. A table of at least byteCap can be
//     part of no run and ends the gathering. The best run among the gathered
//     tables is a candidate, and the scan resumes past them.
//
// A candidate run that includes a table which is not available for compaction
// is ignored and sawUnavailable is set; the region is reassessed once the
// relevant compaction finishes or the problem span expires.
//
// If spanPolicyFunc returns an error, the error is returned.
func (h *findSmallTableRunHelper) findSmallTableRun(
	cmp base.Compare,
	spanPolicyFunc SpanPolicyFunc,
	level, nextLevel manifest.LevelIterator,
	levelNum int,
	problemSpans *problemspans.ByLevel,
	minRunLength int,
	byteCap uint64,
) (smallTableRun, error) {
	var res smallTableRun
	var resSize uint64
	available := func(tables []*manifest.TableMetadata) bool {
		for _, t := range tables {
			if t.CompactionState != manifest.CompactionStateNotCompacting ||
				(problemSpans != nil && problemSpans.Overlaps(levelNum, t.UserKeyBounds())) {
				return false
			}
		}
		return true
	}
	smallSize := byteCap / uint64(minRunLength)
	// policy is the last span policy retrieved, if havePolicy is set.
	var policy SpanPolicy
	var havePolicy bool
	// lastExamined is the last table that was gathered or skipped so far; no
	// run that includes a table after it can include it.
	var lastExamined *manifest.TableMetadata
	tables := h.tables[:0]
	defer func() { h.tables = tables[:0] }()
	iter := level
	iter.First()
	for {
		s := findSmallTable(&iter, smallSize)
		if s == nil {
			return res, nil
		}
		tables = tables[:0]
		// Gather up to minRunLength-1 tables on each side of s, without looking
		// past a table that can be part of no run, and check that some window of
		// minRunLength tables fits under the byte cap. Any such window includes
		// s, since there are fewer than minRunLength tables on each side of it.
		for it := iter.Clone(); len(tables) < minRunLength-1; {
			t := it.Prev()
			if t == nil || t == lastExamined || t.EstimatedDataSize() >= byteCap {
				break
			}
			tables = append(tables, t)
		}
		slices.Reverse(tables)
		sIdx := len(tables)
		tables = append(tables, s)
		for it := iter.Clone(); len(tables)-sIdx < minRunLength; {
			t := it.Next()
			if t == nil || t.EstimatedDataSize() >= byteCap {
				break
			}
			tables = append(tables, t)
		}
		if !hasWindowUnderCap(tables, minRunLength, byteCap) {
			lastExamined = s
			iter.Next()
			continue
		}

		// Determine the region of s.
		region, nothingUnderneath := nextLevelRegion(cmp, &nextLevel, s)
		if !region.contains(cmp, s) {
			// s straddles a next-level table boundary.
			lastExamined = s
			iter.Next()
			continue
		}
		sBounds := s.UserKeyBounds()
		if !havePolicy ||
			(len(policy.KeyRange.Start) > 0 && cmp(sBounds.Start, policy.KeyRange.Start) < 0) ||
			(len(policy.KeyRange.End) > 0 && cmp(sBounds.Start, policy.KeyRange.End) >= 0) {
			var err error
			if policy, err = spanPolicyFunc(sBounds); err != nil {
				return smallTableRun{}, err
			}
			havePolicy = true
		}
		if len(policy.KeyRange.Start) > 0 {
			region.restrictLower(cmp, base.UserKeyExclusive(policy.KeyRange.Start))
		}
		if len(policy.KeyRange.End) > 0 {
			region.restrictUpper(cmp, base.UserKeyExclusive(policy.KeyRange.End))
		}
		if !region.contains(cmp, s) {
			// s straddles a span policy region boundary.
			lastExamined = s
			iter.Next()
			continue
		}

		// Gather the tables of the region: those before s that lie within it,
		// then all those after s up to the region boundary. This leaves iter
		// positioned on the first table past the region (or exhausted).
		start := sIdx
		for start > 0 && region.contains(cmp, tables[start-1]) {
			start--
		}
		tables = tables[:copy(tables, tables[start:sIdx+1])]
		for {
			t := iter.Next()
			if t == nil || t.EstimatedDataSize() >= byteCap || !region.contains(cmp, t) {
				break
			}
			tables = append(tables, t)
		}
		lastExamined = tables[len(tables)-1]

		run := h.findSmallTableRunInRegion(tables, minRunLength, byteCap)
		if run == nil {
			continue
		}
		var size uint64
		for _, t := range run {
			size += t.EstimatedDataSize()
		}
		if len(run) > len(res.tables) || (len(run) == len(res.tables) && size < resSize) {
			if !available(run) {
				res.sawUnavailable = true
				continue
			}
			res.tables = append(res.tables[:0], run...)
			res.nothingUnderneath = nothingUnderneath
			resSize = size
		}
	}
}

// findSmallTable advances iter to the first table, starting with the one it is
// positioned on, whose estimated size is at most maxSize, and returns it. It
// returns nil if there is no such table, leaving iter exhausted.
func findSmallTable(iter *manifest.LevelIterator, maxSize uint64) *manifest.TableMetadata {
	// TODO(radu): use smallestTableSizeAnnotator to skip the B-Tree subtrees
	// whose smallest table is larger than maxSize, finding the next small table
	// in O(log N) rather than O(N).
	for t := iter.Current(); t != nil; t = iter.Next() {
		if t.EstimatedDataSize() <= maxSize {
			return t
		}
	}
	return nil
}

// hasWindowUnderCap returns whether some window of k adjacent tables has a
// total estimated size of at most byteCap.
func hasWindowUnderCap(tables []*manifest.TableMetadata, k int, byteCap uint64) bool {
	if len(tables) < k {
		return false
	}
	var sum uint64
	for i := 0; i < k; i++ {
		sum += tables[i].EstimatedDataSize()
	}
	if sum <= byteCap {
		return true
	}
	for i := k; i < len(tables); i++ {
		sum -= tables[i-k].EstimatedDataSize()
		sum += tables[i].EstimatedDataSize()
		// sum is the total size of tables[i-k+1..i].
		if sum <= byteCap {
			return true
		}
	}
	return false
}

// regionBounds delimits a stretch of the keyspace within which tables can be
// part of the same small-table run. The zero value is the whole keyspace.
type regionBounds struct {
	// lower, if hasLower is set, is a boundary that the region excludes: a
	// table lies within the region only if lower is not an upper bound for the
	// table's start key.
	lower    base.UserKeyBoundary
	hasLower bool
	// upper, if hasUpper is set, is the end boundary of the region: a table lies
	// within the region only if the table's end boundary is at most upper.
	upper    base.UserKeyBoundary
	hasUpper bool
}

// contains returns whether t lies entirely within the region.
func (r *regionBounds) contains(cmp base.Compare, t *manifest.TableMetadata) bool {
	b := t.UserKeyBounds()
	return (!r.hasLower || !r.lower.IsUpperBoundFor(cmp, b.Start)) &&
		(!r.hasUpper || b.End.CompareUpperBounds(cmp, r.upper) <= 0)
}

// restrictLower excludes from the region the keys up to boundary b (inclusive
// or exclusive of b.Key, per its kind), if the region does not exclude them
// already.
func (r *regionBounds) restrictLower(cmp base.Compare, b base.UserKeyBoundary) {
	// Both boundaries are upper bounds of the excluded keyspace, so they compare
	// as such.
	if !r.hasLower || b.CompareUpperBounds(cmp, r.lower) > 0 {
		r.lower, r.hasLower = b, true
	}
}

// restrictUpper excludes from the region the keys past boundary b, if the
// region does not exclude them already.
func (r *regionBounds) restrictUpper(cmp base.Compare, b base.UserKeyBoundary) {
	if !r.hasUpper || b.CompareUpperBounds(cmp, r.upper) < 0 {
		r.upper, r.hasUpper = b, true
	}
}

// nextLevelRegion returns the bounds of the region of t with respect to the
// next level: the bounds of the next-level table that t overlaps, if there is
// one (in which case t straddles a boundary of that table if it does not lie
// within the bounds), or otherwise the gap between the next-level tables that
// surround t, with nothingUnderneath set. If nextLevel is the zero iterator,
// the region is unbounded.
//
// nextLevel is repositioned.
func nextLevelRegion(
	cmp base.Compare, nextLevel *manifest.LevelIterator, t *manifest.TableMetadata,
) (region regionBounds, nothingUnderneath bool) {
	tBounds := t.UserKeyBounds()
	// below is the first next-level table that ends at or after t's start.
	below := nextLevel.SeekGE(cmp, tBounds.Start)
	var prev *manifest.TableMetadata
	if below != nil {
		belowBounds := below.UserKeyBounds()
		if tBounds.End.IsUpperBoundFor(cmp, belowBounds.Start) {
			// t overlaps below.
			return regionBounds{
				lower: base.UserKeyExclusive(belowBounds.Start), hasLower: true,
				upper: belowBounds.End, hasUpper: true,
			}, false
		}
		region.upper, region.hasUpper = base.UserKeyExclusive(belowBounds.Start), true
		prev = nextLevel.Prev()
	} else {
		prev = nextLevel.Last()
	}
	// prev is the last next-level table that ends before t's start, if any.
	if prev != nil {
		region.lower, region.hasLower = prev.UserKeyBounds().End, true
	}
	return region, true
}

// findSmallTableRunInRegion returns the best window of adjacent tables in the
// given region that qualifies for consolidation, or nil if there is none. A
// window qualifies if it has at least minRunLength and at most
// smallTableCompactionMaxFiles tables, its total estimated size is at most
// byteCap, and its largest table is at most half of its total size. The
// longest qualifying window wins, with ties broken by the smallest total size
// and then by position (earliest wins).
//
// The result is a sub-slice of tables.
func (h *findSmallTableRunHelper) findSmallTableRunInRegion(
	tables []*manifest.TableMetadata, minRunLength int, byteCap uint64,
) []*manifest.TableMetadata {
	if len(tables) < minRunLength {
		return nil
	}
	// prefixSum[k] is the total estimated size of tables[0..k].
	if cap(h.prefixSum) < len(tables) {
		h.prefixSum = make([]uint64, len(tables))
	}
	prefixSum := h.prefixSum[:len(tables)]
	for k, t := range tables {
		prefixSum[k] = t.EstimatedDataSize()
		if k > 0 {
			prefixSum[k] += prefixSum[k-1]
		}
	}
	// sum returns the total estimated size of tables[a..b].
	sum := func(a, b int) uint64 {
		if a == 0 {
			return prefixSum[b]
		}
		return prefixSum[b] - prefixSum[a-1]
	}
	sizeOf := func(k int) uint64 { return tables[k].EstimatedDataSize() }

	bestStart, bestEnd := -1, -1
	var bestSize uint64
	// For each window end j, we maintain the smallest start i such that the
	// window [i..j] satisfies the byte and file caps, and the successive maxima
	// of the window's suffixes: maxima[0] is the index of the largest table in
	// [i..j], maxima[1] the index of the largest table after it, and so on.
	i := 0
	// Each table is pushed onto maxima at most once and popping from the front
	// advances the slice start, so a capacity of len(tables) suffices for the
	// whole scan without reallocation.
	if cap(h.maxima) < len(tables) {
		h.maxima = make([]int, 0, len(tables))
	}
	maxima := h.maxima[:0]
	for j := range tables {
		// Tables no larger than tables[j] cannot be the largest table of any
		// suffix that includes tables[j].
		for len(maxima) > 0 && sizeOf(maxima[len(maxima)-1]) <= sizeOf(j) {
			maxima = maxima[:len(maxima)-1]
		}
		maxima = append(maxima, j)
		for sum(i, j) > byteCap || j-i+1 > smallTableCompactionMaxFiles {
			i++
		}
		if i > j {
			// tables[j] by itself exceeds the byte cap; it cannot be part of any
			// window, and neither can a window span it.
			i = j + 1
			maxima = maxima[:0]
			continue
		}
		for maxima[0] < i {
			maxima = maxima[1:]
		}
		// Find the longest suffix [s..j] whose largest table is at most half of
		// its total size: a suffix that includes a table larger than that cannot
		// qualify, and neither can any longer suffix (which has the same largest
		// table).
		s := i
		for _, m := range maxima {
			if 2*sizeOf(m) <= sum(s, j) {
				break
			}
			s = m + 1
		}
		if n := j - s + 1; n >= minRunLength &&
			(n > bestEnd-bestStart+1 || (n == bestEnd-bestStart+1 && sum(s, j) < bestSize)) {
			bestStart, bestEnd, bestSize = s, j, sum(s, j)
		}
	}
	if bestStart < 0 {
		return nil
	}
	return tables[bestStart : bestEnd+1]
}
