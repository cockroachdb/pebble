// Copyright 2025 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

// Package tombspan implements tracking of ranged tombstones (RANGEDELs,
// RANGEKEYDELs) for the purpose of efficiently handling bulk deletions.
//
// # Ranged tombstones
//
// Pebble has two key types: point keys that are defined at a singular user key,
// and range keys that are defined over a span [start,end) of user key space.
// Pebble defines a separate ranged tombstone key kind for each.
//
// A RANGEDEL ("range deletion") tombstone deletes all point keys within a span
// [start,end). Like all keys in Pebble, a RANGEDEL tombstone has a sequence
// number that dictates its ordering within the history of writes. A range
// deletion tombstone applies to all point keys contained within the [start,end)
// bounds with sequence numbers less than the tombstone's sequence number.
//
// A RANGEKEYDEL ("range key deletion") tombstone deletes all range keys defined
// within a span [start,end). A "range key deletion" tombstone applies to all
// range keys defined within the [start,end) span with lesser sequence numbers.
// If a range key extends beyond the [start,end) span, it's truncated or split
// such that the range key is no longer defined within the bounds of the
// tombstone.
//
// ## Disk space reclamation
//
// Pebble reclaims disk space of keys deleted by ranged tombstones during
// asynchronous compactions.
//
// Regular default compactions read sstables from two or more levels, merge
// them, and output new sstables. If input sstables contain ranged tombstones,
// regular compactions read them and apply them, eliding keys that are deleted
// by the tombstones. Only ranged tombstones that are contained within the input
// sstables are applied. The ranged tombstones contained in other sstables
// higher in the LSM are not considered (see #1961).
//
// This regular default compaction process thus requires a ranged tombstone to
// be compacted all the way down the LSM to reclaim all the available disk
// space. This results in slow, asynchronous disk space reclamation and incurs
// write amplification (sstables may contain both live and deleted data,
// requring rewriting the live data). This is okay for narrow ranged tombstones
// deleting tens or hundreds of keys.
//
// However range deletions may be used to efficiently bulk delete wide swaths of
// the keyspace. In CockroachDB this may happen when an entire table or index is
// dropped, or a replica is removed from a node for rebalancing. We need to more
// promptly reclaim disk space for these bulk deletions. We can do this by
// maintaining information about "wide" ranged tombstones outside the narrow
// scope of a regular compaction between two levels.
//
// ## Table stats collection
//
// Whenever a new table is added to the LSM, Pebble's table stats collector
// computes stats for the table to be held in-memory for the lifetime of the
// table. The stats collector reads the table's ranged tombstones and finds the
// set of tables lower in the LSM that overlap. It uses this set to calculate an
// approximation of the amount of disk space that falls within the tombstoned
// span and thus could be reclaimed by compactions. This estimate is used to
// prioritize regular default compactions.
//
// At the same, the table stats collector compares the overlapping tables'
// bounds against the tombstones' bounds, looking for tables that (a) are wholly
// deleted by the tombstones, or (b) have a prefix or suffix of their data
// deleted by the tombstones. If any such tables are found, the tombstone span
// is considered "wide".
//
// # Tombstone span tracking
//
// The [Set] type defined within this package maintains a set of "tombstone
// spans", describing "wide" ranged tombstones that were observed by the table
// stats collector. The table stats collector constructs a WideTombstone
// representing 1 or more ranged tombstones (degfragmented, deduplicated) and
// calls [Set.AddTombstones] to add it to the set.
//
// Tombstones added to the [Set] are not immediately able to drive compactions.
// If a LSM snapshot is open at sequence number beneath one of the originating
// tombstones' sequence numbers, deleting data beneath the tombstone may violate
// that snapshot's isolation.
//
// Consider the below graphic, modelling a wide tombstone covering the keyspace
// [b, r), constructed from the abutting tombstone fragments [b,h)#230,RANGEDEL
// and [h,r)#200,RANGEDEL (assuming they're contained within the same sstable).
//
//	250
//
//	      |-b...230:h-|
//	_____________________________________________________ snapshot #210
//	200               |--h.RANGEDEL.200:r--|
//
//	_____________________________________________________ snapshot #180
//
//	150                     +--------+
//	          +---------+   | 000003 |
//	          | 000002  |   |        |
//	          +_________+   |        |
//	100_____________________|________|___________________ snapshot #100
//	                        +--------+
//	_____________________________________________________ snapshot #70
//	                            +---------------+
//	 50                         | 000001        |
//	                            |               |
//	                            +---------------+
//	______________________________________________________________
//	    a b c d e f g h i j k l m n o p q r s t u v w x y z
//
// A tombstone is only allowed to drop a key once the tombstone and the key fall
// into the same snapshot stripe (i.e., no snapshots exist at sequence numbers
// between the keys' sequence numbers). To simplify logic, we only act on a
// WideTombstone once there are no snapshots at any sequence number less than
// the WideTombstone's highest sequence number. (The WideTombstone falls into
// the last snapshot stripe.)
//
// In the above diagram, the tombstone [b,h)#230,RANGEDEL has the highest
// tombstone sequence number incorporated into the WideTombstone. Initially the
// wide tombstone is held in a queue of pending tombstones. After adding hints or
// when the earliest snapshot close, the caller is responsible for calling
// [Set.UpdateWithEarliestSnapshot]. When the earliest snapshot exceeds the
// WideTombstone's highest seqnum (#230 in the above diagram), the tombstone
// migrates from the pending queue to being incorporated into a region tree
// recording tombstoned spans.
//
// ## Region tree
//
// The region tree maps [start,end) spans to sequence numbers at which keys
// within the span are known to be tombstoned. The region tree maintains
// separate sequence numbers for point keys and range keys. When a WideTombstone
// migrates from its pending state to the tree, the WideTombstone's span is
// updated. The lowest sequence number of an originating tombstone is inserted
// into the tree. In the above diagram, the [h,r)#200,RANGEDEL fragment has the
// lowest sequence number, so the tree is updated to record that over the span
// [b,r) all point keys are deleted beneath sequence number #200.
//
// If a span is already tombstoned, the span is updated to the more powerful of
// the tombstones (i.e, the higher of the sequence numbers).
//
// ## Compaction picking
//
// The tombstone set and the region tree are used to drive compaction picking
// decisions, picking a special type of compaction called a "delete-only
// compaction." These compactions are lightweight metadata compactions that
// apply version edits to the LSM without reading or iterating over sstables.
//
// At the beginning of compaction picking, the compaction picker calls
// [Set.PickCompaction] to check if any delete-only compactions are available to
// be picked. PickCompaction walks the region tree in order. For each defined
// span, it finds sstables that overlap the span across levels of the LSM. It
// searches for tables older than the tombstone that (a) are entirely deleted by
// the tombstone, or (b) have a prefix or suffix of data that is entirely
// deleted by the tombstone.
//
// In the above example, the WideTombstone completely contains two tables,
// 000002 and 000003. The WideTombstone also overlaps the beginning of table
// 000001. All three of these tables may be returned by [Set.PickCompaction]:
// 000002 and 000003 for deletion, and 000001 for excise. PickCompaction will
// return information about only 1 of the tables. The compaction picker will
// then schedule a compaction to perform the table deletion or excise.
//
// If PickCompaction finds that no tables are affected by a tombstoned span, the
// span is forgotten. If any tables would be affected by the tombstoned span if
// they weren't already in the process of being compacted, the span is preserved
// in case the compaction is ultimately cancelled or produces a new output
// sstable that's eligible for a compaction.
//
// ### LargestSeqNumAbsolute
//
// It's crucial that compaction picking only chooses tables that contain data
// older than the tombstones. Sequence numbers provide a total ordering over
// committed writes, and we know a table can be deleted as long as all its keys
// were committed with sequence numbers less than the tombstone sequence number.
//
// However, there's a subtlety. When a regular default compaction observes that
// there is no data within the LSM beneath the compaction, it elides tombstones
// and applies an optimization that updates the keys' sequence numbers to zero.
// If default compactions compact the tombstones that originated a
// WideTombstone, default compactions may begin to update newer keys to have
// zeroed sequence numbers. This zeroing erases the knowledge that keys are too
// recent to delete.
//
// To account for this subtlety, [manifest.TableMetadata] has a
// [LargestSeqNumAbsolute] field. LargestSeqNumAbsolute provides an upper bound
// on the original, commit-time sequence numbers of all keys within the table.
// Compaction picking uses this sequence number when determining whether a table
// contains exclusively data older than the tombstone.
//
// This field is in-memory only and is reset to the TableMetadata's largest
// sequence number on process restart. This lossy behavior is okay, because the
// [Set] is also in-memory only and wiped on restart. On [pebble.Open], the
// table stats collector will scan tables with ranged tombstones to repopulate
// the tombstone spans. Since a key can only have its sequence number zeroed if
// nothing exists beneath it, any tombstones that are more recent than zeroed
// keys have already been removed from the LSM.

package tombspan

import (
	"bytes"
	"cmp"
	"fmt"
	"slices"
	"strings"

	"github.com/RaduBerinde/axisds"
	"github.com/RaduBerinde/axisds/regiontree"
	"github.com/cockroachdb/pebble/internal/base"
	"github.com/cockroachdb/pebble/internal/manifest"
)

// A WideTombstone summarizes a set of tombstones that together delete a wide
// swath of the keyspace.
//
// A WideTombstone is recorded if the stats collector observed at least one
// sstable that is (a) entirely deleted by the described tombstones, or (b) has
// a prefix or suffix of its data that is entirely deleted by the described
// tombstones.
type WideTombstone struct {
	// PointSeqNums and RangeSeqNums are the sequence number ranges of the point
	// and range tombstones used to construct the WideTombstone. The upper bound
	// determines *when* it's safe to delete data (preserving snapshots'
	// isolation guarantees), and the lower bound determines *what* data can be
	// deleted. All of a tables' keys must be less than the sequence number
	// range to be deleted. All of a tables' sequence numbers must fall into
	// the same snapshot stripe as the high end of the sequence number range,
	// and must all be less than the low end of the sequence number range to be
	// deleted.
	PointSeqNums base.SeqNumRange
	RangeSeqNums base.SeqNumRange
	// Bounds are the bounds of the tombstone(s).
	Bounds base.UserKeyBounds
	// Level of the table in the LSM containing the range tombstone(s) when the
	// WideTombstone was created. Only lower levels need to be searched for
	// files that may be deleted.
	Level int
	// Table identifies the sstable containing the range tombstone(s) that
	// created the WideTombstone.
	Table *manifest.TableMetadata
}

// HighestSeqNum returns the highest sequence number of all the tombstones
// described by the WideTombstone (across both point and range key kinds).
func (wt WideTombstone) HighestSeqNum() base.SeqNum {
	return max(wt.PointSeqNums.High, wt.RangeSeqNums.High)
}

// String implements fmt.Stringer.
func (wt WideTombstone) String() string {
	var buf bytes.Buffer
	fmt.Fprintf(&buf, "L%d.%s %s seqnums{", wt.Level, wt.Table.TableNum, wt.Bounds)
	if wt.PointSeqNums.High > 0 {
		fmt.Fprintf(&buf, "point=%s", wt.PointSeqNums)
	}
	if wt.RangeSeqNums.High > 0 {
		if wt.PointSeqNums.High > 0 {
			fmt.Fprint(&buf, ", ")
		}
		fmt.Fprintf(&buf, "range=%s", wt.RangeSeqNums)
	}
	fmt.Fprint(&buf, "}")
	return buf.String()
}

// Make creates a new tombstone set.
func Make(comparer *base.Comparer) Set {
	return Set{
		comparer: comparer,
		tombstonedSpans: regiontree.Make(
			axisds.CompareFn[[]byte](comparer.Compare),
			func(a, b tombstoneSeqNums) bool { return a == b },
		),
	}
}

// Set maintains a set of tombstoned spans. It's used to trigger delete
// compactions that efficiently remove data from the LSM.
//
// Set is not safe for concurrent use.
type Set struct {
	comparer *base.Comparer
	// pending is the set of WideTombstones that cannot yet be used to schedule
	// delete-only compactions, because at least one of their tombstones are not
	// yet in the last snapshot stripe. Pending is sorted by HighestSeqNum() in
	// descending order.
	//
	// When UpdateWithEarliestSnapshot is called with a sufficiently high
	// snapshot sequence number, a prefix of the pending WideTombstones are
	// incorporated into tombstonedSpans and removed from pending.
	pending []WideTombstone
	// tombstonedSpans is a region tree of tombstoned spans with sequence
	// numbers that fall into the last snapshot stripe (i.e, the keys are older
	// than the oldest snapshot). During compaction picking, the tree is scanned
	// and for each span, the current LSM version is examined to see if any
	// tables can be deleted outright or excised. If no relevant tables are
	// found, the span is removed from the tree.
	tombstonedSpans regiontree.T[[]byte, tombstoneSeqNums]
	// scratch is a temporary buffer to avoid allocations while collecting the
	// bounds of spans to remove from the tree of tombstoned spans.
	scratch []base.UserKeyBounds
}

// tombstoneSeqNums holds the earliest sequence numbers for both point and range
// tombstones within a particular span. The zero sequence number indicates the
// absence of a tombstone of the kind within the span.
type tombstoneSeqNums struct {
	pointSeqNum base.SeqNum
	rangeSeqNum base.SeqNum
}

// BoundsSeqNums returns true if all the keys in the provided table are older
// than the tombstones of the provided span, and all keys in the table are
// deleted by tombstones within the span. If the table contains keys of a type
// (Point vs Range) not deleted by the span, the function returns false.
func (ts tombstoneSeqNums) BoundsSeqNums(m *manifest.TableMetadata) bool {
	// A table can only be deleted if all of its keys are older than the
	// earliest tombstone aggregated into the span (for both point and range
	// keys, if any).
	//
	// Note that we use m.LargestSeqNumAbsolute, not m.LargestSeqNum. Consider a
	// compaction that zeroes sequence numbers. A compaction may zero the
	// sequence number of a key with a sequence number >
	// h.tombstoneSmallestSeqNum and set it to zero. If we looked at
	// m.LargestSeqNum, the resulting output file would appear to not contain
	// any keys more recent than the oldest tombstone. To avoid this error, the
	// largest pre-zeroing sequence number is maintained in
	// LargestSeqNumAbsolute and used here to make the determination whether the
	// file's keys are older than all of the span's tombstones.
	return (!m.HasPointKeys || ts.pointSeqNum > 0 && m.LargestSeqNumAbsolute < ts.pointSeqNum) &&
		(!m.HasRangeKeys || ts.rangeSeqNum > 0 && m.LargestSeqNumAbsolute < ts.rangeSeqNum)
}

// String returns a string representation of the tombstoned span.
func (ts tombstoneSeqNums) String() string {
	var buf bytes.Buffer
	fmt.Fprint(&buf, "{")
	if ts.pointSeqNum > 0 {
		fmt.Fprintf(&buf, "point=%s", ts.pointSeqNum)
	}
	if ts.rangeSeqNum > 0 {
		if ts.pointSeqNum > 0 {
			fmt.Fprint(&buf, ", ")
		}
		fmt.Fprintf(&buf, "range=%s", ts.rangeSeqNum)
	}
	fmt.Fprint(&buf, "}")
	return buf.String()
}

// mergeTombstonedSpans is used when updating the tree to contain a new
// tombstoned span. Tombstones with higher sequence numbers are more powerful,
// deleting strictly more data, so merging preserves the highest sequence
// number.
func mergeTombstonedSpans(a, b tombstoneSeqNums) tombstoneSeqNums {
	return tombstoneSeqNums{
		pointSeqNum: max(a.pointSeqNum, b.pointSeqNum),
		rangeSeqNum: max(a.rangeSeqNum, b.rangeSeqNum),
	}
}

// AddTombstones adds the given WideTombstones to the set of tombstones. Callers
// should invoke UpdateWithEarliestSnapshot after adding additional tombstones.
//
// AddTombstones must not be called concurrently with any other method.
func (fs *Set) AddTombstones(tombstones ...WideTombstone) {
	fs.pending = append(fs.pending, tombstones...)
	slices.SortFunc(fs.pending, func(a, b WideTombstone) int {
		return cmp.Compare(b.HighestSeqNum(), a.HighestSeqNum())
	})
}

// UpdateWithEarliestSnapshot updates the set's state with the current oldest
// snapshot. A call to UpdateWithEarliestSnapshot ensures that any subsequent
// call to PickCompaction will consider all tombstoned spans with sequence
// numbers that now fall into the last snapshot stripe.
//
// UpdateWithEarliestSnapshot must not be called concurrently with any other
// method.
func (ts *Set) UpdateWithEarliestSnapshot(earliestSnapshot base.SeqNum) {
	// When a WideTombstone is created, the sequence numbers of the described
	// tombstones are recorded. The highest tombstone sequence number must be in
	// the last snapshot stripe for the WideTombstone to be used to actually
	// delete data.
	for i, h := range ts.pending {
		if earliestSnapshot <= h.HighestSeqNum() {
			// All remaining WideTombstones are not yet in the last snapshot
			// stripe.
			ts.pending = append(ts.pending[:0], ts.pending[i:]...)
			return
		}
		// This WideTombstone's tombstones are now in the last snapshot stripe.
		// Add a tombstoned span.
		s := tombstoneSeqNums{
			pointSeqNum: h.PointSeqNums.Low,
			rangeSeqNum: h.RangeSeqNums.Low,
		}
		ts.tombstonedSpans.Update(h.Bounds.Start, h.Bounds.End.Key,
			func(curr tombstoneSeqNums) tombstoneSeqNums {
				return mergeTombstonedSpans(curr, s)
			})
	}
	// All WideTombstones were added to the tombstoned spans.
	ts.pending = ts.pending[:0]
}

// DeleteOnlyCompaction describes a picked delete-only compaction, applying to a
// single sstable within the LSM.
type DeleteOnlyCompaction struct {
	Level  int
	Table  manifest.LevelTable
	Bounds base.UserKeyBounds
	Excise bool
}

// String implements fmt.Stringer.
func (d DeleteOnlyCompaction) String() string {
	if d.Excise {
		return fmt.Sprintf("excise L%d.%s (partially overlapping with %s)",
			d.Level, d.Table, d.Bounds)
	}
	return fmt.Sprintf("delete L%d.%s (completely contained within %s)",
		d.Level, d.Table, d.Bounds)
}

// PickCompaction consults the set of tombstoned spans to determine if any
// tables within the provided version can be deleted outright, or excised (if
// isExciseAllowed is true). If for any span within the set, no eligible tables
// are found, the set is updated to clear the span.
//
// PickCompaction must not be called concurrently with any other method.
func (ts *Set) PickCompaction(
	v *manifest.Version, isExciseAllowed bool,
) (picked DeleteOnlyCompaction, ok bool) {
	// pickCompactionForSpan is a helper function that picks a compaction for a
	// given tombstoned span. It returns the picked compaction, a boolean
	// indicating whether a compaction was found, and a boolean indicating
	// whether the span should be cleared because it's unlikely to be able to
	// schedule new delete only compactions in the future.
	clearSpans := ts.scratch[:0]
	pickCompactionForSpan := func(tombBounds base.UserKeyBounds, span tombstoneSeqNums) (picked DeleteOnlyCompaction, ok bool) {
		var skippedDueToCompacting bool
		for level := manifest.NumLevels - 1; level > 0; level-- {
			overlaps := v.Overlaps(level, tombBounds)
			iter := overlaps.Iter()
			for m := iter.First(); m != nil; m = iter.Next() {
				// A table can only be deleted if all of its keys are older than
				// the earliest tombstone aggregated into the span, and the
				// tombstone covers all the key kinds contained within the
				// table.
				if !span.BoundsSeqNums(m) {
					continue
				}

				// Skip any currently compacting tables.
				if m.IsCompacting() {
					skippedDueToCompacting = true
					continue
				}

				eligibility := canDeleteOrExciseTable(ts.comparer.Compare, tombBounds, m.UserKeyBounds(), isExciseAllowed)
				if eligibility == tableEligibilityDelete {
					return DeleteOnlyCompaction{
						Level:  level,
						Table:  iter.Take(),
						Bounds: tombBounds,
						Excise: false,
					}, true /* ok */
				} else if eligibility == tableEligibilityExcise && isExciseAllowed {
					return DeleteOnlyCompaction{
						Level:  level,
						Table:  iter.Take(),
						Bounds: tombBounds,
						Excise: true,
					}, true /* ok */
				}
			}
		}

		// We didn't find any tables to delete or excise.
		//
		// If we skipped any tables because they were in the process of being
		// compacted, this tombstone span might be able to delete a table
		// outputted by the in-progress compaction. Or that compaction could be
		// cancelled. In this case we keep the span around so we can try again
		// when the compaction completes.
		//
		// Otherwise, it's unlikely this tombstone span will ever allow us to
		// delete or excise anything. A future compaction would need to split
		// tables in lower levels more favorably, an unlikely event. In this
		// case we record that the span should be cleared. When we're done
		// iterating, we'll clear anything we appended to clearSpans.
		if !skippedDueToCompacting {
			clearSpans = append(clearSpans, tombBounds)
		}
		return DeleteOnlyCompaction{}, false /* ok */
	}

	for bounds, span := range ts.tombstonedSpans.All() {
		tombBounds := base.UserKeyBoundsEndExclusive(bounds.Start, bounds.End)
		if picked, ok = pickCompactionForSpan(tombBounds, span); ok {
			// Found a compaction.
			break
		}
	}

	// Clear any spans that we found are no longer useful.
	//
	// TODO(jackson): Amend the region tree to allow clearing spans while
	// iterating.
	for _, bounds := range clearSpans {
		ts.tombstonedSpans.Update(bounds.Start, bounds.End.Key,
			func(curr tombstoneSeqNums) tombstoneSeqNums { return tombstoneSeqNums{} })
	}
	clear(clearSpans)
	ts.scratch = clearSpans[:0]

	// Return the picked compaction, if any.
	return picked, ok
}

type tableEligibility int8

const (
	tableEligibilityNone tableEligibility = iota
	tableEligibilityDelete
	tableEligibilityExcise
)

func canDeleteOrExciseTable(
	cmp base.Compare, tombBounds, tableBounds base.UserKeyBounds, isExciseAllowed bool,
) tableEligibility {
	if tombBounds.ContainsBounds(cmp, tableBounds) {
		// The table is completely contained within the tombstone's
		// bounds. The table can be deleted outright.
		//
		//	  |------------tombstone-------------|
		//	    |--------sstable--------|
		//
		return tableEligibilityDelete
	}

	// We can't delete the table outright. If excise is enabled and
	// allowed by the format major version, we can try to excise the
	// table. Otherwise, we can continue to the next table.
	if !isExciseAllowed {
		return tableEligibilityNone
	}

	if cmp(tombBounds.Start, tableBounds.Start) <= 0 ||
		tombBounds.End.CompareUpperBounds(cmp, tableBounds.End) >= 0 {
		// The tombstone straddles the left or right boundary of the
		// table, making the table eligible for an excise.
		//
		//	  |---tombstone---|
		//	       |------sstable------|
		//
		// or
		//
		//	        |---tombstone---|
		//	  |------sstable------|
		return tableEligibilityExcise
	}

	// The table was returned by Overlaps, so it must overlap the
	// tombstone's bounds.
	//
	// We checked that the table isn't completely contianed within
	// the bounds of the tombstone, and we checked that neither of
	// its boundaries fall within the bounds of the tombstone.
	//
	// In the remaining case, the tombstone lies completely within
	// the table's bounds:
	//
	//	            |---tombstone----|
	//	      |-----------sstable------------|
	//
	// We choose to do nothing in this case, because we don't know
	// how much data the sstable actually contains within the
	// tombstone's span. If we split the sstable into two virtual
	// tables, we'd increase the number of tables in the level
	// metadata which has its own cost. Eventually an ordinary
	// compaction will delete the data if the table indeed contains
	// keys within the tombstone's span.
	return tableEligibilityNone
}

// String returns a string representation of the tombstones.
func (ts *Set) String() string {
	var buf bytes.Buffer
	if len(ts.pending) > 0 {
		fmt.Fprintf(&buf, "Pending:\n")
		for _, h := range ts.pending {
			fmt.Fprintf(&buf, "  %s\n", h.String())
		}
	}
	if !ts.tombstonedSpans.IsEmpty() {
		fmt.Fprintln(&buf, "Tombstoned spans:")
		fmt.Fprintln(&buf, ts.tombstonedSpans.String(
			axisds.MakeIntervalFormatter(func(b []byte) string {
				return fmt.Sprint(ts.comparer.FormatKey(b))
			})))
	}
	if buf.Len() == 0 {
		return "(none)"
	}
	return strings.TrimSpace(buf.String())
}
