// Copyright 2025 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package pebble

import (
	"context"
	"slices"

	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/pebble/internal/base"
	"github.com/cockroachdb/pebble/internal/invariants"
	"github.com/cockroachdb/pebble/internal/manifest"
	"github.com/cockroachdb/pebble/objstorage"
)

// Excise atomically deletes all data overlapping with the provided span. All
// data overlapping with the span is removed, including from open snapshots.
// Only currently-open iterators will still observe the removed data (because an
// open iterator pins all memtables and sstables in its view of the LSM until
// it's closed). Excise may initiate a flush if there exists unflushed data
// overlapping the excise span.
func (d *DB) Excise(ctx context.Context, span KeyRange) error {
	if err := d.closed.Load(); err != nil {
		panic(err)
	}
	if d.opts.ReadOnly {
		return ErrReadOnly
	}
	// Excise is only supported on prefix keys.
	if d.opts.Comparer.Split(span.Start) != len(span.Start) {
		return errors.New("Excise called with suffixed start key")
	}
	if d.opts.Comparer.Split(span.End) != len(span.End) {
		return errors.New("Excise called with suffixed end key")
	}
	if v := d.FormatMajorVersion(); v < FormatVirtualSSTables {
		return errors.Newf(
			"store has format major version %d; Excise requires at least %d",
			v, FormatVirtualSSTables,
		)
	}
	_, err := d.ingest(ctx, ingestArgs{ExciseSpan: span, ExciseBoundsPolicy: tightExciseBoundsIfLocal})
	return err
}

// exciseBoundsPolicy controls whether we open excised files to obtain tight
// bounds for the remaining file(s).
type exciseBoundsPolicy uint8

const (
	// tightExciseBounds means that we will always open the file to find the exact
	// bounds of the remaining file(s).
	tightExciseBounds exciseBoundsPolicy = iota
	// looseExciseBounds means that we will not open the file and will assign bounds
	// pessimistically.
	looseExciseBounds
	// tightExciseBoundsLocalOnly means that we will only open the file if it is
	// local; otherwise we will assign loose bounds to the remaining file(s).
	tightExciseBoundsIfLocal
)

// exciseTable initializes up to two virtual tables for what is left over after
// excising the given span from the table.
//
// Returns the left and/or right tables, if they exist. The boundsPolicy controls
// whether we create iterators for m to determine tight bounds. Note that if the
// exciseBounds are end-inclusive, tight bounds will be used regardless of the
// policy.
//
// The file bounds must overlap with the excise span.
//
// This method is agnostic to whether d.mu is held or not. Some cases call it with
// the db mutex held (eg. ingest-time excises), while in the case of compactions
// the mutex is not held.
func (d *DB) exciseTable(
	ctx context.Context,
	exciseBounds base.UserKeyBounds,
	m *manifest.TableMetadata,
	level int,
	boundsPolicy exciseBoundsPolicy,
) (leftTable, rightTable *manifest.TableMetadata, _ error) {
	// Check if there's actually an overlap between m and exciseSpan.
	if !exciseBounds.Overlaps(d.cmp, m.UserKeyBounds()) {
		return nil, nil, base.AssertionFailedf("excise span does not overlap table")
	}
	// Fast path: m sits entirely within the exciseSpan, so just delete it.
	if exciseBounds.ContainsInternalKey(d.cmp, m.Smallest()) && exciseBounds.ContainsInternalKey(d.cmp, m.Largest()) {
		return nil, nil, nil
	}

	looseBounds := boundsPolicy == looseExciseBounds ||
		(boundsPolicy == tightExciseBoundsIfLocal && !objstorage.IsLocalTable(d.objProvider, m.TableBacking.DiskFileNum))

	if exciseBounds.End.Kind == base.Inclusive {
		// Loose bounds are not allowed with end-inclusive bounds. This can only
		// happen for ingest splits.
		looseBounds = false
	}

	// The file partially overlaps the excise span; unless looseBounds is true, we
	// will need to open it to determine tight bounds for the left-over table(s).
	var iters iterSet
	if !looseBounds {
		var err error
		iters, err = d.newIters(ctx, m, &IterOptions{
			Category: categoryIngest,
			layer:    manifest.Level(level),
		}, internalIterOpts{}, iterPointKeys|iterRangeDeletions|iterRangeKeys)
		if err != nil {
			return nil, nil, err
		}
		defer func() { _ = iters.CloseAll() }()
	}

	// Create a file to the left of the excise span, if necessary.
	// The bounds of this file will be [m.Smallest, lastKeyBefore(exciseSpan.Start)].
	//
	// We create bounds that are tight on user keys, and we make the effort to find
	// the last key in the original sstable that's smaller than exciseSpan.Start
	// even though it requires some sstable reads. We could choose to create
	// virtual sstables on loose userKey bounds, in which case we could just set
	// leftFile.Largest to an exclusive sentinel at exciseSpan.Start. The biggest
	// issue with that approach would be that it'd lead to lots of small virtual
	// sstables in the LSM that have no guarantee on containing even a single user
	// key within the file bounds. This has the potential to increase both read and
	// write-amp as we will be opening up these sstables only to find no relevant
	// keys in the read path, and compacting sstables on top of them instead of
	// directly into the space occupied by them. We choose to incur the cost of
	// calculating tight bounds at this time instead of creating more work in the
	// future.
	//
	// TODO(bilal): Some of this work can happen without grabbing the manifest
	// lock; we could grab one currentVersion, release the lock, calculate excised
	// files, then grab the lock again and recalculate for just the files that
	// have changed since our previous calculation. Do this optimization as part of
	// https://github.com/cockroachdb/pebble/issues/2112 .
	if d.cmp(m.Smallest().UserKey, exciseBounds.Start) < 0 {
		leftTable = &manifest.TableMetadata{
			Virtual:  true,
			TableNum: d.mu.versions.getNextTableNum(),
			// Note that these are loose bounds for smallest/largest seqnums, but they're
			// sufficient for maintaining correctness.
			SeqNums:                  m.SeqNums,
			LargestSeqNumAbsolute:    m.LargestSeqNumAbsolute,
			SyntheticPrefixAndSuffix: m.SyntheticPrefixAndSuffix,
			BlobReferenceDepth:       m.BlobReferenceDepth,
		}
		if looseBounds {
			looseLeftTableBounds(d.cmp, m, leftTable, exciseBounds.Start)
		} else if err := determineLeftTableBounds(d.cmp, m, leftTable, exciseBounds.Start, iters); err != nil {
			return nil, nil, err
		}

		if leftTable.HasRangeKeys || leftTable.HasPointKeys {
			leftTable.AttachVirtualBacking(m.TableBacking)
			if looseBounds {
				// We don't want to access the object; make up a size.
				leftTable.Size = (m.Size + 1) / 2
			} else if err := determineExcisedTableSize(d.fileCache, m, leftTable); err != nil {
				return nil, nil, err
			}
			determineExcisedTableBlobReferences(m.BlobReferences, m.Size, leftTable, d.FormatMajorVersion())
			if err := leftTable.Validate(d.cmp, d.opts.Comparer.FormatKey); err != nil {
				return nil, nil, err
			}
			leftTable.ValidateVirtual(m)
		} else {
			leftTable = nil
		}
	}
	// Create a file to the right, if necessary.
	if !exciseBounds.End.IsUpperBoundForInternalKey(d.cmp, m.Largest()) {
		// Create a new file, rightFile, between [firstKeyAfter(exciseSpan.End), m.Largest].
		//
		// See comment before the definition of leftFile for the motivation behind
		// calculating tight user-key bounds.
		rightTable = &manifest.TableMetadata{
			Virtual:  true,
			TableNum: d.mu.versions.getNextTableNum(),
			// Note that these are loose bounds for smallest/largest seqnums, but they're
			// sufficient for maintaining correctness.
			SeqNums:                  m.SeqNums,
			LargestSeqNumAbsolute:    m.LargestSeqNumAbsolute,
			SyntheticPrefixAndSuffix: m.SyntheticPrefixAndSuffix,
			BlobReferenceDepth:       m.BlobReferenceDepth,
		}
		if looseBounds {
			// We already checked that the end bound is exclusive.
			looseRightTableBounds(d.cmp, m, rightTable, exciseBounds.End.Key)
		} else if err := determineRightTableBounds(d.cmp, m, rightTable, exciseBounds.End, iters); err != nil {
			return nil, nil, err
		}
		if rightTable.HasRangeKeys || rightTable.HasPointKeys {
			rightTable.AttachVirtualBacking(m.TableBacking)
			if looseBounds {
				// We don't want to access the object; make up a size.
				rightTable.Size = (m.Size + 1) / 2
			} else if err := determineExcisedTableSize(d.fileCache, m, rightTable); err != nil {
				return nil, nil, err
			}
			determineExcisedTableBlobReferences(m.BlobReferences, m.Size, rightTable, d.FormatMajorVersion())
			if err := rightTable.Validate(d.cmp, d.opts.Comparer.FormatKey); err != nil {
				return nil, nil, err
			}
			rightTable.ValidateVirtual(m)
		} else {
			rightTable = nil
		}
	}
	return leftTable, rightTable, nil
}

// exciseOverlapBounds examines the provided list of snapshots, examining each
// eventually file-only snapshot in the list and its bounds. If the snapshot is
// visible at the excise's sequence number, then it accumulates all of the
// eventually file-only snapshot's protected ranges.
func exciseOverlapBounds(
	cmp Compare, sl *snapshotList, exciseSpan KeyRange, exciseSeqNum base.SeqNum,
) []bounded {
	var extended []bounded
	for s := sl.root.next; s != &sl.root; s = s.next {
		if s.efos == nil {
			continue
		}
		if base.Visible(exciseSeqNum, s.efos.seqNum, base.SeqNumMax) {
			// We only worry about snapshots older than the excise. Any snapshots
			// created after the excise should see the excised view of the LSM
			// anyway.
			//
			// Since we delay publishing the excise seqnum as visible until after
			// the apply step, this case will never be hit in practice until we
			// make excises flushable ingests.
			continue
		}
		if invariants.Enabled {
			if s.efos.hasTransitioned() {
				panic("unexpected transitioned EFOS in snapshots list")
			}
		}
		for i := range s.efos.protectedRanges {
			if !s.efos.protectedRanges[i].OverlapsKeyRange(cmp, exciseSpan) {
				continue
			}
			// Our excise conflicts with this EFOS. We need to add its protected
			// ranges to our extended overlap bounds. Grow extended in one
			// allocation if necesary.
			extended = slices.Grow(extended, len(s.efos.protectedRanges))
			for i := range s.efos.protectedRanges {
				extended = append(extended, &s.efos.protectedRanges[i])
			}
			break
		}
	}
	return extended
}

// looseLeftTableBounds initializes the bounds for the table that remains to the
// left of the excise span after excising originalTable, without consulting the
// contents of originalTable. The resulting bounds are loose.
//
// Sets the smallest and largest keys, as well as HasPointKeys/HasRangeKeys in
// the leftFile.
func looseLeftTableBounds(
	cmp Compare, originalTable, leftTable *manifest.TableMetadata, exciseSpanStart []byte,
) {
	if originalTable.HasPointKeys {
		largestPointKey := originalTable.PointKeyBounds.Largest()
		if largestPointKey.IsUpperBoundFor(cmp, exciseSpanStart) {
			largestPointKey = base.MakeRangeDeleteSentinelKey(exciseSpanStart)
		}
		leftTable.ExtendPointKeyBounds(cmp, originalTable.PointKeyBounds.Smallest(), largestPointKey)
	}
	if originalTable.HasRangeKeys {
		largestRangeKey := originalTable.RangeKeyBounds.Largest()
		if largestRangeKey.IsUpperBoundFor(cmp, exciseSpanStart) {
			largestRangeKey = base.MakeExclusiveSentinelKey(InternalKeyKindRangeKeyMin, exciseSpanStart)
		}
		leftTable.ExtendRangeKeyBounds(cmp, originalTable.RangeKeyBounds.Smallest(), largestRangeKey)
	}
}

// looseRightTableBounds initializes the bounds for the table that remains to the
// right of the excise span after excising originalTable, without consulting the
// contents of originalTable. The resulting bounds are loose.
//
// Sets the smallest and largest keys, as well as HasPointKeys/HasRangeKeys in
// the rightFile.
//
// The excise span end bound is assumed to be exclusive; this function cannot be
// used with an inclusive end bound.
func looseRightTableBounds(
	cmp Compare, originalTable, rightTable *manifest.TableMetadata, exciseSpanEnd []byte,
) {
	if originalTable.HasPointKeys {
		smallestPointKey := originalTable.PointKeyBounds.Smallest()
		if !smallestPointKey.IsUpperBoundFor(cmp, exciseSpanEnd) {
			smallestPointKey = base.MakeInternalKey(exciseSpanEnd, 0, base.InternalKeyKindMaxForSSTable)
		}
		rightTable.ExtendPointKeyBounds(cmp, smallestPointKey, originalTable.PointKeyBounds.Largest())
	}
	if originalTable.HasRangeKeys {
		smallestRangeKey := originalTable.RangeKeyBounds.Smallest()
		if !smallestRangeKey.IsUpperBoundFor(cmp, exciseSpanEnd) {
			smallestRangeKey = base.MakeInternalKey(exciseSpanEnd, 0, base.InternalKeyKindRangeKeyMax)
		}
		rightTable.ExtendRangeKeyBounds(cmp, smallestRangeKey, originalTable.RangeKeyBounds.Largest())
	}
}

// determineLeftTableBounds calculates the bounds for the table that remains to
// the left of the excise span after excising originalTable. The bounds around
// the excise span are determined precisely by looking inside the file.
//
// Sets the smallest and largest keys, as well as HasPointKeys/HasRangeKeys in
// the leftFile.
func determineLeftTableBounds(
	cmp Compare,
	originalTable, leftTable *manifest.TableMetadata,
	exciseSpanStart []byte,
	iters iterSet,
) error {
	if originalTable.HasPointKeys && cmp(originalTable.PointKeyBounds.Smallest().UserKey, exciseSpanStart) < 0 {
		// This file will probably contain point keys.
		if kv := iters.Point().SeekLT(exciseSpanStart, base.SeekLTFlagsNone); kv != nil {
			leftTable.ExtendPointKeyBounds(cmp, originalTable.PointKeyBounds.Smallest(), kv.K.Clone())
		}
		rdel, err := iters.RangeDeletion().SeekLT(exciseSpanStart)
		if err != nil {
			return err
		}
		if rdel != nil {
			// Use the smaller of exciseSpanStart and rdel.End.
			lastRangeDel := exciseSpanStart
			if cmp(rdel.End, exciseSpanStart) < 0 {
				// The key is owned by the range del iter, so we need to copy it.
				lastRangeDel = slices.Clone(rdel.End)
			}
			leftTable.ExtendPointKeyBounds(cmp, originalTable.PointKeyBounds.Smallest(),
				base.MakeExclusiveSentinelKey(InternalKeyKindRangeDelete, lastRangeDel))
		}
	}

	if originalTable.HasRangeKeys && cmp(originalTable.RangeKeyBounds.SmallestUserKey(), exciseSpanStart) < 0 {
		rkey, err := iters.RangeKey().SeekLT(exciseSpanStart)
		if err != nil {
			return err
		}
		if rkey != nil {
			// Use the smaller of exciseSpanStart and rkey.End.
			lastRangeKey := exciseSpanStart
			if cmp(rkey.End, exciseSpanStart) < 0 {
				// The key is owned by the range key iter, so we need to copy it.
				lastRangeKey = slices.Clone(rkey.End)
			}
			leftTable.ExtendRangeKeyBounds(cmp, originalTable.RangeKeyBounds.Smallest(),
				base.MakeExclusiveSentinelKey(rkey.LargestKey().Kind(), lastRangeKey))
		}
	}
	return nil
}

// determineRightTableBounds calculates the bounds for the table that remains to
// the right of the excise span after excising originalTable. The bounds around
// the excise span are determined precisely by looking inside the file.
//
// Sets the smallest and largest keys, as well as HasPointKeys/HasRangeKeys in
// the right.
//
// Note that the case where exciseSpanEnd is Inclusive is very restrictive; we
// are only allowed to excise if the original table has no keys or ranges
// overlapping exciseSpanEnd.Key.
func determineRightTableBounds(
	cmp Compare,
	originalTable, rightTable *manifest.TableMetadata,
	exciseSpanEnd base.UserKeyBoundary,
	iters iterSet,
) error {
	if originalTable.HasPointKeys && !exciseSpanEnd.IsUpperBoundForInternalKey(cmp, originalTable.PointKeyBounds.Largest()) {
		if kv := iters.Point().SeekGE(exciseSpanEnd.Key, base.SeekGEFlagsNone); kv != nil {
			if exciseSpanEnd.Kind == base.Inclusive && cmp(exciseSpanEnd.Key, kv.K.UserKey) == 0 {
				return base.AssertionFailedf("cannot excise with an inclusive end key and data overlap at end key")
			}
			rightTable.ExtendPointKeyBounds(cmp, kv.K.Clone(), originalTable.PointKeyBounds.Largest())
		}
		rdel, err := iters.RangeDeletion().SeekGE(exciseSpanEnd.Key)
		if err != nil {
			return err
		}
		if rdel != nil {
			// Use the larger of exciseSpanEnd.Key and rdel.Start.
			firstRangeDel := exciseSpanEnd.Key
			if cmp(rdel.Start, exciseSpanEnd.Key) > 0 {
				// The key is owned by the range del iter, so we need to copy it.
				firstRangeDel = slices.Clone(rdel.Start)
			} else if exciseSpanEnd.Kind != base.Exclusive {
				return base.AssertionFailedf("cannot truncate rangedel during excise with an inclusive upper bound")
			}
			rightTable.ExtendPointKeyBounds(cmp, base.InternalKey{
				UserKey: firstRangeDel,
				Trailer: rdel.SmallestKey().Trailer,
			}, originalTable.PointKeyBounds.Largest())
		}
	}
	if originalTable.HasRangeKeys && !exciseSpanEnd.IsUpperBoundForInternalKey(cmp, originalTable.RangeKeyBounds.Largest()) {
		rkey, err := iters.RangeKey().SeekGE(exciseSpanEnd.Key)
		if err != nil {
			return err
		}
		if rkey != nil {
			// Use the larger of exciseSpanEnd.Key and rkey.Start.
			firstRangeKey := exciseSpanEnd.Key
			if cmp(rkey.Start, exciseSpanEnd.Key) > 0 {
				// The key is owned by the range key iter, so we need to copy it.
				firstRangeKey = slices.Clone(rkey.Start)
			} else if exciseSpanEnd.Kind != base.Exclusive {
				return base.AssertionFailedf("cannot truncate range key during excise with an inclusive upper bound")
			}
			rightTable.ExtendRangeKeyBounds(cmp, base.InternalKey{
				UserKey: firstRangeKey,
				Trailer: rkey.SmallestKey().Trailer,
			}, originalTable.RangeKeyBounds.Largest())
		}
	}
	return nil
}

func determineExcisedTableSize(
	fc *fileCacheHandle, originalTable, excisedTable *manifest.TableMetadata,
) error {
	size, err := fc.estimateSize(originalTable, excisedTable.Smallest().UserKey, excisedTable.Largest().UserKey)
	if err != nil {
		return err
	}
	excisedTable.Size = size
	if size == 0 {
		// On occasion, estimateSize gives us a low estimate, i.e. a 0 file size,
		// such as if the excised file only has range keys/dels and no point
		// keys. This can cause panics in places where we divide by file sizes.
		// Correct for it here.
		excisedTable.Size = 1
	}
	return nil
}

// determineExcisedTableBlobReferences copies blob references from the original
// table to the excised table, scaling each blob reference's value size
// proportionally based on the ratio of the excised table's size to the original
// table's size.
func determineExcisedTableBlobReferences(
	originalBlobReferences manifest.BlobReferences,
	originalSize uint64,
	excisedTable *manifest.TableMetadata,
	fmv FormatMajorVersion,
) {
	if len(originalBlobReferences) == 0 {
		return
	}
	newBlobReferences := make(manifest.BlobReferences, len(originalBlobReferences))
	for i, bf := range originalBlobReferences {
		bf.ValueSize = max(bf.ValueSize*excisedTable.Size/originalSize, 1)
		if fmv < FormatBackingValueSize {
			bf.BackingValueSize = 0
		}
		newBlobReferences[i] = bf
	}
	excisedTable.BlobReferences = newBlobReferences
}

// applyExciseToVersionEdit updates ve with a table deletion for the original
// table and table additions for the left and/or right table.
//
// Either or both of leftTable/rightTable can be nil.
func applyExciseToVersionEdit(
	ve *manifest.VersionEdit, originalTable, leftTable, rightTable *manifest.TableMetadata, level int,
) (newFiles []manifest.NewTableEntry) {
	ve.DeletedTables[manifest.DeletedTableEntry{
		Level:   level,
		FileNum: originalTable.TableNum,
	}] = originalTable
	if leftTable == nil && rightTable == nil {
		return
	}
	if !originalTable.Virtual {
		// If the original table was virtual, then its file backing is already known
		// to the manifest; we don't need to create another file backing. Note that
		// there must be only one CreatedBackingTables entry per backing sstable.
		// This is indicated by the VersionEdit.CreatedBackingTables invariant.
		ve.CreatedBackingTables = append(ve.CreatedBackingTables, originalTable.TableBacking)
	}
	originalLen := len(ve.NewTables)
	if leftTable != nil {
		ve.NewTables = append(ve.NewTables, manifest.NewTableEntry{Level: level, Meta: leftTable})
	}
	if rightTable != nil {
		ve.NewTables = append(ve.NewTables, manifest.NewTableEntry{Level: level, Meta: rightTable})
	}
	return ve.NewTables[originalLen:]
}
