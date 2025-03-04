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
	_, err := d.ingest(ctx, nil, nil, span, nil)
	return err
}

// excise updates ve to include a replacement of the file m with new virtual
// sstables that exclude exciseSpan, returning a slice of newly-created files if
// any. If the entirety of m is deleted by exciseSpan, no new sstables are added
// and m is deleted. Note that ve is updated in-place.
//
// This method is agnostic to whether d.mu is held or not. Some cases call it with
// the db mutex held (eg. ingest-time excises), while in the case of compactions
// the mutex is not held.
func (d *DB) excise(
	ctx context.Context, exciseSpan base.UserKeyBounds, m *tableMetadata, ve *versionEdit, level int,
) ([]manifest.NewTableEntry, error) {
	numCreatedFiles := 0
	// Check if there's actually an overlap between m and exciseSpan.
	mBounds := base.UserKeyBoundsFromInternal(m.Smallest, m.Largest)
	if !exciseSpan.Overlaps(d.cmp, &mBounds) {
		return nil, nil
	}
	ve.DeletedTables[deletedFileEntry{
		Level:   level,
		FileNum: m.FileNum,
	}] = m
	// Fast path: m sits entirely within the exciseSpan, so just delete it.
	if exciseSpan.ContainsInternalKey(d.cmp, m.Smallest) && exciseSpan.ContainsInternalKey(d.cmp, m.Largest) {
		return nil, nil
	}

	var iters iterSet
	var itersLoaded bool
	defer iters.CloseAll()
	loadItersIfNecessary := func() error {
		if itersLoaded {
			return nil
		}
		var err error
		iters, err = d.newIters(ctx, m, &IterOptions{
			Category: categoryIngest,
			layer:    manifest.Level(level),
		}, internalIterOpts{}, iterPointKeys|iterRangeDeletions|iterRangeKeys)
		itersLoaded = true
		return err
	}

	needsBacking := false
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
	// have changed since our previous calculation. Do this optimiaztino as part of
	// https://github.com/cockroachdb/pebble/issues/2112 .
	if d.cmp(m.Smallest.UserKey, exciseSpan.Start) < 0 {
		leftFile := &tableMetadata{
			Virtual:     true,
			FileBacking: m.FileBacking,
			FileNum:     d.mu.versions.getNextFileNum(),
			// Note that these are loose bounds for smallest/largest seqnums, but they're
			// sufficient for maintaining correctness.
			SmallestSeqNum:           m.SmallestSeqNum,
			LargestSeqNum:            m.LargestSeqNum,
			LargestSeqNumAbsolute:    m.LargestSeqNumAbsolute,
			SyntheticPrefixAndSuffix: m.SyntheticPrefixAndSuffix,
		}
		if m.HasPointKeys && !exciseSpan.ContainsInternalKey(d.cmp, m.SmallestPointKey) {
			// This file will probably contain point keys.
			if err := loadItersIfNecessary(); err != nil {
				return nil, err
			}
			smallestPointKey := m.SmallestPointKey
			if kv := iters.Point().SeekLT(exciseSpan.Start, base.SeekLTFlagsNone); kv != nil {
				leftFile.ExtendPointKeyBounds(d.cmp, smallestPointKey, kv.K.Clone())
			}
			// Store the min of (exciseSpan.Start, rdel.End) in lastRangeDel. This
			// needs to be a copy if the key is owned by the range del iter.
			var lastRangeDel []byte
			if rdel, err := iters.RangeDeletion().SeekLT(exciseSpan.Start); err != nil {
				return nil, err
			} else if rdel != nil {
				lastRangeDel = append(lastRangeDel[:0], rdel.End...)
				if d.cmp(lastRangeDel, exciseSpan.Start) > 0 {
					lastRangeDel = exciseSpan.Start
				}
			}
			if lastRangeDel != nil {
				leftFile.ExtendPointKeyBounds(d.cmp, smallestPointKey, base.MakeExclusiveSentinelKey(InternalKeyKindRangeDelete, lastRangeDel))
			}
		}
		if m.HasRangeKeys && !exciseSpan.ContainsInternalKey(d.cmp, m.SmallestRangeKey) {
			// This file will probably contain range keys.
			if err := loadItersIfNecessary(); err != nil {
				return nil, err
			}
			smallestRangeKey := m.SmallestRangeKey
			// Store the min of (exciseSpan.Start, rkey.End) in lastRangeKey. This
			// needs to be a copy if the key is owned by the range key iter.
			var lastRangeKey []byte
			var lastRangeKeyKind InternalKeyKind
			if rkey, err := iters.RangeKey().SeekLT(exciseSpan.Start); err != nil {
				return nil, err
			} else if rkey != nil {
				lastRangeKey = append(lastRangeKey[:0], rkey.End...)
				if d.cmp(lastRangeKey, exciseSpan.Start) > 0 {
					lastRangeKey = exciseSpan.Start
				}
				lastRangeKeyKind = rkey.Keys[0].Kind()
			}
			if lastRangeKey != nil {
				leftFile.ExtendRangeKeyBounds(d.cmp, smallestRangeKey, base.MakeExclusiveSentinelKey(lastRangeKeyKind, lastRangeKey))
			}
		}
		if leftFile.HasRangeKeys || leftFile.HasPointKeys {
			var err error
			leftFile.Size, err = d.fileCache.estimateSize(m, leftFile.Smallest.UserKey, leftFile.Largest.UserKey)
			if err != nil {
				return nil, err
			}
			if leftFile.Size == 0 {
				// On occasion, estimateSize gives us a low estimate, i.e. a 0 file size,
				// such as if the excised file only has range keys/dels and no point
				// keys. This can cause panics in places where we divide by file sizes.
				// Correct for it here.
				leftFile.Size = 1
			}
			if err := leftFile.Validate(d.cmp, d.opts.Comparer.FormatKey); err != nil {
				return nil, err
			}
			leftFile.ValidateVirtual(m)
			ve.NewTables = append(ve.NewTables, newTableEntry{Level: level, Meta: leftFile})
			needsBacking = true
			numCreatedFiles++
		}
	}
	// Create a file to the right, if necessary.
	if exciseSpan.ContainsInternalKey(d.cmp, m.Largest) {
		// No key exists to the right of the excise span in this file.
		if needsBacking && !m.Virtual {
			// If m is virtual, then its file backing is already known to the manifest.
			// We don't need to create another file backing. Note that there must be
			// only one CreatedBackingTables entry per backing sstable. This is
			// indicated by the VersionEdit.CreatedBackingTables invariant.
			ve.CreatedBackingTables = append(ve.CreatedBackingTables, m.FileBacking)
		}
		return ve.NewTables[len(ve.NewTables)-numCreatedFiles:], nil
	}
	// Create a new file, rightFile, between [firstKeyAfter(exciseSpan.End), m.Largest].
	//
	// See comment before the definition of leftFile for the motivation behind
	// calculating tight user-key bounds.
	rightFile := &tableMetadata{
		Virtual:     true,
		FileBacking: m.FileBacking,
		FileNum:     d.mu.versions.getNextFileNum(),
		// Note that these are loose bounds for smallest/largest seqnums, but they're
		// sufficient for maintaining correctness.
		SmallestSeqNum:           m.SmallestSeqNum,
		LargestSeqNum:            m.LargestSeqNum,
		LargestSeqNumAbsolute:    m.LargestSeqNumAbsolute,
		SyntheticPrefixAndSuffix: m.SyntheticPrefixAndSuffix,
	}
	if m.HasPointKeys && !exciseSpan.ContainsInternalKey(d.cmp, m.LargestPointKey) {
		// This file will probably contain point keys
		if err := loadItersIfNecessary(); err != nil {
			return nil, err
		}
		largestPointKey := m.LargestPointKey
		if kv := iters.Point().SeekGE(exciseSpan.End.Key, base.SeekGEFlagsNone); kv != nil {
			if exciseSpan.End.Kind == base.Inclusive && d.equal(exciseSpan.End.Key, kv.K.UserKey) {
				return nil, base.AssertionFailedf("cannot excise with an inclusive end key and data overlap at end key")
			}
			rightFile.ExtendPointKeyBounds(d.cmp, kv.K.Clone(), largestPointKey)
		}
		// Store the max of (exciseSpan.End, rdel.Start) in firstRangeDel. This
		// needs to be a copy if the key is owned by the range del iter.
		var firstRangeDel []byte
		rdel, err := iters.RangeDeletion().SeekGE(exciseSpan.End.Key)
		if err != nil {
			return nil, err
		} else if rdel != nil {
			firstRangeDel = append(firstRangeDel[:0], rdel.Start...)
			if d.cmp(firstRangeDel, exciseSpan.End.Key) < 0 {
				// NB: This can only be done if the end bound is exclusive.
				if exciseSpan.End.Kind != base.Exclusive {
					return nil, base.AssertionFailedf("cannot truncate rangedel during excise with an inclusive upper bound")
				}
				firstRangeDel = exciseSpan.End.Key
			}
		}
		if firstRangeDel != nil {
			smallestPointKey := rdel.SmallestKey()
			smallestPointKey.UserKey = firstRangeDel
			rightFile.ExtendPointKeyBounds(d.cmp, smallestPointKey, largestPointKey)
		}
	}
	if m.HasRangeKeys && !exciseSpan.ContainsInternalKey(d.cmp, m.LargestRangeKey) {
		// This file will probably contain range keys.
		if err := loadItersIfNecessary(); err != nil {
			return nil, err
		}
		largestRangeKey := m.LargestRangeKey
		// Store the max of (exciseSpan.End, rkey.Start) in firstRangeKey. This
		// needs to be a copy if the key is owned by the range key iter.
		var firstRangeKey []byte
		rkey, err := iters.RangeKey().SeekGE(exciseSpan.End.Key)
		if err != nil {
			return nil, err
		} else if rkey != nil {
			firstRangeKey = append(firstRangeKey[:0], rkey.Start...)
			if d.cmp(firstRangeKey, exciseSpan.End.Key) < 0 {
				if exciseSpan.End.Kind != base.Exclusive {
					return nil, base.AssertionFailedf("cannot truncate range key during excise with an inclusive upper bound")
				}
				firstRangeKey = exciseSpan.End.Key
			}
		}
		if firstRangeKey != nil {
			smallestRangeKey := rkey.SmallestKey()
			smallestRangeKey.UserKey = firstRangeKey
			// We call ExtendRangeKeyBounds so any internal boundType fields are
			// set correctly. Note that this is mildly wasteful as we'll be comparing
			// rightFile.{Smallest,Largest}RangeKey with themselves, which can be
			// avoided if we exported ExtendOverallKeyBounds or so.
			rightFile.ExtendRangeKeyBounds(d.cmp, smallestRangeKey, largestRangeKey)
		}
	}
	if rightFile.HasRangeKeys || rightFile.HasPointKeys {
		var err error
		rightFile.Size, err = d.fileCache.estimateSize(m, rightFile.Smallest.UserKey, rightFile.Largest.UserKey)
		if err != nil {
			return nil, err
		}
		if rightFile.Size == 0 {
			// On occasion, estimateSize gives us a low estimate, i.e. a 0 file size,
			// such as if the excised file only has range keys/dels and no point keys.
			// This can cause panics in places where we divide by file sizes. Correct
			// for it here.
			rightFile.Size = 1
		}
		if err := rightFile.Validate(d.cmp, d.opts.Comparer.FormatKey); err != nil {
			return nil, err
		}
		rightFile.ValidateVirtual(m)
		ve.NewTables = append(ve.NewTables, newTableEntry{Level: level, Meta: rightFile})
		needsBacking = true
		numCreatedFiles++
	}

	if needsBacking && !m.Virtual {
		// If m is virtual, then its file backing is already known to the manifest.
		// We don't need to create another file backing. Note that there must be
		// only one CreatedBackingTables entry per backing sstable. This is
		// indicated by the VersionEdit.CreatedBackingTables invariant.
		ve.CreatedBackingTables = append(ve.CreatedBackingTables, m.FileBacking)
	}

	return ve.NewTables[len(ve.NewTables)-numCreatedFiles:], nil
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
