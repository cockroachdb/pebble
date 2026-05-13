// Copyright 2026 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package pebble

import (
	"bytes"
	"context"

	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/pebble/internal/base"
	"github.com/cockroachdb/pebble/internal/invariants"
	"github.com/cockroachdb/pebble/internal/manifest"
	"github.com/cockroachdb/pebble/sstable"
)

// DeleteSuffixRange deletes all point keys and range key entries within the
// given key span whose suffixes fall within the range (lower, upper] as
// determined by the key schema. Keys with no suffix are not affected. The
// affected keys are hidden from all future iterators and eventually dropped
// during compaction.
//
// Internally, the deletion is implemented by attaching a suffix mask to the
// metadata of overlapping SSTables. For SSTables fully contained within the
// span, the mask is applied directly. For SSTables straddling the span
// boundary, the table is split into virtual tables so the mask applies only
// to the portion within the span.
//
// If the memtable overlaps the span, it is flushed before applying the mask
// to ensure all in-memory data is in SSTs.
func (d *DB) DeleteSuffixRange(ctx context.Context, span KeyRange, lower, upper []byte) error {
	if err := d.closed.Load(); err != nil {
		panic(err)
	}
	if d.opts.ReadOnly {
		return ErrReadOnly
	}
	if len(lower) == 0 || len(upper) == 0 {
		return errors.New("empty suffix mask bound")
	}
	if !span.Valid() {
		return errors.New("invalid key range")
	}
	if v := d.FormatMajorVersion(); v < FormatColumnarBlocks {
		return errors.Newf(
			"store has format major version %d; DeleteSuffixRange requires at least %d",
			v, FormatColumnarBlocks,
		)
	}

	if err := d.FlushIfOverlapping(span); err != nil {
		return err
	}

	suffixMask := sstable.SuffixMask{Lower: lower, Upper: upper}

	d.mu.Lock()
	defer d.mu.Unlock()

	// Wait for in-progress compactions to complete. Compactions picked
	// against the current version would conflict with the version edit we're
	// about to apply (we delete files and replace them with masked virtual
	// tables).
	for d.mu.compact.compactingCount > 0 {
		d.mu.compact.cond.Wait()
	}

	_, err := d.mu.versions.UpdateVersionLocked(func() (versionUpdate, error) {
		current := d.mu.versions.currentVersion()
		ve := &manifest.VersionEdit{
			DeletedTables: make(map[manifest.DeletedTableEntry]*manifest.TableMetadata),
		}

		bounds := span.UserKeyBounds()

		for level := 0; level < manifest.NumLevels; level++ {
			overlaps := current.Overlaps(level, bounds)
			iter := overlaps.Iter()
			for m := iter.First(); m != nil; m = iter.Next() {
				tableMask := suffixMask
				if m.SuffixMask.IsSet() {
					tableMask = expandSuffixMask(m.SuffixMask, suffixMask)
					if bytes.Equal(tableMask.Lower, m.SuffixMask.Lower) &&
						bytes.Equal(tableMask.Upper, m.SuffixMask.Upper) {
						continue
					}
				}

				fullyContained := d.cmp(span.Start, m.Smallest().UserKey) <= 0 &&
					d.cmp(span.End, m.Largest().UserKey) >= 0

				if fullyContained {
					d.applySuffixMaskToTable(ve, m, level, tableMask)
				} else {
					if err := d.applySuffixMaskToStraddlingTable(ctx, ve, m, level, span, suffixMask); err != nil {
						return versionUpdate{}, err
					}
				}
			}
		}

		if len(ve.DeletedTables) == 0 && len(ve.NewTables) == 0 {
			return versionUpdate{}, nil
		}

		if invariants.Enabled {
			for _, e := range ve.NewTables {
				if err := e.Meta.Validate(d.cmp, d.opts.Comparer.FormatKey); err != nil {
					return versionUpdate{}, errors.AssertionFailedf(
						"DeleteSuffixRange constructed invalid table %s: %v", e.Meta.TableNum, err)
				}
			}
		}

		return versionUpdate{
			VE: ve,
			InProgressCompactionsFn: func() []compactionInfo {
				return d.getInProgressCompactionInfoLocked(nil)
			},
		}, nil
	})
	if err != nil {
		return err
	}
	d.updateReadStateLocked(d.opts.DebugCheck)
	return nil
}

// applySuffixMaskToTable applies the suffix mask to a table that is fully
// contained within the target span.
func (d *DB) applySuffixMaskToTable(
	ve *manifest.VersionEdit, m *manifest.TableMetadata, level int, suffixMask sstable.SuffixMask,
) {
	newMeta := &manifest.TableMetadata{
		TableNum:                 d.mu.versions.getNextTableNum(),
		Size:                     m.Size,
		CreationTime:             m.CreationTime,
		SeqNums:                  m.SeqNums,
		LargestSeqNumAbsolute:    m.LargestSeqNumAbsolute,
		Virtual:                  true,
		SyntheticPrefixAndSuffix: m.SyntheticPrefixAndSuffix,
		SuffixMask:               suffixMask,
		BlobReferenceDepth:       m.BlobReferenceDepth,
		BlobReferences:           m.BlobReferences,
	}
	if m.HasPointKeys {
		newMeta.ExtendPointKeyBounds(
			d.cmp, m.PointKeyBounds.Smallest(), m.PointKeyBounds.Largest())
	}
	if m.HasRangeKeys {
		newMeta.ExtendRangeKeyBounds(
			d.cmp, m.RangeKeyKinds, m.RangeKeyBounds.Smallest(), m.RangeKeyBounds.Largest())
	}
	newMeta.AttachVirtualBacking(m.TableBacking)

	ve.DeletedTables[manifest.DeletedTableEntry{
		Level:   level,
		FileNum: m.TableNum,
	}] = m
	if !m.Virtual {
		ve.CreatedBackingTables = append(ve.CreatedBackingTables, m.TableBacking)
	}
	ve.NewTables = append(ve.NewTables, manifest.NewTableEntry{Level: level, Meta: newMeta})
}

// applySuffixMaskToStraddlingTable handles a table that straddles the span
// boundary. It splits the table into up to three virtual tables so that the
// mask can be applied only to the portion within the span. The portions
// outside the span remain unmasked.
func (d *DB) applySuffixMaskToStraddlingTable(
	ctx context.Context,
	ve *manifest.VersionEdit,
	m *manifest.TableMetadata,
	level int,
	span KeyRange,
	suffixMask sstable.SuffixMask,
) error {
	exciseBounds := span.UserKeyBounds()

	// exciseTable is used here to split the table: it returns the portions
	// of m outside the span, which we keep unmasked. We separately construct
	// a virtual table for the portion inside the span with the mask applied.
	leftTable, rightTable, err := d.exciseTable(ctx, exciseBounds, m, level, tightExciseBoundsIfLocal)
	if err != nil {
		return err
	}

	ve.DeletedTables[manifest.DeletedTableEntry{
		Level:   level,
		FileNum: m.TableNum,
	}] = m
	if !m.Virtual {
		ve.CreatedBackingTables = append(ve.CreatedBackingTables, m.TableBacking)
	}

	// Add outside portions without the bound.
	if leftTable != nil {
		ve.NewTables = append(ve.NewTables, manifest.NewTableEntry{Level: level, Meta: leftTable})
	}
	if rightTable != nil {
		ve.NewTables = append(ve.NewTables, manifest.NewTableEntry{Level: level, Meta: rightTable})
	}

	// Construct the inside portion with loose bounds clamped to the span.
	middleTable := &manifest.TableMetadata{
		TableNum:                 d.mu.versions.getNextTableNum(),
		CreationTime:             m.CreationTime,
		SeqNums:                  m.SeqNums,
		LargestSeqNumAbsolute:    m.LargestSeqNumAbsolute,
		Virtual:                  true,
		SyntheticPrefixAndSuffix: m.SyntheticPrefixAndSuffix,
		SuffixMask:               suffixMask,
		BlobReferenceDepth:       m.BlobReferenceDepth,
	}
	looseMiddleTableBounds(d.cmp, m, middleTable, exciseBounds)
	if !middleTable.HasPointKeys && !middleTable.HasRangeKeys {
		return nil
	}

	used := uint64(0)
	if leftTable != nil {
		used += leftTable.Size
	}
	if rightTable != nil {
		used += rightTable.Size
	}
	if used < m.Size {
		middleTable.Size = m.Size - used
	} else {
		middleTable.Size = 1
	}
	middleTable.AttachVirtualBacking(m.TableBacking)
	determineSuffixMaskBlobReferences(m.BlobReferences, m.Size, middleTable, d.FormatMajorVersion())
	ve.NewTables = append(ve.NewTables, manifest.NewTableEntry{Level: level, Meta: middleTable})
	return nil
}

// looseMiddleTableBounds sets loose bounds on middleTable for the portion of
// originalTable that lies within the given bounds.
func looseMiddleTableBounds(
	cmp Compare, originalTable, middleTable *manifest.TableMetadata, bounds base.UserKeyBounds,
) {
	if originalTable.HasPointKeys {
		smallest := originalTable.PointKeyBounds.Smallest()
		if cmp(smallest.UserKey, bounds.Start) < 0 {
			smallest = base.MakeInternalKey(bounds.Start, 0, base.InternalKeyKindMaxForSSTable)
		}
		largest := originalTable.PointKeyBounds.Largest()
		if largest.IsUpperBoundFor(cmp, bounds.End.Key) {
			largest = base.MakeRangeDeleteSentinelKey(bounds.End.Key)
		}
		if base.InternalCompare(cmp, smallest, largest) < 0 {
			middleTable.ExtendPointKeyBounds(cmp, smallest, largest)
		}
	}
	if originalTable.HasRangeKeys {
		smallest := originalTable.RangeKeyBounds.Smallest()
		if cmp(smallest.UserKey, bounds.Start) < 0 {
			smallest = base.MakeInternalKey(bounds.Start, 0, base.InternalKeyKindRangeKeyMax)
		}
		largest := originalTable.RangeKeyBounds.Largest()
		if largest.IsUpperBoundFor(cmp, bounds.End.Key) {
			if bounds.End.Kind == base.Inclusive {
				largest = base.MakeExclusiveSentinelKey(base.InternalKeyKindRangeKeyMin, bounds.End.Key)
			} else {
				largest = base.MakeExclusiveSentinelKey(largest.Kind(), bounds.End.Key)
			}
		}
		if base.InternalCompare(cmp, smallest, largest) < 0 {
			middleTable.ExtendRangeKeyBounds(cmp, originalTable.RangeKeyKinds, smallest, largest)
		}
	}
}

// expandSuffixMask returns the union of two suffix masks. The bounds are
// compared using bytes.Compare, which gives ascending suffix magnitude order
// for big-endian encoded MVCC timestamps.
func expandSuffixMask(a, b sstable.SuffixMask) sstable.SuffixMask {
	lower, upper := a.Lower, a.Upper
	if bytes.Compare(b.Lower, lower) < 0 {
		lower = b.Lower
	}
	if bytes.Compare(b.Upper, upper) > 0 {
		upper = b.Upper
	}
	return sstable.SuffixMask{Lower: lower, Upper: upper}
}

func determineSuffixMaskBlobReferences(
	originalRefs manifest.BlobReferences,
	originalSize uint64,
	table *manifest.TableMetadata,
	fmv FormatMajorVersion,
) {
	if len(originalRefs) == 0 {
		return
	}
	determineExcisedTableBlobReferences(originalRefs, originalSize, table, fmv)
}
