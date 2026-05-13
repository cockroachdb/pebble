// Copyright 2026 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package pebble

import (
	"context"

	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/pebble/internal/base"
	"github.com/cockroachdb/pebble/internal/manifest"
	"github.com/cockroachdb/pebble/sstable"
)

// ApplySuffixMask applies a suffix mask to all SSTables whose key ranges
// overlap with the given span. Point keys and range key entries with suffixes
// that fall within the range (lower, upper] (as determined by the key schema)
// are masked from all future iterators and eventually dropped during
// compaction.
//
// For SSTables fully contained within the span, the mask is applied by
// modifying the table's metadata in the manifest. For SSTables straddling the
// span boundary, the table is first virtualized (split at the boundary) and the
// mask is applied only to the portion within the span.
//
// If the memtable overlaps the span, it is flushed before applying the mask
// to ensure all in-memory data is in SSTs.
func (d *DB) ApplySuffixMask(ctx context.Context, span KeyRange, lower, upper []byte) error {
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
	if v := d.FormatMajorVersion(); v < FormatVirtualSSTables {
		return errors.Newf(
			"store has format major version %d; ApplySuffixMask requires at least %d",
			v, FormatVirtualSSTables,
		)
	}

	if err := d.FlushIfOverlapping(span); err != nil {
		return err
	}

	suffixMask := sstable.SuffixMask{Lower: lower, Upper: upper}

	d.mu.Lock()
	defer d.mu.Unlock()

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
				if m.SuffixMask.IsSet() {
					// Already has a suffix mask; skip.
					// TODO: could compare and use the tighter mask.
					continue
				}

				fullyContained := d.cmp(span.Start, m.Smallest().UserKey) <= 0 &&
					d.cmp(span.End, m.Largest().UserKey) >= 0

				if fullyContained {
					d.applySuffixMaskToTable(ve, m, level, suffixMask)
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

		return versionUpdate{
			VE: ve,
			InProgressCompactionsFn: func() []compactionInfo {
				return d.getInProgressCompactionInfoLocked(nil)
			},
		}, nil
	})
	return err
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

	numPieces := uint64(1)
	if leftTable != nil {
		numPieces++
	}
	if rightTable != nil {
		numPieces++
	}
	middleTable.Size = m.Size / numPieces
	if middleTable.Size == 0 {
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
			if bounds.End.Kind == base.Inclusive {
				largest = base.MakeRangeDeleteSentinelKey(bounds.End.Key)
			} else {
				largest = base.MakeExclusiveSentinelKey(largest.Kind(), bounds.End.Key)
			}
		}
		middleTable.ExtendPointKeyBounds(cmp, smallest, largest)
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
		middleTable.ExtendRangeKeyBounds(cmp, originalTable.RangeKeyKinds, smallest, largest)
	}
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
