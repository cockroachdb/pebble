// Copyright 2024 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package pebble

import (
	"context"

	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/pebble/internal/base"
	"github.com/cockroachdb/pebble/internal/keyspan"
	"github.com/cockroachdb/pebble/internal/keyspan/keyspanimpl"
	"github.com/cockroachdb/pebble/internal/manifest"
)

// An overlapChecker provides facilities for checking whether any keys within a
// particular LSM version overlap a set of bounds.
type overlapChecker struct {
	comparer *base.Comparer
	newIters tableNewIters
	opts     IterOptions
	v        *version

	// bufs; reused across calls to avoid allocations.
	upperBoundBuf    []byte
	pointLevelIter   levelIter
	keyspanLevelIter keyspanimpl.LevelIter
}

// DetermineAnyDataOverlapInLevel checks whether any keys within the provided
// bounds and level exist within the checker's associated LSM version. This
// function checks for actual keys within the bounds, performing I/O if
// necessary. It may return false when files within the level overlaps the
// bounds, but the files do not contain any keys within the bounds.
func (c *overlapChecker) DetermineAnyDataOverlapInLevel(
	ctx context.Context, bounds base.UserKeyBounds, level int,
) (bool, error) {
	// Propagating an upper bound can prevent a levelIter from unnecessarily
	// opening files that fall outside bounds if no files within a level overlap
	// the provided bounds.
	c.opts.UpperBound = nil
	if bounds.End.Kind == base.Exclusive {
		c.opts.UpperBound = bounds.End.Key
	} else if c.comparer.ImmediateSuccessor != nil {
		si := c.comparer.Split(bounds.End.Key)
		c.upperBoundBuf = c.comparer.ImmediateSuccessor(c.upperBoundBuf[:0], bounds.End.Key[:si])
		c.opts.UpperBound = c.upperBoundBuf
	}

	// Check for overlap over the keys of L0 by iterating over the sublevels.
	// NB: sublevel 0 contains the newest keys, whereas sublevel n contains the
	// oldest keys.
	if level == 0 {
		for subLevel := 0; subLevel < len(c.v.L0SublevelFiles); subLevel++ {
			manifestIter := c.v.L0Sublevels.Levels[subLevel].Iter()
			pointOverlap, err := c.determinePointKeyOverlapInLevel(
				ctx, bounds, manifest.Level(0), manifestIter)
			if err != nil || pointOverlap {
				return pointOverlap, err
			}
			rangeOverlap, err := c.determineRangeKeyOverlapInLevel(
				ctx, bounds, manifest.Level(0), manifestIter)
			if err != nil || rangeOverlap {
				return rangeOverlap, err
			}
		}
		return false, nil
	}

	pointManifestIter := c.v.Levels[level].Iter()
	pointOverlap, err := c.determinePointKeyOverlapInLevel(
		ctx, bounds, manifest.Level(level), pointManifestIter)
	if pointOverlap || err != nil {
		return pointOverlap, err
	}
	rangeManifestIter := c.v.RangeKeyLevels[level].Iter()
	return c.determineRangeKeyOverlapInLevel(
		ctx, bounds, manifest.Level(level), rangeManifestIter)
}

func (c *overlapChecker) determinePointKeyOverlapInLevel(
	ctx context.Context,
	bounds base.UserKeyBounds,
	level manifest.Level,
	metadataIter manifest.LevelIterator,
) (bool, error) {
	// Check for overlapping point keys.
	{
		c.pointLevelIter.init(ctx, c.opts, c.comparer, c.newIters, metadataIter, level, internalIterOpts{})
		pointOverlap, err := determineOverlapPointIterator(c.comparer.Compare, bounds, &c.pointLevelIter)
		err = errors.CombineErrors(err, c.pointLevelIter.Close())
		if pointOverlap || err != nil {
			return pointOverlap, err
		}
	}
	// Check for overlapping range deletions.
	{
		c.keyspanLevelIter.Init(
			keyspan.SpanIterOptions{}, c.comparer.Compare, tableNewRangeDelIter(ctx, c.newIters),
			metadataIter, level, manifest.KeyTypePoint,
		)
		rangeDeletionOverlap, err := determineOverlapKeyspanIterator(c.comparer.Compare, bounds, &c.keyspanLevelIter)
		err = errors.CombineErrors(err, c.keyspanLevelIter.Close())
		if rangeDeletionOverlap || err != nil {
			return rangeDeletionOverlap, err
		}
	}
	return false, nil
}

func (c *overlapChecker) determineRangeKeyOverlapInLevel(
	ctx context.Context,
	bounds base.UserKeyBounds,
	level manifest.Level,
	metadataIter manifest.LevelIterator,
) (bool, error) {
	// Check for overlapping range keys.
	c.keyspanLevelIter.Init(
		keyspan.SpanIterOptions{}, c.comparer.Compare, tableNewRangeKeyIter(ctx, c.newIters),
		metadataIter, level, manifest.KeyTypeRange,
	)
	rangeKeyOverlap, err := determineOverlapKeyspanIterator(c.comparer.Compare, bounds, &c.keyspanLevelIter)
	return rangeKeyOverlap, errors.CombineErrors(err, c.keyspanLevelIter.Close())
}

func determineOverlapPointIterator(
	cmp base.Compare, bounds base.UserKeyBounds, iter internalIterator,
) (bool, error) {
	kv := iter.SeekGE(bounds.Start, base.SeekGEFlagsNone)
	if kv == nil {
		return false, iter.Error()
	}
	return bounds.End.IsUpperBoundForInternalKey(cmp, kv.K), nil
}

func determineOverlapKeyspanIterator(
	cmp base.Compare, bounds base.UserKeyBounds, iter keyspan.FragmentIterator,
) (bool, error) {
	// NB: The spans surfaced by the fragment iterator are non-overlapping.
	span, err := iter.SeekGE(bounds.Start)
	if err != nil {
		return false, err
	}
	for ; span != nil; span, err = iter.Next() {
		if !bounds.End.IsUpperBoundFor(cmp, span.Start) {
			// The span starts after our bounds.
			return false, nil
		}
		if !span.Empty() {
			return true, nil
		}
	}
	return false, err
}
