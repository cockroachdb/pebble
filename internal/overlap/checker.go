// Copyright 2024 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

// Package overlap provides facilities for checking whether tables have data
// overlap.
package overlap

import (
	"context"

	"github.com/cockroachdb/pebble/internal/base"
	"github.com/cockroachdb/pebble/internal/keyspan"
	"github.com/cockroachdb/pebble/internal/manifest"
)

// WithLSM stores the result of checking for boundary and data overlap between a
// region of key space and the LSM levels, starting from the top (L0) and
// stopping at the highest level with data overlap.
type WithLSM [manifest.NumLevels]WithLevel

// WithLevel is the result of checking overlap against an LSM level.
type WithLevel struct {
	Result Kind
	// SplitFile can be set only when result is OnlyBoundary. If it is set, this
	// file can be split to free up the range of interest.
	SplitFile *manifest.FileMetadata
}

// Kind indicates the kind of overlap detected between a key range and a level.
// We check two types of overlap:
//
//   - file boundary overlap: whether the key range overlaps any of the level's
//     user key boundaries;
//
//   - data overlap: whether the key range overlaps any keys or ranges in the
//     level. Data overlap implies file boundary overlap.
type Kind uint8

const (
	// None indicates that the key range of interest doesn't overlap any tables on
	// the level.
	None Kind = iota + 1
	// OnlyBoundary indicates that there is boundary overlap but no data overlap.
	OnlyBoundary
	// Data indicates that at least a key or range in the level overlaps with the
	// key range of interest. Note that the data overlap check is best-effort and
	// there could be false positives.
	Data
)

// Checker is used to check for data overlap between tables in the LSM and a
// user key region of interest.
type Checker struct {
	cmp             base.Compare
	iteratorFactory IteratorFactory
}

// IteratorFactory is an interface that is used by the Checker to create
// iterators for a given table. All methods can return nil as an empty iterator.
type IteratorFactory interface {
	Points(ctx context.Context, m *manifest.FileMetadata) (base.InternalIterator, error)
	RangeDels(ctx context.Context, m *manifest.FileMetadata) (keyspan.FragmentIterator, error)
	RangeKeys(ctx context.Context, m *manifest.FileMetadata) (keyspan.FragmentIterator, error)
}

// MakeChecker initializes a new Checker.
func MakeChecker(cmp base.Compare, iteratorFactory IteratorFactory) Checker {
	return Checker{
		cmp:             cmp,
		iteratorFactory: iteratorFactory,
	}
}

// LSMOverlap calculates the lsmOverlap for the given region.
func (c *Checker) LSMOverlap(
	ctx context.Context, region base.UserKeyBounds, v *manifest.Version,
) (WithLSM, error) {
	var result WithLSM
	for level := 0; level < manifest.NumLevels; level++ {
		var err error
		result[level], err = c.LevelOverlap(ctx, region, v, level)
		if err != nil || result[level].Result == Data {
			return result, err
		}
	}
	return result, nil
}

// LevelOverlap returns true if there is possible data overlap between an LSM
// level and a user key region.
func (c *Checker) LevelOverlap(
	ctx context.Context, region base.UserKeyBounds, v *manifest.Version, level int,
) (WithLevel, error) {
	// First, check boundary overlap.
	boundaryOverlaps := v.Overlaps(level, region)
	if boundaryOverlaps.Empty() {
		return WithLevel{Result: None}, nil
	}

	overlap, err := c.PossibleOverlap(ctx, region, boundaryOverlaps)
	if err != nil || overlap {
		return WithLevel{Result: Data}, err
	}
	var splitFile *manifest.FileMetadata
	if boundaryOverlaps.Len() == 1 {
		iter := boundaryOverlaps.Iter()
		splitFile = iter.First()
	}
	return WithLevel{
		Result:    OnlyBoundary,
		SplitFile: splitFile,
	}, nil
}

// PossibleOverlap returns true if there is data overlap between a set of files
// in a level and a user key region. If it returns false, all given files have
// no keys or ranges intersecting the region.
//
// The set of files are expected to be those that overlap the region (e.g. the
// result of LevelSlice.Overlaps).
//
// This method is best-effort and allows for false positives (i.e. returning
// true when there is no overlap).
func (c *Checker) PossibleOverlap(
	ctx context.Context, region base.UserKeyBounds, files manifest.LevelSlice,
) (bool, error) {
	// Quick check: if any of the files are completely contained within our
	// region, we report overlap. The only case when this is a false positive is
	// when the file is actually empty (which is possible with virtual tables, but
	// it should be rare).
	//
	// This check is important because the region can be very large in the key
	// space and encompass many files, and we don't want to open any of them in
	// that case.
	//
	// TODO(radu): should we similarly just assume overlap if the region contains
	// fileBounds.Start or fileBounds.End? This can be a false positive only when
	// the bounds are "loose".
	iter := files.Iter()
	for f := iter.First(); f != nil; f = iter.Next() {
		fileBounds := f.UserKeyBounds()
		if region.ContainsBounds(c.cmp, &fileBounds) {
			return true, nil
		}
	}
	// Note: there are at most two files, or we would have returned above.
	for f := iter.First(); f != nil; f = iter.Next() {
		empty, err := c.EmptyRegion(ctx, region, f)
		if err != nil || !empty {
			return true, err
		}
	}
	// Found no overlap.
	return false, nil
}

// EmptyRegion returns true if the given region doesn't overlap with any keys or
// ranges in the given table.
func (c *Checker) EmptyRegion(
	ctx context.Context, region base.UserKeyBounds, m *manifest.FileMetadata,
) (bool, error) {
	empty, err := c.emptyRegionPointsAndRangeDels(ctx, region, m)
	if err != nil || !empty {
		return empty, err
	}
	return c.emptyRegionRangeKeys(ctx, region, m)
}

// emptyRegionPointsAndRangeDels returns true if the file doesn't contain any
// point keys or range del spans that overlap with region.
func (c *Checker) emptyRegionPointsAndRangeDels(
	ctx context.Context, region base.UserKeyBounds, m *manifest.FileMetadata,
) (bool, error) {
	if !m.HasPointKeys {
		return true, nil
	}
	pointBounds := m.UserKeyBoundsByType(manifest.KeyTypePoint)
	if !pointBounds.Overlaps(c.cmp, &region) {
		return true, nil
	}
	points, err := c.iteratorFactory.Points(ctx, m)
	if err != nil {
		return false, err
	}
	if points != nil {
		defer points.Close()
		var kv *base.InternalKV
		if c.cmp(region.Start, pointBounds.Start) <= 0 {
			kv = points.First()
		} else {
			kv = points.SeekGE(region.Start, base.SeekGEFlagsNone)
		}
		if kv == nil && points.Error() != nil {
			return false, points.Error()
		}
		if kv != nil && region.End.IsUpperBoundForInternalKey(c.cmp, kv.K) {
			// Found overlap.
			return false, nil
		}
	}
	rangeDels, err := c.iteratorFactory.RangeDels(ctx, m)
	if err != nil {
		return false, err
	}
	if rangeDels != nil {
		defer rangeDels.Close()
		empty, err := c.emptyFragmentRegion(region, pointBounds.Start, rangeDels)
		if err != nil || !empty {
			return empty, err
		}
	}
	// Found no overlap.
	return true, nil
}

// emptyRegionRangeKeys returns true if the file doesn't contain any range key
// spans that overlap with region.
func (c *Checker) emptyRegionRangeKeys(
	ctx context.Context, region base.UserKeyBounds, m *manifest.FileMetadata,
) (bool, error) {
	if !m.HasRangeKeys {
		return true, nil
	}
	rangeKeyBounds := m.UserKeyBoundsByType(manifest.KeyTypeRange)
	if !rangeKeyBounds.Overlaps(c.cmp, &region) {
		return true, nil
	}
	rangeKeys, err := c.iteratorFactory.RangeKeys(ctx, m)
	if err != nil {
		return false, err
	}
	if rangeKeys != nil {
		defer rangeKeys.Close()
		empty, err := c.emptyFragmentRegion(region, rangeKeyBounds.Start, rangeKeys)
		if err != nil || !empty {
			return empty, err
		}
	}
	// Found no overlap.
	return true, nil
}

// emptyFragmentRegion returns true if the given iterator doesn't contain any
// spans that overlap with region. The fragmentLowerBounds is a known lower
// bound for all the spans.
func (c *Checker) emptyFragmentRegion(
	region base.UserKeyBounds, fragmentLowerBound []byte, fragments keyspan.FragmentIterator,
) (bool, error) {
	var span *keyspan.Span
	var err error
	if c.cmp(region.Start, fragmentLowerBound) <= 0 {
		span, err = fragments.First()
	} else {
		span, err = fragments.SeekGE(region.Start)
	}
	if err != nil {
		return false, err
	}
	if span != nil && span.Empty() {
		return false, base.AssertionFailedf("fragment iterator produced empty span")
	}
	if span != nil && region.End.IsUpperBoundFor(c.cmp, span.Start) {
		// Found overlap.
		return false, nil
	}
	return true, nil
}
