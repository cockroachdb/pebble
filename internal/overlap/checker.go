// Copyright 2024 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

// Package overlap provides facilities for checking whether tables have data
// overlap.
package overlap

import (
	"context"
	"slices"

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
	// file can be split to free up the range of interest. SplitFile is not set
	// for L0 (overlapping tables are allowed in L0).
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

// LSMOverlap calculates the LSM overlap for the given region.
func (c *Checker) LSMOverlap(
	ctx context.Context, region base.UserKeyBounds, v *manifest.Version,
) (WithLSM, error) {
	var result WithLSM
	result[0].Result = None
	for sublevel := 0; sublevel < len(v.L0Sublevels.Levels); sublevel++ {
		res, err := c.LevelOverlap(ctx, region, v.L0Sublevels.Levels[sublevel])
		if err != nil {
			return WithLSM{}, err
		}
		if res.Result == Data {
			result[0].Result = Data
			return result, nil
		}
		if res.Result == OnlyBoundary {
			result[0].Result = OnlyBoundary
			// We don't set SplitFile for L0 (tables in L0 are allowed to overlap).
		}
	}
	for level := 1; level < manifest.NumLevels; level++ {
		var err error
		result[level], err = c.LevelOverlap(ctx, region, v.Levels[level].Slice())
		if err != nil {
			return WithLSM{}, err
		}
		if result[level].Result == Data {
			return result, err
		}
	}
	return result, nil
}

// LevelOverlap returns true if there is possible data overlap between a user
// key region and an L0 sublevel or L1+ level.
func (c *Checker) LevelOverlap(
	ctx context.Context, region base.UserKeyBounds, ls manifest.LevelSlice,
) (WithLevel, error) {
	// Quick check: if the target region contains any file boundaries, we assume
	// data overlap. This is a correct assumption in most cases; it is pessimistic
	// only for external ingestions which could have "loose" boundaries. External
	// ingestions are also the most expensive to look at, so we don't want to do
	// that just in the off chance that we'll find a significant empty region at
	// the boundary.
	//
	// This check is important because the region can be very large in the key
	// space and encompass many files, and we don't want to open any of them in
	// that case.
	startIter := ls.Iter()
	file := startIter.SeekGE(c.cmp, region.Start)
	if file == nil {
		// No overlapping files.
		return WithLevel{Result: None}, nil
	}
	fileBounds := file.UserKeyBounds()
	if !region.End.IsUpperBoundFor(c.cmp, fileBounds.Start) {
		// No overlapping files.
		return WithLevel{Result: None}, nil
	}
	if c.cmp(fileBounds.Start, region.Start) >= 0 || region.End.CompareUpperBounds(c.cmp, fileBounds.End) >= 0 {
		// The file ends or starts inside our region; we assume data overlap.
		return WithLevel{Result: Data}, nil
	}
	// We have a single file to look at; its boundaries enclose our region.
	overlap, err := c.DataOverlapWithFile(ctx, region, file)
	if err != nil {
		return WithLevel{}, err
	}
	if overlap {
		return WithLevel{Result: Data}, nil
	}
	return WithLevel{
		Result:    OnlyBoundary,
		SplitFile: file,
	}, nil
}

// DataOverlapWithFile returns true if the given region overlaps with any keys
// or spans in the given table.
func (c *Checker) DataOverlapWithFile(
	ctx context.Context, region base.UserKeyBounds, m *manifest.FileMetadata,
) (bool, error) {
	if overlap, ok := m.OverlapCache.CheckDataOverlap(c.cmp, region); ok {
		return overlap, nil
	}
	// We want to check overlap with file, but we also want to update the cache
	// with useful information. We try to find two data regions r1 and r2 with a
	// space-in between; r1 ends before region.Start and r2 ends at or after
	// region.Start. See overlapcache.C.ReportEmptyRegion().
	var r1, r2 base.UserKeyBounds

	if m.HasPointKeys {
		lt, ge, err := c.pointKeysAroundKey(ctx, region.Start, m)
		if err != nil {
			return false, err
		}
		r1 = base.UserKeyBoundsInclusive(lt, lt)
		r2 = base.UserKeyBoundsInclusive(ge, ge)

		if err := c.extendRegionsWithSpans(ctx, &r1, &r2, region.Start, m, manifest.KeyTypePoint); err != nil {
			return false, err
		}
	}
	if m.HasRangeKeys {
		if err := c.extendRegionsWithSpans(ctx, &r1, &r2, region.Start, m, manifest.KeyTypeRange); err != nil {
			return false, err
		}
	}
	// If the regions now overlap or touch, it's all one big data region.
	if r1.Start != nil && r2.Start != nil && c.cmp(r1.End.Key, r2.Start) >= 0 {
		m.OverlapCache.ReportDataRegion(c.cmp, base.UserKeyBounds{
			Start: r1.Start,
			End:   r2.End,
		})
		return true, nil
	}
	m.OverlapCache.ReportEmptyRegion(c.cmp, r1, r2)
	// There is overlap iff we overlap with r2.
	overlap := r2.Start != nil && region.End.IsUpperBoundFor(c.cmp, r2.Start)
	return overlap, nil
}

// pointKeysAroundKey returns two consecutive point keys: the greatest key that
// is < key and the smallest key that is >= key. If there is no such key, the
// corresponding return value is nil. Both lt and ge are nil if the file
// contains no point keys.
func (c *Checker) pointKeysAroundKey(
	ctx context.Context, key []byte, m *manifest.FileMetadata,
) (lt, ge []byte, _ error) {
	pointBounds := m.UserKeyBoundsByType(manifest.KeyTypePoint)

	points, err := c.iteratorFactory.Points(ctx, m)
	if points == nil || err != nil {
		return nil, nil, err
	}
	defer points.Close()
	switch {
	case c.cmp(key, pointBounds.Start) <= 0:
		kv := points.First()
		if kv != nil {
			ge = slices.Clone(kv.K.UserKey)
		}
	case c.cmp(key, pointBounds.End.Key) > 0:
		kv := points.Last()
		if kv != nil {
			lt = slices.Clone(kv.K.UserKey)
		}
	default:
		kv := points.SeekLT(key, base.SeekLTFlagsNone)
		if kv != nil {
			lt = slices.Clone(kv.K.UserKey)
		}
		if kv = points.Next(); kv != nil {
			ge = slices.Clone(kv.K.UserKey)
		}
	}
	return lt, ge, points.Error()
}

// extendRegionsWithSpans opens a fragment iterator for either range dels or
// range keys (depending n keyType), finds the last span that ends before key
// and the following span, and extends/replaces regions r1 and r2.
func (c *Checker) extendRegionsWithSpans(
	ctx context.Context,
	r1, r2 *base.UserKeyBounds,
	key []byte,
	m *manifest.FileMetadata,
	keyType manifest.KeyType,
) error {
	var iter keyspan.FragmentIterator
	var err error
	if keyType == manifest.KeyTypePoint {
		iter, err = c.iteratorFactory.RangeDels(ctx, m)
	} else {
		iter, err = c.iteratorFactory.RangeKeys(ctx, m)
	}
	if iter == nil || err != nil {
		return err
	}
	defer iter.Close()

	fragmentBounds := m.UserKeyBoundsByType(keyType)
	switch {
	case c.cmp(key, fragmentBounds.Start) <= 0:
		span, err := iter.First()
		if err != nil {
			return err
		}
		c.updateR2(r2, span)

	case !fragmentBounds.End.IsUpperBoundFor(c.cmp, key):
		span, err := iter.Last()
		if err != nil {
			return err
		}
		c.updateR1(r1, span)

	default:
		span, err := iter.SeekGE(key)
		if err != nil {
			return err
		}
		c.updateR2(r2, span)
		span, err = iter.Prev()
		if err != nil {
			return err
		}
		c.updateR1(r1, span)
	}
	return nil
}

// updateR1 updates r1, the region of data that ends before a key of interest.
func (c *Checker) updateR1(r1 *base.UserKeyBounds, s *keyspan.Span) {
	switch {
	case s == nil:

	case r1.Start == nil || c.cmp(r1.End.Key, s.Start) < 0:
		// Region completely to the right of r1.
		*r1 = base.UserKeyBoundsEndExclusive(slices.Clone(s.Start), slices.Clone(s.End))

	case c.cmp(s.End, r1.Start) < 0:
		// Region completely to the left of r1, nothing to do.

	default:
		// Regions are overlapping or touching.
		if c.cmp(s.Start, r1.Start) < 0 {
			r1.Start = slices.Clone(s.Start)
		}
		if c.cmp(r1.End.Key, s.End) < 0 {
			r1.End = base.UserKeyExclusive(slices.Clone(s.End))
		}
	}
}

// updateR2 updates r2, the region of data that ends before a key of interest.
func (c *Checker) updateR2(r2 *base.UserKeyBounds, s *keyspan.Span) {
	switch {
	case s == nil:

	case r2.Start == nil || c.cmp(s.End, r2.Start) < 0:
		// Region completely to the left of r2.
		*r2 = base.UserKeyBoundsEndExclusive(slices.Clone(s.Start), slices.Clone(s.End))

	case c.cmp(r2.End.Key, s.Start) < 0:
		// Region completely to the right of r2, nothing to do.

	default:
		// Regions are overlapping or touching.
		if c.cmp(s.Start, r2.Start) < 0 {
			r2.Start = slices.Clone(s.Start)
		}
		if c.cmp(r2.End.Key, s.End) < 0 {
			r2.End = base.UserKeyExclusive(slices.Clone(s.End))
		}
	}
}
