// Copyright 2024 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package pebble

import (
	"context"

	"github.com/cockroachdb/pebble/internal/base"
	"github.com/cockroachdb/pebble/internal/keyspan"
	"github.com/cockroachdb/pebble/internal/manifest"
	"github.com/cockroachdb/pebble/internal/overlap"
)

// An overlapChecker provides facilities for checking whether any keys within a
// particular LSM version overlap a set of bounds. It is a thin wrapper for
// dataoverlap.Checker.
type overlapChecker struct {
	comparer  *base.Comparer
	newIters  tableNewIters
	opts      IterOptions
	v         *manifest.Version
	skipProbe bool
}

// DetermineLSMOverlap calculates the overlap.WithLSM for the given bounds.
func (c *overlapChecker) DetermineLSMOverlap(
	ctx context.Context, bounds base.UserKeyBounds,
) (overlap.WithLSM, error) {
	checker := overlap.MakeChecker(c.comparer.Compare, c)
	checker.SkipProbe = c.skipProbe
	return checker.LSMOverlap(ctx, bounds, c.v)
}

var _ overlap.IteratorFactory = (*overlapChecker)(nil)

// Points is part of the overlap.IteratorFactory implementation.
func (c *overlapChecker) Points(
	ctx context.Context, m *manifest.TableMetadata,
) (base.InternalIterator, error) {
	iters, err := c.newIters(ctx, m, &c.opts, internalIterOpts{}, iterPointKeys)
	if err != nil {
		return nil, err
	}
	return iters.point, nil
}

// RangeDels is part of the overlap.IteratorFactory implementation.
func (c *overlapChecker) RangeDels(
	ctx context.Context, m *manifest.TableMetadata,
) (keyspan.FragmentIterator, error) {
	iters, err := c.newIters(ctx, m, &c.opts, internalIterOpts{}, iterRangeDeletions)
	if err != nil {
		return nil, err
	}
	return iters.rangeDeletion, nil
}

// RangeKeys is part of the overlap.IteratorFactory implementation.
func (c *overlapChecker) RangeKeys(
	ctx context.Context, m *manifest.TableMetadata,
) (keyspan.FragmentIterator, error) {
	iters, err := c.newIters(ctx, m, &c.opts, internalIterOpts{}, iterRangeKeys)
	if err != nil {
		return nil, err
	}
	return iters.rangeKey, nil
}
