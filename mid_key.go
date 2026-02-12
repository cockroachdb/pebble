// Copyright 2026 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package pebble

import (
	"context"
	"slices"

	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/pebble/internal/base"
	"github.com/cockroachdb/pebble/internal/manifest"
)

// ApproximateMidKey returns a key that approximately bisects the estimated disk
// usage of the key range [start, end). The returned key can be used by callers
// to divide the range into two subranges of roughly equal size.
//
// Sizes include SST data blocks, value blocks, and referenced blob file data.
// The estimation avoids reading data blocks, using only SST metadata and index
// blocks.
//
// The returned key is a separator key from an SST index block. It is greater
// than or equal to every key in the data block it demarcates, but may not
// correspond to any actual key stored in the database.
//
// If the range contains less data than minSize, or cannot be meaningfully
// bisected, ok is false and midKey is nil.
func (d *DB) ApproximateMidKey(
	ctx context.Context, start, end []byte, minSize uint64,
) (midKey []byte, ok bool, _ error) {
	if err := d.closed.Load(); err != nil {
		panic(err)
	}

	if d.cmp(start, end) >= 0 {
		return nil, false, errors.New("invalid key-range specified (start >= end)")
	}

	readState := d.loadReadState()
	defer readState.unref()

	v := readState.current
	bounds := base.UserKeyBoundsEndExclusive(start, end)

	// Step 1: Collect the size of each SST overlapping [start, end). Each
	// entry records the SST's upper bound (clipped to end), its total size
	// (SST file + blob references), and its metadata.
	type sstSize struct {
		upperBound []byte
		size       uint64
		meta       *manifest.TableMetadata
	}
	var ssts []sstSize

	addFile := func(m *manifest.TableMetadata) {
		size := m.Size + m.EstimatedReferenceSize()
		ub := m.Largest().UserKey
		if d.cmp(ub, end) >= 0 {
			ub = end
		}
		ssts = append(ssts, sstSize{
			upperBound: ub,
			size:       size,
			meta:       m,
		})
	}

	// L0: iterate sublevels individually to avoid transitive expansion.
	for _, ls := range v.L0SublevelFiles {
		for m := range ls.All() {
			if m.Overlaps(d.cmp, &bounds) {
				addFile(m)
			}
		}
	}

	// L1-L6: use Overlaps which returns sorted, non-overlapping slices.
	for level := 1; level < manifest.NumLevels; level++ {
		overlapping := v.Overlaps(level, bounds)
		for m := range overlapping.All() {
			addFile(m)
		}
	}

	if len(ssts) == 0 {
		return nil, false, nil
	}

	// Sort SSTs by upper bound key. L0 SSTs may overlap in key space; we
	// treat each SST's full size as a sequential segment keyed by its upper
	// bound. This is an approximation — L0 files that overlap in key range
	// are in different sublevels and contribute real additional data, so
	// summing their full sizes is appropriate even though the sort doesn't
	// model the overlap precisely.
	slices.SortFunc(ssts, func(a, b sstSize) int {
		return d.cmp(a.upperBound, b.upperBound)
	})

	// Compute total size.
	var totalSize uint64
	for i := range ssts {
		totalSize += ssts[i].size
	}
	if totalSize < minSize {
		return nil, false, nil
	}

	// Step 2: Find the straddling SST — the one whose cumulative size first
	// crosses the midpoint.
	target := totalSize / 2
	var cumulative uint64
	var straddlingIdx int
	for i := range ssts {
		cumulative += ssts[i].size
		if cumulative >= target {
			straddlingIdx = i
			break
		}
	}

	straddling := &ssts[straddlingIdx]
	priorCumulative := cumulative - straddling.size
	deficit := target - priorCumulative

	// If the deficit is 0, the midpoint falls exactly on the boundary before
	// this SST. Use the upper bound of the previous SST.
	if deficit == 0 && straddlingIdx > 0 {
		return slices.Clone(ssts[straddlingIdx-1].upperBound), true, nil
	}

	// If the deficit equals the full SST size, the midpoint falls on this
	// SST's boundary. Use this SST's upper bound, unless it equals end.
	if deficit >= straddling.size {
		if d.cmp(straddling.upperBound, end) >= 0 {
			// Midpoint is at the very end of the range; no useful bisection.
			return nil, false, nil
		}
		return slices.Clone(straddling.upperBound), true, nil
	}

	// Step 3: Refine within the straddling SST via index blocks.
	midKey, err := d.fileCache.estimateMidKey(ctx, straddling.meta, start, deficit)
	if err != nil {
		return nil, false, err
	}
	if midKey == nil {
		// Could not find a mid key within the SST (e.g. very few blocks).
		// Fall back to the SST boundary.
		if d.cmp(straddling.upperBound, end) >= 0 || d.cmp(straddling.upperBound, start) <= 0 {
			return nil, false, nil
		}
		return slices.Clone(straddling.upperBound), true, nil
	}

	// Ensure the returned key is within [start, end).
	if d.cmp(midKey, start) <= 0 || d.cmp(midKey, end) >= 0 {
		return nil, false, nil
	}
	return midKey, true, nil
}
