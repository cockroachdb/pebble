// Copyright 2025 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package pebble

import (
	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/pebble/internal/base"
	"github.com/cockroachdb/pebble/internal/manifest"
)

// EstimateDiskUsage returns the estimated filesystem space used in bytes for
// storing the range `[start, end]`. The estimation is computed as follows:
//
//   - For sstables fully contained in the range the whole file size is included.
//   - For sstables partially contained in the range the overlapping data block sizes
//     are included. Even if a data block partially overlaps, or we cannot determine
//     overlap due to abbreviated index keys, the full data block size is included in
//     the estimation. Note that unlike fully contained sstables, none of the
//     meta-block space is counted for partially overlapped files.
//   - For virtual sstables, we use the overlap between start, end and the virtual
//     sstable bounds to determine disk usage.
//   - There may also exist WAL entries for unflushed keys in this range. This
//     estimation currently excludes space used for the range in the WAL.
func (d *DB) EstimateDiskUsage(start, end []byte) (uint64, error) {
	bytes, _, _, err := d.EstimateDiskUsageByBackingType(start, end)
	return bytes, err
}

// EstimateDiskUsageByBackingType is like EstimateDiskUsage but additionally
// returns the subsets of that size in remote ane external files.
func (d *DB) EstimateDiskUsageByBackingType(
	start, end []byte,
) (totalSize, remoteSize, externalSize uint64, _ error) {
	if err := d.closed.Load(); err != nil {
		panic(err)
	}

	bounds := base.UserKeyBoundsInclusive(start, end)
	if !bounds.Valid(d.cmp) {
		return 0, 0, 0, errors.New("invalid key-range specified (start > end)")
	}

	// Grab and reference the current readState. This prevents the underlying
	// files in the associated version from being deleted if there is a concurrent
	// compaction.
	readState := d.loadReadState()
	defer readState.unref()

	totalSize = *d.mu.annotators.totalFileSize.VersionRangeAnnotation(readState.current, bounds)
	remoteSize = *d.mu.annotators.remoteSize.VersionRangeAnnotation(readState.current, bounds)
	externalSize = *d.mu.annotators.externalSize.VersionRangeAnnotation(readState.current, bounds)

	return
}

// makeFileSizeAnnotator returns an annotator that computes the total
// storage size of files that meet some criteria defined by filter. When
// applicable, this includes both the sstable size and the size of any
// referenced blob files.
func (d *DB) makeFileSizeAnnotator(
	filter func(f *manifest.TableMetadata) bool,
) *manifest.TableAnnotator[uint64] {
	return manifest.NewTableAnnotator[uint64](manifest.SumAggregator{
		AccumulateFunc: func(f *manifest.TableMetadata) (uint64, bool) {
			if filter(f) {
				return f.Size + f.EstimatedReferenceSize(), true
			}
			return 0, true
		},
		AccumulatePartialOverlapFunc: func(f *manifest.TableMetadata, bounds base.UserKeyBounds) uint64 {
			if filter(f) {
				overlappingFileSize, err := d.fileCache.estimateSize(f, bounds.Start, bounds.End.Key)
				if err != nil {
					return 0
				}
				overlapFraction := float64(overlappingFileSize) / float64(f.Size)
				// Scale the blob reference size proportionally to the file
				// overlap from the bounds to approximate only the blob
				// references that overlap with the requested bounds.
				return overlappingFileSize + uint64(float64(f.EstimatedReferenceSize())*overlapFraction)
			}
			return 0
		},
	})
}
