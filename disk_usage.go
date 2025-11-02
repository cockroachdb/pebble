// Copyright 2025 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package pebble

import (
	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/pebble/internal/base"
	"github.com/cockroachdb/pebble/internal/manifest"
	"github.com/cockroachdb/pebble/metrics"
	"github.com/cockroachdb/pebble/objstorage"
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
// returns the subsets of that size in remote and external files.
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

	sizes := d.tableDiskUsageAnnotator.VersionRangeAnnotation(readState.current, bounds)
	externalSize = sizes.External.TotalBytes()
	remoteSize = externalSize + sizes.Shared.TotalBytes()
	totalSize = remoteSize + sizes.Local.TotalBytes()
	return totalSize, remoteSize, externalSize, nil
}

// TableUsageByPlacement contains space usage information for tables, broken
// down by where they are stored.
//
// Depending on context, this can refer to all tables in the LSM, all tables on
// a level, or tables within some specified bounds (in the latter case, for
// tables overlapping the bounds, the usage is a best-effort estimation).
type TableUsageByPlacement struct {
	metrics.ByPlacement[TableDiskUsage]
}

// Accumulate adds the rhs counts and sizes to the receiver.
func (u *TableUsageByPlacement) Accumulate(rhs TableUsageByPlacement) {
	u.Local.Accumulate(rhs.Local)
	u.Shared.Accumulate(rhs.Shared)
	u.External.Accumulate(rhs.External)
}

// TableDiskUsage contains space usage information for a set of sstables.
type TableDiskUsage struct {
	// Physical contains the count and total size of physical tables in the set.
	Physical metrics.CountAndSize

	// Virtual contains the count and total estimated referenced bytes of virtual
	// tables in the set.
	Virtual metrics.CountAndSize

	// ReferencedBytes contains the total estimated size of values stored in blob
	// files referenced by tables in this set (either physical or virtual).
	ReferencedBytes uint64
}

// TotalBytes returns the sum of all the byte fields.
func (u TableDiskUsage) TotalBytes() uint64 {
	return u.Physical.Bytes + u.Virtual.Bytes + u.ReferencedBytes
}

// Accumulate adds the rhs counts and sizes to the receiver.
func (u *TableDiskUsage) Accumulate(rhs TableDiskUsage) {
	u.Physical.Accumulate(rhs.Physical)
	u.Virtual.Accumulate(rhs.Virtual)
	u.ReferencedBytes += rhs.ReferencedBytes
}

func (d *DB) singleTableDiskUsage(
	fileSize uint64, referencedSize uint64, fileNum base.DiskFileNum, isVirtual bool,
) TableUsageByPlacement {
	u := TableDiskUsage{
		ReferencedBytes: referencedSize,
	}
	if isVirtual {
		u.Virtual.Inc(fileSize)
	} else {
		u.Physical.Inc(fileSize)
	}
	placement := objstorage.Placement(d.objProvider, base.FileTypeTable, fileNum)
	var res TableUsageByPlacement
	res.Set(placement, u)
	return res
}

var tableDiskUsageAnnotatorIdx = manifest.NewTableAnnotationIdx()

// makeTableDiskSpaceUsageAnnotator returns an annotator that computes the
// storage size of files. When applicable, this includes both the sstable size
// and the size of any referenced blob files.
func (d *DB) makeTableDiskSpaceUsageAnnotator() manifest.TableAnnotator[TableUsageByPlacement] {
	return manifest.MakeTableAnnotator[TableUsageByPlacement](
		tableDiskUsageAnnotatorIdx,
		manifest.TableAnnotatorFuncs[TableUsageByPlacement]{
			Merge: (*TableUsageByPlacement).Accumulate,
			Table: func(f *manifest.TableMetadata) (v TableUsageByPlacement, cacheOK bool) {
				return d.singleTableDiskUsage(f.Size, f.EstimatedReferenceSize(), f.TableBacking.DiskFileNum, f.Virtual), true
			},
			PartialOverlap: func(f *manifest.TableMetadata, bounds base.UserKeyBounds) TableUsageByPlacement {
				overlappingFileSize, err := d.fileCache.estimateSize(f, bounds.Start, bounds.End.Key)
				if err != nil {
					return TableUsageByPlacement{}
				}
				overlapFraction := float64(overlappingFileSize) / float64(f.Size)
				// Scale the blob reference size proportionally to the file
				// overlap from the bounds to approximate only the blob
				// references that overlap with the requested bounds.
				referencedSize := uint64(float64(f.EstimatedReferenceSize()) * overlapFraction)
				return d.singleTableDiskUsage(overlappingFileSize, referencedSize, f.TableBacking.DiskFileNum, f.Virtual)
			},
		})
}

var blobFileDiskUsageAnnotatorIdx = manifest.NewBlobAnnotationIdx()

// makeDiskSpaceUsageAnnotator returns an annotator that computes the storage size of
// files. When applicable, this includes both the sstable size and the size of
// any referenced blob files.
func (d *DB) makeBlobFileDiskSpaceUsageAnnotator() manifest.BlobFileAnnotator[metrics.CountAndSizeByPlacement] {
	return manifest.MakeBlobFileAnnotator[metrics.CountAndSizeByPlacement](
		blobFileDiskUsageAnnotatorIdx,
		manifest.BlobFileAnnotatorFuncs[metrics.CountAndSizeByPlacement]{
			Merge: (*metrics.CountAndSizeByPlacement).Accumulate,
			BlobFile: func(m manifest.BlobFileMetadata) (res metrics.CountAndSizeByPlacement, cacheOK bool) {
				placement := objstorage.Placement(d.objProvider, base.FileTypeBlob, m.Physical.FileNum)
				res.Ptr(placement).Inc(m.Physical.Size)
				return res, true
			},
		})
}
