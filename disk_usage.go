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

	sizes := d.fileSizeAnnotator.VersionRangeAnnotation(readState.current, bounds)
	return sizes.totalSize, sizes.remoteSize, sizes.externalSize, nil
}

// fileSizeByBacking contains the estimated file size for LSM data within some
// bounds. It is broken down by backing type. The file size refers to both the
// sstable size and an estimate of the referenced blob sizes.
type fileSizeByBacking struct {
	// totalSize is the estimated size of all files for the given bounds.
	totalSize uint64
	// remoteSize is the estimated size of remote files for the given bounds.
	remoteSize uint64
	// externalSize is the estimated size of external files for the given bounds.
	externalSize uint64
}

func (d *DB) singleFileSizeByBacking(
	fileSize uint64, t *manifest.TableMetadata,
) (_ fileSizeByBacking, ok bool) {
	res := fileSizeByBacking{
		totalSize: fileSize,
	}

	objMeta, err := d.objProvider.Lookup(base.FileTypeTable, t.TableBacking.DiskFileNum)
	if err != nil {
		return res, false
	}
	if objMeta.IsRemote() {
		res.remoteSize += fileSize
		if objMeta.IsExternal() {
			res.externalSize += fileSize
		}
	}
	return res, true
}

var fileSizeAnnotatorIdx = manifest.NewTableAnnotationIdx()

// makeFileSizeAnnotator returns an annotator that computes the storage size of
// files. When applicable, this includes both the sstable size and the size of
// any referenced blob files.
func (d *DB) makeFileSizeAnnotator() manifest.TableAnnotator[fileSizeByBacking] {
	return manifest.MakeTableAnnotator[fileSizeByBacking](
		fileSizeAnnotatorIdx,
		manifest.TableAnnotatorFuncs[fileSizeByBacking]{
			Merge: func(dst *fileSizeByBacking, src fileSizeByBacking) {
				dst.totalSize += src.totalSize
				dst.remoteSize += src.remoteSize
				dst.externalSize += src.externalSize
			},
			Table: func(f *manifest.TableMetadata) (v fileSizeByBacking, cacheOK bool) {
				return d.singleFileSizeByBacking(f.Size+f.EstimatedReferenceSize(), f)
			},
			PartialOverlap: func(f *manifest.TableMetadata, bounds base.UserKeyBounds) fileSizeByBacking {
				overlappingFileSize, err := d.fileCache.estimateSize(f, bounds.Start, bounds.End.Key)
				if err != nil {
					return fileSizeByBacking{}
				}
				overlapFraction := float64(overlappingFileSize) / float64(f.Size)
				// Scale the blob reference size proportionally to the file
				// overlap from the bounds to approximate only the blob
				// references that overlap with the requested bounds.
				size := overlappingFileSize + uint64(float64(f.EstimatedReferenceSize())*overlapFraction)
				res, _ := d.singleFileSizeByBacking(size, f)
				return res
			},
		})
}
