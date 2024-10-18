// Copyright 2024 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package block

import (
	"fmt"
	"slices"

	"github.com/cockroachdb/pebble/internal/cache"
)

// FlushGovernor is used to decide when to flush a block. It takes into
// consideration a target block size and (optionally) allocation size classes.
type FlushGovernor struct {
	// We always add another KV to a block if its resulting size does not exceed
	// lowWatermark. The low watermark normally corresponds to an allocation size
	// class boundary.
	lowWatermark int
	// We never add another KV to a block if its existing size exceeds
	// highWatermark. The high watermark normally corresponds to the smallest
	// allocation size class that fits a block of the target size.
	highWatermark int
	// We optionally have sorted list of boundaries between lowWatermark and
	// highWatermark, corresponding to boundaries between allocation classes.
	numBoundaries int
	boundaries    [maxFlushBoundaries]int
}

const maxFlushBoundaries = 4

// This value is the amount of extra bytes we allocate together with the block
// data. This must be taken into account when taking allocator size classes into
// consideration.
//
// For instance, we may have a block of size 1020B that by itself would fit
// within a 1024B class. However, when loaded into the block cache we also
// allocate space for the cache entry metadata. The new allocation may now only
// fit within a 2048B class, which increases internal fragmentation.
const blockAllocationOverhead = cache.ValueMetadataSize + MetadataSize

// MakeFlushGovernor initializes a flush controller.
//
// There are two cases:
//
// 1. No allocation classes. If we don't have any allocatorSizeClasses, or
// targetBlockSize doesn't fit between two allocation classes, then we flush
// right before the block would exceed targetBlockSize (except if the block size
// would be smaller than blockSizeThreshold percent of the target, in which case
// we flush right after the target block size is exceeded).
//
// 2. With allocation classes. We take into account allocation size classes no
// smaller than sizeClassAwareThreshold percent of the target block size and up
// to the first class that fits the target block size. We flush near allocation
// class boundaries to minimize wasted memory space in the block cache (internal
// fragmentation).
//
// The FlushGovernor is immutable and can be copied by value.
func MakeFlushGovernor(
	targetBlockSize int,
	blockSizeThreshold int,
	sizeClassAwareThreshold int,
	allocatorSizeClasses []int,
) FlushGovernor {
	var fg FlushGovernor
	targetSizeWithOverhead := targetBlockSize + blockAllocationOverhead
	// Find the smallest size class that is >= targetSizeWithOverhead.
	upperClassIdx, _ := slices.BinarySearch(allocatorSizeClasses, targetSizeWithOverhead)
	if upperClassIdx == 0 || upperClassIdx == len(allocatorSizeClasses) {
		fg.lowWatermark = (targetBlockSize*blockSizeThreshold + 99) / 100
		fg.highWatermark = targetBlockSize
		return fg
	}

	fg.lowWatermark = (targetBlockSize*sizeClassAwareThreshold + 99) / 100
	fg.highWatermark = allocatorSizeClasses[upperClassIdx] - blockAllocationOverhead
	// Just in case the threshold is very close to 100.
	fg.lowWatermark = min(fg.lowWatermark, fg.highWatermark)

	classes := allocatorSizeClasses[max(0, upperClassIdx-maxFlushBoundaries):upperClassIdx]
	// Remove any classes that would result in blocks smaller than lowWatermark.
	for len(classes) > 0 && classes[0]-blockAllocationOverhead < fg.lowWatermark {
		classes = classes[1:]
	}
	fg.numBoundaries = len(classes)
	for i := range classes {
		fg.boundaries[i] = classes[i] - blockAllocationOverhead
	}
	return fg
}

// LowWatermark returns the minimum size of a block that could be flushed.
// ShouldFlush will never return true if sizeBefore is below the low watermark.
//
// This can be used in a "fast path" check that uses an easy-to-compute
// overestimation of the block size.
func (fg *FlushGovernor) LowWatermark() int {
	return fg.lowWatermark
}

// ShouldFlush returns true if we should flush the current block of sizeBefore
// instead of adding another KV that would increase the block to sizeAfter.
func (fg *FlushGovernor) ShouldFlush(sizeBefore int, sizeAfter int) bool {
	// In rare cases it's possible for the size to stay the same (or even
	// decrease) when we add an entry to the block; tolerate this by always
	// accepting the new entry.
	if sizeBefore >= sizeAfter {
		return false
	}
	if sizeBefore < fg.lowWatermark {
		return false
	}
	if sizeAfter > fg.highWatermark {
		return true
	}
	// We have lowWatermark <= sizeBefore <= sizeAfter <= highWatermark.
	// Note that this is always false when there are no boundaries.
	return fg.wastedSpace(sizeBefore) < fg.wastedSpace(sizeAfter)
}

// wastedSpace returns how much memory is wasted by a block of the given size.
// This is the amount of bytes remaining in the allocation class when loading
// the block in the cache.
func (fg *FlushGovernor) wastedSpace(size int) int {
	for i := 0; i < fg.numBoundaries; i++ {
		if fg.boundaries[i] >= size {
			return fg.boundaries[i] - size
		}
	}
	return fg.highWatermark - size
}

func (fg FlushGovernor) String() string {
	return fmt.Sprintf("low watermark: %d\nhigh watermark: %d\nboundaries: %v\n",
		fg.lowWatermark, fg.highWatermark, fg.boundaries[:fg.numBoundaries])
}
