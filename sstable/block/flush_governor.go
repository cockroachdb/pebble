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
//
// When allocation size classes are used, we use the allocation class that is
// closest to the target block size. We also take into account the next
// allocation class and use it if it reduces internal fragmentation.
type FlushGovernor struct {
	// We always add another KV to a block if its initial size is below
	// lowWatermark (even if the block is very large after adding the KV). This is
	// a safeguard to avoid very small blocks in the presence of large KVs.
	lowWatermark int
	// We never add another KV to a block if its existing size exceeds
	// highWatermark (unless its initial size is < lowWatermark).
	//
	// When using allocation classes, the high watermark corresponds to the
	// allocation size class that follows the target class. Otherwise, it
	// corresponds to the target block size.
	highWatermark int
	// targetBoundary corresponds to the size class we are targeting; if we are
	// not using allocation size classes, targetBoundary equals highWatermark.
	targetBoundary int
}

// This value is the amount of extra bytes we allocate together with the block
// data. This must be taken into account when taking allocator size classes into
// consideration.
//
// For instance, we may have a block of size 1020B that by itself would fit
// within a 1024B class. However, when loaded into the block cache we also
// allocate space for the cache entry metadata. The new allocation may now only
// fit within a 2048B class, which increases internal fragmentation.
//
// TODO: Consider adding extra slack here for future metadata growth (tradeoff:
// wasted memory). Metadata size increases can cause internal fragmentation when
// reading blocks written by older versions that were aligned to size classes
// according to the old metadata size. An alternative design (e.g. a separate
// slab allocator for metadata) could also avoid this issue.
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
	if len(allocatorSizeClasses) == 0 {
		return makeFlushGovernorNoSizeClasses(targetBlockSize, blockSizeThreshold)
	}
	targetSizeWithOverhead := targetBlockSize + blockAllocationOverhead
	classIdx := findClosestClass(allocatorSizeClasses, targetSizeWithOverhead)
	if classIdx == 0 || classIdx == len(allocatorSizeClasses)-1 {
		// Safeguard if our target isn't inside the known classes.
		return makeFlushGovernorNoSizeClasses(targetBlockSize, blockSizeThreshold)
	}

	var fg FlushGovernor
	fg.lowWatermark = (targetBlockSize*sizeClassAwareThreshold + 99) / 100
	fg.targetBoundary = allocatorSizeClasses[classIdx] - blockAllocationOverhead
	fg.highWatermark = allocatorSizeClasses[classIdx+1] - blockAllocationOverhead
	// Safeguard, in case the threshold is very close to 100.
	fg.lowWatermark = min(fg.lowWatermark, fg.targetBoundary)

	return fg
}

func makeFlushGovernorNoSizeClasses(targetBlockSize int, blockSizeThreshold int) FlushGovernor {
	return FlushGovernor{
		lowWatermark:   (targetBlockSize*blockSizeThreshold + 99) / 100,
		highWatermark:  targetBlockSize,
		targetBoundary: targetBlockSize,
	}
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
	// decrease) when we add a KV to the block; tolerate this by always accepting
	// the new KV.
	if sizeBefore >= sizeAfter {
		return false
	}
	if sizeBefore < fg.lowWatermark {
		return false
	}
	if sizeAfter > fg.highWatermark {
		return true
	}
	if sizeAfter > fg.targetBoundary {
		// Flush, unless we're already past the boundary or the KV is large enough
		// that we would waste less space in the next class.
		if sizeBefore <= fg.targetBoundary && fg.highWatermark-sizeAfter > fg.targetBoundary-sizeBefore {
			return true
		}
	}
	return false
}

func (fg FlushGovernor) String() string {
	return fmt.Sprintf("low watermark: %d\nhigh watermark: %d\ntargetBoundary: %v\n",
		fg.lowWatermark, fg.highWatermark, fg.targetBoundary)
}

// findClosestClass returns the index of the allocation class that is closest to
// target. It can be either larger or smaller.
func findClosestClass(allocatorSizeClasses []int, target int) int {
	// Find the first class >= target.
	i, _ := slices.BinarySearch(allocatorSizeClasses, target)
	if i == len(allocatorSizeClasses) || (i > 0 && target-allocatorSizeClasses[i-1] < allocatorSizeClasses[i]-target) {
		i--
	}
	return i
}
