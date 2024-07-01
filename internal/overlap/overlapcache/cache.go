// Copyright 2024 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package overlapcache

import (
	"fmt"
	"sort"
	"sync"

	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/pebble/internal/base"
	"github.com/cockroachdb/pebble/internal/invariants"
)

// C is a data structure that caches information about data regions in a file.
// It is used to speed up related overlap checks during ingestion.
//
// -- Implementation --
//
// The cache maintains information about a small number of regions. A region
// corresponds to a user key interval (UserKeyBounds). We define three types of
// regions:
//   - empty region: it is known that no keys or spans in the file overlap this
//     region.
//   - data region: corresponds to a key or span (or union of keys and spans) in
//     the file. Any single key that falls inside ths region has data overlap.
//   - unknown region.
//
// We maintain a list of disjoint and sorted data regions, along with flags
// which indicate if the regions in-between are empty or unknown. The region
// before data region 0 refers to the entire start of the file up to data region
// 0. THe region after data region n-1 refers to the entire end of the file
// starting from the end of data region n-1.
//
// See testdata/cache for some examples represented visually.
type C struct {
	mu struct {
		sync.Mutex
		n                 int
		dataRegions       [cacheMaxEntries]base.UserKeyBounds
		emptyBeforeRegion [cacheMaxEntries + 1]bool
	}
}

// cacheMaxEntries must be at least 4.
const cacheMaxEntries = 6

// maxKeySize prevents the cache from holding on to very large keys. It is a
// safety precaution.
const maxKeySize = 4096

// CheckDataOverlap tries to determine if the target region overlaps any data
// regions.
func (c *C) CheckDataOverlap(cmp base.Compare, target base.UserKeyBounds) (overlaps, ok bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	n := c.mu.n

	// Find first region which ends after the start of the target region.
	idx := sort.Search(n, func(i int) bool {
		return c.mu.dataRegions[i].End.IsUpperBoundFor(cmp, target.Start)
	})
	if idx < n && target.End.IsUpperBoundFor(cmp, c.mu.dataRegions[idx].Start) {
		// target overlaps with a known data region.
		return true, true
	}
	// The target region falls completely outside regions idx-1 and idx.
	if c.mu.emptyBeforeRegion[idx] {
		// The entire space between data regions idx-1 and idx is known to contain
		// no data.
		return false, true
	}
	// We don't know if there is data in the space between regions idx-1 and idx.
	return false, false
}

// ReportDataRegion informs the cache that the target region contains data.
//
// There is no assumption about the region being maximal (i.e. it could be part
// of a larger data region).
//
// Note that the cache will hold on to the region's key slices indefinitely.
// They should not be modified ever again by the caller.
func (c *C) ReportDataRegion(cmp base.Compare, region base.UserKeyBounds) {
	if len(region.Start) > maxKeySize || len(region.End.Key) > maxKeySize {
		return
	}

	c.mu.Lock()
	defer c.mu.Unlock()
	if invariants.Enabled {
		defer c.check(cmp)
	}
	c.insertRegion(cmp, region, allowLeftExtension|allowRightExtension)
}

// ReportEmptyRegion informs the cache of an empty region, in-between two data
// regions r1 and r2.
//
// Unset regions are accepted and serve as "sentinels" representing the start or
// end of the file. Specifically:
//   - if r1 is unset, the empty region is from the start of the file to the
//     start of r2;
//   - if r2 is unset, the empty region is from the end of r2 to the end of the
//     file;
//   - if both r1 and r2 are unset, the entire file is empty.
//
// There is no assumption about the regions being maximal (i.e. r1 could be part
// of a larger data region extending to the left, and r2 could be part of a
// larger data region extending to the right).
//
// Note that the cache will hold on to the regions' key slices indefinitely.
// They should not be modified ever again by the caller.
func (c *C) ReportEmptyRegion(cmp base.Compare, r1, r2 base.UserKeyBounds) {
	if len(r1.Start) > maxKeySize || len(r1.End.Key) > maxKeySize ||
		len(r2.Start) > maxKeySize || len(r2.End.Key) > maxKeySize {
		return
	}

	c.mu.Lock()
	defer c.mu.Unlock()
	if invariants.Enabled {
		defer c.check(cmp)
	}

	switch {
	case r1.Start == nil && r2.Start == nil:
		// The entire file is empty,
		c.assert(c.mu.n == 0)
		c.mu.emptyBeforeRegion[0] = true
		return

	case r1.Start == nil:
		// We know there is only empty space before r2.
		idx := c.insertRegion(cmp, r2, allowRightExtension)
		c.assert(idx == 0)
		c.mu.emptyBeforeRegion[0] = true
		return

	case r2.Start == nil:
		// We know there is only empty space after r1.
		idx := c.insertRegion(cmp, r1, allowLeftExtension)
		c.assert(idx == c.mu.n-1)
		c.mu.emptyBeforeRegion[c.mu.n] = true
		return
	}

	// Find the first region that contains or ends right at r1.Start.
	r1Idx := c.insertionPoint(cmp, r1)
	r1Overlapping, r1, r1EmptyBefore, _ := c.checkOverlap(cmp, r1Idx, r1, allowLeftExtension)
	r2Idx := r1Idx + r1Overlapping

	r2Overlapping, r2, _, r2EmptyAfter := c.checkOverlap(cmp, r2Idx, r2, allowRightExtension)

	newIdx := c.makeSpace(r1Idx, 2, r2Idx+r2Overlapping)
	c.mu.dataRegions[newIdx] = r1
	c.mu.dataRegions[newIdx+1] = r2
	c.mu.emptyBeforeRegion[newIdx] = r1EmptyBefore
	c.mu.emptyBeforeRegion[newIdx+1] = true
	c.mu.emptyBeforeRegion[newIdx+2] = r2EmptyAfter
}

// insertionPoint returns the first region that contains or ends right at Start.
// We allow an exclusive end bound "touching" the new region, because we can
// coalesce with it.
func (c *C) insertionPoint(cmp base.Compare, region base.UserKeyBounds) int {
	return sort.Search(c.mu.n, func(i int) bool {
		return cmp(c.mu.dataRegions[i].End.Key, region.Start) >= 0
	})
}

// insertRegion inserts a data region, evicting a region if necessary. Returns
// the index where it was inserted.
func (c *C) insertRegion(
	cmp base.Compare, region base.UserKeyBounds, extension allowedExtension,
) (idx int) {
	idx = c.insertionPoint(cmp, region)
	overlapping, extendedRegion, emptyBefore, emptyAfter := c.checkOverlap(cmp, idx, region, extension)
	idx = c.makeSpace(idx, 1, idx+overlapping)
	c.mu.dataRegions[idx] = extendedRegion
	c.mu.emptyBeforeRegion[idx] = emptyBefore
	c.mu.emptyBeforeRegion[idx+1] = emptyAfter
	return idx
}

// allowedExtension represents in which direction it is legal for checkOverlap
// to extend a region; used for sanity checking.
type allowedExtension uint8

const (
	allowLeftExtension allowedExtension = 1 << iota
	allowRightExtension
)

// numOverlappingRegions is called with idx pointing to the first region that
// ends after region.Start and returns the number of regions that overlap with
// (or touch) the target region.
func (c *C) checkOverlap(
	cmp base.Compare, idx int, region base.UserKeyBounds, extension allowedExtension,
) (numOverlapping int, extendedRegion base.UserKeyBounds, emptyBefore, emptyAfter bool) {
	for ; ; numOverlapping++ {
		if idx+numOverlapping >= c.mu.n || cmp(region.End.Key, c.mu.dataRegions[idx+numOverlapping].Start) < 0 {
			break
		}
	}

	// Extend the region if necessary.
	extendedRegion = region
	if numOverlapping > 0 {
		switch cmp(c.mu.dataRegions[idx].Start, region.Start) {
		case -1:
			c.assert(extension&allowLeftExtension != 0)
			extendedRegion.Start = c.mu.dataRegions[idx].Start
			fallthrough
		case 0:
			emptyBefore = c.mu.emptyBeforeRegion[idx]
		}

		switch c.mu.dataRegions[idx+numOverlapping-1].End.CompareUpperBounds(cmp, region.End) {
		case 1:
			c.assert(extension&allowRightExtension != 0)
			extendedRegion.End = c.mu.dataRegions[idx+numOverlapping-1].End
		case 0:
			emptyAfter = c.mu.emptyBeforeRegion[idx+numOverlapping]
		}
	}
	return numOverlapping, extendedRegion, emptyBefore, emptyAfter
}

// makeSpace is used to retain regions [0, keepLeftIdx) and [keepRightIdx, n)
// and leave space for <newRegions> regions in-between.
//
// When necessary, makeSpace evicts regions to make room for the new regions.
//
// Returns the index for the first new region (this equals keepLeftIdx when
// there is no eviction).
func (c *C) makeSpace(keepLeftIdx, newRegions, keepRightIdx int) (firstSpaceIdx int) {
	start := 0
	end := c.mu.n
	newLen := keepLeftIdx + newRegions + (c.mu.n - keepRightIdx)
	for ; newLen > cacheMaxEntries; newLen-- {
		// The result doesn't fit, so we have to evict a region. We choose to evict
		// either the first or the last region, whichever keeps the new region(s)
		// closer to the center. The reasoning is that we want to optimize for the
		// case where we get repeated queries around the same region of interest.
		if (keepLeftIdx - start) > (end - keepRightIdx) {
			start++
			c.mu.emptyBeforeRegion[start] = false
		} else {
			end--
			c.mu.emptyBeforeRegion[end] = false
		}
	}
	c.moveRegions(start, keepLeftIdx, 0)
	c.moveRegions(keepRightIdx, end, keepLeftIdx-start+newRegions)
	if newLen < c.mu.n {
		// Clear the now unused regions so we don't hold on to key slices.
		clear(c.mu.dataRegions[newLen:c.mu.n])
	}
	c.mu.n = newLen
	return keepLeftIdx - start
}

// moveRegions copies the regions [startIdx, endIdx) to
// [newStartIdx, newStartIdx+endIdx-startIdx). The emptyBeforeRegion flags for
// [startIdx, endIdx] are also copied.
func (c *C) moveRegions(startIdx, endIdx int, newStartIdx int) {
	if startIdx >= endIdx || startIdx == newStartIdx {
		return
	}
	copy(c.mu.dataRegions[newStartIdx:], c.mu.dataRegions[startIdx:endIdx])
	copy(c.mu.emptyBeforeRegion[newStartIdx:], c.mu.emptyBeforeRegion[startIdx:endIdx+1])
}

func (c *C) assert(cond bool) {
	if !cond {
		panic(errors.AssertionFailedf("overlapcache: conflicting information"))
	}
}

func (c *C) check(cmp base.Compare) {
	for i := 0; i < c.mu.n; i++ {
		r := &c.mu.dataRegions[i]
		if !r.Valid(cmp) {
			panic(fmt.Sprintf("invalid region %s", r))
		}
		// Regions must not overlap or touch.
		if i > 0 && cmp(c.mu.dataRegions[i-1].End.Key, r.Start) >= 0 {
			panic(fmt.Sprintf("overlapping regions %s %s", c.mu.dataRegions[i-1], r))
		}
	}
}
