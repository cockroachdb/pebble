// Copyright 2025 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package problemspans

import (
	"cmp"
	"fmt"
	"slices"
	"strings"
	"time"

	"github.com/RaduBerinde/axisds"
	"github.com/RaduBerinde/axisds/regiontree"
	"github.com/cockroachdb/crlib/crtime"
	"github.com/cockroachdb/pebble/internal/base"
)

// Set maintains a set of spans with expiration times and allows checking for
// overlap against non-expired spans.
//
// When the spans added to the set are not overlapping, all operations are
// logarithmic.
//
// Set is not safe for concurrent use.
//
// To avoid blow-up in pathological corner cases, the Set limits the amount of
// keys it stores internally (see SetSizeLimit). When this limit is hit, the
// spans (or fragments of spans) that would expire first are dropped.
type Set struct {
	cmp       base.Compare
	nowFn     func() crtime.Mono
	sizeLimit int

	now crtime.Mono

	// We use a region tree with key boundaries and the expirationTime as a
	// property.
	rt regiontree.T[axisds.Endpoint[[]byte], expirationTime]
}

// SetSizeLimit is the maximum number of regions in the region tree. When we
// exceed this limit, we halve the size by removing the earliest expiring
// regions.
const SetSizeLimit = 1000

// expirationTime of a problem span. 0 means that there is no problem span in a
// region. Expiration times <= Set.now are equivalent to 0.
type expirationTime crtime.Mono

// Init must be called before a Set can be used.
func (s *Set) Init(cmp base.Compare) {
	s.init(cmp, crtime.NowMono, SetSizeLimit)
}

func (s *Set) init(cmp base.Compare, nowFn func() crtime.Mono, sizeLimit int) {
	*s = Set{
		cmp:       cmp,
		nowFn:     nowFn,
		sizeLimit: sizeLimit,
	}
	s.cmp = cmp
	s.nowFn = nowFn
	propEqFn := func(a, b expirationTime) bool {
		return a == b ||
			crtime.Mono(a) <= s.now && crtime.Mono(b) <= s.now // Both are expired or 0.
	}
	endpointCmp := axisds.EndpointCompareFn(axisds.CompareFn[[]byte](cmp))
	s.rt = regiontree.Make(endpointCmp, propEqFn)
}

func boundsToEndpoints(bounds base.UserKeyBounds) (start, end axisds.Endpoint[[]byte]) {
	start = axisds.MakeStartEndpoint(bounds.Start, axisds.Inclusive)
	end = axisds.MakeEndEndpoint(bounds.End.Key, axisds.InclusiveIf(bounds.End.Kind == base.Inclusive))
	return start, end
}

// Add a span to the set. The span automatically expires after the given duration.
func (s *Set) Add(bounds base.UserKeyBounds, expiration time.Duration) {
	s.now = s.nowFn()
	expTime := expirationTime(s.now + crtime.Mono(expiration))
	start, end := boundsToEndpoints(bounds)
	s.rt.Update(start, end, func(p expirationTime) expirationTime {
		return max(p, expTime)
	})
	if s.rt.InternalLen() > s.sizeLimit {
		s.reduce()
	}
}

// Overlaps returns true if the bounds overlap with a non-expired span.
func (s *Set) Overlaps(bounds base.UserKeyBounds) bool {
	s.now = s.nowFn()
	start, end := boundsToEndpoints(bounds)
	overlaps := false
	s.rt.Enumerate(start, end, func(start, end axisds.Endpoint[[]byte], exp expirationTime) bool {
		overlaps = true
		return false
	})
	return overlaps
}

// Excise removes a span fragment from all spans in the set. Any overlapping
// non-expired spans are cut accordingly.
func (s *Set) Excise(bounds base.UserKeyBounds) {
	s.now = s.nowFn()
	start, end := boundsToEndpoints(bounds)
	s.rt.Update(start, end, func(p expirationTime) expirationTime {
		return 0
	})
}

// IsEmpty returns true if the set contains no non-expired spans.
func (s *Set) IsEmpty() bool {
	s.now = s.nowFn()
	return s.rt.IsEmpty()
}

// reduce halves the size of the region table, keeping the latest expiring regions.
// Used to avoid unbounded growth in corner cases.
func (s *Set) reduce() {
	type region struct {
		start, end axisds.Endpoint[[]byte]
		exp        expirationTime
	}
	regions := make([]region, 0, s.rt.InternalLen())
	s.now = s.nowFn()
	s.rt.EnumerateAll(func(start, end axisds.Endpoint[[]byte], prop expirationTime) bool {
		regions = append(regions, region{start: start, end: end, exp: prop})
		return true
	})

	byExpiration := make([]int, len(regions))
	for i := range byExpiration {
		byExpiration[i] = i
	}
	// Sort regions by descending expiration time.
	slices.SortFunc(byExpiration, func(i, j int) int {
		c := cmp.Compare(regions[j].exp, regions[i].exp)
		if c == 0 {
			// Break ties by region index, so the result is predictable.
			return cmp.Compare(i, j)
		}
		return c
	})
	// Clear the set and add back regions until we reach sizeLimit/2.
	s.init(s.cmp, s.nowFn, s.sizeLimit)
	for _, idx := range byExpiration {
		r := regions[idx]
		s.rt.Update(r.start, r.end, func(expirationTime) expirationTime { return r.exp })
		if s.rt.InternalLen() >= s.sizeLimit/2 {
			break
		}
	}
}

// String prints all active (non-expired) span fragments.
func (s *Set) String() string {
	var buf strings.Builder
	s.now = s.nowFn()
	s.rt.EnumerateAll(func(start, end axisds.Endpoint[[]byte], prop expirationTime) bool {
		fmt.Fprintf(&buf, "%s  expires in: %s\n", keyEndpointIntervalFormatter(start, end), time.Duration(prop)-time.Duration(s.now))
		return true
	})
	if buf.Len() == 0 {
		return "<empty>"
	}
	return buf.String()
}

var keyBoundaryFormatter axisds.BoundaryFormatter[[]byte] = func(b []byte) string {
	return string(b)
}

var keyEndpointIntervalFormatter = axisds.MakeEndpointIntervalFormatter(keyBoundaryFormatter)
