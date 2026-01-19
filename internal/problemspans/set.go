// Copyright 2025 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package problemspans

import (
	"fmt"
	"strings"
	"time"

	"github.com/RaduBerinde/axisds/v2"
	"github.com/RaduBerinde/axisds/v2/regiontree"
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
type Set struct {
	cmp   base.Compare
	nowFn func() crtime.Mono

	now crtime.Mono

	// We use a region tree with key boundaries and the expirationTime as a
	// property.
	rt regiontree.T[axisds.Endpoint[[]byte], expirationTime]
}

// expirationTime of a problem span. 0 means that there is no problem span in a
// region. Expiration times <= Set.now are equivalent to 0.
type expirationTime crtime.Mono

// Init must be called before a Set can be used.
func (s *Set) Init(cmp base.Compare) {
	s.init(cmp, crtime.NowMono)
}

func (s *Set) init(cmp base.Compare, nowFn func() crtime.Mono) {
	*s = Set{}
	s.cmp = cmp
	s.nowFn = nowFn
	// The region tree supports a property equality function that "evolves" over
	// time, in that some properties that used to not be equal become equal. In
	// our case expired properties become equal to 0.
	//
	// Note that the region tree automatically removes boundaries between two
	// regions that have expired, even during enumeration.
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
}

// Overlaps returns true if the bounds overlap with a non-expired span.
func (s *Set) Overlaps(bounds base.UserKeyBounds) bool {
	s.now = s.nowFn()
	start, end := boundsToEndpoints(bounds)
	return s.rt.AnyWithGC(start, end, func(exp expirationTime) bool {
		return crtime.Mono(exp) > s.now
	})
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

// Len returns the number of non-overlapping spans that have not expired. Two
// spans that touch are both counted if they have different expiration times.
func (s *Set) Len() int {
	s.now = s.nowFn()
	n := 0
	for range s.rt.All() {
		n++
	}
	return n
}

// String prints all active (non-expired) span fragments.
func (s *Set) String() string {
	var buf strings.Builder
	s.now = s.nowFn()
	for i, exp := range s.rt.All() {
		fmt.Fprintf(&buf, "%s  expires in: %s\n", keyEndpointIntervalFormatter(i), time.Duration(exp)-time.Duration(s.now))
	}
	if buf.Len() == 0 {
		return "<empty>"
	}
	return buf.String()
}

var keyBoundaryFormatter axisds.BoundaryFormatter[[]byte] = func(b []byte) string {
	return string(b)
}

var keyEndpointIntervalFormatter = axisds.MakeEndpointIntervalFormatter(keyBoundaryFormatter)
