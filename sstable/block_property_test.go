// Copyright 2021 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package sstable

import (
	"bytes"
	"github.com/stretchr/testify/require"
	"math"
	"testing"
)

func TestIntervalEncodeDecode(t *testing.T) {
	testCases := []struct {
		name  string
		lower uint64
		upper uint64
		len int
	}{
		{
			name: "empty zero",
			lower: 0,
			upper: 0,
			len: 0,
		},
		{
			name: "empty non-zero",
			lower: 5,
			upper: 5,
			len: 0,
		},
		{
			name: "empty lower > upper",
			lower: math.MaxUint64,
			upper: math.MaxUint64-1,
			len: 0,
		},
		{
			name: "small",
			lower: 50,
			upper: 61,
			len: 2,
		},
		{
			name: "big",
			lower: 0,
			upper: math.MaxUint64,
			len: 11,
		},
	}
	for _, tc := range testCases {
		buf := make([]byte, 100)
		t.Run(tc.name, func(t *testing.T) {
			i1 := interval{lower: tc.lower, upper: tc.upper}
			b1 := i1.encode(nil)
			b2 := i1.encode(buf[:0])
			require.True(t, bytes.Equal(b1, b2), "%x != %x", b1, b2)
			expectedInterval := i1
			if expectedInterval.lower >= expectedInterval.upper {
				expectedInterval = interval{}
			}
			// Arbitrary initial value.
			arbitraryInterval := interval{lower: 1000, upper:1000}
			i2 := arbitraryInterval
			i2.decode(b1)
			require.Equal(t, expectedInterval, i2)
			i2 = arbitraryInterval
			i2.decode(b2)
			require.Equal(t, expectedInterval, i2)
			require.Equal(t, tc.len, len(b1))
		})
	}
}

func TestIntervalUnionIntersects(t *testing.T) {
	testCases := []struct {
		name  string
		i1 interval
		i2 interval
		union interval
		intersects bool
	}{
		{
			name: "empty and empty",
			i1: interval{},
			i2: interval{},
			union: interval{},
			intersects: false,
		},
		{
			name: "empty and empty non-zero",
			i1: interval{},
			i2: interval{100, 99},
			union: interval{},
			intersects: false,
		},
		{
			name: "empty and non-empty",
			i1: interval{},
			i2: interval{80, 100},
			union: interval{80, 100},
			intersects: false,
		},
		{
			name: "disjoint sets",
			i1: interval{50, 60},
			i2: interval{math.MaxUint64-5, math.MaxUint64},
			union: interval{50, math.MaxUint64},
			intersects: false,
		},
		{
			name: "adjacent sets",
			i1: interval{50, 60},
			i2: interval{60, 100},
			union: interval{50, 100},
			intersects: false,
		},
		{
			name: "overlapping sets",
			i1: interval{50, 60},
			i2: interval{59, 120},
			union: interval{50, 120},
			intersects: true,
		},
	}
	isEmpty := func(i interval) bool {
		return i.lower >= i.upper
	}
	// adjustUnionExpectation exists because union does not try to
	// canonicalize empty sets by turning them into [0, 0), since it is
	// unnecessary -- the higher level context of the BlockIntervalCollector
	// will do so when calling interval.encode.
	adjustUnionExpectation := func(expected interval, i1 interval, i2 interval) interval {
		if isEmpty(i2) {
			return i1
		}
		if isEmpty(i1) {
			return i2
		}
		return expected
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			require.Equal(t, tc.intersects, tc.i1.intersects(tc.i2))
			require.Equal(t, tc.intersects, tc.i2.intersects(tc.i1))
			require.Equal(t, !isEmpty(tc.i1), tc.i1.intersects(tc.i1))
			require.Equal(t, !isEmpty(tc.i2), tc.i2.intersects(tc.i2))
			union := tc.i1
			union.union(tc.i2)
			require.Equal(t, adjustUnionExpectation(tc.union, tc.i1, tc.i2), union)
			union = tc.i2
			union.union(tc.i1)
			require.Equal(t, adjustUnionExpectation(tc.union, tc.i2, tc.i1), union)
		})
	}
}

func TestBlockIntervalCollector(t *testing.T) {

}

func TestBlockIntervalFilter(t *testing.T) {

}

func TestBlockPropertiesEncoder(t *testing.T) {

}

func TestBlockPropertiesFilterer(t *testing.T) {
	
}
