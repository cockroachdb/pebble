// Copyright 2021 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package sstable

import (
	"bytes"
	"fmt"
	"math"
	"math/rand"
	"sort"
	"strconv"
	"strings"
	"testing"

	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/pebble/internal/base"
	"github.com/cockroachdb/pebble/internal/datadriven"
	"github.com/cockroachdb/pebble/internal/rangekey"
	"github.com/cockroachdb/pebble/internal/testkeys"
	"github.com/stretchr/testify/require"
)

func TestIntervalEncodeDecode(t *testing.T) {
	testCases := []struct {
		name  string
		lower uint64
		upper uint64
		len   int
	}{
		{
			name:  "empty zero",
			lower: 0,
			upper: 0,
			len:   0,
		},
		{
			name:  "empty non-zero",
			lower: 5,
			upper: 5,
			len:   0,
		},
		{
			name:  "empty lower > upper",
			lower: math.MaxUint64,
			upper: math.MaxUint64 - 1,
			len:   0,
		},
		{
			name:  "small",
			lower: 50,
			upper: 61,
			len:   2,
		},
		{
			name:  "big",
			lower: 0,
			upper: math.MaxUint64,
			len:   11,
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
			arbitraryInterval := interval{lower: 1000, upper: 1000}
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
		name       string
		i1         interval
		i2         interval
		union      interval
		intersects bool
	}{
		{
			name:       "empty and empty",
			i1:         interval{},
			i2:         interval{},
			union:      interval{},
			intersects: false,
		},
		{
			name:       "empty and empty non-zero",
			i1:         interval{},
			i2:         interval{100, 99},
			union:      interval{},
			intersects: false,
		},
		{
			name:       "empty and non-empty",
			i1:         interval{},
			i2:         interval{80, 100},
			union:      interval{80, 100},
			intersects: false,
		},
		{
			name:       "disjoint sets",
			i1:         interval{50, 60},
			i2:         interval{math.MaxUint64 - 5, math.MaxUint64},
			union:      interval{50, math.MaxUint64},
			intersects: false,
		},
		{
			name:       "adjacent sets",
			i1:         interval{50, 60},
			i2:         interval{60, 100},
			union:      interval{50, 100},
			intersects: false,
		},
		{
			name:       "overlapping sets",
			i1:         interval{50, 60},
			i2:         interval{59, 120},
			union:      interval{50, 120},
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

type testDataBlockIntervalCollector struct {
	i interval
}

func (c *testDataBlockIntervalCollector) Add(key InternalKey, value []byte) error {
	return nil
}

func (c *testDataBlockIntervalCollector) FinishDataBlock() (lower uint64, upper uint64, err error) {
	return c.i.lower, c.i.upper, nil
}

func TestBlockIntervalCollector(t *testing.T) {
	var points, ranges testDataBlockIntervalCollector
	bic := NewBlockIntervalCollector("foo", &points, &ranges)
	require.Equal(t, "foo", bic.Name())
	// Set up the point key collector with an initial (empty) interval.
	points.i = interval{1, 1}
	// First data block has empty point key interval.
	encoded, err := bic.FinishDataBlock(nil)
	require.NoError(t, err)
	require.True(t, bytes.Equal(nil, encoded))
	bic.AddPrevDataBlockToIndexBlock()
	// Second data block contains a point and range key interval. The latter
	// should not contribute to the block interval.
	points.i = interval{20, 25}
	ranges.i = interval{5, 150}
	encoded, err = bic.FinishDataBlock(nil)
	require.NoError(t, err)
	var decoded interval
	require.NoError(t, decoded.decode(encoded))
	require.Equal(t, interval{20, 25}, decoded)
	var encodedIndexBlock []byte
	// Finish index block before including second data block.
	encodedIndexBlock, err = bic.FinishIndexBlock(nil)
	require.NoError(t, err)
	require.True(t, bytes.Equal(nil, encodedIndexBlock))
	bic.AddPrevDataBlockToIndexBlock()
	// Third data block.
	points.i = interval{10, 15}
	encoded, err = bic.FinishDataBlock(nil)
	require.NoError(t, err)
	require.NoError(t, decoded.decode(encoded))
	require.Equal(t, interval{10, 15}, decoded)
	bic.AddPrevDataBlockToIndexBlock()
	// Fourth data block.
	points.i = interval{100, 105}
	encoded, err = bic.FinishDataBlock(nil)
	require.NoError(t, err)
	require.NoError(t, decoded.decode(encoded))
	require.Equal(t, interval{100, 105}, decoded)
	// Finish index block before including fourth data block.
	encodedIndexBlock, err = bic.FinishIndexBlock(nil)
	require.NoError(t, err)
	require.NoError(t, decoded.decode(encodedIndexBlock))
	require.Equal(t, interval{10, 25}, decoded)
	bic.AddPrevDataBlockToIndexBlock()
	// Finish index block that contains only fourth data block.
	encodedIndexBlock, err = bic.FinishIndexBlock(nil)
	require.NoError(t, err)
	require.NoError(t, decoded.decode(encodedIndexBlock))
	require.Equal(t, interval{100, 105}, decoded)
	var encodedTable []byte
	// Finish table. The table interval is the union of the current point key
	// table interval [10, 105) and the range key interval [5, 150).
	encodedTable, err = bic.FinishTable(nil)
	require.NoError(t, err)
	require.NoError(t, decoded.decode(encodedTable))
	require.Equal(t, interval{5, 150}, decoded)
}

func TestBlockIntervalFilter(t *testing.T) {
	testCases := []struct {
		name       string
		filter     interval
		prop       interval
		intersects bool
	}{
		{
			name:       "non-empty and empty",
			filter:     interval{10, 15},
			prop:       interval{},
			intersects: false,
		},
		{
			name:       "does not intersect",
			filter:     interval{10, 15},
			prop:       interval{15, 20},
			intersects: false,
		},
		{
			name:       "intersects",
			filter:     interval{10, 15},
			prop:       interval{14, 20},
			intersects: true,
		},
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			var points testDataBlockIntervalCollector
			name := "foo"
			bic := NewBlockIntervalCollector(name, &points, nil)
			bif := NewBlockIntervalFilter(name, tc.filter.lower, tc.filter.upper)
			points.i = tc.prop
			prop, _ := bic.FinishDataBlock(nil)
			intersects, err := bif.Intersects(prop)
			require.NoError(t, err)
			require.Equal(t, tc.intersects, intersects)
		})
	}
}

func TestBlockPropertiesEncoderDecoder(t *testing.T) {
	var encoder blockPropertiesEncoder
	scratch := encoder.getScratchForProp()
	scratch = append(scratch, []byte("foo")...)
	encoder.addProp(1, scratch)
	scratch = encoder.getScratchForProp()
	require.LessOrEqual(t, 3, cap(scratch))
	scratch = append(scratch, []byte("cockroach")...)
	encoder.addProp(10, scratch)
	props1 := encoder.props()
	unsafeProps := encoder.unsafeProps()
	require.True(t, bytes.Equal(props1, unsafeProps), "%x != %x", props1, unsafeProps)
	decodeProps1 := func() {
		decoder := blockPropertiesDecoder{props: props1}
		require.False(t, decoder.done())
		id, prop, err := decoder.next()
		require.NoError(t, err)
		require.Equal(t, shortID(1), id)
		require.Equal(t, string(prop), "foo")
		require.False(t, decoder.done())
		id, prop, err = decoder.next()
		require.NoError(t, err)
		require.Equal(t, shortID(10), id)
		require.Equal(t, string(prop), "cockroach")
		require.True(t, decoder.done())
	}
	decodeProps1()

	encoder.resetProps()
	scratch = encoder.getScratchForProp()
	require.LessOrEqual(t, 9, cap(scratch))
	scratch = append(scratch, []byte("bar")...)
	encoder.addProp(10, scratch)
	props2 := encoder.props()
	unsafeProps = encoder.unsafeProps()
	require.True(t, bytes.Equal(props2, unsafeProps), "%x != %x", props2, unsafeProps)
	// Safe props should still decode.
	decodeProps1()
	// Decode props2
	decoder := blockPropertiesDecoder{props: props2}
	require.False(t, decoder.done())
	id, prop, err := decoder.next()
	require.NoError(t, err)
	require.Equal(t, shortID(10), id)
	require.Equal(t, string(prop), "bar")
	require.True(t, decoder.done())
}

// filterWithTrueForEmptyProp is a wrapper for BlockPropertyFilter that
// delegates to it except when the property is empty, in which case it returns
// true.
type filterWithTrueForEmptyProp struct {
	BlockPropertyFilter
}

func (b filterWithTrueForEmptyProp) Intersects(prop []byte) (bool, error) {
	if len(prop) == 0 {
		return true, nil
	}
	return b.BlockPropertyFilter.Intersects(prop)
}

func TestBlockPropertiesFilterer_IntersectsUserPropsAndFinishInit(t *testing.T) {
	// props with id=0, interval [10, 20); id=10, interval [110, 120).
	var dbic testDataBlockIntervalCollector
	bic0 := NewBlockIntervalCollector("p0", &dbic, nil)
	bic0Id := byte(0)
	bic10 := NewBlockIntervalCollector("p10", &dbic, nil)
	bic10Id := byte(10)
	dbic.i = interval{10, 20}
	prop0 := append([]byte(nil), bic0Id)
	_, err := bic0.FinishDataBlock(nil)
	require.NoError(t, err)
	prop0, err = bic0.FinishTable(prop0)
	require.NoError(t, err)
	dbic.i = interval{110, 120}
	prop10 := append([]byte(nil), bic10Id)
	_, err = bic10.FinishDataBlock(nil)
	require.NoError(t, err)
	prop10, err = bic10.FinishTable(prop10)
	require.NoError(t, err)
	prop0Str := string(prop0)
	prop10Str := string(prop10)
	type filter struct {
		name string
		i    interval
	}
	testCases := []struct {
		name      string
		userProps map[string]string
		filters   []filter

		// Expected results
		intersects            bool
		shortIDToFiltersIndex []int
	}{
		{
			name:       "no filter, no props",
			userProps:  map[string]string{},
			filters:    nil,
			intersects: true,
		},
		{
			name:      "no props",
			userProps: map[string]string{},
			filters: []filter{
				{name: "p0", i: interval{20, 30}},
				{name: "p10", i: interval{20, 30}},
			},
			intersects: true,
		},
		{
			name:      "prop0, does not intersect",
			userProps: map[string]string{"p0": prop0Str},
			filters: []filter{
				{name: "p0", i: interval{20, 30}},
				{name: "p10", i: interval{20, 30}},
			},
			intersects: false,
		},
		{
			name:      "prop0, intersects",
			userProps: map[string]string{"p0": prop0Str},
			filters: []filter{
				{name: "p0", i: interval{11, 21}},
				{name: "p10", i: interval{20, 30}},
			},
			intersects:            true,
			shortIDToFiltersIndex: []int{0},
		},
		{
			name:      "prop10, does not intersect",
			userProps: map[string]string{"p10": prop10Str},
			filters: []filter{
				{name: "p0", i: interval{11, 21}},
				{name: "p10", i: interval{20, 30}},
			},
			intersects: false,
		},
		{
			name:      "prop10, intersects",
			userProps: map[string]string{"p10": prop10Str},
			filters: []filter{
				{name: "p0", i: interval{11, 21}},
				{name: "p10", i: interval{115, 125}},
			},
			intersects:            true,
			shortIDToFiltersIndex: []int{-1, -1, -1, -1, -1, -1, -1, -1, -1, -1, 1},
		},
		{
			name:      "prop10, intersects",
			userProps: map[string]string{"p10": prop10Str},
			filters: []filter{
				{name: "p10", i: interval{115, 125}},
				{name: "p0", i: interval{11, 21}},
			},
			intersects:            true,
			shortIDToFiltersIndex: []int{-1, -1, -1, -1, -1, -1, -1, -1, -1, -1, 0},
		},
		{
			name:      "prop0 and prop10, does not intersect",
			userProps: map[string]string{"p0": prop0Str, "p10": prop10Str},
			filters: []filter{
				{name: "p10", i: interval{115, 125}},
				{name: "p0", i: interval{20, 30}},
			},
			intersects:            false,
			shortIDToFiltersIndex: []int{-1, -1, -1, -1, -1, -1, -1, -1, -1, -1, 0},
		},
		{
			name:      "prop0 and prop10, does not intersect",
			userProps: map[string]string{"p0": prop0Str, "p10": prop10Str},
			filters: []filter{
				{name: "p0", i: interval{10, 20}},
				{name: "p10", i: interval{125, 135}},
			},
			intersects:            false,
			shortIDToFiltersIndex: []int{0},
		},
		{
			name:      "prop0 and prop10, intersects",
			userProps: map[string]string{"p0": prop0Str, "p10": prop10Str},
			filters: []filter{
				{name: "p10", i: interval{115, 125}},
				{name: "p0", i: interval{10, 20}},
			},
			intersects:            true,
			shortIDToFiltersIndex: []int{1, -1, -1, -1, -1, -1, -1, -1, -1, -1, 0},
		},
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			var filters []BlockPropertyFilter
			for _, f := range tc.filters {
				filter := NewBlockIntervalFilter(f.name, f.i.lower, f.i.upper)
				filters = append(filters, filter)
			}
			filterer := NewBlockPropertiesFilterer(filters)
			intersects, err := filterer.IntersectsUserPropsAndFinishInit(tc.userProps)
			require.NoError(t, err)
			require.Equal(t, tc.intersects, intersects)
			require.Equal(t, tc.shortIDToFiltersIndex, filterer.shortIDToFiltersIndex)
		})
	}
}

func TestBlockPropertiesFilterer_Intersects(t *testing.T) {
	// Setup two different properties values to filter against.
	var emptyProps []byte
	// props with id=0, interval [10, 20); id=10, interval [110, 120).
	var encoder blockPropertiesEncoder
	var dbic testDataBlockIntervalCollector
	bic0 := NewBlockIntervalCollector("", &dbic, nil)
	bic0Id := shortID(0)
	bic10 := NewBlockIntervalCollector("", &dbic, nil)
	bic10Id := shortID(10)
	dbic.i = interval{10, 20}
	prop, err := bic0.FinishDataBlock(encoder.getScratchForProp())
	require.NoError(t, err)
	encoder.addProp(bic0Id, prop)
	dbic.i = interval{110, 120}
	prop, err = bic10.FinishDataBlock(encoder.getScratchForProp())
	require.NoError(t, err)
	encoder.addProp(bic10Id, prop)
	props0And10 := encoder.props()
	type filter struct {
		shortID                shortID
		i                      interval
		intersectsForEmptyProp bool
	}
	testCases := []struct {
		name  string
		props []byte
		// filters must be in ascending order of shortID.
		filters    []filter
		intersects bool
	}{
		{
			name:       "no filter, empty props",
			props:      emptyProps,
			intersects: true,
		},
		{
			name:       "no filter",
			props:      props0And10,
			intersects: true,
		},
		{
			name:  "filter 0, empty props, does not intersect",
			props: emptyProps,
			filters: []filter{
				{
					shortID: 0,
					i:       interval{5, 15},
				},
			},
			intersects: false,
		},
		{
			name:  "filter 10, empty props, does not intersect",
			props: emptyProps,
			filters: []filter{
				{
					shortID: 0,
					i:       interval{105, 111},
				},
			},
			intersects: false,
		},
		{
			name:  "filter 0, intersects",
			props: props0And10,
			filters: []filter{
				{
					shortID: 0,
					i:       interval{5, 15},
				},
			},
			intersects: true,
		},
		{
			name:  "filter 0, does not intersect",
			props: props0And10,
			filters: []filter{
				{
					shortID: 0,
					i:       interval{20, 25},
				},
			},
			intersects: false,
		},
		{
			name:  "filter 10, intersects",
			props: props0And10,
			filters: []filter{
				{
					shortID: 10,
					i:       interval{105, 111},
				},
			},
			intersects: true,
		},
		{
			name:  "filter 10, does not intersect",
			props: props0And10,
			filters: []filter{
				{
					shortID: 10,
					i:       interval{105, 110},
				},
			},
			intersects: false,
		},
		{
			name:  "filter 5, does not intersect since no property",
			props: props0And10,
			filters: []filter{
				{
					shortID: 5,
					i:       interval{105, 110},
				},
			},
			intersects: false,
		},
		{
			name:  "filter 0 and 5, intersects and not intersects means overall not intersects",
			props: props0And10,
			filters: []filter{
				{
					shortID: 0,
					i:       interval{5, 15},
				},
				{
					shortID: 5,
					i:       interval{105, 110},
				},
			},
			intersects: false,
		},
		{
			name:  "filter 0, 5, 7, 11, all intersect",
			props: props0And10,
			filters: []filter{
				{
					shortID: 0,
					i:       interval{5, 15},
				},
				{
					shortID:                5,
					i:                      interval{105, 110},
					intersectsForEmptyProp: true,
				},
				{
					shortID:                7,
					i:                      interval{105, 110},
					intersectsForEmptyProp: true,
				},
				{
					shortID:                11,
					i:                      interval{105, 110},
					intersectsForEmptyProp: true,
				},
			},
			intersects: true,
		},
		{
			name:  "filter 0, 5, 7, 10, 11, all intersect",
			props: props0And10,
			filters: []filter{
				{
					shortID: 0,
					i:       interval{5, 15},
				},
				{
					shortID:                5,
					i:                      interval{105, 110},
					intersectsForEmptyProp: true,
				},
				{
					shortID:                7,
					i:                      interval{105, 110},
					intersectsForEmptyProp: true,
				},
				{
					shortID: 10,
					i:       interval{105, 111},
				},
				{
					shortID:                11,
					i:                      interval{105, 110},
					intersectsForEmptyProp: true,
				},
			},
			intersects: true,
		},
		{
			name:  "filter 0, 5, 7, 10, 11, all intersect except for 10",
			props: props0And10,
			filters: []filter{
				{
					shortID: 0,
					i:       interval{5, 15},
				},
				{
					shortID:                5,
					i:                      interval{105, 110},
					intersectsForEmptyProp: true,
				},
				{
					shortID:                7,
					i:                      interval{105, 110},
					intersectsForEmptyProp: true,
				},
				{
					shortID: 10,
					i:       interval{105, 110},
				},
				{
					shortID:                11,
					i:                      interval{105, 110},
					intersectsForEmptyProp: true,
				},
			},
			intersects: false,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			var filters []BlockPropertyFilter
			var shortIDToFiltersIndex []int
			if len(tc.filters) > 0 {
				shortIDToFiltersIndex = make([]int, tc.filters[len(tc.filters)-1].shortID+1)
				for i := range shortIDToFiltersIndex {
					shortIDToFiltersIndex[i] = -1
				}
			}
			for _, f := range tc.filters {
				filter := NewBlockIntervalFilter("", f.i.lower, f.i.upper)
				bpf := BlockPropertyFilter(filter)
				if f.intersectsForEmptyProp {
					bpf = filterWithTrueForEmptyProp{filter}
				}
				shortIDToFiltersIndex[f.shortID] = len(filters)
				filters = append(filters, bpf)
			}
			doFiltering := func() {
				bpFilterer := BlockPropertiesFilterer{
					filters:               filters,
					shortIDToFiltersIndex: shortIDToFiltersIndex,
				}
				intersects, err := bpFilterer.intersects(tc.props)
				require.NoError(t, err)
				require.Equal(t, tc.intersects, intersects)
			}
			doFiltering()
			if len(filters) > 1 {
				// Permute the filters so that the use of
				// shortIDToFiltersIndex is better tested.
				permutation := rand.Perm(len(filters))
				filterPerm := make([]BlockPropertyFilter, len(filters))
				for i := range permutation {
					filterPerm[i] = filters[permutation[i]]
					shortIDToFiltersIndex[tc.filters[permutation[i]].shortID] = i
				}
				filters = filterPerm
				doFiltering()
			}
		})
	}
}

// valueCharBlockIntervalCollector implements DataBlockIntervalCollector by
// maintaining the (inclusive) lower and (exclusive) upper bound of a fixed
// character position in the value, when represented as an integer.
type valueCharBlockIntervalCollector struct {
	charIdx      int
	initialized  bool
	lower, upper uint64
}

var _ DataBlockIntervalCollector = &valueCharBlockIntervalCollector{}

// Add implements DataBlockIntervalCollector by maintaining the lower and upper
// bound of a fixed character position in the value.
func (c *valueCharBlockIntervalCollector) Add(_ InternalKey, value []byte) error {
	charIdx := c.charIdx
	if charIdx == -1 {
		charIdx = len(value) - 1
	}
	val, err := strconv.Atoi(string(value[charIdx]))
	if err != nil {
		return err
	}
	uval := uint64(val)
	if !c.initialized {
		c.lower, c.upper = uval, uval+1
		c.initialized = true
		return nil
	}
	if uval < c.lower {
		c.lower = uval
	}
	if uval >= c.upper {
		c.upper = uval + 1
	}

	return nil
}

// Finish implements DataBlockIntervalCollector, returning the lower and upper
// bound for the block. The range is reset to zero in anticipation of the next
// block.
func (c *valueCharBlockIntervalCollector) FinishDataBlock() (lower, upper uint64, err error) {
	l, u := c.lower, c.upper
	c.lower, c.upper = 0, 0
	c.initialized = false
	return l, u, nil
}

// suffixIntervalCollector maintains an interval over the timestamps in
// MVCC-like suffixes for keys (e.g. foo@123).
type suffixIntervalCollector struct {
	initialized  bool
	lower, upper uint64
}

// Add implements DataBlockIntervalCollector by adding the timestamp(s) in the
// suffix(es) of this record to the current interval.
//
// Note that range sets and unsets may have multiple suffixes. Range key deletes
// do not have a suffix. All other point keys have a single suffix.
func (c *suffixIntervalCollector) Add(key InternalKey, value []byte) error {
	var bs [][]byte
	// Range keys have their suffixes encoded into the value.
	if rangekey.IsRangeKey(key.Kind()) {
		if key.Kind() == base.InternalKeyKindRangeKeyDelete {
			return nil
		}
		_, v, ok := rangekey.DecodeEndKey(key.Kind(), value)
		if !ok {
			return errors.New("could not decode end key")
		}

		switch key.Kind() {
		case base.InternalKeyKindRangeKeySet:
			var sv rangekey.SuffixValue
			for len(v) > 0 {
				sv, v, ok = rangekey.DecodeSuffixValue(v)
				if !ok {
					return errors.New("could not decode suffix value")
				}
				bs = append(bs, sv.Suffix)
			}
		case base.InternalKeyKindRangeKeyUnset:
			var suffix []byte
			for len(v) > 0 {
				suffix, v, ok = rangekey.DecodeSuffix(v)
				if !ok {
					return errors.New("could not decode suffix")
				}
				bs = append(bs, suffix)
			}
		default:
			return errors.Newf("unexpected range key kind: %s", key.Kind())
		}
	} else {
		// All other keys have a single suffix encoded into the value.
		bs = append(bs, key.UserKey)
	}

	for _, b := range bs {
		i := testkeys.Comparer.Split(b)
		ts, err := strconv.Atoi(string(b[i+1:]))
		if err != nil {
			return err
		}
		uts := uint64(ts)
		if !c.initialized {
			c.lower, c.upper = uts, uts+1
			c.initialized = true
			return nil
		}
		if uts < c.lower {
			c.lower = uts
		}
		if uts >= c.upper {
			c.upper = uts + 1
		}
	}
	return nil
}

// FinishDataBlock implements DataBlockIntervalCollector.
func (c *suffixIntervalCollector) FinishDataBlock() (lower, upper uint64, err error) {
	l, u := c.lower, c.upper
	c.lower, c.upper = 0, 0
	c.initialized = false
	return l, u, nil
}

func TestBlockProperties(t *testing.T) {
	var r *Reader
	defer func() {
		if r != nil {
			require.NoError(t, r.Close())
		}
	}()

	datadriven.RunTest(t, "testdata/block_properties", func(td *datadriven.TestData) string {
		switch td.Cmd {
		case "build":
			if r != nil {
				_ = r.Close()
				r = nil
			}
			var opts WriterOptions
			for _, cmd := range td.CmdArgs {
				switch cmd.Key {
				case "block-size":
					if len(cmd.Vals) != 1 {
						return fmt.Sprintf("%s: arg %s expects 1 value", td.Cmd, cmd.Key)
					}
					var err error
					opts.BlockSize, err = strconv.Atoi(cmd.Vals[0])
					if err != nil {
						return err.Error()
					}
				case "collectors":
					for _, c := range cmd.Vals {
						var points, ranges DataBlockIntervalCollector
						switch c {
						case "value-first":
							points = &valueCharBlockIntervalCollector{charIdx: 0}
						case "value-last":
							points = &valueCharBlockIntervalCollector{charIdx: -1}
						case "suffix":
							points, ranges = &suffixIntervalCollector{}, &suffixIntervalCollector{}
						case "suffix-point-keys-only":
							points = &suffixIntervalCollector{}
						case "suffix-range-keys-only":
							ranges = &suffixIntervalCollector{}
						case "nil-points-and-ranges":
							points, ranges = nil, nil
						default:
							return fmt.Sprintf("unknown collector: %s", c)
						}
						name := c
						opts.BlockPropertyCollectors = append(
							opts.BlockPropertyCollectors,
							func() BlockPropertyCollector {
								return NewBlockIntervalCollector(name, points, ranges)
							})
					}
				}
			}
			var meta *WriterMetadata
			var err error
			func() {
				defer func() {
					if r := recover(); r != nil {
						err = errors.Errorf("%v", r)
					}
				}()
				meta, r, err = runBuildCmd(td, &opts)
			}()
			if err != nil {
				return err.Error()
			}
			return fmt.Sprintf("point:    [%s,%s]\nrangedel: [%s,%s]\nrangekey: [%s,%s]\nseqnums:  [%d,%d]\n",
				meta.SmallestPoint, meta.LargestPoint,
				meta.SmallestRangeDel, meta.LargestRangeDel,
				meta.SmallestRangeKey, meta.LargestRangeKey,
				meta.SmallestSeqNum, meta.LargestSeqNum)

		case "collectors":
			var lines []string
			for k, v := range r.Properties.UserProperties {
				lines = append(lines, fmt.Sprintf("%d: %s", v[0], k))
			}
			linesSorted := sort.StringSlice(lines)
			linesSorted.Sort()
			return strings.Join(lines, "\n")

		case "table-props":
			var lines []string
			for _, val := range r.Properties.UserProperties {
				id := shortID(val[0])
				var i interval
				if err := i.decode([]byte(val[1:])); err != nil {
					return err.Error()
				}
				lines = append(lines, fmt.Sprintf("%d: [%d, %d)", id, i.lower, i.upper))
			}
			linesSorted := sort.StringSlice(lines)
			linesSorted.Sort()
			return strings.Join(lines, "\n")

		case "block-props":
			bh, err := r.readIndex()
			if err != nil {
				return err.Error()
			}
			i, err := newBlockIter(r.Compare, r.Split, bh.Get())
			if err != nil {
				return err.Error()
			}
			defer bh.Release()
			var sb strings.Builder
			for key, val := i.First(); key != nil; key, val = i.Next() {
				sb.WriteString(fmt.Sprintf("%s:\n", key))
				bhp, err := decodeBlockHandleWithProperties(val)
				if err != nil {
					return err.Error()
				}
				d := blockPropertiesDecoder{props: bhp.Props}
				var lines []string
				for !d.done() {
					id, prop, err := d.next()
					if err != nil {
						return err.Error()
					}
					var i interval
					if err := i.decode(prop); err != nil {
						return err.Error()
					}
					lines = append(lines, fmt.Sprintf("  %d: [%d, %d)\n", id, i.lower, i.upper))
				}
				linesSorted := sort.StringSlice(lines)
				linesSorted.Sort()
				for _, line := range lines {
					sb.WriteString(line)
				}
			}
			return sb.String()

		case "filter":
			var (
				name           string
				min, max       uint64
				err            error
				points, ranges []BlockPropertyFilter
			)
			for _, cmd := range td.CmdArgs {
				name = cmd.Vals[0]
				minS, maxS := cmd.Vals[1], cmd.Vals[2]
				min, err = strconv.ParseUint(minS, 10, 64)
				if err != nil {
					return err.Error()
				}
				max, err = strconv.ParseUint(maxS, 10, 64)
				if err != nil {
					return err.Error()
				}

				filter := NewBlockIntervalFilter(name, min, max)
				switch cmd.Key {
				case "point-filter":
					points = append(points, filter)
				case "range-filter":
					ranges = append(ranges, filter)
				default:
					return fmt.Sprintf("unknown command: %s", td.Cmd)
				}
			}

			// Point keys filter matches.
			var buf bytes.Buffer
			var f *BlockPropertiesFilterer
			buf.WriteString("points: ")
			if len(points) > 0 {
				f = NewBlockPropertiesFilterer(points)
				ok, err := f.IntersectsUserPropsAndFinishInit(r.Properties.UserProperties)
				if err != nil {
					return err.Error()
				}
				buf.WriteString(strconv.FormatBool(ok))
				if !ok {
					f = nil
				}

				// Enumerate point key data blocks encoded into the index.
				if f != nil {
					indexH, err := r.readIndex()
					if err != nil {
						return err.Error()
					}
					defer indexH.Release()

					buf.WriteString(", blocks=[")

					var blocks []int
					var i int
					iter, _ := newBlockIter(r.Compare, r.Split, indexH.Get())
					for key, value := iter.First(); key != nil; key, value = iter.Next() {
						bh, err := decodeBlockHandleWithProperties(value)
						if err != nil {
							return err.Error()
						}
						ok, err := f.intersects(bh.Props)
						if err != nil {
							return err.Error()
						}
						if ok {
							blocks = append(blocks, i)
						}
						i++
					}
					for i, b := range blocks {
						buf.WriteString(strconv.Itoa(b))
						if i < len(blocks)-1 {
							buf.WriteString(",")
						}
					}
					buf.WriteString("]")
				}
			} else {
				// Without filters, the table matches by default.
				buf.WriteString("true (no filters provided)")
			}
			buf.WriteString("\n")

			// Range key filter matches.
			buf.WriteString("ranges: ")
			if len(ranges) > 0 {
				f := NewBlockPropertiesFilterer(ranges)
				ok, err := f.IntersectsUserPropsAndFinishInit(r.Properties.UserProperties)
				if err != nil {
					return err.Error()
				}
				buf.WriteString(strconv.FormatBool(ok))
			} else {
				// Without filters, the table matches by default.
				buf.WriteString("true (no filters provided)")
			}
			buf.WriteString("\n")

			return buf.String()

		default:
			return fmt.Sprintf("unknown command: %s", td.Cmd)
		}
	})
}
