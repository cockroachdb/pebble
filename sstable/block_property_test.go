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
	var dbic testDataBlockIntervalCollector
	bic := NewBlockIntervalCollector("foo", 3, &dbic)
	require.Equal(t, "foo", bic.Name())
	require.Equal(t, 3, int(bic.ShortID()))
	// Single key to call Add once. The real action is in the other methods.
	key := InternalKey{UserKey: []byte("a")}
	require.NoError(t, bic.Add(key, nil))
	dbic.i = interval{1, 1}
	// First data block has empty interval.
	encoded, err := bic.FinishDataBlock(nil)
	require.NoError(t, err)
	require.True(t, bytes.Equal(nil, encoded))
	bic.AddPrevDataBlockToIndexBlock()
	// Second data block.
	dbic.i = interval{20, 25}
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
	dbic.i = interval{10, 15}
	encoded, err = bic.FinishDataBlock(nil)
	require.NoError(t, err)
	require.NoError(t, decoded.decode(encoded))
	require.Equal(t, interval{10, 15}, decoded)
	bic.AddPrevDataBlockToIndexBlock()
	// Fourth data block.
	dbic.i = interval{100, 105}
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
	// Finish table.
	encodedTable, err = bic.FinishTable(nil)
	require.NoError(t, err)
	require.NoError(t, decoded.decode(encodedTable))
	require.Equal(t, interval{10, 105}, decoded)
}

func TestBlockIntervalFilter(t *testing.T) {
	testCases := []struct {
		name  string
		filter interval
		prop interval
		intersects bool
	}{
		{
			name: "non-empty and empty",
			filter: interval{10, 15},
			prop: interval{},
			intersects: false,
		},
		{
			name: "does not intersect",
			filter: interval{10, 15},
			prop: interval{15, 20},
			intersects: false,
		},
		{
			name: "intersects",
			filter: interval{10, 15},
			prop: interval{14, 20},
			intersects: true,
		},
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			var dbic testDataBlockIntervalCollector
			name := "foo"
			id := uint16(3)
			bic := NewBlockIntervalCollector(name, id, &dbic)
			bif := NewBlockIntervalFilter(name, id, tc.filter.lower, tc.filter.upper)
			dbic.i = tc.prop
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
		require.Equal(t, uint16(1), id)
		require.Equal(t, string(prop), "foo")
		require.False(t, decoder.done())
		id, prop, err = decoder.next()
		require.NoError(t, err)
		require.Equal(t, uint16(10), id)
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
	require.Equal(t, uint16(10), id)
	require.Equal(t, string(prop), "bar")
	require.True(t, decoder.done())
}

// filterWithTRueForEmptyProp is a wrapper for BlockIntervalFilter that
// delegates to it except when the property is empty, in which case it returns
// true.
type filterWithTrueForEmptyProp struct {
	*BlockIntervalFilter
}
func (b filterWithTrueForEmptyProp) Intersects(prop []byte) (bool, error) {
	if len(prop) == 0 {
		return true, nil
	}
	return b.BlockIntervalFilter.Intersects(prop)
}

func TestBlockPropertiesFilterer(t *testing.T) {
	// Setup two different properties values to filter against.
	var emptyProps []byte
	// props with id=0, interval [10, 20); id=10, interval [110, 120).
	var encoder blockPropertiesEncoder
	var dbic testDataBlockIntervalCollector
	bic0 := NewBlockIntervalCollector("", 0, &dbic)
	bic10 := NewBlockIntervalCollector("", 10, &dbic)
	dbic.i = interval{10, 20}
	prop, err := bic0.FinishDataBlock(encoder.getScratchForProp())
	require.NoError(t, err)
	encoder.addProp(bic0.ShortID(), prop)
	dbic.i = interval{110, 120}
	prop, err = bic10.FinishDataBlock(encoder.getScratchForProp())
	require.NoError(t, err)
	encoder.addProp(bic10.ShortID(), prop)
	props0And10 := encoder.props()
	type filter struct {
		shortID uint16
		i interval
		intersectsForEmptyProp bool
	}
	testCases := []struct{
		name string
		props []byte
		filters []filter
		intersects bool
	}{
		{
			name: "no filter, empty props",
			props: emptyProps,
			intersects: true,
		},
		{
			name: "no filter",
			props: props0And10,
			intersects: true,
		},
		{
			name: "filter 0, empty props, does not intersect",
			props: emptyProps,
			filters: []filter{
				{
					shortID: 0,
					i: interval{5, 15},
				},
			},
			intersects: false,
		},
		{
			name: "filter 10, empty props, does not intersect",
			props: emptyProps,
			filters: []filter{
				{
					shortID: 0,
					i: interval{105, 111},
				},
			},
			intersects: false,
		},
		{
			name: "filter 0, intersects",
			props: props0And10,
			filters: []filter{
				{
					shortID: 0,
					i: interval{5, 15},
				},
			},
			intersects: true,
		},
		{
			name: "filter 0, does not intersect",
			props: props0And10,
			filters: []filter{
				{
					shortID: 0,
					i: interval{20, 25},
				},
			},
			intersects: false,
		},
		{
			name: "filter 10, intersects",
			props: props0And10,
			filters: []filter{
				{
					shortID: 10,
					i: interval{105, 111},
				},
			},
			intersects: true,
		},
		{
			name: "filter 10, does not intersect",
			props: props0And10,
			filters: []filter{
				{
					shortID: 10,
					i: interval{105, 110},
				},
			},
			intersects: false,
		},
		{
			name: "filter 5, does not intersect since no property",
			props: props0And10,
			filters: []filter{
				{
					shortID: 5,
					i: interval{105, 110},
				},
			},
			intersects: false,
		},
		{
			name: "filter 0 and 5, intersects and not intersects means overall not intersects",
			props: props0And10,
			filters: []filter{
				{
					shortID: 0,
					i: interval{5, 15},
				},
				{
					shortID: 5,
					i: interval{105, 110},
				},
			},
			intersects: false,
		},
		{
			name: "filter 0, 5, 7, 11, all intersect",
			props: props0And10,
			filters: []filter{
				{
					shortID: 0,
					i: interval{5, 15},
				},
				{
					shortID: 5,
					i: interval{105, 110},
					intersectsForEmptyProp: true,
				},
				{
					shortID: 7,
					i: interval{105, 110},
					intersectsForEmptyProp: true,
				},
				{
					shortID: 11,
					i: interval{105, 110},
					intersectsForEmptyProp: true,
				},
			},
			intersects: true,
		},
		{
			name: "filter 0, 5, 7, 10, 11, all intersect",
			props: props0And10,
			filters: []filter{
				{
					shortID: 0,
					i: interval{5, 15},
				},
				{
					shortID: 5,
					i: interval{105, 110},
					intersectsForEmptyProp: true,
				},
				{
					shortID: 7,
					i: interval{105, 110},
					intersectsForEmptyProp: true,
				},
				{
					shortID: 10,
					i: interval{105, 111},
				},
				{
					shortID: 11,
					i: interval{105, 110},
					intersectsForEmptyProp: true,
				},
			},
			intersects: true,
		},
		{
			name: "filter 0, 5, 7, 10, 11, all intersect except for 10",
			props: props0And10,
			filters: []filter{
				{
					shortID: 0,
					i: interval{5, 15},
				},
				{
					shortID: 5,
					i: interval{105, 110},
					intersectsForEmptyProp: true,
				},
				{
					shortID: 7,
					i: interval{105, 110},
					intersectsForEmptyProp: true,
				},
				{
					shortID: 10,
					i: interval{105, 110},
				},
				{
					shortID: 11,
					i: interval{105, 110},
					intersectsForEmptyProp: true,
				},
			},
			intersects: false,
		},

	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			var filters []BlockPropertyFilter
			for _, f := range tc.filters {
				filter := NewBlockIntervalFilter("", f.shortID, f.i.lower, f.i.upper)
				bpf := BlockPropertyFilter(filter)
				if f.intersectsForEmptyProp {
					bpf = filterWithTrueForEmptyProp{filter}
				}
				filters = append(filters, bpf)
			}
			intersects, err := blockPropertiesFilterer(filters).intersects(tc.props)
			require.NoError(t, err)
			require.Equal(t, tc.intersects, intersects)
		})
	}
}
