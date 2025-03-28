// Copyright 2021 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package sstable

import (
	"bytes"
	"context"
	"encoding/binary"
	"fmt"
	"io"
	"math"
	"math/bits"
	"math/rand/v2"
	"slices"
	"sort"
	"strconv"
	"strings"
	"testing"

	"github.com/cockroachdb/crlib/testutils/leaktest"
	"github.com/cockroachdb/datadriven"
	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/pebble/internal/base"
	"github.com/cockroachdb/pebble/internal/keyspan"
	"github.com/cockroachdb/pebble/internal/rangekey"
	"github.com/cockroachdb/pebble/internal/testkeys"
	"github.com/cockroachdb/pebble/sstable/block"
	"github.com/stretchr/testify/require"
)

func TestIntervalEncodeDecode(t *testing.T) {
	defer leaktest.AfterTest(t)()
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
			i1 := BlockInterval{Lower: tc.lower, Upper: tc.upper}
			b1 := encodeBlockInterval(i1, nil)
			b2 := encodeBlockInterval(i1, buf[:0])
			require.True(t, bytes.Equal(b1, b2), "%x != %x", b1, b2)
			expectedInterval := i1
			require.Equal(t, tc.len, len(b1))
			if expectedInterval.Lower >= expectedInterval.Upper {
				expectedInterval = BlockInterval{}
			}
			decodeAndCheck(t, b1, expectedInterval)
			decodeAndCheck(t, b2, expectedInterval)
		})
	}
}

func decodeAndCheck(t *testing.T, buf []byte, expected BlockInterval) {
	i2, err := decodeBlockInterval(buf)
	require.NoError(t, err)
	require.Equal(t, expected, i2)
}

func TestIntervalUnionIntersects(t *testing.T) {
	defer leaktest.AfterTest(t)()
	testCases := []struct {
		name       string
		i1         BlockInterval
		i2         BlockInterval
		union      BlockInterval
		intersects bool
	}{
		{
			name:       "empty and empty",
			i1:         BlockInterval{},
			i2:         BlockInterval{},
			union:      BlockInterval{},
			intersects: false,
		},
		{
			name:       "empty and empty non-zero",
			i1:         BlockInterval{},
			i2:         BlockInterval{100, 99},
			union:      BlockInterval{},
			intersects: false,
		},
		{
			name:       "empty and non-empty",
			i1:         BlockInterval{},
			i2:         BlockInterval{80, 100},
			union:      BlockInterval{80, 100},
			intersects: false,
		},
		{
			name:       "disjoint sets",
			i1:         BlockInterval{50, 60},
			i2:         BlockInterval{math.MaxUint64 - 5, math.MaxUint64},
			union:      BlockInterval{50, math.MaxUint64},
			intersects: false,
		},
		{
			name:       "adjacent sets",
			i1:         BlockInterval{50, 60},
			i2:         BlockInterval{60, 100},
			union:      BlockInterval{50, 100},
			intersects: false,
		},
		{
			name:       "overlapping sets",
			i1:         BlockInterval{50, 60},
			i2:         BlockInterval{59, 120},
			union:      BlockInterval{50, 120},
			intersects: true,
		},
	}
	// adjustUnionExpectation exists because union does not try to
	// canonicalize empty sets by turning them into [0, 0), since it is
	// unnecessary -- the higher level context of the BlockIntervalCollector
	// will do so when calling BlockInterval.encode.
	adjustUnionExpectation := func(expected BlockInterval, i1 BlockInterval, i2 BlockInterval) BlockInterval {
		if i2.IsEmpty() {
			return i1
		}
		if i1.IsEmpty() {
			return i2
		}
		return expected
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			require.Equal(t, tc.intersects, tc.i1.Intersects(tc.i2))
			require.Equal(t, tc.intersects, tc.i2.Intersects(tc.i1))
			require.Equal(t, !tc.i1.IsEmpty(), tc.i1.Intersects(tc.i1))
			require.Equal(t, !tc.i2.IsEmpty(), tc.i2.Intersects(tc.i2))
			union := tc.i1
			union.UnionWith(tc.i2)
			require.Equal(t, adjustUnionExpectation(tc.union, tc.i1, tc.i2), union)
			union = tc.i2
			union.UnionWith(tc.i1)
			require.Equal(t, adjustUnionExpectation(tc.union, tc.i2, tc.i1), union)
		})
	}
}

type testIntervalMapper struct{}

// MapPointKey is part of the IntervalMapper interface.
func (testIntervalMapper) MapPointKey(key InternalKey, value []byte) (BlockInterval, error) {
	return stringToInterval(key.UserKey), nil
}

// MapRangeKey is part of the IntervalMapper interface.
func (testIntervalMapper) MapRangeKeys(span Span) (BlockInterval, error) {
	var r BlockInterval
	for _, k := range span.Keys {
		if len(k.Suffix) > 0 {
			r.UnionWith(stringToInterval(k.Suffix))
		}
	}
	return r, nil
}

func stringToInterval(str []byte) BlockInterval {
	ts, err := strconv.Atoi(string(str))
	if err != nil {
		panic(err)
	}
	uts := uint64(ts)
	return BlockInterval{uts, uts + 1}
}

func addTestPointKeys(t *testing.T, bic BlockPropertyCollector, suffixes ...int) {
	for _, suffix := range suffixes {
		k := base.MakeInternalKey([]byte(fmt.Sprint(suffix)), 0, InternalKeyKindSet)
		require.NoError(t, bic.AddPointKey(k, nil))
	}
}

func addTestRangeKeys(t *testing.T, bic BlockPropertyCollector, suffixes ...int) {
	var s keyspan.Span
	s.Start = []byte("a")
	s.End = []byte("b")
	for _, suffix := range suffixes {
		s.Keys = append(s.Keys, keyspan.Key{
			Trailer: base.MakeTrailer(0, base.InternalKeyKindRangeKeySet),
			Suffix:  []byte(fmt.Sprint(suffix)),
			Value:   []byte("foo"),
		})
	}
	require.NoError(t, bic.AddRangeKeys(s))
}

func TestBlockIntervalCollector(t *testing.T) {
	defer leaktest.AfterTest(t)()
	bic := NewBlockIntervalCollector("foo", testIntervalMapper{}, nil /* suffixReplacer */)
	require.Equal(t, "foo", bic.Name())

	// First data block has empty point key interval.
	encoded, err := bic.FinishDataBlock(nil)
	require.NoError(t, err)
	require.True(t, bytes.Equal(nil, encoded))
	bic.AddPrevDataBlockToIndexBlock()
	// Second data block contains a point and range key interval. The latter
	// should not contribute to the block interval.
	addTestPointKeys(t, bic, 20, 24)
	addTestRangeKeys(t, bic, 5, 10, 15)
	addTestRangeKeys(t, bic, 149)
	encoded, err = bic.FinishDataBlock(nil)
	require.NoError(t, err)
	decoded, err := decodeBlockInterval(encoded)
	require.NoError(t, err)
	require.Equal(t, BlockInterval{20, 25}, decoded)
	var encodedIndexBlock []byte
	// Finish index block before including second data block.
	encodedIndexBlock, err = bic.FinishIndexBlock(nil)
	require.NoError(t, err)
	require.True(t, bytes.Equal(nil, encodedIndexBlock))
	bic.AddPrevDataBlockToIndexBlock()
	// Third data block.
	addTestPointKeys(t, bic, 14, 10)
	encoded, err = bic.FinishDataBlock(nil)
	require.NoError(t, err)
	decodeAndCheck(t, encoded, BlockInterval{10, 15})
	bic.AddPrevDataBlockToIndexBlock()
	// Fourth data block.
	addTestPointKeys(t, bic, 100, 104)
	encoded, err = bic.FinishDataBlock(nil)
	require.NoError(t, err)
	decodeAndCheck(t, encoded, BlockInterval{100, 105})
	// Finish index block before including fourth data block.
	encodedIndexBlock, err = bic.FinishIndexBlock(nil)
	require.NoError(t, err)
	decodeAndCheck(t, encodedIndexBlock, BlockInterval{10, 25})
	bic.AddPrevDataBlockToIndexBlock()
	// Finish index block that contains only fourth data block.
	encodedIndexBlock, err = bic.FinishIndexBlock(nil)
	require.NoError(t, err)
	decodeAndCheck(t, encodedIndexBlock, BlockInterval{100, 105})
	var encodedTable []byte
	// Finish table. The table interval is the union of the current point key
	// table interval [10, 105) and the range key interval [5, 150).
	encodedTable, err = bic.FinishTable(nil)
	require.NoError(t, err)
	decodeAndCheck(t, encodedTable, BlockInterval{5, 150})
}

func TestBlockIntervalFilter(t *testing.T) {
	defer leaktest.AfterTest(t)()
	testCases := []struct {
		name       string
		filter     BlockInterval
		prop       BlockInterval
		intersects bool
	}{
		{
			name:       "non-empty and empty",
			filter:     BlockInterval{10, 15},
			prop:       BlockInterval{},
			intersects: false,
		},
		{
			name:       "does not intersect",
			filter:     BlockInterval{10, 15},
			prop:       BlockInterval{15, 20},
			intersects: false,
		},
		{
			name:       "intersects",
			filter:     BlockInterval{10, 15},
			prop:       BlockInterval{14, 20},
			intersects: true,
		},
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			name := "foo"
			// The mapper here won't actually be used.
			bic := NewBlockIntervalCollector(name, &testIntervalMapper{}, nil)
			bif := NewBlockIntervalFilter(name, tc.filter.Lower, tc.filter.Upper, nil)
			bic.(*BlockIntervalCollector).blockInterval = tc.prop
			prop, _ := bic.FinishDataBlock(nil)
			intersects, err := bif.Intersects(prop)
			require.NoError(t, err)
			require.Equal(t, tc.intersects, intersects)
		})
	}
}

func TestBlockPropertiesEncoderDecoder(t *testing.T) {
	defer leaktest.AfterTest(t)()
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

	expect := func(decoder *blockPropertiesDecoder, expectedID shortID, expectedVal string) {
		t.Helper()
		require.False(t, decoder.Done())
		id, prop, err := decoder.Next()
		require.NoError(t, err)
		require.Equal(t, expectedID, id)
		require.Equal(t, string(prop), expectedVal)
	}

	decodeProps1 := func() {
		decoder := makeBlockPropertiesDecoder(11, props1)
		expect(&decoder, 0, "")
		expect(&decoder, 1, "foo")
		for i := shortID(2); i < 10; i++ {
			expect(&decoder, i, "")
		}
		expect(&decoder, 10, "cockroach")
		require.True(t, decoder.Done())
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
	decoder := makeBlockPropertiesDecoder(11, props2)
	for i := shortID(0); i < 10; i++ {
		expect(&decoder, i, "")
	}
	expect(&decoder, 10, "bar")
	require.True(t, decoder.Done())
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

func (b filterWithTrueForEmptyProp) SyntheticSuffixIntersects(
	prop []byte, suffix []byte,
) (bool, error) {
	panic("unimplemented")
}

func TestBlockPropertiesFilterer_IntersectsUserPropsAndFinishInit(t *testing.T) {
	defer leaktest.AfterTest(t)()
	// props with id=0, interval [10, 20); id=10, interval [110, 120).
	bic0 := NewBlockIntervalCollector("p0", testIntervalMapper{}, nil)
	bic0Id := byte(0)
	bic10 := NewBlockIntervalCollector("p10", testIntervalMapper{}, nil)
	bic10Id := byte(10)

	addTestPointKeys(t, bic0, 10, 19)
	_, err := bic0.FinishDataBlock(nil)
	require.NoError(t, err)
	prop0, err := bic0.FinishTable([]byte{bic0Id})
	require.NoError(t, err)

	addTestPointKeys(t, bic10, 110, 119)
	_, err = bic10.FinishDataBlock(nil)
	require.NoError(t, err)
	prop10, err := bic10.FinishTable([]byte{bic10Id})
	require.NoError(t, err)
	prop0Str := string(prop0)
	prop10Str := string(prop10)
	type filter struct {
		name string
		i    BlockInterval
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
				{name: "p0", i: BlockInterval{20, 30}},
				{name: "p10", i: BlockInterval{20, 30}},
			},
			intersects: true,
		},
		{
			name:      "prop0, does not intersect",
			userProps: map[string]string{"p0": prop0Str},
			filters: []filter{
				{name: "p0", i: BlockInterval{20, 30}},
				{name: "p10", i: BlockInterval{20, 30}},
			},
			intersects: false,
		},
		{
			name:      "prop0, intersects",
			userProps: map[string]string{"p0": prop0Str},
			filters: []filter{
				{name: "p0", i: BlockInterval{11, 21}},
				{name: "p10", i: BlockInterval{20, 30}},
			},
			intersects:            true,
			shortIDToFiltersIndex: []int{0},
		},
		{
			name:      "prop10, does not intersect",
			userProps: map[string]string{"p10": prop10Str},
			filters: []filter{
				{name: "p0", i: BlockInterval{11, 21}},
				{name: "p10", i: BlockInterval{20, 30}},
			},
			intersects: false,
		},
		{
			name:      "prop10, intersects",
			userProps: map[string]string{"p10": prop10Str},
			filters: []filter{
				{name: "p0", i: BlockInterval{11, 21}},
				{name: "p10", i: BlockInterval{115, 125}},
			},
			intersects:            true,
			shortIDToFiltersIndex: []int{-1, -1, -1, -1, -1, -1, -1, -1, -1, -1, 1},
		},
		{
			name:      "prop10, intersects",
			userProps: map[string]string{"p10": prop10Str},
			filters: []filter{
				{name: "p10", i: BlockInterval{115, 125}},
				{name: "p0", i: BlockInterval{11, 21}},
			},
			intersects:            true,
			shortIDToFiltersIndex: []int{-1, -1, -1, -1, -1, -1, -1, -1, -1, -1, 0},
		},
		{
			name:      "prop0 and prop10, does not intersect",
			userProps: map[string]string{"p0": prop0Str, "p10": prop10Str},
			filters: []filter{
				{name: "p10", i: BlockInterval{115, 125}},
				{name: "p0", i: BlockInterval{20, 30}},
			},
			intersects:            false,
			shortIDToFiltersIndex: []int{-1, -1, -1, -1, -1, -1, -1, -1, -1, -1, 0},
		},
		{
			name:      "prop0 and prop10, does not intersect",
			userProps: map[string]string{"p0": prop0Str, "p10": prop10Str},
			filters: []filter{
				{name: "p0", i: BlockInterval{10, 20}},
				{name: "p10", i: BlockInterval{125, 135}},
			},
			intersects:            false,
			shortIDToFiltersIndex: []int{0},
		},
		{
			name:      "prop0 and prop10, intersects",
			userProps: map[string]string{"p0": prop0Str, "p10": prop10Str},
			filters: []filter{
				{name: "p10", i: BlockInterval{115, 125}},
				{name: "p0", i: BlockInterval{10, 20}},
			},
			intersects:            true,
			shortIDToFiltersIndex: []int{1, -1, -1, -1, -1, -1, -1, -1, -1, -1, 0},
		},
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			var filters []BlockPropertyFilter
			for _, f := range tc.filters {
				filter := NewBlockIntervalFilter(f.name, f.i.Lower, f.i.Upper, nil)
				filters = append(filters, filter)
			}
			filterer := newBlockPropertiesFilterer(filters, nil, nil)
			intersects, err := filterer.intersectsUserPropsAndFinishInit(tc.userProps)
			require.NoError(t, err)
			require.Equal(t, tc.intersects, intersects)
			require.Equal(t, tc.shortIDToFiltersIndex, filterer.shortIDToFiltersIndex)
		})
	}
}

func TestBlockPropertiesFilterer_Intersects(t *testing.T) {
	defer leaktest.AfterTest(t)()
	// Setup two different properties values to filter against.
	var emptyProps []byte
	// props with id=0, interval [10, 20); id=10, interval [110, 120).
	var encoder blockPropertiesEncoder
	bic0 := NewBlockIntervalCollector("", testIntervalMapper{}, nil)
	bic0Id := shortID(0)
	bic10 := NewBlockIntervalCollector("", testIntervalMapper{}, nil)
	bic10Id := shortID(10)

	addTestPointKeys(t, bic0, 19, 10, 15)
	prop, err := bic0.FinishDataBlock(encoder.getScratchForProp())
	require.NoError(t, err)
	encoder.addProp(bic0Id, prop)

	addTestPointKeys(t, bic10, 110, 119)
	prop, err = bic10.FinishDataBlock(encoder.getScratchForProp())
	require.NoError(t, err)
	encoder.addProp(bic10Id, prop)
	props0And10 := encoder.props()
	type filter struct {
		shortID                shortID
		i                      BlockInterval
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
					i:       BlockInterval{5, 15},
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
					i:       BlockInterval{105, 111},
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
					i:       BlockInterval{5, 15},
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
					i:       BlockInterval{20, 25},
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
					i:       BlockInterval{105, 111},
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
					i:       BlockInterval{105, 110},
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
					i:       BlockInterval{105, 110},
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
					i:       BlockInterval{5, 15},
				},
				{
					shortID: 5,
					i:       BlockInterval{105, 110},
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
					i:       BlockInterval{5, 15},
				},
				{
					shortID:                5,
					i:                      BlockInterval{105, 110},
					intersectsForEmptyProp: true,
				},
				{
					shortID:                7,
					i:                      BlockInterval{105, 110},
					intersectsForEmptyProp: true,
				},
				{
					shortID:                11,
					i:                      BlockInterval{105, 110},
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
					i:       BlockInterval{5, 15},
				},
				{
					shortID:                5,
					i:                      BlockInterval{105, 110},
					intersectsForEmptyProp: true,
				},
				{
					shortID:                7,
					i:                      BlockInterval{105, 110},
					intersectsForEmptyProp: true,
				},
				{
					shortID: 10,
					i:       BlockInterval{105, 111},
				},
				{
					shortID:                11,
					i:                      BlockInterval{105, 110},
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
					i:       BlockInterval{5, 15},
				},
				{
					shortID:                5,
					i:                      BlockInterval{105, 110},
					intersectsForEmptyProp: true,
				},
				{
					shortID:                7,
					i:                      BlockInterval{105, 110},
					intersectsForEmptyProp: true,
				},
				{
					shortID: 10,
					i:       BlockInterval{105, 110},
				},
				{
					shortID:                11,
					i:                      BlockInterval{105, 110},
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
				filter := NewBlockIntervalFilter("", f.i.Lower, f.i.Upper, nil)
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
					boundLimitedShortID:   -1,
				}
				intersects, err := bpFilterer.intersects(tc.props)
				require.NoError(t, err)
				require.Equal(t, tc.intersects, intersects == blockIntersects)
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

// valueCharIntervalMapper implements DataBlockIntervalCollector by maintaining
// the range of values for a fixed character position in the value, when
// represented as an integer.
type valueCharIntervalMapper struct {
	charIdx int
}

var _ IntervalMapper = &valueCharIntervalMapper{}

func (c *valueCharIntervalMapper) MapPointKey(_ InternalKey, value []byte) (BlockInterval, error) {
	charIdx := c.charIdx
	if charIdx == -1 {
		charIdx = len(value) - 1
	}
	val, err := strconv.Atoi(string(value[charIdx]))
	if err != nil {
		return BlockInterval{}, err
	}
	uval := uint64(val)
	return BlockInterval{Lower: uval, Upper: uval + 1}, nil
}

func (c *valueCharIntervalMapper) MapRangeKeys(span Span) (BlockInterval, error) {
	return BlockInterval{}, nil
}

func TestBlockProperties(t *testing.T) {
	defer leaktest.AfterTest(t)()

	var r *Reader
	defer func() {
		if r != nil {
			require.NoError(t, r.Close())
		}
	}()

	var stats base.InternalIteratorStats
	datadriven.RunTest(t, "testdata/block_properties", func(t *testing.T, td *datadriven.TestData) string {
		switch td.Cmd {
		case "build":
			if r != nil {
				_ = r.Close()
				r = nil
			}
			var output string
			r, output = runBlockPropertiesBuildCmd(td)
			return output

		case "collectors":
			return runCollectorsCmd(r, td)

		case "table-props":
			return runTablePropsCmd(r, td)

		case "block-props":
			return runBlockPropsCmd(r)

		case "filter":
			syntheticSuffix := make([]byte, 0, binary.MaxVarintLen64)
			var points, ranges []BlockPropertyFilter
			for _, cmd := range td.CmdArgs {
				if cmd.Key == "synthetic" {
					var suffix uint64
					td.ScanArgs(t, "synthetic", &suffix)
					syntheticSuffix = binary.AppendUvarint(nil, suffix)
					continue
				}
				filter, err := parseIntervalFilter(cmd)
				if err != nil {
					return err.Error()
				}
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
				f = newBlockPropertiesFilterer(points, nil, syntheticSuffix)
				ok, err := f.intersectsUserPropsAndFinishInit(r.Properties.UserProperties)
				if err != nil {
					return err.Error()
				}
				buf.WriteString(strconv.FormatBool(ok))
				if !ok {
					f = nil
				}

				// Enumerate point key data blocks encoded into the index.
				if f != nil {
					indexH, err := r.readTopLevelIndexBlock(context.Background(), block.NoReadEnv, noReadHandle)
					if err != nil {
						return err.Error()
					}
					defer indexH.Release()

					buf.WriteString(", blocks=[")

					var blocks []int
					var i int
					iter := r.tableFormat.newIndexIter()
					if err := iter.Init(r.Comparer, indexH.BlockData(), NoTransforms); err != nil {
						return err.Error()
					}
					for valid := iter.First(); valid; valid = iter.Next() {
						bh, err := iter.BlockHandleWithProperties()
						if err != nil {
							return err.Error()
						}
						intersects, err := f.intersects(bh.Props)
						if err != nil {
							return err.Error()
						}
						if intersects == blockIntersects {
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
				f := newBlockPropertiesFilterer(ranges, nil, nil)
				ok, err := f.intersectsUserPropsAndFinishInit(r.Properties.UserProperties)
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

		case "iter":
			var lower, upper []byte
			var filters []BlockPropertyFilter
			for _, arg := range td.CmdArgs {
				switch arg.Key {
				case "lower":
					lower = []byte(arg.Vals[0])
				case "upper":
					upper = []byte(arg.Vals[0])
				case "point-key-filter":
					f, err := parseIntervalFilter(arg)
					if err != nil {
						return err.Error()
					}
					filters = append(filters, f)
				}
			}
			filterer := newBlockPropertiesFilterer(filters, nil, nil)
			ok, err := filterer.intersectsUserPropsAndFinishInit(r.Properties.UserProperties)
			if err != nil {
				return err.Error()
			} else if !ok {
				return "filter excludes entire table"
			}
			iter, err := r.NewPointIter(context.Background(), IterOptions{
				Lower:                lower,
				Upper:                upper,
				Transforms:           NoTransforms,
				Filterer:             filterer,
				FilterBlockSizeLimit: NeverUseFilterBlock,
				Env:                  block.ReadEnv{Stats: &stats, IterStats: nil},
				ReaderProvider:       MakeTrivialReaderProvider(r),
			})
			if err != nil {
				return err.Error()
			}
			return runIterCmd(td, iter, false)

		default:
			return fmt.Sprintf("unknown command: %s", td.Cmd)
		}
	})
}

func TestBlockProperties_BoundLimited(t *testing.T) {
	defer leaktest.AfterTest(t)()
	var r *Reader
	defer func() {
		if r != nil {
			require.NoError(t, r.Close())
		}
	}()

	var stats base.InternalIteratorStats
	datadriven.RunTest(t, "testdata/block_properties_boundlimited", func(t *testing.T, td *datadriven.TestData) string {
		switch td.Cmd {
		case "build":
			if r != nil {
				_ = r.Close()
				r = nil
			}
			var output string
			r, output = runBlockPropertiesBuildCmd(td)
			return output
		case "collectors":
			return runCollectorsCmd(r, td)
		case "table-props":
			return runTablePropsCmd(r, td)
		case "block-props":
			return runBlockPropsCmd(r)
		case "iter":
			var buf bytes.Buffer
			var lower, upper []byte
			syntheticSuffix := make([]byte, 0, binary.MaxVarintLen64)
			filter := boundLimitedWrapper{
				w:   &buf,
				cmp: testkeys.Comparer.Compare,
			}
			for _, arg := range td.CmdArgs {
				switch arg.Key {
				case "lower":
					lower = []byte(arg.Vals[0])
				case "upper":
					upper = []byte(arg.Vals[0])
				case "filter":
					f, err := parseIntervalFilter(arg)
					if err != nil {
						return err.Error()
					}
					filter.inner = f
				case "filter-upper":
					ik := base.MakeInternalKey([]byte(arg.Vals[0]), 0, base.InternalKeyKindSet)
					filter.upper = &ik
				case "filter-lower":
					ik := base.MakeInternalKey([]byte(arg.Vals[0]), 0, base.InternalKeyKindSet)
					filter.lower = &ik
				case "synthetic":
					var suffix uint64
					td.ScanArgs(t, "synthetic", &suffix)
					syntheticSuffix = binary.AppendUvarint(nil, suffix)
				}
			}
			if filter.inner == nil {
				return "missing block property filter"
			}

			filterer := newBlockPropertiesFilterer(nil, &filter, syntheticSuffix)
			ok, err := filterer.intersectsUserPropsAndFinishInit(r.Properties.UserProperties)
			if err != nil {
				return err.Error()
			} else if !ok {
				return "filter excludes entire table"
			}
			iter, err := r.NewPointIter(context.Background(), IterOptions{
				Lower:                lower,
				Upper:                upper,
				Transforms:           NoTransforms,
				Filterer:             filterer,
				FilterBlockSizeLimit: NeverUseFilterBlock,
				Env:                  block.ReadEnv{Stats: &stats, IterStats: nil},
				ReaderProvider:       MakeTrivialReaderProvider(r),
			})
			if err != nil {
				return err.Error()
			}
			return runIterCmd(td, iter, false, runIterCmdEveryOp(func(w io.Writer) {
				// Copy the bound-limited-wrapper's accumulated output to the
				// iterator's writer. This interleaves its output with the
				// iterator output.
				io.Copy(w, &buf)
				buf.Reset()
			}))
		default:
			return fmt.Sprintf("unrecognized command %q", td.Cmd)
		}
	})
}

type boundLimitedWrapper struct {
	w     io.Writer
	cmp   base.Compare
	inner BlockPropertyFilter
	lower *InternalKey
	upper *InternalKey
}

func (bl *boundLimitedWrapper) Name() string { return bl.inner.Name() }

func (bl *boundLimitedWrapper) Intersects(prop []byte) (bool, error) {
	propString := fmt.Sprintf("%x", prop)
	i, err := decodeBlockInterval(prop)
	if err == nil {
		// If it decodes as an interval, pretty print it as an interval.
		propString = fmt.Sprintf("[%d, %d)", i.Lower, i.Upper)
	}

	v, err := bl.inner.Intersects(prop)
	if bl.w != nil {
		fmt.Fprintf(bl.w, "    filter.Intersects(%s) = (%t, %v)\n", propString, v, err)
	}
	return v, err
}

func (bl *boundLimitedWrapper) SyntheticSuffixIntersects(prop []byte, suffix []byte) (bool, error) {
	propString := fmt.Sprintf("%x", prop)
	i, err := decodeBlockInterval(prop)
	if err == nil {
		// If it decodes as an interval, pretty print it as an interval.
		propString = fmt.Sprintf("[%d, %d)", i.Lower, i.Upper)
	}

	v, err := bl.inner.SyntheticSuffixIntersects(prop, suffix)
	if bl.w != nil {
		fmt.Fprintf(bl.w, "    filter.SyntheticSuffixIntersects(%s) = (%t, %v)\n", propString, v, err)
	}
	return v, err
}

func (bl *boundLimitedWrapper) KeyIsWithinLowerBound(key []byte) (ret bool) {
	if bl.lower == nil {
		ret = true
	} else {
		ret = bl.cmp(key, bl.lower.UserKey) >= 0
	}
	if bl.w != nil {
		fmt.Fprintf(bl.w, "    filter.KeyIsWithinLowerBound(%s) = %t\n", key, ret)
	}
	return ret
}

func (bl *boundLimitedWrapper) KeyIsWithinUpperBound(key []byte) (ret bool) {
	if bl.upper == nil {
		ret = true
	} else {
		ret = bl.cmp(key, bl.upper.UserKey) <= 0
	}
	if bl.w != nil {
		fmt.Fprintf(bl.w, "    filter.KeyIsWithinUpperBound(%s) = %t\n", key, ret)
	}
	return ret
}

var _ BlockIntervalSuffixReplacer = testingBlockIntervalSuffixReplacer{}

type testingBlockIntervalSuffixReplacer struct{}

func (sr testingBlockIntervalSuffixReplacer) ApplySuffixReplacement(
	interval BlockInterval, newSuffix []byte,
) (BlockInterval, error) {
	synthDecoded, _ := binary.Uvarint(newSuffix)
	return BlockInterval{Lower: synthDecoded, Upper: synthDecoded + 1}, nil
}

func parseIntervalFilter(cmd datadriven.CmdArg) (BlockPropertyFilter, error) {
	name := cmd.Vals[0]
	minS, maxS := cmd.Vals[1], cmd.Vals[2]
	min, err := strconv.ParseUint(minS, 10, 64)
	if err != nil {
		return nil, err
	}
	max, err := strconv.ParseUint(maxS, 10, 64)
	if err != nil {
		return nil, err
	}
	return NewBlockIntervalFilter(name, min, max, testingBlockIntervalSuffixReplacer{}), nil
}

func runCollectorsCmd(r *Reader, td *datadriven.TestData) string {
	var lines []string
	for k, v := range r.Properties.UserProperties {
		lines = append(lines, fmt.Sprintf("%d: %s", v[0], k))
	}
	linesSorted := sort.StringSlice(lines)
	linesSorted.Sort()
	return strings.Join(lines, "\n")
}

func runTablePropsCmd(r *Reader, td *datadriven.TestData) string {
	var lines []string
	for _, val := range r.Properties.UserProperties {
		id := shortID(val[0])
		i, err := decodeBlockInterval([]byte(val[1:]))
		if err != nil {
			return err.Error()
		}
		lines = append(lines, fmt.Sprintf("%d: [%d, %d)", id, i.Lower, i.Upper))
	}
	linesSorted := sort.StringSlice(lines)
	linesSorted.Sort()
	return strings.Join(lines, "\n")
}

func runBlockPropertiesBuildCmd(td *datadriven.TestData) (r *Reader, out string) {
	opts := WriterOptions{
		Comparer:       testkeys.Comparer,
		TableFormat:    TableFormatPebblev2,
		IndexBlockSize: math.MaxInt32, // Default to a single level index for simplicity.
	}
	for _, cmd := range td.CmdArgs {
		switch cmd.Key {
		case "block-size":
			if len(cmd.Vals) != 1 {
				return r, fmt.Sprintf("%s: arg %s expects 1 value", td.Cmd, cmd.Key)
			}
			var err error
			opts.BlockSize, err = strconv.Atoi(cmd.Vals[0])
			if err != nil {
				return r, err.Error()
			}
		case "collectors":
			for _, c := range cmd.Vals {
				var mapper IntervalMapper
				switch c {
				case "value-first":
					mapper = &valueCharIntervalMapper{charIdx: 0}
				case "value-last":
					mapper = &valueCharIntervalMapper{charIdx: -1}
				case "suffix":
					mapper = &testKeysSuffixIntervalMapper{}
				case "suffix-point-keys-only":
					mapper = &testKeysSuffixIntervalMapper{ignoreRangeKeys: true}
				case "suffix-range-keys-only":
					mapper = &testKeysSuffixIntervalMapper{ignorePoints: true}
				case "nil-points-and-ranges":
					mapper = nil
				default:
					return r, fmt.Sprintf("unknown collector: %s", c)
				}
				name := c
				opts.BlockPropertyCollectors = append(
					opts.BlockPropertyCollectors,
					func() BlockPropertyCollector {
						return NewBlockIntervalCollector(name, mapper, nil)
					})
			}
		case "index-block-size":
			var err error
			opts.IndexBlockSize, err = strconv.Atoi(cmd.Vals[0])
			if err != nil {
				return r, err.Error()
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
		meta, r, err = runBuildCmd(td, &opts, nil /* cacheHandle */)
	}()
	if err != nil {
		return r, err.Error()
	}
	return r, formatWriterMetadata(td, meta)
}

func runBlockPropsCmd(r *Reader) string {
	bh, err := r.readTopLevelIndexBlock(context.Background(), block.NoReadEnv, noReadHandle)
	if err != nil {
		return err.Error()
	}
	defer bh.Release()
	twoLevelIndex := r.Properties.IndexPartitions > 0
	i := r.tableFormat.newIndexIter()
	if err := i.Init(r.Comparer, bh.BlockData(), NoTransforms); err != nil {
		return err.Error()
	}
	var sb strings.Builder
	decodeProps := func(props []byte, indent string) error {
		d := makeBlockPropertiesDecoder(11, props)
		var lines []string
		for !d.Done() {
			id, prop, err := d.Next()
			if err != nil {
				return err
			}
			if prop == nil {
				continue
			}
			i, err := decodeBlockInterval(prop)
			if err != nil {
				return err
			}
			lines = append(lines, fmt.Sprintf("%s%d: [%d, %d)\n", indent, id, i.Lower, i.Upper))
		}
		linesSorted := sort.StringSlice(lines)
		linesSorted.Sort()
		for _, line := range lines {
			sb.WriteString(line)
		}
		return nil
	}

	for valid := i.First(); valid; valid = i.Next() {
		sb.WriteString(fmt.Sprintf("%s:\n", i.Separator()))
		bhp, err := i.BlockHandleWithProperties()
		if err != nil {
			return err.Error()
		}
		if err := decodeProps(bhp.Props, "  "); err != nil {
			return err.Error()
		}

		// If the table has a two-level index, also decode the index
		// block that bhp points to, along with its block properties.
		if twoLevelIndex {
			subIndex, err := r.readIndexBlock(context.Background(), block.NoReadEnv, noReadHandle, bhp.Handle)
			if err != nil {
				return err.Error()
			}
			err = func() error {
				defer subIndex.Release()
				subiter := r.tableFormat.newIndexIter()
				if err := subiter.Init(r.Comparer, subIndex.BlockData(), NoTransforms); err != nil {
					return err
				}
				for valid := subiter.First(); valid; valid = subiter.Next() {
					sb.WriteString(fmt.Sprintf("  %s:\n", subiter.Separator()))
					dataBH, err := subiter.BlockHandleWithProperties()
					if err != nil {
						return err
					}
					if err := decodeProps(dataBH.Props, "    "); err != nil {
						return err
					}
				}
				return nil
			}()
			if err != nil {
				return err.Error()
			}
		}
	}
	return sb.String()
}

func randomTestCollectors(
	rng *rand.Rand,
) (names []string, collectors []func() BlockPropertyCollector) {
	names = slices.Clone(testCollectorNames())
	rng.Shuffle(len(names), func(i, j int) { names[i], names[j] = names[j], names[i] })
	names = names[:rng.IntN(len(names)+1)]
	collectors = testCollectorsByNames(names...)
	return names, collectors
}

func testCollectorsByNames(names ...string) []func() BlockPropertyCollector {
	collectors := make([]func() BlockPropertyCollector, len(names))
	for i, name := range names {
		collectors[i] = testCollectorConstructors[name]
	}
	return collectors
}

var testCollectorNames = func() []string {
	names := make([]string, 0, len(testCollectorConstructors))
	for name := range testCollectorConstructors {
		names = append(names, name)
	}
	sort.Strings(names)
	return names
}

var testCollectorConstructors = map[string]func() BlockPropertyCollector{
	"count": keyCountCollectorFn("count"),
	"parity": func() BlockPropertyCollector {
		fn := func(v uint64) uint64 { return v & 1 }
		m := &testkeySuffixIntervalMapper{fn: fn}
		return NewBlockIntervalCollector("parity", m, m)
	},
	"log10": func() BlockPropertyCollector {
		fn := func(v uint64) uint64 { return uint64(math.Log10(float64(v))) + 1 }
		m := &testkeySuffixIntervalMapper{fn: fn}
		return NewBlockIntervalCollector("log10", m, m)
	},
	"onebits": func() BlockPropertyCollector {
		fn := func(v uint64) uint64 { return uint64(bits.OnesCount(uint(v))) }
		m := &testkeySuffixIntervalMapper{fn: fn}
		return NewBlockIntervalCollector("onebits", m, m)
	},
}

type keyCountCollector struct {
	name                string
	block, index, table int
}

var _ BlockPropertyCollector = &keyCountCollector{}

func keyCountCollectorFn(name string) func() BlockPropertyCollector {
	return func() BlockPropertyCollector { return &keyCountCollector{name: name} }
}

func (p *keyCountCollector) Name() string { return p.name }

func (p *keyCountCollector) AddPointKey(k InternalKey, _ []byte) error {
	p.block++
	return nil
}

func (p *keyCountCollector) AddRangeKeys(span Span) error {
	return rangekey.Encode(span, func(k base.InternalKey, v []byte) error {
		p.table++
		return nil
	})
}

func (p *keyCountCollector) FinishDataBlock(buf []byte) ([]byte, error) {
	buf = append(buf, []byte(strconv.Itoa(int(p.block)))...)
	p.table += p.block
	return buf, nil
}

func (p *keyCountCollector) AddPrevDataBlockToIndexBlock() {
	p.index += p.block
	p.block = 0
}

func (p *keyCountCollector) FinishIndexBlock(buf []byte) ([]byte, error) {
	buf = append(buf, []byte(strconv.Itoa(int(p.index)))...)
	p.index = 0
	return buf, nil
}

func (p *keyCountCollector) FinishTable(buf []byte) ([]byte, error) {
	buf = append(buf, []byte(strconv.Itoa(int(p.table)))...)
	p.table = 0
	return buf, nil
}

func (p *keyCountCollector) AddCollectedWithSuffixReplacement(
	oldProp []byte, oldSuffix, newSuffix []byte,
) error {
	n, err := strconv.Atoi(string(oldProp))
	if err != nil {
		return errors.Wrap(err, "parsing key count property")
	}
	p.block = n
	return nil
}

func (p *keyCountCollector) SupportsSuffixReplacement() bool {
	return true
}

type testkeySuffixIntervalMapper struct {
	fn func(uint64) uint64
}

var _ IntervalMapper = (*testkeySuffixIntervalMapper)(nil)
var _ BlockIntervalSuffixReplacer = (*testkeySuffixIntervalMapper)(nil)

// MapPointKey is part of the IntervalMapper interface.
func (i *testkeySuffixIntervalMapper) MapPointKey(
	key InternalKey, value []byte,
) (BlockInterval, error) {
	j := testkeys.Comparer.Split(key.UserKey)
	if len(key.UserKey) == j {
		return BlockInterval{}, nil
	}
	v, err := testkeys.ParseSuffix(key.UserKey[j:])
	if err != nil {
		return BlockInterval{}, errors.Wrap(err, "mapping point key")
	}
	mv := i.fn(uint64(v))
	return BlockInterval{mv, mv + 1}, nil
}

// MapRangeKeys is part of the IntervalMapper interface.
func (i *testkeySuffixIntervalMapper) MapRangeKeys(span Span) (BlockInterval, error) {
	return BlockInterval{}, nil
}

// ApplySuffixReplacement is part of the BlockIntervalSuffixReplacer interface.
func (i *testkeySuffixIntervalMapper) ApplySuffixReplacement(
	interval BlockInterval, newSuffix []byte,
) (BlockInterval, error) {
	if len(newSuffix) == 0 {
		return BlockInterval{}, nil
	}
	v, err := testkeys.ParseSuffix(newSuffix)
	if err != nil {
		return BlockInterval{}, errors.Wrap(err, "applying suffix replacement")
	}
	mv := i.fn(uint64(v))
	return BlockInterval{mv, mv + 1}, nil
}
