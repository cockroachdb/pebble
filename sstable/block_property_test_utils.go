// Copyright 2022 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package sstable

import (
	"fmt"
	"math"

	"github.com/cockroachdb/pebble/v2/internal/testkeys"
)

// Code in this file contains utils for testing. It implements interval block
// property collectors and filters on the suffixes of keys in the format used
// by the testkeys package (eg, 'key@5').

const testKeysBlockPropertyName = `pebble.internal.testkeys.suffixes`

// NewTestKeysBlockPropertyCollector constructs a sstable property collector
// over testkey suffixes.
func NewTestKeysBlockPropertyCollector() BlockPropertyCollector {
	return NewBlockIntervalCollector(
		testKeysBlockPropertyName,
		&testKeysSuffixIntervalMapper{},
		nil)
}

// NewTestKeysBlockPropertyFilter constructs a new block-property filter that excludes
// blocks containing exclusively suffixed keys where all the suffixes fall
// outside of the range [filterMin, filterMax).
//
// The filter only filters based on data derived from the key. The iteration
// results of this block property filter are deterministic for unsuffixed keys
// and keys with suffixes within the range [filterMin, filterMax). For keys with
// suffixes outside the range, iteration is nondeterministic.
func NewTestKeysBlockPropertyFilter(filterMin, filterMax uint64) *BlockIntervalFilter {
	return NewBlockIntervalFilter(testKeysBlockPropertyName, filterMin, filterMax, testKeysBlockIntervalSyntheticReplacer{})
}

var _ BlockIntervalSuffixReplacer = testKeysBlockIntervalSyntheticReplacer{}

type testKeysBlockIntervalSyntheticReplacer struct{}

// ApplySuffixReplacement implements BlockIntervalSyntheticReplacer.
func (sr testKeysBlockIntervalSyntheticReplacer) ApplySuffixReplacement(
	interval BlockInterval, newSuffix []byte,
) (BlockInterval, error) {
	decoded, err := testkeys.ParseSuffix(newSuffix)
	if err != nil {
		return BlockInterval{}, err
	}
	// The testKeysSuffixIntervalMapper below maps keys with no suffix to
	// [0, MaxUint64); ignore that.
	if interval.Upper != math.MaxUint64 && uint64(decoded) < interval.Upper {
		panic(fmt.Sprintf("the synthetic suffix %d is less than the property upper bound %d", decoded, interval.Upper))
	}
	return BlockInterval{uint64(decoded), uint64(decoded) + 1}, nil
}

// NewTestKeysMaskingFilter constructs a TestKeysMaskingFilter that implements
// pebble.BlockPropertyFilterMask for efficient range-key masking using the
// testkeys block property filter. The masking filter wraps a block interval
// filter, and modifies the configured interval when Pebble requests it.
func NewTestKeysMaskingFilter() TestKeysMaskingFilter {
	return TestKeysMaskingFilter{BlockIntervalFilter: NewTestKeysBlockPropertyFilter(0, math.MaxUint64)}
}

// TestKeysMaskingFilter implements BlockPropertyFilterMask and may be used to mask
// point keys with the testkeys-style suffixes (eg, @4) that are masked by range
// keys with testkeys-style suffixes.
type TestKeysMaskingFilter struct {
	*BlockIntervalFilter
}

// SetSuffix implements pebble.BlockPropertyFilterMask.
func (f TestKeysMaskingFilter) SetSuffix(suffix []byte) error {
	ts, err := testkeys.ParseSuffix(suffix)
	if err != nil {
		return err
	}
	f.BlockIntervalFilter.SetInterval(uint64(ts), math.MaxUint64)
	return nil
}

// Intersects implements the BlockPropertyFilter interface.
func (f TestKeysMaskingFilter) Intersects(prop []byte) (bool, error) {
	return f.BlockIntervalFilter.Intersects(prop)
}

// SyntheticSuffixIntersects implements the BlockPropertyFilter interface.
func (f TestKeysMaskingFilter) SyntheticSuffixIntersects(prop []byte, suffix []byte) (bool, error) {
	return f.BlockIntervalFilter.SyntheticSuffixIntersects(prop, suffix)
}

// testKeysSuffixIntervalMapper maps keys to intervals according to the
// timestamps in MVCC-like suffixes for keys (e.g. "foo@123" -> 123).
type testKeysSuffixIntervalMapper struct {
	ignorePoints    bool
	ignoreRangeKeys bool
}

var _ IntervalMapper = &testKeysSuffixIntervalMapper{}

// MapPointKey is part of the IntervalMapper interface.
func (c *testKeysSuffixIntervalMapper) MapPointKey(
	key InternalKey, value []byte,
) (BlockInterval, error) {
	if c.ignorePoints {
		return BlockInterval{}, nil
	}
	n := testkeys.Comparer.Split(key.UserKey)
	return testKeysSuffixToInterval(key.UserKey[n:]), nil
}

// MapRangeKeys is part of the IntervalMapper interface.
func (c *testKeysSuffixIntervalMapper) MapRangeKeys(span Span) (BlockInterval, error) {
	if c.ignoreRangeKeys {
		return BlockInterval{}, nil
	}
	var result BlockInterval
	for _, k := range span.Keys {
		if len(k.Suffix) > 0 {
			result.UnionWith(testKeysSuffixToInterval(k.Suffix))
		}
	}
	return result, nil
}

func testKeysSuffixToInterval(suffix []byte) BlockInterval {
	if len(suffix) == 0 {
		return BlockInterval{0, math.MaxUint64}
	}
	n, err := testkeys.ParseSuffix(suffix)
	if err != nil {
		panic(err)
	}
	return BlockInterval{uint64(n), uint64(n) + 1}
}
