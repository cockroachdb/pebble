// Copyright 2018 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package sstable

import (
	"math/rand"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"testing/quick"
	"time"

	"github.com/kr/pretty"
	"github.com/stretchr/testify/require"
)

func TestPropertiesLoad(t *testing.T) {
	expected := Properties{
		CommonProperties: CommonProperties{
			NumEntries:         1727,
			NumDeletions:       17,
			NumRangeDeletions:  17,
			RawKeySize:         23938,
			RawValueSize:       1912,
			CompressionName:    "Snappy",
			CompressionOptions: "window_bits=-14; level=32767; strategy=0; max_dict_bytes=0; zstd_max_train_bytes=0; enabled=0; ",
		},
		ComparerName:           "leveldb.BytewiseComparator",
		DataSize:               13913,
		ExternalFormatVersion:  2,
		IndexSize:              325,
		MergerName:             "nullptr",
		NumDataBlocks:          14,
		PropertyCollectorNames: "[]",
	}

	{
		// Check that we can read properties from a table.
		f, err := os.Open(filepath.FromSlash("testdata/h.sst"))
		require.NoError(t, err)

		r, err := newReader(f, ReaderOptions{})

		require.NoError(t, err)
		defer r.Close()

		r.Properties.Loaded = nil

		if diff := pretty.Diff(expected, r.Properties); diff != nil {
			t.Fatalf("%s", strings.Join(diff, "\n"))
		}
	}
}

var testProps = Properties{
	CommonProperties: CommonProperties{
		NumDeletions:       15,
		NumEntries:         16,
		NumRangeDeletions:  18,
		NumRangeKeyDels:    19,
		NumRangeKeySets:    20,
		RawKeySize:         25,
		RawValueSize:       26,
		CompressionName:    "compression name",
		CompressionOptions: "compression option",
	},
	ComparerName:           "comparator name",
	DataSize:               3,
	ExternalFormatVersion:  4,
	FilterPolicyName:       "filter policy name",
	FilterSize:             5,
	IndexPartitions:        10,
	IndexSize:              11,
	IndexType:              12,
	IsStrictObsolete:       true,
	MergerName:             "merge operator name",
	NumDataBlocks:          14,
	NumMergeOperands:       17,
	NumRangeKeyUnsets:      21,
	NumValueBlocks:         22,
	NumValuesInValueBlocks: 23,
	PropertyCollectorNames: "prefix collector names",
	TopLevelIndexSize:      27,
	UserProperties: map[string]string{
		"user-prop-a": "1",
		"user-prop-b": "2",
	},
}

func TestPropertiesSave(t *testing.T) {
	expected := &Properties{}
	*expected = testProps

	check1 := func(e *Properties) {
		// Check that we can save properties and read them back.
		var w rawBlockWriter
		w.restartInterval = propertiesBlockRestartInterval
		e.save(TableFormatPebblev2, &w)
		var props Properties

		require.NoError(t, props.load(w.finish(), 0, make(map[string]struct{})))
		props.Loaded = nil
		if diff := pretty.Diff(*e, props); diff != nil {
			t.Fatalf("%s", strings.Join(diff, "\n"))
		}
	}

	check1(expected)

	rng := rand.New(rand.NewSource(time.Now().UnixNano()))
	for i := 0; i < 1000; i++ {
		v, _ := quick.Value(reflect.TypeOf(Properties{}), rng)
		props := v.Interface().(Properties)
		if props.IndexPartitions == 0 {
			props.TopLevelIndexSize = 0
		}
		props.Loaded = nil
		check1(&props)
	}
}

func TestScalePropertiesBadSizes(t *testing.T) {
	// Verify that GetScaledProperties works even if the given sizes are not
	// valid.
	p := Properties{
		CommonProperties: CommonProperties{
			NumDeletions:    0,
			NumEntries:      1,
			ValueBlocksSize: 10,
		},
	}
	scaled := p.GetScaledProperties(100, 0)
	require.Equal(t, uint64(0), scaled.NumDeletions)
	require.Equal(t, uint64(1), scaled.NumEntries)
	require.Equal(t, uint64(1), scaled.ValueBlocksSize)

	scaled = p.GetScaledProperties(100, 1000)
	require.Equal(t, uint64(0), scaled.NumDeletions)
	require.Equal(t, uint64(1), scaled.NumEntries)
	require.Equal(t, uint64(10), scaled.ValueBlocksSize)
}

func BenchmarkPropertiesLoad(b *testing.B) {
	var w rawBlockWriter
	w.restartInterval = propertiesBlockRestartInterval
	testProps.save(TableFormatPebblev2, &w)
	block := w.finish()

	b.ResetTimer()
	p := &Properties{}
	for i := 0; i < b.N; i++ {
		*p = Properties{}
		require.NoError(b, p.load(block, 0, nil))
	}
}
