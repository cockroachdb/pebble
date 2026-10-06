// Copyright 2018 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package sstable

import (
	"fmt"
	"math"
	randv1 "math/rand"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"testing/quick"
	"time"
	"unsafe"

	"github.com/cockroachdb/crlib/testutils/leaktest"
	"github.com/cockroachdb/pebble/sstable/rowblk"
	"github.com/cockroachdb/pebble/vfs"
	"github.com/kr/pretty"
	"github.com/stretchr/testify/require"
)

func TestPropertiesLoad(t *testing.T) {
	defer leaktest.AfterTest(t)()
	expected := Properties{
		CommonProperties: CommonProperties{
			NumEntries:         1727,
			NumDeletions:       17,
			NumRangeDeletions:  17,
			RawKeySize:         23938,
			RawValueSize:       1912,
			NumDataBlocks:      14,
			CompressionName:    "Snappy",
			CompressionOptions: "window_bits=-14; level=32767; strategy=0; max_dict_bytes=0; zstd_max_train_bytes=0; enabled=0; ",
		},
		ComparerName:           "leveldb.BytewiseComparator",
		DataSize:               13913,
		IndexSize:              325,
		MaxUserKeySize:         14,
		MergerName:             "nullptr",
		PropertyCollectorNames: "[]",
	}

	{
		// Check that we can read properties from a table.
		f, err := vfs.Default.Open(filepath.FromSlash("testdata/h.sst"))
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
		NumDeletions:            15,
		NumEntries:              16,
		NumRangeDeletions:       18,
		NumRangeKeyDels:         19,
		NumRangeKeySets:         20,
		RawKeySize:              25,
		RawValueSize:            26,
		NumDataBlocks:           14,
		NumTombstoneDenseBlocks: 2,
		CompressionName:         "compression name",
		CompressionOptions:      "compression option",
	},
	ComparerName:           "comparator name",
	DataSize:               3,
	FilterPolicyName:       "filter policy name",
	FilterSize:             5,
	IndexPartitions:        10,
	IndexSize:              11,
	IndexType:              12,
	IsStrictObsolete:       true,
	KeySchemaName:          "key schema name",
	MaxUserKeySize:         30,
	MergerName:             "merge operator name",
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
	defer leaktest.AfterTest(t)()
	expected := &Properties{}
	*expected = testProps

	check1 := func(e *Properties) {
		// Check that we can save properties and read them back.
		var w rowblk.Writer
		w.RestartInterval = propertiesBlockRestartInterval
		require.NoError(t, e.save(TableFormatPebblev2, &w))
		var props Properties

		require.NoError(t, props.load(w.Finish(), make(map[string]struct{})))
		props.Loaded = nil
		if diff := pretty.Diff(*e, props); diff != nil {
			t.Fatalf("%s", strings.Join(diff, "\n"))
		}
	}

	check1(expected)

	rng := randv1.New(randv1.NewSource(time.Now().UnixNano()))
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

// TestPropertiesMaxUserKeySize tests the encoding of the MaxUserKeySize
// property across table formats.
func TestPropertiesMaxUserKeySize(t *testing.T) {
	defer leaktest.AfterTest(t)()

	for tf := TableFormatLevelDB; tf <= TableFormatMax; tf++ {
		for _, v := range []uint64{0, 1, 127, 128, 1 << 20, 1<<32 + 1, math.MaxUint64} {
			t.Run(fmt.Sprintf("%s/%d", tf, v), func(t *testing.T) {
				p := testProps
				p.MaxUserKeySize = v
				var w rowblk.Writer
				w.RestartInterval = propertiesBlockRestartInterval
				require.NoError(t, p.save(tf, &w))
				var loaded Properties
				require.NoError(t, loaded.load(w.Finish(), nil /* deniedUserProperties */))

				// The property is not serialized when it is zero (unknown), nor for
				// RocksDB formats.
				expectPresent := v != 0 && tf >= TableFormatPebblev1
				_, ok := loaded.Loaded[unsafe.Offsetof(loaded.MaxUserKeySize)]
				require.Equal(t, expectPresent, ok)
				require.NotContains(t, loaded.UserProperties, "pebble.max.user-key.size")
				if expectPresent {
					require.Equal(t, v, loaded.MaxUserKeySize)
					require.Contains(t, loaded.String(), fmt.Sprintf("pebble.max.user-key.size: %d\n", v))
				} else {
					require.Zero(t, loaded.MaxUserKeySize)
					require.NotContains(t, loaded.String(), "pebble.max.user-key.size")
				}
			})
		}
	}
}

func TestScalePropertiesBadSizes(t *testing.T) {
	// Verify that GetScaledProperties works even if the given sizes are not
	// valid.
	p := Properties{
		CommonProperties: CommonProperties{
			NumDeletions:  0,
			NumEntries:    1,
			NumDataBlocks: 10,
		},
	}
	scaled := p.GetScaledProperties(100, 0)
	require.Equal(t, uint64(0), scaled.NumDeletions)
	require.Equal(t, uint64(1), scaled.NumEntries)
	require.Equal(t, uint64(1), scaled.NumDataBlocks)

	scaled = p.GetScaledProperties(100, 1000)
	require.Equal(t, uint64(0), scaled.NumDeletions)
	require.Equal(t, uint64(1), scaled.NumEntries)
	require.Equal(t, uint64(10), scaled.NumDataBlocks)
}

func BenchmarkPropertiesLoad(b *testing.B) {
	var w rowblk.Writer
	w.RestartInterval = propertiesBlockRestartInterval
	require.NoError(b, testProps.save(TableFormatPebblev2, &w))
	block := w.Finish()

	b.ResetTimer()
	p := &Properties{}
	for i := 0; i < b.N; i++ {
		*p = Properties{}
		require.NoError(b, p.load(block, nil))
	}
}
