// Copyright 2018 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package sstable

import (
	"bytes"
	"context"
	randv1 "math/rand"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"testing/quick"
	"time"

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
			NumEntries:        1727,
			NumDeletions:      17,
			NumRangeDeletions: 17,
			RawKeySize:        23938,
			RawValueSize:      1912,
			NumDataBlocks:     14,
			CompressionName:   "Snappy",
		},
		ComparerName:           "leveldb.BytewiseComparator",
		DataSize:               13913,
		IndexSize:              325,
		MergerName:             "nullptr",
		PropertyCollectorNames: "[]",
	}

	{
		// Check that we can read properties from a table.
		f, err := vfs.Default.Open(filepath.FromSlash("testdata/hamlet-sst/000002.sst"))
		require.NoError(t, err)

		r, err := newReader(f, ReaderOptions{})

		require.NoError(t, err)
		defer r.Close()

		loadedProps, err := r.ReadPropertiesBlock(context.Background(), nil)
		require.NoError(t, err)

		loadedProps.Loaded = nil

		if diff := pretty.Diff(expected, loadedProps); diff != nil {
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
	MergerName:             "merge operator name",
	NumMergeOperands:       17,
	NumRangeKeyUnsets:      21,
	NumValueBlocks:         22,
	NumValuesInValueBlocks: 23,
	PropertyCollectorNames: "prefix collector names",
	TopLevelIndexSize:      27,
	CompressionStats:       "Snappy:1024/2048",
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
		require.NoError(t, e.saveToRowWriter(TableFormatPebblev2, &w))
		var props Properties

		i, err := rowblk.NewRawIter(bytes.Compare, w.Finish())
		require.NoError(t, err)
		require.NoError(t, props.load(i.All()))
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

func TestScalePropertiesBadSizes(t *testing.T) {
	// Verify that GetScaledProperties works even if the given sizes are not
	// valid.
	p := CommonProperties{
		NumDeletions:  0,
		NumEntries:    1,
		NumDataBlocks: 10,
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

func TestScalePropertiesOverflow(t *testing.T) {
	// Verify that GetScaledProperties works even if the given sizes are not
	// valid.
	p := CommonProperties{
		RawKeySize:   1 << 40,
		RawValueSize: 1 << 50,
	}
	scaled := p.GetScaledProperties(1<<60, 1<<60)
	require.Equal(t, p, scaled)

	scaled = p.GetScaledProperties(1<<60, 1<<50)
	require.Equal(t, scaled, CommonProperties{
		RawKeySize:   1 << 30,
		RawValueSize: 1 << 40,
	})
}

func BenchmarkPropertiesLoad(b *testing.B) {
	var w rowblk.Writer
	w.RestartInterval = propertiesBlockRestartInterval
	require.NoError(b, testProps.saveToRowWriter(TableFormatPebblev2, &w))
	block := w.Finish()

	b.ResetTimer()
	p := &Properties{}
	for i := 0; i < b.N; i++ {
		*p = Properties{}
		it, err := rowblk.NewRawIter(bytes.Compare, block)
		require.NoError(b, err)
		require.NoError(b, p.load(it.All()))
	}
}
