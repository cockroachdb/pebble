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
		NumEntries:             1727,
		NumDeletions:           17,
		NumRangeDeletions:      17,
		RawKeySize:             23938,
		RawValueSize:           1912,
		NumDataBlocks:          14,
		ComparerName:           "leveldb.BytewiseComparator",
		DataSize:               13913,
		IndexSize:              325,
		MergerName:             "nullptr",
		PropertyCollectorNames: "[]",
		CompressionName:        "Snappy",
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

		loadedProps.Loaded = 0

		if diff := pretty.Diff(expected, loadedProps); diff != nil {
			t.Fatalf("%s", strings.Join(diff, "\n"))
		}
	}
}

var testProps = Properties{
	NumDeletions:            15,
	NumEntries:              16,
	NumRangeDeletions:       18,
	NumRangeKeyDels:         19,
	NumRangeKeySets:         20,
	RawKeySize:              25,
	RawValueSize:            26,
	NumDataBlocks:           14,
	NumTombstoneDenseBlocks: 2,
	ComparerName:            "comparator name",
	DataSize:                3,
	FilterFamily:            "filter policy name",
	FilterSize:              5,
	IndexPartitions:         10,
	IndexSize:               11,
	IndexType:               12,
	IsStrictObsolete:        true,
	KeySchemaName:           "key schema name",
	MergerName:              "merge operator name",
	NumMergeOperands:        17,
	NumRangeKeyUnsets:       21,
	NumValueBlocks:          22,
	NumValuesInValueBlocks:  23,
	PropertyCollectorNames:  "prefix collector names",
	TopLevelIndexSize:       27,
	CompressionName:         "compression name",
	CompressionStats:        "Snappy:1024/2048",
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
		props.Loaded = 0
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
		props.Loaded = 0
		check1(&props)
	}
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
