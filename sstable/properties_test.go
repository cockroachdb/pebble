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
		ComparerName:           "leveldb.BytewiseComparator",
		CompressionName:        "Snappy",
		CompressionOptions:     "window_bits=-14; level=32767; strategy=0; max_dict_bytes=0; zstd_max_train_bytes=0; enabled=0; ",
		DataSize:               13913,
		ExternalFormatVersion:  2,
		IndexSize:              325,
		MergerName:             "nullptr",
		NumDataBlocks:          14,
		PrefixExtractorName:    "nullptr",
		PropertyCollectorNames: "[KeyCountPropertyCollector]",
		UserProperties: map[string]string{
			"test.key-count": "1727",
		},
		WholeKeyFiltering: false,
	}

	// Part of the CommonProperties embedded struct and must be set separately.
	expected.NumEntries = 1727
	expected.NumDeletions = 17
	expected.NumRangeDeletions = 17
	expected.RawKeySize = 23938
	expected.RawValueSize = 1912

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

func TestPropertiesSave(t *testing.T) {
	expected := &Properties{
		ComparerName:           "comparator name",
		CompressionName:        "compression name",
		CompressionOptions:     "compression option",
		DataSize:               3,
		ExternalFormatVersion:  4,
		FilterPolicyName:       "filter policy name",
		FilterSize:             5,
		GlobalSeqNum:           8,
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
		PrefixExtractorName:    "prefix extractor name",
		PrefixFiltering:        true,
		PropertyCollectorNames: "prefix collector names",
		TopLevelIndexSize:      27,
		WholeKeyFiltering:      true,
		UserProperties: map[string]string{
			"user-prop-a": "1",
			"user-prop-b": "2",
		},
	}

	// Part of the CommonProps embedded struct and must be set separately.
	expected.NumDeletions = 15
	expected.NumEntries = 16
	expected.NumRangeDeletions = 18
	expected.NumRangeKeyDels = 19
	expected.NumRangeKeySets = 20
	expected.RawKeySize = 25
	expected.RawValueSize = 26

	check1 := func(expected *Properties) {
		// Check that we can save properties and read them back.
		var w rawBlockWriter
		w.restartInterval = propertiesBlockRestartInterval
		expected.save(TableFormatPebblev2, &w)
		var props Properties

		require.NoError(t, props.load(w.finish(), 0, make(map[string]struct{})))
		props.Loaded = nil
		if diff := pretty.Diff(*expected, props); diff != nil {
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
		check1(&props)
	}
}
