// Copyright 2018 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package sstable

import (
	"math"
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
		ColumnFamilyID:         math.MaxInt32,
		ComparerName:           "leveldb.BytewiseComparator",
		CompressionName:        "Snappy",
		CompressionOptions:     "window_bits=-14; level=32767; strategy=0; max_dict_bytes=0; zstd_max_train_bytes=0; enabled=0; ",
		DataSize:               13913,
		ExternalFormatVersion:  2,
		IndexSize:              325,
		MergerName:             "nullptr",
		NumDataBlocks:          14,
		NumEntries:             1727,
		NumDeletions:           17,
		NumRangeDeletions:      17,
		PrefixExtractorName:    "nullptr",
		PropertyCollectorNames: "[KeyCountPropertyCollector]",
		RawKeySize:             23938,
		RawValueSize:           1912,
		UserProperties: map[string]string{
			"test.key-count": "1727",
		},
		WholeKeyFiltering: false,
	}

	{
		// Check that we can read properties from a table.
		f, err := os.Open(filepath.FromSlash("testdata/h.sst"))
		require.NoError(t, err)

		r, err := NewReader(f, ReaderOptions{})
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
		ColumnFamilyID:           1,
		ColumnFamilyName:         "column family name",
		ComparerName:             "comparator name",
		CompressionName:          "compression name",
		CompressionOptions:       "compression option",
		CreationTime:             2,
		DataSize:                 3,
		ExternalFormatVersion:    4,
		FilterPolicyName:         "filter policy name",
		FilterSize:               5,
		FixedKeyLen:              6,
		FormatVersion:            7,
		GlobalSeqNum:             8,
		IndexKeyIsUserKey:        9,
		IndexPartitions:          10,
		IndexSize:                11,
		IndexType:                12,
		IndexValueIsDeltaEncoded: 13,
		MergerName:               "merge operator name",
		NumDataBlocks:            14,
		NumDeletions:             15,
		NumEntries:               16,
		NumMergeOperands:         17,
		NumRangeDeletions:        18,
		NumRangeKeyDels:          19,
		NumRangeKeySets:          20,
		NumRangeKeyUnsets:        21,
		NumValueBlocks:           22,
		NumValuesInValueBlocks:   23,
		OldestKeyTime:            24,
		PrefixExtractorName:      "prefix extractor name",
		PrefixFiltering:          true,
		PropertyCollectorNames:   "prefix collector names",
		RawKeySize:               25,
		RawValueSize:             26,
		TopLevelIndexSize:        27,
		WholeKeyFiltering:        true,
		UserProperties: map[string]string{
			"user-prop-a": "1",
			"user-prop-b": "2",
		},
		ValueBlocksSize: 28,
	}

	check1 := func(expected *Properties) {
		// Check that we can save properties and read them back.
		var w rawBlockWriter
		w.restartInterval = propertiesBlockRestartInterval
		expected.save(&w)
		var props Properties
		require.NoError(t, props.load(w.finish(), 0))
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
