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
)

func TestPropertiesLoad(t *testing.T) {
	expected := Properties{
		ColumnFamilyID:         math.MaxInt32,
		ComparerName:           "leveldb.BytewiseComparator",
		CompressionName:        "Snappy",
		CompressionOptions:     "window_bits=-14; level=32767; strategy=0; max_dict_bytes=0; zstd_max_train_bytes=0; enabled=0; ",
		DataSize:               13913,
		IndexSize:              325,
		MergerName:             "nullptr",
		NumDataBlocks:          14,
		NumEntries:             1710,
		PrefixExtractorName:    "nullptr",
		PropertyCollectorNames: "[KeyCountPropertyCollector]",
		RawKeySize:             23717,
		RawValueSize:           1835,
		Version:                2,
		UserProperties: map[string]string{
			"test.key-count": "1710",
		},
		WholeKeyFiltering: false,
		ValueOffsets: map[string]uint64{
			"rocksdb.block.based.table.index.type":          14199,
			"rocksdb.block.based.table.prefix.filtering":    14248,
			"rocksdb.block.based.table.whole.key.filtering": 14271,
			"rocksdb.column.family.id":                      14273,
			"rocksdb.comparator":                            14291,
			"rocksdb.compression":                           14329,
			"rocksdb.compression_options":                   14353,
			"rocksdb.creation.time":                         14453,
			"rocksdb.data.size":                             14465,
			"rocksdb.deleted.keys":                          14482,
			"rocksdb.external_sst_file.global_seqno":        14515,
			"rocksdb.external_sst_file.version":             14551,
			"rocksdb.filter.size":                           14551,
			"rocksdb.fixed.key.length":                      14571,
			"rocksdb.format.version":                        14587,
			"rocksdb.index.key.is.user.key":                 14611,
			"rocksdb.index.size":                            14625,
			"rocksdb.index.value.is.delta.encoded":          14652,
			"rocksdb.merge.operands":                        14664,
			"rocksdb.merge.operator":                        14682,
			"rocksdb.num.data.blocks":                       14696,
			"rocksdb.num.entries":                           14711,
			"rocksdb.num.range-deletions":                   14731,
			"rocksdb.oldest.key.time":                       14746,
			"rocksdb.prefix.extractor.name":                 14771,
			"rocksdb.property.collectors":                   14800,
			"rocksdb.raw.key.size":                          14840,
			"rocksdb.raw.value.size":                        14860,
			"test.key-count":                                14867,
		},
	}

	{
		// Check that we can read properties from a table.
		f, err := os.Open(filepath.FromSlash("testdata/h.sst"))
		if err != nil {
			t.Fatal(err)
		}
		defer f.Close()
		r, err := NewReader(f, 0, 0, nil)
		if err != nil {
			t.Fatal(err)
		}

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
		FilterPolicyName:         "filter policy name",
		FilterSize:               4,
		FixedKeyLen:              5,
		FormatVersion:            6,
		GlobalSeqNum:             7,
		IndexKeyIsUserKey:        8,
		IndexPartitions:          9,
		IndexSize:                10,
		IndexType:                11,
		IndexValueIsDeltaEncoded: 12,
		MergerName:               "merge operator name",
		NumDataBlocks:            13,
		NumDeletions:             14,
		NumEntries:               15,
		NumMergeOperands:         16,
		NumRangeDeletions:        17,
		OldestKeyTime:            18,
		PrefixExtractorName:      "prefix extractor name",
		PrefixFiltering:          true,
		PropertyCollectorNames:   "prefix collector names",
		RawKeySize:               19,
		RawValueSize:             20,
		TopLevelIndexSize:        21,
		Version:                  22,
		WholeKeyFiltering:        true,
		UserProperties: map[string]string{
			"user-prop-a": "1",
			"user-prop-b": "2",
		},
	}

	check1 := func(expected *Properties) {
		// Check that we can save properties and read them back.
		var w rawBlockWriter
		w.restartInterval = propertiesBlockRestartInterval
		expected.save(&w)
		var props Properties
		if err := props.load(w.finish(), 0); err != nil {
			t.Fatal(err)
		}
		props.ValueOffsets = nil
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
