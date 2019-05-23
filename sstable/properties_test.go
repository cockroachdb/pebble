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
		ComparatorName:         "leveldb.BytewiseComparator",
		CompressionName:        "Snappy",
		DataSize:               13086,
		IndexSize:              165,
		MergeOperatorName:      "nullptr",
		NumDataBlocks:          7,
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
			"rocksdb.block.based.table.index.type":          13122,
			"rocksdb.block.based.table.prefix.filtering":    13171,
			"rocksdb.block.based.table.whole.key.filtering": 13220,
			"rocksdb.column.family.id":                      13248,
			"rocksdb.comparator":                            13274,
			"rocksdb.compression":                           13322,
			"rocksdb.creation.time":                         13352,
			"rocksdb.data.size":                             13373,
			"rocksdb.external_sst_file.global_seqno":        13416,
			"rocksdb.external_sst_file.version":             13460,
			"rocksdb.filter.size":                           13486,
			"rocksdb.fixed.key.length":                      13514,
			"rocksdb.format.version":                        13540,
			"rocksdb.index.size":                            13562,
			"rocksdb.merge.operator":                        13589,
			"rocksdb.num.data.blocks":                       13622,
			"rocksdb.num.entries":                           13645,
			"rocksdb.oldest.key.time":                       13673,
			"rocksdb.prefix.extractor.name":                 13706,
			"rocksdb.property.collectors":                   13743,
			"rocksdb.raw.key.size":                          13793,
			"rocksdb.raw.value.size":                        13821,
			"test.key-count":                                13840,
		},
	}

	{
		// Check that we can read properties from a table.
		f, err := os.Open(filepath.FromSlash("testdata/h.sst"))
		if err != nil {
			t.Fatal(err)
		}
		defer f.Close()
		r := NewReader(f, 0, nil)

		if diff := pretty.Diff(expected, r.Properties); diff != nil {
			t.Fatalf("%s", strings.Join(diff, "\n"))
		}
	}
}

func TestPropertiesSave(t *testing.T) {
	expected := &Properties{
		ColumnFamilyID:         1,
		ColumnFamilyName:       "column family name",
		ComparatorName:         "comparator name",
		CompressionName:        "compression name",
		CreationTime:           2,
		DataSize:               3,
		FilterPolicyName:       "filter policy name",
		FilterSize:             4,
		FixedKeyLen:            5,
		FormatVersion:          6,
		GlobalSeqNum:           7,
		IndexKeyIsUserKey:      8,
		IndexPartitions:        9,
		IndexSize:              10,
		IndexType:              11,
		MergeOperatorName:      "merge operator name",
		NumDataBlocks:          12,
		NumDeletions:           13,
		NumEntries:             14,
		NumRangeDeletions:      15,
		OldestKeyTime:          16,
		PrefixExtractorName:    "prefix extractor name",
		PrefixFiltering:        true,
		PropertyCollectorNames: "prefix collector names",
		RawKeySize:             17,
		RawValueSize:           18,
		TopLevelIndexSize:      19,
		Version:                20,
		WholeKeyFiltering:      true,
		UserProperties: map[string]string{
			"user-prop-a": "1",
			"user-prop-b": "2",
		},
	}

	check1 := func(expected *Properties) {
		// Check that we can save properties and read them back.
		var w rawBlockWriter
		w.restartInterval = 1
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
