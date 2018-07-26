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
		PropertyCollectorNames: "[]",
		RawKeySize:             23717,
		RawValueSize:           1835,
		Version:                2,
		WholeKeyFiltering:      true,
	}

	{
		// Check that we can read properties from a table.
		f, err := os.Open(filepath.FromSlash("../testdata/h.sst"))
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
		NumEntries:             13,
		NumRangeDeletions:      14,
		OldestKeyTime:          15,
		PrefixExtractorName:    "prefix extractor name",
		PrefixFiltering:        true,
		PropertyCollectorNames: "prefix collector names",
		RawKeySize:             16,
		RawValueSize:           17,
		TopLevelIndexSize:      18,
		Version:                19,
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
		if err := props.load(w.finish()); err != nil {
			t.Fatal(err)
		}
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
