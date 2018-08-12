// Copyright 2018 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package pebble

import (
	"regexp"
	"sort"
	"strings"
	"testing"

	"github.com/petermattis/pebble/db"
)

func TestIngestLoad(t *testing.T) {
	// TODO(peter): Test loading of metadata.
}

func TestIngestSortAndVerify(t *testing.T) {
	isError := func(err error, re string) bool {
		if err == nil && re == "" {
			return true
		}
		if err == nil || re == "" {
			return false
		}
		matched, merr := regexp.MatchString(re, err.Error())
		if merr != nil {
			return false
		}
		return matched
	}

	testCases := []struct {
		input    string
		expected string
	}{
		{"", ""},
		{"a-b", ""},
		{"a-b c-d e-f", ""},
		{"c-d a-b e-f", ""},
		{"a-b b-d e-f", "files have overlapping ranges"},
		{"c-d d-e a-b", "files have overlapping ranges"},
	}

	comparers := []struct {
		name string
		cmp  db.Compare
	}{
		{"default", db.DefaultComparer.Compare},
		{"reverse", func(a, b []byte) int { return db.DefaultComparer.Compare(b, a) }},
	}

	for _, comparer := range comparers {
		t.Run(comparer.name, func(t *testing.T) {
			cmp := comparer.cmp
			for _, c := range testCases {
				t.Run("", func(t *testing.T) {
					var meta []*ingestMetadata
					for _, p := range strings.Fields(c.input) {
						parts := strings.Split(p, "-")
						if len(parts) != 2 {
							t.Fatalf("malformed test case: %s", c.input)
						}
						if cmp([]byte(parts[0]), []byte(parts[1])) > 0 {
							parts[0], parts[1] = parts[1], parts[0]
						}
						meta = append(meta, &ingestMetadata{
							fileMetadata: fileMetadata{
								smallest: db.InternalKey{UserKey: []byte(parts[0])},
								largest:  db.InternalKey{UserKey: []byte(parts[1])},
							},
						})
					}
					if err := ingestSortAndVerify(cmp, meta); !isError(err, c.expected) {
						t.Fatalf("expected %s, but found %v", c.expected, err)
					}
					sorted := sort.SliceIsSorted(meta, func(i, j int) bool {
						return cmp(meta[i].smallest.UserKey, meta[j].smallest.UserKey) < 0
					})
					if !sorted {
						t.Fatalf("expected files to be sorted")
					}
				})
			}
		})
	}
}

func TestIngestLink(t *testing.T) {
	// TODO(peter): Test linking of tables into the DB directory. Test cleanup
	// when one of the tables cannot be linked.
}

func TestIngestMemtableOverlaps(t *testing.T) {
	// TODO(peter): Test detection of memtable overlaps.
}

func TestIngestTargetLevel(t *testing.T) {
	// TODO(peter): Test various cases for ingesting sstables into the correct
	// level of the LSM.
}

func TestIngestGlobalSeqNum(t *testing.T) {
	// TODO(peter): Test that the sequence number for entries added via ingestion
	// is correct.
}

func TestIngest(t *testing.T) {
	// TODO(peter): Test that ingest works end-to-end.
}
