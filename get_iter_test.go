// Copyright 2018 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package pebble

import (
	"strings"
	"testing"

	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/pebble/internal/base"
	"github.com/cockroachdb/pebble/internal/manifest"
)

func TestGetIter(t *testing.T) {
	// testTable is a table to insert into a version.
	// Each element of data is a string of the form "internalKey value".
	type testTable struct {
		level   int
		fileNum FileNum
		data    []string
	}

	testCases := []struct {
		description string
		// badOrdering is whether this test case has a table ordering violation.
		badOrdering bool
		// tables are the tables to populate the version with.
		tables []testTable
		// queries are the queries to run against the version. Each element has
		// the form "internalKey wantedValue". The internalKey is passed to the
		// version.get method, wantedValue may be "ErrNotFound" if the query
		// should return that error.
		queries []string
	}{
		{
			description: "empty: an empty version",
			queries: []string{
				"abc.MAX.101 ErrNotFound",
			},
		},

		{
			description: "single-0: one level-0 table",
			tables: []testTable{
				{
					level:   0,
					fileNum: 10,
					data: []string{
						"the.SET.101 a",
						"cat.SET.102 b",
						"on_.SET.103 c",
						"the.SET.104 d",
						"mat.SET.105 e",
						"the.DEL.106 ",
						"the.MERGE.107 g",
					},
				},
			},
			queries: []string{
				"aaa.MAX.105 ErrNotFound",
				"cat.MAX.105 b",
				"hat.MAX.105 ErrNotFound",
				"mat.MAX.105 e",
				"the.MAX.108 g",
				"the.MAX.107 g",
				"the.MAX.106 ErrNotFound",
				"the.MAX.105 d",
				"the.MAX.104 d",
				"the.MAX.104 d",
				"the.MAX.103 a",
				"the.MAX.102 a",
				"the.MAX.101 a",
				"the.MAX.100 ErrNotFound",
				"zzz.MAX.105 ErrNotFound",
			},
		},

		{
			description: "triple-0: three level-0 tables",
			tables: []testTable{
				{
					level:   0,
					fileNum: 10,
					data: []string{
						"the.SET.101 a",
						"cat.SET.102 b",
						"on_.SET.103 c",
						"the.SET.104 d",
						"mat.SET.105 e",
						"the.DEL.106 ",
						"the.SET.107 g",
					},
				},
				{
					level:   0,
					fileNum: 11,
					data: []string{
						"awk.SET.111 w",
						"cat.SET.112 x",
						"man.SET.113 y",
						"sed.SET.114 z",
					},
				},
				{
					level:   0,
					fileNum: 12,
					data: []string{
						"the.DEL.121 ",
						"cat.DEL.122 ",
						"man.DEL.123 ",
						"was.SET.124 D",
						"not.SET.125 E",
						"the.SET.126 F",
						"man.SET.127 G",
					},
				},
			},
			queries: []string{
				"aaa.MAX.105 ErrNotFound",
				"awk.MAX.135 w",
				"awk.MAX.125 w",
				"awk.MAX.115 w",
				"awk.MAX.105 ErrNotFound",
				"cat.MAX.135 ErrNotFound",
				"cat.MAX.125 ErrNotFound",
				"cat.MAX.115 x",
				"cat.MAX.105 b",
				"man.MAX.135 G",
				"man.MAX.125 ErrNotFound",
				"man.MAX.115 y",
				"man.MAX.105 ErrNotFound",
				"on_.MAX.135 c",
				"on_.MAX.125 c",
				"on_.MAX.115 c",
				"on_.MAX.105 c",
				"the.MAX.135 F",
				"the.MAX.127 F",
				"the.MAX.126 F",
				"the.MAX.125 ErrNotFound",
				"the.MAX.122 ErrNotFound",
				"the.MAX.121 ErrNotFound",
				"the.MAX.120 g",
				"the.MAX.115 g",
				"the.MAX.114 g",
				"the.MAX.111 g",
				"the.MAX.110 g",
				"the.MAX.108 g",
				"the.MAX.107 g",
				"the.MAX.106 ErrNotFound",
				"the.MAX.105 d",
				"the.MAX.104 d",
				"the.MAX.104 d",
				"the.MAX.103 a",
				"the.MAX.102 a",
				"the.MAX.101 a",
				"the.MAX.100 ErrNotFound",
				"zzz.MAX.105 ErrNotFound",
			},
		},

		{
			description: "quad-4: four level-4 tables",
			tables: []testTable{
				{
					level:   4,
					fileNum: 11,
					data: []string{
						"aardvark.SET.101 a1",
						"alpaca__.SET.201 a2",
						"anteater.SET.301 a3",
					},
				},
				{
					level:   4,
					fileNum: 22,
					data: []string{
						"baboon__.SET.102 b1",
						"baboon__.DEL.202 ",
						"baboon__.SET.302 b3",
						"bear____.SET.402 b4",
						"bear____.DEL.502 ",
						"buffalo_.SET.602 b6",
					},
				},
				{
					level:   4,
					fileNum: 33,
					data: []string{
						"buffalo_.SET.103 B1",
					},
				},
				{
					level:   4,
					fileNum: 44,
					data: []string{
						"chipmunk.SET.104 c1",
						"chipmunk.SET.204 c2",
					},
				},
			},
			queries: []string{
				"a_______.MAX.999 ErrNotFound",
				"aardvark.MAX.999 a1",
				"aardvark.MAX.102 a1",
				"aardvark.MAX.101 a1",
				"aardvark.MAX.100 ErrNotFound",
				"alpaca__.MAX.999 a2",
				"alpaca__.MAX.200 ErrNotFound",
				"anteater.MAX.999 a3",
				"anteater.MAX.302 a3",
				"anteater.MAX.301 a3",
				"anteater.MAX.300 ErrNotFound",
				"anteater.MAX.000 ErrNotFound",
				"b_______.MAX.999 ErrNotFound",
				"baboon__.MAX.999 b3",
				"baboon__.MAX.302 b3",
				"baboon__.MAX.301 ErrNotFound",
				"baboon__.MAX.202 ErrNotFound",
				"baboon__.MAX.201 b1",
				"baboon__.MAX.102 b1",
				"baboon__.MAX.101 ErrNotFound",
				"bear____.MAX.999 ErrNotFound",
				"bear____.MAX.500 b4",
				"bear____.MAX.000 ErrNotFound",
				"buffalo_.MAX.999 b6",
				"buffalo_.MAX.603 b6",
				"buffalo_.MAX.602 b6",
				"buffalo_.MAX.601 B1",
				"buffalo_.MAX.104 B1",
				"buffalo_.MAX.103 B1",
				"buffalo_.MAX.102 ErrNotFound",
				"buffalo_.MAX.000 ErrNotFound",
				"c_______.MAX.999 ErrNotFound",
				"chipmunk.MAX.999 c2",
				"chipmunk.MAX.205 c2",
				"chipmunk.MAX.204 c2",
				"chipmunk.MAX.203 c1",
				"chipmunk.MAX.105 c1",
				"chipmunk.MAX.104 c1",
				"chipmunk.MAX.103 ErrNotFound",
				"chipmunk.MAX.000 ErrNotFound",
				"d_______.MAX.999 ErrNotFound",
			},
		},

		{
			description: "complex: many tables at many levels",
			tables: []testTable{
				{
					level:   0,
					fileNum: 50,
					data: []string{
						"alfalfa__.SET.501 p1",
						"asparagus.SET.502 p2",
						"cabbage__.DEL.503 ",
						"spinach__.MERGE.504 p3",
					},
				},
				{
					level:   0,
					fileNum: 51,
					data: []string{
						"asparagus.SET.511 q1",
						"asparagus.SET.512 q2",
						"asparagus.SET.513 q3",
						"beans____.SET.514 q4",
						"broccoli_.SET.515 q5",
						"cabbage__.SET.516 q6",
						"celery___.SET.517 q7",
						"spinach__.MERGE.518 q8",
					},
				},
				{
					level:   1,
					fileNum: 40,
					data: []string{
						"alfalfa__.SET.410 r1",
						"asparagus.SET.420 r2",
						"arugula__.SET.430 r3",
					},
				},
				{
					level:   1,
					fileNum: 41,
					data: []string{
						"beans____.SET.411 s1",
						"beans____.SET.421 s2",
						"bokchoy__.DEL.431 ",
						"broccoli_.SET.441 s4",
					},
				},
				{
					level:   1,
					fileNum: 42,
					data: []string{
						"cabbage__.SET.412 t1",
						"corn_____.DEL.422 ",
						"spinach__.MERGE.432 t2",
					},
				},
				{
					level:   2,
					fileNum: 30,
					data: []string{
						"alfalfa__.SET.310 u1",
						"bokchoy__.SET.320 u2",
						"celery___.SET.330 u3",
						"corn_____.SET.340 u4",
						"spinach__.MERGE.350 u5",
					},
				},
			},
			queries: []string{
				"a________.MAX.999 ErrNotFound",
				"alfalfa__.MAX.520 p1",
				"alfalfa__.MAX.510 p1",
				"alfalfa__.MAX.500 r1",
				"alfalfa__.MAX.400 u1",
				"alfalfa__.MAX.300 ErrNotFound",
				"asparagus.MAX.520 q3",
				"asparagus.MAX.510 p2",
				"asparagus.MAX.500 r2",
				"asparagus.MAX.400 ErrNotFound",
				"asparagus.MAX.300 ErrNotFound",
				"arugula__.MAX.520 r3",
				"arugula__.MAX.510 r3",
				"arugula__.MAX.500 r3",
				"arugula__.MAX.400 ErrNotFound",
				"arugula__.MAX.300 ErrNotFound",
				"beans____.MAX.520 q4",
				"beans____.MAX.510 s2",
				"beans____.MAX.500 s2",
				"beans____.MAX.400 ErrNotFound",
				"beans____.MAX.300 ErrNotFound",
				"bokchoy__.MAX.520 ErrNotFound",
				"bokchoy__.MAX.510 ErrNotFound",
				"bokchoy__.MAX.500 ErrNotFound",
				"bokchoy__.MAX.400 u2",
				"bokchoy__.MAX.300 ErrNotFound",
				"broccoli_.MAX.520 q5",
				"broccoli_.MAX.510 s4",
				"broccoli_.MAX.500 s4",
				"broccoli_.MAX.400 ErrNotFound",
				"broccoli_.MAX.300 ErrNotFound",
				"cabbage__.MAX.520 q6",
				"cabbage__.MAX.510 ErrNotFound",
				"cabbage__.MAX.500 t1",
				"cabbage__.MAX.400 ErrNotFound",
				"cabbage__.MAX.300 ErrNotFound",
				"celery___.MAX.520 q7",
				"celery___.MAX.510 u3",
				"celery___.MAX.500 u3",
				"celery___.MAX.400 u3",
				"celery___.MAX.300 ErrNotFound",
				"corn_____.MAX.520 ErrNotFound",
				"corn_____.MAX.510 ErrNotFound",
				"corn_____.MAX.500 ErrNotFound",
				"corn_____.MAX.400 u4",
				"corn_____.MAX.300 ErrNotFound",
				"d________.MAX.999 ErrNotFound",
				"spinach__.MAX.999 u5t2p3q8",
				"spinach__.MAX.518 u5t2p3q8",
				"spinach__.MAX.517 u5t2p3",
				"spinach__.MAX.504 u5t2p3",
				"spinach__.MAX.503 u5t2",
				"spinach__.MAX.432 u5t2",
				"spinach__.MAX.431 u5",
				"spinach__.MAX.350 u5",
				"spinach__.MAX.349 ErrNotFound",
			},
		},

		{
			description: "broken invariants 0: non-increasing level 0 sequence numbers",
			badOrdering: true,
			tables: []testTable{
				{
					level:   0,
					fileNum: 19,
					data: []string{
						"a.SET.101 a",
						"b.SET.102 b",
					},
				},
				{
					level:   0,
					fileNum: 20,
					data: []string{
						"c.SET.101 c",
					},
				},
			},
		},

		{
			description: "broken invariants 1: non-increasing level 0 sequence numbers",
			badOrdering: true,
			tables: []testTable{
				{
					level:   0,
					fileNum: 19,
					data: []string{
						"a.SET.101 a",
						"b.SET.102 b",
					},
				},
				{
					level:   0,
					fileNum: 20,
					data: []string{
						"c.SET.100 c",
						"d.SET.101 d",
					},
				},
			},
		},

		{
			description: "broken invariants 2: matching level 0 sequence numbers, considered acceptable",
			badOrdering: false,
			tables: []testTable{
				{
					level:   0,
					fileNum: 19,
					data: []string{
						"a.SET.101 a",
					},
				},
				{
					level:   0,
					fileNum: 20,
					data: []string{
						"a.SET.101 a",
					},
				},
			},
		},

		{
			description: "broken invariants 3: level non-0 overlapping internal key ranges",
			badOrdering: true,
			tables: []testTable{
				{
					level:   5,
					fileNum: 11,
					data: []string{
						"bat.SET.101 xxx",
						"dog.SET.102 xxx",
					},
				},
				{
					level:   5,
					fileNum: 12,
					data: []string{
						"cow.SET.103 xxx",
						"pig.SET.104 xxx",
					},
				},
			},
		},
	}

	cmp := DefaultComparer.Compare
	equal := DefaultComparer.Equal
	for _, tc := range testCases {
		desc := tc.description[:strings.Index(tc.description, ":")]

		// m is a map from file numbers to DBs.
		m := map[FileNum]*memTable{}
		newIter := func(
			file *manifest.FileMetadata, _ *IterOptions, _ *uint64,
		) (internalIterator, internalIterator, error) {
			d, ok := m[file.FileNum]
			if !ok {
				return nil, nil, errors.New("no such file")
			}
			return d.newIter(nil), nil, nil
		}

		var files [numLevels][]*fileMetadata
		for _, tt := range tc.tables {
			d := newMemTable(memTableOptions{})
			defer d.close()
			m[tt.fileNum] = d

			meta := &fileMetadata{
				FileNum: tt.fileNum,
			}
			for i, datum := range tt.data {
				s := strings.Split(datum, " ")
				ikey := base.ParseInternalKey(s[0])
				err := d.set(ikey, []byte(s[1]))
				if err != nil {
					t.Fatalf("desc=%q: memtable Set: %v", desc, err)
				}

				if i == 0 {
					meta.Smallest = ikey
					meta.SmallestSeqNum = ikey.SeqNum()
					meta.Largest = ikey
					meta.LargestSeqNum = ikey.SeqNum()
				} else {
					if base.InternalCompare(cmp, ikey, meta.Smallest) < 0 {
						meta.Smallest = ikey
					}
					if base.InternalCompare(cmp, ikey, meta.Largest) > 0 {
						meta.Largest = ikey
					}
					if meta.SmallestSeqNum > ikey.SeqNum() {
						meta.SmallestSeqNum = ikey.SeqNum()
					}
					if meta.LargestSeqNum < ikey.SeqNum() {
						meta.LargestSeqNum = ikey.SeqNum()
					}
				}
			}

			files[tt.level] = append(files[tt.level], meta)
		}
		v := manifest.NewVersion(cmp, base.DefaultFormatter, 10<<20, files)
		err := v.CheckOrdering(cmp, base.DefaultFormatter)
		if tc.badOrdering && err == nil {
			t.Errorf("desc=%q: want bad ordering, got nil error", desc)
			continue
		} else if !tc.badOrdering && err != nil {
			t.Errorf("desc=%q: bad ordering: %v", desc, err)
			continue
		}

		get := func(v *version, ikey InternalKey) ([]byte, error) {
			var buf struct {
				dbi Iterator
				get getIter
			}

			get := &buf.get
			get.cmp = cmp
			get.equal = equal
			get.newIters = newIter
			get.key = ikey.UserKey
			get.l0 = v.L0SublevelFiles
			get.version = v
			get.snapshot = ikey.SeqNum() + 1

			i := &buf.dbi
			i.cmp = cmp
			i.equal = equal
			i.merge = DefaultMerger.Merge
			i.iter = get

			defer i.Close()
			if !i.First() {
				err := i.Error()
				if err != nil {
					return nil, err
				}
				return nil, ErrNotFound
			}
			return i.Value(), nil
		}

		for _, query := range tc.queries {
			s := strings.Split(query, " ")
			ikey := base.ParseInternalKey(s[0])
			value, err := get(v, ikey)
			got, want := "", s[1]
			if err != nil {
				if err != ErrNotFound {
					t.Errorf("desc=%q: query=%q: %v", desc, s[0], err)
					continue
				}
				got = "ErrNotFound"
			} else {
				got = string(value)
			}
			if got != want {
				t.Errorf("desc=%q: query=%q: got %q, want %q", desc, s[0], got, want)
			}
		}
	}
}
