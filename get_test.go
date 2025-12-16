// Copyright 2018 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package pebble

import (
	"context"
	"strings"
	"testing"

	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/pebble/internal/base"
	"github.com/cockroachdb/pebble/internal/manifest"
	"github.com/cockroachdb/pebble/internal/testkeys"
	"github.com/cockroachdb/pebble/internal/testutils"
)

func TestGetIter(t *testing.T) {
	// testTable is a table to insert into a version.
	// Each element of data is a string of the form "internalKey value".
	type testTable struct {
		level    int
		tableNum base.TableNum
		data     []string
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
				"abc#101,SEPARATOR ErrNotFound",
			},
		},

		{
			description: "single-0: one level-0 table",
			tables: []testTable{
				{
					level:    0,
					tableNum: 10,
					data: []string{
						"the#101,SET a",
						"cat#102,SET b",
						"on_#103,SET c",
						"the#104,SET d",
						"mat#105,SET e",
						"the#106,DEL ",
						"the#107,MERGE g",
					},
				},
			},
			queries: []string{
				"aaa#105,SEPARATOR ErrNotFound",
				"cat#105,SEPARATOR b",
				"hat#105,SEPARATOR ErrNotFound",
				"mat#105,SEPARATOR e",
				"the#108,SEPARATOR g",
				"the#107,SEPARATOR g",
				"the#106,SEPARATOR ErrNotFound",
				"the#105,SEPARATOR d",
				"the#104,SEPARATOR d",
				"the#104,SEPARATOR d",
				"the#103,SEPARATOR a",
				"the#102,SEPARATOR a",
				"the#101,SEPARATOR a",
				"the#100,SEPARATOR ErrNotFound",
				"zzz#105,SEPARATOR ErrNotFound",
			},
		},

		{
			description: "triple-0: three level-0 tables",
			tables: []testTable{
				{
					level:    0,
					tableNum: 10,
					data: []string{
						"the#101,SET a",
						"cat#102,SET b",
						"on_#103,SET c",
						"the#104,SET d",
						"mat#105,SET e",
						"the#106,DEL ",
						"the#107,SET g",
					},
				},
				{
					level:    0,
					tableNum: 11,
					data: []string{
						"awk#111,SET w",
						"cat#112,SET x",
						"man#113,SET y",
						"sed#114,SET z",
					},
				},
				{
					level:    0,
					tableNum: 12,
					data: []string{
						"the#121,DEL ",
						"cat#122,DEL ",
						"man#123,DEL ",
						"was#124,SET D",
						"not#125,SET E",
						"the#126,SET F",
						"man#127,SET G",
					},
				},
			},
			queries: []string{
				"aaa#105,SEPARATOR ErrNotFound",
				"awk#135,SEPARATOR w",
				"awk#125,SEPARATOR w",
				"awk#115,SEPARATOR w",
				"awk#105,SEPARATOR ErrNotFound",
				"cat#135,SEPARATOR ErrNotFound",
				"cat#125,SEPARATOR ErrNotFound",
				"cat#115,SEPARATOR x",
				"cat#105,SEPARATOR b",
				"man#135,SEPARATOR G",
				"man#125,SEPARATOR ErrNotFound",
				"man#115,SEPARATOR y",
				"man#105,SEPARATOR ErrNotFound",
				"on_#135,SEPARATOR c",
				"on_#125,SEPARATOR c",
				"on_#115,SEPARATOR c",
				"on_#105,SEPARATOR c",
				"the#135,SEPARATOR F",
				"the#127,SEPARATOR F",
				"the#126,SEPARATOR F",
				"the#125,SEPARATOR ErrNotFound",
				"the#122,SEPARATOR ErrNotFound",
				"the#121,SEPARATOR ErrNotFound",
				"the#120,SEPARATOR g",
				"the#115,SEPARATOR g",
				"the#114,SEPARATOR g",
				"the#111,SEPARATOR g",
				"the#110,SEPARATOR g",
				"the#108,SEPARATOR g",
				"the#107,SEPARATOR g",
				"the#106,SEPARATOR ErrNotFound",
				"the#105,SEPARATOR d",
				"the#104,SEPARATOR d",
				"the#104,SEPARATOR d",
				"the#103,SEPARATOR a",
				"the#102,SEPARATOR a",
				"the#101,SEPARATOR a",
				"the#100,SEPARATOR ErrNotFound",
				"zzz#105,SEPARATOR ErrNotFound",
			},
		},

		{
			description: "complex: many tables at many levels",
			tables: []testTable{
				{
					level:    0,
					tableNum: 50,
					data: []string{
						"alfalfa__#501,SET p1",
						"asparagus#502,SET p2",
						"cabbage__#503,DEL ",
						"spinach__#504,MERGE p3",
					},
				},
				{
					level:    0,
					tableNum: 51,
					data: []string{
						"asparagus#511,SET q1",
						"asparagus#512,SET q2",
						"asparagus#513,SET q3",
						"beans____#514,SET q4",
						"broccoli_#515,SET q5",
						"cabbage__#516,SET q6",
						"celery___#517,SET q7",
						"spinach__#518,MERGE q8",
					},
				},
				{
					level:    1,
					tableNum: 40,
					data: []string{
						"alfalfa__#410,SET r1",
						"asparagus#420,SET r2",
						"arugula__#430,SET r3",
					},
				},
				{
					level:    1,
					tableNum: 41,
					data: []string{
						"beans____#411,SET s1",
						"beans____#421,SET s2",
						"bokchoy__#431,DEL ",
						"broccoli_#441,SET s4",
					},
				},
				{
					level:    1,
					tableNum: 42,
					data: []string{
						"cabbage__#412,SET t1",
						"corn_____#422,DEL ",
						"spinach__#432,MERGE t2",
					},
				},
				{
					level:    2,
					tableNum: 30,
					data: []string{
						"alfalfa__#310,SET u1",
						"bokchoy__#320,SET u2",
						"celery___#330,SET u3",
						"corn_____#340,SET u4",
						"spinach__#350,MERGE u5",
					},
				},
			},
			queries: []string{
				"a________#999,SEPARATOR ErrNotFound",
				"alfalfa__#520,SEPARATOR p1",
				"alfalfa__#510,SEPARATOR p1",
				"alfalfa__#500,SEPARATOR r1",
				"alfalfa__#400,SEPARATOR u1",
				"alfalfa__#300,SEPARATOR ErrNotFound",
				"asparagus#520,SEPARATOR q3",
				"asparagus#510,SEPARATOR p2",
				"asparagus#500,SEPARATOR r2",
				"asparagus#400,SEPARATOR ErrNotFound",
				"asparagus#300,SEPARATOR ErrNotFound",
				"arugula__#520,SEPARATOR r3",
				"arugula__#510,SEPARATOR r3",
				"arugula__#500,SEPARATOR r3",
				"arugula__#400,SEPARATOR ErrNotFound",
				"arugula__#300,SEPARATOR ErrNotFound",
				"beans____#520,SEPARATOR q4",
				"beans____#510,SEPARATOR s2",
				"beans____#500,SEPARATOR s2",
				"beans____#400,SEPARATOR ErrNotFound",
				"beans____#300,SEPARATOR ErrNotFound",
				"bokchoy__#520,SEPARATOR ErrNotFound",
				"bokchoy__#510,SEPARATOR ErrNotFound",
				"bokchoy__#500,SEPARATOR ErrNotFound",
				"bokchoy__#400,SEPARATOR u2",
				"bokchoy__#300,SEPARATOR ErrNotFound",
				"broccoli_#520,SEPARATOR q5",
				"broccoli_#510,SEPARATOR s4",
				"broccoli_#500,SEPARATOR s4",
				"broccoli_#400,SEPARATOR ErrNotFound",
				"broccoli_#300,SEPARATOR ErrNotFound",
				"cabbage__#520,SEPARATOR q6",
				"cabbage__#510,SEPARATOR ErrNotFound",
				"cabbage__#500,SEPARATOR t1",
				"cabbage__#400,SEPARATOR ErrNotFound",
				"cabbage__#300,SEPARATOR ErrNotFound",
				"celery___#520,SEPARATOR q7",
				"celery___#510,SEPARATOR u3",
				"celery___#500,SEPARATOR u3",
				"celery___#400,SEPARATOR u3",
				"celery___#300,SEPARATOR ErrNotFound",
				"corn_____#520,SEPARATOR ErrNotFound",
				"corn_____#510,SEPARATOR ErrNotFound",
				"corn_____#500,SEPARATOR ErrNotFound",
				"corn_____#400,SEPARATOR u4",
				"corn_____#300,SEPARATOR ErrNotFound",
				"d________#999,SEPARATOR ErrNotFound",
				"spinach__#999,SEPARATOR u5t2p3q8",
				"spinach__#518,SEPARATOR u5t2p3q8",
				"spinach__#517,SEPARATOR u5t2p3",
				"spinach__#504,SEPARATOR u5t2p3",
				"spinach__#503,SEPARATOR u5t2",
				"spinach__#432,SEPARATOR u5t2",
				"spinach__#431,SEPARATOR u5",
				"spinach__#350,SEPARATOR u5",
				"spinach__#349,SEPARATOR ErrNotFound",
			},
		},

		{
			description: "broken invariants 0: non-increasing level 0 sequence numbers",
			badOrdering: true,
			tables: []testTable{
				{
					level:    0,
					tableNum: 19,
					data: []string{
						"a#101,SET a",
						"b#102,SET b",
					},
				},
				{
					level:    0,
					tableNum: 20,
					data: []string{
						"c#101,SET c",
					},
				},
			},
		},

		{
			description: "broken invariants 1: non-increasing level 0 sequence numbers",
			badOrdering: true,
			tables: []testTable{
				{
					level:    0,
					tableNum: 19,
					data: []string{
						"a#101,SET a",
						"b#102,SET b",
					},
				},
				{
					level:    0,
					tableNum: 20,
					data: []string{
						"c#100,SET c",
						"d#101,SET d",
					},
				},
			},
		},

		{
			description: "broken invariants 2: matching level 0 sequence numbers, considered acceptable",
			badOrdering: false,
			tables: []testTable{
				{
					level:    0,
					tableNum: 19,
					data: []string{
						"a#101,SET a",
					},
				},
				{
					level:    0,
					tableNum: 20,
					data: []string{
						"a#101,SET a",
					},
				},
			},
		},

		{
			description: "broken invariants 3: level non-0 overlapping internal key ranges",
			badOrdering: true,
			tables: []testTable{
				{
					level:    5,
					tableNum: 11,
					data: []string{
						"bat#101,SET xxx",
						"dog#102,SET xxx",
					},
				},
				{
					level:    5,
					tableNum: 12,
					data: []string{
						"cow#103,SET xxx",
						"pig#104,SET xxx",
					},
				},
			},
		},
	}

	cmp := testkeys.Comparer
	for _, tc := range testCases {
		desc := tc.description[:strings.Index(tc.description, ":")]

		// m is a map from file numbers to DBs.
		m := map[base.TableNum]*memTable{}
		newIter := func(
			_ context.Context, file *manifest.TableMetadata, _ *IterOptions, _ internalIterOpts, _ iterKinds,
		) (iterSet, error) {
			d, ok := m[file.TableNum]
			if !ok {
				return iterSet{}, errors.New("no such file")
			}
			return iterSet{point: d.newIter(nil)}, nil
		}

		var files [numLevels][]*manifest.TableMetadata
		for _, tt := range tc.tables {
			d := newMemTable(memTableOptions{})
			m[tt.tableNum] = d

			meta := &manifest.TableMetadata{
				TableNum: tt.tableNum,
			}
			meta.InitPhysicalBacking()
			for i, datum := range tt.data {
				s := strings.Split(datum, " ")
				ikey := base.ParseInternalKey(s[0])
				err := d.set(ikey, []byte(s[1]))
				if err != nil {
					t.Fatalf("desc=%q: memtable Set: %v", desc, err)
				}

				meta.ExtendPointKeyBounds(cmp.Compare, ikey, ikey)
				if i == 0 {
					meta.SeqNums.Low = ikey.SeqNum()
					meta.SeqNums.High = ikey.SeqNum()
				} else {
					if meta.SeqNums.Low > ikey.SeqNum() {
						meta.SeqNums.Low = ikey.SeqNum()
					}
					if meta.SeqNums.High < ikey.SeqNum() {
						meta.SeqNums.High = ikey.SeqNum()
					}
				}
				meta.LargestSeqNumAbsolute = meta.SeqNums.High
			}

			files[tt.level] = append(files[tt.level], meta)
		}
		l0Organizer := manifest.NewL0Organizer(cmp, 10<<20 /*flushSplitBytes*/)
		v := manifest.NewVersionForTesting(cmp, l0Organizer, files)
		err := v.CheckOrdering()
		if tc.badOrdering && err == nil {
			t.Errorf("desc=%q: want bad ordering, got nil error", desc)
			continue
		} else if !tc.badOrdering && err != nil {
			t.Errorf("desc=%q: bad ordering: %v", desc, err)
			continue
		}

		get := func(v *manifest.Version, ikey InternalKey) ([]byte, error) {
			var buf struct {
				dbi Iterator
				get getIter
			}

			get := &buf.get
			get.comparer = testkeys.Comparer
			get.newIters = newIter
			get.key = ikey.UserKey
			get.prefix = ikey.UserKey[:testkeys.Comparer.Split(ikey.UserKey)]
			get.l0 = v.L0SublevelFiles
			get.version = v
			get.snapshot = ikey.SeqNum() + 1
			get.iterOpts = IterOptions{
				Category:                      categoryGet,
				logger:                        testutils.Logger{T: t},
				snapshotForHideObsoletePoints: get.snapshot,
			}

			i := &buf.dbi
			i.comparer = testkeys.Comparer
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
