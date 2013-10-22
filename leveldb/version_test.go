// Copyright 2012 The LevelDB-Go Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package leveldb

import (
	"errors"
	"fmt"
	"strconv"
	"strings"
	"testing"

	"code.google.com/p/leveldb-go/leveldb/db"
	"code.google.com/p/leveldb-go/leveldb/memdb"
)

func TestIkeyRange(t *testing.T) {
	testCases := []struct {
		input, want string
	}{
		{
			"",
			"-",
		},
		{
			"a-e",
			"a-e",
		},
		{
			"a-e a-e",
			"a-e",
		},
		{
			"c-g a-e",
			"a-g",
		},
		{
			"a-e c-g a-e",
			"a-g",
		},
		{
			"b-d f-g",
			"b-g",
		},
		{
			"d-e b-d",
			"b-e",
		},
		{
			"e-e",
			"e-e",
		},
		{
			"f-g e-e d-e c-g b-d a-e",
			"a-g",
		},
	}
	for _, tc := range testCases {
		var f []fileMetadata
		if tc.input != "" {
			for _, s := range strings.Split(tc.input, " ") {
				f = append(f, fileMetadata{
					smallest: internalKey(s[0:1]),
					largest:  internalKey(s[2:3]),
				})
			}
		}

		smallest0, largest0 := ikeyRange(db.DefaultComparer, f, nil)
		got0 := string(smallest0) + "-" + string(largest0)
		if got0 != tc.want {
			t.Errorf("first []fileMetadata is %v\ngot  %s\nwant %s", tc.input, got0, tc.want)
		}

		smallest1, largest1 := ikeyRange(db.DefaultComparer, nil, f)
		got1 := string(smallest1) + "-" + string(largest1)
		if got1 != tc.want {
			t.Errorf("second []fileMetadata is %v\ngot  %s\nwant %s", tc.input, got1, tc.want)
		}
	}
}

type tableIkeyFinderFunc func(fileNum uint64, ikey internalKey) (db.Iterator, error)

func (f tableIkeyFinderFunc) find(fileNum uint64, ikey internalKey) (db.Iterator, error) {
	return f(fileNum, ikey)
}

var makeIkeyKinds = map[string]internalKeyKind{
	"DEL": internalKeyKindDelete,
	"MAX": internalKeyKindMax,
	"SET": internalKeyKindSet,
}

// makeIkey converts a string like "foo.DEL.123" into an internal key
// consisting of a user key "foo", kind delete, and sequence number 123.
func makeIkey(s string) internalKey {
	x := strings.Split(s, ".")
	ukey := x[0]
	kind := makeIkeyKinds[x[1]]
	seqNum, _ := strconv.ParseUint(x[2], 10, 64)
	return makeInternalKey(nil, []byte(ukey), kind, seqNum)
}

func TestVersion(t *testing.T) {
	// testTable is a table to insert into a version.
	// Each element of data is a string of the form "internalKey value".
	type testTable struct {
		level   int
		fileNum uint64
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
						"the.SET.107 g",
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
			},
		},

		{
			description: "broken invariants 0: non-increasing level 0 file numbers",
			badOrdering: true,
			tables: []testTable{
				{
					level:   0,
					fileNum: 20,
				},
				{
					level:   0,
					fileNum: 19,
				},
			},
		},

		{
			description: "broken invariants 1: non-increasing level 0 file numbers",
			badOrdering: true,
			tables: []testTable{
				{
					level:   0,
					fileNum: 20,
				},
				{
					level:   0,
					fileNum: 21,
				},
				{
					level:   0,
					fileNum: 21,
				},
				{
					level:   0,
					fileNum: 22,
				},
			},
		},

		{
			description: "broken invariants 2: level non-0 overlapping internal key ranges",
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

	icmp := internalKeyComparer{db.DefaultComparer}
	for _, tc := range testCases {
		desc := tc.description[:strings.Index(tc.description, ":")]

		// m is a map from file numbers to DBs.
		m := map[uint64]db.DB{}
		tiFinder := tableIkeyFinderFunc(func(fileNum uint64, ikey internalKey) (db.Iterator, error) {
			d, ok := m[fileNum]
			if !ok {
				return nil, errors.New("no such file")
			}
			return d.Find(ikey, nil), nil
		})

		v := version{}
		for _, tt := range tc.tables {
			d := memdb.New(&db.Options{
				Comparer: icmp,
			})
			defer d.Close()
			m[tt.fileNum] = d

			var smallest, largest internalKey
			for i, datum := range tt.data {
				s := strings.Split(datum, " ")
				ikey := makeIkey(s[0])
				err := d.Set(ikey, []byte(s[1]), nil)
				if err != nil {
					t.Fatalf("desc=%q: memdb Set: %v", desc, err)
				}

				if i == 0 {
					smallest = ikey
					largest = ikey
				} else {
					if icmp.Compare(ikey, smallest) < 0 {
						smallest = ikey
					}
					if icmp.Compare(ikey, largest) > 0 {
						largest = ikey
					}
				}
			}

			v.files[tt.level] = append(v.files[tt.level], fileMetadata{
				fileNum:  tt.fileNum,
				smallest: smallest,
				largest:  largest,
			})
		}

		err := v.checkOrdering(icmp)
		if tc.badOrdering && err == nil {
			t.Errorf("desc=%q: want bad ordering, got nil error", desc)
			continue
		} else if !tc.badOrdering && err != nil {
			t.Errorf("desc=%q: bad ordering: %v", desc, err)
			continue
		}

		for _, query := range tc.queries {
			s := strings.Split(query, " ")
			value, err := v.get(makeIkey(s[0]), tiFinder, db.DefaultComparer, nil)
			got, want := "", s[1]
			if err != nil {
				if err != db.ErrNotFound {
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

func TestOverlaps(t *testing.T) {
	m00 := fileMetadata{
		fileNum:  700,
		size:     1,
		smallest: makeIkey("b.SET.7008"),
		largest:  makeIkey("e.SET.7009"),
	}
	m01 := fileMetadata{
		fileNum:  701,
		size:     1,
		smallest: makeIkey("c.SET.7018"),
		largest:  makeIkey("f.SET.7019"),
	}
	m02 := fileMetadata{
		fileNum:  702,
		size:     1,
		smallest: makeIkey("f.SET.7028"),
		largest:  makeIkey("g.SET.7029"),
	}
	m03 := fileMetadata{
		fileNum:  703,
		size:     1,
		smallest: makeIkey("x.SET.7038"),
		largest:  makeIkey("y.SET.7039"),
	}
	m04 := fileMetadata{
		fileNum:  704,
		size:     1,
		smallest: makeIkey("n.SET.7048"),
		largest:  makeIkey("p.SET.7049"),
	}
	m05 := fileMetadata{
		fileNum:  705,
		size:     1,
		smallest: makeIkey("p.SET.7058"),
		largest:  makeIkey("p.SET.7059"),
	}
	m06 := fileMetadata{
		fileNum:  706,
		size:     1,
		smallest: makeIkey("p.SET.7068"),
		largest:  makeIkey("u.SET.7069"),
	}
	m07 := fileMetadata{
		fileNum:  707,
		size:     1,
		smallest: makeIkey("r.SET.7078"),
		largest:  makeIkey("s.SET.7079"),
	}

	m10 := fileMetadata{
		fileNum:  710,
		size:     1,
		smallest: makeIkey("d.SET.7108"),
		largest:  makeIkey("g.SET.7109"),
	}
	m11 := fileMetadata{
		fileNum:  711,
		size:     1,
		smallest: makeIkey("g.SET.7118"),
		largest:  makeIkey("j.SET.7119"),
	}
	m12 := fileMetadata{
		fileNum:  712,
		size:     1,
		smallest: makeIkey("n.SET.7128"),
		largest:  makeIkey("p.SET.7129"),
	}
	m13 := fileMetadata{
		fileNum:  713,
		size:     1,
		smallest: makeIkey("p.SET.7138"),
		largest:  makeIkey("p.SET.7139"),
	}
	m14 := fileMetadata{
		fileNum:  714,
		size:     1,
		smallest: makeIkey("p.SET.7148"),
		largest:  makeIkey("u.SET.7149"),
	}

	v := version{
		files: [numLevels][]fileMetadata{
			0: {m00, m01, m02, m03, m04, m05, m06, m07},
			1: {m10, m11, m12, m13, m14},
		},
	}

	testCases := []struct {
		level        int
		ukey0, ukey1 string
		want         string
	}{
		// Level 0: m00=b-e, m01=c-f, m02=f-g, m03=x-y, m04=n-p, m05=p-p, m06=p-u, m07=r-s.
		// Note that:
		//   - the slice isn't sorted (e.g. m02=f-g, m03=x-y, m04=n-p),
		//   - m00 and m01 overlap (not just touch),
		//   - m06 contains m07,
		//   - m00, m01 and m02 transitively overlap/touch each other, and
		//   - m04, m05, m06 and m07 transitively overlap/touch each other.
		{0, "a", "a", ""},
		{0, "a", "b", "m00 m01 m02"},
		{0, "a", "d", "m00 m01 m02"},
		{0, "a", "e", "m00 m01 m02"},
		{0, "a", "g", "m00 m01 m02"},
		{0, "a", "z", "m00 m01 m02 m03 m04 m05 m06 m07"},
		{0, "c", "e", "m00 m01 m02"},
		{0, "d", "d", "m00 m01 m02"},
		{0, "g", "n", "m00 m01 m02 m04 m05 m06 m07"},
		{0, "h", "i", ""},
		{0, "h", "o", "m04 m05 m06 m07"},
		{0, "h", "u", "m04 m05 m06 m07"},
		{0, "k", "l", ""},
		{0, "k", "o", "m04 m05 m06 m07"},
		{0, "k", "p", "m04 m05 m06 m07"},
		{0, "n", "o", "m04 m05 m06 m07"},
		{0, "n", "z", "m03 m04 m05 m06 m07"},
		{0, "o", "z", "m03 m04 m05 m06 m07"},
		{0, "p", "z", "m03 m04 m05 m06 m07"},
		{0, "q", "z", "m03 m04 m05 m06 m07"},
		{0, "r", "s", "m04 m05 m06 m07"},
		{0, "r", "z", "m03 m04 m05 m06 m07"},
		{0, "s", "z", "m03 m04 m05 m06 m07"},
		{0, "u", "z", "m03 m04 m05 m06 m07"},
		{0, "y", "z", "m03"},
		{0, "z", "z", ""},

		// Level 1: m10=d-g, m11=g-j, m12=n-p, m13=p-p, m14=p-u.
		{1, "a", "a", ""},
		{1, "a", "b", ""},
		{1, "a", "d", "m10"},
		{1, "a", "e", "m10"},
		{1, "a", "g", "m10 m11"},
		{1, "a", "z", "m10 m11 m12 m13 m14"},
		{1, "c", "e", "m10"},
		{1, "d", "d", "m10"},
		{1, "g", "n", "m10 m11 m12"},
		{1, "h", "i", "m11"},
		{1, "h", "o", "m11 m12"},
		{1, "h", "u", "m11 m12 m13 m14"},
		{1, "k", "l", ""},
		{1, "k", "o", "m12"},
		{1, "k", "p", "m12 m13 m14"},
		{1, "n", "o", "m12"},
		{1, "n", "z", "m12 m13 m14"},
		{1, "o", "z", "m12 m13 m14"},
		{1, "p", "z", "m12 m13 m14"},
		{1, "q", "z", "m14"},
		{1, "r", "s", "m14"},
		{1, "r", "z", "m14"},
		{1, "s", "z", "m14"},
		{1, "u", "z", "m14"},
		{1, "y", "z", ""},
		{1, "z", "z", ""},

		// Level 2: empty.
		{2, "a", "z", ""},
	}

	for _, tc := range testCases {
		o := v.overlaps(tc.level, db.DefaultComparer, []byte(tc.ukey0), []byte(tc.ukey1))
		s := make([]string, len(o))
		for i, meta := range o {
			s[i] = fmt.Sprintf("m%02d", meta.fileNum%100)
		}
		got := strings.Join(s, " ")
		if got != tc.want {
			t.Errorf("level=%d, range=%s-%s\ngot  %v\nwant %v", tc.level, tc.ukey0, tc.ukey1, got, tc.want)
		}
	}
}
