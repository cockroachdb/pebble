// Copyright 2013 The LevelDB-Go Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package leveldb

import (
	"sort"
	"strconv"
	"strings"
	"testing"

	"code.google.com/p/leveldb-go/leveldb/db"
)

func TestPickCompaction(t *testing.T) {
	fileNums := func(f []fileMetadata) string {
		ss := make([]string, 0, len(f))
		for _, meta := range f {
			ss = append(ss, strconv.Itoa(int(meta.fileNum)))
		}
		sort.Strings(ss)
		return strings.Join(ss, ",")
	}

	testCases := []struct {
		desc    string
		version version
		want    string
	}{
		{
			desc: "no compaction",
			version: version{
				files: [numLevels][]fileMetadata{
					0: []fileMetadata{
						{
							fileNum:  100,
							size:     1,
							smallest: makeIkey("i.SET.101"),
							largest:  makeIkey("j.SET.102"),
						},
					},
				},
			},
			want: "",
		},

		{
			desc: "1 L0 file",
			version: version{
				files: [numLevels][]fileMetadata{
					0: []fileMetadata{
						{
							fileNum:  100,
							size:     1,
							smallest: makeIkey("i.SET.101"),
							largest:  makeIkey("j.SET.102"),
						},
					},
				},
				compactionScore: 99,
				compactionLevel: 0,
			},
			want: "100  ",
		},

		{
			desc: "2 L0 files (0 overlaps)",
			version: version{
				files: [numLevels][]fileMetadata{
					0: []fileMetadata{
						{
							fileNum:  100,
							size:     1,
							smallest: makeIkey("i.SET.101"),
							largest:  makeIkey("j.SET.102"),
						},
						{
							fileNum:  110,
							size:     1,
							smallest: makeIkey("k.SET.111"),
							largest:  makeIkey("l.SET.112"),
						},
					},
				},
				compactionScore: 99,
				compactionLevel: 0,
			},
			want: "100  ",
		},

		{
			desc: "2 L0 files, with ikey overlap",
			version: version{
				files: [numLevels][]fileMetadata{
					0: []fileMetadata{
						{
							fileNum:  100,
							size:     1,
							smallest: makeIkey("i.SET.101"),
							largest:  makeIkey("p.SET.102"),
						},
						{
							fileNum:  110,
							size:     1,
							smallest: makeIkey("j.SET.111"),
							largest:  makeIkey("q.SET.112"),
						},
					},
				},
				compactionScore: 99,
				compactionLevel: 0,
			},
			want: "100,110  ",
		},

		{
			desc: "2 L0 files, with ukey overlap",
			version: version{
				files: [numLevels][]fileMetadata{
					0: []fileMetadata{
						{
							fileNum:  100,
							size:     1,
							smallest: makeIkey("i.SET.101"),
							largest:  makeIkey("i.SET.102"),
						},
						{
							fileNum:  110,
							size:     1,
							smallest: makeIkey("i.SET.111"),
							largest:  makeIkey("i.SET.112"),
						},
					},
				},
				compactionScore: 99,
				compactionLevel: 0,
			},
			want: "100,110  ",
		},

		{
			desc: "1 L0 file, 2 L1 files (0 overlaps)",
			version: version{
				files: [numLevels][]fileMetadata{
					0: []fileMetadata{
						{
							fileNum:  100,
							size:     1,
							smallest: makeIkey("i.SET.101"),
							largest:  makeIkey("i.SET.102"),
						},
					},
					1: []fileMetadata{
						{
							fileNum:  200,
							size:     1,
							smallest: makeIkey("a.SET.201"),
							largest:  makeIkey("b.SET.202"),
						},
						{
							fileNum:  210,
							size:     1,
							smallest: makeIkey("y.SET.211"),
							largest:  makeIkey("z.SET.212"),
						},
					},
				},
				compactionScore: 99,
				compactionLevel: 0,
			},
			want: "100  ",
		},

		{
			desc: "1 L0 file, 2 L1 files (1 overlap), 4 L2 files (3 overlaps)",
			version: version{
				files: [numLevels][]fileMetadata{
					0: []fileMetadata{
						{
							fileNum:  100,
							size:     1,
							smallest: makeIkey("i.SET.101"),
							largest:  makeIkey("t.SET.102"),
						},
					},
					1: []fileMetadata{
						{
							fileNum:  200,
							size:     1,
							smallest: makeIkey("a.SET.201"),
							largest:  makeIkey("e.SET.202"),
						},
						{
							fileNum:  210,
							size:     1,
							smallest: makeIkey("f.SET.211"),
							largest:  makeIkey("j.SET.212"),
						},
					},
					2: []fileMetadata{
						{
							fileNum:  300,
							size:     1,
							smallest: makeIkey("a.SET.301"),
							largest:  makeIkey("b.SET.302"),
						},
						{
							fileNum:  310,
							size:     1,
							smallest: makeIkey("c.SET.311"),
							largest:  makeIkey("g.SET.312"),
						},
						{
							fileNum:  320,
							size:     1,
							smallest: makeIkey("h.SET.321"),
							largest:  makeIkey("m.SET.322"),
						},
						{
							fileNum:  330,
							size:     1,
							smallest: makeIkey("n.SET.331"),
							largest:  makeIkey("z.SET.332"),
						},
					},
				},
				compactionScore: 99,
				compactionLevel: 0,
			},
			want: "100 210 310,320,330",
		},

		{
			desc: "4 L1 files, 2 L2 files, can grow",
			version: version{
				files: [numLevels][]fileMetadata{
					1: []fileMetadata{
						{
							fileNum:  200,
							size:     1,
							smallest: makeIkey("i1.SET.201"),
							largest:  makeIkey("i2.SET.202"),
						},
						{
							fileNum:  210,
							size:     1,
							smallest: makeIkey("j1.SET.211"),
							largest:  makeIkey("j2.SET.212"),
						},
						{
							fileNum:  220,
							size:     1,
							smallest: makeIkey("k1.SET.221"),
							largest:  makeIkey("k2.SET.222"),
						},
						{
							fileNum:  230,
							size:     1,
							smallest: makeIkey("l1.SET.231"),
							largest:  makeIkey("l2.SET.232"),
						},
					},
					2: []fileMetadata{
						{
							fileNum:  300,
							size:     1,
							smallest: makeIkey("a0.SET.301"),
							largest:  makeIkey("l0.SET.302"),
						},
						{
							fileNum:  310,
							size:     1,
							smallest: makeIkey("l2.SET.311"),
							largest:  makeIkey("z2.SET.312"),
						},
					},
				},
				compactionScore: 99,
				compactionLevel: 1,
			},
			want: "200,210,220 300 ",
		},

		{
			desc: "4 L1 files, 2 L2 files, can't grow (range)",
			version: version{
				files: [numLevels][]fileMetadata{
					1: []fileMetadata{
						{
							fileNum:  200,
							size:     1,
							smallest: makeIkey("i1.SET.201"),
							largest:  makeIkey("i2.SET.202"),
						},
						{
							fileNum:  210,
							size:     1,
							smallest: makeIkey("j1.SET.211"),
							largest:  makeIkey("j2.SET.212"),
						},
						{
							fileNum:  220,
							size:     1,
							smallest: makeIkey("k1.SET.221"),
							largest:  makeIkey("k2.SET.222"),
						},
						{
							fileNum:  230,
							size:     1,
							smallest: makeIkey("l1.SET.231"),
							largest:  makeIkey("l2.SET.232"),
						},
					},
					2: []fileMetadata{
						{
							fileNum:  300,
							size:     1,
							smallest: makeIkey("a0.SET.301"),
							largest:  makeIkey("j0.SET.302"),
						},
						{
							fileNum:  310,
							size:     1,
							smallest: makeIkey("j2.SET.311"),
							largest:  makeIkey("z2.SET.312"),
						},
					},
				},
				compactionScore: 99,
				compactionLevel: 1,
			},
			want: "200 300 ",
		},

		{
			desc: "4 L1 files, 2 L2 files, can't grow (size)",
			version: version{
				files: [numLevels][]fileMetadata{
					1: []fileMetadata{
						{
							fileNum:  200,
							size:     expandedCompactionByteSizeLimit - 1,
							smallest: makeIkey("i1.SET.201"),
							largest:  makeIkey("i2.SET.202"),
						},
						{
							fileNum:  210,
							size:     expandedCompactionByteSizeLimit - 1,
							smallest: makeIkey("j1.SET.211"),
							largest:  makeIkey("j2.SET.212"),
						},
						{
							fileNum:  220,
							size:     expandedCompactionByteSizeLimit - 1,
							smallest: makeIkey("k1.SET.221"),
							largest:  makeIkey("k2.SET.222"),
						},
						{
							fileNum:  230,
							size:     expandedCompactionByteSizeLimit - 1,
							smallest: makeIkey("l1.SET.231"),
							largest:  makeIkey("l2.SET.232"),
						},
					},
					2: []fileMetadata{
						{
							fileNum:  300,
							size:     expandedCompactionByteSizeLimit - 1,
							smallest: makeIkey("a0.SET.301"),
							largest:  makeIkey("l0.SET.302"),
						},
						{
							fileNum:  310,
							size:     expandedCompactionByteSizeLimit - 1,
							smallest: makeIkey("l2.SET.311"),
							largest:  makeIkey("z2.SET.312"),
						},
					},
				},
				compactionScore: 99,
				compactionLevel: 1,
			},
			want: "200 300 ",
		},
	}

	for _, tc := range testCases {
		vs := &versionSet{
			ucmp: db.DefaultComparer,
			icmp: internalKeyComparer{db.DefaultComparer},
		}
		vs.dummyVersion.prev = &vs.dummyVersion
		vs.dummyVersion.next = &vs.dummyVersion
		vs.append(&tc.version)

		c, got := pickCompaction(vs), ""
		if c != nil {
			got0 := fileNums(c.inputs[0])
			got1 := fileNums(c.inputs[1])
			got2 := fileNums(c.inputs[2])
			got = got0 + " " + got1 + " " + got2
		}
		if got != tc.want {
			t.Fatalf("%s:\ngot  %q\nwant %q", tc.desc, got, tc.want)
		}
	}
}
