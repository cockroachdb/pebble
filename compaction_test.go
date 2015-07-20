// Copyright 2013 The LevelDB-Go Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package leveldb

import (
	"bytes"
	"fmt"
	"sort"
	"strconv"
	"strings"
	"testing"
	"time"

	"code.google.com/p/leveldb-go/leveldb/db"
	"code.google.com/p/leveldb-go/leveldb/memfs"
	"code.google.com/p/leveldb-go/leveldb/table"
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

func TestIsBaseLevelForUkey(t *testing.T) {
	testCases := []struct {
		desc    string
		level   int
		version version
		wants   map[string]bool
	}{
		{
			desc:    "empty",
			level:   1,
			version: version{},
			wants: map[string]bool{
				"x": true,
			},
		},
		{
			desc:  "non-empty",
			level: 1,
			version: version{
				files: [numLevels][]fileMetadata{
					1: []fileMetadata{
						{
							smallest: makeIkey("c.SET.801"),
							largest:  makeIkey("g.SET.800"),
						},
						{
							smallest: makeIkey("x.SET.701"),
							largest:  makeIkey("y.SET.700"),
						},
					},
					2: []fileMetadata{
						{
							smallest: makeIkey("d.SET.601"),
							largest:  makeIkey("h.SET.600"),
						},
						{
							smallest: makeIkey("r.SET.501"),
							largest:  makeIkey("t.SET.500"),
						},
					},
					3: []fileMetadata{
						{
							smallest: makeIkey("f.SET.401"),
							largest:  makeIkey("g.SET.400"),
						},
						{
							smallest: makeIkey("w.SET.301"),
							largest:  makeIkey("x.SET.300"),
						},
					},
					4: []fileMetadata{
						{
							smallest: makeIkey("f.SET.201"),
							largest:  makeIkey("m.SET.200"),
						},
						{
							smallest: makeIkey("t.SET.101"),
							largest:  makeIkey("t.SET.100"),
						},
					},
				},
			},
			wants: map[string]bool{
				"b": true,
				"c": true,
				"d": true,
				"e": true,
				"f": false,
				"g": false,
				"h": false,
				"l": false,
				"m": false,
				"n": true,
				"q": true,
				"r": true,
				"s": true,
				"t": false,
				"u": true,
				"v": true,
				"w": false,
				"x": false,
				"y": true,
				"z": true,
			},
		},
		{
			desc:  "repeated ukey",
			level: 1,
			version: version{
				files: [numLevels][]fileMetadata{
					6: []fileMetadata{
						{
							smallest: makeIkey("i.SET.401"),
							largest:  makeIkey("i.SET.400"),
						},
						{
							smallest: makeIkey("i.SET.301"),
							largest:  makeIkey("k.SET.300"),
						},
						{
							smallest: makeIkey("k.SET.201"),
							largest:  makeIkey("m.SET.200"),
						},
						{
							smallest: makeIkey("m.SET.101"),
							largest:  makeIkey("m.SET.100"),
						},
					},
				},
			},
			wants: map[string]bool{
				"h": true,
				"i": false,
				"j": false,
				"k": false,
				"l": false,
				"m": false,
				"n": true,
			},
		},
	}

	for _, tc := range testCases {
		c := compaction{
			version: &tc.version,
			level:   tc.level,
		}
		for ukey, want := range tc.wants {
			if got := c.isBaseLevelForUkey(db.DefaultComparer, []byte(ukey)); got != want {
				t.Errorf("%s: ukey=%q: got %v, want %v", tc.desc, ukey, got, want)
			}
		}
	}
}

func TestCompaction(t *testing.T) {
	const writeBufferSize = 1000

	fs := memfs.New()
	d, err := Open("", &db.Options{
		FileSystem:      fs,
		WriteBufferSize: writeBufferSize,
	})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}

	get1 := func(x db.DB) (ret string) {
		b := &bytes.Buffer{}
		iter := x.Find(nil, nil)
		for iter.Next() {
			b.Write(internalKey(iter.Key()).ukey())
		}
		if err := iter.Close(); err != nil {
			t.Fatalf("iterator Close: %v", err)
		}
		return b.String()
	}
	getAll := func() (gotMem, gotDisk string, err error) {
		d.mu.Lock()
		defer d.mu.Unlock()

		if d.mem != nil {
			gotMem = get1(d.mem)
		}
		ss := []string(nil)
		v := d.versions.currentVersion()
		for _, files := range v.files {
			for _, meta := range files {
				f, err := fs.Open(dbFilename("", fileTypeTable, meta.fileNum))
				if err != nil {
					return "", "", fmt.Errorf("Open: %v", err)
				}
				defer f.Close()
				r := table.NewReader(f, &db.Options{
					Comparer: internalKeyComparer{db.DefaultComparer},
				})
				defer r.Close()
				ss = append(ss, get1(r)+".")
			}
		}
		sort.Strings(ss)
		return gotMem, strings.Join(ss, ""), nil
	}

	value := bytes.Repeat([]byte("x"), writeBufferSize*6/10)
	testCases := []struct {
		key, wantMem, wantDisk string
	}{
		{"+A", "A", ""},
		{"+a", "Aa", ""},
		{"+B", "B", "Aa."},
		{"+b", "Bb", "Aa."},
		// The next level-0 table overwrites the B key.
		{"+C", "C", "Aa.Bb."},
		{"+B", "BC", "Aa.Bb."},
		// The next level-0 table deletes the a key.
		{"+D", "D", "Aa.BC.Bb."},
		{"-a", "Da", "Aa.BC.Bb."},
		{"+d", "Dad", "Aa.BC.Bb."},
		// The next addition creates the fourth level-0 table, and l0CompactionTrigger == 4,
		// so this triggers a non-trivial compaction into one level-1 table. Note that the
		// keys in this one larger table are interleaved from the four smaller ones.
		{"+E", "E", "ABCDbd."},
		{"+e", "Ee", "ABCDbd."},
		{"+F", "F", "ABCDbd.Ee."},
	}
	for _, tc := range testCases {
		if key := tc.key[1:]; tc.key[0] == '+' {
			if err := d.Set([]byte(key), value, nil); err != nil {
				t.Errorf("%q: Set: %v", key, err)
				break
			}
		} else {
			if err := d.Delete([]byte(key), nil); err != nil {
				t.Errorf("%q: Delete: %v", key, err)
				break
			}
		}

		// Allow any writes to the memfs to complete.
		time.Sleep(1 * time.Millisecond)

		gotMem, gotDisk, err := getAll()
		if err != nil {
			t.Errorf("%q: %v", tc.key, err)
			break
		}
		if gotMem != tc.wantMem {
			t.Errorf("%q: mem: got %q, want %q", tc.key, gotMem, tc.wantMem)
		}
		if gotDisk != tc.wantDisk {
			t.Errorf("%q: ldb: got %q, want %q", tc.key, gotDisk, tc.wantDisk)
		}
	}

	if err := d.Close(); err != nil {
		t.Fatalf("db Close: %v", err)
	}
}
