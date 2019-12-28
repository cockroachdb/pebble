// Copyright 2013 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package pebble

import (
	"bytes"
	"fmt"
	"math"
	"regexp"
	"runtime"
	"sort"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/cockroachdb/pebble/internal/base"
	"github.com/cockroachdb/pebble/internal/datadriven"
	"github.com/cockroachdb/pebble/internal/manifest"
	"github.com/cockroachdb/pebble/sstable"
	"github.com/cockroachdb/pebble/vfs"
)

func TestPickCompaction(t *testing.T) {
	fileNums := func(f []fileMetadata) string {
		ss := make([]string, 0, len(f))
		for _, meta := range f {
			ss = append(ss, strconv.Itoa(int(meta.FileNum)))
		}
		sort.Strings(ss)
		return strings.Join(ss, ",")
	}

	opts := (*Options)(nil).EnsureDefaults()
	testCases := []struct {
		desc    string
		version version
		picker  compactionPicker
		want    string
	}{
		{
			desc: "no compaction",
			version: version{
				Files: [numLevels][]fileMetadata{
					0: []fileMetadata{
						{
							FileNum:  100,
							Size:     1,
							Smallest: base.ParseInternalKey("i.SET.101"),
							Largest:  base.ParseInternalKey("j.SET.102"),
						},
					},
				},
			},
			want: "",
		},

		{
			desc: "1 L0 file",
			version: version{
				Files: [numLevels][]fileMetadata{
					0: []fileMetadata{
						{
							FileNum:  100,
							Size:     1,
							Smallest: base.ParseInternalKey("i.SET.101"),
							Largest:  base.ParseInternalKey("j.SET.102"),
						},
					},
				},
			},
			picker: compactionPicker{
				score:     99,
				level:     0,
				baseLevel: 1,
			},
			want: "100  ",
		},

		{
			desc: "2 L0 files (0 overlaps)",
			version: version{
				Files: [numLevels][]fileMetadata{
					0: []fileMetadata{
						{
							FileNum:  100,
							Size:     1,
							Smallest: base.ParseInternalKey("i.SET.101"),
							Largest:  base.ParseInternalKey("j.SET.102"),
						},
						{
							FileNum:  110,
							Size:     1,
							Smallest: base.ParseInternalKey("k.SET.111"),
							Largest:  base.ParseInternalKey("l.SET.112"),
						},
					},
				},
			},
			picker: compactionPicker{
				score:     99,
				level:     0,
				baseLevel: 1,
			},
			want: "100  ",
		},

		{
			desc: "2 L0 files, with ikey overlap",
			version: version{
				Files: [numLevels][]fileMetadata{
					0: []fileMetadata{
						{
							FileNum:  100,
							Size:     1,
							Smallest: base.ParseInternalKey("i.SET.101"),
							Largest:  base.ParseInternalKey("p.SET.102"),
						},
						{
							FileNum:  110,
							Size:     1,
							Smallest: base.ParseInternalKey("j.SET.111"),
							Largest:  base.ParseInternalKey("q.SET.112"),
						},
					},
				},
			},
			picker: compactionPicker{
				score:     99,
				level:     0,
				baseLevel: 1,
			},
			want: "100,110  ",
		},

		{
			desc: "2 L0 files, with ukey overlap",
			version: version{
				Files: [numLevels][]fileMetadata{
					0: []fileMetadata{
						{
							FileNum:  100,
							Size:     1,
							Smallest: base.ParseInternalKey("i.SET.101"),
							Largest:  base.ParseInternalKey("i.SET.102"),
						},
						{
							FileNum:  110,
							Size:     1,
							Smallest: base.ParseInternalKey("i.SET.111"),
							Largest:  base.ParseInternalKey("i.SET.112"),
						},
					},
				},
			},
			picker: compactionPicker{
				score:     99,
				level:     0,
				baseLevel: 1,
			},
			want: "100,110  ",
		},

		{
			desc: "1 L0 file, 2 L1 files (0 overlaps)",
			version: version{
				Files: [numLevels][]fileMetadata{
					0: []fileMetadata{
						{
							FileNum:  100,
							Size:     1,
							Smallest: base.ParseInternalKey("i.SET.101"),
							Largest:  base.ParseInternalKey("i.SET.102"),
						},
					},
					1: []fileMetadata{
						{
							FileNum:  200,
							Size:     1,
							Smallest: base.ParseInternalKey("a.SET.201"),
							Largest:  base.ParseInternalKey("b.SET.202"),
						},
						{
							FileNum:  210,
							Size:     1,
							Smallest: base.ParseInternalKey("y.SET.211"),
							Largest:  base.ParseInternalKey("z.SET.212"),
						},
					},
				},
			},
			picker: compactionPicker{
				score:     99,
				level:     0,
				baseLevel: 1,
			},
			want: "100  ",
		},

		{
			desc: "1 L0 file, 2 L1 files (1 overlap), 4 L2 files (3 overlaps)",
			version: version{
				Files: [numLevels][]fileMetadata{
					0: []fileMetadata{
						{
							FileNum:  100,
							Size:     1,
							Smallest: base.ParseInternalKey("i.SET.101"),
							Largest:  base.ParseInternalKey("t.SET.102"),
						},
					},
					1: []fileMetadata{
						{
							FileNum:  200,
							Size:     1,
							Smallest: base.ParseInternalKey("a.SET.201"),
							Largest:  base.ParseInternalKey("e.SET.202"),
						},
						{
							FileNum:  210,
							Size:     1,
							Smallest: base.ParseInternalKey("f.SET.211"),
							Largest:  base.ParseInternalKey("j.SET.212"),
						},
					},
					2: []fileMetadata{
						{
							FileNum:  300,
							Size:     1,
							Smallest: base.ParseInternalKey("a.SET.301"),
							Largest:  base.ParseInternalKey("b.SET.302"),
						},
						{
							FileNum:  310,
							Size:     1,
							Smallest: base.ParseInternalKey("c.SET.311"),
							Largest:  base.ParseInternalKey("g.SET.312"),
						},
						{
							FileNum:  320,
							Size:     1,
							Smallest: base.ParseInternalKey("h.SET.321"),
							Largest:  base.ParseInternalKey("m.SET.322"),
						},
						{
							FileNum:  330,
							Size:     1,
							Smallest: base.ParseInternalKey("n.SET.331"),
							Largest:  base.ParseInternalKey("z.SET.332"),
						},
					},
				},
			},
			picker: compactionPicker{
				score:     99,
				level:     0,
				baseLevel: 1,
			},
			want: "100 210 310,320,330",
		},

		{
			desc: "4 L1 files, 2 L2 files, can grow",
			version: version{
				Files: [numLevels][]fileMetadata{
					1: []fileMetadata{
						{
							FileNum:  200,
							Size:     1,
							Smallest: base.ParseInternalKey("i1.SET.201"),
							Largest:  base.ParseInternalKey("i2.SET.202"),
						},
						{
							FileNum:  210,
							Size:     1,
							Smallest: base.ParseInternalKey("j1.SET.211"),
							Largest:  base.ParseInternalKey("j2.SET.212"),
						},
						{
							FileNum:  220,
							Size:     1,
							Smallest: base.ParseInternalKey("k1.SET.221"),
							Largest:  base.ParseInternalKey("k2.SET.222"),
						},
						{
							FileNum:  230,
							Size:     1,
							Smallest: base.ParseInternalKey("l1.SET.231"),
							Largest:  base.ParseInternalKey("l2.SET.232"),
						},
					},
					2: []fileMetadata{
						{
							FileNum:  300,
							Size:     1,
							Smallest: base.ParseInternalKey("a0.SET.301"),
							Largest:  base.ParseInternalKey("l0.SET.302"),
						},
						{
							FileNum:  310,
							Size:     1,
							Smallest: base.ParseInternalKey("l2.SET.311"),
							Largest:  base.ParseInternalKey("z2.SET.312"),
						},
					},
				},
			},
			picker: compactionPicker{
				score:     99,
				level:     1,
				baseLevel: 1,
			},
			want: "200,210,220 300 ",
		},

		{
			desc: "4 L1 files, 2 L2 files, can't grow (range)",
			version: version{
				Files: [numLevels][]fileMetadata{
					1: []fileMetadata{
						{
							FileNum:  200,
							Size:     1,
							Smallest: base.ParseInternalKey("i1.SET.201"),
							Largest:  base.ParseInternalKey("i2.SET.202"),
						},
						{
							FileNum:  210,
							Size:     1,
							Smallest: base.ParseInternalKey("j1.SET.211"),
							Largest:  base.ParseInternalKey("j2.SET.212"),
						},
						{
							FileNum:  220,
							Size:     1,
							Smallest: base.ParseInternalKey("k1.SET.221"),
							Largest:  base.ParseInternalKey("k2.SET.222"),
						},
						{
							FileNum:  230,
							Size:     1,
							Smallest: base.ParseInternalKey("l1.SET.231"),
							Largest:  base.ParseInternalKey("l2.SET.232"),
						},
					},
					2: []fileMetadata{
						{
							FileNum:  300,
							Size:     1,
							Smallest: base.ParseInternalKey("a0.SET.301"),
							Largest:  base.ParseInternalKey("j0.SET.302"),
						},
						{
							FileNum:  310,
							Size:     1,
							Smallest: base.ParseInternalKey("j2.SET.311"),
							Largest:  base.ParseInternalKey("z2.SET.312"),
						},
					},
				},
			},
			picker: compactionPicker{
				score:     99,
				level:     1,
				baseLevel: 1,
			},
			want: "200 300 ",
		},

		{
			desc: "4 L1 files, 2 L2 files, can't grow (size)",
			version: version{
				Files: [numLevels][]fileMetadata{
					1: []fileMetadata{
						{
							FileNum:  200,
							Size:     expandedCompactionByteSizeLimit(opts, 1) - 1,
							Smallest: base.ParseInternalKey("i1.SET.201"),
							Largest:  base.ParseInternalKey("i2.SET.202"),
						},
						{
							FileNum:  210,
							Size:     expandedCompactionByteSizeLimit(opts, 1) - 1,
							Smallest: base.ParseInternalKey("j1.SET.211"),
							Largest:  base.ParseInternalKey("j2.SET.212"),
						},
						{
							FileNum:  220,
							Size:     expandedCompactionByteSizeLimit(opts, 1) - 1,
							Smallest: base.ParseInternalKey("k1.SET.221"),
							Largest:  base.ParseInternalKey("k2.SET.222"),
						},
						{
							FileNum:  230,
							Size:     expandedCompactionByteSizeLimit(opts, 1) - 1,
							Smallest: base.ParseInternalKey("l1.SET.231"),
							Largest:  base.ParseInternalKey("l2.SET.232"),
						},
					},
					2: []fileMetadata{
						{
							FileNum:  300,
							Size:     expandedCompactionByteSizeLimit(opts, 2) - 1,
							Smallest: base.ParseInternalKey("a0.SET.301"),
							Largest:  base.ParseInternalKey("l0.SET.302"),
						},
						{
							FileNum:  310,
							Size:     expandedCompactionByteSizeLimit(opts, 2) - 1,
							Smallest: base.ParseInternalKey("l2.SET.311"),
							Largest:  base.ParseInternalKey("z2.SET.312"),
						},
					},
				},
			},
			picker: compactionPicker{
				score:     99,
				level:     1,
				baseLevel: 1,
			},
			want: "200 300 ",
		},
	}

	for _, tc := range testCases {
		vs := &versionSet{
			opts:    opts,
			cmp:     DefaultComparer.Compare,
			cmpName: DefaultComparer.Name,
		}
		vs.versions.Init(nil)
		vs.append(&tc.version)
		vs.picker = &tc.picker
		vs.picker.vers = &tc.version

		c, got := vs.picker.pickAuto(opts, new(uint64)), ""
		if c != nil {
			got0 := fileNums(c.inputs[0])
			got1 := fileNums(c.inputs[1])
			got2 := fileNums(c.grandparents)
			got = got0 + " " + got1 + " " + got2
		}
		if got != tc.want {
			t.Fatalf("%s:\ngot  %q\nwant %q", tc.desc, got, tc.want)
		}
	}
}

func TestElideTombstone(t *testing.T) {
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
				Files: [numLevels][]fileMetadata{
					1: []fileMetadata{
						{
							Smallest: base.ParseInternalKey("c.SET.801"),
							Largest:  base.ParseInternalKey("g.SET.800"),
						},
						{
							Smallest: base.ParseInternalKey("x.SET.701"),
							Largest:  base.ParseInternalKey("y.SET.700"),
						},
					},
					2: []fileMetadata{
						{
							Smallest: base.ParseInternalKey("d.SET.601"),
							Largest:  base.ParseInternalKey("h.SET.600"),
						},
						{
							Smallest: base.ParseInternalKey("r.SET.501"),
							Largest:  base.ParseInternalKey("t.SET.500"),
						},
					},
					3: []fileMetadata{
						{
							Smallest: base.ParseInternalKey("f.SET.401"),
							Largest:  base.ParseInternalKey("g.SET.400"),
						},
						{
							Smallest: base.ParseInternalKey("w.SET.301"),
							Largest:  base.ParseInternalKey("x.SET.300"),
						},
					},
					4: []fileMetadata{
						{
							Smallest: base.ParseInternalKey("f.SET.201"),
							Largest:  base.ParseInternalKey("m.SET.200"),
						},
						{
							Smallest: base.ParseInternalKey("t.SET.101"),
							Largest:  base.ParseInternalKey("t.SET.100"),
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
				Files: [numLevels][]fileMetadata{
					6: []fileMetadata{
						{
							Smallest: base.ParseInternalKey("i.SET.401"),
							Largest:  base.ParseInternalKey("i.SET.400"),
						},
						{
							Smallest: base.ParseInternalKey("i.SET.301"),
							Largest:  base.ParseInternalKey("k.SET.300"),
						},
						{
							Smallest: base.ParseInternalKey("k.SET.201"),
							Largest:  base.ParseInternalKey("m.SET.200"),
						},
						{
							Smallest: base.ParseInternalKey("m.SET.101"),
							Largest:  base.ParseInternalKey("m.SET.100"),
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
			cmp:         DefaultComparer.Compare,
			version:     &tc.version,
			startLevel:  tc.level,
			outputLevel: tc.level + 1,
		}
		for ukey, want := range tc.wants {
			if got := c.elideTombstone([]byte(ukey)); got != want {
				t.Errorf("%s: ukey=%q: got %v, want %v", tc.desc, ukey, got, want)
			}
		}
	}
}

func TestCompaction(t *testing.T) {
	const memTableSize = 10000
	// Tuned so that 2 values can reside in the memtable before a flush, but a
	// 3rd value will cause a flush. Needs to account for the max skiplist node
	// size.
	const valueSize = 3500

	mem := vfs.NewMem()
	d, err := Open("", &Options{
		FS:           mem,
		MemTableSize: memTableSize,
		DebugCheck:   true,
		enablePacing: true,
	})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	mockLimiter := mockCountLimiter{burst: int(math.MaxInt32)}
	d.compactionLimiter = &mockLimiter

	get1 := func(iter internalIterator) (ret string) {
		b := &bytes.Buffer{}
		for key, _ := iter.First(); key != nil; key, _ = iter.Next() {
			b.Write(key.UserKey)
		}
		if err := iter.Close(); err != nil {
			t.Fatalf("iterator Close: %v", err)
		}
		return b.String()
	}
	getAll := func() (gotMem, gotDisk string, err error) {
		d.mu.Lock()
		defer d.mu.Unlock()

		if d.mu.mem.mutable != nil {
			gotMem = get1(d.mu.mem.mutable.newIter(nil))
		}
		ss := []string(nil)
		v := d.mu.versions.currentVersion()
		for _, files := range v.Files {
			for _, meta := range files {
				f, err := mem.Open(base.MakeFilename(mem, "", fileTypeTable, meta.FileNum))
				if err != nil {
					return "", "", fmt.Errorf("Open: %v", err)
				}
				defer f.Close()
				r, err := sstable.NewReader(f, sstable.ReaderOptions{})
				if err != nil {
					return "", "", fmt.Errorf("NewReader: %v", err)
				}
				defer r.Close()
				ss = append(ss, get1(r.NewIter(nil /* lower */, nil /* upper */))+".")
			}
		}
		sort.Strings(ss)
		return gotMem, strings.Join(ss, ""), nil
	}

	value := bytes.Repeat([]byte("x"), valueSize)
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

		// try backs off to allow any writes to the memfs to complete.
		err := try(100*time.Microsecond, 20*time.Second, func() error {
			gotMem, gotDisk, err := getAll()
			if err != nil {
				return err
			}
			if testing.Verbose() {
				fmt.Printf("mem=%s (%s) disk=%s (%s)\n", gotMem, tc.wantMem, gotDisk, tc.wantDisk)
			}

			if gotMem != tc.wantMem {
				return fmt.Errorf("mem: got %q, want %q", gotMem, tc.wantMem)
			}
			if gotDisk != tc.wantDisk {
				return fmt.Errorf("ldb: got %q, want %q", gotDisk, tc.wantDisk)
			}
			return nil
		})
		if err != nil {
			t.Errorf("%q: %v", tc.key, err)
		}
	}

	if err := d.Close(); err != nil {
		t.Fatalf("db Close: %v", err)
	}

	if !(mockLimiter.allowCount > 0) {
		t.Errorf("limiter allow: got %d, want >%d", mockLimiter.allowCount, 0)
	}
	if mockLimiter.waitCount != 0 {
		t.Errorf("limiter wait: got %d, want %d", mockLimiter.waitCount, 0)
	}
}

func TestManualCompaction(t *testing.T) {
	mem := vfs.NewMem()
	err := mem.MkdirAll("ext", 0755)
	if err != nil {
		t.Fatal(err)
	}

	d, err := Open("", &Options{
		FS:         mem,
		DebugCheck: true,
	})
	if err != nil {
		t.Fatal(err)
	}

	datadriven.RunTest(t, "testdata/manual_compaction", func(td *datadriven.TestData) string {
		switch td.Cmd {
		case "batch":
			b := d.NewIndexedBatch()
			if err := runBatchDefineCmd(td, b); err != nil {
				return err.Error()
			}
			b.Commit(nil)
			return ""

		case "define":
			var err error
			if d, err = runDBDefineCmd(td, nil /* options */); err != nil {
				return err.Error()
			}

			d.mu.Lock()
			s := d.mu.versions.currentVersion().String()
			d.mu.Unlock()
			return s

		case "iter":
			iter := d.NewIter(nil)
			defer iter.Close()
			var b bytes.Buffer
			for _, line := range strings.Split(td.Input, "\n") {
				parts := strings.Fields(line)
				if len(parts) == 0 {
					continue
				}
				switch parts[0] {
				case "seek-ge":
					if len(parts) != 2 {
						return fmt.Sprintf("seek-ge <key>\n")
					}
					iter.SeekGE([]byte(strings.TrimSpace(parts[1])))
				case "seek-lt":
					if len(parts) != 2 {
						return fmt.Sprintf("seek-lt <key>\n")
					}
					iter.SeekLT([]byte(strings.TrimSpace(parts[1])))
				case "next":
					iter.Next()
				case "prev":
					iter.Prev()
				default:
					return fmt.Sprintf("unknown op: %s", parts[0])
				}
				if iter.Valid() {
					fmt.Fprintf(&b, "%s:%s\n", iter.Key(), iter.Value())
				} else if err := iter.Error(); err != nil {
					fmt.Fprintf(&b, "err=%v\n", err)
				} else {
					fmt.Fprintf(&b, ".\n")
				}
			}
			return b.String()

		case "compact":
			if err := runCompactCmd(td, d); err != nil {
				return err.Error()
			}

			d.mu.Lock()
			s := d.mu.versions.currentVersion().DebugString(base.DefaultFormatter)
			d.mu.Unlock()
			return s

		default:
			return fmt.Sprintf("unknown command: %s", td.Cmd)
		}
	})
}

func TestCompactionFindGrandparentLimit(t *testing.T) {
	cmp := DefaultComparer.Compare
	var grandparents []fileMetadata

	parseMeta := func(s string) fileMetadata {
		parts := strings.Split(s, "-")
		if len(parts) != 2 {
			t.Fatalf("malformed table spec: %s", s)
		}
		return fileMetadata{
			Smallest: InternalKey{UserKey: []byte(parts[0])},
			Largest:  InternalKey{UserKey: []byte(parts[1])},
		}
	}

	datadriven.RunTest(t, "testdata/compaction_find_grandparent_limit",
		func(d *datadriven.TestData) string {
			switch d.Cmd {
			case "define":
				grandparents = nil
				if len(d.Input) == 0 {
					return ""
				}
				for _, data := range strings.Split(d.Input, "\n") {
					parts := strings.Fields(data)
					if len(parts) != 2 {
						return fmt.Sprintf("malformed test:\n%s", d.Input)
					}

					meta := parseMeta(parts[0])
					var err error
					meta.Size, err = strconv.ParseUint(parts[1], 10, 64)
					if err != nil {
						return err.Error()
					}
					grandparents = append(grandparents, meta)
				}
				manifest.SortBySmallest(grandparents, cmp)
				return ""

			case "compact":
				c := &compaction{
					cmp:          cmp,
					grandparents: grandparents,
				}
				if len(d.CmdArgs) != 1 {
					return fmt.Sprintf("%s expects 1 argument", d.Cmd)
				}
				if len(d.CmdArgs[0].Vals) != 1 {
					return fmt.Sprintf("%s expects 1 value", d.CmdArgs[0].Key)
				}
				var err error
				c.maxOverlapBytes, err = strconv.ParseUint(d.CmdArgs[0].Vals[0], 10, 64)
				if err != nil {
					return err.Error()
				}

				var buf bytes.Buffer
				var smallest, largest string
				var grandparentLimit []byte
				for i, key := range strings.Fields(d.Input) {
					if i == 0 {
						smallest = key
						grandparentLimit = c.findGrandparentLimit([]byte(key))
					}
					if grandparentLimit != nil && c.cmp(grandparentLimit, []byte(key)) < 0 {
						fmt.Fprintf(&buf, "%s-%s\n", smallest, largest)
						smallest = key
						grandparentLimit = c.findGrandparentLimit([]byte(key))
					}
					largest = key
				}
				fmt.Fprintf(&buf, "%s-%s\n", smallest, largest)
				return buf.String()

			default:
				return fmt.Sprintf("unknown command: %s", d.Cmd)
			}
		})
}

func TestCompactionOutputLevel(t *testing.T) {
	opts := (*Options)(nil).EnsureDefaults()
	version := &version{}

	datadriven.RunTest(t, "testdata/compaction_output_level",
		func(d *datadriven.TestData) (res string) {
			defer func() {
				if r := recover(); r != nil {
					res = fmt.Sprintln(r)
				}
			}()

			switch d.Cmd {
			case "compact":
				var start, base int
				d.ScanArgs(t, "start", &start)
				d.ScanArgs(t, "base", &base)
				c := newCompaction(opts, version, start, base, new(uint64))
				return fmt.Sprintf("output=%d\nmax-output-file-size=%d\n",
					c.outputLevel, c.maxOutputFileSize)

			default:
				return fmt.Sprintf("unknown command: %s", d.Cmd)
			}
		})
}

func TestCompactionSetupInputs(t *testing.T) {
	parseMeta := func(s string) fileMetadata {
		parts := strings.Split(s, "-")
		if len(parts) != 2 {
			t.Fatalf("malformed table spec: %s", s)
		}
		m := fileMetadata{
			Smallest: base.ParseInternalKey(strings.TrimSpace(parts[0])),
			Largest:  base.ParseInternalKey(strings.TrimSpace(parts[1])),
		}
		m.SmallestSeqNum = m.Smallest.SeqNum()
		m.LargestSeqNum = m.Largest.SeqNum()
		return m
	}

	datadriven.RunTest(t, "testdata/compaction_setup_inputs",
		func(d *datadriven.TestData) string {
			switch d.Cmd {
			case "setup-inputs":
				if len(d.CmdArgs) != 2 {
					return fmt.Sprintf("setup-inputs <start> <end>")
				}

				c := &compaction{
					cmp:              DefaultComparer.Compare,
					format:           DefaultComparer.Format,
					version:          &version{},
					startLevel:       -1,
					outputLevel:      -1,
					maxExpandedBytes: 1 << 30,
				}
				var files *[]fileMetadata
				fileNum := uint64(1)

				for _, data := range strings.Split(d.Input, "\n") {
					switch data {
					case "L0", "L1", "L2", "L3", "L4", "L5", "L6":
						level, err := strconv.Atoi(data[1:])
						if err != nil {
							return err.Error()
						}
						if c.startLevel == -1 {
							c.startLevel = level
							files = &c.version.Files[level]
						} else if c.outputLevel == -1 {
							if c.startLevel >= level {
								return fmt.Sprintf("startLevel=%d >= outputLevel=%d\n", c.startLevel, level)
							}
							c.outputLevel = level
							files = &c.version.Files[level]
						} else {
							return fmt.Sprintf("outputLevel already set\n")
						}

					default:
						meta := parseMeta(data)
						meta.FileNum = fileNum
						fileNum++
						*files = append(*files, meta)
					}
				}

				if c.outputLevel == -1 {
					c.outputLevel = c.startLevel + 1
				}
				c.inputs[0] = c.version.Overlaps(c.startLevel, c.cmp,
					[]byte(d.CmdArgs[0].String()), []byte(d.CmdArgs[1].String()))

				c.setupInputs()

				var buf bytes.Buffer
				for i := range c.inputs {
					files := c.inputs[i]
					if len(files) == 0 {
						continue
					}

					switch i {
					case 0:
						fmt.Fprintf(&buf, "L%d\n", c.startLevel)
					case 1:
						fmt.Fprintf(&buf, "L%d\n", c.outputLevel)
					}

					for j := range files {
						fmt.Fprintf(&buf, "  %s\n", files[j])
					}
				}
				return buf.String()

			default:
				return fmt.Sprintf("unknown command: %s", d.Cmd)
			}
		})
}

func TestCompactionExpandInputs(t *testing.T) {
	cmp := DefaultComparer.Compare
	var files []fileMetadata

	parseMeta := func(s string) fileMetadata {
		parts := strings.Split(s, "-")
		if len(parts) != 2 {
			t.Fatalf("malformed table spec: %s", s)
		}
		return fileMetadata{
			Smallest: base.ParseInternalKey(parts[0]),
			Largest:  base.ParseInternalKey(parts[1]),
		}
	}

	datadriven.RunTest(t, "testdata/compaction_expand_inputs",
		func(d *datadriven.TestData) string {
			switch d.Cmd {
			case "define":
				files = nil
				if len(d.Input) == 0 {
					return ""
				}
				for _, data := range strings.Split(d.Input, "\n") {
					meta := parseMeta(data)
					meta.FileNum = uint64(len(files))
					files = append(files, meta)
				}
				manifest.SortBySmallest(files, cmp)
				return ""

			case "expand-inputs":
				c := &compaction{
					cmp:        cmp,
					version:    &version{},
					startLevel: 1,
				}
				c.version.Files[c.startLevel] = files
				if len(d.CmdArgs) != 1 {
					return fmt.Sprintf("%s expects 1 argument", d.Cmd)
				}
				index, err := strconv.ParseInt(d.CmdArgs[0].String(), 10, 64)
				if err != nil {
					return err.Error()
				}

				inputs := c.expandInputs(c.startLevel, files[index:index+1])

				var buf bytes.Buffer
				for i := range inputs {
					f := &inputs[i]
					fmt.Fprintf(&buf, "%d: %s-%s\n", f.FileNum, f.Smallest, f.Largest)
				}
				return buf.String()

			default:
				return fmt.Sprintf("unknown command: %s", d.Cmd)
			}
		})
}

func TestCompactionAtomicUnitBounds(t *testing.T) {
	cmp := DefaultComparer.Compare
	var files []fileMetadata

	parseMeta := func(s string) fileMetadata {
		parts := strings.Split(s, "-")
		if len(parts) != 2 {
			t.Fatalf("malformed table spec: %s", s)
		}
		return fileMetadata{
			Smallest: base.ParseInternalKey(parts[0]),
			Largest:  base.ParseInternalKey(parts[1]),
		}
	}

	datadriven.RunTest(t, "testdata/compaction_atomic_unit_bounds",
		func(d *datadriven.TestData) string {
			switch d.Cmd {
			case "define":
				files = nil
				if len(d.Input) == 0 {
					return ""
				}
				for _, data := range strings.Split(d.Input, "\n") {
					meta := parseMeta(data)
					meta.FileNum = uint64(len(files))
					files = append(files, meta)
				}
				manifest.SortBySmallest(files, cmp)
				return ""

			case "atomic-unit-bounds":
				c := &compaction{
					cmp: cmp,
				}
				c.inputs[0] = files
				if len(d.CmdArgs) != 1 {
					return fmt.Sprintf("%s expects 1 argument", d.Cmd)
				}
				index, err := strconv.ParseInt(d.CmdArgs[0].String(), 10, 64)
				if err != nil {
					return err.Error()
				}

				lower, upper := c.atomicUnitBounds(&files[index])
				return fmt.Sprintf("%s-%s\n", lower, upper)

			default:
				return fmt.Sprintf("unknown command: %s", d.Cmd)
			}
		})
}

func TestCompactionAllowZeroSeqNum(t *testing.T) {
	var d *DB

	metaRE := regexp.MustCompile(`^L([0-9]+):([^-]+)-(.+)$`)
	parseMeta := func(s string) (level int, meta fileMetadata) {
		match := metaRE.FindStringSubmatch(s)
		if match == nil {
			t.Fatalf("malformed table spec: %s", s)
		}
		level, err := strconv.Atoi(match[1])
		if err != nil {
			t.Fatalf("malformed table spec: %s: %s", s, err)
		}
		meta = fileMetadata{
			Smallest: InternalKey{UserKey: []byte(match[2])},
			Largest:  InternalKey{UserKey: []byte(match[3])},
		}
		return level, meta
	}

	datadriven.RunTest(t, "testdata/compaction_allow_zero_seqnum",
		func(td *datadriven.TestData) string {
			switch td.Cmd {
			case "define":
				var err error
				if d, err = runDBDefineCmd(td, nil /* options */); err != nil {
					return err.Error()
				}

				d.mu.Lock()
				s := d.mu.versions.currentVersion().String()
				d.mu.Unlock()
				return s

			case "allow-zero-seqnum":
				d.mu.Lock()
				c := &compaction{
					cmp:     d.cmp,
					version: d.mu.versions.currentVersion(),
				}
				d.mu.Unlock()

				var buf bytes.Buffer
				for _, line := range strings.Split(td.Input, "\n") {
					parts := strings.Fields(line)
					if len(parts) == 0 {
						continue
					}
					c.flushing = nil
					c.inputs[0] = nil
					c.inputs[1] = nil
					c.startLevel = -1

					var iter internalIterator

					switch {
					case len(parts) == 1 && parts[0] == "flush":
						c.outputLevel = 0
						d.mu.Lock()
						c.flushing = d.mu.mem.queue
						d.mu.Unlock()

						var err error
						if iter, err = c.newInputIter(nil); err != nil {
							return err.Error()
						}
					default:
						for _, p := range parts {
							level, meta := parseMeta(p)
							i := 0
							switch {
							case c.startLevel == -1:
								c.startLevel = level
							case c.startLevel+1 == level:
								i = 1
							case c.startLevel != level:
								return fmt.Sprintf("invalid level %d: expected %d or %d",
									level, c.startLevel, c.startLevel+1)
							}
							c.inputs[i] = append(c.inputs[i], meta)
						}
						c.outputLevel = c.startLevel + 1
					}

					fmt.Fprintf(&buf, "%t\n", c.allowZeroSeqNum(iter))
				}
				return buf.String()

			default:
				return fmt.Sprintf("unknown command: %s", td.Cmd)
			}
		})
}

func TestCompactionCheckOrdering(t *testing.T) {
	parseMeta := func(s string) fileMetadata {
		parts := strings.Split(s, "-")
		if len(parts) != 2 {
			t.Fatalf("malformed table spec: %s", s)
		}
		m := fileMetadata{
			Smallest: base.ParseInternalKey(strings.TrimSpace(parts[0])),
			Largest:  base.ParseInternalKey(strings.TrimSpace(parts[1])),
		}
		m.SmallestSeqNum = m.Smallest.SeqNum()
		m.LargestSeqNum = m.Largest.SeqNum()
		return m
	}

	datadriven.RunTest(t, "testdata/compaction_check_ordering",
		func(d *datadriven.TestData) string {
			switch d.Cmd {
			case "check-ordering":
				c := &compaction{
					cmp:         DefaultComparer.Compare,
					format:      DefaultComparer.Format,
					logger:      panicLogger{},
					startLevel:  -1,
					outputLevel: -1,
				}
				var files *[]fileMetadata
				fileNum := uint64(1)

				for _, data := range strings.Split(d.Input, "\n") {
					switch data {
					case "L0", "L1", "L2", "L3", "L4", "L5", "L6":
						level, err := strconv.Atoi(data[1:])
						if err != nil {
							return err.Error()
						}
						if c.startLevel == -1 {
							c.startLevel = level
							files = &c.inputs[0]
						} else if c.outputLevel == -1 {
							if c.startLevel >= level {
								return fmt.Sprintf("startLevel=%d >= outputLevel=%d\n", c.startLevel, level)
							}
							c.outputLevel = level
							files = &c.inputs[1]
						} else {
							return fmt.Sprintf("outputLevel already set\n")
						}

					default:
						meta := parseMeta(data)
						meta.FileNum = fileNum
						fileNum++
						*files = append(*files, meta)
					}
				}

				// Note that we configure a panicLogger to be used when a fatal error
				// is logged. If a panic occurs, we catch the value and transform it
				// back into a string stored in result.
				result := "OK"
				func() {
					defer func() {
						if r := recover(); r != nil {
							result = fmt.Sprint(r)
						}
					}()

					newIters := func(
						_ *fileMetadata, _ *IterOptions, _ *uint64,
					) (internalIterator, internalIterator, error) {
						return &errorIter{}, nil, nil
					}
					_, _ = c.newInputIter(newIters)
				}()
				return result

			default:
				return fmt.Sprintf("unknown command: %s", d.Cmd)
			}
		})
}

func TestFlushInvariant(t *testing.T) {
	for _, disableWAL := range []bool{false, true} {
		t.Run(fmt.Sprintf("disableWAL=%t", disableWAL), func(t *testing.T) {
			for i := 0; i < 2; i++ {
				t.Run("", func(t *testing.T) {
					errCh := make(chan error)
					defer close(errCh)
					d, err := Open("", &Options{
						DisableWAL: disableWAL,
						FS:         vfs.NewMem(),
						EventListener: EventListener{
							BackgroundError: func(err error) {
								errCh <- err
								runtime.Goexit() // ensure we don't try to reschedule the flush
							},
						},
						DebugCheck: true,
					})
					if err != nil {
						t.Fatal(err)
					}
					if err := d.Set([]byte("hello"), nil, NoSync); err != nil {
						t.Fatal(err)
					}

					// Contort the DB into a state where it does something invalid.
					d.mu.Lock()
					switch i {
					case 0:
						// Force the next log number to be 0.
						d.mu.versions.nextFileNum = 0
					case 1:
						// Force the flushing memtable to have a log number equal to the new
						// log's number.
						d.mu.mem.mutable.logNum = d.mu.versions.nextFileNum
					}
					d.mu.Unlock()

					flushCh, err := d.AsyncFlush()
					if err != nil {
						t.Fatal(err)
					}
					select {
					case err := <-errCh:
						if disableWAL {
							t.Fatalf("expected success, but found %v", err)
						} else if errFlushInvariant != err {
							t.Fatalf("expected %q, but found %v", errFlushInvariant, err)
						}
					case <-flushCh:
						if !disableWAL {
							t.Fatalf("expected error but found success")
						}
					}
				})
			}
		})
	}
}
