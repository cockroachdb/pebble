// Copyright 2012 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package manifest

import (
	"fmt"
	"strconv"
	"strings"
	"sync"
	"testing"

	"github.com/cockroachdb/pebble/internal/base"
	"github.com/cockroachdb/pebble/internal/datadriven"
)

func ikey(s string) InternalKey {
	return base.MakeInternalKey([]byte(s), 0, base.InternalKeyKindSet)
}

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
		var f []*FileMetadata
		if tc.input != "" {
			for _, s := range strings.Split(tc.input, " ") {
				f = append(f, &FileMetadata{
					Smallest: ikey(s[0:1]),
					Largest:  ikey(s[2:3]),
				})
			}
		}

		smallest0, largest0 := KeyRange(base.DefaultComparer.Compare, f, nil)
		got0 := string(smallest0.UserKey) + "-" + string(largest0.UserKey)
		if got0 != tc.want {
			t.Errorf("first []fileMetadata is %v\ngot  %s\nwant %s", tc.input, got0, tc.want)
		}

		smallest1, largest1 := KeyRange(base.DefaultComparer.Compare, nil, f)
		got1 := string(smallest1.UserKey) + "-" + string(largest1.UserKey)
		if got1 != tc.want {
			t.Errorf("second []fileMetadata is %v\ngot  %s\nwant %s", tc.input, got1, tc.want)
		}
	}
}

func TestOverlaps(t *testing.T) {
	m00 := &FileMetadata{
		FileNum:  700,
		Size:     1,
		Smallest: base.ParseInternalKey("b.SET.7008"),
		Largest:  base.ParseInternalKey("e.SET.7009"),
	}
	m01 := &FileMetadata{
		FileNum:  701,
		Size:     1,
		Smallest: base.ParseInternalKey("c.SET.7018"),
		Largest:  base.ParseInternalKey("f.SET.7019"),
	}
	m02 := &FileMetadata{
		FileNum:  702,
		Size:     1,
		Smallest: base.ParseInternalKey("f.SET.7028"),
		Largest:  base.ParseInternalKey("g.SET.7029"),
	}
	m03 := &FileMetadata{
		FileNum:  703,
		Size:     1,
		Smallest: base.ParseInternalKey("x.SET.7038"),
		Largest:  base.ParseInternalKey("y.SET.7039"),
	}
	m04 := &FileMetadata{
		FileNum:  704,
		Size:     1,
		Smallest: base.ParseInternalKey("n.SET.7048"),
		Largest:  base.ParseInternalKey("p.SET.7049"),
	}
	m05 := &FileMetadata{
		FileNum:  705,
		Size:     1,
		Smallest: base.ParseInternalKey("p.SET.7058"),
		Largest:  base.ParseInternalKey("p.SET.7059"),
	}
	m06 := &FileMetadata{
		FileNum:  706,
		Size:     1,
		Smallest: base.ParseInternalKey("p.SET.7068"),
		Largest:  base.ParseInternalKey("u.SET.7069"),
	}
	m07 := &FileMetadata{
		FileNum:  707,
		Size:     1,
		Smallest: base.ParseInternalKey("r.SET.7078"),
		Largest:  base.ParseInternalKey("s.SET.7079"),
	}

	m10 := &FileMetadata{
		FileNum:  710,
		Size:     1,
		Smallest: base.ParseInternalKey("d.SET.7108"),
		Largest:  base.ParseInternalKey("g.SET.7109"),
	}
	m11 := &FileMetadata{
		FileNum:  711,
		Size:     1,
		Smallest: base.ParseInternalKey("g.SET.7118"),
		Largest:  base.ParseInternalKey("j.SET.7119"),
	}
	m12 := &FileMetadata{
		FileNum:  712,
		Size:     1,
		Smallest: base.ParseInternalKey("n.SET.7128"),
		Largest:  base.ParseInternalKey("p.SET.7129"),
	}
	m13 := &FileMetadata{
		FileNum:  713,
		Size:     1,
		Smallest: base.ParseInternalKey("p.SET.7138"),
		Largest:  base.ParseInternalKey("p.SET.7139"),
	}
	m14 := &FileMetadata{
		FileNum:  714,
		Size:     1,
		Smallest: base.ParseInternalKey("p.SET.7148"),
		Largest:  base.ParseInternalKey("u.SET.7149"),
	}

	v := Version{
		Files: [NumLevels][]*FileMetadata{
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

	cmp := base.DefaultComparer.Compare
	for _, tc := range testCases {
		o := v.Overlaps(tc.level, cmp, []byte(tc.ukey0), []byte(tc.ukey1))
		s := make([]string, len(o))
		for i, meta := range o {
			s[i] = fmt.Sprintf("m%02d", meta.FileNum%100)
		}
		got := strings.Join(s, " ")
		if got != tc.want {
			t.Errorf("level=%d, range=%s-%s\ngot  %v\nwant %v", tc.level, tc.ukey0, tc.ukey1, got, tc.want)
		}
	}
}

func TestVersionUnref(t *testing.T) {
	list := &VersionList{}
	list.Init(&sync.Mutex{})
	v := &Version{Deleted: func([]uint64) {}}
	v.Ref()
	list.PushBack(v)
	v.Unref()
	if !list.Empty() {
		t.Fatalf("expected version list to be empty")
	}
}

func TestCheckOrdering(t *testing.T) {
	parseMeta := func(s string) FileMetadata {
		parts := strings.Split(s, "-")
		if len(parts) != 2 {
			t.Fatalf("malformed table spec: %s", s)
		}
		m := FileMetadata{
			Smallest: base.ParseInternalKey(strings.TrimSpace(parts[0])),
			Largest:  base.ParseInternalKey(strings.TrimSpace(parts[1])),
		}
		m.SmallestSeqNum = m.Smallest.SeqNum()
		m.LargestSeqNum = m.Largest.SeqNum()
		return m
	}

	datadriven.RunTest(t, "testdata/version_check_ordering",
		func(d *datadriven.TestData) string {
			switch d.Cmd {
			case "check-ordering":
				// TODO(sumeer): move this Version parsing code to utils, to
				// avoid repeating it, and make it the inverse of
				// Version.DebugString().
				v := Version{}
				var files *[]*FileMetadata
				fileNum := uint64(1)

				for _, data := range strings.Split(d.Input, "\n") {
					switch data {
					case "L0", "L1", "L2", "L3", "L4", "L5", "L6":
						level, err := strconv.Atoi(data[1:])
						if err != nil {
							return err.Error()
						}
						files = &v.Files[level]

					default:
						meta := parseMeta(data)
						meta.FileNum = fileNum
						fileNum++
						*files = append(*files, &meta)
					}
				}

				cmp := base.DefaultComparer.Compare
				result := "OK"
				v.InitL0Sublevels(cmp)
				err := v.CheckOrdering(cmp, base.DefaultFormatter)
				if err != nil {
					result = fmt.Sprint(err)
				}
				return result

			default:
				return fmt.Sprintf("unknown command: %s", d.Cmd)
			}
		})
}
