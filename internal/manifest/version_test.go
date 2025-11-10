// Copyright 2012 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package manifest

import (
	"bytes"
	"fmt"
	"math/rand/v2"
	"reflect"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/cockroachdb/datadriven"
	"github.com/cockroachdb/pebble/internal/base"
	"github.com/cockroachdb/pebble/internal/testkeys"
	"github.com/stretchr/testify/require"
)

func levelMetadata(level int, files ...*TableMetadata) LevelMetadata {
	return MakeLevelMetadata(base.DefaultComparer.Compare, level, files)
}

func ikey(s string) InternalKey {
	return base.MakeInternalKey([]byte(s), 0, base.InternalKeyKindSet)
}

func TestIkeyRange(t *testing.T) {
	cmp := base.DefaultComparer.Compare
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
		var f []*TableMetadata
		if tc.input != "" {
			for i, s := range strings.Split(tc.input, " ") {
				m := (&TableMetadata{
					TableNum: base.FileNum(i),
				}).ExtendPointKeyBounds(cmp, ikey(s[0:1]), ikey(s[2:3]))
				m.InitPhysicalBacking()
				f = append(f, m)
			}
		}
		levelMetadata := MakeLevelMetadata(base.DefaultComparer.Compare, 0, f)

		bounds := KeyRange(base.DefaultComparer.Compare, levelMetadata.All())
		got := string(bounds.Start) + "-" + string(bounds.End.Key)
		if got != tc.want {
			t.Errorf("KeyRange(%q) = %q, %q", tc.input, got, tc.want)
		}
	}
}

func TestOverlaps(t *testing.T) {
	var v *Version
	datadriven.RunTest(t, "testdata/overlaps", func(t *testing.T, d *datadriven.TestData) string {
		switch d.Cmd {
		case "define":
			var err error
			l0Organizer := NewL0Organizer(base.DefaultComparer, 64*1024 /* flushSplitBytes */)
			v, err = ParseVersionDebug(testkeys.Comparer, l0Organizer, d.Input)
			if err != nil {
				return err.Error()
			}
			return v.String()
		case "overlaps":
			var level int
			var start, end string
			var exclusiveEnd bool
			d.ScanArgs(t, "level", &level)
			d.ScanArgs(t, "start", &start)
			d.ScanArgs(t, "end", &end)
			d.ScanArgs(t, "exclusive-end", &exclusiveEnd)
			overlaps := v.Overlaps(level, base.UserKeyBoundsEndExclusiveIf([]byte(start), []byte(end), exclusiveEnd))
			var buf bytes.Buffer
			fmt.Fprintf(&buf, "%d files:\n", overlaps.Len())
			for f := range overlaps.All() {
				fmt.Fprintf(&buf, "%s\n", f.DebugString(base.DefaultFormatter, false))
			}
			return buf.String()
		default:
			return fmt.Sprintf("unknown command: %s", d.Cmd)
		}
	})
}

func TestContains(t *testing.T) {
	cmp := base.DefaultComparer.Compare
	newFileMeta := func(fileNum base.FileNum, size uint64, smallest, largest base.InternalKey) *TableMetadata {
		m := (&TableMetadata{
			TableNum: fileNum,
			Size:     size,
		}).ExtendPointKeyBounds(cmp, smallest, largest)
		m.InitPhysicalBacking()
		return m
	}
	m00 := newFileMeta(
		700,
		1,
		base.ParseInternalKey("b#7008,SET"),
		base.ParseInternalKey("e#7009,SET"),
	)
	m01 := newFileMeta(
		701,
		1,
		base.ParseInternalKey("c#7018,SET"),
		base.ParseInternalKey("f#7019,SET"),
	)
	m02 := newFileMeta(
		702,
		1,
		base.ParseInternalKey("f#7028,SET"),
		base.ParseInternalKey("g#7029,SET"),
	)
	m03 := newFileMeta(
		703,
		1,
		base.ParseInternalKey("x#7038,SET"),
		base.ParseInternalKey("y#7039,SET"),
	)
	m04 := newFileMeta(
		704,
		1,
		base.ParseInternalKey("n#7048,SET"),
		base.ParseInternalKey("p#7049,SET"),
	)
	m05 := newFileMeta(
		705,
		1,
		base.ParseInternalKey("p#7058,SET"),
		base.ParseInternalKey("p#7059,SET"),
	)
	m06 := newFileMeta(
		706,
		1,
		base.ParseInternalKey("p#7068,SET"),
		base.ParseInternalKey("u#7069,SET"),
	)
	m07 := newFileMeta(
		707,
		1,
		base.ParseInternalKey("r#7078,SET"),
		base.ParseInternalKey("s#7079,SET"),
	)

	m10 := newFileMeta(
		710,
		1,
		base.ParseInternalKey("d#7108,SET"),
		base.ParseInternalKey("g#7109,SET"),
	)
	m11 := newFileMeta(
		711,
		1,
		base.ParseInternalKey("h#7118,SET"),
		base.ParseInternalKey("j#7119,SET"),
	)
	m12 := newFileMeta(
		712,
		1,
		base.ParseInternalKey("n#7128,SET"),
		base.ParseInternalKey("o#7129,SET"),
	)
	m13 := newFileMeta(
		713,
		1,
		base.ParseInternalKey("p#7149,SET"),
		base.ParseInternalKey("p#7148,SET"),
	)
	m14 := newFileMeta(
		714,
		1,
		base.ParseInternalKey("q#7138,SET"),
		base.ParseInternalKey("u#7139,SET"),
	)

	v := Version{
		Levels: [NumLevels]LevelMetadata{
			0: levelMetadata(0, m00, m01, m02, m03, m04, m05, m06, m07),
			1: levelMetadata(1, m10, m11, m12, m13, m14),
		},
		cmp: testkeys.Comparer,
	}
	require.NoError(t, v.CheckOrdering())

	testCases := []struct {
		level int
		file  *TableMetadata
		want  bool
	}{
		// Level 0: m00=b-e, m01=c-f, m02=f-g, m03=x-y, m04=n-p, m05=p-p, m06=p-u, m07=r-s.
		// Note that:
		//   - the slice isn't sorted (e.g. m02=f-g, m03=x-y, m04=n-p),
		//   - m00 and m01 overlap (not just touch),
		//   - m06 contains m07,
		//   - m00, m01 and m02 transitively overlap/touch each other, and
		//   - m04, m05, m06 and m07 transitively overlap/touch each other.
		{0, m00, true},
		{0, m01, true},
		{0, m02, true},
		{0, m03, true},
		{0, m04, true},
		{0, m05, true},
		{0, m06, true},
		{0, m07, true},
		{0, m10, false},
		{0, m11, false},
		{0, m12, false},
		{0, m13, false},
		{0, m14, false},
		{1, m00, false},
		{1, m01, false},
		{1, m02, false},
		{1, m03, false},
		{1, m04, false},
		{1, m05, false},
		{1, m06, false},
		{1, m07, false},
		{1, m10, true},
		{1, m11, true},
		{1, m12, true},
		{1, m13, true},
		{1, m14, true},

		// Level 2: empty.
		{2, m00, false},
		{2, m14, false},
	}

	for _, tc := range testCases {
		got := v.Contains(tc.level, tc.file)
		if got != tc.want {
			t.Errorf("level=%d, file=%s\ngot %t\nwant %t", tc.level, tc.file, got, tc.want)
		}
	}
}

func TestVersionUnref(t *testing.T) {
	list := &VersionList{}
	list.Init(&sync.Mutex{})
	v := &Version{Deleted: func(ObsoleteFiles) {}}
	v.Ref()
	list.PushBack(v)
	v.Unref()
	if !list.Empty() {
		t.Fatalf("expected version list to be empty")
	}
}

func TestCheckOrdering(t *testing.T) {
	datadriven.RunTest(t, "testdata/version_check_ordering",
		func(t *testing.T, d *datadriven.TestData) string {
			switch d.Cmd {
			case "check-ordering":
				l0Organizer := NewL0Organizer(base.DefaultComparer, 10*1024*1024 /* flushSplitBytes */)
				v, err := ParseVersionDebug(base.DefaultComparer, l0Organizer, d.Input)
				if err != nil {
					return err.Error()
				}
				// L0 files compare on sequence numbers. Use the seqnums from the
				// smallest / largest bounds for the table.
				for m := range v.Levels[0].All() {
					m.SmallestSeqNum = m.Smallest().SeqNum()
					m.LargestSeqNum = m.Largest().SeqNum()
					m.LargestSeqNumAbsolute = m.LargestSeqNum
				}
				if err = v.CheckOrdering(); err != nil {
					return err.Error()
				}
				return "OK"

			default:
				return fmt.Sprintf("unknown command: %s", d.Cmd)
			}
		})
}

func TestCalculateInuseKeyRanges(t *testing.T) {
	newFileMeta := func(fileNum base.FileNum, size uint64, smallest, largest base.InternalKey) *TableMetadata {
		m := &TableMetadata{
			TableNum: fileNum,
			Size:     size,
		}
		m.ExtendPointKeyBounds(base.DefaultComparer.Compare, smallest, largest)
		m.InitPhysicalBacking()
		return m
	}
	tests := []struct {
		name     string
		levels   [NumLevels][]*TableMetadata
		level    int
		depth    int
		smallest []byte
		largest  []byte
		want     []base.UserKeyBounds
	}{
		{
			name: "No files in next level",
			levels: [NumLevels][]*TableMetadata{
				1: {
					newFileMeta(
						1,
						1,
						base.ParseInternalKey("a#2,SET"),
						base.ParseInternalKey("c#2,SET"),
					),
					newFileMeta(
						2,
						1,
						base.ParseInternalKey("d#2,SET"),
						base.ParseInternalKey("e#2,SET"),
					),
				},
			},
			level:    1,
			depth:    2,
			smallest: []byte("a"),
			largest:  []byte("e"),
			want: []base.UserKeyBounds{
				base.UserKeyBoundsInclusive([]byte("a"), []byte("c")),
				base.UserKeyBoundsInclusive([]byte("d"), []byte("e")),
			},
		},
		{
			name: "No overlapping key ranges",
			levels: [NumLevels][]*TableMetadata{
				1: {
					newFileMeta(
						1,
						1,
						base.ParseInternalKey("a#1,SET"),
						base.ParseInternalKey("c#1,SET"),
					),
					newFileMeta(
						2,
						1,
						base.ParseInternalKey("l#1,SET"),
						base.ParseInternalKey("p#1,SET"),
					),
				},
				2: {
					newFileMeta(
						3,
						1,
						base.ParseInternalKey("d#1,SET"),
						base.ParseInternalKey("i#1,SET"),
					),
					newFileMeta(
						4,
						1,
						base.ParseInternalKey("s#1,SET"),
						base.ParseInternalKey("w#1,SET"),
					),
				},
			},
			level:    1,
			depth:    2,
			smallest: []byte("a"),
			largest:  []byte("z"),
			want: []base.UserKeyBounds{
				base.UserKeyBoundsInclusive([]byte("a"), []byte("c")),
				base.UserKeyBoundsInclusive([]byte("d"), []byte("i")),
				base.UserKeyBoundsInclusive([]byte("l"), []byte("p")),
				base.UserKeyBoundsInclusive([]byte("s"), []byte("w")),
			},
		},
		{
			name: "First few non-overlapping, followed by overlapping",
			levels: [NumLevels][]*TableMetadata{
				1: {
					newFileMeta(
						1,
						1,
						base.ParseInternalKey("a#1,SET"),
						base.ParseInternalKey("c#1,SET"),
					),
					newFileMeta(
						2,
						1,
						base.ParseInternalKey("d#1,SET"),
						base.ParseInternalKey("e#1,SET"),
					),
					newFileMeta(
						3,
						1,
						base.ParseInternalKey("n#1,SET"),
						base.ParseInternalKey("o#1,SET"),
					),
					newFileMeta(
						4,
						1,
						base.ParseInternalKey("p#1,SET"),
						base.ParseInternalKey("q#1,SET"),
					),
				},
				2: {
					newFileMeta(
						5,
						1,
						base.ParseInternalKey("m#1,SET"),
						base.ParseInternalKey("q#1,SET"),
					),
					newFileMeta(
						6,
						1,
						base.ParseInternalKey("s#1,SET"),
						base.ParseInternalKey("w#1,SET"),
					),
				},
			},
			level:    1,
			depth:    2,
			smallest: []byte("a"),
			largest:  []byte("z"),
			want: []base.UserKeyBounds{
				base.UserKeyBoundsInclusive([]byte("a"), []byte("c")),
				base.UserKeyBoundsInclusive([]byte("d"), []byte("e")),
				base.UserKeyBoundsInclusive([]byte("m"), []byte("q")),
				base.UserKeyBoundsInclusive([]byte("s"), []byte("w")),
			},
		},
		{
			name: "All overlapping",
			levels: [NumLevels][]*TableMetadata{
				1: {
					newFileMeta(
						1,
						1,
						base.ParseInternalKey("d#1,SET"),
						base.ParseInternalKey("e#1,SET"),
					),
					newFileMeta(
						2,
						1,
						base.ParseInternalKey("n#1,SET"),
						base.ParseInternalKey("o#1,SET"),
					),
					newFileMeta(
						3,
						1,
						base.ParseInternalKey("p#1,SET"),
						base.ParseInternalKey("q#1,SET"),
					),
				},
				2: {
					newFileMeta(
						4,
						1,
						base.ParseInternalKey("a#1,SET"),
						base.ParseInternalKey("c#1,SET"),
					),
					newFileMeta(
						5,
						1,
						base.ParseInternalKey("d#1,SET"),
						base.ParseInternalKey("w#1,SET"),
					),
				},
			},
			level:    1,
			depth:    2,
			smallest: []byte("a"),
			largest:  []byte("z"),
			want: []base.UserKeyBounds{
				base.UserKeyBoundsInclusive([]byte("a"), []byte("c")),
				base.UserKeyBoundsInclusive([]byte("d"), []byte("w")),
			},
		},
		{
			name: "Touching ranges",
			levels: [NumLevels][]*TableMetadata{
				1: {
					newFileMeta(
						1,
						1,
						base.ParseInternalKey("a#1,SET"),
						base.ParseInternalKey("b#inf,RANGEDEL"),
					),
				},
				2: {
					newFileMeta(
						4,
						1,
						base.ParseInternalKey("b#1,SET"),
						base.ParseInternalKey("c#1,SET"),
					),
				},
			},
			level:    1,
			depth:    2,
			smallest: []byte("a"),
			largest:  []byte("z"),
			want: []base.UserKeyBounds{
				base.UserKeyBoundsInclusive([]byte("a"), []byte("c")),
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			l0Organizer := NewL0Organizer(base.DefaultComparer, 64*1024 /* flushSplitBytes */)
			v := NewVersionForTesting(base.DefaultComparer, l0Organizer, tt.levels)
			if err := v.CheckOrdering(); err != nil {
				t.Fatal(err)
			}

			if got := v.CalculateInuseKeyRanges(l0Organizer, tt.level, tt.depth, tt.smallest, tt.largest); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("CalculateInuseKeyRanges() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestCalculateInuseKeyRangesRandomized(t *testing.T) {
	var (
		fileNum     = base.FileNum(0)
		seed        = uint64(time.Now().UnixNano())
		rng         = rand.New(rand.NewPCG(0, seed))
		endKeyspace = 26 * 26
		cmp         = base.DefaultComparer.Compare
	)
	t.Logf("Using rng seed %d.", seed)

	for iter := 0; iter < 100; iter++ {
		makeUserKey := func(i int) []byte {
			if i >= endKeyspace {
				i = endKeyspace - 1
			}
			return []byte{byte(i/26 + 'a'), byte(i%26 + 'a')}
		}
		makeIK := func(level, i int) InternalKey {
			seqNum := base.SeqNum(NumLevels-level) * 100
			if level == 0 {
				seqNum += base.SeqNum(i)
			}
			return base.MakeInternalKey(
				makeUserKey(i),
				seqNum,
				base.InternalKeyKindSet,
			)
		}
		makeFile := func(level, start, end int) *TableMetadata {
			fileNum++
			m := &TableMetadata{TableNum: fileNum}
			m.ExtendPointKeyBounds(
				cmp,
				makeIK(level, start),
				makeIK(level, end),
			)
			m.SmallestSeqNum = m.Smallest().SeqNum()
			m.LargestSeqNum = m.Largest().SeqNum()
			m.LargestSeqNumAbsolute = m.LargestSeqNum
			m.InitPhysicalBacking()
			return m
		}
		overlaps := func(startA, endA, startB, endB []byte) bool {
			disjoint := cmp(endB, startA) < 0 || cmp(endA, startB) < 0
			return !disjoint
		}
		var files [NumLevels][]*TableMetadata
		for l := 0; l < NumLevels; l++ {
			for i := 0; i < rand.IntN(10); i++ {
				s := rng.IntN(endKeyspace)
				maxWidth := rng.IntN(endKeyspace-s) + 1
				e := rng.IntN(maxWidth) + s
				sKey, eKey := makeUserKey(s), makeUserKey(e)
				// Discard the key range if it overlaps any existing files
				// within this level.
				var o bool
				for _, f := range files[l] {
					o = o || overlaps(sKey, eKey, f.Smallest().UserKey, f.Largest().UserKey)
				}
				if o {
					continue
				}
				files[l] = append(files[l], makeFile(l, s, e))
			}
			slices.SortFunc(files[l], func(a, b *TableMetadata) int {
				return cmp(a.Smallest().UserKey, b.Smallest().UserKey)
			})
		}
		l0Organizer := NewL0Organizer(base.DefaultComparer, 64*1024 /* flushSplitBytes */)
		v := NewVersionForTesting(base.DefaultComparer, l0Organizer, files)
		if err := v.CheckOrdering(); err != nil {
			t.Fatal(err)
		}
		t.Log(v.DebugString())
		for i := 0; i < 1000; i++ {
			l := rng.IntN(NumLevels)
			s := rng.IntN(endKeyspace)
			maxWidth := rng.IntN(endKeyspace-s) + 1
			e := rng.IntN(maxWidth) + s
			sKey, eKey := makeUserKey(s), makeUserKey(e)
			keyRanges := v.CalculateInuseKeyRanges(l0Organizer, l, NumLevels-1, sKey, eKey)

			for level := l; level < NumLevels; level++ {
				for _, f := range files[level] {
					if !overlaps(sKey, eKey, f.Smallest().UserKey, f.Largest().UserKey) {
						// This file doesn't overlap the queried range. Skip it.
						continue
					}
					// This file does overlap the queried range. The key range
					// [MAX(f.Smallest, sKey), MIN(f.Largest, eKey)] must be fully
					// contained by a key range in keyRanges.
					checkStart, checkEnd := f.Smallest().UserKey, f.Largest().UserKey
					if cmp(checkStart, sKey) < 0 {
						checkStart = sKey
					}
					if cmp(checkEnd, eKey) > 0 {
						checkEnd = eKey
					}
					var contained bool
					for _, kr := range keyRanges {
						contained = contained ||
							(cmp(checkStart, kr.Start) >= 0 && cmp(checkEnd, kr.End.Key) <= 0)
					}
					if !contained {
						t.Errorf("Seed %d, iter %d: File %s overlaps %q-%q, but is not fully contained in any of the key ranges.",
							seed, iter, f, sKey, eKey)
					}
				}
			}
		}
	}
}

func TestIterAllocs(t *testing.T) {
	t.Run("LevelSlice", func(t *testing.T) {
		var testLevelSlice = func() LevelSlice {
			const n = 10
			var tables []*TableMetadata
			for i := 0; i < n; i++ {
				tables = append(tables, &TableMetadata{
					TableNum:     base.FileNum(i),
					TableBacking: &TableBacking{},
				})
			}
			return NewLevelSliceSeqSorted(tables)
		}()
		allocs := testing.AllocsPerRun(100, func() {
			for v := range testLevelSlice.All() {
				if v == nil {
					panic("nil")
				}
			}
		})
		if allocs > 0 {
			t.Fatalf("allocs=%f", allocs)
		}
	})
	t.Run("LevelMetadata", func(t *testing.T) {
		var testLevelMetadata = func() LevelMetadata {
			const n = 1000
			var tables []*TableMetadata
			for i := 0; i < n; i++ {
				tables = append(tables, &TableMetadata{
					TableNum:     base.FileNum(i),
					TableBacking: &TableBacking{},
				})
			}
			return MakeLevelMetadata(base.DefaultComparer.Compare, 0, tables)
		}()
		allocs := testing.AllocsPerRun(100, func() {
			for v := range testLevelMetadata.All() {
				if v == nil {
					panic("nil")
				}
			}
		})
		if allocs > 0 {
			t.Fatalf("allocs=%f", allocs)
		}
	})
}
