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
	"sync"
	"testing"
	"time"

	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/errors/oserror"
	"github.com/cockroachdb/pebble/internal/base"
	"github.com/cockroachdb/pebble/internal/datadriven"
	"github.com/cockroachdb/pebble/internal/errorfs"
	"github.com/cockroachdb/pebble/internal/manifest"
	"github.com/cockroachdb/pebble/internal/rangedel"
	"github.com/cockroachdb/pebble/sstable"
	"github.com/cockroachdb/pebble/vfs"
	"github.com/stretchr/testify/require"
)

func newVersion(opts *Options, files [numLevels][]*fileMetadata) *version {
	return manifest.NewVersion(
		opts.Comparer.Compare,
		opts.Comparer.FormatKey,
		opts.FlushSplitBytes,
		files)
}

type compactionPickerForTesting struct {
	score     float64
	level     int
	baseLevel int
	opts      *Options
	vers      *manifest.Version
}

var _ compactionPicker = &compactionPickerForTesting{}

func (p *compactionPickerForTesting) getScores([]compactionInfo) [numLevels]float64 {
	return [numLevels]float64{}
}

func (p *compactionPickerForTesting) getBaseLevel() int {
	return p.baseLevel
}

func (p *compactionPickerForTesting) getEstimatedMaxWAmp() float64 {
	return 0
}

func (p *compactionPickerForTesting) estimatedCompactionDebt(l0ExtraSize uint64) uint64 {
	return 0
}

func (p *compactionPickerForTesting) forceBaseLevel1() {}

func (p *compactionPickerForTesting) pickAuto(env compactionEnv) (pc *pickedCompaction) {
	if p.score < 1 {
		return nil
	}
	outputLevel := p.level + 1
	if p.level == 0 {
		outputLevel = p.baseLevel
	}
	iter := p.vers.Levels[p.level].Iter()
	iter.First()
	cInfo := candidateLevelInfo{
		level:       p.level,
		outputLevel: outputLevel,
		file:        iter.Take(),
	}
	return pickAutoHelper(env, p.opts, p.vers, cInfo, p.baseLevel)
}

func (p *compactionPickerForTesting) pickElisionOnlyCompaction(
	env compactionEnv,
) (pc *pickedCompaction) {
	return nil
}

func (p *compactionPickerForTesting) pickManual(
	env compactionEnv, manual *manualCompaction,
) (pc *pickedCompaction, retryLater bool) {
	if p == nil {
		return nil, false
	}
	return pickManualHelper(p.opts, manual, p.vers, p.baseLevel), false
}

func (p *compactionPickerForTesting) pickReadTriggeredCompaction(
	env compactionEnv,
) (pc *pickedCompaction) {
	return nil
}

func TestPickCompaction(t *testing.T) {
	fileNums := func(files manifest.LevelSlice) string {
		var ss []string
		files.Each(func(meta *fileMetadata) {
			ss = append(ss, strconv.Itoa(int(meta.FileNum)))
		})
		sort.Strings(ss)
		return strings.Join(ss, ",")
	}

	opts := (*Options)(nil).EnsureDefaults()
	testCases := []struct {
		desc    string
		version *version
		picker  compactionPickerForTesting
		want    string
	}{
		{
			desc: "no compaction",
			version: newVersion(opts, [numLevels][]*fileMetadata{
				0: {
					{
						FileNum:  100,
						Size:     1,
						Smallest: base.ParseInternalKey("i.SET.101"),
						Largest:  base.ParseInternalKey("j.SET.102"),
					},
				},
			}),
			want: "",
		},

		{
			desc: "1 L0 file",
			version: newVersion(opts, [numLevels][]*fileMetadata{
				0: {
					{
						FileNum:  100,
						Size:     1,
						Smallest: base.ParseInternalKey("i.SET.101"),
						Largest:  base.ParseInternalKey("j.SET.102"),
					},
				},
			}),
			picker: compactionPickerForTesting{
				score:     99,
				level:     0,
				baseLevel: 1,
			},
			want: "100  ",
		},

		{
			desc: "2 L0 files (0 overlaps)",
			version: newVersion(opts, [numLevels][]*fileMetadata{
				0: {
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
			}),
			picker: compactionPickerForTesting{
				score:     99,
				level:     0,
				baseLevel: 1,
			},
			want: "100  ",
		},

		{
			desc: "2 L0 files, with ikey overlap",
			version: newVersion(opts, [numLevels][]*fileMetadata{
				0: {
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
			}),
			picker: compactionPickerForTesting{
				score:     99,
				level:     0,
				baseLevel: 1,
			},
			want: "100,110  ",
		},

		{
			desc: "2 L0 files, with ukey overlap",
			version: newVersion(opts, [numLevels][]*fileMetadata{
				0: {
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
			}),
			picker: compactionPickerForTesting{
				score:     99,
				level:     0,
				baseLevel: 1,
			},
			want: "100,110  ",
		},

		{
			desc: "1 L0 file, 2 L1 files (0 overlaps)",
			version: newVersion(opts, [numLevels][]*fileMetadata{
				0: {
					{
						FileNum:  100,
						Size:     1,
						Smallest: base.ParseInternalKey("i.SET.101"),
						Largest:  base.ParseInternalKey("i.SET.102"),
					},
				},
				1: {
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
			}),
			picker: compactionPickerForTesting{
				score:     99,
				level:     0,
				baseLevel: 1,
			},
			want: "100  ",
		},

		{
			desc: "1 L0 file, 2 L1 files (1 overlap), 4 L2 files (3 overlaps)",
			version: newVersion(opts, [numLevels][]*fileMetadata{
				0: {
					{
						FileNum:  100,
						Size:     1,
						Smallest: base.ParseInternalKey("i.SET.101"),
						Largest:  base.ParseInternalKey("t.SET.102"),
					},
				},
				1: {
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
				2: {
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
			}),
			picker: compactionPickerForTesting{
				score:     99,
				level:     0,
				baseLevel: 1,
			},
			want: "100 210 310,320,330",
		},

		{
			desc: "4 L1 files, 2 L2 files, can grow",
			version: newVersion(opts, [numLevels][]*fileMetadata{
				1: {
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
				2: {
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
			}),
			picker: compactionPickerForTesting{
				score:     99,
				level:     1,
				baseLevel: 1,
			},
			want: "200,210,220 300 ",
		},

		{
			desc: "4 L1 files, 2 L2 files, can't grow (range)",
			version: newVersion(opts, [numLevels][]*fileMetadata{
				1: {
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
				2: {
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
			}),
			picker: compactionPickerForTesting{
				score:     99,
				level:     1,
				baseLevel: 1,
			},
			want: "200 300 ",
		},

		{
			desc: "4 L1 files, 2 L2 files, can't grow (size)",
			version: newVersion(opts, [numLevels][]*fileMetadata{
				1: {
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
				2: {
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
			}),
			picker: compactionPickerForTesting{
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
		vs.append(tc.version)
		tc.picker.opts = opts
		tc.picker.vers = tc.version
		vs.picker = &tc.picker
		env := compactionEnv{bytesCompacted: new(uint64)}

		pc, got := vs.picker.pickAuto(env), ""
		if pc != nil {
			c := newCompaction(pc, opts, env.bytesCompacted)
			got0 := fileNums(c.startLevel.files)
			got1 := fileNums(c.outputLevel.files)
			got2 := fileNums(c.grandparents)
			got = got0 + " " + got1 + " " + got2
		}
		if got != tc.want {
			t.Fatalf("%s:\ngot  %q\nwant %q", tc.desc, got, tc.want)
		}
	}
}

func TestElideTombstone(t *testing.T) {
	opts := &Options{}
	opts.EnsureDefaults()

	type want struct {
		key      string
		expected bool
	}

	testCases := []struct {
		desc    string
		level   int
		version *version
		wants   []want
	}{
		{
			desc:    "empty",
			level:   1,
			version: newVersion(opts, [numLevels][]*fileMetadata{}),
			wants: []want{
				{"x", true},
			},
		},
		{
			desc:  "non-empty",
			level: 1,
			version: newVersion(opts, [numLevels][]*fileMetadata{
				1: {
					{
						Smallest: base.ParseInternalKey("c.SET.801"),
						Largest:  base.ParseInternalKey("g.SET.800"),
					},
					{
						Smallest: base.ParseInternalKey("x.SET.701"),
						Largest:  base.ParseInternalKey("y.SET.700"),
					},
				},
				2: {
					{
						Smallest: base.ParseInternalKey("d.SET.601"),
						Largest:  base.ParseInternalKey("h.SET.600"),
					},
					{
						Smallest: base.ParseInternalKey("r.SET.501"),
						Largest:  base.ParseInternalKey("t.SET.500"),
					},
				},
				3: {
					{
						Smallest: base.ParseInternalKey("f.SET.401"),
						Largest:  base.ParseInternalKey("g.SET.400"),
					},
					{
						Smallest: base.ParseInternalKey("w.SET.301"),
						Largest:  base.ParseInternalKey("x.SET.300"),
					},
				},
				4: {
					{
						Smallest: base.ParseInternalKey("f.SET.201"),
						Largest:  base.ParseInternalKey("m.SET.200"),
					},
					{
						Smallest: base.ParseInternalKey("t.SET.101"),
						Largest:  base.ParseInternalKey("t.SET.100"),
					},
				},
			}),
			wants: []want{
				{"b", true},
				{"c", true},
				{"d", true},
				{"e", true},
				{"f", false},
				{"g", false},
				{"h", false},
				{"l", false},
				{"m", false},
				{"n", true},
				{"q", true},
				{"r", true},
				{"s", true},
				{"t", false},
				{"u", true},
				{"v", true},
				{"w", false},
				{"x", false},
				{"y", true},
				{"z", true},
			},
		},
		{
			desc:  "repeated ukey",
			level: 1,
			version: newVersion(opts, [numLevels][]*fileMetadata{
				6: {
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
			}),
			wants: []want{
				{"h", true},
				{"i", false},
				{"j", false},
				{"k", false},
				{"l", false},
				{"m", false},
				{"n", true},
			},
		},
	}

	for _, tc := range testCases {
		c := compaction{
			cmp:      DefaultComparer.Compare,
			version:  tc.version,
			inputs:   []compactionLevel{{level: tc.level}, {level: tc.level + 1}},
			smallest: base.ParseInternalKey("a.SET.0"),
			largest:  base.ParseInternalKey("z.SET.0"),
		}
		c.startLevel, c.outputLevel = &c.inputs[0], &c.inputs[1]
		c.setupInuseKeyRanges()
		for _, w := range tc.wants {
			if got := c.elideTombstone([]byte(w.key)); got != w.expected {
				t.Errorf("%s: ukey=%q: got %v, want %v", tc.desc, w.key, got, w.expected)
			}
		}
	}
}

func TestElideRangeTombstone(t *testing.T) {
	opts := (*Options)(nil).EnsureDefaults()

	type want struct {
		key      string
		endKey   string
		expected bool
	}

	testCases := []struct {
		desc     string
		level    int
		version  *version
		wants    []want
		flushing flushableList
	}{
		{
			desc:    "empty",
			level:   1,
			version: newVersion(opts, [numLevels][]*fileMetadata{}),
			wants: []want{
				{"x", "y", true},
			},
		},
		{
			desc:  "non-empty",
			level: 1,
			version: newVersion(opts, [numLevels][]*fileMetadata{
				1: {
					{
						Smallest: base.ParseInternalKey("c.SET.801"),
						Largest:  base.ParseInternalKey("g.SET.800"),
					},
					{
						Smallest: base.ParseInternalKey("x.SET.701"),
						Largest:  base.ParseInternalKey("y.SET.700"),
					},
				},
				2: {
					{
						Smallest: base.ParseInternalKey("d.SET.601"),
						Largest:  base.ParseInternalKey("h.SET.600"),
					},
					{
						Smallest: base.ParseInternalKey("r.SET.501"),
						Largest:  base.ParseInternalKey("t.SET.500"),
					},
				},
				3: {
					{
						Smallest: base.ParseInternalKey("f.SET.401"),
						Largest:  base.ParseInternalKey("g.SET.400"),
					},
					{
						Smallest: base.ParseInternalKey("w.SET.301"),
						Largest:  base.ParseInternalKey("x.SET.300"),
					},
				},
				4: {
					{
						Smallest: base.ParseInternalKey("f.SET.201"),
						Largest:  base.ParseInternalKey("m.SET.200"),
					},
					{
						Smallest: base.ParseInternalKey("t.SET.101"),
						Largest:  base.ParseInternalKey("t.SET.100"),
					},
				},
			}),
			wants: []want{
				{"b", "c", true},
				{"c", "d", true},
				{"d", "e", true},
				{"e", "f", false},
				{"f", "g", false},
				{"g", "h", false},
				{"h", "i", false},
				{"l", "m", false},
				{"m", "n", false},
				{"n", "o", true},
				{"q", "r", true},
				{"r", "s", true},
				{"s", "t", false},
				{"t", "u", false},
				{"u", "v", true},
				{"v", "w", false},
				{"w", "x", false},
				{"x", "y", false},
				{"y", "z", true},
			},
		},
		{
			desc:  "flushing",
			level: -1,
			version: newVersion(opts, [numLevels][]*fileMetadata{
				0: {
					{
						Smallest: base.ParseInternalKey("h.SET.901"),
						Largest:  base.ParseInternalKey("j.SET.900"),
					},
				},
				1: {
					{
						Smallest: base.ParseInternalKey("c.SET.801"),
						Largest:  base.ParseInternalKey("g.SET.800"),
					},
					{
						Smallest: base.ParseInternalKey("x.SET.701"),
						Largest:  base.ParseInternalKey("y.SET.700"),
					},
				},
			}),
			wants: []want{
				{"m", "n", false},
			},
			// Pretend one memtable is being flushed
			flushing: flushableList{nil},
		},
	}

	for _, tc := range testCases {
		c := compaction{
			cmp:      DefaultComparer.Compare,
			version:  tc.version,
			inputs:   []compactionLevel{{level: tc.level}, {level: tc.level + 1}},
			smallest: base.ParseInternalKey("a.SET.0"),
			largest:  base.ParseInternalKey("z.SET.0"),
			flushing: tc.flushing,
		}
		c.startLevel, c.outputLevel = &c.inputs[0], &c.inputs[1]
		c.setupInuseKeyRanges()
		for _, w := range tc.wants {
			if got := c.elideRangeTombstone([]byte(w.key), []byte(w.endKey)); got != w.expected {
				t.Errorf("%s: keys=%q-%q: got %v, want %v", tc.desc, w.key, w.endKey, got, w.expected)
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
	opts := &Options{
		FS:                    mem,
		MemTableSize:          memTableSize,
		DebugCheck:            DebugCheckLevels,
		L0CompactionThreshold: 8,
	}
	opts.private.enablePacing = true
	d, err := Open("", opts)
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
		for _, levelMetadata := range v.Levels {
			iter := levelMetadata.Iter()
			for meta := iter.First(); meta != nil; meta = iter.Next() {
				f, err := mem.Open(base.MakeFilename(mem, "", fileTypeTable, meta.FileNum))
				if err != nil {
					return "", "", errors.WithStack(err)
				}
				r, err := sstable.NewReader(f, sstable.ReaderOptions{})
				if err != nil {
					return "", "", errors.WithStack(err)
				}
				defer r.Close()
				iter, err := r.NewIter(nil /* lower */, nil /* upper */)
				if err != nil {
					return "", "", errors.WithStack(err)
				}
				ss = append(ss, get1(iter)+".")
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
		{"+E", "E", "Aa.BC.Bb.Dad."},
		{"+e", "Ee", "Aa.BC.Bb.Dad."},
		// The next addition creates the fourth level-0 table, and l0CompactionTrigger == 8,
		// but since the sublevel count is doubled when comparing with l0CompactionTrigger,
		// the addition of the 4th sublevel triggers a non-trivial compaction into one level-1 table.
		// Note that the keys in this one larger table are interleaved from the four smaller ones.
		{"+F", "F", "ABCDEbde."},
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
				return errors.Errorf("mem: got %q, want %q", gotMem, tc.wantMem)
			}
			if gotDisk != tc.wantDisk {
				return errors.Errorf("ldb: got %q, want %q", gotDisk, tc.wantDisk)
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

	if mockLimiter.allowCount != 0 {
		t.Errorf("limiter allow: got %d, want %d", mockLimiter.allowCount, 0)
	}
	if mockLimiter.waitCount == 0 {
		t.Errorf("limiter wait: got %d, want >%d", mockLimiter.waitCount, 0)
	}
}

func TestManualCompaction(t *testing.T) {
	var mem vfs.FS
	var d *DB
	defer func() {
		if d != nil {
			require.NoError(t, d.Close())
		}
	}()

	reset := func() {
		if d != nil {
			require.NoError(t, d.Close())
		}
		mem = vfs.NewMem()
		require.NoError(t, mem.MkdirAll("ext", 0755))

		opts := &Options{
			FS:         mem,
			DebugCheck: DebugCheckLevels,
		}
		opts.private.disableAutomaticCompactions = true

		var err error
		d, err = Open("", opts)
		require.NoError(t, err)
	}
	reset()

	var ongoingCompaction *compaction

	datadriven.RunTest(t, "testdata/manual_compaction", func(td *datadriven.TestData) string {
		switch td.Cmd {
		case "reset":
			reset()
			return ""

		case "batch":
			b := d.NewIndexedBatch()
			if err := runBatchDefineCmd(td, b); err != nil {
				return err.Error()
			}
			require.NoError(t, b.Commit(nil))
			return ""

		case "build":
			if err := runBuildCmd(td, d, mem); err != nil {
				return err.Error()
			}
			return ""

		case "compact":
			if err := runCompactCmd(td, d); err != nil {
				return err.Error()
			}
			return runLSMCmd(td, d)

		case "define":
			if d != nil {
				if err := d.Close(); err != nil {
					return err.Error()
				}
			}

			mem = vfs.NewMem()
			opts := &Options{
				FS:         mem,
				DebugCheck: DebugCheckLevels,
			}
			opts.private.disableAutomaticCompactions = true

			var err error
			if d, err = runDBDefineCmd(td, opts); err != nil {
				return err.Error()
			}

			d.mu.Lock()
			s := d.mu.versions.currentVersion().String()
			d.mu.Unlock()
			return s

		case "ingest":
			if err := runIngestCmd(td, d, mem); err != nil {
				return err.Error()
			}
			return runLSMCmd(td, d)

		case "iter":
			// TODO(peter): runDBDefineCmd doesn't properly update the visible
			// sequence number. So we have to use a snapshot with a very large
			// sequence number, otherwise the DB appears empty.
			snap := Snapshot{
				db:     d,
				seqNum: InternalKeySeqNumMax,
			}
			iter := snap.NewIter(nil)
			return runIterCmd(td, iter, true)

		case "async-compact":
			var s string
			ch := make(chan error, 1)
			go func() {
				if err := runCompactCmd(td, d); err != nil {
					ch <- err
					close(ch)
					return
				}
				d.mu.Lock()
				s = d.mu.versions.currentVersion().DebugString(base.DefaultFormatter)
				d.mu.Unlock()
				close(ch)
			}()

			manualDone := func() bool {
				select {
				case <-ch:
					return true
				default:
					return false
				}
			}

			err := try(100*time.Microsecond, 20*time.Second, func() error {
				if manualDone() {
					return nil
				}

				d.mu.Lock()
				defer d.mu.Unlock()
				if len(d.mu.compact.manual) == 0 {
					return errors.New("no manual compaction queued")
				}
				manual := d.mu.compact.manual[0]
				if manual.retries == 0 {
					return errors.New("manual compaction has not been retried")
				}
				return nil
			})
			if err != nil {
				return err.Error()
			}

			if manualDone() {
				return "manual compaction did not block for ongoing\n" + s
			}

			d.mu.Lock()
			delete(d.mu.compact.inProgress, ongoingCompaction)
			d.mu.compact.compactingCount--
			ongoingCompaction = nil
			d.maybeScheduleCompaction()
			d.mu.Unlock()
			if err := <-ch; err != nil {
				return err.Error()
			}
			return "manual compaction blocked until ongoing finished\n" + s

		case "add-ongoing-compaction":
			var startLevel int
			var outputLevel int
			td.ScanArgs(t, "startLevel", &startLevel)
			td.ScanArgs(t, "outputLevel", &outputLevel)
			ongoingCompaction = &compaction{
				inputs: []compactionLevel{{level: startLevel}, {level: outputLevel}},
			}
			ongoingCompaction.startLevel = &ongoingCompaction.inputs[0]
			ongoingCompaction.outputLevel = &ongoingCompaction.inputs[1]
			d.mu.Lock()
			d.mu.compact.inProgress[ongoingCompaction] = struct{}{}
			d.mu.compact.compactingCount++
			d.mu.Unlock()
			return ""

		case "remove-ongoing-compaction":
			d.mu.Lock()
			delete(d.mu.compact.inProgress, ongoingCompaction)
			d.mu.compact.compactingCount--
			ongoingCompaction = nil
			d.mu.Unlock()
			return ""

		case "set-concurrent-compactions":
			td.ScanArgs(t, "num", &d.opts.MaxConcurrentCompactions)
			return ""

		case "wait-pending-table-stats":
			return runTableStatsCmd(td, d)

		default:
			return fmt.Sprintf("unknown command: %s", td.Cmd)
		}
	})
}

func TestCompactionFindGrandparentLimit(t *testing.T) {
	cmp := DefaultComparer.Compare
	var grandparents []*fileMetadata

	var fileNum base.FileNum
	parseMeta := func(s string) *fileMetadata {
		parts := strings.Split(s, "-")
		if len(parts) != 2 {
			t.Fatalf("malformed table spec: %s", s)
		}
		fileNum++
		return &fileMetadata{
			FileNum:  fileNum,
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
				return ""

			case "compact":
				c := &compaction{
					cmp:          cmp,
					grandparents: manifest.NewLevelSliceKeySorted(cmp, grandparents),
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

func TestCompactionFindL0Limit(t *testing.T) {
	cmp := DefaultComparer.Compare

	fileNumCounter := 1
	parseMeta := func(s string) (*fileMetadata, error) {
		fields := strings.Fields(s)
		parts := strings.Split(fields[0], "-")
		if len(parts) != 2 {
			return nil, errors.Errorf("malformed table spec: %s", s)
		}
		m := &fileMetadata{
			FileNum:  base.FileNum(fileNumCounter),
			Smallest: base.ParseInternalKey(strings.TrimSpace(parts[0])),
			Largest:  base.ParseInternalKey(strings.TrimSpace(parts[1])),
		}
		fileNumCounter++
		m.SmallestSeqNum = m.Smallest.SeqNum()
		m.LargestSeqNum = m.Largest.SeqNum()

		for _, field := range fields[1:] {
			parts := strings.Split(field, "=")
			switch parts[0] {
			case "size":
				size, err := strconv.ParseUint(parts[1], 10, 64)
				if err != nil {
					t.Fatal(err)
				}
				m.Size = size
			}
		}
		return m, nil
	}

	var vers *version
	flushSplitBytes := int64(0)

	datadriven.RunTest(t, "testdata/compaction_find_l0_limit",
		func(d *datadriven.TestData) string {
			switch d.Cmd {
			case "define":
				fileMetas := [manifest.NumLevels][]*fileMetadata{}
				baseLevel := manifest.NumLevels - 1
				level := 0
				var err error
				for _, arg := range d.CmdArgs {
					switch arg.Key {
					case "flush_split_bytes":
						flushSplitBytes, err = strconv.ParseInt(arg.Vals[0], 10, 64)
						if err != nil {
							t.Fatal(err)
						}
					}
				}
				for _, data := range strings.Split(d.Input, "\n") {
					data = strings.TrimSpace(data)
					switch data {
					case "L0", "L1", "L2", "L3", "L4", "L5", "L6":
						level, err = strconv.Atoi(data[1:])
						if err != nil {
							return err.Error()
						}
					default:
						meta, err := parseMeta(data)
						if err != nil {
							return err.Error()
						}
						if level != 0 && level < baseLevel {
							baseLevel = level
						}
						fileMetas[level] = append(fileMetas[level], meta)
					}
				}

				vers = manifest.NewVersion(DefaultComparer.Compare, base.DefaultFormatter, flushSplitBytes, fileMetas)
				flushSplitKeys := vers.L0Sublevels.FlushSplitKeys()

				var buf strings.Builder
				buf.WriteString(vers.DebugString(base.DefaultFormatter))
				buf.WriteString("flush split keys:\n")
				for _, key := range flushSplitKeys {
					fmt.Fprintf(&buf, "\t%s\n", base.DefaultFormatter(key))
				}

				return buf.String()

			case "flush":
				c := &compaction{
					cmp:      cmp,
					version:  vers,
					l0Limits: vers.L0Sublevels.FlushSplitKeys(),
					inputs:   []compactionLevel{{level: -1}, {level: 0}},
				}
				c.startLevel, c.outputLevel = &c.inputs[0], &c.inputs[1]

				var buf bytes.Buffer
				var smallest, largest string
				var l0Limit []byte
				for i, key := range strings.Fields(d.Input) {
					if i == 0 {
						smallest = key
						l0Limit = c.findL0Limit([]byte(key))
					}
					if l0Limit != nil && c.cmp(l0Limit, []byte(key)) < 0 {
						fmt.Fprintf(&buf, "%s-%s\n", smallest, largest)
						smallest = key
						l0Limit = c.findL0Limit([]byte(key))
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
				pc := newPickedCompaction(opts, version, start, base)
				c := newCompaction(pc, opts, new(uint64))
				return fmt.Sprintf("output=%d\nmax-output-file-size=%d\n",
					c.outputLevel.level, c.maxOutputFileSize)

			default:
				return fmt.Sprintf("unknown command: %s", d.Cmd)
			}
		})
}

func TestCompactionAtomicUnitBounds(t *testing.T) {
	cmp := DefaultComparer.Compare
	var files manifest.LevelSlice

	parseMeta := func(s string) *fileMetadata {
		parts := strings.Split(s, "-")
		if len(parts) != 2 {
			t.Fatalf("malformed table spec: %s", s)
		}
		return &fileMetadata{
			Smallest: base.ParseInternalKey(parts[0]),
			Largest:  base.ParseInternalKey(parts[1]),
		}
	}

	datadriven.RunTest(t, "testdata/compaction_atomic_unit_bounds",
		func(d *datadriven.TestData) string {
			switch d.Cmd {
			case "define":
				files = manifest.LevelSlice{}
				if len(d.Input) == 0 {
					return ""
				}
				var ff []*fileMetadata
				for _, data := range strings.Split(d.Input, "\n") {
					meta := parseMeta(data)
					meta.FileNum = FileNum(len(ff))
					ff = append(ff, meta)
				}
				files = manifest.NewLevelSliceKeySorted(cmp, ff)
				return ""

			case "atomic-unit-bounds":
				c := &compaction{
					cmp:    cmp,
					inputs: []compactionLevel{{files: files}, {}},
				}
				c.startLevel, c.outputLevel = &c.inputs[0], &c.inputs[1]
				if len(d.CmdArgs) != 1 {
					return fmt.Sprintf("%s expects 1 argument", d.Cmd)
				}
				index, err := strconv.ParseInt(d.CmdArgs[0].String(), 10, 64)
				if err != nil {
					return err.Error()
				}
				iter := files.Iter()
				// Advance iter to `index`.
				_ = iter.First()
				for i := int64(0); i < index; i++ {
					_ = iter.Next()
				}
				atomicUnit, _ := expandToAtomicUnit(c.cmp, iter.Take().Slice(), true /* disableIsCompacting */)
				lower, upper := manifest.KeyRange(c.cmp, atomicUnit.Iter())
				return fmt.Sprintf("%s-%s\n", lower.UserKey, upper.UserKey)

			default:
				return fmt.Sprintf("unknown command: %s", d.Cmd)
			}
		})
}

func TestCompactionDeleteOnlyHints(t *testing.T) {
	parseUint64 := func(s string) uint64 {
		v, err := strconv.ParseUint(s, 10, 64)
		require.NoError(t, err)
		return v
	}
	var d *DB
	var compactInfo *CompactionInfo // protected by d.mu

	datadriven.RunTest(t, "testdata/compaction_delete_only_hints",
		func(td *datadriven.TestData) string {
			switch td.Cmd {
			case "define":
				if d != nil {
					compactInfo = nil
					if err := d.Close(); err != nil {
						return err.Error()
					}
				}
				opts := &Options{
					FS:         vfs.NewMem(),
					DebugCheck: DebugCheckLevels,
					EventListener: EventListener{
						CompactionEnd: func(info CompactionInfo) {
							compactInfo = &info
						},
					},
				}
				opts.private.disableTableStats = true
				var err error
				d, err = runDBDefineCmd(td, opts)
				if err != nil {
					return err.Error()
				}
				d.mu.Lock()
				t := time.Now()
				d.timeNow = func() time.Time {
					t = t.Add(time.Second)
					return t
				}
				s := d.mu.versions.currentVersion().DebugString(base.DefaultFormatter)
				d.mu.Unlock()
				return s

			case "set-hints":
				d.mu.Lock()
				defer d.mu.Unlock()
				d.mu.compact.deletionHints = d.mu.compact.deletionHints[:0]
				var buf bytes.Buffer
				for _, data := range strings.Split(td.Input, "\n") {
					parts := strings.FieldsFunc(strings.TrimSpace(data),
						func(r rune) bool { return r == '-' || r == ' ' || r == '.' })

					start, end := []byte(parts[2]), []byte(parts[3])

					var tombstoneFile *fileMetadata
					tombstoneLevel := int(parseUint64(parts[0][1:]))
					// Find the file in the current version.
					v := d.mu.versions.currentVersion()
					overlaps := v.Overlaps(tombstoneLevel, d.opts.Comparer.Compare, start, end)
					iter := overlaps.Iter()
					for m := iter.First(); m != nil; m = iter.Next() {
						if m.FileNum.String() == parts[1] {
							tombstoneFile = m
						}
					}
					h := deleteCompactionHint{
						start:                   []byte(parts[2]),
						end:                     []byte(parts[3]),
						fileSmallestSeqNum:      parseUint64(parts[4]),
						tombstoneLevel:          tombstoneLevel,
						tombstoneFile:           tombstoneFile,
						tombstoneSmallestSeqNum: parseUint64(parts[5]),
						tombstoneLargestSeqNum:  parseUint64(parts[6]),
					}
					d.mu.compact.deletionHints = append(d.mu.compact.deletionHints, h)
					fmt.Fprintln(&buf, h.String())
				}
				return buf.String()

			case "maybe-compact":
				d.mu.Lock()
				d.maybeScheduleCompaction()
				for d.mu.compact.compactingCount > 0 {
					d.mu.compact.cond.Wait()
				}

				var buf bytes.Buffer
				fmt.Fprintf(&buf, "Deletion hints:\n")
				for _, h := range d.mu.compact.deletionHints {
					fmt.Fprintf(&buf, "  %s\n", h.String())
				}
				if len(d.mu.compact.deletionHints) == 0 {
					fmt.Fprintf(&buf, "  (none)\n")
				}
				fmt.Fprintf(&buf, "Compactions:\n")
				s := "(none)"
				if compactInfo != nil {
					// JobID's aren't deterministic, especially w/ table stats
					// enabled. Use a fixed job ID for data-driven test output.
					compactInfo.JobID = 100
					s = compactInfo.String()
				}
				fmt.Fprintf(&buf, "  %s", s)
				d.mu.Unlock()
				return buf.String()

			case "compact":
				if err := runCompactCmd(td, d); err != nil {
					return err.Error()
				}
				d.mu.Lock()
				compactInfo = nil
				s := d.mu.versions.currentVersion().DebugString(base.DefaultFormatter)
				d.mu.Unlock()
				return s

			default:
				return fmt.Sprintf("unknown command: %s", td.Cmd)
			}
		})
}

func TestCompactionTombstones(t *testing.T) {
	var d *DB
	var compactInfo *CompactionInfo // protected by d.mu

	compactionString := func() string {
		for d.mu.compact.compactingCount > 0 {
			d.mu.compact.cond.Wait()
		}

		s := "(none)"
		if compactInfo != nil {
			// JobID's aren't deterministic, especially w/ table stats
			// enabled. Use a fixed job ID for data-driven test output.
			compactInfo.JobID = 100
			s = compactInfo.String()
			compactInfo = nil
		}
		return s
	}

	datadriven.RunTest(t, "testdata/compaction_tombstones",
		func(td *datadriven.TestData) string {
			switch td.Cmd {
			case "define":
				if d != nil {
					compactInfo = nil
					if err := d.Close(); err != nil {
						return err.Error()
					}
				}
				opts := &Options{
					FS:         vfs.NewMem(),
					DebugCheck: DebugCheckLevels,
					EventListener: EventListener{
						CompactionEnd: func(info CompactionInfo) {
							compactInfo = &info
						},
					},
				}
				var err error
				d, err = runDBDefineCmd(td, opts)
				if err != nil {
					return err.Error()
				}
				d.mu.Lock()
				t := time.Now()
				d.timeNow = func() time.Time {
					t = t.Add(time.Second)
					return t
				}
				s := d.mu.versions.currentVersion().DebugString(base.DefaultFormatter)
				d.mu.Unlock()
				return s

			case "maybe-compact":
				d.mu.Lock()
				d.opts.private.disableAutomaticCompactions = false
				d.maybeScheduleCompaction()
				s := compactionString()
				d.mu.Unlock()
				return s

			case "wait-pending-table-stats":
				return runTableStatsCmd(td, d)

			case "close-snapshot":
				seqNum, err := strconv.ParseUint(strings.TrimSpace(td.Input), 0, 64)
				if err != nil {
					return err.Error()
				}
				d.mu.Lock()
				var s *Snapshot
				l := &d.mu.snapshots
				for i := l.root.next; i != &l.root; i = i.next {
					if i.seqNum == seqNum {
						s = i
					}
				}
				d.mu.Unlock()
				if s == nil {
					return "(not found)"
				} else if err := s.Close(); err != nil {
					return err.Error()
				}

				d.mu.Lock()
				// Closing the snapshot may have triggered a compaction.
				str := compactionString()
				d.mu.Unlock()
				return str

			case "version":
				d.mu.Lock()
				s := d.mu.versions.currentVersion().DebugString(base.DefaultFormatter)
				d.mu.Unlock()
				return s

			default:
				return fmt.Sprintf("unknown command: %s", td.Cmd)
			}
		})
}

func TestCompactionReadTriggered(t *testing.T) {
	var d *DB
	var compactInfo *CompactionInfo // protected by d.mu

	compactionString := func() string {
		for d.mu.compact.compactingCount > 0 {
			d.mu.compact.cond.Wait()
		}

		s := "(none)"
		if compactInfo != nil {
			// JobID's aren't deterministic, especially w/ table stats
			// enabled. Use a fixed job ID for data-driven test output.
			compactInfo.JobID = 100
			s = compactInfo.String()
			compactInfo = nil
		}
		return s
	}

	datadriven.RunTest(t, "testdata/compaction_read_triggered",
		func(td *datadriven.TestData) string {
			switch td.Cmd {
			case "define":
				if d != nil {
					compactInfo = nil
					if err := d.Close(); err != nil {
						return err.Error()
					}
				}
				opts := &Options{
					FS:         vfs.NewMem(),
					DebugCheck: DebugCheckLevels,
					EventListener: EventListener{
						CompactionEnd: func(info CompactionInfo) {
							compactInfo = &info
						},
					},
				}
				var err error
				d, err = runDBDefineCmd(td, opts)
				if err != nil {
					return err.Error()
				}
				d.mu.Lock()
				t := time.Now()
				d.timeNow = func() time.Time {
					t = t.Add(time.Second)
					return t
				}
				s := d.mu.versions.currentVersion().DebugString(base.DefaultFormatter)
				d.mu.Unlock()
				return s

			case "add-read-compaction":
				d.mu.Lock()
				for _, arg := range td.CmdArgs {
					switch arg.Key {
					case "flushing":
						switch arg.Vals[0] {
						case "true":
							d.mu.compact.flushing = true
						default:
							d.mu.compact.flushing = false
						}
					}
				}
				for _, line := range strings.Split(td.Input, "\n") {
					if line == "" {
						continue
					}
					parts := strings.Split(line, " ")
					if len(parts) != 2 {
						return "error: malformed data for add-read-compaction. usage: <level>: <start>-<end>"
					}
					if l, err := strconv.Atoi(parts[0][:1]); err == nil {
						keys := strings.Split(parts[1], "-")

						rc := readCompaction{
							level: l,
							start: []byte(keys[0]),
							end:   []byte(keys[1]),
						}
						d.mu.compact.readCompactions = append(d.mu.compact.readCompactions, rc)
					} else {
						return err.Error()
					}
				}
				d.mu.Unlock()
				return ""

			case "show-read-compactions":
				d.mu.Lock()
				var sb strings.Builder
				if len(d.mu.compact.readCompactions) == 0 {
					sb.WriteString("(none)")
				}
				for _, rc := range d.mu.compact.readCompactions {
					sb.WriteString(fmt.Sprintf("(level: %d, start: %s, end: %s)\n", rc.level, string(rc.start), string(rc.end)))
				}
				d.mu.Unlock()
				return sb.String()

			case "maybe-compact":
				d.mu.Lock()
				d.opts.private.disableAutomaticCompactions = false
				d.maybeScheduleCompaction()
				s := compactionString()
				d.mu.Unlock()
				return s

			case "version":
				d.mu.Lock()
				s := d.mu.versions.currentVersion().DebugString(base.DefaultFormatter)
				d.mu.Unlock()
				return s

			default:
				return fmt.Sprintf("unknown command: %s", td.Cmd)
			}
		})
}

func TestCompactionInuseKeyRanges(t *testing.T) {
	parseMeta := func(s string) *fileMetadata {
		parts := strings.Split(s, "-")
		if len(parts) != 2 {
			t.Fatalf("malformed table spec: %s", s)
		}
		m := &fileMetadata{
			Smallest: base.ParseInternalKey(strings.TrimSpace(parts[0])),
			Largest:  base.ParseInternalKey(strings.TrimSpace(parts[1])),
		}
		m.SmallestSeqNum = m.Smallest.SeqNum()
		m.LargestSeqNum = m.Largest.SeqNum()
		return m
	}

	opts := (*Options)(nil).EnsureDefaults()

	var c *compaction
	datadriven.RunTest(t, "testdata/compaction_inuse_key_ranges", func(td *datadriven.TestData) string {
		switch td.Cmd {
		case "define":
			c = &compaction{
				cmp:       DefaultComparer.Compare,
				formatKey: DefaultComparer.FormatKey,
				inputs:    []compactionLevel{{}, {}},
			}
			c.startLevel, c.outputLevel = &c.inputs[0], &c.inputs[1]
			var files [numLevels][]*fileMetadata
			var currentLevel int
			fileNum := FileNum(1)

			for _, data := range strings.Split(td.Input, "\n") {
				switch data {
				case "L0", "L1", "L2", "L3", "L4", "L5", "L6":
					level, err := strconv.Atoi(data[1:])
					if err != nil {
						return err.Error()
					}
					currentLevel = level

				default:
					meta := parseMeta(data)
					meta.FileNum = fileNum
					fileNum++
					files[currentLevel] = append(files[currentLevel], meta)
				}
			}
			c.version = newVersion(opts, files)
			return c.version.DebugString(c.formatKey)

		case "inuse-key-ranges":
			var buf bytes.Buffer
			for _, line := range strings.Split(td.Input, "\n") {
				parts := strings.Fields(line)
				if len(parts) != 3 {
					fmt.Fprintf(&buf, "expected <level> <smallest> <largest>: %q\n", line)
					continue
				}
				level, err := strconv.Atoi(parts[0])
				if err != nil {
					fmt.Fprintf(&buf, "expected <level> <smallest> <largest>: %q: %v\n", line, err)
					continue
				}
				c.outputLevel.level = level
				c.smallest.UserKey = []byte(parts[1])
				c.largest.UserKey = []byte(parts[2])

				c.inuseKeyRanges = nil
				c.setupInuseKeyRanges()
				if len(c.inuseKeyRanges) == 0 {
					fmt.Fprintf(&buf, ".\n")
				} else {
					for i, r := range c.inuseKeyRanges {
						if i > 0 {
							fmt.Fprintf(&buf, " ")
						}
						fmt.Fprintf(&buf, "%s-%s", r.Start, r.End)
					}
					fmt.Fprintf(&buf, "\n")
				}
			}
			return buf.String()

		default:
			return fmt.Sprintf("unknown command: %s", td.Cmd)
		}
	})
}

func TestCompactionAllowZeroSeqNum(t *testing.T) {
	var d *DB
	defer func() {
		if d != nil {
			require.NoError(t, d.Close())
		}
	}()

	metaRE := regexp.MustCompile(`^L([0-9]+):([^-]+)-(.+)$`)
	var fileNum base.FileNum
	parseMeta := func(s string) (level int, meta *fileMetadata) {
		match := metaRE.FindStringSubmatch(s)
		if match == nil {
			t.Fatalf("malformed table spec: %s", s)
		}
		level, err := strconv.Atoi(match[1])
		if err != nil {
			t.Fatalf("malformed table spec: %s: %s", s, err)
		}
		fileNum++
		meta = &fileMetadata{
			FileNum:  fileNum,
			Smallest: InternalKey{UserKey: []byte(match[2])},
			Largest:  InternalKey{UserKey: []byte(match[3])},
		}
		return level, meta
	}

	datadriven.RunTest(t, "testdata/compaction_allow_zero_seqnum",
		func(td *datadriven.TestData) string {
			switch td.Cmd {
			case "define":
				if d != nil {
					if err := d.Close(); err != nil {
						return err.Error()
					}
				}

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
					inputs:  []compactionLevel{{}, {}},
				}
				c.startLevel, c.outputLevel = &c.inputs[0], &c.inputs[1]
				d.mu.Unlock()

				var buf bytes.Buffer
				for _, line := range strings.Split(td.Input, "\n") {
					parts := strings.Fields(line)
					if len(parts) == 0 {
						continue
					}
					c.flushing = nil
					c.startLevel.level = -1

					var startFiles, outputFiles []*fileMetadata
					var iter internalIterator

					switch {
					case len(parts) == 1 && parts[0] == "flush":
						c.outputLevel.level = 0
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
							if c.startLevel.level == -1 {
								c.startLevel.level = level
							}

							switch level {
							case c.startLevel.level:
								startFiles = append(startFiles, meta)
							case c.startLevel.level + 1:
								outputFiles = append(outputFiles, meta)
							default:
								return fmt.Sprintf("invalid level %d: expected %d or %d",
									level, c.startLevel.level, c.startLevel.level+1)
							}
						}
						c.outputLevel.level = c.startLevel.level + 1
						c.startLevel.files = manifest.NewLevelSliceSpecificOrder(startFiles)
						c.outputLevel.files = manifest.NewLevelSliceKeySorted(c.cmp, outputFiles)
					}

					c.smallest, c.largest = manifest.KeyRange(c.cmp,
						c.startLevel.files.Iter(),
						c.outputLevel.files.Iter())

					c.inuseKeyRanges = nil
					c.setupInuseKeyRanges()
					fmt.Fprintf(&buf, "%t\n", c.allowZeroSeqNum(iter))
				}
				return buf.String()

			default:
				return fmt.Sprintf("unknown command: %s", td.Cmd)
			}
		})
}

func TestCompactionErrorOnUserKeyOverlap(t *testing.T) {
	parseMeta := func(s string) *fileMetadata {
		parts := strings.Split(s, "-")
		if len(parts) != 2 {
			t.Fatalf("malformed table spec: %s", s)
		}
		m := &fileMetadata{
			Smallest: base.ParseInternalKey(strings.TrimSpace(parts[0])),
			Largest:  base.ParseInternalKey(strings.TrimSpace(parts[1])),
		}
		m.SmallestSeqNum = m.Smallest.SeqNum()
		m.LargestSeqNum = m.Largest.SeqNum()
		return m
	}

	datadriven.RunTest(t, "testdata/compaction_error_on_user_key_overlap",
		func(d *datadriven.TestData) string {
			switch d.Cmd {
			case "error-on-user-key-overlap":
				c := &compaction{
					cmp:       DefaultComparer.Compare,
					formatKey: DefaultComparer.FormatKey,
				}
				var files []manifest.NewFileEntry
				fileNum := FileNum(1)

				for _, data := range strings.Split(d.Input, "\n") {
					meta := parseMeta(data)
					meta.FileNum = fileNum
					fileNum++
					files = append(files, manifest.NewFileEntry{Level: 1, Meta: meta})
				}

				result := "OK"
				ve := &versionEdit{
					NewFiles: files,
				}
				if err := c.errorOnUserKeyOverlap(ve); err != nil {
					result = fmt.Sprint(err)
				}
				return result

			default:
				return fmt.Sprintf("unknown command: %s", d.Cmd)
			}
		})
}

// TestCompactionErrorCleanup tests an error encountered during a compaction
// after some output tables have been created. It ensures that the pending
// output tables are removed from the filesystem.
func TestCompactionErrorCleanup(t *testing.T) {
	// protected by d.mu
	var (
		initialSetupDone bool
		tablesCreated    []FileNum
	)

	mem := vfs.NewMem()
	ii := errorfs.OnIndex(math.MaxInt32) // start disabled
	opts := &Options{
		FS:     errorfs.Wrap(mem, ii),
		Levels: make([]LevelOptions, numLevels),
		EventListener: EventListener{
			TableCreated: func(info TableCreateInfo) {
				t.Log(info)

				// If the initial setup is over, record tables created and
				// inject an error immediately after the second table is
				// created.
				if initialSetupDone {
					tablesCreated = append(tablesCreated, info.FileNum)
					if len(tablesCreated) >= 2 {
						ii.SetIndex(0)
					}
				}
			},
		},
	}
	for i := range opts.Levels {
		opts.Levels[i].TargetFileSize = 1
	}
	d, err := Open("", opts)
	require.NoError(t, err)

	ingest := func(keys ...string) {
		t.Helper()
		f, err := mem.Create("ext")
		require.NoError(t, err)

		w := sstable.NewWriter(f, sstable.WriterOptions{})
		for _, k := range keys {
			require.NoError(t, w.Set([]byte(k), nil))
		}
		require.NoError(t, w.Close())
		require.NoError(t, d.Ingest([]string{"ext"}))
	}
	ingest("a", "c")
	ingest("b")

	// Trigger a manual compaction, which will encounter an injected error
	// after the second table is created.
	d.mu.Lock()
	initialSetupDone = true
	d.mu.Unlock()
	err = d.Compact([]byte("a"), []byte("d"))
	require.Error(t, err, "injected error")

	d.mu.Lock()
	if len(tablesCreated) < 2 {
		t.Fatalf("expected 2 output tables created by compaction: found %d", len(tablesCreated))
	}
	d.mu.Unlock()

	require.NoError(t, d.Close())
	for _, fileNum := range tablesCreated {
		filename := fmt.Sprintf("%s.sst", fileNum)
		if _, err = mem.Stat(filename); err == nil || !oserror.IsNotExist(err) {
			t.Errorf("expected %q to not exist: %s", filename, err)
		}
	}
}

func TestCompactionCheckOrdering(t *testing.T) {
	parseMeta := func(s string) *fileMetadata {
		parts := strings.Split(s, "-")
		if len(parts) != 2 {
			t.Fatalf("malformed table spec: %s", s)
		}
		m := &fileMetadata{
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
					cmp:       DefaultComparer.Compare,
					formatKey: DefaultComparer.FormatKey,
					logger:    panicLogger{},
					inputs:    []compactionLevel{{level: -1}, {level: -1}},
				}
				c.startLevel, c.outputLevel = &c.inputs[0], &c.inputs[1]
				var startFiles, outputFiles []*fileMetadata
				var files *[]*fileMetadata
				fileNum := FileNum(1)

				for _, data := range strings.Split(d.Input, "\n") {
					switch data {
					case "L0", "L1", "L2", "L3", "L4", "L5", "L6":
						level, err := strconv.Atoi(data[1:])
						if err != nil {
							return err.Error()
						}
						if c.startLevel.level == -1 {
							c.startLevel.level = level
							files = &startFiles
						} else if c.outputLevel.level == -1 {
							if c.startLevel.level >= level {
								return fmt.Sprintf("startLevel=%d >= outputLevel=%d\n", c.startLevel.level, level)
							}
							c.outputLevel.level = level
							files = &outputFiles
						} else {
							return "outputLevel already set\n"
						}

					default:
						meta := parseMeta(data)
						meta.FileNum = fileNum
						fileNum++
						*files = append(*files, meta)
					}
				}

				c.startLevel.files = manifest.NewLevelSliceSpecificOrder(startFiles)
				c.outputLevel.files = manifest.NewLevelSliceSpecificOrder(outputFiles)
				if c.outputLevel.level == -1 {
					c.outputLevel.level = 0
				}

				newIters := func(
					_ *manifest.FileMetadata, _ *IterOptions, _ *uint64,
				) (internalIterator, internalIterator, error) {
					return &errorIter{}, nil, nil
				}
				result := "OK"
				_, err := c.newInputIter(newIters)
				if err != nil {
					result = fmt.Sprint(err)
				}
				return result

			default:
				return fmt.Sprintf("unknown command: %s", d.Cmd)
			}
		})
}

type mockSplitter struct {
	shouldSplitVal compactionSplitSuggestion
}

func (m *mockSplitter) shouldSplitBefore(
	key *InternalKey, tw *sstable.Writer,
) compactionSplitSuggestion {
	return m.shouldSplitVal
}

func (m *mockSplitter) onNewOutput(key *InternalKey) []byte {
	return nil
}

func TestCompactionOutputSplitters(t *testing.T) {
	var main, child0, child1 compactionOutputSplitter
	pickSplitter := func(input string) *compactionOutputSplitter {
		switch input {
		case "main":
			return &main
		case "child0":
			return &child0
		case "child1":
			return &child1
		default:
			t.Fatalf("invalid splitter slot: %s", input)
			return nil
		}
	}

	datadriven.RunTest(t, "testdata/compaction_output_splitters",
		func(d *datadriven.TestData) string {
			switch d.Cmd {
			case "reset":
				main = nil
				child0 = nil
				child1 = nil
			case "init":
				if len(d.CmdArgs) < 2 {
					return "expected at least 2 args"
				}
				splitterToInit := pickSplitter(d.CmdArgs[0].Key)
				switch d.CmdArgs[1].Key {
				case "array":
					*splitterToInit = &splitterGroup{
						cmp:       base.DefaultComparer.Compare,
						splitters: []compactionOutputSplitter{child0, child1},
					}
				case "mock":
					*splitterToInit = &mockSplitter{}
				case "userkey":
					*splitterToInit = &userKeyChangeSplitter{
						cmp:      base.DefaultComparer.Compare,
						splitter: child0,
					}
				case "nonzeroseqnum":
					c := &compaction{
						rangeDelFrag: rangedel.Fragmenter{
							Cmp:    base.DefaultComparer.Compare,
							Format: base.DefaultFormatter,
							Emit:   func(fragmented []rangedel.Tombstone) {},
						},
					}
					frag := &c.rangeDelFrag
					if len(d.CmdArgs) >= 3 {
						if d.CmdArgs[2].Key == "tombstone" {
							// Add a tombstone so Empty() returns false.
							frag.Add(base.ParseInternalKey("foo.RANGEDEL.10"), []byte("pan"))
						}
					}
					*splitterToInit = &nonZeroSeqNumSplitter{
						c:        c,
						splitter: child0,
					}
				}
				(*splitterToInit).onNewOutput(nil)
			case "set-should-split":
				if len(d.CmdArgs) < 2 {
					return "expected at least 2 args"
				}
				splitterToSet := (*pickSplitter(d.CmdArgs[0].Key)).(*mockSplitter)
				var val compactionSplitSuggestion
				switch d.CmdArgs[1].Key {
				case "split-now":
					val = splitNow
				case "split-soon":
					val = splitSoon
				case "no-split":
					val = noSplit
				default:
					t.Fatalf("unexpected value for should-split: %s", d.CmdArgs[1].Key)
				}
				splitterToSet.shouldSplitVal = val
			case "should-split-before":
				if len(d.CmdArgs) < 1 {
					return "expected at least 1 arg"
				}
				key := base.ParseInternalKey(d.CmdArgs[0].Key)
				shouldSplit := main.shouldSplitBefore(&key, nil)
				if shouldSplit == splitNow {
					main.onNewOutput(&key)
				}
				return shouldSplit.String()
			default:
				return fmt.Sprintf("unknown command: %s", d.Cmd)
			}
			return "ok"
		})
}

func TestFlushInvariant(t *testing.T) {
	for _, disableWAL := range []bool{false, true} {
		t.Run(fmt.Sprintf("disableWAL=%t", disableWAL), func(t *testing.T) {
			for i := 0; i < 2; i++ {
				t.Run("", func(t *testing.T) {
					errCh := make(chan error, 1)
					defer close(errCh)
					d, err := Open("", &Options{
						DisableWAL: disableWAL,
						FS:         vfs.NewMem(),
						EventListener: EventListener{
							BackgroundError: func(err error) {
								select {
								case errCh <- err:
								default:
								}
							},
						},
						DebugCheck: DebugCheckLevels,
					})
					require.NoError(t, err)

					require.NoError(t, d.Set([]byte("hello"), nil, NoSync))

					// Contort the DB into a state where it does something invalid.
					d.mu.Lock()
					switch i {
					case 0:
						// Force the next log number to be 0.
						d.mu.versions.nextFileNum = 0
					case 1:
						// Force the flushing memtable to have a log number equal to the new
						// log's number.
						d.mu.mem.queue[len(d.mu.mem.queue)-1].logNum = d.mu.versions.nextFileNum
					}
					d.mu.Unlock()

					flushCh, err := d.AsyncFlush()
					require.NoError(t, err)

					select {
					case err := <-errCh:
						if disableWAL {
							t.Fatalf("expected success, but found %v", err)
						} else if !errors.Is(err, errFlushInvariant) {
							t.Fatalf("expected %q, but found %v", errFlushInvariant, err)
						}
					case <-flushCh:
						if !disableWAL {
							t.Fatalf("expected error but found success")
						}
					}

					require.NoError(t, d.Close())
				})
			}
		})
	}
}

func TestCompactFlushQueuedMemTable(t *testing.T) {
	// Verify that manual compaction forces a flush of a queued memtable.

	mem := vfs.NewMem()
	d, err := Open("", &Options{
		FS: mem,
	})
	require.NoError(t, err)

	// Add the key "a" to the memtable, then fill up the memtable with the key
	// "b". The compaction will only overlap with the queued memtable, not the
	// mutable memtable.
	require.NoError(t, d.Set([]byte("a"), nil, nil))
	for {
		require.NoError(t, d.Set([]byte("b"), nil, nil))
		d.mu.Lock()
		done := len(d.mu.mem.queue) == 2
		d.mu.Unlock()
		if done {
			break
		}
	}

	require.NoError(t, d.Compact([]byte("a"), []byte("a\x00")))
	d.mu.Lock()
	require.Equal(t, 1, len(d.mu.mem.queue))
	d.mu.Unlock()
	require.NoError(t, d.Close())
}

func TestCompactFlushQueuedLargeBatch(t *testing.T) {
	// Verify that compaction forces a flush of a queued large batch.

	mem := vfs.NewMem()
	d, err := Open("", &Options{
		FS: mem,
	})
	require.NoError(t, err)

	// The default large batch threshold is slightly less than 1/2 of the
	// memtable size which makes triggering a problem with flushing queued large
	// batches irritating. Manually adjust the threshold to 1/8 of the memtable
	// size in order to more easily create a situation where a large batch is
	// queued but not automatically flushed.
	d.mu.Lock()
	d.largeBatchThreshold = d.opts.MemTableSize / 8
	require.Equal(t, 1, len(d.mu.mem.queue))
	d.mu.Unlock()

	// Set a record with a large value. This will be transformed into a large
	// batch and placed in the flushable queue.
	require.NoError(t, d.Set([]byte("a"), bytes.Repeat([]byte("v"), d.largeBatchThreshold), nil))
	d.mu.Lock()
	require.Greater(t, len(d.mu.mem.queue), 1)
	d.mu.Unlock()

	require.NoError(t, d.Compact([]byte("a"), []byte("a\x00")))
	d.mu.Lock()
	require.Equal(t, 1, len(d.mu.mem.queue))
	d.mu.Unlock()

	require.NoError(t, d.Close())
}

// Regression test for #747. Test a problematic series of "cleaner" operations
// that could previously lead to DB.disableFileDeletions blocking forever even
// though no cleaning was in progress.
func TestCleanerCond(t *testing.T) {
	d, err := Open("", &Options{
		FS: vfs.NewMem(),
	})
	require.NoError(t, err)

	for i := 0; i < 10; i++ {
		d.mu.Lock()
		require.True(t, d.acquireCleaningTurn(true))
		d.mu.Unlock()

		var wg sync.WaitGroup
		wg.Add(2)

		go func() {
			defer wg.Done()
			d.mu.Lock()
			if d.acquireCleaningTurn(true) {
				d.releaseCleaningTurn()
			}
			d.mu.Unlock()
		}()

		runtime.Gosched()

		go func() {
			defer wg.Done()
			d.mu.Lock()
			d.disableFileDeletions()
			d.enableFileDeletions()
			d.mu.Unlock()
		}()

		runtime.Gosched()

		d.mu.Lock()
		d.releaseCleaningTurn()
		d.mu.Unlock()

		wg.Wait()
	}

	require.NoError(t, d.Close())
}

func TestAdjustGrandparentOverlapBytesForFlush(t *testing.T) {
	// 500MB in Lbase
	var lbaseFiles []*manifest.FileMetadata
	const lbaseSize = 5 << 20
	for i := 0; i < 100; i++ {
		lbaseFiles =
			append(lbaseFiles, &manifest.FileMetadata{Size: lbaseSize, FileNum: FileNum(i)})
	}
	const maxOutputFileSize = 2 << 20
	// 20MB max overlap, so flush split into 25 files.
	const maxOverlapBytes = 20 << 20
	ls := manifest.NewLevelSliceSpecificOrder(lbaseFiles)
	testCases := []struct {
		flushingBytes        uint64
		adjustedOverlapBytes uint64
	}{
		// Flushes large enough that 25 files is acceptable.
		{flushingBytes: 128 << 20, adjustedOverlapBytes: 20971520},
		{flushingBytes: 64 << 20, adjustedOverlapBytes: 20971520},
		// Small increase in adjustedOverlapBytes.
		{flushingBytes: 32 << 20, adjustedOverlapBytes: 32768000},
		// Large increase in adjusterOverlapBytes, to limit to 4 files.
		{flushingBytes: 1 << 20, adjustedOverlapBytes: 131072000},
	}
	for _, tc := range testCases {
		t.Run("", func(t *testing.T) {
			c := compaction{
				grandparents:      ls,
				maxOverlapBytes:   maxOverlapBytes,
				maxOutputFileSize: maxOutputFileSize,
			}
			adjustGrandparentOverlapBytesForFlush(&c, tc.flushingBytes)
			require.Equal(t, tc.adjustedOverlapBytes, c.maxOverlapBytes)
		})
	}
}

func TestCompactionInvalidBounds(t *testing.T) {
	db, err := Open("", &Options{
		FS: vfs.NewMem(),
	})
	require.NoError(t, err)
	defer db.Close()
	require.NoError(t, db.Compact([]byte("a"), []byte("b")))
	require.Error(t, db.Compact([]byte("a"), []byte("a")))
	require.Error(t, db.Compact([]byte("b"), []byte("a")))
}
