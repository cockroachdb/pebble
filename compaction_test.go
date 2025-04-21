// Copyright 2013 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package pebble

import (
	"bytes"
	"cmp"
	"context"
	crand "crypto/rand"
	"fmt"
	"math"
	"math/rand/v2"
	"path/filepath"
	"regexp"
	"slices"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/cockroachdb/crlib/crstrings"
	"github.com/cockroachdb/crlib/crtime"
	"github.com/cockroachdb/datadriven"
	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/errors/oserror"
	"github.com/cockroachdb/pebble/internal/base"
	"github.com/cockroachdb/pebble/internal/compact"
	"github.com/cockroachdb/pebble/internal/manifest"
	"github.com/cockroachdb/pebble/internal/testkeys"
	"github.com/cockroachdb/pebble/internal/testutils"
	"github.com/cockroachdb/pebble/objstorage"
	"github.com/cockroachdb/pebble/objstorage/objstorageprovider"
	"github.com/cockroachdb/pebble/objstorage/remote"
	"github.com/cockroachdb/pebble/sstable"
	"github.com/cockroachdb/pebble/vfs"
	"github.com/cockroachdb/pebble/vfs/errorfs"
	"github.com/stretchr/testify/require"
)

func newVersion(opts *Options, files [numLevels][]*tableMetadata) *version {
	v, _ := newVersionAndL0Organizer(opts, files)
	return v
}

func newVersionAndL0Organizer(
	opts *Options, files [numLevels][]*tableMetadata,
) (*version, *manifest.L0Organizer) {
	l0Organizer := manifest.NewL0Organizer(opts.Comparer, opts.FlushSplitBytes)
	v := manifest.NewVersionForTesting(
		opts.Comparer,
		l0Organizer,
		files)
	if err := v.CheckOrdering(); err != nil {
		panic(err)
	}
	return v, l0Organizer
}

type compactionPickerForTesting struct {
	score       float64
	level       int
	baseLevel   int
	opts        *Options
	vers        *manifest.Version
	l0Organizer *manifest.L0Organizer
}

var _ compactionPicker = &compactionPickerForTesting{}

func (p *compactionPickerForTesting) getScores([]compactionInfo) [numLevels]float64 {
	return [numLevels]float64{}
}

func (p *compactionPickerForTesting) getBaseLevel() int {
	return p.baseLevel
}

func (p *compactionPickerForTesting) estimatedCompactionDebt(l0ExtraSize uint64) uint64 {
	return 0
}

func (p *compactionPickerForTesting) forceBaseLevel1() {}

func (p *compactionPickerForTesting) pickAutoScore(env compactionEnv) (pc *pickedCompaction) {
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
	if cInfo.level == 0 {
		return pickL0(env, p.opts, p.vers, p.l0Organizer, p.baseLevel)
	}
	return pickAutoLPositive(env, p.opts, p.vers, p.l0Organizer, cInfo, p.baseLevel)
}

func (p *compactionPickerForTesting) pickAutoNonScore(env compactionEnv) (pc *pickedCompaction) {
	return nil
}

func TestPickCompaction(t *testing.T) {
	fileNums := func(files manifest.LevelSlice) string {
		var ss []string
		for meta := range files.All() {
			ss = append(ss, strconv.Itoa(int(meta.FileNum)))
		}
		sort.Strings(ss)
		return strings.Join(ss, ",")
	}

	opts := DefaultOptions()
	newFileMeta := func(fileNum base.FileNum, size uint64, smallest, largest base.InternalKey) *tableMetadata {
		m := (&tableMetadata{
			FileNum: fileNum,
			Size:    size,
		}).ExtendPointKeyBounds(opts.Comparer.Compare, smallest, largest)
		m.InitPhysicalBacking()
		return m
	}

	testCases := []struct {
		desc      string
		files     [numLevels][]*tableMetadata
		picker    compactionPickerForTesting
		want      string
		wantMulti bool
	}{
		{
			desc: "no compaction",
			files: [numLevels][]*tableMetadata{
				0: {
					newFileMeta(
						100,
						1,
						base.ParseInternalKey("i.SET.101"),
						base.ParseInternalKey("j.SET.102"),
					),
				},
			},
			want: "",
		},

		{
			desc: "1 L0 file",
			files: [numLevels][]*tableMetadata{
				0: {
					newFileMeta(
						100,
						1,
						base.ParseInternalKey("i.SET.101"),
						base.ParseInternalKey("j.SET.102"),
					),
				},
			},
			picker: compactionPickerForTesting{
				score:     99,
				level:     0,
				baseLevel: 1,
			},
			want: "100  ",
		},

		{
			desc: "2 L0 files (0 overlaps)",
			files: [numLevels][]*tableMetadata{
				0: {
					newFileMeta(
						100,
						1,
						base.ParseInternalKey("i.SET.101"),
						base.ParseInternalKey("j.SET.102"),
					),
					newFileMeta(
						110,
						1,
						base.ParseInternalKey("k.SET.111"),
						base.ParseInternalKey("l.SET.112"),
					),
				},
			},
			picker: compactionPickerForTesting{
				score:     99,
				level:     0,
				baseLevel: 1,
			},
			want: "100,110  ",
		},

		{
			desc: "2 L0 files, with ikey overlap",
			files: [numLevels][]*tableMetadata{
				0: {
					newFileMeta(
						100,
						1,
						base.ParseInternalKey("i.SET.101"),
						base.ParseInternalKey("p.SET.102"),
					),
					newFileMeta(
						110,
						1,
						base.ParseInternalKey("j.SET.111"),
						base.ParseInternalKey("q.SET.112"),
					),
				},
			},
			picker: compactionPickerForTesting{
				score:     99,
				level:     0,
				baseLevel: 1,
			},
			want: "100,110  ",
		},

		{
			desc: "2 L0 files, with ukey overlap",
			files: [numLevels][]*tableMetadata{
				0: {
					newFileMeta(
						100,
						1,
						base.ParseInternalKey("i.SET.102"),
						base.ParseInternalKey("i.SET.101"),
					),
					newFileMeta(
						110,
						1,
						base.ParseInternalKey("i.SET.112"),
						base.ParseInternalKey("i.SET.111"),
					),
				},
			},
			picker: compactionPickerForTesting{
				score:     99,
				level:     0,
				baseLevel: 1,
			},
			want: "100,110  ",
		},

		{
			desc: "1 L0 file, 2 L1 files (0 overlaps)",
			files: [numLevels][]*tableMetadata{
				0: {
					newFileMeta(
						100,
						1,
						base.ParseInternalKey("i.SET.102"),
						base.ParseInternalKey("i.SET.101"),
					),
				},
				1: {
					newFileMeta(
						200,
						1,
						base.ParseInternalKey("a.SET.201"),
						base.ParseInternalKey("b.SET.202"),
					),
					newFileMeta(
						210,
						1,
						base.ParseInternalKey("y.SET.211"),
						base.ParseInternalKey("z.SET.212"),
					),
				},
			},
			picker: compactionPickerForTesting{
				score:     99,
				level:     0,
				baseLevel: 1,
			},
			want: "100  ",
		},

		{
			desc: "1 L0 file, 2 L1 files (1 overlap), 4 L2 files (3 overlaps)",
			files: [numLevels][]*tableMetadata{
				0: {
					newFileMeta(
						100,
						1,
						base.ParseInternalKey("i.SET.102"),
						base.ParseInternalKey("t.SET.101"),
					),
				},
				1: {
					newFileMeta(
						200,
						1,
						base.ParseInternalKey("a.SET.201"),
						base.ParseInternalKey("e.SET.202"),
					),
					newFileMeta(
						210,
						1,
						base.ParseInternalKey("f.SET.211"),
						base.ParseInternalKey("j.SET.212"),
					),
				},
				2: {
					newFileMeta(
						300,
						1,
						base.ParseInternalKey("a.SET.301"),
						base.ParseInternalKey("b.SET.302"),
					),
					newFileMeta(
						310,
						1,
						base.ParseInternalKey("c.SET.311"),
						base.ParseInternalKey("g.SET.312"),
					),
					newFileMeta(
						320,
						1,
						base.ParseInternalKey("h.SET.321"),
						base.ParseInternalKey("m.SET.322"),
					),
					newFileMeta(
						330,
						1,
						base.ParseInternalKey("n.SET.331"),
						base.ParseInternalKey("z.SET.332"),
					),
				},
			},
			picker: compactionPickerForTesting{
				score:     99,
				level:     0,
				baseLevel: 1,
			},
			want: "100 210 310,320,330",
		},

		{
			desc: "4 L1 files, 2 L2 files, can grow",
			files: [numLevels][]*tableMetadata{
				1: {
					newFileMeta(
						200,
						1,
						base.ParseInternalKey("i1.SET.201"),
						base.ParseInternalKey("i2.SET.202"),
					),
					newFileMeta(
						210,
						1,
						base.ParseInternalKey("j1.SET.211"),
						base.ParseInternalKey("j2.SET.212"),
					),
					newFileMeta(
						220,
						1,
						base.ParseInternalKey("k1.SET.221"),
						base.ParseInternalKey("k2.SET.222"),
					),
					newFileMeta(
						230,
						1,
						base.ParseInternalKey("l1.SET.231"),
						base.ParseInternalKey("l2.SET.232"),
					),
				},
				2: {
					newFileMeta(
						300,
						1,
						base.ParseInternalKey("a0.SET.301"),
						base.ParseInternalKey("l0.SET.302"),
					),
					newFileMeta(
						310,
						1,
						base.ParseInternalKey("l2.SET.311"),
						base.ParseInternalKey("z2.SET.312"),
					),
				},
			},
			picker: compactionPickerForTesting{
				score:     99,
				level:     1,
				baseLevel: 1,
			},
			want:      "200,210,220 300  ",
			wantMulti: true,
		},

		{
			desc: "4 L1 files, 2 L2 files, can't grow (range)",
			files: [numLevels][]*tableMetadata{
				1: {
					newFileMeta(
						200,
						1,
						base.ParseInternalKey("i1.SET.201"),
						base.ParseInternalKey("i2.SET.202"),
					),
					newFileMeta(
						210,
						1,
						base.ParseInternalKey("j1.SET.211"),
						base.ParseInternalKey("j2.SET.212"),
					),
					newFileMeta(
						220,
						1,
						base.ParseInternalKey("k1.SET.221"),
						base.ParseInternalKey("k2.SET.222"),
					),
					newFileMeta(
						230,
						1,
						base.ParseInternalKey("l1.SET.231"),
						base.ParseInternalKey("l2.SET.232"),
					),
				},
				2: {
					newFileMeta(
						300,
						1,
						base.ParseInternalKey("a0.SET.301"),
						base.ParseInternalKey("j0.SET.302"),
					),
					newFileMeta(
						310,
						1,
						base.ParseInternalKey("j2.SET.311"),
						base.ParseInternalKey("z2.SET.312"),
					),
				},
			},
			picker: compactionPickerForTesting{
				score:     99,
				level:     1,
				baseLevel: 1,
			},
			want:      "200 300  ",
			wantMulti: true,
		},

		{
			desc: "4 L1 files, 2 L2 files, can't grow (size)",
			files: [numLevels][]*tableMetadata{
				1: {
					newFileMeta(
						200,
						expandedCompactionByteSizeLimit(opts, 1, math.MaxUint64)-1,
						base.ParseInternalKey("i1.SET.201"),
						base.ParseInternalKey("i2.SET.202"),
					),
					newFileMeta(
						210,
						expandedCompactionByteSizeLimit(opts, 1, math.MaxUint64)-1,
						base.ParseInternalKey("j1.SET.211"),
						base.ParseInternalKey("j2.SET.212"),
					),
					newFileMeta(
						220,
						expandedCompactionByteSizeLimit(opts, 1, math.MaxUint64)-1,
						base.ParseInternalKey("k1.SET.221"),
						base.ParseInternalKey("k2.SET.222"),
					),
					newFileMeta(
						230,
						expandedCompactionByteSizeLimit(opts, 1, math.MaxUint64)-1,
						base.ParseInternalKey("l1.SET.231"),
						base.ParseInternalKey("l2.SET.232"),
					),
				},
				2: {
					newFileMeta(
						300,
						expandedCompactionByteSizeLimit(opts, 2, math.MaxUint64)-1,
						base.ParseInternalKey("a0.SET.301"),
						base.ParseInternalKey("l0.SET.302"),
					),
					newFileMeta(
						310,
						expandedCompactionByteSizeLimit(opts, 2, math.MaxUint64)-1,
						base.ParseInternalKey("l2.SET.311"),
						base.ParseInternalKey("z2.SET.312"),
					),
				},
			},
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
			opts: opts,
			cmp:  DefaultComparer,
		}
		tc.picker.opts = opts
		tc.picker.vers, tc.picker.l0Organizer = newVersionAndL0Organizer(opts, tc.files)
		vs.versions.Init(nil)
		vs.append(tc.picker.vers)
		vs.picker = &tc.picker
		pc, got := vs.picker.pickAutoScore(compactionEnv{diskAvailBytes: math.MaxUint64}), ""
		if pc != nil {
			c := newCompaction(pc, opts, time.Now(), nil /* provider */, noopGrantHandle{}, neverSeparateValues)

			gotStart := fileNums(c.startLevel.files)
			gotML := ""
			observedMulti := len(c.extraLevels) > 0
			if observedMulti {
				gotML = " " + fileNums(c.extraLevels[0].files)
			}
			gotOutput := " " + fileNums(c.outputLevel.files)
			gotGrandparents := " " + fileNums(c.grandparents)
			got = gotStart + gotML + gotOutput + gotGrandparents
			if tc.wantMulti != observedMulti {
				t.Fatalf("Expected Multi %t; Observed Multi %t, for %s", tc.wantMulti, observedMulti, got)
			}

		}
		if got != tc.want {
			t.Fatalf("%s:\ngot  %q\nwant %q", tc.desc, got, tc.want)
		}
	}
}

func TestAutomaticFlush(t *testing.T) {
	const memTableSize = 10000
	// Tuned so that 2 values can reside in the memtable before a flush, but a
	// 3rd value will cause a flush. Needs to account for the max skiplist node
	// size.
	const valueSize = 3500

	mem := vfs.NewMem()
	opts := &Options{
		Comparer:              testkeys.Comparer,
		DebugCheck:            DebugCheckLevels,
		FS:                    mem,
		L0CompactionThreshold: 8,
		MemTableSize:          memTableSize,
	}
	opts = opts.testingRandomized(t)
	opts.WithFSDefaults()
	d, err := Open("", opts)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}

	get1 := func(iter internalIterator) (ret string) {
		b := &bytes.Buffer{}
		for kv := iter.First(); kv != nil; kv = iter.Next() {
			b.Write(kv.K.UserKey)
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
		provider, err := objstorageprovider.Open(objstorageprovider.DefaultSettings(mem, "" /* dirName */))
		if err != nil {
			t.Fatalf("%v", err)
		}
		defer provider.Close()
		for _, levelMetadata := range v.Levels {
			for meta := range levelMetadata.All() {
				if meta.Virtual {
					continue
				}
				f, err := provider.OpenForReading(context.Background(), base.FileTypeTable, meta.FileBacking.DiskFileNum, objstorage.OpenOptions{})
				if err != nil {
					return "", "", errors.WithStack(err)
				}
				r, err := sstable.NewReader(context.Background(), f, opts.MakeReaderOptions())
				if err != nil {
					return "", "", errors.WithStack(err)
				}
				defer r.Close()
				iter, err := r.NewIter(sstable.NoTransforms, nil /* lower */, nil /* upper */, sstable.AssertNoBlobHandles)
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
}

func TestValidateVersionEdit(t *testing.T) {
	const badKey = "malformed-key"

	errValidationFailed := errors.New("validation failed")
	validateFn := func(key []byte) error {
		if string(key) == badKey {
			return errValidationFailed
		}
		return nil
	}

	cmp := DefaultComparer.Compare
	newFileMeta := func(smallest, largest base.InternalKey) *tableMetadata {
		m := (&tableMetadata{}).ExtendPointKeyBounds(cmp, smallest, largest)
		m.InitPhysicalBacking()
		return m
	}

	testCases := []struct {
		desc    string
		ve      *versionEdit
		vFunc   func([]byte) error
		wantErr error
	}{
		{
			desc: "single new file; start key",
			ve: &versionEdit{
				NewTables: []manifest.NewTableEntry{
					{
						Meta: newFileMeta(
							manifest.InternalKey{UserKey: []byte(badKey)},
							manifest.InternalKey{UserKey: []byte("z")},
						),
					},
				},
			},
			vFunc:   validateFn,
			wantErr: errValidationFailed,
		},
		{
			desc: "single new file; end key",
			ve: &versionEdit{
				NewTables: []manifest.NewTableEntry{
					{
						Meta: newFileMeta(
							manifest.InternalKey{UserKey: []byte("a")},
							manifest.InternalKey{UserKey: []byte(badKey)},
						),
					},
				},
			},
			vFunc:   validateFn,
			wantErr: errValidationFailed,
		},
		{
			desc: "multiple new files",
			ve: &versionEdit{
				NewTables: []manifest.NewTableEntry{
					{
						Meta: newFileMeta(
							manifest.InternalKey{UserKey: []byte("a")},
							manifest.InternalKey{UserKey: []byte("c")},
						),
					},
					{
						Meta: newFileMeta(
							manifest.InternalKey{UserKey: []byte(badKey)},
							manifest.InternalKey{UserKey: []byte("z")},
						),
					},
				},
			},
			vFunc:   validateFn,
			wantErr: errValidationFailed,
		},
		{
			desc: "single deleted file; start key",
			ve: &versionEdit{
				DeletedTables: map[manifest.DeletedTableEntry]*manifest.TableMetadata{
					deletedFileEntry{Level: 0, FileNum: 0}: newFileMeta(
						manifest.InternalKey{UserKey: []byte(badKey)},
						manifest.InternalKey{UserKey: []byte("z")},
					),
				},
			},
			vFunc:   validateFn,
			wantErr: errValidationFailed,
		},
		{
			desc: "single deleted file; end key",
			ve: &versionEdit{
				DeletedTables: map[manifest.DeletedTableEntry]*manifest.TableMetadata{
					deletedFileEntry{Level: 0, FileNum: 0}: newFileMeta(
						manifest.InternalKey{UserKey: []byte("a")},
						manifest.InternalKey{UserKey: []byte(badKey)},
					),
				},
			},
			vFunc:   validateFn,
			wantErr: errValidationFailed,
		},
		{
			desc: "multiple deleted files",
			ve: &versionEdit{
				DeletedTables: map[manifest.DeletedTableEntry]*manifest.TableMetadata{
					deletedFileEntry{Level: 0, FileNum: 0}: newFileMeta(
						manifest.InternalKey{UserKey: []byte("a")},
						manifest.InternalKey{UserKey: []byte("c")},
					),
					deletedFileEntry{Level: 0, FileNum: 1}: newFileMeta(
						manifest.InternalKey{UserKey: []byte(badKey)},
						manifest.InternalKey{UserKey: []byte("z")},
					),
				},
			},
			vFunc:   validateFn,
			wantErr: errValidationFailed,
		},
		{
			desc: "no errors",
			ve: &versionEdit{
				NewTables: []manifest.NewTableEntry{
					{
						Level: 0,
						Meta: newFileMeta(
							manifest.InternalKey{UserKey: []byte("b")},
							manifest.InternalKey{UserKey: []byte("c")},
						),
					},
					{
						Level: 0,
						Meta: newFileMeta(
							manifest.InternalKey{UserKey: []byte("d")},
							manifest.InternalKey{UserKey: []byte("g")},
						),
					},
				},
				DeletedTables: map[manifest.DeletedTableEntry]*manifest.TableMetadata{
					deletedFileEntry{Level: 6, FileNum: 0}: newFileMeta(
						manifest.InternalKey{UserKey: []byte("a")},
						manifest.InternalKey{UserKey: []byte("d")},
					),
					deletedFileEntry{Level: 6, FileNum: 1}: newFileMeta(
						manifest.InternalKey{UserKey: []byte("x")},
						manifest.InternalKey{UserKey: []byte("z")},
					),
				},
			},
			vFunc: validateFn,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.desc, func(t *testing.T) {
			logger := &base.InMemLogger{}
			validateVersionEdit(tc.ve, tc.vFunc, base.DefaultFormatter, logger)
			msg := strings.TrimSpace(logger.String())
			if tc.wantErr != nil {
				if !strings.Contains(msg, tc.wantErr.Error()) {
					t.Fatalf("got: %q; want: %s", msg, tc.wantErr)
				}
			} else if msg != "" {
				t.Fatalf("got %s; wanted no error", msg)
			}
		})
	}
}

// TestCompaction tests compaction mechanics. It is a datadriven test, with
// multiple datadriven test files in the testdata/compaction directory.
func TestCompaction(t *testing.T) {
	var mem vfs.FS
	var d *DB
	defer func() {
		if d != nil {
			require.NoError(t, closeAllSnapshots(d))
			require.NoError(t, d.Close())
		}
	}()

	seed := uint64(time.Now().UnixNano())
	rng := rand.New(rand.NewPCG(0, seed))
	t.Logf("seed: %d", seed)

	randVersion := func(min, max FormatMajorVersion) FormatMajorVersion {
		return FormatMajorVersion(int(min) + rng.IntN(int(max)-int(min)+1))
	}

	var compactionLog bytes.Buffer
	compactionLogEventListener := &EventListener{
		CompactionEnd: func(info CompactionInfo) {
			// Ensure determinism.
			info.JobID = 1
			info.Duration = time.Second
			info.TotalDuration = time.Second
			fmt.Fprintln(&compactionLog, info.String())
		},
	}
	reset := func(minVersion, maxVersion FormatMajorVersion) {
		compactionLog.Reset()
		if d != nil {
			require.NoError(t, closeAllSnapshots(d))
			require.NoError(t, d.Close())
		}
		mem = vfs.NewMem()
		require.NoError(t, mem.MkdirAll("ext", 0755))

		opts := &Options{
			FS:                          mem,
			DebugCheck:                  DebugCheckLevels,
			DisableAutomaticCompactions: true,
			EventListener:               compactionLogEventListener,
			FormatMajorVersion:          randVersion(minVersion, maxVersion),
		}
		opts.WithFSDefaults()
		opts.Experimental.EnableColumnarBlocks = func() bool { return true }
		opts.Experimental.CompactionScheduler = NewConcurrencyLimitSchedulerWithNoPeriodicGrantingForTest()

		var err error
		d, err = Open("", opts)
		require.NoError(t, err)
	}

	// d.mu must be held when calling.
	createOngoingCompaction := func(start, end []byte, startLevel, outputLevel int) (ongoingCompaction *compaction) {
		ongoingCompaction = &compaction{
			inputs:   []compactionLevel{{level: startLevel}, {level: outputLevel}},
			smallest: InternalKey{UserKey: start},
			largest:  InternalKey{UserKey: end},
		}
		ongoingCompaction.startLevel = &ongoingCompaction.inputs[0]
		ongoingCompaction.outputLevel = &ongoingCompaction.inputs[1]
		// Mark files as compacting.
		curr := d.mu.versions.currentVersion()
		ongoingCompaction.startLevel.files = curr.Overlaps(startLevel, base.UserKeyBoundsInclusive(start, end))
		ongoingCompaction.outputLevel.files = curr.Overlaps(outputLevel, base.UserKeyBoundsInclusive(start, end))
		for _, cl := range ongoingCompaction.inputs {
			for f := range cl.files.All() {
				f.CompactionState = manifest.CompactionStateCompacting
			}
		}
		d.mu.compact.inProgress[ongoingCompaction] = struct{}{}
		d.mu.compact.compactingCount++
		return
	}

	// d.mu must be held when calling.
	deleteOngoingCompaction := func(ongoingCompaction *compaction) {
		for _, cl := range ongoingCompaction.inputs {
			for f := range cl.files.All() {
				f.CompactionState = manifest.CompactionStateNotCompacting
			}
		}
		delete(d.mu.compact.inProgress, ongoingCompaction)
		d.mu.compact.compactingCount--
	}

	runTest := func(t *testing.T, testData string, minVersion, maxVersion FormatMajorVersion, verbose bool) {
		reset(minVersion, maxVersion)
		var ongoingCompaction *compaction
		datadriven.RunTest(t, testData, func(t *testing.T, td *datadriven.TestData) string {
			switch td.Cmd {
			case "reset":
				reset(minVersion, maxVersion)
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
				s := describeLSM(d, verbose)
				if td.HasArg("hide-file-num") {
					re := regexp.MustCompile(`([0-9]*):\[`)
					s = re.ReplaceAllString(s, "[")
				}
				if td.HasArg("hide-size") {
					re := regexp.MustCompile(` size:([0-9]*)`)
					s = re.ReplaceAllString(s, "")
				}
				return s

			case "define":
				if d != nil {
					if err := closeAllSnapshots(d); err != nil {
						return err.Error()
					}
					if err := d.Close(); err != nil {
						return err.Error()
					}
				}

				mem = vfs.NewMem()
				opts := &Options{
					FS:                          mem,
					DebugCheck:                  DebugCheckLevels,
					EventListener:               compactionLogEventListener,
					FormatMajorVersion:          randVersion(minVersion, maxVersion),
					DisableAutomaticCompactions: true,
				}
				opts.WithFSDefaults()
				opts.Experimental.EnableColumnarBlocks = func() bool { return true }
				opts.Experimental.CompactionScheduler = NewConcurrencyLimitSchedulerWithNoPeriodicGrantingForTest()

				var err error
				if d, err = runDBDefineCmd(td, opts); err != nil {
					return err.Error()
				}

				s := d.mu.versions.currentVersion().String()
				if verbose {
					s = d.mu.versions.currentVersion().DebugString()
				}
				if td.HasArg("hide-size") {
					re := regexp.MustCompile(` size:([0-9]*)`)
					s = re.ReplaceAllString(s, "")
				}
				return s

			case "file-sizes":
				return runTableFileSizesCmd(td, d)

			case "flush":
				if err := d.Flush(); err != nil {
					return err.Error()
				}
				return describeLSM(d, verbose)

			case "get":
				return runGetCmd(t, td, d)

			case "ingest":
				if err := runIngestCmd(td, d, mem); err != nil {
					return err.Error()
				}
				d.mu.Lock()
				s := d.mu.versions.currentVersion().String()
				if verbose {
					s = d.mu.versions.currentVersion().DebugString()
				}
				d.mu.Unlock()
				return s

			case "iter":
				// TODO(peter): runDBDefineCmd doesn't properly update the visible
				// sequence number. So we have to use a snapshot with a very large
				// sequence number, otherwise the DB appears empty.
				snap := Snapshot{
					db:     d,
					seqNum: base.SeqNumMax,
				}
				iter, _ := snap.NewIter(nil)
				return runIterCmd(td, iter, true)

			case "lsm":
				return runLSMCmd(td, d)

			case "populate":
				b := d.NewBatch()
				runPopulateCmd(t, td, b)
				count := b.Count()
				require.NoError(t, b.Commit(nil))
				return fmt.Sprintf("wrote %d keys\n", count)

			case "auto-compact":
				d.mu.Lock()
				prev := d.opts.DisableAutomaticCompactions
				d.opts.DisableAutomaticCompactions = false
				d.maybeScheduleCompaction()
				for d.mu.compact.compactingCount > 0 {
					d.mu.compact.cond.Wait()
				}
				d.opts.DisableAutomaticCompactions = prev
				d.mu.Unlock()
				return describeLSM(d, verbose)

			case "set-disable-auto-compact":
				var v bool
				td.ScanArgs(t, "v", &v)
				d.mu.Lock()
				d.opts.DisableAutomaticCompactions = v
				d.mu.Unlock()
				return ""

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
					s = d.mu.versions.currentVersion().String()
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
					return nil
				})
				if err != nil {
					return err.Error()
				}

				if manualDone() {
					return "manual compaction did not block for ongoing\n" + s
				}

				d.mu.Lock()
				deleteOngoingCompaction(ongoingCompaction)
				ongoingCompaction = nil
				d.mu.Unlock()
				d.opts.Experimental.CompactionScheduler.(*ConcurrencyLimitScheduler).
					adjustRunningCompactionsForTesting(-1)
				// If the ongoing compaction conflicted with the manual compaction,
				// the CompactionScheduler may believe there is no waiting compaction.
				// So explicitly call maybeScheduleCompaction.
				d.mu.Lock()
				d.maybeScheduleCompaction()
				d.mu.Unlock()
				if err := <-ch; err != nil {
					return err.Error()
				}
				return "manual compaction blocked until ongoing finished\n" + s

			case "async-compact-with-cancellation":
				// Cancels a manual compaction that is blocked by an ongoing
				// compaction. There can be multiple manual compactions created when
				// parallel is specified.
				var s string
				ch := make(chan error, 1)
				// Number of blocked manual compactions.
				var numBlocked int
				td.ScanArgs(t, "num-blocked", &numBlocked)
				var cancelFunc atomic.Pointer[context.CancelFunc]
				go func() {
					compactFunc, cf, err := runCompactCmdAsync(td, d, true)
					if err == nil {
						cancelFunc.Store(&cf)
						err = compactFunc()
					}
					if err != nil {
						ch <- err
						close(ch)
						return
					}
					d.mu.Lock()
					s = d.mu.versions.currentVersion().String()
					d.mu.Unlock()
					close(ch)
				}()
				var compErr error
				var compDone bool
				err := try(100*time.Microsecond, 20*time.Second, func() error {
					select {
					case compErr = <-ch:
						// Unexpected, since compaction should be blocked.
						compDone = true
						return nil
					default:
					}
					if cancelFunc.Load() == nil {
						return errors.New("not yet attempted to run manual compactions")
					}
					d.mu.Lock()
					defer d.mu.Unlock()
					if len(d.mu.compact.manual) != numBlocked {
						return errors.Errorf("expected %d waiting manual compactions, versus actual %d",
							numBlocked, len(d.mu.compact.manual))
					}
					// Expect to be back to the fake ongoing compactions when the
					// non-blocked manual compactions are done.
					if d.mu.compact.compactingCount != 1 {
						return errors.Errorf("expected 1 ongoing compaction, versus actual %d",
							d.mu.compact.compactingCount)
					}
					return nil
				})
				if err != nil {
					return err.Error()
				}
				if compDone {
					return "manual compaction did not block for ongoing\n" + s
				}
				// Cancel the manual compaction.
				(*cancelFunc.Load())()
				// Wait for the cancellation to succeed.
				compErr = <-ch
				if compErr == nil {
					return "manual compaction did not have an error\n" + s
				}
				d.mu.Lock()
				deleteOngoingCompaction(ongoingCompaction)
				ongoingCompaction = nil
				numQueuedManualCompactions := len(d.mu.compact.manual)
				d.mu.Unlock()
				d.opts.Experimental.CompactionScheduler.(*ConcurrencyLimitScheduler).
					adjustRunningCompactionsForTesting(-1)
				return fmt.Sprintf(
					"manual compaction cancelled: %s, current queued compactions: %d\n%s",
					compErr.Error(), numQueuedManualCompactions, s)

			case "add-ongoing-compaction":
				var startLevel int
				var outputLevel int
				var start string
				var end string
				td.ScanArgs(t, "startLevel", &startLevel)
				td.ScanArgs(t, "outputLevel", &outputLevel)
				td.ScanArgs(t, "start", &start)
				td.ScanArgs(t, "end", &end)
				d.mu.Lock()
				ongoingCompaction = createOngoingCompaction([]byte(start), []byte(end), startLevel, outputLevel)
				d.mu.Unlock()
				d.opts.Experimental.CompactionScheduler.(*ConcurrencyLimitScheduler).
					adjustRunningCompactionsForTesting(+1)
				return ""

			case "remove-ongoing-compaction":
				d.mu.Lock()
				deleteOngoingCompaction(ongoingCompaction)
				ongoingCompaction = nil
				d.mu.Unlock()
				d.opts.Experimental.CompactionScheduler.(*ConcurrencyLimitScheduler).
					adjustRunningCompactionsForTesting(-1)

				return ""

			case "set-concurrent-compactions":
				var concurrentCompactions int
				td.ScanArgs(t, "num", &concurrentCompactions)
				d.opts.MaxConcurrentCompactions = func() int {
					return concurrentCompactions
				}
				return ""

			case "sstable-properties":
				return runSSTablePropertiesCmd(t, td, d)

			case "wait-pending-table-stats":
				return runTableStatsCmd(td, d)

			case "close-snapshots":
				d.mu.Lock()
				// Re-enable automatic compactions if they were disabled so that
				// closing snapshots can trigger elision-only compactions if
				// necessary.
				d.opts.DisableAutomaticCompactions = false

				var ss []*Snapshot
				l := &d.mu.snapshots
				for i := l.root.next; i != &l.root; i = i.next {
					ss = append(ss, i)
				}
				d.mu.Unlock()
				for i := range ss {
					if err := ss[i].Close(); err != nil {
						return err.Error()
					}
				}
				return ""

			case "compaction-log":
				defer compactionLog.Reset()
				s := compactionLog.String()
				if td.HasArg("sort") {
					lines := strings.Split(s, "\n")
					sort.Strings(lines)
					// Remove empty lines.
					i := 0
					for ; i < len(lines); i++ {
						if len(lines[i]) != 0 {
							break
						}
					}
					lines = lines[i:]
					s = strings.Join(lines, "\n")
				}
				return s

			default:
				return fmt.Sprintf("unknown command: %s", td.Cmd)
			}
		})
	}

	type testConfig struct {
		minVersion FormatMajorVersion // inclusive, FormatMinSupported if unspecified.
		maxVersion FormatMajorVersion // inclusive, internalFormatNewest if unspecified.
		verbose    bool
	}
	testConfigs := map[string]testConfig{
		"singledel_set_with_del": {},
		"range_keys":             {verbose: true},
		"file_boundaries_delsized": {
			minVersion: FormatDeleteSizedAndObsolete,
			maxVersion: FormatFlushableIngestExcises,
		},
		"set_with_del_sstable_Pebblev4": {
			minVersion: FormatDeleteSizedAndObsolete,
			maxVersion: FormatFlushableIngestExcises,
		},
		"multilevel": {},
		"set_with_del_sstable_Pebblev5": {
			minVersion: FormatColumnarBlocks,
			maxVersion: FormatColumnarBlocks,
		},
		"set_with_del_sstable_Pebblev6": {
			minVersion: FormatTableFormatV6,
			maxVersion: internalFormatNewest,
		},
		"value_separation": {
			minVersion: FormatExperimentalValueSeparation,
			maxVersion: FormatExperimentalValueSeparation,
			verbose:    true,
		},
		"score_compaction_picked_before_manual": {
			// Run at a specific version, so that a single sstable format is used,
			// since the test prints the compaction log which includes file sizes.
			minVersion: FormatExperimentalValueSeparation,
			maxVersion: FormatExperimentalValueSeparation,
		},
		"compaction_cancellation": {
			// Run at a specific version, so that a single sstable format is used,
			// since the test prints the compaction log which includes file sizes.
			minVersion: FormatExperimentalValueSeparation,
			maxVersion: FormatExperimentalValueSeparation,
		},
	}
	datadriven.Walk(t, "testdata/compaction", func(t *testing.T, path string) {
		filename := filepath.Base(path)
		tc, ok := testConfigs[filename]
		if !ok {
			t.Fatalf("unknown test config: %s", filename)
		}
		t.Run(filename, func(t *testing.T) {
			minVersion, maxVersion := tc.minVersion, tc.maxVersion
			if minVersion == 0 {
				minVersion = FormatMinSupported
			}
			if maxVersion == 0 {
				maxVersion = internalFormatNewest
			}
			runTest(t, path, minVersion, maxVersion, tc.verbose)
		})
	})
}

func TestCompactionOutputLevel(t *testing.T) {
	opts := DefaultOptions()
	version := manifest.NewInitialVersion(opts.Comparer)
	l0Organizer := manifest.NewL0Organizer(opts.Comparer, 0 /* flushSplitBytes */)

	datadriven.RunTest(t, "testdata/compaction_output_level",
		func(t *testing.T, d *datadriven.TestData) (res string) {
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
				pc := newPickedCompaction(opts, version, l0Organizer, start, defaultOutputLevel(start, base), base)
				c := newCompaction(pc, opts, time.Now(), nil /* provider */, noopGrantHandle{}, neverSeparateValues)
				return fmt.Sprintf("output=%d\nmax-output-file-size=%d\n",
					c.outputLevel.level, c.maxOutputFileSize)

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
	defer func() {
		if d != nil {
			require.NoError(t, closeAllSnapshots(d))
			require.NoError(t, d.Close())
		}
	}()

	var compactInfo *CompactionInfo // protected by d.mu
	reset := func() (*Options, error) {
		if d != nil {
			compactInfo = nil
			if err := closeAllSnapshots(d); err != nil {
				return nil, err
			}
			if err := d.Close(); err != nil {
				return nil, err
			}
		}
		opts := &Options{
			FS:         vfs.NewMem(),
			DebugCheck: DebugCheckLevels,
			// Collection of table stats can trigger compactions. As we want full
			// control over when compactions are run, disable stats by default.
			DisableTableStats: true,
			EventListener: &EventListener{
				CompactionEnd: func(info CompactionInfo) {
					if compactInfo != nil {
						return
					}
					compactInfo = &info
				},
			},
			FormatMajorVersion: internalFormatNewest,
		}
		opts.WithFSDefaults()
		opts.Experimental.EnableDeleteOnlyCompactionExcises = func() bool { return true }
		opts.Experimental.EnableColumnarBlocks = func() bool { return true }
		return opts, nil
	}

	compactionString := func() string {
		for d.mu.compact.compactingCount > 0 {
			d.mu.compact.cond.Wait()
		}

		s := "(none)"
		if compactInfo != nil {
			// Fix the job ID and durations for determinism.
			compactInfo.JobID = 100
			compactInfo.Duration = time.Second
			compactInfo.TotalDuration = 2 * time.Second
			s = compactInfo.String()
			compactInfo = nil
		}
		return s
	}

	var err error
	var opts *Options
	datadriven.RunTest(t, "testdata/compaction_delete_only_hints",
		func(t *testing.T, td *datadriven.TestData) string {
			switch td.Cmd {
			case "define":
				opts, err = reset()
				if err != nil {
					return err.Error()
				}
				d, err = runDBDefineCmd(td, opts)
				if err != nil {
					return err.Error()
				}
				d.mu.Lock()
				s := d.mu.versions.currentVersion().String()
				d.mu.Unlock()
				return s

			case "force-set-hints":
				d.mu.Lock()
				defer d.mu.Unlock()
				d.mu.compact.deletionHints = d.mu.compact.deletionHints[:0]
				var buf bytes.Buffer
				for _, data := range strings.Split(td.Input, "\n") {
					parts := strings.FieldsFunc(strings.TrimSpace(data),
						func(r rune) bool { return r == '-' || r == ' ' || r == '.' })

					start, end := []byte(parts[2]), []byte(parts[3])

					var tombstoneFile *tableMetadata
					tombstoneLevel := int(parseUint64(parts[0][1:]))

					// Set file number to the value provided in the input.
					tombstoneFile = &tableMetadata{
						FileNum: base.FileNum(parseUint64(parts[1])),
					}

					var hintType deleteCompactionHintType
					switch typ := parts[7]; typ {
					case "point_key_only":
						hintType = deleteCompactionHintTypePointKeyOnly
					case "range_key_only":
						hintType = deleteCompactionHintTypeRangeKeyOnly
					case "point_and_range_key":
						hintType = deleteCompactionHintTypePointAndRangeKey
					default:
						return fmt.Sprintf("unknown hint type: %s", typ)
					}

					h := deleteCompactionHint{
						hintType:                hintType,
						start:                   start,
						end:                     end,
						fileSmallestSeqNum:      base.SeqNum(parseUint64(parts[4])),
						tombstoneLevel:          tombstoneLevel,
						tombstoneFile:           tombstoneFile,
						tombstoneSmallestSeqNum: base.SeqNum(parseUint64(parts[5])),
						tombstoneLargestSeqNum:  base.SeqNum(parseUint64(parts[6])),
					}
					d.mu.compact.deletionHints = append(d.mu.compact.deletionHints, h)
					fmt.Fprintln(&buf, h.String())
				}
				return buf.String()

			case "get-hints":
				d.mu.Lock()
				defer d.mu.Unlock()

				// Force collection of table stats. This requires re-enabling the
				// collection flag. We also do not want compactions to run as part of
				// the stats collection job, so we disable it temporarily.
				d.opts.DisableTableStats = false
				d.opts.DisableAutomaticCompactions = true
				defer func() {
					d.opts.DisableTableStats = true
					d.opts.DisableAutomaticCompactions = false
				}()

				// NB: collectTableStats attempts to acquire the lock. Temporarily
				// unlock here to avoid a deadlock.
				d.mu.Unlock()
				didRun := d.collectTableStats()
				d.mu.Lock()

				if !didRun {
					// If a job was already running, wait for the results.
					d.waitTableStats()
				}

				hints := d.mu.compact.deletionHints
				if len(hints) == 0 {
					return "(none)"
				}
				var buf bytes.Buffer
				for _, h := range hints {
					buf.WriteString(h.String() + "\n")
				}
				return buf.String()

			case "maybe-compact":
				d.mu.Lock()
				d.maybeScheduleCompaction()

				var buf bytes.Buffer
				fmt.Fprintf(&buf, "Deletion hints:\n")
				for _, h := range d.mu.compact.deletionHints {
					fmt.Fprintf(&buf, "  %s\n", h.String())
				}
				if len(d.mu.compact.deletionHints) == 0 {
					fmt.Fprintf(&buf, "  (none)\n")
				}
				fmt.Fprintf(&buf, "Compactions:\n")
				fmt.Fprintf(&buf, "  %s", compactionString())
				d.mu.Unlock()
				return buf.String()

			case "compact":
				if err := runCompactCmd(td, d); err != nil {
					return err.Error()
				}
				d.mu.Lock()
				compactInfo = nil
				s := d.mu.versions.currentVersion().String()
				d.mu.Unlock()
				return s

			case "close-snapshot":
				seqNum := base.ParseSeqNum(strings.TrimSpace(td.Input))
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

			case "iter":
				snap := Snapshot{
					db:     d,
					seqNum: base.SeqNumMax,
				}
				iter, _ := snap.NewIter(nil)
				return runIterCmd(td, iter, true)

			case "reset":
				opts, err = reset()
				if err != nil {
					return err.Error()
				}
				d, err = Open("", opts)
				if err != nil {
					return err.Error()
				}
				return ""

			case "ingest":
				if err = runBuildCmd(td, d, d.opts.FS); err != nil {
					return err.Error()
				}
				if err = runIngestCmd(td, d, d.opts.FS); err != nil {
					return err.Error()
				}
				return "OK"

			case "describe-lsm":
				d.mu.Lock()
				s := d.mu.versions.currentVersion().String()
				d.mu.Unlock()
				return s

			default:
				return fmt.Sprintf("unknown command: %s", td.Cmd)
			}
		})
}

func TestCompactionTombstones(t *testing.T) {
	var d *DB
	defer func() {
		if d != nil {
			require.NoError(t, closeAllSnapshots(d))
			require.NoError(t, d.Close())
		}
	}()

	var compactInfo []*CompactionInfo // protected by d.mu

	compactionString := func() string {
		for d.mu.compact.compactingCount > 0 {
			d.mu.compact.cond.Wait()
		}

		if len(compactInfo) == 0 {
			return "(none)"
		}
		for _, c := range compactInfo {
			// Fix the job ID and durations for determinism.
			c.JobID = 0
			c.Duration = time.Second
			c.TotalDuration = 2 * time.Second
			if len(compactInfo) > 1 {
				// The output table numbering is non-deterministic when there are
				// multiple concurrent compactions.
				for i := range c.Output.Tables {
					c.Output.Tables[i].FileNum = 0
				}
			}
		}
		// Sort for determinism. We use the negative value of cmp.Compare to sort
		// delete-only before default since it is more natural in the output, as
		// delete-only is selected first.
		slices.SortFunc(compactInfo, func(a, b *CompactionInfo) int {
			return -cmp.Compare(a.String(), b.String())
		})
		var b strings.Builder
		jobID := 100
		for _, c := range compactInfo {
			// Fix the job ID.
			c.JobID = jobID
			jobID++
			fmt.Fprintf(&b, "%s\n", c.String())
		}
		compactInfo = nil
		return b.String()
	}

	datadriven.RunTest(t, "testdata/compaction_tombstones",
		func(t *testing.T, td *datadriven.TestData) string {
			switch td.Cmd {
			case "define":
				if d != nil {
					require.NoError(t, closeAllSnapshots(d))
					if err := d.Close(); err != nil {
						return err.Error()
					}
					compactInfo = nil
				}
				opts := &Options{
					FS:         vfs.NewMem(),
					DebugCheck: DebugCheckLevels,
					EventListener: &EventListener{
						CompactionEnd: func(info CompactionInfo) {
							compactInfo = append(compactInfo, &info)
						},
					},
					FormatMajorVersion: internalFormatNewest,
				}
				opts.WithFSDefaults()
				opts.Experimental.EnableDeleteOnlyCompactionExcises = func() bool { return true }
				opts.Experimental.EnableColumnarBlocks = func() bool { return true }
				opts.Experimental.CompactionScheduler = NewConcurrencyLimitSchedulerWithNoPeriodicGrantingForTest()
				var err error
				d, err = runDBDefineCmd(td, opts)
				if err != nil {
					return err.Error()
				}
				d.mu.Lock()
				s := d.mu.versions.currentVersion().String()
				d.mu.Unlock()
				return s

			case "maybe-compact":
				d.mu.Lock()
				d.opts.DisableAutomaticCompactions = false
				d.maybeScheduleCompaction()
				s := compactionString()
				d.mu.Unlock()
				return s

			case "wait-pending-table-stats":
				return runTableStatsCmd(td, d)

			case "close-snapshot":
				seqNum := base.ParseSeqNum(strings.TrimSpace(td.Input))
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

			case "close":
				if err := d.Close(); err != nil {
					return err.Error()
				}
				d = nil
				return ""

			case "version":
				d.mu.Lock()
				s := d.mu.versions.currentVersion().String()
				d.mu.Unlock()
				return s

			default:
				return fmt.Sprintf("unknown command: %s", td.Cmd)
			}
		})
}

func closeAllSnapshots(d *DB) error {
	d.mu.Lock()
	var ss []*Snapshot
	l := &d.mu.snapshots
	for i := l.root.next; i != &l.root; i = i.next {
		ss = append(ss, i)
	}
	d.mu.Unlock()
	for i := range ss {
		if err := ss[i].Close(); err != nil {
			return err
		}
	}
	return nil
}

func TestCompactionReadTriggeredQueue(t *testing.T) {

	// Convert a read compaction to a string which this test
	// understands.
	showRC := func(rc *readCompaction) string {
		return fmt.Sprintf(
			"L%d: %s-%s %d\n", rc.level, string(rc.start), string(rc.end), rc.fileNum,
		)
	}

	var queue *readCompactionQueue

	datadriven.RunTest(t, "testdata/read_compaction_queue",
		func(t *testing.T, td *datadriven.TestData) string {
			switch td.Cmd {
			case "create":
				queue = &readCompactionQueue{}
				return "(success)"
			case "add-compaction":
				for _, line := range crstrings.Lines(td.Input) {
					parts := strings.Split(line, " ")

					if len(parts) != 3 {
						return "error: malformed data for add-compaction. usage: <level>: <start>-<end> <filenum>"
					}
					if l, err := strconv.Atoi(parts[0][1:2]); err == nil {
						keys := strings.Split(parts[1], "-")
						fileNum, _ := strconv.Atoi(parts[2])
						rc := readCompaction{
							level:   l,
							start:   []byte(keys[0]),
							end:     []byte(keys[1]),
							fileNum: base.FileNum(fileNum),
						}
						queue.add(&rc, DefaultComparer.Compare)
					} else {
						return err.Error()
					}
				}
				return ""
			case "remove-compaction":
				rc := queue.remove()
				if rc == nil {
					return "(nil)"
				}
				return showRC(rc)
			case "print-size":
				// Print the size of the queue.
				return fmt.Sprintf("%d", queue.size)
			case "print-queue":
				// Print each element of the queue on a separate line.
				var sb strings.Builder
				if queue.size == 0 {
					sb.WriteString("(empty)")
				}

				for i := 0; i < queue.size; i++ {
					rc := queue.at(i)
					sb.WriteString(showRC(rc))
				}
				return sb.String()
			default:
				return fmt.Sprintf("unknown command: %s", td.Cmd)
			}
		},
	)
}

func (qu *readCompactionQueue) at(i int) *readCompaction {
	if i >= qu.size {
		return nil
	}

	return qu.queue[i]
}

func TestCompactionReadTriggered(t *testing.T) {
	var d *DB
	defer func() {
		if d != nil {
			require.NoError(t, d.Close())
		}
	}()

	var compactInfo *CompactionInfo // protected by d.mu

	compactionString := func() string {
		for d.mu.compact.compactingCount > 0 {
			d.mu.compact.cond.Wait()
		}

		s := "(none)"
		if compactInfo != nil {
			// Fix the job ID and durations for determinism.
			compactInfo.JobID = 100
			compactInfo.Duration = time.Second
			compactInfo.TotalDuration = 2 * time.Second
			s = compactInfo.String()
			compactInfo = nil
		}
		return s
	}

	datadriven.RunTest(t, "testdata/compaction_read_triggered",
		func(t *testing.T, td *datadriven.TestData) string {
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
					EventListener: &EventListener{
						CompactionEnd: func(info CompactionInfo) {
							compactInfo = &info
						},
					},
				}
				opts.WithFSDefaults()
				var err error
				d, err = runDBDefineCmd(td, opts)
				if err != nil {
					return err.Error()
				}
				d.mu.Lock()
				s := d.mu.versions.currentVersion().String()
				d.mu.Unlock()
				return s

			case "add-read-compaction":
				d.mu.Lock()
				td.MaybeScanArgs(t, "flushing", &d.mu.compact.flushing)
				for _, line := range crstrings.Lines(td.Input) {
					parts := strings.Split(line, " ")
					if len(parts) != 3 {
						return "error: malformed data for add-read-compaction. usage: <level>: <start>-<end> <filenum>"
					}
					if l, err := strconv.Atoi(parts[0][:1]); err == nil {
						keys := strings.Split(parts[1], "-")
						fileNum, _ := strconv.Atoi(parts[2])
						rc := readCompaction{
							level:   l,
							start:   []byte(keys[0]),
							end:     []byte(keys[1]),
							fileNum: base.FileNum(fileNum),
						}
						d.mu.compact.readCompactions.add(&rc, DefaultComparer.Compare)
					} else {
						return err.Error()
					}
				}
				d.mu.Unlock()
				return ""

			case "show-read-compactions":
				d.mu.Lock()
				var sb strings.Builder
				if d.mu.compact.readCompactions.size == 0 {
					sb.WriteString("(none)")
				}
				for i := 0; i < d.mu.compact.readCompactions.size; i++ {
					rc := d.mu.compact.readCompactions.at(i)
					sb.WriteString(fmt.Sprintf("(level: %d, start: %s, end: %s)\n", rc.level, string(rc.start), string(rc.end)))
				}
				d.mu.Unlock()
				return sb.String()

			case "maybe-compact":
				d.mu.Lock()
				d.opts.DisableAutomaticCompactions = false
				d.maybeScheduleCompaction()
				s := compactionString()
				d.mu.Unlock()
				return s

			case "version":
				d.mu.Lock()
				s := d.mu.versions.currentVersion().String()
				d.mu.Unlock()
				return s

			default:
				return fmt.Sprintf("unknown command: %s", td.Cmd)
			}
		})
}

func TestCompactionAllowZeroSeqNum(t *testing.T) {
	var d *DB
	defer func() {
		if d != nil {
			require.NoError(t, closeAllSnapshots(d))
			require.NoError(t, d.Close())
		}
	}()

	metaRE := regexp.MustCompile(`^L([0-9]+):([^-]+)-(.+)$`)
	var fileNum base.FileNum
	parseMeta := func(s string) (level int, meta *tableMetadata) {
		match := metaRE.FindStringSubmatch(s)
		if match == nil {
			t.Fatalf("malformed table spec: %s", s)
		}
		level, err := strconv.Atoi(match[1])
		if err != nil {
			t.Fatalf("malformed table spec: %s: %s", s, err)
		}
		fileNum++
		meta = (&tableMetadata{
			FileNum: fileNum,
		}).ExtendPointKeyBounds(
			d.cmp,
			InternalKey{UserKey: []byte(match[2])},
			InternalKey{UserKey: []byte(match[3])},
		)
		meta.InitPhysicalBacking()
		return level, meta
	}

	datadriven.RunTest(t, "testdata/compaction_allow_zero_seqnum",
		func(t *testing.T, td *datadriven.TestData) string {
			switch td.Cmd {
			case "define":
				if d != nil {
					require.NoError(t, closeAllSnapshots(d))
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
					cmp:      d.cmp,
					comparer: d.opts.Comparer,
					version:  d.mu.versions.currentVersion(),
					inputs:   []compactionLevel{{}, {}},
				}
				c.startLevel, c.outputLevel = &c.inputs[0], &c.inputs[1]
				d.mu.Unlock()

				var buf bytes.Buffer
				for _, line := range crstrings.Lines(td.Input) {
					parts := strings.Fields(line)
					c.flushing = nil
					c.startLevel.level = -1

					var startFiles, outputFiles []*tableMetadata

					switch {
					case len(parts) == 1 && parts[0] == "flush":
						c.outputLevel.level = 0
						d.mu.Lock()
						c.flushing = d.mu.mem.queue
						d.mu.Unlock()

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
						c.startLevel.files.All(),
						c.outputLevel.files.All())

					c.delElision, c.rangeKeyElision = compact.SetupTombstoneElision(
						c.cmp, c.version, d.mu.versions.l0Organizer, c.outputLevel.level, base.UserKeyBoundsFromInternal(c.smallest, c.largest),
					)
					fmt.Fprintf(&buf, "%t\n", c.allowZeroSeqNum())
				}
				return buf.String()

			default:
				return fmt.Sprintf("unknown command: %s", td.Cmd)
			}
		})
}

func TestCompactionErrorOnUserKeyOverlap(t *testing.T) {
	cmp := DefaultComparer.Compare
	parseMeta := func(s string) *tableMetadata {
		parts := strings.Split(s, "-")
		if len(parts) != 2 {
			t.Fatalf("malformed table spec: %s", s)
		}
		m := (&tableMetadata{}).ExtendPointKeyBounds(
			cmp,
			base.ParseInternalKey(strings.TrimSpace(parts[0])),
			base.ParseInternalKey(strings.TrimSpace(parts[1])),
		)
		m.SmallestSeqNum = m.Smallest().SeqNum()
		m.LargestSeqNum = m.Largest().SeqNum()
		m.LargestSeqNumAbsolute = m.LargestSeqNum
		m.InitPhysicalBacking()
		return m
	}

	datadriven.RunTest(t, "testdata/compaction_error_on_user_key_overlap",
		func(t *testing.T, d *datadriven.TestData) string {
			switch d.Cmd {
			case "error-on-user-key-overlap":
				c := &compaction{
					cmp:       DefaultComparer.Compare,
					comparer:  DefaultComparer,
					formatKey: DefaultComparer.FormatKey,
				}
				var files []manifest.NewTableEntry
				fileNum := base.FileNum(1)

				for _, data := range strings.Split(d.Input, "\n") {
					meta := parseMeta(data)
					meta.FileNum = fileNum
					fileNum++
					files = append(files, manifest.NewTableEntry{Level: 1, Meta: meta})
				}

				result := "OK"
				ve := &versionEdit{
					NewTables: files,
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
		tablesCreated    []base.DiskFileNum
	)

	mem := vfs.NewMem()
	ii := errorfs.OnIndex(math.MaxInt32) // start disabled
	opts := &Options{
		FS:     errorfs.Wrap(mem, errorfs.ErrInjected.If(ii)),
		Levels: make([]LevelOptions, numLevels),
		EventListener: &EventListener{
			TableCreated: func(info TableCreateInfo) {
				t.Log(info)

				// If the initial setup is over, record tables created and
				// inject an error immediately after the second table is
				// created.
				if initialSetupDone {
					tablesCreated = append(tablesCreated, info.FileNum)
					if len(tablesCreated) >= 2 {
						ii.Store(0)
					}
				}
			},
		},
	}
	opts.WithFSDefaults()
	for i := range opts.Levels {
		opts.Levels[i].TargetFileSize = 1
	}
	opts.testingRandomized(t)
	d, err := Open("", opts)
	require.NoError(t, err)

	ingest := func(keys ...string) {
		t.Helper()
		f, err := mem.Create("ext", vfs.WriteCategoryUnspecified)
		require.NoError(t, err)

		w := sstable.NewWriter(objstorageprovider.NewFileWritable(f), sstable.WriterOptions{
			TableFormat: d.TableFormat(),
		})
		for _, k := range keys {
			require.NoError(t, w.Set([]byte(k), nil))
		}
		require.NoError(t, w.Close())
		require.NoError(t, d.Ingest(context.Background(), []string{"ext"}))
	}
	ingest("a", "c")
	ingest("b")

	// Trigger a manual compaction, which will encounter an injected error
	// after the second table is created.
	d.mu.Lock()
	initialSetupDone = true
	d.mu.Unlock()
	err = d.Compact(context.Background(), []byte("a"), []byte("d"), false)
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
	cmp := DefaultComparer.Compare
	parseMeta := func(s string) *tableMetadata {
		parts := strings.Split(s, "-")
		if len(parts) != 2 {
			t.Fatalf("malformed table spec: %s", s)
		}
		m := (&tableMetadata{}).ExtendPointKeyBounds(
			cmp,
			base.ParseInternalKey(strings.TrimSpace(parts[0])),
			base.ParseInternalKey(strings.TrimSpace(parts[1])),
		)
		m.SmallestSeqNum = m.Smallest().SeqNum()
		m.LargestSeqNum = m.Largest().SeqNum()
		m.LargestSeqNumAbsolute = m.LargestSeqNum
		m.InitPhysicalBacking()
		return m
	}

	datadriven.RunTest(t, "testdata/compaction_check_ordering",
		func(t *testing.T, d *datadriven.TestData) string {
			switch d.Cmd {
			case "check-ordering":
				c := &compaction{
					cmp:       DefaultComparer.Compare,
					comparer:  DefaultComparer,
					formatKey: DefaultComparer.FormatKey,
					logger:    panicLogger{},
					inputs:    []compactionLevel{{level: -1}, {level: -1}},
				}
				c.startLevel, c.outputLevel = &c.inputs[0], &c.inputs[1]
				var startFiles, outputFiles []*tableMetadata
				var sublevels []manifest.LevelSlice
				var files *[]*tableMetadata
				var sublevel []*tableMetadata
				var sublevelNum int
				var parsingSublevel bool
				fileNum := base.FileNum(1)

				switchSublevel := func() {
					if sublevel != nil {
						sublevels = append(
							sublevels, manifest.NewLevelSliceSpecificOrder(sublevel),
						)
						sublevel = nil
					}
					parsingSublevel = false
				}

				for _, data := range strings.Split(d.Input, "\n") {
					if data[0] == 'L' && len(data) == 4 {
						// Format L0.{sublevel}.
						switchSublevel()
						level, err := strconv.Atoi(data[1:2])
						if err != nil {
							return err.Error()
						}
						sublevelNum, err = strconv.Atoi(data[3:])
						if err != nil {
							return err.Error()
						}
						if c.startLevel.level == -1 {
							c.startLevel.level = level
							files = &startFiles
						}
						parsingSublevel = true
					} else if data[0] == 'L' {
						switchSublevel()
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
					} else {
						meta := parseMeta(data)
						meta.FileNum = fileNum
						fileNum++
						*files = append(*files, meta)
						if parsingSublevel {
							meta.SubLevel = sublevelNum
							sublevel = append(sublevel, meta)
						}
					}
				}

				switchSublevel()
				c.startLevel.files = manifest.NewLevelSliceSpecificOrder(startFiles)
				c.outputLevel.files = manifest.NewLevelSliceSpecificOrder(outputFiles)
				if c.outputLevel.level == -1 {
					c.outputLevel.level = 0
				}
				if c.startLevel.level == 0 {
					// We don't change the input files for the compaction beyond this point.
					c.startLevel.l0SublevelInfo = generateSublevelInfo(c.cmp, c.startLevel.files)
				}

				newIters := func(
					_ context.Context, _ *manifest.TableMetadata, _ *IterOptions, _ internalIterOpts, _ iterKinds,
				) (iterSet, error) {
					return iterSet{point: &errorIter{}}, nil
				}
				result := "OK"
				_, _, _, err := c.newInputIters(newIters, nil, internalIterOpts{})
				if err != nil {
					result = fmt.Sprint(err)
				}
				return result

			default:
				return fmt.Sprintf("unknown command: %s", d.Cmd)
			}
		})
}

func TestCompactFlushQueuedMemTableAndFlushMetrics(t *testing.T) {
	t.Run("", func(t *testing.T) {
		// Verify that manual compaction forces a flush of a queued memtable.

		mem := vfs.NewMem()
		opts := &Options{
			FS: mem,
		}
		opts.WithFSDefaults()
		d, err := Open("", testingRandomized(t, opts))
		require.NoError(t, err)

		// Add the key "a" to the memtable, then fill up the memtable with the key
		// prefix "b". The compaction will only overlap with the queued memtable,
		// not the mutable memtable.
		// NB: The initial memtable size is 256KB, which is filled up with random
		// values which typically don't compress well. The test also appends the
		// random value to the "b" key to limit overwriting of the same key, which
		// would get collapsed at flush time since there are no open snapshots.
		value := make([]byte, 50)
		_, err = crand.Read(value)
		require.NoError(t, err)
		require.NoError(t, d.Set([]byte("a"), value, nil))
		for {
			_, err = crand.Read(value)
			require.NoError(t, err)
			require.NoError(t, d.Set(append([]byte("b"), value...), value, nil))
			d.mu.Lock()
			done := len(d.mu.mem.queue) == 2
			d.mu.Unlock()
			if done {
				break
			}
		}

		require.NoError(t, d.Compact(context.Background(), []byte("a"), []byte("a\x00"), false))
		d.mu.Lock()
		require.Equal(t, 1, len(d.mu.mem.queue))
		d.mu.Unlock()
		// Flush metrics are updated after and non-atomically with the memtable
		// being removed from the queue.
		for begin := time.Now(); ; {
			metrics := d.Metrics()
			require.NotNil(t, metrics)
			if metrics.Flush.WriteThroughput.Bytes >= 50*1024 {
				// The writes (during which the flush is idle) and the flush work
				// should not be so fast as to be unrealistic. If these turn out to be
				// flaky we could instead inject a clock.
				tinyInterval := 50 * time.Microsecond
				testutils.DurationIsAtLeast(t, metrics.Flush.WriteThroughput.WorkDuration, tinyInterval)
				testutils.DurationIsAtLeast(t, metrics.Flush.WriteThroughput.IdleDuration, tinyInterval)
				break
			}
			if time.Since(begin) > 2*time.Second {
				t.Fatal("flush did not happen")
			}
			time.Sleep(time.Millisecond)
		}
		require.NoError(t, d.Close())
	})
}

func TestCompactFlushQueuedLargeBatch(t *testing.T) {
	// Verify that compaction forces a flush of a queued large batch.

	mem := vfs.NewMem()
	opts := &Options{
		FS: mem,
	}
	opts.WithFSDefaults()
	d, err := Open("", testingRandomized(t, opts))
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
	require.NoError(t, d.Set([]byte("a"), bytes.Repeat([]byte("v"), int(d.largeBatchThreshold)), nil))
	d.mu.Lock()
	require.Greater(t, len(d.mu.mem.queue), 1)
	d.mu.Unlock()

	require.NoError(t, d.Compact(context.Background(), []byte("a"), []byte("a\x00"), false))
	d.mu.Lock()
	require.Equal(t, 1, len(d.mu.mem.queue))
	d.mu.Unlock()

	require.NoError(t, d.Close())
}

func TestFlushError(t *testing.T) {
	// Error the first five times we try to write a sstable.
	var errorOps atomic.Int32
	errorOps.Store(3)
	fs := errorfs.Wrap(vfs.NewMem(), errorfs.InjectorFunc(func(op errorfs.Op) error {
		if op.Kind == errorfs.OpCreate && filepath.Ext(op.Path) == ".sst" && errorOps.Add(-1) >= 0 {
			return errorfs.ErrInjected
		}
		return nil
	}))
	opts := &Options{
		FS: fs,
		EventListener: &EventListener{
			BackgroundError: func(err error) {
				t.Log(err)
			},
		},
	}
	opts.WithFSDefaults()
	d, err := Open("", testingRandomized(t, opts))
	require.NoError(t, err)
	require.NoError(t, d.Set([]byte("a"), []byte("foo"), NoSync))
	require.NoError(t, d.Flush())
	require.NoError(t, d.Close())
}

func TestAdjustGrandparentOverlapBytesForFlush(t *testing.T) {
	// 500MB in Lbase
	var lbaseFiles []*manifest.TableMetadata
	const lbaseSize = 5 << 20
	for i := 0; i < 100; i++ {
		m := &manifest.TableMetadata{Size: lbaseSize, FileNum: base.FileNum(i)}
		m.InitPhysicalBacking()
		lbaseFiles =
			append(lbaseFiles, m)
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
	opts := &Options{
		FS: vfs.NewMem(),
	}
	opts.WithFSDefaults()
	db, err := Open("", testingRandomized(t, opts))
	require.NoError(t, err)
	defer db.Close()
	require.NoError(t, db.Compact(context.Background(), []byte("a"), []byte("b"), false))
	require.Error(t, db.Compact(context.Background(), []byte("a"), []byte("a"), false))
	require.Error(t, db.Compact(context.Background(), []byte("b"), []byte("a"), false))
}

func TestMarkedForCompaction(t *testing.T) {
	var mem vfs.FS = vfs.NewMem()
	var d *DB
	defer func() {
		if d != nil {
			require.NoError(t, d.Close())
		}
	}()

	var buf bytes.Buffer
	opts := &Options{
		FS:                          mem,
		DebugCheck:                  DebugCheckLevels,
		DisableAutomaticCompactions: true,
		FormatMajorVersion:          internalFormatNewest,
		EventListener: &EventListener{
			CompactionEnd: func(info CompactionInfo) {
				// Fix the job ID and durations for determinism.
				info.JobID = 100
				info.Duration = time.Second
				info.TotalDuration = 2 * time.Second
				fmt.Fprintln(&buf, info)
			},
		},
	}
	opts.WithFSDefaults()
	opts.Experimental.EnableColumnarBlocks = func() bool { return true }

	reset := func() {
		if d != nil {
			require.NoError(t, d.Close())
		}
		mem = vfs.NewMem()
		require.NoError(t, mem.MkdirAll("ext", 0755))

		var err error
		d, err = Open("", opts)
		require.NoError(t, err)
	}
	datadriven.RunTest(t, "testdata/marked_for_compaction", func(t *testing.T, td *datadriven.TestData) string {
		switch td.Cmd {
		case "reset":
			reset()
			return ""

		case "define":
			if d != nil {
				if err := d.Close(); err != nil {
					return err.Error()
				}
			}
			var err error
			if d, err = runDBDefineCmd(td, opts); err != nil {
				return err.Error()
			}
			d.mu.Lock()
			defer d.mu.Unlock()
			t := time.Now()
			d.timeNow = func() time.Time {
				t = t.Add(time.Second)
				return t
			}
			s := d.mu.versions.currentVersion().DebugString()
			return s

		case "mark-for-compaction":
			d.mu.Lock()
			defer d.mu.Unlock()
			vers := d.mu.versions.currentVersion()
			var fileNum uint64
			td.ScanArgs(t, "file", &fileNum)
			for l, lm := range vers.Levels {
				for f := range lm.All() {
					if f.FileNum != base.FileNum(fileNum) {
						continue
					}
					f.MarkedForCompaction = true
					vers.Stats.MarkedForCompaction++
					markedForCompactionAnnotator.InvalidateLevelAnnotation(vers.Levels[l])
					return fmt.Sprintf("marked L%d.%s", l, f.FileNum)
				}
			}
			return "not-found"

		case "maybe-compact":
			d.mu.Lock()
			defer d.mu.Unlock()
			d.opts.DisableAutomaticCompactions = false
			d.maybeScheduleCompaction()
			for d.mu.compact.compactingCount > 0 {
				d.mu.compact.cond.Wait()
			}

			fmt.Fprintln(&buf, d.mu.versions.currentVersion().DebugString())
			s := strings.TrimSpace(buf.String())
			buf.Reset()
			opts.DisableAutomaticCompactions = true
			return s

		default:
			return fmt.Sprintf("unknown command: %s", td.Cmd)
		}
	})
}

// createManifestErrorInjector injects errors (when enabled) into vfs.FS calls
// to create MANIFEST files.
type createManifestErrorInjector struct {
	enabled atomic.Bool
}

// TODO(jackson): Replace the createManifestErrorInjector with the composition
// of primitives defined in errorfs. This may require additional primitives.

func (i *createManifestErrorInjector) String() string { return "MANIFEST-Creates" }

// enable enables error injection for the vfs.FS.
func (i *createManifestErrorInjector) enable() {
	i.enabled.Store(true)
}

// MaybeError implements errorfs.Injector.
func (i *createManifestErrorInjector) MaybeError(op errorfs.Op) error {
	if !i.enabled.Load() {
		return nil
	}
	// This necessitates having a MaxManifestSize of 1, to reliably induce
	// UpdateVersionLocked errors.
	if strings.Contains(op.Path, "MANIFEST") && op.Kind == errorfs.OpCreate {
		return errorfs.ErrInjected
	}
	return nil
}

var _ errorfs.Injector = &createManifestErrorInjector{}

// TestCompaction_UpdateVersionFails exercises a flush or ingest encountering an
// unrecoverable error during UpdateVersionLocked.
//
// Regression test for #1669.
func TestCompaction_UpdateVersionFails(t *testing.T) {
	// flushKeys writes the given keys to the DB, flushing the resulting memtable.
	var key = []byte("foo")
	flushErrC := make(chan error)
	flushKeys := func(db *DB) error {
		b := db.NewBatch()
		err := b.Set(key, nil, nil)
		require.NoError(t, err)
		err = b.Commit(nil)
		require.NoError(t, err)
		// An error from a failing flush is returned asynchronously.
		go func() { _ = db.Flush() }()
		return <-flushErrC
	}

	// ingestKeys adds the given keys to the DB via an ingestion.
	ingestKeys := func(db *DB) error {
		// Create an SST for ingestion.
		const fName = "ext"
		f, err := db.opts.FS.Create(fName, vfs.WriteCategoryUnspecified)
		require.NoError(t, err)
		w := sstable.NewWriter(objstorageprovider.NewFileWritable(f), sstable.WriterOptions{})
		require.NoError(t, w.Set(key, nil))
		require.NoError(t, w.Close())
		// Ingest the SST.
		return db.Ingest(context.Background(), []string{fName})
	}

	testCases := []struct {
		name              string
		addFn             func(db *DB) error
		backgroundErrorFn func(*DB, error)
	}{
		{
			name:  "flush",
			addFn: flushKeys,
			backgroundErrorFn: func(db *DB, err error) {
				require.True(t, errors.Is(err, errorfs.ErrInjected))
				flushErrC <- err
				// A flush will attempt to retry in the background. For the purposes of
				// testing this particular scenario, where we would have crashed anyway,
				// drop the memtable on the floor to short circuit the retry loop.
				// NB: we hold db.mu here.
				var cur *flushableEntry
				cur, db.mu.mem.queue = db.mu.mem.queue[0], db.mu.mem.queue[1:]
				cur.readerUnrefLocked(true)
			},
		},
		{
			name:  "ingest",
			addFn: ingestKeys,
		},
	}

	runTest := func(t *testing.T, addFn func(db *DB) error, bgFn func(*DB, error)) {
		var db *DB
		inj := &createManifestErrorInjector{}
		logger := &fatalCapturingLogger{t: t}
		opts := &Options{
			FS: errorfs.Wrap(vfs.NewMem(), inj),
			// Rotate the manifest after each write. This is required to trigger a
			// file creation, into which errors can be injected.
			MaxManifestFileSize: 1,
			Logger:              logger,
			EventListener: &EventListener{
				BackgroundError: func(err error) {
					if bgFn != nil {
						bgFn(db, err)
					}
				},
			},
			DisableAutomaticCompactions: true,
		}
		opts.WithFSDefaults()

		db, err := Open("", opts)
		require.NoError(t, err)
		defer func() { _ = db.Close() }()

		inj.enable()
		err = addFn(db)
		require.True(t, errors.Is(err, errorfs.ErrInjected))

		// Under normal circumstances, such an error in UpdateVersionLocked would
		// panic and cause the DB to terminate here. Assert that we captured the
		// fatal error.
		require.True(t, errors.Is(logger.err, errorfs.ErrInjected))
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			runTest(t, tc.addFn, tc.backgroundErrorFn)
		})
	}
}

// TestSharedObjectDeletePacing tests that we don't throttle shared object
// deletes (see the TargetBytesDeletionRate option).
func TestSharedObjectDeletePacing(t *testing.T) {
	var opts Options
	opts.FS = vfs.NewMem()
	opts.Experimental.RemoteStorage = remote.MakeSimpleFactory(map[remote.Locator]remote.Storage{
		"": remote.NewInMem(),
	})
	opts.Experimental.CreateOnShared = remote.CreateOnSharedAll
	opts.TargetByteDeletionRate = 1
	opts.Logger = testLogger{t}

	d, err := Open("", &opts)
	require.NoError(t, err)
	require.NoError(t, d.SetCreatorID(1))

	randVal := func() []byte {
		res := make([]byte, 1024)
		_, err := crand.Read(res)
		require.NoError(t, err)
		return res
	}

	// We must set up things so that we will have more live bytes than obsolete
	// bytes, otherwise delete pacing will be disabled anyway.
	key := func(i int) string {
		return fmt.Sprintf("k%02d", i)
	}
	const numKeys = 20
	for i := 1; i <= numKeys; i++ {
		require.NoError(t, d.Set([]byte(key(i)), randVal(), nil))
		require.NoError(t, d.Compact(context.Background(), []byte(key(i)), []byte(key(i)+"1"), false))
	}

	done := make(chan struct{})
	go func() {
		err = d.DeleteRange([]byte(key(5)), []byte(key(9)), nil)
		if err == nil {
			err = d.Compact(context.Background(), []byte(key(5)), []byte(key(9)), false)
		}
		// Wait for objects to be deleted.
		for {
			time.Sleep(10 * time.Millisecond)
			if len(d.objProvider.List()) < numKeys-2 {
				break
			}
		}
		close(done)
	}()

	select {
	case <-time.After(60 * time.Second):
		// Don't close the DB in this case (the goroutine above might panic).
		t.Fatalf("compaction timed out, possibly due to incorrect deletion pacing")
	case <-done:
	}
	require.NoError(t, err)
	d.Close()
}

type WriteErrorInjector struct {
	enabled atomic.Bool
}

// TODO(jackson): Replace WriteErrorInjector with use of primitives in errorfs,
// adding new primitives as necessary.

func (i *WriteErrorInjector) String() string { return "FileWrites(ErrInjected)" }

// enable enables error injection for the vfs.FS.
func (i *WriteErrorInjector) enable() {
	i.enabled.Store(true)
}

// disable disabled error injection for the vfs.FS.
func (i *WriteErrorInjector) disable() {
	i.enabled.Store(false)
}

// MaybeError implements errorfs.Injector.
func (i *WriteErrorInjector) MaybeError(op errorfs.Op) error {
	if !i.enabled.Load() {
		return nil
	}
	// Fail any future write.
	if op.Kind == errorfs.OpFileWrite {
		return errorfs.ErrInjected
	}
	return nil
}

var _ errorfs.Injector = &WriteErrorInjector{}

// Cumulative compaction stats shouldn't be updated on compaction error.
func TestCompactionErrorStats(t *testing.T) {
	// protected by d.mu
	var (
		useInjector   bool
		tablesCreated []base.DiskFileNum
	)

	mem := vfs.NewMem()
	injector := &WriteErrorInjector{}
	opts := &Options{
		FS:     errorfs.Wrap(mem, injector),
		Levels: make([]LevelOptions, numLevels),
		EventListener: &EventListener{
			TableCreated: func(info TableCreateInfo) {
				t.Log(info)

				if useInjector {
					// We'll write 3 tables during compaction, and we only need
					// the writes to error on the third file write, so only enable
					// the injector after the first two files have been written to.
					tablesCreated = append(tablesCreated, info.FileNum)
					if len(tablesCreated) >= 2 {
						injector.enable()
					}
				}
			},
		},
	}
	opts.WithFSDefaults()
	for i := range opts.Levels {
		opts.Levels[i].TargetFileSize = 1
	}
	opts.testingRandomized(t)
	d, err := Open("", opts)
	require.NoError(t, err)

	ingest := func(keys ...string) {
		t.Helper()
		f, err := mem.Create("ext", vfs.WriteCategoryUnspecified)
		require.NoError(t, err)

		w := sstable.NewWriter(objstorageprovider.NewFileWritable(f), sstable.WriterOptions{
			TableFormat: d.TableFormat(),
		})
		for _, k := range keys {
			require.NoError(t, w.Set([]byte(k), nil))
		}
		require.NoError(t, w.Close())
		require.NoError(t, d.Ingest(context.Background(), []string{"ext"}))
	}
	ingest("a", "c")
	// Snapshot will preserve the older "a" key during compaction.
	snap := d.NewSnapshot()
	ingest("a", "b")

	// Trigger a manual compaction, which will encounter an injected error
	// after the second table is created.
	d.mu.Lock()
	useInjector = true
	d.mu.Unlock()

	err = d.Compact(context.Background(), []byte("a"), []byte("d"), false)
	require.Error(t, err, "injected error")

	// Due to the error, stats shouldn't have been updated.
	d.mu.Lock()
	require.Equal(t, 0, int(d.mu.snapshots.cumulativePinnedCount))
	require.Equal(t, 0, int(d.mu.snapshots.cumulativePinnedSize))
	useInjector = false
	d.mu.Unlock()

	injector.disable()

	// The following compaction won't error, but snapshot is open, so snapshot
	// pinned stats should update.
	require.NoError(t, d.Compact(context.Background(), []byte("a"), []byte("d"), false))
	require.NoError(t, snap.Close())

	d.mu.Lock()
	require.Equal(t, 1, int(d.mu.snapshots.cumulativePinnedCount))
	require.Equal(t, 9, int(d.mu.snapshots.cumulativePinnedSize))
	d.mu.Unlock()
	require.NoError(t, d.Close())
}

func TestCompactionCorruption(t *testing.T) {
	mem := vfs.NewMem()
	var numFinishedCompactions atomic.Int32
	var once sync.Once
	opts := &Options{
		FS:                 mem,
		FormatMajorVersion: FormatNewest,
		EventListener: &EventListener{
			BackgroundError: func(error) {},
			DataCorruption: func(info DataCorruptionInfo) {
				if testing.Verbose() {
					once.Do(func() { fmt.Printf("got expected data corruption: %s\n", info.Path) })
				}
			},
			CompactionBegin: func(info CompactionInfo) {
				if testing.Verbose() {
					fmt.Printf("%d: compaction begin (L%d)\n", info.JobID, info.Output.Level)
				}
			},
			CompactionEnd: func(info CompactionInfo) {
				if testing.Verbose() {
					fmt.Printf("%d: compaction end (L%d)\n", info.JobID, info.Output.Level)
				}
				if info.Err == nil {
					numFinishedCompactions.Add(1)
				}
			},
		},
		L0CompactionThreshold:     1,
		L0CompactionFileThreshold: 10,
	}
	opts.WithFSDefaults()
	remoteStorage := remote.NewInMem()
	opts.Experimental.RemoteStorage = remote.MakeSimpleFactory(map[remote.Locator]remote.Storage{
		"external-locator": remoteStorage,
	})
	opts.EnsureDefaults()
	opts.Levels[0].TargetFileSize = 8192
	d, err := Open("", opts)
	require.NoError(t, err)

	var now crtime.AtomicMono
	now.Store(1)
	d.problemSpans.InitForTesting(manifest.NumLevels, d.cmp, func() crtime.Mono { return now.Load() })

	var workloadWG sync.WaitGroup
	var stopWorkload atomic.Bool
	defer stopWorkload.Store(true)
	startWorkload := func() {
		stopWorkload.Store(false)
		workloadWG.Add(1)
		go func() {
			defer workloadWG.Done()
			for !stopWorkload.Load() {
				b := d.NewBatch()
				// Write some random keys of the form a012345.
				for i := 0; i < 100; i++ {
					v := make([]byte, 100+rand.IntN(100))
					for i := range v {
						v[i] = byte(rand.Uint32())
					}
					key := fmt.Sprintf("%c%06d", 'a'+byte(rand.IntN(int('z'-'a'+1))), rand.IntN(1000000))
					if err := b.Set([]byte(key), v, nil); err != nil {
						panic(err)
					}
				}
				if err := b.Commit(nil); err != nil {
					panic(err)
				}
				if err := d.Flush(); err != nil {
					panic(err)
				}
				time.Sleep(10 * time.Millisecond)
			}
		}()
	}

	datadriven.RunTest(t, "testdata/compaction_corruption", func(t *testing.T, td *datadriven.TestData) string {
		// wait until fn() returns true.
		wait := func(what string, fn func() bool) {
			const timeout = 2 * time.Minute
			start := time.Now()
			for !fn() {
				if time.Since(start) > timeout {
					td.Fatalf(t, "timeout waiting for %s\n%s\n", what, d.DebugString())
				}
				time.Sleep(10 * time.Millisecond)
			}
		}

		switch td.Cmd {
		case "build-remote":
			require.NoError(t, runBuildRemoteCmd(td, d, remoteStorage))

		case "ingest-external":
			require.NoError(t, runIngestExternalCmd(t, td, d, remoteStorage, "external-locator"))

		case "move-remote-object":
			if len(td.CmdArgs) < 2 {
				td.Fatalf(t, "build <path> <new-path> argument missing")
			}
			before := td.CmdArgs[0].String()
			after := td.CmdArgs[1].String()
			ctx := context.Background()
			reader, objSize, err := remoteStorage.ReadObject(ctx, before)
			require.NoError(t, err)
			buf := make([]byte, objSize)
			require.NoError(t, reader.ReadAt(ctx, buf, 0))
			require.NoError(t, reader.Close())
			require.NoError(t, remoteStorage.Delete(before))
			writer, err := remoteStorage.CreateObject(after)
			require.NoError(t, err)
			n, err := writer.Write(buf)
			require.NoError(t, err)
			require.Equal(t, len(buf), n)
			require.NoError(t, writer.Close())
			return fmt.Sprintf("%s -> %s", before, after)

		case "start-workload":
			startWorkload()

		case "stop-workload":
			stopWorkload.Store(true)
			workloadWG.Wait()

		case "wait-for-problem-span":
			wait("problem span", func() bool {
				return !d.problemSpans.IsEmpty()
			})
			if testing.Verbose() {
				fmt.Printf("%s: wait-for-problem-span:\n%s", td.Pos, d.problemSpans.String())
			}

		case "wait-for-compactions":
			target := numFinishedCompactions.Load() + 5
			wait("compactions", func() bool {
				return numFinishedCompactions.Load() >= target
			})

		case "expire-spans":
			now.Store(now.Load() + crtime.Mono(30*time.Minute))

		case "wait-for-no-external-files":
			wait("no external files", func() bool {
				return !hasExternalFiles(d)
			})

		default:
			return fmt.Sprintf("unknown command: %s", td.Cmd)
		}

		return ""
	})
}

func hasExternalFiles(d *DB) bool {
	v := func() *version {
		d.mu.Lock()
		defer d.mu.Unlock()

		v := d.mu.versions.currentVersion()
		v.Ref()
		return v
	}()
	defer v.Unref()

	for level := 0; level < manifest.NumLevels; level++ {
		iter := v.Levels[level].Iter()
		for m := iter.First(); m != nil; m = iter.Next() {
			if m.Virtual {
				meta, err := d.objProvider.Lookup(base.FileTypeTable, m.FileBacking.DiskFileNum)
				if err != nil {
					panic(err)
				}
				if meta.IsExternal() {
					return true
				}
			}
		}
	}
	return false
}
