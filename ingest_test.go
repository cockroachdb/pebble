// Copyright 2018 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package pebble

import (
	"bytes"
	"fmt"
	"io"
	"os"
	"sort"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/cockroachdb/datadriven"
	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/errors/oserror"
	"github.com/cockroachdb/pebble/internal/base"
	"github.com/cockroachdb/pebble/internal/errorfs"
	"github.com/cockroachdb/pebble/internal/keyspan"
	"github.com/cockroachdb/pebble/internal/rangekey"
	"github.com/cockroachdb/pebble/internal/testkeys"
	"github.com/cockroachdb/pebble/sstable"
	"github.com/cockroachdb/pebble/vfs"
	"github.com/kr/pretty"
	"github.com/stretchr/testify/require"
	"golang.org/x/exp/rand"
)

func TestSSTableKeyCompare(t *testing.T) {
	var buf bytes.Buffer
	datadriven.RunTest(t, "testdata/sstable_key_compare", func(t *testing.T, td *datadriven.TestData) string {
		switch td.Cmd {
		case "cmp":
			buf.Reset()
			for _, line := range strings.Split(td.Input, "\n") {
				fields := strings.Fields(line)
				a := base.ParseInternalKey(fields[0])
				b := base.ParseInternalKey(fields[1])
				got := sstableKeyCompare(testkeys.Comparer.Compare, a, b)
				fmt.Fprintf(&buf, "%38s", fmt.Sprint(a.Pretty(base.DefaultFormatter)))
				switch got {
				case -1:
					fmt.Fprint(&buf, " < ")
				case +1:
					fmt.Fprint(&buf, " > ")
				case 0:
					fmt.Fprint(&buf, " = ")
				}
				fmt.Fprintf(&buf, "%s\n", fmt.Sprint(b.Pretty(base.DefaultFormatter)))
			}
			return buf.String()
		default:
			return fmt.Sprintf("unrecognized command %q", td.Cmd)
		}
	})
}

func TestIngestLoad(t *testing.T) {
	mem := vfs.NewMem()

	datadriven.RunTest(t, "testdata/ingest_load", func(t *testing.T, td *datadriven.TestData) string {
		switch td.Cmd {
		case "load":
			writerOpts := sstable.WriterOptions{}
			var dbVersion FormatMajorVersion
			for _, cmdArgs := range td.CmdArgs {
				v, err := strconv.Atoi(cmdArgs.Vals[0])
				if err != nil {
					return err.Error()
				}
				switch k := cmdArgs.Key; k {
				case "writer-version":
					fmv := FormatMajorVersion(v)
					writerOpts.TableFormat = fmv.MaxTableFormat()
				case "db-version":
					dbVersion = FormatMajorVersion(v)
				default:
					return fmt.Sprintf("unknown cmd %s\n", k)
				}
			}
			f, err := mem.Create("ext")
			if err != nil {
				return err.Error()
			}
			w := sstable.NewWriter(f, writerOpts)
			for _, data := range strings.Split(td.Input, "\n") {
				if strings.HasPrefix(data, "rangekey: ") {
					data = strings.TrimPrefix(data, "rangekey: ")
					s := keyspan.ParseSpan(data)
					err := rangekey.Encode(&s, w.AddRangeKey)
					if err != nil {
						return err.Error()
					}
					continue
				}

				j := strings.Index(data, ":")
				if j < 0 {
					return fmt.Sprintf("malformed input: %s\n", data)
				}
				key := base.ParseInternalKey(data[:j])
				value := []byte(data[j+1:])
				if err := w.Add(key, value); err != nil {
					return err.Error()
				}
			}
			w.Close()

			opts := &Options{
				Comparer: DefaultComparer,
				FS:       mem,
			}
			meta, _, err := ingestLoad(opts, dbVersion, []string{"ext"}, 0, []FileNum{1})
			if err != nil {
				return err.Error()
			}
			var buf bytes.Buffer
			for _, m := range meta {
				fmt.Fprintf(&buf, "%d: %s-%s\n", m.FileNum, m.Smallest, m.Largest)
				fmt.Fprintf(&buf, "  points: %s-%s\n", m.SmallestPointKey, m.LargestPointKey)
				fmt.Fprintf(&buf, "  ranges: %s-%s\n", m.SmallestRangeKey, m.LargestRangeKey)
			}
			return buf.String()

		default:
			return fmt.Sprintf("unknown command: %s", td.Cmd)
		}
	})
}

func TestIngestLoadRand(t *testing.T) {
	mem := vfs.NewMem()
	rng := rand.New(rand.NewSource(uint64(time.Now().UnixNano())))
	cmp := DefaultComparer.Compare
	version := FormatNewest

	randBytes := func(size int) []byte {
		data := make([]byte, size)
		for i := range data {
			data[i] = byte(rng.Int() & 0xff)
		}
		return data
	}

	paths := make([]string, 1+rng.Intn(10))
	pending := make([]FileNum, len(paths))
	expected := make([]*fileMetadata, len(paths))
	for i := range paths {
		paths[i] = fmt.Sprint(i)
		pending[i] = FileNum(rng.Int63())
		expected[i] = &fileMetadata{
			FileNum: pending[i],
		}
		expected[i].StatsMarkValid()

		func() {
			f, err := mem.Create(paths[i])
			require.NoError(t, err)

			keys := make([]InternalKey, 1+rng.Intn(100))
			for i := range keys {
				keys[i] = base.MakeInternalKey(
					randBytes(1+rng.Intn(10)),
					0,
					InternalKeyKindSet)
			}
			sort.Slice(keys, func(i, j int) bool {
				return base.InternalCompare(cmp, keys[i], keys[j]) < 0
			})

			expected[i].ExtendPointKeyBounds(cmp, keys[0], keys[len(keys)-1])

			w := sstable.NewWriter(f, sstable.WriterOptions{
				TableFormat: version.MaxTableFormat(),
			})
			var count uint64
			for i := range keys {
				if i > 0 && base.InternalCompare(cmp, keys[i-1], keys[i]) == 0 {
					// Duplicate key, ignore.
					continue
				}
				w.Add(keys[i], nil)
				count++
			}
			expected[i].Stats.NumEntries = count
			require.NoError(t, w.Close())

			meta, err := w.Metadata()
			require.NoError(t, err)

			expected[i].Size = meta.Size
		}()
	}

	opts := &Options{
		Comparer: DefaultComparer,
		FS:       mem,
	}
	meta, _, err := ingestLoad(opts, version, paths, 0, pending)
	require.NoError(t, err)

	for _, m := range meta {
		m.CreationTime = 0
	}
	if diff := pretty.Diff(expected, meta); diff != nil {
		t.Fatalf("%s", strings.Join(diff, "\n"))
	}
}

func TestIngestLoadInvalid(t *testing.T) {
	mem := vfs.NewMem()
	f, err := mem.Create("invalid")
	require.NoError(t, err)
	require.NoError(t, f.Close())

	opts := &Options{
		Comparer: DefaultComparer,
		FS:       mem,
	}
	if _, _, err := ingestLoad(opts, FormatNewest, []string{"invalid"}, 0, []FileNum{1}); err == nil {
		t.Fatalf("expected error, but found success")
	}
}

func TestIngestSortAndVerify(t *testing.T) {
	comparers := map[string]Compare{
		"default": DefaultComparer.Compare,
		"reverse": func(a, b []byte) int {
			return DefaultComparer.Compare(b, a)
		},
	}

	t.Run("", func(t *testing.T) {
		datadriven.RunTest(t, "testdata/ingest_sort_and_verify", func(t *testing.T, d *datadriven.TestData) string {
			switch d.Cmd {
			case "ingest":
				var buf bytes.Buffer
				var meta []*fileMetadata
				var paths []string
				var cmpName string
				d.ScanArgs(t, "cmp", &cmpName)
				cmp := comparers[cmpName]
				if cmp == nil {
					return fmt.Sprintf("%s unknown comparer: %s", d.Cmd, cmpName)
				}
				for i, data := range strings.Split(d.Input, "\n") {
					parts := strings.Split(data, "-")
					if len(parts) != 2 {
						return fmt.Sprintf("malformed test case: %s", d.Input)
					}
					smallest := base.ParseInternalKey(parts[0])
					largest := base.ParseInternalKey(parts[1])
					if cmp(smallest.UserKey, largest.UserKey) > 0 {
						return fmt.Sprintf("range %v-%v is not valid", smallest, largest)
					}
					m := (&fileMetadata{}).ExtendPointKeyBounds(cmp, smallest, largest)
					meta = append(meta, m)
					paths = append(paths, strconv.Itoa(i))
				}
				err := ingestSortAndVerify(cmp, meta, paths)
				if err != nil {
					return fmt.Sprintf("%v\n", err)
				}
				for i := range meta {
					fmt.Fprintf(&buf, "%s: %v-%v\n", paths[i], meta[i].Smallest, meta[i].Largest)
				}
				return buf.String()

			default:
				return fmt.Sprintf("unknown command: %s", d.Cmd)
			}
		})
	})
}

func TestIngestLink(t *testing.T) {
	// Test linking of tables into the DB directory. Test cleanup when one of the
	// tables cannot be linked.

	const dir = "db"
	const count = 10
	for i := 0; i <= count; i++ {
		t.Run("", func(t *testing.T) {
			mem := vfs.NewMem()
			opts := &Options{FS: mem}
			opts.EnsureDefaults()
			require.NoError(t, mem.MkdirAll(dir, 0755))

			paths := make([]string, 10)
			meta := make([]*fileMetadata, len(paths))
			contents := make([][]byte, len(paths))
			for j := range paths {
				paths[j] = fmt.Sprintf("external%d", j)
				meta[j] = &fileMetadata{}
				meta[j].FileNum = FileNum(j)
				f, err := mem.Create(paths[j])
				require.NoError(t, err)

				contents[j] = []byte(fmt.Sprintf("data%d", j))
				// memFile.Write will modify the supplied buffer when invariants are
				// enabled, so provide a throw-away copy.
				_, err = f.Write(append([]byte(nil), contents[j]...))
				require.NoError(t, err)
				require.NoError(t, f.Close())
			}

			if i < count {
				mem.Remove(paths[i])
			}

			err := ingestLink(0 /* jobID */, opts, dir, paths, meta)
			if i < count {
				if err == nil {
					t.Fatalf("expected error, but found success")
				}
			} else {
				require.NoError(t, err)
			}

			files, err := mem.List(dir)
			require.NoError(t, err)

			sort.Strings(files)

			if i < count {
				if len(files) > 0 {
					t.Fatalf("expected all of the files to be cleaned up, but found:\n%s",
						strings.Join(files, "\n"))
				}
			} else {
				if len(files) != count {
					t.Fatalf("expected %d files, but found:\n%s", count, strings.Join(files, "\n"))
				}
				for j := range files {
					ftype, fileNum, ok := base.ParseFilename(mem, files[j])
					if !ok {
						t.Fatalf("unable to parse filename: %s", files[j])
					}
					if fileTypeTable != ftype {
						t.Fatalf("expected table, but found %d", ftype)
					}
					if FileNum(j) != fileNum {
						t.Fatalf("expected table %d, but found %d", j, fileNum)
					}
					f, err := mem.Open(mem.PathJoin(dir, files[j]))
					require.NoError(t, err)

					data, err := io.ReadAll(f)
					require.NoError(t, err)
					require.NoError(t, f.Close())
					if !bytes.Equal(contents[j], data) {
						t.Fatalf("expected %s, but found %s", contents[j], data)
					}
				}
			}
		})
	}
}

func TestIngestLinkFallback(t *testing.T) {
	// Verify that ingestLink succeeds if linking fails by falling back to
	// copying.
	mem := vfs.NewMem()
	src, err := mem.Create("source")
	require.NoError(t, err)

	opts := &Options{FS: errorfs.Wrap(mem, errorfs.OnIndex(0))}
	opts.EnsureDefaults()

	meta := []*fileMetadata{{FileNum: 1}}
	require.NoError(t, ingestLink(0, opts, "", []string{"source"}, meta))

	dest, err := mem.Open("000001.sst")
	require.NoError(t, err)

	// We should be able to write bytes to src, and not have them show up in
	// dest.
	_, _ = src.Write([]byte("test"))
	data, err := io.ReadAll(dest)
	require.NoError(t, err)
	if len(data) != 0 {
		t.Fatalf("expected copy, but files appear to be hard linked: [%s] unexpectedly found", data)
	}
}

func TestIngestMemtableOverlaps(t *testing.T) {
	comparers := []Comparer{
		{Name: "default", Compare: DefaultComparer.Compare, FormatKey: DefaultComparer.FormatKey},
		{
			Name:      "reverse",
			Compare:   func(a, b []byte) int { return DefaultComparer.Compare(b, a) },
			FormatKey: DefaultComparer.FormatKey,
		},
	}
	m := make(map[string]*Comparer)
	for i := range comparers {
		c := &comparers[i]
		m[c.Name] = c
	}

	for _, comparer := range comparers {
		t.Run(comparer.Name, func(t *testing.T) {
			var mem *memTable

			parseMeta := func(s string) *fileMetadata {
				parts := strings.Split(s, "-")
				meta := &fileMetadata{}
				if len(parts) != 2 {
					t.Fatalf("malformed table spec: %s", s)
				}
				var smallest, largest base.InternalKey
				if strings.Contains(parts[0], ".") {
					if !strings.Contains(parts[1], ".") {
						t.Fatalf("malformed table spec: %s", s)
					}
					smallest = base.ParseInternalKey(parts[0])
					largest = base.ParseInternalKey(parts[1])
				} else {
					smallest = InternalKey{UserKey: []byte(parts[0])}
					largest = InternalKey{UserKey: []byte(parts[1])}
				}
				// If we're using a reverse comparer, flip the file bounds.
				if mem.cmp(smallest.UserKey, largest.UserKey) > 0 {
					smallest, largest = largest, smallest
				}
				meta.ExtendPointKeyBounds(comparer.Compare, smallest, largest)
				return meta
			}

			datadriven.RunTest(t, "testdata/ingest_memtable_overlaps", func(t *testing.T, d *datadriven.TestData) string {
				switch d.Cmd {
				case "define":
					b := newBatch(nil)
					if err := runBatchDefineCmd(d, b); err != nil {
						return err.Error()
					}

					opts := &Options{
						Comparer: &comparer,
					}
					opts.EnsureDefaults()
					if len(d.CmdArgs) > 1 {
						return fmt.Sprintf("%s expects at most 1 argument", d.Cmd)
					}
					if len(d.CmdArgs) == 1 {
						opts.Comparer = m[d.CmdArgs[0].String()]
						if opts.Comparer == nil {
							return fmt.Sprintf("%s unknown comparer: %s", d.Cmd, d.CmdArgs[0].String())
						}
					}

					mem = newMemTable(memTableOptions{Options: opts})
					if err := mem.apply(b, 0); err != nil {
						return err.Error()
					}
					return ""

				case "overlaps":
					var buf bytes.Buffer
					for _, data := range strings.Split(d.Input, "\n") {
						var meta []*fileMetadata
						for _, part := range strings.Fields(data) {
							meta = append(meta, parseMeta(part))
						}
						fmt.Fprintf(&buf, "%t\n", ingestMemtableOverlaps(mem.cmp, mem, meta))
					}
					return buf.String()

				default:
					return fmt.Sprintf("unknown command: %s", d.Cmd)
				}
			})
		})
	}
}

func BenchmarkIngestOverlappingMemtable(b *testing.B) {
	assertNoError := func(err error) {
		b.Helper()
		if err != nil {
			b.Fatal(err)
		}
	}

	for count := 1; count < 6; count++ {
		b.Run(fmt.Sprintf("memtables=%d", count), func(b *testing.B) {
			for i := 0; i < b.N; i++ {
				b.StopTimer()
				mem := vfs.NewMem()
				d, err := Open("", &Options{
					FS: mem,
				})
				assertNoError(err)

				// Create memtables.
				for {
					assertNoError(d.Set([]byte("a"), nil, nil))
					d.mu.Lock()
					done := len(d.mu.mem.queue) == count
					d.mu.Unlock()
					if done {
						break
					}
				}

				// Create the overlapping sstable that will force a flush when ingested.
				f, err := mem.Create("ext")
				assertNoError(err)
				w := sstable.NewWriter(f, sstable.WriterOptions{})
				assertNoError(w.Set([]byte("a"), nil))
				assertNoError(w.Close())

				b.StartTimer()
				assertNoError(d.Ingest([]string{"ext"}))
			}
		})
	}
}

func TestIngestTargetLevel(t *testing.T) {
	var d *DB
	defer func() {
		if d != nil {
			// Ignore errors because this test defines fake in-progress transactions
			// that prohibit clean shutdown.
			_ = d.Close()
		}
	}()

	parseMeta := func(s string) *fileMetadata {
		var rkey bool
		if len(s) >= 4 && s[0:4] == "rkey" {
			rkey = true
			s = s[5:]
		}
		parts := strings.Split(s, "-")
		if len(parts) != 2 {
			t.Fatalf("malformed table spec: %s", s)
		}
		var m *fileMetadata
		if rkey {
			m = (&fileMetadata{}).ExtendRangeKeyBounds(
				d.cmp,
				InternalKey{UserKey: []byte(parts[0])},
				InternalKey{UserKey: []byte(parts[1])},
			)
		} else {
			m = (&fileMetadata{}).ExtendPointKeyBounds(
				d.cmp,
				InternalKey{UserKey: []byte(parts[0])},
				InternalKey{UserKey: []byte(parts[1])},
			)
		}
		return m
	}

	datadriven.RunTest(t, "testdata/ingest_target_level", func(t *testing.T, td *datadriven.TestData) string {
		switch td.Cmd {
		case "define":
			if d != nil {
				// Ignore errors because this test defines fake in-progress
				// transactions that prohibit clean shutdown.
				_ = d.Close()
			}

			var err error
			opts := Options{
				FormatMajorVersion: FormatNewest,
			}
			if d, err = runDBDefineCmd(td, &opts); err != nil {
				return err.Error()
			}

			readState := d.loadReadState()
			c := &checkConfig{
				logger:    d.opts.Logger,
				cmp:       d.cmp,
				readState: readState,
				newIters:  d.newIters,
				// TODO: runDBDefineCmd doesn't properly update the visible
				// sequence number. So we have to explicitly configure level checker with a very large
				// sequence number, otherwise the DB appears empty.
				seqNum: InternalKeySeqNumMax,
			}
			if err := checkLevelsInternal(c); err != nil {
				return err.Error()
			}
			readState.unref()

			d.mu.Lock()
			s := d.mu.versions.currentVersion().String()
			d.mu.Unlock()
			return s

		case "target":
			var buf bytes.Buffer
			for _, target := range strings.Split(td.Input, "\n") {
				meta := parseMeta(target)
				level, err := ingestTargetLevel(
					d.newIters, d.tableNewRangeKeyIter, IterOptions{logger: d.opts.Logger},
					d.cmp, d.mu.versions.currentVersion(), 1, d.mu.compact.inProgress, meta,
				)
				if err != nil {
					return err.Error()
				}
				fmt.Fprintf(&buf, "%d\n", level)
			}
			return buf.String()

		default:
			return fmt.Sprintf("unknown command: %s", td.Cmd)
		}
	})
}

func TestIngest(t *testing.T) {
	var mem vfs.FS
	var d *DB
	var flushed bool
	defer func() {
		require.NoError(t, d.Close())
	}()

	reset := func() {
		if d != nil {
			require.NoError(t, d.Close())
		}

		mem = vfs.NewMem()
		require.NoError(t, mem.MkdirAll("ext", 0755))
		opts := &Options{
			FS:                    mem,
			L0CompactionThreshold: 100,
			L0StopWritesThreshold: 100,
			DebugCheck:            DebugCheckLevels,
			EventListener: &EventListener{FlushEnd: func(info FlushInfo) {
				flushed = true
			}},
			FormatMajorVersion: FormatNewest,
		}
		// Disable automatic compactions because otherwise we'll race with
		// delete-only compactions triggered by ingesting range tombstones.
		opts.DisableAutomaticCompactions = true

		var err error
		d, err = Open("", opts)
		require.NoError(t, err)
	}
	reset()

	datadriven.RunTest(t, "testdata/ingest", func(t *testing.T, td *datadriven.TestData) string {
		switch td.Cmd {
		case "reset":
			reset()
			return ""
		case "batch":
			b := d.NewIndexedBatch()
			if err := runBatchDefineCmd(td, b); err != nil {
				return err.Error()
			}
			if err := b.Commit(nil); err != nil {
				return err.Error()
			}
			return ""

		case "build":
			if err := runBuildCmd(td, d, mem); err != nil {
				return err.Error()
			}
			return ""

		case "ingest":
			flushed = false
			if err := runIngestCmd(td, d, mem); err != nil {
				return err.Error()
			}
			// Wait for a possible flush.
			d.mu.Lock()
			for d.mu.compact.flushing {
				d.mu.compact.cond.Wait()
			}
			d.mu.Unlock()
			if flushed {
				return "memtable flushed"
			}
			return ""

		case "get":
			return runGetCmd(td, d)

		case "iter":
			iter := d.NewIter(&IterOptions{
				KeyTypes: IterKeyTypePointsAndRanges,
			})
			return runIterCmd(td, iter, true)

		case "lsm":
			return runLSMCmd(td, d)

		case "metrics":
			// The asynchronous loading of table stats can change metrics, so
			// wait for all the tables' stats to be loaded.
			d.mu.Lock()
			d.waitTableStats()
			d.mu.Unlock()

			return d.Metrics().String()

		case "wait-pending-table-stats":
			return runTableStatsCmd(td, d)

		case "compact":
			if len(td.CmdArgs) != 2 {
				panic("insufficient args for compact command")
			}
			l := td.CmdArgs[0].Key
			r := td.CmdArgs[1].Key
			err := d.Compact([]byte(l), []byte(r), false)
			if err != nil {
				return err.Error()
			}
			return ""
		default:
			return fmt.Sprintf("unknown command: %s", td.Cmd)
		}
	})
}

func TestIngestError(t *testing.T) {
	for i := int32(0); ; i++ {
		mem := vfs.NewMem()

		f0, err := mem.Create("ext0")
		require.NoError(t, err)
		w := sstable.NewWriter(f0, sstable.WriterOptions{})
		require.NoError(t, w.Set([]byte("d"), nil))
		require.NoError(t, w.Close())
		f1, err := mem.Create("ext1")
		require.NoError(t, err)
		w = sstable.NewWriter(f1, sstable.WriterOptions{})
		require.NoError(t, w.Set([]byte("d"), nil))
		require.NoError(t, w.Close())

		inj := errorfs.OnIndex(-1)
		d, err := Open("", &Options{
			FS:                    errorfs.Wrap(mem, inj),
			Logger:                panicLogger{},
			L0CompactionThreshold: 8,
		})
		require.NoError(t, err)
		// Force the creation of an L0 sstable that overlaps with the tables
		// we'll attempt to ingest. This ensures that we exercise filesystem
		// codepaths when determining the ingest target level.
		require.NoError(t, d.Set([]byte("a"), nil, nil))
		require.NoError(t, d.Set([]byte("d"), nil, nil))
		require.NoError(t, d.Flush())

		t.Run(fmt.Sprintf("index-%d", i), func(t *testing.T) {
			defer func() {
				if r := recover(); r != nil {
					if e, ok := r.(error); ok && errors.Is(e, errorfs.ErrInjected) {
						return
					}
					// d.opts.Logger.Fatalf won't propagate ErrInjected
					// itself, but should contain the error message.
					if strings.HasSuffix(fmt.Sprint(r), errorfs.ErrInjected.Error()) {
						return
					}
					t.Fatal(r)
				}
			}()

			inj.SetIndex(i)
			err1 := d.Ingest([]string{"ext0"})
			err2 := d.Ingest([]string{"ext1"})
			err := firstError(err1, err2)
			if err != nil && !errors.Is(err, errorfs.ErrInjected) {
				t.Fatal(err)
			}
		})

		// d.Close may error if we failed to flush the manifest.
		_ = d.Close()

		// If the injector's index is non-negative, the i-th filesystem
		// operation was never executed.
		if inj.Index() >= 0 {
			break
		}
	}
}

func TestIngestIdempotence(t *testing.T) {
	// Use an on-disk filesystem, because Ingest with a MemFS will copy, not
	// link the ingested file.
	dir, err := os.MkdirTemp("", "ingest-idempotence")
	require.NoError(t, err)
	defer os.RemoveAll(dir)
	fs := vfs.Default

	path := fs.PathJoin(dir, "ext")
	f, err := fs.Create(fs.PathJoin(dir, "ext"))
	require.NoError(t, err)
	w := sstable.NewWriter(f, sstable.WriterOptions{})
	require.NoError(t, w.Set([]byte("d"), nil))
	require.NoError(t, w.Close())

	d, err := Open(dir, &Options{
		FS: fs,
	})
	require.NoError(t, err)
	const count = 4
	for i := 0; i < count; i++ {
		ingestPath := fs.PathJoin(dir, fmt.Sprintf("ext%d", i))
		require.NoError(t, fs.Link(path, ingestPath))
		require.NoError(t, d.Ingest([]string{ingestPath}))
	}
	require.NoError(t, d.Close())
}

func TestIngestCompact(t *testing.T) {
	var buf syncedBuffer
	mem := vfs.NewMem()
	lel := MakeLoggingEventListener(&buf)
	d, err := Open("", &Options{
		EventListener:         &lel,
		FS:                    mem,
		L0CompactionThreshold: 1,
		L0StopWritesThreshold: 1,
	})
	require.NoError(t, err)

	src := func(i int) string {
		return fmt.Sprintf("ext%d", i)
	}
	f, err := mem.Create(src(0))
	require.NoError(t, err)

	w := sstable.NewWriter(f, sstable.WriterOptions{})
	key := []byte("a")
	require.NoError(t, w.Add(base.MakeInternalKey(key, 0, InternalKeyKindSet), nil))
	require.NoError(t, w.Close())

	// Make N copies of the sstable.
	const count = 20
	for i := 1; i < count; i++ {
		require.NoError(t, vfs.Copy(d.opts.FS, src(0), src(i)))
	}

	// Ingest the same sstable multiple times. Compaction should take place as
	// ingestion happens, preventing an indefinite write stall from occurring.
	for i := 0; i < count; i++ {
		if i == 10 {
			// Half-way through the ingestions, set a key in the memtable to force
			// overlap with the memtable which will require the memtable to be
			// flushed.
			require.NoError(t, d.Set(key, nil, nil))
		}
		require.NoError(t, d.Ingest([]string{src(i)}))
	}

	require.NoError(t, d.Close())
}

func TestConcurrentIngest(t *testing.T) {
	mem := vfs.NewMem()
	d, err := Open("", &Options{
		FS: mem,
	})
	require.NoError(t, err)

	// Create an sstable with 2 keys. This is necessary to trigger the overlap
	// bug because an sstable with a single key will not have overlap in internal
	// key space and the sequence number assignment had already guaranteed
	// correct ordering.
	src := func(i int) string {
		return fmt.Sprintf("ext%d", i)
	}
	f, err := mem.Create(src(0))
	require.NoError(t, err)

	w := sstable.NewWriter(f, sstable.WriterOptions{})
	require.NoError(t, w.Set([]byte("a"), nil))
	require.NoError(t, w.Set([]byte("b"), nil))
	require.NoError(t, w.Close())

	// Make N copies of the sstable.
	errCh := make(chan error, 5)
	for i := 1; i < cap(errCh); i++ {
		require.NoError(t, vfs.Copy(d.opts.FS, src(0), src(i)))
	}

	// Perform N ingestions concurrently.
	for i := 0; i < cap(errCh); i++ {
		go func(i int) {
			err := d.Ingest([]string{src(i)})
			if err == nil {
				if _, err = d.opts.FS.Stat(src(i)); oserror.IsNotExist(err) {
					err = nil
				}
			}
			errCh <- err
		}(i)
	}
	for i := 0; i < cap(errCh); i++ {
		require.NoError(t, <-errCh)
	}

	require.NoError(t, d.Close())
}

func TestConcurrentIngestCompact(t *testing.T) {
	for i := 0; i < 2; i++ {
		t.Run("", func(t *testing.T) {
			mem := vfs.NewMem()
			compactionReady := make(chan struct{})
			compactionBegin := make(chan struct{})
			d, err := Open("", &Options{
				FS: mem,
				EventListener: &EventListener{
					TableCreated: func(info TableCreateInfo) {
						if info.Reason == "compacting" {
							close(compactionReady)
							<-compactionBegin
						}
					},
				},
			})
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

			compact := func(start, end string) {
				t.Helper()
				require.NoError(t, d.Compact([]byte(start), []byte(end), false))
			}

			lsm := func() string {
				d.mu.Lock()
				s := d.mu.versions.currentVersion().String()
				d.mu.Unlock()
				return s
			}

			expectLSM := func(expected string) {
				t.Helper()
				expected = strings.TrimSpace(expected)
				actual := strings.TrimSpace(lsm())
				if expected != actual {
					t.Fatalf("expected\n%s\nbut found\n%s", expected, actual)
				}
			}

			ingest("a")
			ingest("a")
			ingest("c")
			ingest("c")

			expectLSM(`
0.0:
  000005:[a#2,SET-a#2,SET]
  000007:[c#4,SET-c#4,SET]
6:
  000004:[a#1,SET-a#1,SET]
  000006:[c#3,SET-c#3,SET]
`)

			// At this point ingestion of an sstable containing only key "b" will be
			// targeted at L6. Yet a concurrent compaction of sstables 5 and 7 will
			// create a new sstable in L6 spanning ["a"-"c"]. So the ingestion must
			// actually target L5.

			switch i {
			case 0:
				// Compact, then ingest.
				go func() {
					<-compactionReady

					ingest("b")

					close(compactionBegin)
				}()

				compact("a", "z")

				expectLSM(`
0.0:
  000009:[b#5,SET-b#5,SET]
6:
  000008:[a#0,SET-c#0,SET]
`)

			case 1:
				// Ingest, then compact
				var wg sync.WaitGroup
				wg.Add(1)
				go func() {
					defer wg.Done()
					close(compactionBegin)
					compact("a", "z")
				}()

				ingest("b")
				wg.Wait()

				// Because we're performing the ingestion and compaction concurrently,
				// we can't guarantee any particular LSM structure at this point. The
				// test will fail with an assertion error due to overlapping sstables
				// if there is insufficient synchronization between ingestion and
				// compaction.
			}

			require.NoError(t, d.Close())
		})
	}
}

func TestIngestFlushQueuedMemTable(t *testing.T) {
	// Verify that ingestion forces a flush of a queued memtable.

	mem := vfs.NewMem()
	d, err := Open("", &Options{
		FS: mem,
	})
	require.NoError(t, err)

	// Add the key "a" to the memtable, then fill up the memtable with the key
	// "b". The ingested sstable will only overlap with the queued memtable.
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

	ingest := func(keys ...string) {
		t.Helper()
		f, err := mem.Create("ext")
		require.NoError(t, err)

		w := sstable.NewWriter(f, sstable.WriterOptions{})
		for _, k := range keys {
			require.NoError(t, w.Set([]byte(k), nil))
		}
		require.NoError(t, w.Close())
		stats, err := d.IngestWithStats([]string{"ext"})
		require.NoError(t, err)
		require.Equal(t, stats.ApproxIngestedIntoL0Bytes, stats.Bytes)
		require.Less(t, uint64(0), stats.Bytes)
	}

	ingest("a")

	require.NoError(t, d.Close())
}

func TestIngestStats(t *testing.T) {
	mem := vfs.NewMem()
	d, err := Open("", &Options{
		FS: mem,
	})
	require.NoError(t, err)

	ingest := func(expectedLevel int, keys ...string) {
		t.Helper()
		f, err := mem.Create("ext")
		require.NoError(t, err)

		w := sstable.NewWriter(f, sstable.WriterOptions{})
		for _, k := range keys {
			require.NoError(t, w.Set([]byte(k), nil))
		}
		require.NoError(t, w.Close())
		stats, err := d.IngestWithStats([]string{"ext"})
		require.NoError(t, err)
		if expectedLevel == 0 {
			require.Equal(t, stats.ApproxIngestedIntoL0Bytes, stats.Bytes)
		} else {
			require.EqualValues(t, 0, stats.ApproxIngestedIntoL0Bytes)
		}
		require.Less(t, uint64(0), stats.Bytes)
	}
	ingest(6, "a")
	ingest(0, "a")
	ingest(6, "b", "g")
	ingest(0, "c")
	require.NoError(t, d.Close())
}

func TestIngestFlushQueuedLargeBatch(t *testing.T) {
	// Verify that ingestion forces a flush of a queued large batch.

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
	d.mu.Unlock()

	// Set a record with a large value. This will be transformed into a large
	// batch and placed in the flushable queue.
	require.NoError(t, d.Set([]byte("a"), bytes.Repeat([]byte("v"), d.largeBatchThreshold), nil))

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

	ingest("a")

	require.NoError(t, d.Close())
}

func TestIngestMemtablePendingOverlap(t *testing.T) {
	mem := vfs.NewMem()
	d, err := Open("", &Options{
		FS: mem,
	})
	require.NoError(t, err)

	d.mu.Lock()
	// Use a custom commit pipeline apply function to give us control over
	// timing of events.
	assignedBatch := make(chan struct{})
	applyBatch := make(chan struct{})
	originalApply := d.commit.env.apply
	d.commit.env.apply = func(b *Batch, mem *memTable) error {
		assignedBatch <- struct{}{}
		applyBatch <- struct{}{}
		return originalApply(b, mem)
	}
	d.mu.Unlock()

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

	var wg sync.WaitGroup
	wg.Add(2)

	// First, Set('c') begins. This call will:
	//
	// * enqueue the batch to the pending queue.
	// * allocate a sequence number `x`.
	// * write the batch to the WAL.
	//
	// and then block until we read from the `applyBatch` channel down below.
	go func() {
		err := d.Set([]byte("c"), nil, nil)
		if err != nil {
			t.Error(err)
		}
		wg.Done()
	}()

	// When the above Set('c') is ready to apply, it sends on the
	// `assignedBatch` channel. Once that happens, we start Ingest('a', 'c').
	// The Ingest('a', 'c') allocates sequence number `x + 1`.
	go func() {
		// Wait until the Set has grabbed a sequence number before ingesting.
		<-assignedBatch
		ingest("a", "c")
		wg.Done()
	}()

	// The Set('c')#1 and Ingest('a', 'c')#2 are both pending. To maintain
	// sequence number invariants, the Set needs to be applied and flushed
	// before the Ingest determines its target level.
	//
	// Sleep a bit to ensure that the Ingest has time to call into
	// AllocateSeqNum. Once it allocates its sequence number, it should see
	// that there are unpublished sequence numbers below it and spin until the
	// Set's sequence number is published. After sleeping, read from
	// `applyBatch` to actually allow the Set to apply and publish its
	// sequence number.
	time.Sleep(100 * time.Millisecond)
	<-applyBatch

	// Wait for both calls to complete.
	wg.Wait()
	require.NoError(t, d.Flush())
	require.NoError(t, d.CheckLevels(nil))
	require.NoError(t, d.Close())
}

type ingestCrashFS struct {
	vfs.FS
}

func (fs ingestCrashFS) Link(oldname, newname string) error {
	if err := fs.FS.Link(oldname, newname); err != nil {
		return err
	}
	panic(errorfs.ErrInjected)
}

type noRemoveFS struct {
	vfs.FS
}

func (fs noRemoveFS) Remove(string) error {
	return errorfs.ErrInjected
}

func TestIngestFileNumReuseCrash(t *testing.T) {
	const count = 10
	// Use an on-disk filesystem, because Ingest with a MemFS will copy, not
	// link the ingested file.
	dir, err := os.MkdirTemp("", "ingest-filenum-reuse")
	require.NoError(t, err)
	defer os.RemoveAll(dir)
	fs := vfs.Default

	readFile := func(s string) []byte {
		f, err := fs.Open(fs.PathJoin(dir, s))
		require.NoError(t, err)
		b, err := io.ReadAll(f)
		require.NoError(t, err)
		require.NoError(t, f.Close())
		return b
	}

	// Create sstables to ingest.
	var files []string
	var fileBytes [][]byte
	for i := 0; i < count; i++ {
		name := fmt.Sprintf("ext%d", i)
		f, err := fs.Create(fs.PathJoin(dir, name))
		require.NoError(t, err)
		w := sstable.NewWriter(f, sstable.WriterOptions{})
		require.NoError(t, w.Set([]byte(fmt.Sprintf("foo%d", i)), nil))
		require.NoError(t, w.Close())
		files = append(files, name)
		fileBytes = append(fileBytes, readFile(name))
	}

	// Open a database with a filesystem that will successfully link the
	// ingested files but then panic. This is an approximation of what a crash
	// after linking but before updating the manifest would look like.
	d, err := Open(dir, &Options{
		FS: ingestCrashFS{FS: fs},
	})
	// A flush here ensures the file num bumps from creating OPTIONS files,
	// etc get recorded in the manifest. We want the nextFileNum after the
	// restart to be the same as one of our ingested sstables.
	require.NoError(t, err)
	require.NoError(t, d.Set([]byte("boop"), nil, nil))
	require.NoError(t, d.Flush())
	for _, f := range files {
		func() {
			defer func() { err = recover().(error) }()
			err = d.Ingest([]string{fs.PathJoin(dir, f)})
		}()
		if err == nil || !errors.Is(err, errorfs.ErrInjected) {
			t.Fatalf("expected injected error, got %v", err)
		}
	}
	// Leave something in the WAL so that Open will flush while replaying the
	// WAL.
	require.NoError(t, d.Set([]byte("wal"), nil, nil))
	require.NoError(t, d.Close())

	// There are now two links to each external file: the original extX link
	// and a numbered sstable link. The sstable files are still not a part of
	// the manifest and so they may be overwritten. Open will detect the
	// obsolete number sstables and try to remove them. The FS here is wrapped
	// to induce errors on Remove calls. Even if we're unsuccessful in
	// removing the obsolete files, the external files should not be
	// overwritten.
	d, err = Open(dir, &Options{FS: noRemoveFS{FS: fs}})
	require.NoError(t, err)
	require.NoError(t, d.Set([]byte("bar"), nil, nil))
	require.NoError(t, d.Flush())
	require.NoError(t, d.Close())

	// None of the external files should change despite modifying the linked
	// versions.
	for i, f := range files {
		afterBytes := readFile(f)
		require.Equal(t, fileBytes[i], afterBytes)
	}
}

func TestIngest_UpdateSequenceNumber(t *testing.T) {
	mem := vfs.NewMem()
	cmp := base.DefaultComparer.Compare
	parse := func(input string) (*sstable.Writer, error) {
		f, err := mem.Create("ext")
		if err != nil {
			return nil, err
		}
		w := sstable.NewWriter(f, sstable.WriterOptions{
			TableFormat: sstable.TableFormatMax,
		})
		for _, data := range strings.Split(input, "\n") {
			if strings.HasPrefix(data, "rangekey: ") {
				data = strings.TrimPrefix(data, "rangekey: ")
				s := keyspan.ParseSpan(data)
				err := rangekey.Encode(&s, w.AddRangeKey)
				if err != nil {
					return nil, err
				}
				continue
			}
			j := strings.Index(data, ":")
			if j < 0 {
				return nil, errors.Newf("malformed input: %s\n", data)
			}
			key := base.ParseInternalKey(data[:j])
			value := []byte(data[j+1:])
			if err := w.Add(key, value); err != nil {
				return nil, err
			}
		}
		return w, nil
	}

	var (
		seqnum uint64
		err    error
		metas  []*fileMetadata
	)
	datadriven.RunTest(t, "testdata/ingest_update_seqnums", func(t *testing.T, td *datadriven.TestData) string {
		switch td.Cmd {
		case "starting-seqnum":
			seqnum, err = strconv.ParseUint(td.Input, 10, 64)
			if err != nil {
				return err.Error()
			}
			return ""

		case "reset":
			metas = metas[:0]
			return ""

		case "load":
			w, err := parse(td.Input)
			if err != nil {
				return err.Error()
			}
			if err = w.Close(); err != nil {
				return err.Error()
			}
			defer w.Close()

			// Format the bounds of the table.
			wm, err := w.Metadata()
			if err != nil {
				return err.Error()
			}

			// Upper bounds for range dels and range keys are expected to be sentinel
			// keys.
			maybeUpdateUpperBound := func(key base.InternalKey) base.InternalKey {
				switch k := key.Kind(); {
				case k == base.InternalKeyKindRangeDelete:
					key.Trailer = base.InternalKeyRangeDeleteSentinel
				case rangekey.IsRangeKey(k):
					return base.MakeExclusiveSentinelKey(k, key.UserKey)
				}
				return key
			}

			// Construct the file metadata from the writer metadata.
			m := &fileMetadata{
				SmallestSeqNum: 0, // Simulate an ingestion.
				LargestSeqNum:  0,
			}
			if wm.HasPointKeys {
				m.ExtendPointKeyBounds(cmp, wm.SmallestPoint, wm.LargestPoint)
			}
			if wm.HasRangeDelKeys {
				m.ExtendPointKeyBounds(
					cmp,
					wm.SmallestRangeDel,
					maybeUpdateUpperBound(wm.LargestRangeDel),
				)
			}
			if wm.HasRangeKeys {
				m.ExtendRangeKeyBounds(
					cmp,
					wm.SmallestRangeKey,
					maybeUpdateUpperBound(wm.LargestRangeKey),
				)
			}
			if err := m.Validate(cmp, base.DefaultFormatter); err != nil {
				return err.Error()
			}

			// Collect this file.
			metas = append(metas, m)

			// Return an index number for the file.
			return fmt.Sprintf("file %d\n", len(metas)-1)

		case "update-files":
			// Update the bounds across all files.
			if err = ingestUpdateSeqNum(cmp, base.DefaultFormatter, seqnum, metas); err != nil {
				return err.Error()
			}

			var buf bytes.Buffer
			for i, m := range metas {
				fmt.Fprintf(&buf, "file %d:\n", i)
				fmt.Fprintf(&buf, "  combined: %s-%s\n", m.Smallest, m.Largest)
				fmt.Fprintf(&buf, "    points: %s-%s\n", m.SmallestPointKey, m.LargestPointKey)
				fmt.Fprintf(&buf, "    ranges: %s-%s\n", m.SmallestRangeKey, m.LargestRangeKey)
			}

			return buf.String()

		default:
			return fmt.Sprintf("unknown command %s\n", td.Cmd)
		}
	})
}

func TestIngestCleanup(t *testing.T) {
	fns := []base.FileNum{0, 1, 2}

	testCases := []struct {
		closeFiles   []base.FileNum
		cleanupFiles []base.FileNum
		wantErr      error
	}{
		// Close and remove all files.
		{
			closeFiles:   fns,
			cleanupFiles: fns,
		},
		// Remove a non-existent file.
		{
			closeFiles:   fns,
			cleanupFiles: []base.FileNum{3},
			wantErr:      oserror.ErrNotExist,
		},
		// Remove a file that has not been closed.
		{
			closeFiles:   []base.FileNum{0, 2},
			cleanupFiles: fns,
			wantErr:      oserror.ErrInvalid,
		},
		// Remove all files, one of which is still open, plus a file that does not exist.
		{
			closeFiles:   []base.FileNum{0, 2},
			cleanupFiles: []base.FileNum{0, 1, 2, 3},
			wantErr:      oserror.ErrInvalid, // The first error encountered is due to the open file.
		},
	}

	for _, tc := range testCases {
		t.Run("", func(t *testing.T) {
			mem := vfs.NewMem()
			mem.UseWindowsSemantics(true)

			// Create the files in the VFS.
			metaMap := make(map[base.FileNum]vfs.File)
			for _, fn := range fns {
				path := base.MakeFilepath(mem, "", base.FileTypeTable, fn)
				f, err := mem.Create(path)
				metaMap[fn] = f
				require.NoError(t, err)
			}

			// Close a select number of files.
			for _, m := range tc.closeFiles {
				f, ok := metaMap[m]
				if !ok {
					continue
				}
				require.NoError(t, f.Close())
			}

			// Cleanup the set of files in the FS.
			var toRemove []*fileMetadata
			for _, fn := range tc.cleanupFiles {
				toRemove = append(toRemove, &fileMetadata{FileNum: fn})
			}

			err := ingestCleanup(mem, "", toRemove)
			if tc.wantErr != nil {
				require.Equal(t, tc.wantErr, err)
			} else {
				require.NoError(t, err)
			}
		})
	}
}

// fatalCapturingLogger captures a fatal error instead of panicking.
type fatalCapturingLogger struct {
	defaultLogger
	err error
}

// Fatalf implements the Logger.Fatalf interface.
func (l *fatalCapturingLogger) Fatalf(_ string, args ...interface{}) {
	l.err = args[0].(error)
}

func TestIngestValidation(t *testing.T) {
	type keyVal struct {
		key, val []byte
	}
	type corruptionLocation int
	const (
		corruptionLocationNone corruptionLocation = iota
		corruptionLocationStart
		corruptionLocationEnd
		corruptionLocationInternal
	)
	type errLocation int
	const (
		errLocationNone errLocation = iota
		errLocationIngest
		errLocationValidation
	)
	const (
		nKeys     = 1_000
		keySize   = 16
		valSize   = 100
		blockSize = 100

		ingestTableName = "ext"
	)

	seed := uint64(time.Now().UnixNano())
	rng := rand.New(rand.NewSource(seed))
	t.Logf("rng seed = %d", seed)

	testCases := []struct {
		description string
		cLoc        corruptionLocation
		wantErrType errLocation
	}{
		{
			description: "no corruption",
			cLoc:        corruptionLocationNone,
			wantErrType: errLocationNone,
		},
		{
			description: "start block",
			cLoc:        corruptionLocationStart,
			wantErrType: errLocationIngest,
		},
		{
			description: "end block",
			cLoc:        corruptionLocationEnd,
			wantErrType: errLocationIngest,
		},
		{
			description: "non-end block",
			cLoc:        corruptionLocationInternal,
			wantErrType: errLocationValidation,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.description, func(t *testing.T) {
			var wg sync.WaitGroup
			wg.Add(1)

			fs := vfs.NewMem()
			logger := &fatalCapturingLogger{}
			opts := &Options{
				FS:     fs,
				Logger: logger,
				EventListener: &EventListener{
					TableValidated: func(i TableValidatedInfo) {
						wg.Done()
					},
				},
			}
			opts.Experimental.ValidateOnIngest = true
			d, err := Open("", opts)
			require.NoError(t, err)
			defer func() { require.NoError(t, d.Close()) }()

			corrupt := func(f vfs.File) {
				// Compute the layout of the sstable in order to find the
				// appropriate block locations to corrupt.
				r, err := sstable.NewReader(f, sstable.ReaderOptions{})
				require.NoError(t, err)
				l, err := r.Layout()
				require.NoError(t, err)
				require.NoError(t, r.Close())

				// Select an appropriate data block to corrupt.
				var blockIdx int
				switch tc.cLoc {
				case corruptionLocationStart:
					blockIdx = 0
				case corruptionLocationEnd:
					blockIdx = len(l.Data) - 1
				case corruptionLocationInternal:
					blockIdx = 1 + rng.Intn(len(l.Data)-2)
				default:
					t.Fatalf("unknown corruptionLocation: %T", tc.cLoc)
				}
				bh := l.Data[blockIdx]

				osF, err := os.OpenFile(ingestTableName, os.O_RDWR, 0600)
				require.NoError(t, err)
				defer func() { require.NoError(t, osF.Close()) }()

				// Corrupting a key will cause the ingestion to fail due to a
				// malformed key, rather than a block checksum mismatch.
				// Instead, we corrupt the last byte in the selected block,
				// before the trailer, which corresponds to a value.
				offset := bh.Offset + bh.Length - 1
				_, err = osF.WriteAt([]byte("\xff"), int64(offset))
				require.NoError(t, err)
			}

			type errT struct {
				errLoc errLocation
				err    error
			}
			runIngest := func(keyVals []keyVal) (et errT) {
				// The vfs.File does not allow for random reads and writes.
				// Create a disk-backed file outside of the DB FS that we can
				// open as a regular os.File, if required.
				tmpFS := vfs.Default
				f, err := tmpFS.Create(ingestTableName)
				require.NoError(t, err)
				defer func() { _ = tmpFS.Remove(ingestTableName) }()

				w := sstable.NewWriter(f, sstable.WriterOptions{
					BlockSize:   blockSize,     // Create many smaller blocks.
					Compression: NoCompression, // For simpler debugging.
				})
				for _, kv := range keyVals {
					require.NoError(t, w.Set(kv.key, kv.val))
				}
				require.NoError(t, w.Close())

				// Possibly corrupt the file.
				if tc.cLoc != corruptionLocationNone {
					f, err = tmpFS.Open(ingestTableName)
					require.NoError(t, err)
					corrupt(f)
				}

				// Copy the file into the DB's FS.
				_, err = vfs.Clone(tmpFS, fs, ingestTableName, ingestTableName)
				require.NoError(t, err)

				// Ingest the external table.
				err = d.Ingest([]string{ingestTableName})
				if err != nil {
					et.errLoc = errLocationIngest
					et.err = err
					return
				}

				// Wait for the validation on the sstable to complete.
				wg.Wait()

				// Return any error encountered during validation.
				if logger.err != nil {
					et.errLoc = errLocationValidation
					et.err = logger.err
				}

				return
			}

			// Construct a set of keys to ingest.
			var keyVals []keyVal
			for i := 0; i < nKeys; i++ {
				key := make([]byte, keySize)
				_, err = rng.Read(key)
				require.NoError(t, err)

				val := make([]byte, valSize)
				_, err = rng.Read(val)
				require.NoError(t, err)

				keyVals = append(keyVals, keyVal{key, val})
			}

			// Keys must be sorted.
			sort.Slice(keyVals, func(i, j int) bool {
				return d.cmp(keyVals[i].key, keyVals[j].key) <= 0
			})

			// Run the ingestion.
			et := runIngest(keyVals)

			// Assert we saw the errors we expect.
			switch tc.wantErrType {
			case errLocationNone:
				require.Equal(t, errLocationNone, et.errLoc)
				require.NoError(t, et.err)
			case errLocationIngest:
				require.Equal(t, errLocationIngest, et.errLoc)
				require.Error(t, et.err)
				require.True(t, errors.Is(et.err, base.ErrCorruption))
			case errLocationValidation:
				require.Equal(t, errLocationValidation, et.errLoc)
				require.Error(t, et.err)
				require.True(t, errors.Is(et.err, base.ErrCorruption))
			default:
				t.Fatalf("unknown wantErrType %T", tc.wantErrType)
			}
		})
	}
}

// BenchmarkManySSTables measures the cost of various operations with various
// counts of SSTables within the database.
func BenchmarkManySSTables(b *testing.B) {
	counts := []int{10, 1_000, 10_000, 100_000, 1_000_000}
	ops := []string{"ingest", "calculateInuseKeyRanges"}
	for _, op := range ops {
		b.Run(op, func(b *testing.B) {
			for _, count := range counts {
				b.Run(fmt.Sprintf("sstables=%d", count), func(b *testing.B) {
					mem := vfs.NewMem()
					d, err := Open("", &Options{
						FS: mem,
					})
					require.NoError(b, err)

					var paths []string
					for i := 0; i < count; i++ {
						n := fmt.Sprintf("%07d", i)
						f, err := mem.Create(n)
						require.NoError(b, err)
						w := sstable.NewWriter(f, sstable.WriterOptions{})
						require.NoError(b, w.Set([]byte(n), nil))
						require.NoError(b, w.Close())
						paths = append(paths, n)
					}
					require.NoError(b, d.Ingest(paths))

					{
						const broadIngest = "broad.sst"
						f, err := mem.Create(broadIngest)
						require.NoError(b, err)
						w := sstable.NewWriter(f, sstable.WriterOptions{})
						require.NoError(b, w.Set([]byte("0"), nil))
						require.NoError(b, w.Set([]byte("Z"), nil))
						require.NoError(b, w.Close())
						require.NoError(b, d.Ingest([]string{broadIngest}))
					}

					switch op {
					case "ingest":
						runBenchmarkManySSTablesIngest(b, d, mem, count)
					case "calculateInuseKeyRanges":
						runBenchmarkManySSTablesInUseKeyRanges(b, d, count)
					}
					require.NoError(b, d.Close())
				})
			}
		})
	}
}

func runBenchmarkManySSTablesIngest(b *testing.B, d *DB, fs vfs.FS, count int) {
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		n := fmt.Sprintf("%07d", count+i)
		f, err := fs.Create(n)
		require.NoError(b, err)
		w := sstable.NewWriter(f, sstable.WriterOptions{})
		require.NoError(b, w.Set([]byte(n), nil))
		require.NoError(b, w.Close())
		require.NoError(b, d.Ingest([]string{n}))
	}
}

func runBenchmarkManySSTablesInUseKeyRanges(b *testing.B, d *DB, count int) {
	// This benchmark is pretty contrived, but it's not easy to write a
	// microbenchmark for this in a more natural way. L6 has many files, and
	// L5 has 1 file spanning the entire breadth of L5.
	d.mu.Lock()
	defer d.mu.Unlock()
	v := d.mu.versions.currentVersion()
	b.ResetTimer()

	smallest := []byte("0")
	largest := []byte("z")
	for i := 0; i < b.N; i++ {
		_ = calculateInuseKeyRanges(v, d.cmp, 0, numLevels-1, smallest, largest)
	}
}
