// Copyright 2018 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package pebble

import (
	"bytes"
	"fmt"
	"io/ioutil"
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
	"github.com/kr/pretty"
	"golang.org/x/exp/rand"
)

func TestIngestLoad(t *testing.T) {
	mem := vfs.NewMem()

	datadriven.RunTest(t, "testdata/ingest_load", func(td *datadriven.TestData) string {
		switch td.Cmd {
		case "load":
			f, err := mem.Create("ext")
			if err != nil {
				return err.Error()
			}
			w := sstable.NewWriter(f, sstable.WriterOptions{})
			for _, data := range strings.Split(td.Input, "\n") {
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
			meta, _, err := ingestLoad(opts, []string{"ext"}, 0, []uint64{1})
			if err != nil {
				return err.Error()
			}
			var buf bytes.Buffer
			for _, m := range meta {
				fmt.Fprintf(&buf, "%d: %s-%s\n", m.FileNum, m.Smallest, m.Largest)
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

	randBytes := func(size int) []byte {
		data := make([]byte, size)
		for i := range data {
			data[i] = byte(rng.Int() & 0xff)
		}
		return data
	}

	paths := make([]string, 1+rng.Intn(10))
	pending := make([]uint64, len(paths))
	expected := make([]*fileMetadata, len(paths))
	for i := range paths {
		paths[i] = fmt.Sprint(i)
		pending[i] = uint64(rng.Int63())
		expected[i] = &fileMetadata{
			FileNum: pending[i],
		}

		func() {
			f, err := mem.Create(paths[i])
			if err != nil {
				t.Fatal(err)
			}
			defer f.Close()

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

			expected[i].Smallest = keys[0]
			expected[i].Largest = keys[len(keys)-1]

			w := sstable.NewWriter(f, sstable.WriterOptions{})
			for i := range keys {
				if i > 0 && base.InternalCompare(cmp, keys[i-1], keys[i]) == 0 {
					// Duplicate key, ignore.
					continue
				}
				w.Add(keys[i], nil)
			}
			if err := w.Close(); err != nil {
				t.Fatal(err)
			}

			meta, err := w.Metadata()
			if err != nil {
				t.Fatal(err)
			}
			expected[i].Size = meta.Size
		}()
	}

	opts := &Options{
		Comparer: DefaultComparer,
		FS:       mem,
	}
	meta, _, err := ingestLoad(opts, paths, 0, pending)
	if err != nil {
		t.Fatal(err)
	}
	if diff := pretty.Diff(expected, meta); diff != nil {
		t.Fatalf("%s", strings.Join(diff, "\n"))
	}
}

func TestIngestLoadInvalid(t *testing.T) {
	mem := vfs.NewMem()
	f, err := mem.Create("invalid")
	if err != nil {
		t.Fatal(err)
	}
	f.Close()

	opts := &Options{
		Comparer: DefaultComparer,
		FS:       mem,
	}
	if _, _, err := ingestLoad(opts, []string{"invalid"}, 0, []uint64{1}); err == nil {
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
		datadriven.RunTest(t, "testdata/ingest_sort_and_verify", func(d *datadriven.TestData) string {
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
					meta = append(meta, &fileMetadata{
						Smallest: smallest,
						Largest:  largest,
					})
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
			if err := mem.MkdirAll(dir, 0755); err != nil {
				t.Fatal(err)
			}

			paths := make([]string, 10)
			meta := make([]*fileMetadata, len(paths))
			contents := make([][]byte, len(paths))
			for j := range paths {
				paths[j] = fmt.Sprintf("external%d", j)
				meta[j] = &fileMetadata{}
				meta[j].FileNum = uint64(j)
				f, err := mem.Create(paths[j])
				if err != nil {
					t.Fatal(err)
				}
				contents[j] = []byte(fmt.Sprintf("data%d", j))
				if _, err := f.Write(contents[j]); err != nil {
					t.Fatal(err)
				}
				f.Close()
			}

			if i < count {
				mem.Remove(paths[i])
			}

			err := ingestLink(0 /* jobID */, opts, dir, paths, meta)
			if i < count {
				if err == nil {
					t.Fatalf("expected error, but found success")
				}
			} else if err != nil {
				t.Fatal(err)
			}

			files, err := mem.List(dir)
			if err != nil {
				t.Fatal(err)
			}
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
					if uint64(j) != fileNum {
						t.Fatalf("expected table %d, but found %d", j, fileNum)
					}
					f, err := mem.Open(mem.PathJoin(dir, files[j]))
					if err != nil {
						t.Fatal(err)
					}
					data, err := ioutil.ReadAll(f)
					if err != nil {
						t.Fatal(err)
					}
					f.Close()
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
	if err != nil {
		t.Fatal(err)
	}

	opts := &Options{FS: &errorFS{mem, 0}}
	opts.EnsureDefaults()

	meta := []*fileMetadata{{FileNum: 1}}
	if err := ingestLink(0, opts, "", []string{"source"}, meta); err != nil {
		t.Fatal(err)
	}

	dest, err := mem.Open("000001.sst")
	if err != nil {
		t.Fatal(err)
	}

	// We should be able to write bytes to src, and not have them show up in
	// dest.
	_, _ = src.Write([]byte("test"))
	data, err := ioutil.ReadAll(dest)
	if err != nil {
		t.Fatal(err)
	}
	if len(data) != 0 {
		t.Fatalf("expected copy, but files appear to be hard linked: [%s] unexpectedly found", data)
	}
}

func TestIngestMemtableOverlaps(t *testing.T) {
	comparers := []Comparer{
		{Name: "default", Compare: DefaultComparer.Compare},
		{Name: "reverse", Compare: func(a, b []byte) int {
			return DefaultComparer.Compare(b, a)
		}},
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
				if len(parts) != 2 {
					t.Fatalf("malformed table spec: %s", s)
				}
				if mem.cmp([]byte(parts[0]), []byte(parts[1])) > 0 {
					parts[0], parts[1] = parts[1], parts[0]
				}
				return &fileMetadata{
					Smallest: InternalKey{UserKey: []byte(parts[0])},
					Largest:  InternalKey{UserKey: []byte(parts[1])},
				}
			}

			datadriven.RunTest(t, "testdata/ingest_memtable_overlaps", func(d *datadriven.TestData) string {
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

					mem = newMemTable(opts, 0 /* size */, nil /* reservation */)
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

func TestIngestTargetLevel(t *testing.T) {
	cmp := DefaultComparer.Compare
	var vers *version
	var compactions map[*compaction]struct{}

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

	parseCompaction := func(outputLevel int, s string) *compaction {
		m := parseMeta(s[len("compact:"):])
		return &compaction{
			outputLevel: outputLevel,
			smallest:    m.Smallest,
			largest:     m.Largest,
		}
	}

	datadriven.RunTest(t, "testdata/ingest_target_level", func(d *datadriven.TestData) string {
		switch d.Cmd {
		case "define":
			vers = &version{}
			compactions = make(map[*compaction]struct{})
			if len(d.Input) == 0 {
				return ""
			}
			for _, data := range strings.Split(d.Input, "\n") {
				parts := strings.SplitN(data, ":", 2)
				if len(parts) != 2 {
					return fmt.Sprintf("malformed test:\n%s", d.Input)
				}
				level, err := strconv.Atoi(parts[0])
				if err != nil {
					return err.Error()
				}
				if vers.Files[level] != nil {
					return fmt.Sprintf("level %d already filled", level)
				}
				for _, table := range strings.Fields(parts[1]) {
					if strings.HasPrefix(table, "compact:") {
						compactions[parseCompaction(level, table)] = struct{}{}
					} else {
						vers.Files[level] = append(vers.Files[level], parseMeta(table))
					}
				}

				if level == 0 {
					manifest.SortBySeqNum(vers.Files[level])
				} else {
					manifest.SortBySmallest(vers.Files[level], cmp)
				}
			}
			return ""

		case "target":
			var buf bytes.Buffer
			for _, target := range strings.Split(d.Input, "\n") {
				meta := parseMeta(target)
				level := ingestTargetLevel(cmp, vers, compactions, &meta)
				fmt.Fprintf(&buf, "%d\n", level)
			}
			return buf.String()

		default:
			return fmt.Sprintf("unknown command: %s", d.Cmd)
		}
	})
}

func TestIngest(t *testing.T) {
	mem := vfs.NewMem()
	err := mem.MkdirAll("ext", 0755)
	if err != nil {
		t.Fatal(err)
	}

	d, err := Open("", &Options{
		FS:                    mem,
		L0CompactionThreshold: 100,
		L0StopWritesThreshold: 100,
	})
	if err != nil {
		t.Fatal(err)
	}

	datadriven.RunTest(t, "testdata/ingest", func(td *datadriven.TestData) string {
		switch td.Cmd {
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
			if err := runIngestCmd(td, d, mem); err != nil {
				return err.Error()
			}
			return ""

		case "get":
			return runGetCmd(td, d)

		case "iter":
			iter := d.NewIter(nil)
			defer iter.Close()
			return runIterCmd(td, iter)

		case "lsm":
			return runLSMCmd(td, d)

		default:
			return fmt.Sprintf("unknown command: %s", td.Cmd)
		}
	})
}

func TestIngestCompact(t *testing.T) {
	var buf syncedBuffer
	mem := vfs.NewMem()
	d, err := Open("", &Options{
		EventListener:         MakeLoggingEventListener(&buf),
		FS:                    mem,
		L0CompactionThreshold: 1,
		L0StopWritesThreshold: 1,
	})
	if err != nil {
		t.Fatal(err)
	}

	f, err := mem.Create("ext")
	if err != nil {
		t.Fatal(err)
	}
	w := sstable.NewWriter(f, sstable.WriterOptions{})
	key := []byte("a")
	if err := w.Add(base.MakeInternalKey(key, 0, InternalKeyKindSet), nil); err != nil {
		t.Fatal(err)
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}

	// Ingest the same sstable multiple times. Compaction should take place as
	// ingestion happens, preventing an indefinite write stall from occurring.
	for i := 0; i < 20; i++ {
		if i == 10 {
			// Half-way through the ingestions, set a key in the memtable to force
			// overlap with the memtable which will require the memtable to be
			// flushed.
			if err := d.Set(key, nil, nil); err != nil {
				t.Fatal(err)
			}
		}
		if err := d.Ingest([]string{"ext"}); err != nil {
			t.Fatal(err)
		}
	}
}

func TestConcurrentIngest(t *testing.T) {
	mem := vfs.NewMem()
	d, err := Open("", &Options{
		FS: mem,
	})
	if err != nil {
		t.Fatal(err)
	}

	// Create an sstable with 2 keys. This is necessary to trigger the overlap
	// bug because an sstable with a single key will not have overlap in internal
	// key space and the sequence number assignment had already guaranteed
	// correct ordering.
	f, err := mem.Create("ext")
	if err != nil {
		t.Fatal(err)
	}
	w := sstable.NewWriter(f, sstable.WriterOptions{})
	if err := w.Set([]byte("a"), nil); err != nil {
		t.Fatal(err)
	}
	if err := w.Set([]byte("b"), nil); err != nil {
		t.Fatal(err)
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}

	// Ingest the same sstable multiple times concurrently.
	errCh := make(chan error, 5)
	for i := 0; i < cap(errCh); i++ {
		go func() {
			errCh <- d.Ingest([]string{"ext"})
		}()
	}
	for i := 0; i < cap(errCh); i++ {
		err := <-errCh
		if err != nil {
			t.Fatal(err)
		}
	}
}

func TestConcurrentIngestCompact(t *testing.T) {
	mem := vfs.NewMem()
	compactionReady := make(chan struct{})
	compactionBegin := make(chan struct{})
	d, err := Open("", &Options{
		FS: mem,
		EventListener: EventListener{
			TableCreated: func(info TableCreateInfo) {
				if info.Reason == "compacting" {
					close(compactionReady)
					<-compactionBegin
				}
			},
		},
	})
	if err != nil {
		t.Fatal(err)
	}

	ingest := func(keys ...string) {
		t.Helper()
		f, err := mem.Create("ext")
		if err != nil {
			t.Fatal(err)
		}
		w := sstable.NewWriter(f, sstable.WriterOptions{})
		for _, k := range keys {
			if err := w.Set([]byte(k), nil); err != nil {
				t.Fatal(err)
			}
		}
		if err := w.Close(); err != nil {
			t.Fatal(err)
		}
		if err := d.Ingest([]string{"ext"}); err != nil {
			t.Fatal(err)
		}
	}

	compact := func(start, end string) {
		t.Helper()
		if err := d.Compact([]byte(start), []byte(end)); err != nil {
			t.Fatal(err)
		}
	}

	lsm := func() string {
		d.mu.Lock()
		s := d.mu.versions.currentVersion().DebugString(base.DefaultFormatter)
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
5:
  5:[a#2,SET-a#2,SET]
  7:[c#4,SET-c#4,SET]
6:
  4:[a#1,SET-a#1,SET]
  6:[c#3,SET-c#3,SET]
`)

	// At this point ingestion of an sstable containing only key "b" will be
	// targetted at L6. Yet a concurrent compaction of sstables 5 and 7 will
	// create a new sstable in L6 spanning ["a"-"c"]. So the ingestion must
	// actually target L5.

	go func() {
		<-compactionReady

		ingest("b")

		close(compactionBegin)
	}()

	compact("a", "z")

	expectLSM(`
5:
  9:[b#5,SET-b#5,SET]
6:
  8:[a#0,SET-c#0,SET]
`)
}
