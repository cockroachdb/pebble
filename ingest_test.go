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
			w := sstable.NewWriter(f, nil, LevelOptions{})
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

			w := sstable.NewWriter(f, nil, LevelOptions{})
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
				var cmpName string
				d.ScanArgs(t, "cmp", &cmpName)
				cmp := comparers[cmpName]
				if cmp == nil {
					return fmt.Sprintf("%s unknown comparer: %s", d.Cmd, cmpName)
				}
				for _, data := range strings.Split(d.Input, "\n") {
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
				}
				err := ingestSortAndVerify(cmp, meta)
				if err != nil {
					return fmt.Sprintf("%v\n", err)
				}
				for i := range meta {
					fmt.Fprintf(&buf, "%v-%v\n", meta[i].Smallest, meta[i].Largest)
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

					mem = newMemTable(opts)
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

	datadriven.RunTest(t, "testdata/ingest_target_level", func(d *datadriven.TestData) string {
		switch d.Cmd {
		case "define":
			vers = &version{}
			if len(d.Input) == 0 {
				return ""
			}
			for _, data := range strings.Split(d.Input, "\n") {
				parts := strings.Split(data, ":")
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
					vers.Files[level] = append(vers.Files[level], parseMeta(table))
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
				level := ingestTargetLevel(cmp, vers, &meta)
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
	})
	if err != nil {
		t.Fatal(err)
	}

	datadriven.RunTest(t, "testdata/ingest", func(td *datadriven.TestData) string {
		switch td.Cmd {
		case "build", "batch":
			b := d.NewIndexedBatch()
			if err := runBatchDefineCmd(td, b); err != nil {
				return err.Error()
			}

			switch td.Cmd {
			case "build":
				if len(td.CmdArgs) != 1 {
					return "build <path>: argument missing\n"
				}
				path := td.CmdArgs[0].String()

				f, err := mem.Create(path)
				if err != nil {
					return err.Error()
				}
				w := sstable.NewWriter(f, nil, LevelOptions{})
				iters := []internalIterator{
					b.newInternalIter(nil),
					b.newRangeDelIter(nil),
				}
				for _, iter := range iters {
					if iter == nil {
						continue
					}
					for key, val := iter.First(); key != nil; key, val = iter.Next() {
						tmp := *key
						tmp.SetSeqNum(0)
						if err := w.Add(tmp, val); err != nil {
							return err.Error()
						}
					}
					if err := iter.Close(); err != nil {
						return err.Error()
					}
				}
				if err := w.Close(); err != nil {
					return err.Error()
				}

			case "batch":
				if err := b.Commit(nil); err != nil {
					return err.Error()
				}
			}
			return ""

		case "ingest":
			var paths []string
			for _, arg := range td.CmdArgs {
				paths = append(paths, arg.String())
			}

			if err := d.Ingest(paths); err != nil {
				return err.Error()
			}
			for _, path := range paths {
				if err := mem.Remove(path); err != nil {
					return err.Error()
				}
			}
			return ""

		case "get":
			var buf bytes.Buffer
			for _, data := range strings.Split(td.Input, "\n") {
				v, err := d.Get([]byte(data))
				if err != nil {
					fmt.Fprintf(&buf, "%s: %s\n", data, err)
				} else {
					fmt.Fprintf(&buf, "%s:%s\n", data, v)
				}
			}
			return buf.String()

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

		case "lsm":
			d.mu.Lock()
			s := d.mu.versions.currentVersion().String()
			d.mu.Unlock()
			return s

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
	w := sstable.NewWriter(f, nil, LevelOptions{})
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
