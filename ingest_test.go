// Copyright 2018 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package pebble

import (
	"bytes"
	"fmt"
	"io/ioutil"
	"math/rand"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/kr/pretty"
	"github.com/petermattis/pebble/datadriven"
	"github.com/petermattis/pebble/db"
	"github.com/petermattis/pebble/sstable"
	"github.com/petermattis/pebble/storage"
)

func TestIngestLoad(t *testing.T) {
	mem := storage.NewMem()
	rng := rand.New(rand.NewSource(time.Now().UnixNano()))
	cmp := db.DefaultComparer.Compare

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
			fileNum: pending[i],
		}

		func() {
			f, err := mem.Create(paths[i])
			if err != nil {
				t.Fatal(err)
			}
			defer f.Close()

			keys := make([]db.InternalKey, 1+rng.Intn(100))
			for i := range keys {
				keys[i] = db.MakeInternalKey(
					randBytes(1+rng.Intn(10)),
					uint64(rng.Int63n(int64(db.InternalKeySeqNumMax))),
					db.InternalKeyKindSet)
			}
			sort.Slice(keys, func(i, j int) bool {
				return db.InternalCompare(cmp, keys[i], keys[j]) < 0
			})

			expected[i].smallest = keys[0]
			expected[i].largest = keys[len(keys)-1]

			w := sstable.NewWriter(f, nil, db.LevelOptions{})
			for i := range keys {
				w.Add(keys[i], nil)
			}
			if err := w.Close(); err != nil {
				t.Fatal(err)
			}

			stat, err := w.Stat()
			if err != nil {
				t.Fatal(err)
			}
			expected[i].size = uint64(stat.Size())
		}()
	}

	opts := &db.Options{
		Comparer: db.DefaultComparer,
		Storage:  mem,
	}
	meta, err := ingestLoad(opts, paths, pending)
	if err != nil {
		t.Fatal(err)
	}
	if diff := pretty.Diff(expected, meta); diff != nil {
		t.Fatalf("%s", strings.Join(diff, "\n"))
	}
}

func TestIngestLoadNonExistent(t *testing.T) {
	opts := &db.Options{
		Comparer: db.DefaultComparer,
		Storage:  storage.NewMem(),
	}
	if _, err := ingestLoad(opts, []string{"non-existent"}, []uint64{1}); err == nil {
		t.Fatalf("expected error, but found success")
	}
}

func TestIngestLoadEmpty(t *testing.T) {
	mem := storage.NewMem()
	f, err := mem.Create("empty")
	if err != nil {
		t.Fatal(err)
	}
	f.Close()

	opts := &db.Options{
		Comparer: db.DefaultComparer,
		Storage:  mem,
	}
	if _, err := ingestLoad(opts, []string{"empty"}, []uint64{1}); err == nil {
		t.Fatalf("expected error, but found success")
	}
}

func TestIngestSortAndVerify(t *testing.T) {
	isError := func(err error, re string) bool {
		if err == nil && re == "" {
			return true
		}
		if err == nil || re == "" {
			return false
		}
		matched, merr := regexp.MatchString(re, err.Error())
		if merr != nil {
			return false
		}
		return matched
	}

	testCases := []struct {
		input    string
		expected string
	}{
		{"", ""},
		{"a-b", ""},
		{"a-b c-d e-f", ""},
		{"c-d a-b e-f", ""},
		{"a-b b-d e-f", "files have overlapping ranges"},
		{"c-d d-e a-b", "files have overlapping ranges"},
	}

	comparers := []struct {
		name string
		cmp  db.Compare
	}{
		{"default", db.DefaultComparer.Compare},
		{"reverse", func(a, b []byte) int { return db.DefaultComparer.Compare(b, a) }},
	}

	for _, comparer := range comparers {
		t.Run(comparer.name, func(t *testing.T) {
			cmp := comparer.cmp
			for _, c := range testCases {
				t.Run("", func(t *testing.T) {
					var meta []*fileMetadata
					for _, p := range strings.Fields(c.input) {
						parts := strings.Split(p, "-")
						if len(parts) != 2 {
							t.Fatalf("malformed test case: %s", c.input)
						}
						if cmp([]byte(parts[0]), []byte(parts[1])) > 0 {
							parts[0], parts[1] = parts[1], parts[0]
						}
						meta = append(meta, &fileMetadata{
							smallest: db.InternalKey{UserKey: []byte(parts[0])},
							largest:  db.InternalKey{UserKey: []byte(parts[1])},
						})
					}
					if err := ingestSortAndVerify(cmp, meta); !isError(err, c.expected) {
						t.Fatalf("expected %s, but found %v", c.expected, err)
					}
					sorted := sort.SliceIsSorted(meta, func(i, j int) bool {
						return cmp(meta[i].smallest.UserKey, meta[j].smallest.UserKey) < 0
					})
					if !sorted {
						t.Fatalf("expected files to be sorted")
					}
				})
			}
		})
	}
}

func TestIngestLink(t *testing.T) {
	// Test linking of tables into the DB directory. Test cleanup when one of the
	// tables cannot be linked.

	const db = "db"
	const count = 10
	for i := 0; i <= count; i++ {
		t.Run("", func(t *testing.T) {
			mem := storage.NewMem()
			if err := mem.MkdirAll(db, 0755); err != nil {
				t.Fatal(err)
			}

			paths := make([]string, 10)
			meta := make([]*fileMetadata, len(paths))
			contents := make([][]byte, len(paths))
			for j := range paths {
				paths[j] = fmt.Sprintf("external%d", j)
				meta[j] = &fileMetadata{}
				meta[j].fileNum = uint64(j)
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

			err := ingestLink(mem, db, paths, meta)
			if i < count {
				if err == nil {
					t.Fatalf("expected error, but found success")
				}
			} else if err != nil {
				t.Fatal(err)
			}

			files, err := mem.List(db)
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
					ftype, fileNum, ok := parseDBFilename(files[j])
					if !ok {
						t.Fatalf("unable to parse filename: %s", files[j])
					}
					if fileTypeTable != ftype {
						t.Fatalf("expected table, but found %d", ftype)
					}
					if uint64(j) != fileNum {
						t.Fatalf("expected table %d, but found %d", j, fileNum)
					}
					f, err := mem.Open(db + "/" + files[j])
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

func TestIngestMemtableOverlaps(t *testing.T) {
	testCases := []struct {
		ingest   string
		memtable string
		expected bool
	}{
		{"a-b", "c", false},
		{"a-b", "a", true},
		{"a-b", "b", true},
		{"b-c", "a", false},
		{"b-c e-f", "a d g", false},
		{"b-c e-f", "a d e g", true},
		{"b-c e-f", "a c d g", true},
	}

	comparers := []db.Comparer{
		{Name: "default", Compare: db.DefaultComparer.Compare},
		{Name: "reverse", Compare: func(a, b []byte) int {
			return db.DefaultComparer.Compare(b, a)
		}},
	}

	for _, comparer := range comparers {
		t.Run(comparer.Name, func(t *testing.T) {
			cmp := comparer.Compare
			for _, c := range testCases {
				t.Run("", func(t *testing.T) {
					var meta []*fileMetadata
					for _, p := range strings.Fields(c.ingest) {
						parts := strings.Split(p, "-")
						if len(parts) != 2 {
							t.Fatalf("malformed test case: %s", c.ingest)
						}
						if cmp([]byte(parts[0]), []byte(parts[1])) > 0 {
							parts[0], parts[1] = parts[1], parts[0]
						}
						meta = append(meta, &fileMetadata{
							smallest: db.InternalKey{UserKey: []byte(parts[0])},
							largest:  db.InternalKey{UserKey: []byte(parts[1])},
						})
					}

					opts := &db.Options{
						Comparer: &comparer,
					}
					opts.EnsureDefaults()
					mem := newMemTable(opts)
					for _, key := range strings.Fields(c.memtable) {
						if err := mem.set(db.InternalKey{UserKey: []byte(key)}, nil); err != nil {
							t.Fatal(err)
						}
					}

					result := ingestMemtableOverlaps(mem, meta)
					if c.expected != result {
						t.Fatalf("expected %t, but found %t", c.expected, result)
					}
				})
			}
		})
	}
}

func TestIngestTargetLevel(t *testing.T) {
	cmp := db.DefaultComparer.Compare
	var vers *version

	parseMeta := func(s string) fileMetadata {
		parts := strings.Split(s, "-")
		if len(parts) != 2 {
			t.Fatalf("malformed table spec: %s", s)
		}
		return fileMetadata{
			smallest: db.InternalKey{UserKey: []byte(parts[0])},
			largest:  db.InternalKey{UserKey: []byte(parts[1])},
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
					t.Fatalf("malformed test:\n%s", d.Input)
				}
				level, err := strconv.Atoi(parts[0])
				if err != nil {
					t.Fatal(err)
				}
				if vers.files[level] != nil {
					t.Fatalf("level %d already filled", level)
				}
				for _, table := range strings.Fields(parts[1]) {
					vers.files[level] = append(vers.files[level], parseMeta(table))
				}

				if level == 0 {
					sort.Sort(byFileNum(vers.files[level]))
				} else {
					sort.Sort(bySmallest{vers.files[level], cmp})
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
			t.Fatalf("unknown command: %s", d.Cmd)
			return ""
		}
	})
}

func TestIngest(t *testing.T) {
	fs := storage.NewMem()
	err := fs.MkdirAll("ext", 0755)
	if err != nil {
		t.Fatal(err)
	}

	d, err := Open("", &db.Options{
		Storage: fs,
	})
	if err != nil {
		t.Fatal(err)
	}

	datadriven.RunTest(t, "testdata/ingest", func(td *datadriven.TestData) string {
		switch td.Cmd {
		case "ingest", "batch":
			b := d.NewIndexedBatch()
			for _, line := range strings.Split(td.Input, "\n") {
				parts := strings.Fields(line)
				if len(parts) == 0 {
					continue
				}
				var err error
				switch parts[0] {
				case "set":
					if len(parts) != 3 {
						t.Fatalf("%s expects 2 arguments", parts[0])
					}
					err = b.Set([]byte(parts[1]), []byte(parts[2]), nil)
				case "del":
					if len(parts) != 2 {
						t.Fatalf("%s expects 1 argument", parts[0])
					}
					err = b.Delete([]byte(parts[1]), nil)
				case "merge":
					if len(parts) != 3 {
						t.Fatalf("%s expects 2 arguments", parts[0])
					}
					err = b.Merge([]byte(parts[1]), []byte(parts[2]), nil)
				default:
					t.Fatalf("unknown op: %s", parts[0])
				}
				if err != nil {
					t.Fatal(err)
				}
			}

			switch td.Cmd {
			case "ingest":
				f, err := fs.Create("ext/0")
				if err != nil {
					t.Fatal(err)
				}
				w := sstable.NewWriter(f, nil, db.LevelOptions{})
				iter := b.newInternalIter(nil)
				for iter.First(); iter.Valid(); iter.Next() {
					key := iter.Key()
					key.SetSeqNum(10000)
					w.Add(key, iter.Value())
				}
				iter.Close()
				w.Close()

				if err := d.Ingest([]string{"ext/0"}); err != nil {
					t.Fatal(err)
				}
				if err := fs.Remove("ext/0"); err != nil {
					t.Fatal(err)
				}

			case "batch":
				b.Commit(nil)
			}

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
			t.Fatalf("unknown command: %s", td.Cmd)
		}
		return ""
	})
}
