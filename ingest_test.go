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
	"strings"
	"testing"
	"time"

	"github.com/kr/pretty"
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
	// TODO(peter): Test various cases for ingesting sstables into the correct
	// level of the LSM.
}

func TestIngestGlobalSeqNum(t *testing.T) {
	// TODO(peter): Test that the sequence number for entries added via ingestion
	// is correct.
}

func TestIngest(t *testing.T) {
	// TODO(peter): Test that ingest works end-to-end.
}
