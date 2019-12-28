// Copyright 2018 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package pebble

import (
	"bytes"
	"fmt"
	"strconv"
	"strings"
	"testing"

	"github.com/cockroachdb/pebble/internal/base"
	"github.com/cockroachdb/pebble/internal/datadriven"
	"github.com/cockroachdb/pebble/sstable"
	"github.com/cockroachdb/pebble/vfs"
)

func TestRangeDel(t *testing.T) {
	var d *DB

	datadriven.RunTest(t, "testdata/range_del", func(td *datadriven.TestData) string {
		switch td.Cmd {
		case "define":
			var err error
			if d, err = runDBDefineCmd(td, nil /* options */); err != nil {
				return err.Error()
			}

			d.mu.Lock()
			// Disable the "dynamic base level" code for this test.
			d.mu.versions.picker.baseLevel = 1
			s := fmt.Sprintf("mem: %d\n%s", len(d.mu.mem.queue), d.mu.versions.currentVersion())
			d.mu.Unlock()
			return s

		case "compact":
			if err := runCompactCmd(td, d); err != nil {
				return err.Error()
			}
			d.mu.Lock()
			// Disable the "dynamic base level" code for this test.
			d.mu.versions.picker.baseLevel = 1
			s := d.mu.versions.currentVersion().String()
			d.mu.Unlock()
			return s

		case "get":
			return runGetCmd(td, d)

		case "iter":
			snap := Snapshot{
				db:     d,
				seqNum: InternalKeySeqNumMax,
			}

			for _, arg := range td.CmdArgs {
				if len(arg.Vals) != 1 {
					return fmt.Sprintf("%s: %s=<value>", td.Cmd, arg.Key)
				}
				switch arg.Key {
				case "seq":
					var err error
					snap.seqNum, err = strconv.ParseUint(arg.Vals[0], 10, 64)
					if err != nil {
						return err.Error()
					}
				default:
					return fmt.Sprintf("%s: unknown arg: %s", td.Cmd, arg.Key)
				}
			}

			iter := snap.NewIter(nil)
			defer iter.Close()
			return runIterCmd(td, iter)

		default:
			return fmt.Sprintf("unknown command: %s", td.Cmd)
		}
	})
}

// Verify that range tombstones at higher levels do not unintentionally delete
// newer keys at lower levels. This test sets up one such scenario. The base
// problem is that range tombstones are not truncated to sstable boundaries on
// disk, only in memory.
func TestRangeDelCompactionTruncation(t *testing.T) {
	// Use a small target file size so that there is a single key per sstable.
	d, err := Open("", &Options{
		FS: vfs.NewMem(),
		Levels: []LevelOptions{
			{TargetFileSize: 100},
			{TargetFileSize: 100},
			{TargetFileSize: 1},
		},
		DebugCheck: true,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer d.Close()

	d.mu.Lock()
	d.mu.versions.dynamicBaseLevel = false
	d.mu.Unlock()

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

	if err := d.Set([]byte("a"), bytes.Repeat([]byte("b"), 100), nil); err != nil {
		t.Fatal(err)
	}
	snap1 := d.NewSnapshot()
	defer snap1.Close()
	// Flush so that each version of "a" ends up in its own L0 table. If we
	// allowed both versions in the same L0 table, compaction could trivially
	// move the single L0 table to L1.
	if err := d.Flush(); err != nil {
		t.Fatal(err)
	}
	if err := d.Set([]byte("b"), bytes.Repeat([]byte("c"), 100), nil); err != nil {
		t.Fatal(err)
	}
	snap2 := d.NewSnapshot()
	defer snap2.Close()
	if err := d.DeleteRange([]byte("a"), []byte("d"), nil); err != nil {
		t.Fatal(err)
	}

	// Compact to produce the L1 tables.
	if err := d.Compact([]byte("c"), []byte("c")); err != nil {
		t.Fatal(err)
	}
	expectLSM(`
1:
  8:[a#2,RANGEDEL-b#72057594037927935,RANGEDEL]
  9:[b#1,SET-d#72057594037927935,RANGEDEL]
`)

	// Compact again to move one of the tables to L2.
	if err := d.Compact([]byte("c"), []byte("c")); err != nil {
		t.Fatal(err)
	}
	expectLSM(`
1:
  8:[a#2,RANGEDEL-b#72057594037927935,RANGEDEL]
2:
  9:[b#1,SET-d#72057594037927935,RANGEDEL]
`)

	// Write "b" and "c" to a new table.
	if err := d.Set([]byte("b"), []byte("d"), nil); err != nil {
		t.Fatal(err)
	}
	if err := d.Set([]byte("c"), []byte("e"), nil); err != nil {
		t.Fatal(err)
	}
	if err := d.Flush(); err != nil {
		t.Fatal(err)
	}
	expectLSM(`
0:
  11:[b#3,SET-c#4,SET]
1:
  8:[a#2,RANGEDEL-b#72057594037927935,RANGEDEL]
2:
  9:[b#1,SET-d#72057594037927935,RANGEDEL]
`)

	// "b" is still visible at this point as it should be.
	if _, err := d.Get([]byte("b")); err != nil {
		t.Fatalf("expected success, but found %v", err)
	}

	keys := func() string {
		iter := d.NewIter(nil)
		defer iter.Close()
		var buf bytes.Buffer
		var sep string
		for iter.First(); iter.Valid(); iter.Next() {
			fmt.Fprintf(&buf, "%s%s", sep, iter.Key())
			sep = " "
		}
		return buf.String()
	}

	if expected, actual := `b c`, keys(); expected != actual {
		t.Fatalf("expected %q, but found %q", expected, actual)
	}

	// Compact the L0 table. This will compact the L0 table into L1 and do to the
	// sstable target size settings will create 2 tables in L1. Then L1 table
	// containing "c" will be compacted again with the L2 table creating two
	// tables in L2. Lastly, the L2 table containing "c" will be compacted
	// creating the L3 table.
	if err := d.Compact([]byte("c"), []byte("c")); err != nil {
		t.Fatal(err)
	}
	expectLSM(`
1:
  12:[a#2,RANGEDEL-b#72057594037927935,RANGEDEL]
3:
  17:[b#3,SET-b#3,SET]
  18:[b#1,SET-c#72057594037927935,RANGEDEL]
  19:[c#4,SET-d#72057594037927935,RANGEDEL]
`)

	// The L1 table still contains a tombstone from [a,d) which will improperly
	// delete the newer version of "b" in L2.
	if _, err := d.Get([]byte("b")); err != nil {
		t.Errorf("expected success, but found %v", err)
	}

	if expected, actual := `b c`, keys(); expected != actual {
		t.Errorf("expected %q, but found %q", expected, actual)
	}
}

// This is an alternate scenario to the one created in
// TestRangeDelCompactionTruncation that would result in the bounds for an
// sstable expanding to overlap its left neighbor if we failed to truncate an
// sstable's boundaries to the compaction input boundaries.
func TestRangeDelCompactionTruncation2(t *testing.T) {
	// Use a small target file size so that there is a single key per sstable.
	d, err := Open("", &Options{
		FS: vfs.NewMem(),
		Levels: []LevelOptions{
			{TargetFileSize: 100},
			{TargetFileSize: 100},
			{TargetFileSize: 1},
		},
		DebugCheck: true,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer d.Close()

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

	if err := d.Set([]byte("b"), bytes.Repeat([]byte("b"), 100), nil); err != nil {
		t.Fatal(err)
	}
	snap1 := d.NewSnapshot()
	defer snap1.Close()
	// Flush so that each version of "b" ends up in its own L0 table. If we
	// allowed both versions in the same L0 table, compaction could trivially
	// move the single L0 table to L1.
	if err := d.Flush(); err != nil {
		t.Fatal(err)
	}
	if err := d.Set([]byte("b"), bytes.Repeat([]byte("c"), 100), nil); err != nil {
		t.Fatal(err)
	}
	snap2 := d.NewSnapshot()
	defer snap2.Close()
	if err := d.DeleteRange([]byte("a"), []byte("d"), nil); err != nil {
		t.Fatal(err)
	}

	// Compact to produce the L1 tables.
	if err := d.Compact([]byte("b"), []byte("b")); err != nil {
		t.Fatal(err)
	}
	expectLSM(`
6:
  8:[a#2,RANGEDEL-b#1,SET]
  9:[b#0,RANGEDEL-d#72057594037927935,RANGEDEL]
`)

	if err := d.Set([]byte("c"), bytes.Repeat([]byte("d"), 100), nil); err != nil {
		t.Fatal(err)
	}

	if err := d.Compact([]byte("c"), []byte("c")); err != nil {
		t.Fatal(err)
	}
	expectLSM(`
6:
  12:[a#2,RANGEDEL-b#1,SET]
  13:[b#0,RANGEDEL-c#72057594037927935,RANGEDEL]
  14:[c#3,SET-d#72057594037927935,RANGEDEL]
`)
}

// TODO(peter): rewrite this test, TestRangeDelCompactionTruncation, and
// TestRangeDelCompactionTruncation2 as data-driven tests.
func TestRangeDelCompactionTruncation3(t *testing.T) {
	// Use a small target file size so that there is a single key per sstable.
	d, err := Open("tmp", &Options{
		Cleaner: ArchiveCleaner{},
		FS:      vfs.NewMem(),
		Levels: []LevelOptions{
			{TargetFileSize: 100},
			{TargetFileSize: 100},
			{TargetFileSize: 1},
		},
		DebugCheck: true,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer d.Close()

	d.mu.Lock()
	d.mu.versions.dynamicBaseLevel = false
	d.mu.Unlock()

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

	if err := d.Set([]byte("b"), bytes.Repeat([]byte("b"), 100), nil); err != nil {
		t.Fatal(err)
	}
	snap1 := d.NewSnapshot()
	defer snap1.Close()

	// Flush so that each version of "b" ends up in its own L0 table. If we
	// allowed both versions in the same L0 table, compaction could trivially
	// move the single L0 table to L1.
	if err := d.Flush(); err != nil {
		t.Fatal(err)
	}
	if err := d.Set([]byte("b"), bytes.Repeat([]byte("c"), 100), nil); err != nil {
		t.Fatal(err)
	}
	snap2 := d.NewSnapshot()
	defer snap2.Close()

	if err := d.DeleteRange([]byte("a"), []byte("d"), nil); err != nil {
		t.Fatal(err)
	}
	snap3 := d.NewSnapshot()
	defer snap3.Close()

	if _, err := d.Get([]byte("b")); err != ErrNotFound {
		t.Fatalf("expected not found, but found %v", err)
	}

	// Compact a few times to move the tables down to L3.
	for i := 0; i < 3; i++ {
		if err := d.Compact([]byte("b"), []byte("b")); err != nil {
			t.Fatal(err)
		}
	}
	expectLSM(`
3:
  12:[a#2,RANGEDEL-b#1,SET]
  13:[b#0,RANGEDEL-d#72057594037927935,RANGEDEL]
`)

	if err := d.Set([]byte("c"), bytes.Repeat([]byte("d"), 100), nil); err != nil {
		t.Fatal(err)
	}

	if err := d.Compact([]byte("c"), []byte("c")); err != nil {
		t.Fatal(err)
	}
	expectLSM(`
4:
  20:[a#2,RANGEDEL-b#1,SET]
  21:[b#0,RANGEDEL-c#72057594037927935,RANGEDEL]
  22:[c#3,SET-d#72057594037927935,RANGEDEL]
`)

	if err := d.Compact([]byte("c"), []byte("c")); err != nil {
		t.Fatal(err)
	}
	expectLSM(`
5:
  23:[a#2,RANGEDEL-b#1,SET]
  24:[b#0,RANGEDEL-c#72057594037927935,RANGEDEL]
  25:[c#3,SET-d#72057594037927935,RANGEDEL]
`)

	if _, err := d.Get([]byte("b")); err != ErrNotFound {
		t.Fatalf("expected not found, but found %v", err)
	}

	if err := d.Compact([]byte("a"), []byte("a")); err != nil {
		t.Fatal(err)
	}
	expectLSM(`
5:
  25:[c#3,SET-d#72057594037927935,RANGEDEL]
6:
  26:[a#2,RANGEDEL-b#1,SET]
  27:[b#0,RANGEDEL-c#72057594037927935,RANGEDEL]
`)

	if v, err := d.Get([]byte("b")); err != ErrNotFound {
		t.Fatalf("expected not found, but found %v [%s]", err, v)
	}
}

func BenchmarkRangeDelIterate(b *testing.B) {
	for _, entries := range []int{10, 1000, 100000} {
		b.Run(fmt.Sprintf("entries=%d", entries), func(b *testing.B) {
			for _, deleted := range []int{entries, entries - 1} {
				b.Run(fmt.Sprintf("deleted=%d", deleted), func(b *testing.B) {
					mem := vfs.NewMem()
					d, err := Open("", &Options{
						Cache:      NewCache(128 << 20), // 128 MB
						FS:         mem,
						DebugCheck: true,
					})
					if err != nil {
						b.Fatal(err)
					}
					defer d.Close()

					makeKey := func(i int) []byte {
						return []byte(fmt.Sprintf("%09d", i))
					}

					// Create an sstable with N entries and ingest it. This is a fast way
					// to get a lot of entries into pebble.
					f, err := mem.Create("ext")
					if err != nil {
						b.Fatal(err)
					}
					w := sstable.NewWriter(f, sstable.WriterOptions{
						BlockSize: 32 << 10, // 32 KB
					})
					for i := 0; i < entries; i++ {
						key := base.MakeInternalKey(makeKey(i), 0, InternalKeyKindSet)
						if err := w.Add(key, nil); err != nil {
							b.Fatal(err)
						}
					}
					if err := w.Close(); err != nil {
						b.Fatal(err)
					}
					if err := d.Ingest([]string{"ext"}); err != nil {
						b.Fatal(err)
					}
					if err := mem.Remove("ext"); err != nil {
						b.Fatal(err)
					}

					// Create a range tombstone that deletes most (or all) of those entries.
					from := makeKey(0)
					to := makeKey(deleted)
					if err := d.DeleteRange(from, to, nil); err != nil {
						b.Fatal(err)
					}

					b.ResetTimer()
					for i := 0; i < b.N; i++ {
						iter := d.NewIter(nil)
						iter.SeekGE(from)
						if deleted < entries {
							if !iter.Valid() {
								b.Fatal("key not found")
							}
						} else if iter.Valid() {
							b.Fatal("unexpected key found")
						}
						if err := iter.Close(); err != nil {
							b.Fatal(err)
						}
					}
				})
			}
		})
	}
}
