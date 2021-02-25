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
	"time"

	"github.com/cockroachdb/pebble/internal/base"
	"github.com/cockroachdb/pebble/internal/datadriven"
	"github.com/cockroachdb/pebble/sstable"
	"github.com/cockroachdb/pebble/vfs"
	"github.com/stretchr/testify/require"
)

func TestRangeDel(t *testing.T) {
	var d *DB
	defer func() {
		if d != nil {
			require.NoError(t, d.Close())
		}
	}()
	opts := &Options{}
	opts.private.disableAutomaticCompactions = true

	datadriven.RunTest(t, "testdata/range_del", func(td *datadriven.TestData) string {
		switch td.Cmd {
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
			// Disable the "dynamic base level" code for this test.
			d.mu.versions.picker.forceBaseLevel1()
			s := fmt.Sprintf("mem: %d\n%s", len(d.mu.mem.queue), d.mu.versions.currentVersion())
			d.mu.Unlock()
			return s

		case "wait-pending-table-stats":
			return runTableStatsCmd(td, d)

		case "compact":
			if err := runCompactCmd(td, d); err != nil {
				return err.Error()
			}
			d.mu.Lock()
			// Disable the "dynamic base level" code for this test.
			d.mu.versions.picker.forceBaseLevel1()
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
			return runIterCmd(td, iter, true)

		default:
			return fmt.Sprintf("unknown command: %s", td.Cmd)
		}
	})
}

func TestDeleteRangeFlushDelay(t *testing.T) {
	opts := &Options{FS: vfs.NewMem()}
	opts.Experimental.DeleteRangeFlushDelay = 10 * time.Millisecond
	d, err := Open("", opts)
	require.NoError(t, err)

	// Ensure that all the various means of deleting a range trigger the flush
	// delay.
	cases := []func(){
		func() {
			require.NoError(t, d.DeleteRange([]byte("a"), []byte("z"), nil))
		},
		func() {
			b := d.NewBatch()
			require.NoError(t, b.DeleteRange([]byte("a"), []byte("z"), nil))
			require.NoError(t, b.Commit(nil))
		},
		func() {
			b := d.NewBatch()
			op := b.DeleteRangeDeferred(1, 1)
			op.Key[0] = 'a'
			op.Value[0] = 'z'
			op.Finish()
			require.NoError(t, b.Commit(nil))
		},
		func() {
			b := d.NewBatch()
			b2 := d.NewBatch()
			require.NoError(t, b.DeleteRange([]byte("a"), []byte("z"), nil))
			require.NoError(t, b2.SetRepr(b.Repr()))
			require.NoError(t, b2.Commit(nil))
			require.NoError(t, b.Close())
		},
		func() {
			b := d.NewBatch()
			b2 := d.NewBatch()
			require.NoError(t, b.DeleteRange([]byte("a"), []byte("z"), nil))
			require.NoError(t, b2.Apply(b, nil))
			require.NoError(t, b2.Commit(nil))
			require.NoError(t, b.Close())
		},
	}

	for _, f := range cases {
		d.mu.Lock()
		flushed := d.mu.mem.queue[len(d.mu.mem.queue)-1].flushed
		d.mu.Unlock()
		f()
		<-flushed
	}
	require.NoError(t, d.Close())
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
		DebugCheck: DebugCheckLevels,
	})
	require.NoError(t, err)
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

	require.NoError(t, d.Set([]byte("a"), bytes.Repeat([]byte("b"), 100), nil))
	snap1 := d.NewSnapshot()
	defer snap1.Close()
	// Flush so that each version of "a" ends up in its own L0 table. If we
	// allowed both versions in the same L0 table, compaction could trivially
	// move the single L0 table to L1.
	require.NoError(t, d.Flush())
	require.NoError(t, d.Set([]byte("b"), bytes.Repeat([]byte("c"), 100), nil))

	snap2 := d.NewSnapshot()
	defer snap2.Close()
	require.NoError(t, d.DeleteRange([]byte("a"), []byte("d"), nil))

	// Compact to produce the L1 tables.
	require.NoError(t, d.Compact([]byte("c"), []byte("c\x00")))
	expectLSM(`
1:
  000008:[a#3,RANGEDEL-b#72057594037927935,RANGEDEL]
  000009:[b#3,RANGEDEL-d#72057594037927935,RANGEDEL]
`)

	// Compact again to move one of the tables to L2.
	require.NoError(t, d.Compact([]byte("c"), []byte("c\x00")))
	expectLSM(`
1:
  000008:[a#3,RANGEDEL-b#72057594037927935,RANGEDEL]
2:
  000009:[b#3,RANGEDEL-d#72057594037927935,RANGEDEL]
`)

	// Write "b" and "c" to a new table.
	require.NoError(t, d.Set([]byte("b"), []byte("d"), nil))
	require.NoError(t, d.Set([]byte("c"), []byte("e"), nil))
	require.NoError(t, d.Flush())
	expectLSM(`
0.0:
  000011:[b#4,SET-c#5,SET]
1:
  000008:[a#3,RANGEDEL-b#72057594037927935,RANGEDEL]
2:
  000009:[b#3,RANGEDEL-d#72057594037927935,RANGEDEL]
`)

	// "b" is still visible at this point as it should be.
	if _, closer, err := d.Get([]byte("b")); err != nil {
		t.Fatalf("expected success, but found %v", err)
	} else {
		closer.Close()
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
	require.NoError(t, d.Compact([]byte("c"), []byte("c\x00")))
	expectLSM(`
1:
  000012:[a#3,RANGEDEL-b#72057594037927935,RANGEDEL]
3:
  000017:[b#4,SET-b#4,SET]
  000018:[b#3,RANGEDEL-c#72057594037927935,RANGEDEL]
  000019:[c#5,SET-d#72057594037927935,RANGEDEL]
`)

	// The L1 table still contains a tombstone from [a,d) which will improperly
	// delete the newer version of "b" in L2.
	if _, closer, err := d.Get([]byte("b")); err != nil {
		t.Errorf("expected success, but found %v", err)
	} else {
		closer.Close()
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
		DebugCheck: DebugCheckLevels,
	})
	require.NoError(t, err)
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

	require.NoError(t, d.Set([]byte("b"), bytes.Repeat([]byte("b"), 100), nil))
	snap1 := d.NewSnapshot()
	defer snap1.Close()
	// Flush so that each version of "b" ends up in its own L0 table. If we
	// allowed both versions in the same L0 table, compaction could trivially
	// move the single L0 table to L1.
	require.NoError(t, d.Flush())
	require.NoError(t, d.Set([]byte("b"), bytes.Repeat([]byte("c"), 100), nil))
	snap2 := d.NewSnapshot()
	defer snap2.Close()
	require.NoError(t, d.DeleteRange([]byte("a"), []byte("d"), nil))

	// Compact to produce the L1 tables.
	require.NoError(t, d.Compact([]byte("b"), []byte("b\x00")))
	expectLSM(`
6:
  000008:[a#3,RANGEDEL-b#2,SET]
  000009:[b#1,RANGEDEL-d#72057594037927935,RANGEDEL]
`)

	require.NoError(t, d.Set([]byte("c"), bytes.Repeat([]byte("d"), 100), nil))
	require.NoError(t, d.Compact([]byte("c"), []byte("c\x00")))
	expectLSM(`
6:
  000012:[a#3,RANGEDEL-b#2,SET]
  000013:[b#1,RANGEDEL-c#72057594037927935,RANGEDEL]
  000014:[c#4,SET-d#72057594037927935,RANGEDEL]
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
		DebugCheck: DebugCheckLevels,
	})
	require.NoError(t, err)
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

	require.NoError(t, d.Set([]byte("b"), bytes.Repeat([]byte("b"), 100), nil))
	snap1 := d.NewSnapshot()
	defer snap1.Close()

	// Flush so that each version of "b" ends up in its own L0 table. If we
	// allowed both versions in the same L0 table, compaction could trivially
	// move the single L0 table to L1.
	require.NoError(t, d.Flush())
	require.NoError(t, d.Set([]byte("b"), bytes.Repeat([]byte("c"), 100), nil))
	snap2 := d.NewSnapshot()
	defer snap2.Close()

	require.NoError(t, d.DeleteRange([]byte("a"), []byte("d"), nil))
	snap3 := d.NewSnapshot()
	defer snap3.Close()

	if _, _, err := d.Get([]byte("b")); err != ErrNotFound {
		t.Fatalf("expected not found, but found %v", err)
	}

	// Compact a few times to move the tables down to L3.
	for i := 0; i < 3; i++ {
		require.NoError(t, d.Compact([]byte("b"), []byte("b\x00")))
	}
	expectLSM(`
3:
  000012:[a#3,RANGEDEL-b#2,SET]
  000013:[b#1,RANGEDEL-d#72057594037927935,RANGEDEL]
`)

	require.NoError(t, d.Set([]byte("c"), bytes.Repeat([]byte("d"), 100), nil))

	require.NoError(t, d.Compact([]byte("c"), []byte("c\x00")))
	expectLSM(`
4:
  000020:[a#3,RANGEDEL-b#2,SET]
  000021:[b#1,RANGEDEL-c#72057594037927935,RANGEDEL]
  000022:[c#4,SET-d#72057594037927935,RANGEDEL]
`)

	require.NoError(t, d.Compact([]byte("c"), []byte("c\x00")))
	expectLSM(`
5:
  000023:[a#3,RANGEDEL-b#2,SET]
  000024:[b#1,RANGEDEL-c#72057594037927935,RANGEDEL]
  000025:[c#4,SET-d#72057594037927935,RANGEDEL]
`)

	if _, _, err := d.Get([]byte("b")); err != ErrNotFound {
		t.Fatalf("expected not found, but found %v", err)
	}

	require.NoError(t, d.Compact([]byte("a"), []byte("a\x00")))
	expectLSM(`
5:
  000025:[c#4,SET-d#72057594037927935,RANGEDEL]
6:
  000026:[a#3,RANGEDEL-b#2,SET]
  000027:[b#1,RANGEDEL-c#72057594037927935,RANGEDEL]
`)

	if v, _, err := d.Get([]byte("b")); err != ErrNotFound {
		t.Fatalf("expected not found, but found %v [%s]", err, v)
	}
}

func BenchmarkRangeDelIterate(b *testing.B) {
	for _, entries := range []int{10, 1000, 100000} {
		b.Run(fmt.Sprintf("entries=%d", entries), func(b *testing.B) {
			for _, deleted := range []int{entries, entries - 1} {
				b.Run(fmt.Sprintf("deleted=%d", deleted), func(b *testing.B) {
					for _, snapshotCompact := range []bool{false, true} {
						b.Run(fmt.Sprintf("snapshotAndCompact=%t", snapshotCompact), func(b *testing.B) {
							benchmarkRangeDelIterate(b, entries, deleted, snapshotCompact)
						})
					}
				})
			}
		})
	}
}

func benchmarkRangeDelIterate(b *testing.B, entries, deleted int, snapshotCompact bool) {
	mem := vfs.NewMem()
	cache := NewCache(128 << 20) // 128 MB
	defer cache.Unref()

	d, err := Open("", &Options{
		Cache:      cache,
		FS:         mem,
		DebugCheck: DebugCheckLevels,
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

	// Some benchmarks test snapshots that force the range tombstone into the
	// same level as the covered data.
	// See https://github.com/cockroachdb/pebble/issues/1070.
	if snapshotCompact {
		s := d.NewSnapshot()
		defer func() { require.NoError(b, s.Close()) }()
	}

	// Create a range tombstone that deletes most (or all) of those entries.
	from := makeKey(0)
	to := makeKey(deleted)
	if err := d.DeleteRange(from, to, nil); err != nil {
		b.Fatal(err)
	}

	if snapshotCompact {
		require.NoError(b, d.Compact(makeKey(0), makeKey(entries)))
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
}
