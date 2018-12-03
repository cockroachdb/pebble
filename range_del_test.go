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

	"github.com/petermattis/pebble/db"
	"github.com/petermattis/pebble/internal/datadriven"
	"github.com/petermattis/pebble/storage"
)

func TestRangeDel(t *testing.T) {
	var d *DB

	datadriven.RunTest(t, "testdata/range_del", func(td *datadriven.TestData) string {
		switch td.Cmd {
		case "define":
			var err error
			if d, err = runDBDefineCmd(td); err != nil {
				return err.Error()
			}

			d.mu.Lock()
			s := fmt.Sprintf("mem: %d\n%s", len(d.mu.mem.queue), d.mu.versions.currentVersion())
			d.mu.Unlock()
			return s

		case "get":
			snap := Snapshot{
				db:     d,
				seqNum: db.InternalKeySeqNumMax,
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

			var buf bytes.Buffer
			for _, data := range strings.Split(td.Input, "\n") {
				v, err := snap.Get([]byte(data))
				if err != nil {
					fmt.Fprintf(&buf, "%s\n", err)
				} else {
					fmt.Fprintf(&buf, "%s\n", v)
				}
			}
			return buf.String()

		case "iter":
			snap := Snapshot{
				db:     d,
				seqNum: db.InternalKeySeqNumMax,
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
	t.Skipf("TODO(peter,rangedel): fix handling of range tombstones during Get")

	// Use a small target file size so that there is a single key per sstable.
	d, err := Open("", &db.Options{
		Storage: storage.NewMem(),
		Levels: []db.LevelOptions{
			{TargetFileSize: 100},
			{TargetFileSize: 100},
			{TargetFileSize: 1},
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	defer d.Close()

	lsm := func() string {
		d.mu.Lock()
		s := d.mu.versions.currentVersion().DebugString()
		d.mu.Unlock()
		return s
	}
	expectLSM := func(expected string) {
		t.Helper()
		expected = strings.TrimSpace(expected)
		actual := strings.TrimSpace(lsm())
		if expected != actual {
			t.Fatalf("expected\n%sbut found\n%s", expected, actual)
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
1: a#2,15-b#72057594037927935,15 b#1,1-d#72057594037927935,15
`)

	// Compact again to move one of the tables to L2.
	if err := d.Compact([]byte("c"), []byte("c")); err != nil {
		t.Fatal(err)
	}
	expectLSM(`
1: a#2,15-b#72057594037927935,15
2: b#1,1-d#72057594037927935,15
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
0: b#3,1-c#4,1
1: a#2,15-b#72057594037927935,15
2: b#1,1-d#72057594037927935,15
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
1: a#2,15-b#72057594037927935,15
2: b#3,1-b#3,1
3: b#2,15-c#72057594037927935,15 c#4,1-d#72057594037927935,15
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
