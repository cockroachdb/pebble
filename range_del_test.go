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

// Verify that truncating a range tombstone when a user-key straddles an
// sstable does not result in a version of that user-key reappearing.
func TestRangeDelCompactionTruncation(t *testing.T) {
	// Use a small target file size so that there is a single key per sstable.
	d, err := Open("", &db.Options{
		Storage: storage.NewMem(),
		Levels: []db.LevelOptions{
			{TargetFileSize: 1},
			{TargetFileSize: 1},
		},
	})
	if err != nil {
		t.Fatal(err)
	}

	if err := d.Set([]byte("a"), []byte("b"), nil); err != nil {
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
	if err := d.Set([]byte("a"), []byte("b"), nil); err != nil {
		t.Fatal(err)
	}
	snap2 := d.NewSnapshot()
	defer snap2.Close()
	if err := d.DeleteRange([]byte("a"), []byte("b"), nil); err != nil {
		t.Fatal(err)
	}
	if err := d.Compact([]byte("a"), []byte("b")); err != nil {
		t.Fatal(err)
	}

	lsm := func() string {
		d.mu.Lock()
		s := d.mu.versions.currentVersion().DebugString()
		d.mu.Unlock()
		return s
	}
	actual := lsm()
	const expected = "1: a#2,15-a#1,1 a#0,15-b#72057594037927935,15\n"
	if expected != actual {
		t.Fatalf("expected\n%sbut found\n%s", expected, actual)
	}

	if _, err := d.Get([]byte("a")); err != db.ErrNotFound {
		t.Fatalf("expected ErrNotFound, but found %v", err)
	}
}

// TODO(peter): This causes a bug in RocksDB, but not in Pebble. The difference
// appears to be in the handling of version.overlaps. Pebble finds overlaps
// using user-keys, while RocksDB finds overlaps using internal keys.
func TestRangeDelCompactionTruncation2(t *testing.T) {
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

	lsm := func() string {
		d.mu.Lock()
		s := d.mu.versions.currentVersion().DebugString()
		d.mu.Unlock()
		return s
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
	if err := d.Compact([]byte("a"), []byte("d")); err != nil {
		t.Fatal(err)
	}
	fmt.Println(lsm())
	if err := d.Compact([]byte("c"), []byte("c")); err != nil {
		t.Fatal(err)
	}
	fmt.Println(lsm())

	if err := d.Set([]byte("b"), []byte("d"), nil); err != nil {
		t.Fatal(err)
	}
	if err := d.Set([]byte("c"), []byte("e"), nil); err != nil {
		t.Fatal(err)
	}
	if _, err := d.Get([]byte("b")); err != nil {
		t.Fatalf("expected success, but found %v", err)
	}

	if err := d.Flush(); err != nil {
		t.Fatal(err)
	}
	fmt.Println(lsm())
	if err := d.Compact([]byte("c"), []byte("c")); err != nil {
		t.Fatal(err)
	}
	fmt.Println(lsm())

	if _, err := d.Get([]byte("b")); err == nil {
		t.Fatalf("expected ErrNotFound, but found %v", err)
	}
}
