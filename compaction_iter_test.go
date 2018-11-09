// Copyright 2018 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package pebble

import (
	"bytes"
	"fmt"
	"sort"
	"strconv"
	"strings"
	"testing"

	"github.com/petermattis/pebble/db"
	"github.com/petermattis/pebble/internal/datadriven"
)

func TestSnapshotIndex(t *testing.T) {
	testCases := []struct {
		snapshots []uint64
		seq       uint64
		expected  int
	}{
		{[]uint64{}, 1, 0},
		{[]uint64{1}, 0, 0},
		{[]uint64{1}, 1, 1},
		{[]uint64{1}, 2, 1},
		{[]uint64{1, 3}, 1, 1},
		{[]uint64{1, 3}, 2, 1},
		{[]uint64{1, 3}, 3, 2},
		{[]uint64{1, 3}, 4, 2},
		{[]uint64{1, 3, 3}, 2, 1},
	}
	for _, c := range testCases {
		t.Run("", func(t *testing.T) {
			idx := snapshotIndex(c.seq, c.snapshots)
			if c.expected != idx {
				t.Fatalf("expected %d, but got %d", c.expected, idx)
			}
		})
	}
}

func TestCompactionIter(t *testing.T) {
	var keys []db.InternalKey
	var vals [][]byte
	var snapshots []uint64
	var elideTombstones bool

	newIter := func() *compactionIter {
		return &compactionIter{
			cmp:       db.DefaultComparer.Compare,
			merge:     db.DefaultMerger.Merge,
			iter:      &fakeIter{keys: keys, vals: vals},
			snapshots: snapshots,
			elideTombstone: func([]byte) bool {
				return elideTombstones
			},
		}
	}

	datadriven.RunTest(t, "testdata/compaction_iter", func(d *datadriven.TestData) string {
		switch d.Cmd {
		case "define":
			keys = keys[:0]
			vals = vals[:0]
			for _, key := range strings.Split(d.Input, "\n") {
				j := strings.Index(key, ":")
				keys = append(keys, db.ParseInternalKey(key[:j]))
				vals = append(vals, []byte(key[j+1:]))
			}
			return ""

		case "iter":
			snapshots = snapshots[:0]
			elideTombstones = false
			for _, arg := range d.CmdArgs {
				switch arg.Key {
				case "snapshots":
					for _, val := range arg.Vals {
						seqNum, err := strconv.Atoi(val)
						if err != nil {
							return err.Error()
						}
						snapshots = append(snapshots, uint64(seqNum))
					}
				case "elide-tombstones":
					var err error
					elideTombstones, err = strconv.ParseBool(arg.Vals[0])
					if err != nil {
						return err.Error()
					}
				default:
					t.Fatalf("%s: unknown arg: %s", d.Cmd, arg.Key)
				}
			}
			sort.Slice(snapshots, func(i, j int) bool {
				return snapshots[i] < snapshots[j]
			})

			iter := newIter()
			var b bytes.Buffer
			for _, line := range strings.Split(d.Input, "\n") {
				parts := strings.Fields(line)
				if len(parts) == 0 {
					continue
				}
				switch parts[0] {
				case "first":
					iter.First()
				case "next":
					iter.Next()
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

		default:
			t.Fatalf("unknown command: %s", d.Cmd)
		}

		return ""
	})
}
