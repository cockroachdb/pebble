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

	"github.com/cockroachdb/pebble/internal/base"
	"github.com/cockroachdb/pebble/internal/datadriven"
)

func TestSnapshotIndex(t *testing.T) {
	testCases := []struct {
		snapshots      []uint64
		seq            uint64
		expectedIndex  int
		expectedSeqNum uint64
	}{
		{[]uint64{}, 1, 0, InternalKeySeqNumMax},
		{[]uint64{1}, 0, 0, 1},
		{[]uint64{1}, 1, 1, InternalKeySeqNumMax},
		{[]uint64{1}, 2, 1, InternalKeySeqNumMax},
		{[]uint64{1, 3}, 1, 1, 3},
		{[]uint64{1, 3}, 2, 1, 3},
		{[]uint64{1, 3}, 3, 2, InternalKeySeqNumMax},
		{[]uint64{1, 3}, 4, 2, InternalKeySeqNumMax},
		{[]uint64{1, 3, 3}, 2, 1, 3},
	}
	for _, c := range testCases {
		t.Run("", func(t *testing.T) {
			idx, seqNum := snapshotIndex(c.seq, c.snapshots)
			if c.expectedIndex != idx {
				t.Fatalf("expected %d, but got %d", c.expectedIndex, idx)
			}
			if c.expectedSeqNum != seqNum {
				t.Fatalf("expected %d, but got %d", c.expectedSeqNum, seqNum)
			}
		})
	}
}

func TestCompactionIter(t *testing.T) {
	var keys []InternalKey
	var vals [][]byte
	var snapshots []uint64
	var elideTombstones bool

	newIter := func() *compactionIter {
		return newCompactionIter(
			DefaultComparer.Compare,
			DefaultMerger.Merge,
			&fakeIter{keys: keys, vals: vals},
			snapshots,
			false, /* allowZeroSeqNum */
			func([]byte) bool {
				return elideTombstones
			},
			func(_, _ []byte) bool {
				return elideTombstones
			},
		)
	}

	datadriven.RunTest(t, "testdata/compaction_iter", func(d *datadriven.TestData) string {
		switch d.Cmd {
		case "define":
			keys = keys[:0]
			vals = vals[:0]
			for _, key := range strings.Split(d.Input, "\n") {
				j := strings.Index(key, ":")
				keys = append(keys, base.ParseInternalKey(key[:j]))
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
					return fmt.Sprintf("%s: unknown arg: %s", d.Cmd, arg.Key)
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
				case "tombstones":
					var key []byte
					if len(parts) == 2 {
						key = []byte(parts[1])
					}
					for _, v := range iter.Tombstones(key) {
						fmt.Fprintf(&b, "%s-%s#%d\n",
							v.Start.UserKey, v.End, v.Start.SeqNum())
					}
					fmt.Fprintf(&b, ".\n")
					continue
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
			return fmt.Sprintf("unknown command: %s", d.Cmd)
		}
	})
}
