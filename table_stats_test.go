// Copyright 2020 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package pebble

import (
	"fmt"
	"testing"

	"github.com/cockroachdb/pebble/internal/base"
	"github.com/cockroachdb/pebble/internal/datadriven"
	"github.com/cockroachdb/pebble/internal/keyspan"
	"github.com/cockroachdb/pebble/vfs"
	"github.com/stretchr/testify/require"
)

func TestTableStats(t *testing.T) {
	fs := vfs.NewMem()
	// loadedInfo is protected by d.mu.
	var loadedInfo *TableStatsInfo
	opts := &Options{
		FS: fs,
		EventListener: EventListener{
			TableStatsLoaded: func(info TableStatsInfo) {
				loadedInfo = &info
			},
		},
	}
	opts.DisableAutomaticCompactions = true

	d, err := Open("", opts)
	require.NoError(t, err)
	defer func() {
		if d != nil {
			require.NoError(t, d.Close())
		}
	}()

	datadriven.RunTest(t, "testdata/table_stats", func(td *datadriven.TestData) string {
		switch td.Cmd {
		case "disable":
			d.mu.Lock()
			d.opts.private.disableTableStats = true
			d.mu.Unlock()
			return ""

		case "enable":
			d.mu.Lock()
			d.opts.private.disableTableStats = false
			d.maybeCollectTableStatsLocked()
			d.mu.Unlock()
			return ""

		case "define":
			require.NoError(t, d.Close())
			loadedInfo = nil

			d, err = runDBDefineCmd(td, opts)
			if err != nil {
				return err.Error()
			}
			d.mu.Lock()
			s := d.mu.versions.currentVersion().String()
			d.mu.Unlock()
			return s

		case "reopen":
			require.NoError(t, d.Close())
			loadedInfo = nil

			// Open using existing file system.
			d, err = Open("", opts)
			require.NoError(t, err)
			return ""

		case "batch":
			b := d.NewBatch()
			if err := runBatchDefineCmd(td, b); err != nil {
				return err.Error()
			}
			b.Commit(nil)
			return ""

		case "flush":
			if err := d.Flush(); err != nil {
				return err.Error()
			}

			d.mu.Lock()
			s := d.mu.versions.currentVersion().DebugString(base.DefaultFormatter)
			d.mu.Unlock()
			return s

		case "wait-pending-table-stats":
			return runTableStatsCmd(td, d)

		case "wait-loaded-initial":
			d.mu.Lock()
			for d.mu.tableStats.loading || !d.mu.tableStats.loadedInitial {
				d.mu.tableStats.cond.Wait()
			}
			s := loadedInfo.String()
			d.mu.Unlock()
			return s

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

		default:
			return fmt.Sprintf("unknown command: %s", td.Cmd)
		}
	})
}

func TestForeachDefragmentedTombstone(t *testing.T) {
	mktomb := func(start, end string, seqnum uint64) keyspan.Span {
		s := base.MakeInternalKey([]byte(start), seqnum, base.InternalKeyKindRangeDelete)
		return keyspan.Span{Start: s, End: []byte(end)}
	}

	testCases := []struct {
		fragmented []keyspan.Span
		want       [][2]string
		wantSeq    [][2]uint64
	}{
		{
			fragmented: []keyspan.Span{
				mktomb("a", "c", 2),
				mktomb("e", "g", 2),
				mktomb("l", "m", 2),
				mktomb("v", "z", 2),
			},
			want:    [][2]string{{"a", "c"}, {"e", "g"}, {"l", "m"}, {"v", "z"}},
			wantSeq: [][2]uint64{{2, 2}, {2, 2}, {2, 2}, {2, 2}},
		},
		{
			fragmented: []keyspan.Span{
				mktomb("a", "c", 2),
				mktomb("c", "f", 5),
				mktomb("c", "f", 2),
				mktomb("f", "m", 5),
			},
			want:    [][2]string{{"a", "m"}},
			wantSeq: [][2]uint64{{2, 5}},
		},
		{
			fragmented: []keyspan.Span{
				mktomb("a", "b", 10),
				mktomb("a", "b", 8),
				mktomb("a", "b", 7),
				mktomb("a", "b", 2),
				mktomb("g", "k", 4),
			},
			want:    [][2]string{{"a", "b"}, {"g", "k"}},
			wantSeq: [][2]uint64{{2, 10}, {4, 4}},
		},
		{
			fragmented: []keyspan.Span{
				mktomb("a", "b", 10),
				mktomb("b", "c", 10),
				mktomb("b", "c", 7),
				mktomb("b", "c", 6),
				mktomb("c", "d", 10),
				mktomb("c", "d", 6),
				mktomb("d", "e", 6),
			},
			want:    [][2]string{{"a", "e"}},
			wantSeq: [][2]uint64{{6, 10}},
		},
	}

	for _, tc := range testCases {
		iter := keyspan.NewIter(DefaultComparer.Compare, tc.fragmented)
		var got [][2]string
		var gotSeq [][2]uint64
		err := foreachDefragmentedTombstone(iter, DefaultComparer.Compare,
			func(start, end []byte, smallest, largest uint64) error {
				got = append(got, [2]string{string(start), string(end)})
				gotSeq = append(gotSeq, [2]uint64{smallest, largest})
				return nil
			})
		require.NoError(t, err)
		require.Equal(t, tc.want, got)
		require.Equal(t, tc.wantSeq, gotSeq)
	}
}
