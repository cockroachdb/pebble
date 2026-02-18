// Copyright 2025 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

//go:build invariants

package pebble

import (
	"strings"
	"testing"

	"github.com/cockroachdb/datadriven"
	"github.com/cockroachdb/pebble/internal/base"
	"github.com/cockroachdb/pebble/internal/itertest"
	"github.com/cockroachdb/pebble/internal/manifest"
	"github.com/cockroachdb/pebble/internal/testkeys"
	"github.com/cockroachdb/pebble/internal/treesteps"
	"github.com/cockroachdb/pebble/vfs"
	"github.com/stretchr/testify/require"
)

// TestTreeSteps tests the treesteps recording for various iterator types,
// generating visualization URLs showing iterator behavior.
func TestTreeSteps(t *testing.T) {
	if !treesteps.Enabled {
		t.Skip("treesteps not available in this build")
	}
	datadriven.Walk(t, "testdata/treesteps", func(t *testing.T, path string) {
		var d *DB
		defer func() {
			if d != nil {
				require.NoError(t, d.Close())
				d = nil
			}
		}()
		datadriven.RunTest(t, path, func(t *testing.T, td *datadriven.TestData) string {
			switch td.Cmd {
			case "define":
				if d != nil {
					require.NoError(t, d.Close())
					d = nil
				}
				opts := &Options{
					Comparer:                    testkeys.Comparer,
					FS:                          vfs.NewMem(),
					FormatMajorVersion:          FormatNewest,
					DisableAutomaticCompactions: true,
				}
				var err error
				d, err = runDBDefineCmd(td, opts)
				require.NoError(t, err)
				return d.DebugString()

			case "level-iter":
				v := d.DebugCurrentVersion()
				var opts IterOptions
				iter := newLevelIter(t.Context(), opts, testkeys.Comparer, d.newIters, v.Levels[1].Iter(), manifest.Level(1), internalIterOpts{})
				defer iter.Close()
				rec := treeStepsStartRecording(t, td, iter)
				out := itertest.RunInternalIterCmd(t, td, iter, itertest.Verbose)
				url := rec.Finish().URL()
				return out + url.String()

			case "merging-iter":
				v := d.DebugCurrentVersion()
				levelIters := make([]mergingIterLevel, 0, len(v.Levels))
				for l := 1; l < len(v.Levels); l++ {
					if v.Levels[l].Empty() {
						continue
					}
					var opts IterOptions
					li := newLevelIter(t.Context(), opts, testkeys.Comparer, d.newIters, v.Levels[l].Iter(), manifest.Level(l), internalIterOpts{})
					li.interleaveRangeDels = true
					levelIters = append(levelIters, mergingIterLevel{iter: li, getTombstone: li})
				}
				miter := &mergingIter{}
				miter.forceEnableSeekOpt = true
				var stats base.InternalIteratorStats
				miter.init(nil /* opts */, &stats, d.cmp, d.split, levelIters...)
				defer miter.Close()
				rec := treeStepsStartRecording(t, td, miter)
				out := itertest.RunInternalIterCmd(t, td, miter, itertest.Verbose)
				url := rec.Finish().URL()
				return out + url.String()

			case "iterator":
				iter, _ := d.NewIter(nil)
				rec := treeStepsStartRecording(t, td, iter)
				out := runIterCmd(td, iter, true /* closeIter */)
				url := rec.Finish().URL()
				return out + url.String()

			default:
				return "unknown command"
			}
		})
	})
}

func treeStepsStartRecording(
	t *testing.T, td *datadriven.TestData, node treesteps.Node,
) *treesteps.Recording {
	var opts []treesteps.RecordingOption
	var depth int
	td.MaybeScanArgs(t, "depth", &depth)
	if depth != 0 {
		opts = append(opts, treesteps.MaxTreeDepth(depth))
	}
	return treesteps.StartRecording(node, strings.ReplaceAll(td.Pos, "\\", "/"), opts...)
}
