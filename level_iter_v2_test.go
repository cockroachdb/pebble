// Copyright 2026 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package pebble

import (
	"context"
	"fmt"
	"strings"
	"testing"

	"github.com/cockroachdb/crlib/crstrings"
	"github.com/cockroachdb/datadriven"
	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/pebble/internal/base"
	"github.com/cockroachdb/pebble/internal/iterv2"
	"github.com/cockroachdb/pebble/internal/keyspan"
	"github.com/cockroachdb/pebble/internal/manifest"
	"github.com/cockroachdb/pebble/internal/testkeys"
)

// TestLevelIterV2 runs datadriven tests for levelIterV2 using fake per-file
// iterators (no real sstables). The driver focuses on TrySeekUsingNext and
// related code paths in level_iter_v2.go.
//
// Commands:
//
//	define
//	  F
//	    <key>#<seq>,<kind> ...           point keys
//	    <start>-<end>:{(#seq,RANGEDEL)}  range deletion spans (optional)
//	  F
//	    ...
//
//	iter [lower=<key>] [upper=<key>]
//	  first
//	  next
//	  seek-ge <key> [try-seek-using-next]
//	  seek-prefix-ge <key> [try-seek-using-next]
//	  ...
func TestLevelIterV2(t *testing.T) {
	type file struct {
		points   []base.InternalKV
		spans    []keyspan.Span
		smallest base.InternalKey
		largest  base.InternalKey
	}
	var files []file
	var metas []*manifest.TableMetadata

	cmp := testkeys.Comparer
	newIters := func(
		_ context.Context,
		f *manifest.TableMetadata,
		opts *IterOptions,
		_ internalIterOpts,
		kinds iterKinds,
	) (iterSet, error) {
		fi := files[f.TableNum]
		var set iterSet
		if kinds.Point() {
			it := base.NewFakeIter(cmp, fi.points)
			it.SetBounds(opts.GetLowerBound(), opts.GetUpperBound())
			set.point = it
		}
		if kinds.RangeDeletion() {
			set.rangeDeletion = keyspan.NewIter(cmp.Compare, fi.spans)
		}
		return set, nil
	}

	datadriven.RunTest(t, "testdata/level_iter_v2", func(t *testing.T, d *datadriven.TestData) string {
		switch d.Cmd {
		case "define":
			files = nil
			metas = nil
			cur := -1
			for line := range crstrings.LinesSeq(d.Input) {
				line = strings.TrimSpace(line)
				if line == "" {
					continue
				}
				if line == "F" {
					files = append(files, file{})
					cur++
					continue
				}
				if cur < 0 {
					return errors.AssertionFailedf("data before first F line: %q", line).Error()
				}
				if strings.Contains(line, ":{") {
					files[cur].spans = append(files[cur].spans, keyspan.ParseSpan(line))
					continue
				}
				for field := range strings.FieldsSeq(line) {
					k := base.ParseInternalKey(field)
					files[cur].points = append(files[cur].points, base.MakeInternalKV(k, nil))
				}
			}

			// Compute file bounds from the points and rangedel spans.
			var buf strings.Builder
			for i := range files {
				fi := &files[i]
				if len(fi.points) == 0 && len(fi.spans) == 0 {
					return errors.AssertionFailedf("file %d has no points or spans", i).Error()
				}
				if len(fi.points) > 0 {
					fi.smallest = fi.points[0].K.Clone()
					fi.largest = fi.points[len(fi.points)-1].K.Clone()
				}
				meta := &manifest.TableMetadata{TableNum: base.TableNum(i)}
				if len(fi.points) > 0 {
					meta.ExtendPointKeyBounds(cmp.Compare, fi.smallest, fi.largest)
				}
				for _, s := range fi.spans {
					meta.ExtendPointKeyBounds(cmp.Compare,
						base.MakeRangeDeleteSentinelKey(s.Start),
						base.MakeRangeDeleteSentinelKey(s.End))
				}
				meta.InitPhysicalBacking()
				metas = append(metas, meta)
				fmt.Fprintf(&buf, "F%d: %s-%s\n", i,
					meta.PointKeyBounds.Smallest(), meta.PointKeyBounds.Largest())
			}
			return buf.String()

		case "iter":
			var opts IterOptions
			for _, arg := range d.CmdArgs {
				if len(arg.Vals) != 1 {
					return fmt.Sprintf("%s: %s=<value>", d.Cmd, arg.Key)
				}
				switch arg.Key {
				case "lower":
					opts.LowerBound = []byte(arg.Vals[0])
				case "upper":
					opts.UpperBound = []byte(arg.Vals[0])
				default:
					return fmt.Sprintf("%s: unknown arg: %s", d.Cmd, arg.Key)
				}
			}
			slice := manifest.NewLevelSliceKeySorted(cmp.Compare, metas)
			iter := newLevelIterV2(
				context.Background(), opts, cmp, newIters,
				slice.Iter(), manifest.Level(1), internalIterOpts{},
			)
			defer iter.Close()
			return iterv2.RunIterOps(t, iter, d.Input)

		default:
			return fmt.Sprintf("unknown command: %s", d.Cmd)
		}
	})
}
