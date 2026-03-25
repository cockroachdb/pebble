// Copyright 2026 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package pebble

import (
	"fmt"
	"math/rand/v2"
	"slices"
	"strings"
	"testing"

	"github.com/cockroachdb/crlib/crstrings"
	"github.com/cockroachdb/datadriven"
	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/pebble/internal/base"
	"github.com/cockroachdb/pebble/internal/itertest"
	"github.com/cockroachdb/pebble/internal/iterv2"
	"github.com/cockroachdb/pebble/internal/keyspan"
	"github.com/cockroachdb/pebble/internal/testkeys"
)

// mergingTestLevel represents a single level of a merging iterator test, with
// sorted point keys and non-overlapping range deletion spans.
type mergingTestLevel struct {
	points []base.InternalKV // sorted by InternalCompare
	spans  []keyspan.Span    // non-overlapping, sorted by Start
}

// mergeLevels returns the surviving point keys after applying range deletion
// filtering across all levels. A point is "surviving" if it is visible at the
// given snapshot and not shadowed by any visible RANGEDEL with a strictly
// greater sequence number.
func mergeLevels(
	cmp *base.Comparer, levels []mergingTestLevel, snapshot base.SeqNum,
) []base.InternalKV {
	// Collect all visible range deletions from all levels.
	type rangeDel struct {
		start, end []byte
		seqNum     base.SeqNum
	}
	var rangeDels []rangeDel
	for _, lvl := range levels {
		for _, span := range lvl.spans {
			for _, k := range span.Keys {
				if k.Trailer.SeqNum() < snapshot {
					rangeDels = append(rangeDels, rangeDel{
						start:  span.Start,
						end:    span.End,
						seqNum: k.Trailer.SeqNum(),
					})
				}
			}
		}
	}

	// Collect all visible points, filtering out those shadowed by range
	// deletions.
	var surviving []base.InternalKV
	for _, lvl := range levels {
		for _, p := range lvl.points {
			if p.SeqNum() >= snapshot {
				continue
			}
			shadowed := false
			for _, rd := range rangeDels {
				if cmp.Compare(rd.start, p.K.UserKey) <= 0 &&
					cmp.Compare(p.K.UserKey, rd.end) < 0 &&
					rd.seqNum > p.SeqNum() {
					shadowed = true
					break
				}
			}
			if !shadowed {
				surviving = append(surviving, p)
			}
		}
	}

	// Sort surviving points by InternalCompare.
	slices.SortFunc(surviving, func(a, b base.InternalKV) int {
		return base.InternalCompare(cmp.Compare, a.K, b.K)
	})
	return surviving
}

// newMergingIterV2FromLevels builds a mergingIterV2 from test levels by
// wrapping each level's data in an iterv2.TestIter.
func newMergingIterV2FromLevels(
	cmp *base.Comparer, levels []mergingTestLevel, snapshot base.SeqNum,
) *mergingIterV2 {
	iters := make([]iterv2.Iter, len(levels))
	for i, lvl := range levels {
		iters[i] = iterv2.NewTestIter(lvl.points, lvl.spans, nil, nil, nil, nil)
		if rand.IntN(2) == 0 {
			iters[i] = iterv2.NewInvalidating(iters[i])
		}
	}
	return newMergingIterV2(cmp.Compare, cmp.Split, snapshot, iters...)
}

// parseMergingTestLevels parses a define block into a slice of
// mergingTestLevel. Each level starts with an "L" line, followed by point keys
// (in "key#seq,KIND" format) and range deletion spans (in
// "start-end:{(#seq,RANGEDEL) ...}" format, as understood by
// keyspan.ParseSpan).
func parseMergingTestLevels(input string) []mergingTestLevel {
	var levels []mergingTestLevel
	curLevel := -1
	for line := range crstrings.LinesSeq(input) {
		line = strings.TrimSpace(line)
		if line == "" {
			continue
		}
		if line == "L" {
			levels = append(levels, mergingTestLevel{})
			curLevel++
			continue
		}
		if curLevel < 0 {
			panic(errors.AssertionFailedf("key/span data before first L line: %q", line))
		}
		// If the line contains a "-" followed by ":{", it's a span.
		if strings.Contains(line, ":{") || strings.Contains(line, "-") && strings.Contains(line, "RANGEDEL") {
			span := keyspan.ParseSpan(line)
			levels[curLevel].spans = append(levels[curLevel].spans, span)
		} else {
			// Parse space-separated point keys.
			for field := range strings.FieldsSeq(line) {
				k := base.ParseInternalKey(field)
				levels[curLevel].points = append(levels[curLevel].points, base.MakeInternalKV(k, nil))
			}
		}
	}
	return levels
}

// formatMergingTestLevels formats levels for display in define output.
func formatMergingTestLevels(levels []mergingTestLevel) string {
	var buf strings.Builder
	for i, lvl := range levels {
		fmt.Fprintf(&buf, "L%d\n", i)
		for _, p := range lvl.points {
			fmt.Fprintf(&buf, "  %s\n", p.K)
		}
		for _, s := range lvl.spans {
			fmt.Fprintf(&buf, "  %s\n", s)
		}
	}
	return buf.String()
}

// TestMergingIterV2 runs datadriven tests for the merging iterator v2.
//
// Commands:
//
//	define [snapshot=<seq>]
//	  L
//	    <key>#<seq>,<kind> ...      point keys
//	    <start>-<end>:{...}         range deletion spans
//	  L
//	    ...
//
//	iter
//	  first
//	  next
//	  seek-ge <key>
//	  ...
func TestMergingIterV2(t *testing.T) {
	var levels []mergingTestLevel
	var snapshot base.SeqNum

	datadriven.RunTest(t, "testdata/merging_iter_v2", func(t *testing.T, d *datadriven.TestData) string {
		switch d.Cmd {
		case "define":
			snapshot = base.SeqNumMax
			for _, arg := range d.CmdArgs {
				switch arg.Key {
				case "snapshot":
					snapshot = base.ParseSeqNum(arg.Vals[0])
				}
			}
			levels = parseMergingTestLevels(d.Input)
			return formatMergingTestLevels(levels)

		case "iter":
			iter := newMergingIterV2FromLevels(testkeys.Comparer, levels, snapshot)
			defer iter.Close()
			return itertest.RunInternalIterCmd(t, d, iter, itertest.Verbose)

		default:
			return fmt.Sprintf("unknown command: %s", d.Cmd)
		}
	})
}
