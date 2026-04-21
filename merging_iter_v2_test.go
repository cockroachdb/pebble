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

// mergeLevels returns the surviving point keys after applying range deletion
// filtering across all levels. A point is "surviving" if it is visible at the
// given snapshot and not shadowed by any visible RANGEDEL with a strictly
// greater sequence number.
func mergeLevels(
	cmp *base.Comparer, levels []iterv2.TestIterData, snapshot base.SeqNum,
) []base.InternalKV {
	// Collect all visible range deletions from all levels.
	type rangeDel struct {
		start, end []byte
		seqNum     base.SeqNum
	}
	var rangeDels []rangeDel
	for _, lvl := range levels {
		for _, span := range lvl.Spans {
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
		for _, p := range lvl.Points {
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
	rng *rand.Rand, cmp *base.Comparer, levels []iterv2.TestIterData, snapshot base.SeqNum,
) *mergingIterV2 {
	iters := make([]iterv2.Iter, len(levels))
	for i := range levels {
		iters[i] = iterv2.NewTestIter(levels[i])
		if rng.IntN(2) == 0 {
			iters[i] = iterv2.NewInvalidating(iters[i])
		}
		if rng.IntN(2) == 0 {
			iters[i] = iterv2.NewOpCheckIter(iters[i], cmp, nil, nil)
		}
	}
	return newMergingIterV2(cmp.Compare, cmp.Split, snapshot, iters...)
}

// parseMergingTestLevels parses a define block. Each level starts with an "L"
// line, followed by point keys (in "key#seq,KIND" format) and range deletion
// spans (in "start-end:{(#seq,RANGEDEL) ...}" format, as understood by
// keyspan.ParseSpan).
func parseMergingTestLevels(input string) []iterv2.TestIterData {
	var levels []iterv2.TestIterData
	curLevel := -1
	for line := range crstrings.LinesSeq(input) {
		line = strings.TrimSpace(line)
		if line == "" {
			continue
		}
		if line == "L" {
			levels = append(levels, iterv2.TestIterData{})
			curLevel++
			continue
		}
		if curLevel < 0 {
			panic(errors.AssertionFailedf("key/span data before first L line: %q", line))
		}
		// If the line contains a "-" followed by ":{", it's a span.
		if strings.Contains(line, ":{") || strings.Contains(line, "-") && strings.Contains(line, "RANGEDEL") {
			span := keyspan.ParseSpan(line)
			levels[curLevel].Spans = append(levels[curLevel].Spans, span)
		} else {
			// Parse space-separated point keys.
			for field := range strings.FieldsSeq(line) {
				k := base.ParseInternalKey(field)
				levels[curLevel].Points = append(levels[curLevel].Points, base.MakeInternalKV(k, nil))
			}
		}
	}
	return levels
}

// formatMergingTestLevels formats levels for display in define output.
func formatMergingTestLevels(levels []iterv2.TestIterData) string {
	var buf strings.Builder
	for i, lvl := range levels {
		fmt.Fprintf(&buf, "L%d\n", i)
		for _, p := range lvl.Points {
			fmt.Fprintf(&buf, "  %s\n", p.K)
		}
		for _, s := range lvl.Spans {
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
	var levels []iterv2.TestIterData
	var snapshot base.SeqNum

	rng := rand.New(rand.NewPCG(rand.Uint64(), rand.Uint64()))
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
			iter := newMergingIterV2FromLevels(rng, testkeys.Comparer, levels, snapshot)
			defer iter.Close()
			return itertest.RunInternalIterCmd(t, d, iter, itertest.Verbose)

		default:
			return fmt.Sprintf("unknown command: %s", d.Cmd)
		}
	})
}
