// Copyright 2024 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package compact

import (
	"fmt"
	"strings"
	"testing"

	"github.com/cockroachdb/datadriven"
	"github.com/cockroachdb/pebble/internal/base"
	"github.com/cockroachdb/pebble/internal/keyspan"
)

// TestRangeDelSpanCompactor tests the range key coalescing and striping logic.
func TestRangeDelSpanCompactor(t *testing.T) {
	var c RangeDelSpanCompactor
	var output keyspan.Span
	datadriven.RunTest(t, "testdata/range_del_compactor", func(t *testing.T, td *datadriven.TestData) string {
		switch td.Cmd {
		case "compact":
			var snapshots []uint64
			td.MaybeScanArgs(t, "snapshots", &snapshots)
			keyRanges := maybeParseInUseKeyRanges(td)
			span := keyspan.ParseSpan(td.Input)

			c = MakeRangeDelSpanCompactor(
				base.DefaultComparer.Compare,
				base.DefaultComparer.Equal,
				snapshots,
				ElideTombstonesOutsideOf(keyRanges),
			)

			c.Compact(&span, &output)
			if output.Empty() {
				return "."
			}
			return output.String()

		default:
			return fmt.Sprintf("unknown command: %s", td.Cmd)
		}
	})
}

// TestRangeKeySpanCompactor tests the range key coalescing and striping logic.
func TestRangeKeySpanCompactor(t *testing.T) {
	var c RangeKeySpanCompactor
	var output keyspan.Span
	datadriven.RunTest(t, "testdata/range_key_compactor", func(t *testing.T, td *datadriven.TestData) string {
		switch td.Cmd {
		case "compact":
			var snapshots []uint64
			td.MaybeScanArgs(t, "snapshots", &snapshots)
			keyRanges := maybeParseInUseKeyRanges(td)
			span := keyspan.ParseSpan(td.Input)

			c = MakeRangeKeySpanCompactor(
				base.DefaultComparer.Compare,
				base.DefaultComparer.Equal,
				snapshots,
				ElideTombstonesOutsideOf(keyRanges),
			)

			c.Compact(&span, &output)
			if output.Empty() {
				return "."
			}
			return output.String()

		default:
			return fmt.Sprintf("unknown command: %s", td.Cmd)
		}
	})
}

func maybeParseInUseKeyRanges(td *datadriven.TestData) []base.UserKeyBounds {
	arg, ok := td.Arg("in-use-key-ranges")
	if !ok {
		return nil
	}
	keyRanges := make([]base.UserKeyBounds, len(arg.Vals))
	for i, keyRange := range arg.Vals {
		parts := strings.SplitN(keyRange, "-", 2)
		start := []byte(strings.TrimSpace(parts[0]))
		end := []byte(strings.TrimSpace(parts[1]))
		keyRanges[i] = base.UserKeyBoundsInclusive(start, end)
	}
	return keyRanges
}
