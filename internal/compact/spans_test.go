// Copyright 2024 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package compact

import (
	"fmt"
	"strings"
	"testing"

	"github.com/cockroachdb/datadriven"
	"github.com/cockroachdb/pebble/v2/internal/base"
	"github.com/cockroachdb/pebble/v2/internal/keyspan"
	"github.com/cockroachdb/pebble/v2/internal/testkeys"
	"github.com/cockroachdb/pebble/v2/objstorage"
	"github.com/cockroachdb/pebble/v2/sstable"
	"github.com/cockroachdb/pebble/v2/sstable/colblk"
	"github.com/stretchr/testify/require"
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
			var s Snapshots
			for _, v := range snapshots {
				s = append(s, base.SeqNum(v))
			}
			keyRanges := maybeParseInUseKeyRanges(td)
			span := keyspan.ParseSpan(td.Input)

			c = MakeRangeDelSpanCompactor(
				base.DefaultComparer.Compare,
				base.DefaultComparer.Equal,
				s,
				ElideTombstonesOutsideOf(keyRanges),
			)

			c.Compact(&span, &output)
			if output.Empty() {
				return "."
			}
			return output.String()

		default:
			td.Fatalf(t, "unknown command: %s", td.Cmd)
			return ""
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
			var s Snapshots
			for _, v := range snapshots {
				s = append(s, base.SeqNum(v))
			}
			keyRanges := maybeParseInUseKeyRanges(td)
			span := keyspan.ParseSpan(td.Input)

			c = MakeRangeKeySpanCompactor(
				base.DefaultComparer.Compare,
				base.DefaultComparer.CompareRangeSuffixes,
				s,
				ElideTombstonesOutsideOf(keyRanges),
			)

			c.Compact(&span, &output)
			if output.Empty() {
				return "."
			}
			return output.String()

		default:
			td.Fatalf(t, "unknown command: %s", td.Cmd)
			return ""
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

func TestSplitAndEncodeSpan(t *testing.T) {
	var span keyspan.Span
	datadriven.RunTest(t, "testdata/split_and_encode_span", func(t *testing.T, td *datadriven.TestData) string {
		switch td.Cmd {
		case "set":
			span = keyspan.ParseSpan(td.Input)
			return ""

		case "encode":
			var upToStr string
			td.MaybeScanArgs(t, "up-to", &upToStr)
			var upToKey []byte
			if upToStr != "" {
				upToKey = []byte(upToStr)
			}

			obj := &objstorage.MemObj{}
			keySchema := colblk.DefaultKeySchema(testkeys.Comparer, 16)
			wo := sstable.WriterOptions{
				Comparer:    testkeys.Comparer,
				KeySchema:   &keySchema,
				TableFormat: sstable.TableFormatMax,
			}
			tw := sstable.NewRawWriter(obj, wo)
			require.NoError(t, SplitAndEncodeSpan(base.DefaultComparer.Compare, &span, upToKey, tw))
			require.NoError(t, tw.Close())
			_, rangeDels, rangeKeys, err := sstable.ReadAll(obj, sstable.ReaderOptions{
				Comparer:   wo.Comparer,
				KeySchemas: sstable.MakeKeySchemas(wo.KeySchema),
			})
			require.NoError(t, err)
			require.LessOrEqual(t, len(rangeDels)+len(rangeKeys), 1)
			s := "."
			if all := append(rangeDels, rangeKeys...); len(all) == 1 {
				s = all[0].String()
			}
			remaining := "."
			if !span.Empty() {
				remaining = span.String()
			}
			return fmt.Sprintf("Encoded:   %s\nRemaining: %s\n", s, remaining)

		default:
			td.Fatalf(t, "unknown command: %s", td.Cmd)
			return ""
		}
	})
}
