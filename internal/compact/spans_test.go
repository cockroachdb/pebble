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
	"github.com/cockroachdb/pebble/objstorage"
	"github.com/cockroachdb/pebble/sstable"
	"github.com/stretchr/testify/require"
)

// TestRangeKeySpanCompactor tests the range key coalescing and striping logic.
func TestRangeKeySpanCompactor(t *testing.T) {
	var c RangeKeySpanCompactor
	datadriven.RunTest(t, "testdata/range_key_compactor", func(t *testing.T, td *datadriven.TestData) string {
		switch td.Cmd {
		case "compact":
			var snapshots []uint64
			var keyRanges []base.UserKeyBounds
			td.MaybeScanArgs(t, "snapshots", &snapshots)
			if arg, ok := td.Arg("in-use-key-ranges"); ok {
				for _, keyRange := range arg.Vals {
					parts := strings.SplitN(keyRange, "-", 2)
					start := []byte(strings.TrimSpace(parts[0]))
					end := []byte(strings.TrimSpace(parts[1]))
					keyRanges = append(keyRanges, base.UserKeyBoundsInclusive(start, end))
				}
			}
			span := keyspan.ParseSpan(td.Input)
			for i := range span.Keys {
				if i > 0 {
					if span.Keys[i-1].Trailer < span.Keys[i].Trailer {
						return "span keys not sorted"
					}
				}
			}

			c = MakeRangeKeySpanCompactor(
				base.DefaultComparer.Compare,
				base.DefaultComparer.Equal,
				snapshots,
				ElideTombstonesOutsideOf(keyRanges),
			)

			c.Compact(&span)
			return c.String()

		case "encode":
			var upToKey string
			td.MaybeScanArgs(t, "up-to", &upToKey)

			obj := &objstorage.MemObj{}
			w := sstable.NewWriter(obj, sstable.WriterOptions{TableFormat: sstable.TableFormatMax})

			if upToKey == "" {
				require.NoError(t, c.Encode(w))
			} else {
				require.NoError(t, c.EncodeUpTo([]byte(upToKey), w))
			}

			require.NoError(t, w.Close())
			_, _, rangeKeys := sstable.ReadAll(obj)
			require.LessOrEqual(t, len(rangeKeys), 1)
			s := "."
			if len(rangeKeys) == 1 {
				s = rangeKeys[0].String()
			}
			return fmt.Sprintf("Encoded:   %s\nRemaining: %s\n", s, c.String())

		default:
			return fmt.Sprintf("unknown command: %s", td.Cmd)
		}
	})
}
