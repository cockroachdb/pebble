// Copyright 2021 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.
package pebble

import (
	"bytes"
	"fmt"
	"testing"

	"github.com/cockroachdb/pebble/internal/datadriven"
	"github.com/cockroachdb/pebble/internal/testkeys"
	"github.com/cockroachdb/pebble/vfs"
	"github.com/stretchr/testify/require"
)

func TestRangeKeys(t *testing.T) {
	var d *DB
	var b *Batch
	newIter := func(o *IterOptions) *Iterator {
		if b != nil {
			return b.NewIter(o)
		}
		return d.NewIter(o)
	}

	datadriven.RunTest(t, "testdata/rangekeys", func(td *datadriven.TestData) string {
		switch td.Cmd {
		case "reset":
			if b != nil {
				require.NoError(t, b.Close())
				b = nil
			}
			if d != nil {
				require.NoError(t, d.Close())
			}
			opts := &Options{
				FS:       vfs.NewMem(),
				Comparer: testkeys.Comparer,
			}
			opts.Experimental.RangeKeys = new(RangeKeysArena)
			var err error
			d, err = Open("", opts)
			require.NoError(t, err)
			return ""
		case "populate":
			b := d.NewBatch()
			runPopulateCmd(t, td, b)
			count := b.Count()
			require.NoError(t, b.Commit(nil))
			return fmt.Sprintf("wrote %d keys\n", count)
		case "batch":
			b := d.NewBatch()
			require.NoError(t, runBatchDefineCmd(td, b))
			count := b.Count()
			require.NoError(t, b.Commit(nil))
			return fmt.Sprintf("wrote %d keys\n", count)
		case "indexed-batch":
			b = d.NewIndexedBatch()
			require.NoError(t, runBatchDefineCmd(td, b))
			count := b.Count()
			return fmt.Sprintf("created indexed batch with %d keys\n", count)
		case "commit-batch":
			if b == nil {
				return "no pending batch"
			}
			count := b.Count()
			require.NoError(t, d.Apply(b, nil))
			b = nil
			return fmt.Sprintf("wrote %d keys\n", count)
		case "combined-iter":
			o := &IterOptions{KeyTypes: IterKeyTypePointsAndRanges}
			for _, arg := range td.CmdArgs {
				if arg.Key != "mask-suffix" {
					continue
				}
				o.RangeKeyMasking.Suffix = []byte(arg.Vals[0])
			}
			iter := newIter(o)
			return runIterCmd(td, iter, true /* close iter */)
		case "rangekey-iter":
			iter := newIter(&IterOptions{KeyTypes: IterKeyTypeRangesOnly})
			return runIterCmd(td, iter, true /* close iter */)
		case "scan-rangekeys":
			var buf bytes.Buffer
			iter := newIter(&IterOptions{KeyTypes: IterKeyTypeRangesOnly})
			defer iter.Close()
			for iter.First(); iter.Valid(); iter.Next() {
				start, end := iter.RangeBounds()
				fmt.Fprintf(&buf, "[%s, %s)\n", start, end)
				writeRangeKeys(&buf, iter)
				fmt.Fprintln(&buf)
			}
			return buf.String()
		default:
			return fmt.Sprintf("unknown command %q", td.Cmd)
		}
	})
	if d != nil {
		require.NoError(t, d.Close())
	}
}
