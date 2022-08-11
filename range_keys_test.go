// Copyright 2021 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.
package pebble

import (
	"bytes"
	"fmt"
	"strconv"
	"testing"

	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/pebble/internal/datadriven"
	"github.com/cockroachdb/pebble/internal/testkeys"
	"github.com/cockroachdb/pebble/internal/testkeys/blockprop"
	"github.com/cockroachdb/pebble/vfs"
	"github.com/stretchr/testify/require"
)

// TODO(jackson): Add a range keys test with concurrency: the logic to cache
// fragmented spans is susceptible to races.

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
				FS:                 vfs.NewMem(),
				Comparer:           testkeys.Comparer,
				FormatMajorVersion: FormatRangeKeys,
				BlockPropertyCollectors: []func() BlockPropertyCollector{
					blockprop.NewBlockPropertyCollector,
				},
			}
			opts.DisableAutomaticCompactions = true
			opts.EnsureDefaults()

			for _, cmdArg := range td.CmdArgs {
				switch cmdArg.Key {
				case "format-major-version":
					v, err := strconv.Atoi(cmdArg.Vals[0])
					if err != nil {
						return err.Error()
					}
					// Override the DB version.
					opts.FormatMajorVersion = FormatMajorVersion(v)
				case "block-size":
					v, err := strconv.Atoi(cmdArg.Vals[0])
					if err != nil {
						return err.Error()
					}
					for i := range opts.Levels {
						opts.Levels[i].BlockSize = v
					}
				default:
					return fmt.Sprintf("unknown command %s\n", cmdArg.Key)
				}

			}

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
			var err error
			func() {
				defer func() {
					if r := recover(); r != nil {
						err = errors.New(r.(string))
					}
				}()
				err = b.Commit(nil)
			}()
			if err != nil {
				return err.Error()
			}
			count := b.Count()
			return fmt.Sprintf("wrote %d keys\n", count)
		case "flush":
			err := d.Flush()
			if err != nil {
				return err.Error()
			}
			return ""
		case "indexed-batch":
			b = d.NewIndexedBatch()
			require.NoError(t, runBatchDefineCmd(td, b))
			count := b.Count()
			return fmt.Sprintf("created indexed batch with %d keys\n", count)
		case "lsm":
			return runLSMCmd(td, d)
		case "metrics":
			d.mu.Lock()
			d.waitTableStats()
			d.mu.Unlock()
			m := d.Metrics()
			return fmt.Sprintf("Metrics.Keys.RangeKeySetsCount = %d\n", m.Keys.RangeKeySetsCount)
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
				switch arg.Key {
				case "mask-suffix":
					o.RangeKeyMasking.Suffix = []byte(arg.Vals[0])
				case "mask-filter":
					o.RangeKeyMasking.Filter = blockprop.NewMaskingFilter()
				case "lower":
					o.LowerBound = []byte(arg.Vals[0])
				case "upper":
					o.UpperBound = []byte(arg.Vals[0])
				}
			}
			var iter *Iterator
			var err error
			func() {
				defer func() {
					if r := recover(); r != nil {
						err = errors.New(r.(string))
					}
				}()
				iter = newIter(o)
			}()
			if err != nil {
				return err.Error()
			}
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
		case "wait-table-stats":
			d.mu.Lock()
			d.waitTableStats()
			d.mu.Unlock()
			return ""
		default:
			return fmt.Sprintf("unknown command %q", td.Cmd)
		}
	})
	if d != nil {
		require.NoError(t, d.Close())
	}
}
