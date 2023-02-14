// Copyright 2023 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package pebble

import (
	"fmt"
	"strconv"
	"testing"

	"github.com/cockroachdb/datadriven"
	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/pebble/bloom"
	"github.com/cockroachdb/pebble/internal/base"
	"github.com/cockroachdb/pebble/internal/testkeys"
	"github.com/cockroachdb/pebble/sstable"
	"github.com/cockroachdb/pebble/vfs"
	"github.com/stretchr/testify/require"
)

func TestInternalIter(t *testing.T) {
	var d *DB
	type internalIterReader interface {
		NewInternalIter(o *IterOptions) *InternalIterator
	}
	iters := map[string]*InternalIterator{}
	batches := map[string]*Batch{}
	snaps := map[string]*Snapshot{}
	newIter := func(name string, reader internalIterReader, o *IterOptions) *InternalIterator {
		it := reader.NewInternalIter(o)
		if name != "" {
			iters[name] = it
		}
		return it
	}
	parseOpts := func(td *datadriven.TestData) (*Options, error) {
		opts := &Options{
			FS:                 vfs.NewMem(),
			Comparer:           testkeys.Comparer,
			FormatMajorVersion: FormatRangeKeys,
			BlockPropertyCollectors: []func() BlockPropertyCollector{
				sstable.NewTestKeysBlockPropertyCollector,
			},
		}
		opts.DisableAutomaticCompactions = true
		opts.EnsureDefaults()
		opts.WithFSDefaults()

		for _, cmdArg := range td.CmdArgs {
			switch cmdArg.Key {
			case "format-major-version":
				v, err := strconv.Atoi(cmdArg.Vals[0])
				if err != nil {
					return nil, err
				}
				// Override the DB version.
				opts.FormatMajorVersion = FormatMajorVersion(v)
			case "block-size":
				v, err := strconv.Atoi(cmdArg.Vals[0])
				if err != nil {
					return nil, err
				}
				for i := range opts.Levels {
					opts.Levels[i].BlockSize = v
				}
			case "index-block-size":
				v, err := strconv.Atoi(cmdArg.Vals[0])
				if err != nil {
					return nil, err
				}
				for i := range opts.Levels {
					opts.Levels[i].IndexBlockSize = v
				}
			case "target-file-size":
				v, err := strconv.Atoi(cmdArg.Vals[0])
				if err != nil {
					return nil, err
				}
				for i := range opts.Levels {
					opts.Levels[i].TargetFileSize = int64(v)
				}
			case "bloom-bits-per-key":
				v, err := strconv.Atoi(cmdArg.Vals[0])
				if err != nil {
					return nil, err
				}
				fp := bloom.FilterPolicy(v)
				opts.Filters = map[string]FilterPolicy{fp.Name(): fp}
				for i := range opts.Levels {
					opts.Levels[i].FilterPolicy = fp
				}
			case "merger":
				switch cmdArg.Vals[0] {
				case "appender":
					opts.Merger = base.DefaultMerger
				default:
					return nil, errors.Newf("unrecognized Merger %q\n", cmdArg.Vals[0])
				}
			}
		}
		return opts, nil
	}
	cleanup := func() (err error) {
		for key, batch := range batches {
			err = firstError(err, batch.Close())
			delete(batches, key)
		}
		for key, iter := range iters {
			err = firstError(err, iter.Close())
			delete(iters, key)
		}
		for key, snap := range snaps {
			err = firstError(err, snap.Close())
			delete(snaps, key)
		}
		if d != nil {
			err = firstError(err, d.Close())
			d = nil
		}
		return err
	}
	defer cleanup()

	datadriven.RunTest(t, "testdata/internal_iter_user", func(t *testing.T, td *datadriven.TestData) string {
		switch td.Cmd {
		case "define":
			if err := cleanup(); err != nil {
				return err.Error()
			}
			opts, err := parseOpts(td)
			if err != nil {
				return err.Error()
			}
			d, err = runDBDefineCmd(td, opts)
			if err != nil {
				return err.Error()
			}
			return runLSMCmd(td, d)

		case "reset":
			if err := cleanup(); err != nil {
				t.Fatal(err)
				return err.Error()
			}
			opts, err := parseOpts(td)
			if err != nil {
				t.Fatal(err)
				return err.Error()
			}

			d, err = Open("", opts)
			require.NoError(t, err)
			return ""
		case "snapshot":
			s := d.NewSnapshot()
			var name string
			for _, arg := range td.CmdArgs {
				switch arg.Key {
				case "name":
					name = arg.Vals[0]
				default:
					return fmt.Sprintf("unrecognized command argument %q\n", arg.Key)
				}
			}
			snaps[name] = s
			return ""
		case "batch":
			var commit bool
			var name string
			for _, arg := range td.CmdArgs {
				switch arg.Key {
				case "name":
					name = arg.Vals[0]
				case "commit":
					commit = true
				default:
					return fmt.Sprintf("unrecognized command argument %q\n", arg.Key)
				}
			}
			b := d.NewIndexedBatch()
			require.NoError(t, runBatchDefineCmd(td, b))
			var err error
			if commit {
				func() {
					defer func() {
						if r := recover(); r != nil {
							err = errors.New(r.(string))
						}
					}()
					err = b.Commit(nil)
				}()
			} else if name != "" {
				batches[name] = b
			}
			if err != nil {
				return err.Error()
			}
			count := b.Count()
			if commit {
				return fmt.Sprintf("committed %d keys\n", count)
			}
			return fmt.Sprintf("wrote %d keys to batch %q\n", count, name)
		case "compact":
			if err := runCompactCmd(td, d); err != nil {
				return err.Error()
			}
			return runLSMCmd(td, d)
		case "flush":
			err := d.Flush()
			if err != nil {
				return err.Error()
			}
			return ""
		case "lsm":
			return runLSMCmd(td, d)
		case "commit":
			name := pluckStringCmdArg(td, "batch")
			b := batches[name]
			defer b.Close()
			count := b.Count()
			require.NoError(t, d.Apply(b, nil))
			delete(batches, name)
			return fmt.Sprintf("committed %d keys\n", count)
		case "internal-iter":
			o := &IterOptions{KeyTypes: IterKeyTypePointsAndRanges}
			var reader internalIterReader = d
			var name string
			for _, arg := range td.CmdArgs {
				switch arg.Key {
				case "key-type":
					switch arg.Vals[0] {
					case "point":
						o.KeyTypes = IterKeyTypePointsOnly
					case "both":
						o.KeyTypes = IterKeyTypePointsAndRanges
					case "range":
						o.KeyTypes = IterKeyTypeRangesOnly
					}
				case "mask-suffix":
					o.RangeKeyMasking.Suffix = []byte(arg.Vals[0])
				case "mask-filter":
					o.RangeKeyMasking.Filter = func() BlockPropertyFilterMask {
						return sstable.NewTestKeysMaskingFilter()
					}
				case "lower":
					o.LowerBound = []byte(arg.Vals[0])
				case "upper":
					o.UpperBound = []byte(arg.Vals[0])
				case "name":
					name = arg.Vals[0]
				case "point-key-filter":
					if len(arg.Vals) != 2 {
						return fmt.Sprintf("blockprop-filter expects 2 arguments, received %d", len(arg.Vals))
					}
					min, err := strconv.ParseUint(arg.Vals[0], 10, 64)
					if err != nil {
						return err.Error()
					}
					max, err := strconv.ParseUint(arg.Vals[1], 10, 64)
					if err != nil {
						return err.Error()
					}
					o.PointKeyFilters = []sstable.BlockPropertyFilter{
						sstable.NewTestKeysBlockPropertyFilter(min, max),
					}
				case "snapshot":
					name := arg.Vals[0]
					snap, ok := snaps[name]
					if !ok {
						return fmt.Sprintf("no snapshot found for name %s", name)
					}
					reader = snap
				}
			}
			iter := newIter(name, reader, o)
			var err error
			if err != nil {
				return err.Error()
			}
			return runUserInternalIterCmd(td, iter, name == "")
		default:
			return fmt.Sprintf("unknown command %q", td.Cmd)
		}
	})
}
