// Copyright 2022 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package pebble

import (
	"fmt"
	"testing"

	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/pebble/internal/datadriven"
	"github.com/cockroachdb/pebble/internal/testkeys"
	"github.com/cockroachdb/pebble/sstable"
	"github.com/cockroachdb/pebble/vfs"
	"github.com/stretchr/testify/require"
)

func TestExternalIterator(t *testing.T) {
	mem := vfs.NewMem()
	o := &Options{
		FS:                 mem,
		Comparer:           testkeys.Comparer,
		FormatMajorVersion: FormatRangeKeys,
	}
	o.EnsureDefaults()
	d, err := Open("", o)
	require.NoError(t, err)
	defer func() { require.NoError(t, d.Close()) }()

	datadriven.RunTest(t, "testdata/external_iterator", func(td *datadriven.TestData) string {
		switch td.Cmd {
		case "reset":
			mem = vfs.NewMem()
			return ""
		case "build":
			if err := runBuildCmd(td, d, mem); err != nil {
				return err.Error()
			}
			return ""
		case "iter":
			opts := IterOptions{KeyTypes: IterKeyTypePointsAndRanges}
			var externalIterOpts []ExternalIterOption
			var files [][]sstable.ReadableFile
			for _, arg := range td.CmdArgs {
				switch arg.Key {
				case "fwd-only":
					externalIterOpts = append(externalIterOpts, ExternalIterForwardOnly{})
				case "mask-suffix":
					opts.RangeKeyMasking.Suffix = []byte(arg.Vals[0])
				case "lower":
					opts.LowerBound = []byte(arg.Vals[0])
				case "upper":
					opts.UpperBound = []byte(arg.Vals[0])
				case "files":
					for _, v := range arg.Vals {
						f, err := mem.Open(v)
						require.NoError(t, err)
						files = append(files, []sstable.ReadableFile{f})
					}
				}
			}
			it, err := NewExternalIter(o, &opts, files, externalIterOpts...)
			require.NoError(t, err)
			return runIterCmd(td, it, true /* close iter */)
		default:
			return fmt.Sprintf("unknown command: %s", td.Cmd)
		}
	})
}

func TestSimpleLevelIter(t *testing.T) {
	mem := vfs.NewMem()
	o := &Options{
		FS:                 mem,
		Comparer:           testkeys.Comparer,
		FormatMajorVersion: FormatRangeKeys,
	}
	o.EnsureDefaults()
	d, err := Open("", o)
	require.NoError(t, err)
	defer func() { require.NoError(t, d.Close()) }()

	datadriven.RunTest(t, "testdata/simple_level_iter", func(td *datadriven.TestData) string {
		switch td.Cmd {
		case "reset":
			mem = vfs.NewMem()
			return ""
		case "build":
			if err := runBuildCmd(td, d, mem); err != nil {
				return err.Error()
			}
			return ""
		case "iter":
			var files []sstable.ReadableFile
			for _, arg := range td.CmdArgs {
				switch arg.Key {
				case "files":
					for _, v := range arg.Vals {
						f, err := mem.Open(v)
						require.NoError(t, err)
						files = append(files, f)
					}
				}
			}
			readers, err := openExternalTables(o, files, 0, o.MakeReaderOptions())
			require.NoError(t, err)
			defer func() {
				for i := range readers {
					_ = readers[i].Close()
				}
			}()
			var internalIters []internalIterator
			for i := range readers {
				iter, err := readers[i].NewIter(nil, nil)
				require.NoError(t, err)
				internalIters = append(internalIters, iter)
			}
			it := &simpleLevelIter{cmp: o.Comparer.Compare, iters: internalIters}
			it.init(IterOptions{})

			response := runInternalIterCmd(td, it)
			require.NoError(t, it.Close())
			return response
		default:
			return fmt.Sprintf("unknown command: %s", td.Cmd)
		}
	})
}

func TestSimpleIterError(t *testing.T) {
	s := simpleLevelIter{cmp: DefaultComparer.Compare, iters: []internalIterator{&errorIter{err: errors.New("injected")}}}
	s.init(IterOptions{})
	defer s.Close()

	iterKey, _ := s.First()
	require.Nil(t, iterKey)
	require.Error(t, s.Error())
}
