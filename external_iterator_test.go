// Copyright 2022 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package pebble

import (
	"fmt"
	"strings"
	"testing"

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

	var it *Iterator
	defer func() {
		if it != nil {
			require.NoError(t, it.Close())
		}
	}()
	datadriven.RunTest(t, "testdata/external_iterator", func(td *datadriven.TestData) string {
		switch td.Cmd {
		case "build":
			if err := runBuildCmd(td, d, mem); err != nil {
				return err.Error()
			}
			return ""
		case "new-external-iter":
			if it != nil {
				require.NoError(t, it.Close())
				it = nil
			}

			opts := IterOptions{KeyTypes: IterKeyTypePointsAndRanges}
			for _, arg := range td.CmdArgs {
				switch arg.Key {
				case "mask-suffix":
					opts.RangeKeyMasking.Suffix = []byte(arg.Vals[0])
				case "lower":
					opts.LowerBound = []byte(arg.Vals[0])
				case "upper":
					opts.UpperBound = []byte(arg.Vals[0])
				}
			}
			var files []sstable.ReadableFile
			for _, filename := range strings.Split(td.Input, "\n") {
				f, err := mem.Open(filename)
				require.NoError(t, err)
				files = append(files, f)
			}
			tableFmt := FormatNewest.MaxTableFormat()
			var err error
			it, err = NewExternalIter(o, &opts, tableFmt, files)
			require.NoError(t, err)
			return ""
		case "iter":
			return runIterCmd(td, it, false /* close iter */)
		default:
			return fmt.Sprintf("unknown command: %s", td.Cmd)
		}
	})
}
