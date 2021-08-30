// Copyright 2021 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package vfs

import (
	"bytes"
	"fmt"
	"os"
	"sort"
	"testing"

	"github.com/cockroachdb/pebble/internal/datadriven"
	"github.com/stretchr/testify/require"
)

func TestAtomicMarker(t *testing.T) {
	markers := map[string]*AtomicMarker{}
	memFS := NewMem()

	var buf bytes.Buffer
	datadriven.RunTest(t, "testdata/atomic_marker", func(td *datadriven.TestData) string {
		switch td.Cmd {
		case "mkdir-all":
			if len(td.CmdArgs) != 1 {
				return "usage: mkdir-all <dir>"
			}
			if err := memFS.MkdirAll(td.CmdArgs[0].String(), os.ModePerm); err != nil {
				return err.Error()
			}
			return ""

		case "locate":
			var dir, marker string
			td.ScanArgs(t, "dir", &dir)
			td.ScanArgs(t, "marker", &marker)
			m, err := LocateMarker(memFS, dir, marker)
			if err != nil {
				return err.Error()
			}
			markers[memFS.PathJoin(dir, marker)] = m
			return m.Current()

		case "current":
			var dir, marker string
			td.ScanArgs(t, "dir", &dir)
			td.ScanArgs(t, "marker", &marker)
			m := markers[memFS.PathJoin(dir, marker)]
			require.NotNil(t, m)
			return m.Current()

		case "move":
			var dir, marker string
			td.ScanArgs(t, "dir", &dir)
			td.ScanArgs(t, "marker", &marker)
			m := markers[memFS.PathJoin(dir, marker)]
			require.NotNil(t, m)
			err := m.Move(td.Input)
			if err != nil {
				return err.Error()
			}
			return m.Current()

		case "list":
			var dir string
			td.ScanArgs(t, "dir", &dir)
			ls, err := memFS.List(dir)
			if err != nil {
				return err.Error()
			}
			sort.Strings(ls)
			buf.Reset()
			for _, filename := range ls {
				fmt.Fprintln(&buf, filename)
			}
			return buf.String()

		default:
			panic(fmt.Sprintf("unknown command %q", td.Cmd))
		}
	})
}
