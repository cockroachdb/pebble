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

func TestAtomicMarker_FilenameRoundtrip(t *testing.T) {
	filenames := []string{
		"marker.foo.000003.MANIFEST-000021",
		"marker.bar.000003.MANIFEST-000021",
		"marker.version.000003.1",
		"marker.version.000003.1.2.3.4",
		"marker.current.500000.MANIFEST-000001",
		"marker.current.18446744073709551615.MANIFEST-000001",
	}
	for _, testFilename := range filenames {
		t.Run(testFilename, func(t *testing.T) {
			name, iter, value, ok := parseMarkerFilename(testFilename)
			require.True(t, ok)

			filename := markerFilename(name, iter, value)
			require.Equal(t, testFilename, filename)
		})
	}
}

func TestAtomicMarker_Parsefilename(t *testing.T) {
	testCases := map[string]bool{
		"marker.current.000003.MANIFEST-000021":  true,
		"marker.current.10.MANIFEST-000021":      true,
		"marker.v.10.1.2.3.4":                    true,
		"marker.name.18446744073709551615.value": true,
		"marke.current.000003.MANIFEST-000021":   false,
		"marker.current.foo.MANIFEST-000021":     false,
		"marker.current.ffffff.MANIFEST-000021":  false,
	}
	for filename, wantOk := range testCases {
		t.Run(filename, func(t *testing.T) {
			_, _, _, ok := parseMarkerFilename(filename)
			require.Equal(t, wantOk, ok)
		})
	}
}

func TestAtomicMarker(t *testing.T) {
	markers := map[string]*AtomicMarker{}
	memFS := NewMem()

	var buf bytes.Buffer
	datadriven.RunTest(t, "testdata/atomic_marker", func(td *datadriven.TestData) string {
		switch td.Cmd {
		case "current":
			var dir, marker string
			td.ScanArgs(t, "dir", &dir)
			td.ScanArgs(t, "marker", &marker)
			m := markers[memFS.PathJoin(dir, marker)]
			require.NotNil(t, m)
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

		case "mkdir-all":
			if len(td.CmdArgs) != 1 {
				return "usage: mkdir-all <dir>"
			}
			if err := memFS.MkdirAll(td.CmdArgs[0].String(), os.ModePerm); err != nil {
				return err.Error()
			}
			return ""

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

		case "remove-obsolete":
			var dir, marker string
			td.ScanArgs(t, "dir", &dir)
			td.ScanArgs(t, "marker", &marker)
			m := markers[memFS.PathJoin(dir, marker)]
			require.NotNil(t, m)
			require.NoError(t, m.RemoveObsolete())
			return ""

		case "touch":
			f, err := memFS.Create(td.CmdArgs[0].String())
			if err != nil {
				return err.Error()
			}
			if err := f.Close(); err != nil {
				return err.Error()
			}
			return ""

		default:
			panic(fmt.Sprintf("unknown command %q", td.Cmd))
		}
	})
}
