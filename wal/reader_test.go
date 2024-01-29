// Copyright 2024 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package wal

import (
	"bytes"
	"fmt"
	"os"
	"slices"
	"strings"
	"testing"

	"github.com/cockroachdb/datadriven"
	"github.com/cockroachdb/pebble/vfs"
	"github.com/stretchr/testify/require"
)

func TestList(t *testing.T) {
	var buf bytes.Buffer
	filesystems := map[string]*vfs.MemFS{}
	getFS := func(name string) *vfs.MemFS {
		if _, ok := filesystems[name]; !ok {
			filesystems[name] = vfs.NewMem()
		}
		return filesystems[name]
	}

	datadriven.RunTest(t, "testdata/list", func(t *testing.T, td *datadriven.TestData) string {
		buf.Reset()
		switch td.Cmd {
		case "list":
			var dirs []Dir
			for _, arg := range td.CmdArgs {
				var dirname string
				if len(arg.Vals) > 1 {
					dirname = arg.Vals[1]
				}
				dirs = append(dirs, Dir{
					FS:      getFS(arg.Vals[0]),
					Dirname: dirname,
				})
			}
			logs, err := listLogs(dirs...)
			if err != nil {
				return err.Error()
			}
			for i := range logs {
				fmt.Fprintln(&buf, logs[i].String())
			}
			return buf.String()
		case "reset":
			clear(filesystems)
			return ""
		case "touch":
			for _, l := range strings.Split(strings.TrimSpace(td.Input), "\n") {
				if l == "" {
					continue
				}
				fields := strings.Fields(l)
				fsName := fields[0]
				filename := fields[1]

				fs := getFS(fsName)
				require.NoError(t, fs.MkdirAll(fs.PathDir(filename), os.ModePerm))
				f, err := fs.Create(filename, vfs.WriteCategoryUnspecified)
				require.NoError(t, err)
				require.NoError(t, f.Close())
			}

			// Sort the filesystem names for determinism.
			var names []string
			for name := range filesystems {
				names = append(names, name)
			}
			slices.Sort(names)
			for _, name := range names {
				fmt.Fprintf(&buf, "%s:\n", name)
				fmt.Fprint(&buf, filesystems[name].String())
			}
			return buf.String()
		default:
			return fmt.Sprintf("unrecognized command %q", td.Cmd)
		}
	})
}
