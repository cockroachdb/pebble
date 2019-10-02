// Copyright 2019 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package pebble

import (
	"bytes"
	"fmt"
	"sort"
	"strings"
	"testing"

	"github.com/cockroachdb/pebble/internal/datadriven"
	"github.com/cockroachdb/pebble/vfs"
)

func TestArchiveCleaner(t *testing.T) {
	dbs := make(map[string]*DB)
	mem := vfs.NewMem()
	opts := &Options{
		FS:      mem,
		Cleaner: ArchiveCleaner{},
	}

	datadriven.RunTest(t, "testdata/cleaner", func(td *datadriven.TestData) string {
		switch td.Cmd {
		case "batch":
			if len(td.CmdArgs) != 1 {
				return "batch <db>"
			}
			d := dbs[td.CmdArgs[0].String()]
			b := d.NewBatch()
			if err := runBatchDefineCmd(td, b); err != nil {
				return err.Error()
			}
			if err := b.Commit(Sync); err != nil {
				return err.Error()
			}
			return ""

		case "compact":
			if len(td.CmdArgs) != 1 {
				return "compact <db>"
			}
			d := dbs[td.CmdArgs[0].String()]
			if err := d.Compact(nil, []byte("\xff")); err != nil {
				return err.Error()
			}
			return ""

		case "flush":
			if len(td.CmdArgs) != 1 {
				return "flush <db>"
			}
			d := dbs[td.CmdArgs[0].String()]
			if err := d.Flush(); err != nil {
				return err.Error()
			}
			return ""

		case "list":
			var buf bytes.Buffer

			if len(td.CmdArgs) != 1 {
				return "list <dir>"
			}
			paths, err := mem.List(td.CmdArgs[0].String())
			if err != nil {
				return err.Error()
			}
			sort.Strings(paths)
			fmt.Fprintf(&buf, "%s\n", strings.Join(paths, "\n"))
			return buf.String()

		case "open":
			if len(td.CmdArgs) != 1 && len(td.CmdArgs) != 2 {
				return "open <dir> [readonly]"
			}
			opts.ReadOnly = false
			if len(td.CmdArgs) == 2 {
				if td.CmdArgs[1].String() != "readonly" {
					return "open <dir> [readonly]"
				}
				opts.ReadOnly = true
			}

			dir := td.CmdArgs[0].String()
			d, err := Open(dir, opts)
			if err != nil {
				return err.Error()
			}
			dbs[dir] = d
			return ""

		default:
			return fmt.Sprintf("unknown command: %s", td.Cmd)
		}
	})
}
