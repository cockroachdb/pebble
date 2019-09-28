// Copyright 2019 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package pebble

import (
	"fmt"
	"sort"
	"strings"
	"testing"

	"github.com/cockroachdb/pebble/internal/datadriven"
	"github.com/cockroachdb/pebble/vfs"
)

func TestCheckpoint(t *testing.T) {
	dbs := make(map[string]*DB)
	var buf syncedBuffer
	mem := vfs.NewMem()
	opts := &Options{
		FS: loggingFS{mem, &buf},
	}

	datadriven.RunTest(t, "testdata/checkpoint", func(td *datadriven.TestData) string {
		switch td.Cmd {
		case "batch":
			if len(td.CmdArgs) != 1 {
				return "batch <db>"
			}
			buf.Reset()
			d := dbs[td.CmdArgs[0].String()]
			b := d.NewBatch()
			if err := runBatchDefineCmd(td, b); err != nil {
				return err.Error()
			}
			if err := b.Commit(Sync); err != nil {
				return err.Error()
			}
			return buf.String()

		case "checkpoint":
			if len(td.CmdArgs) != 2 {
				return "checkpoint <db> <dir>"
			}
			buf.Reset()
			d := dbs[td.CmdArgs[0].String()]
			if err := d.Checkpoint(td.CmdArgs[1].String()); err != nil {
				return err.Error()
			}
			return buf.String()

		case "compact":
			if len(td.CmdArgs) != 1 {
				return "compact <db>"
			}
			buf.Reset()
			d := dbs[td.CmdArgs[0].String()]
			if err := d.Compact(nil, []byte("\xff")); err != nil {
				return err.Error()
			}
			return buf.String()

		case "flush":
			if len(td.CmdArgs) != 1 {
				return "flush <db>"
			}
			buf.Reset()
			d := dbs[td.CmdArgs[0].String()]
			if err := d.Flush(); err != nil {
				return err.Error()
			}
			return buf.String()

		case "list":
			if len(td.CmdArgs) != 1 {
				return "list <dir>"
			}
			paths, err := mem.List(td.CmdArgs[0].String())
			if err != nil {
				return err.Error()
			}
			sort.Strings(paths)
			buf.Reset()
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

			buf.Reset()
			dir := td.CmdArgs[0].String()
			d, err := Open(dir, opts)
			if err != nil {
				return err.Error()
			}
			dbs[dir] = d
			return buf.String()

		case "scan":
			if len(td.CmdArgs) != 1 {
				return "scan <db>"
			}
			buf.Reset()
			d := dbs[td.CmdArgs[0].String()]
			iter := d.NewIter(nil)
			for valid := iter.First(); valid; valid = iter.Next() {
				fmt.Fprintf(&buf, "%s %s\n", iter.Key(), iter.Value())
			}
			fmt.Fprintf(&buf, ".\n")
			if err := iter.Close(); err != nil {
				fmt.Fprintf(&buf, "%v\n", err)
			}
			return buf.String()

		default:
			return fmt.Sprintf("unknown command: %s", td.Cmd)
		}
	})
}
