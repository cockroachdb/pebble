// Copyright 2019 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package pebble

import (
	"fmt"
	"sort"
	"strings"
	"testing"

	"github.com/cockroachdb/datadriven"
	"github.com/cockroachdb/pebble/internal/base"
	"github.com/cockroachdb/pebble/vfs"
	"github.com/stretchr/testify/require"
)

func TestArchiveCleaner(t *testing.T) {
	dbs := make(map[string]*DB)
	defer func() {
		for _, db := range dbs {
			require.NoError(t, db.Close())
		}
	}()

	mem := vfs.NewMem()
	var memLog base.InMemLogger
	opts := (&Options{
		Cleaner: ArchiveCleaner{},
		FS:      vfs.WithLogging(mem, memLog.Infof),
		WALDir:  "wal",
	}).WithFSDefaults()

	datadriven.RunTest(t, "testdata/cleaner", func(t *testing.T, td *datadriven.TestData) string {
		switch td.Cmd {
		case "batch":
			if len(td.CmdArgs) != 1 {
				return "batch <db>"
			}
			memLog.Reset()
			d := dbs[td.CmdArgs[0].String()]
			b := d.NewBatch()
			if err := runBatchDefineCmd(td, b); err != nil {
				return err.Error()
			}
			if err := b.Commit(Sync); err != nil {
				return err.Error()
			}
			return memLog.String()

		case "compact":
			if len(td.CmdArgs) != 1 {
				return "compact <db>"
			}
			memLog.Reset()
			d := dbs[td.CmdArgs[0].String()]
			if err := d.Compact(nil, []byte("\xff"), false); err != nil {
				return err.Error()
			}
			return memLog.String()

		case "flush":
			if len(td.CmdArgs) != 1 {
				return "flush <db>"
			}
			memLog.Reset()
			d := dbs[td.CmdArgs[0].String()]
			if err := d.Flush(); err != nil {
				return err.Error()
			}
			return memLog.String()

		case "list":
			if len(td.CmdArgs) != 1 {
				return "list <dir>"
			}
			paths, err := mem.List(td.CmdArgs[0].String())
			if err != nil {
				return err.Error()
			}
			sort.Strings(paths)
			return fmt.Sprintf("%s\n", strings.Join(paths, "\n"))

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

			memLog.Reset()
			dir := td.CmdArgs[0].String()
			d, err := Open(dir, opts)
			if err != nil {
				return err.Error()
			}
			dbs[dir] = d
			return memLog.String()

		default:
			return fmt.Sprintf("unknown command: %s", td.Cmd)
		}
	})
}
