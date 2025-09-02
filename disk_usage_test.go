// Copyright 2025 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package pebble

import (
	"fmt"
	"testing"

	"github.com/cockroachdb/datadriven"
	"github.com/cockroachdb/pebble/objstorage/remote"
	"github.com/cockroachdb/pebble/vfs"
	"github.com/stretchr/testify/require"
)

// Test that the EstimateDiskUsage and EstimateDiskUsageByBackingType should panic when the DB is closed
func TestEstimateDiskUsageClosedDB(t *testing.T) {
	mem := vfs.NewMem()
	d, err := Open("", &Options{FS: mem})
	require.NoError(t, err)
	require.NoError(t, d.Set([]byte("key"), []byte("value"), nil))
	require.NoError(t, d.Close())

	// Attempting to estimate on closed DB should panic
	require.Panics(t, func() {
		d.EstimateDiskUsage([]byte("a"), []byte("z"))
	})

	require.Panics(t, func() {
		d.EstimateDiskUsageByBackingType([]byte("a"), []byte("z"))
	})
}

// Test the EstimateDiskUsage and EstimateDiskUsageByBackingType data driven tests
func TestEstimateDiskUsageDataDriven(t *testing.T) {
	fs := vfs.NewMem()
	remoteStorage := remote.NewInMem()
	var d *DB
	defer func() {
		if d != nil {
			_ = d.Close()
		}
	}()

	datadriven.RunTest(t, "testdata/disk_usage", func(t *testing.T, td *datadriven.TestData) string {
		switch td.Cmd {
		case "open":
			if d != nil {
				require.NoError(t, d.Close())
				d = nil
			}
			opts := &Options{FS: fs, FormatMajorVersion: FormatExciseBoundsRecord}
			opts.Experimental.RemoteStorage = remote.MakeSimpleFactory(map[remote.Locator]remote.Storage{
				"external-locator": remoteStorage,
			})
			require.NoError(t, parseDBOptionsArgs(opts, td.CmdArgs))
			var err error
			d, err = Open("", opts)
			require.NoError(t, err)
			return ""

		case "close":
			if d != nil {
				require.NoError(t, d.Close())
				d = nil
			}
			return ""

		case "batch":
			b := d.NewBatch()
			if err := runBatchDefineCmd(td, b); err != nil {
				return err.Error()
			}
			if err := b.Commit(nil); err != nil {
				return err.Error()
			}
			return ""

		case "flush":
			if err := d.Flush(); err != nil {
				return err.Error()
			}
			return ""

		case "build":
			if err := runBuildCmd(td, d, fs); err != nil {
				return err.Error()
			}
			return ""

		case "ingest":
			if err := runIngestCmd(td, d, fs); err != nil {
				return err.Error()
			}
			return ""
		case "build-remote":
			if err := runBuildRemoteCmd(td, d, remoteStorage); err != nil {
				return err.Error()
			}
			return ""
		case "ingest-external":
			if err := runIngestExternalCmd(t, td, d, remoteStorage, "external-locator"); err != nil {
				return err.Error()
			}
			return ""
		case "compact":
			if err := runCompactCmd(td, d); err != nil {
				return err.Error()
			}
			return runLSMCmd(td, d)

		case "estimate-disk-usage":
			// Parse range arguments, default to "a" and "z" if not specified
			start := []byte("a")
			end := []byte("z")
			if len(td.CmdArgs) >= 2 {
				start = []byte(td.CmdArgs[0].Key)
				end = []byte(td.CmdArgs[1].Key)
			}
			size, err := d.EstimateDiskUsage(start, end)
			if err != nil {
				return err.Error()
			}
			if arg, ok := td.Arg("expect"); ok && len(arg.Vals) > 0 {
				switch arg.Vals[0] {
				case "zero":
					require.Equal(t, uint64(0), size)
				case "non-zero":
					require.Greater(t, size, uint64(0))
				}
			}
			return "success"

		case "estimate-disk-usage-by-backing-type":
			// Parse range arguments, default to "a" and "z" if not specified
			start := []byte("a")
			end := []byte("z")
			if len(td.CmdArgs) >= 2 {
				start = []byte(td.CmdArgs[0].Key)
				end = []byte(td.CmdArgs[1].Key)
			}
			total, remote, external, err := d.EstimateDiskUsageByBackingType(start, end)
			if err != nil {
				return err.Error()
			}
			// remote and external should be less than or equal to total
			require.LessOrEqual(t, remote, total)
			require.LessOrEqual(t, external, remote)

			if arg, ok := td.Arg("expect-total"); ok && len(arg.Vals) > 0 {
				switch arg.Vals[0] {
				case "zero":
					require.Equal(t, uint64(0), total)
				case "non-zero":
					require.Greater(t, total, uint64(0))
				}
			}
			if arg, ok := td.Arg("expect-remote"); ok && len(arg.Vals) > 0 {
				switch arg.Vals[0] {
				case "zero":
					require.Equal(t, uint64(0), remote)
				case "non-zero":
					require.Greater(t, remote, uint64(0))
				}
			}
			if arg, ok := td.Arg("expect-external"); ok && len(arg.Vals) > 0 {
				switch arg.Vals[0] {
				case "zero":
					require.Equal(t, uint64(0), external)
				case "non-zero":
					require.Greater(t, external, uint64(0))
				}
			}
			return "success"

		default:
			return fmt.Sprintf("unknown command: %s", td.Cmd)
		}
	})
}
