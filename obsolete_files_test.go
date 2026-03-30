// Copyright 2019 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package pebble

import (
	"context"
	"fmt"
	"sort"
	"strings"
	"testing"

	"github.com/cockroachdb/crlib/testutils/leaktest"
	"github.com/cockroachdb/datadriven"
	"github.com/cockroachdb/pebble/internal/base"
	"github.com/cockroachdb/pebble/internal/testutils"
	"github.com/cockroachdb/pebble/vfs"
	"github.com/stretchr/testify/require"
)

// TestScanObsoleteFilesUsesFullPath verifies that scanObsoleteFiles uses
// directory-qualified paths when Stat-ing obsolete MANIFEST and OPTIONS files.
// Regression test for a bug where bare filenames (e.g. "MANIFEST-000001") were
// passed to FS.Stat instead of the full path (e.g. "db/MANIFEST-000001"),
// causing the Stat to resolve against the wrong directory on non-local
// filesystems.
func TestScanObsoleteFilesUsesFullPath(t *testing.T) {
	defer leaktest.AfterTest(t)()

	mem := vfs.NewMem()
	dir := "db" // non-root dirname to exercise the path joining

	// Open a DB, write some data, and flush to force MANIFEST rotation.
	d, err := Open(dir, &Options{
		FS:                 mem,
		FormatMajorVersion: FormatNewest,
		Logger:             testutils.Logger{T: t},
	})
	require.NoError(t, err)
	require.NoError(t, d.Set([]byte("a"), []byte("1"), Sync))
	require.NoError(t, d.Flush())
	require.NoError(t, d.Set([]byte("b"), []byte("2"), Sync))
	require.NoError(t, d.Flush())
	require.NoError(t, d.Close())

	// The directory should now contain at least one obsolete MANIFEST or
	// OPTIONS file from the rotations above.
	listing, err := mem.List(dir)
	require.NoError(t, err)
	var hasOldManifest, hasOldOptions bool
	for _, name := range listing {
		ft, _, ok := base.ParseFilename(mem, name)
		if !ok {
			continue
		}
		if ft == base.FileTypeManifest {
			hasOldManifest = true
		}
		if ft == base.FileTypeOptions {
			hasOldOptions = true
		}
	}
	// Sanity check: we should have at least some MANIFEST/OPTIONS files.
	require.True(t, hasOldManifest || hasOldOptions,
		"expected at least one MANIFEST or OPTIONS file in %v", listing)

	// Wrap the FS with a statTrackingFS that records all Stat calls and
	// fails the test if any Stat is called with a bare filename (without
	// the "db/" directory prefix).
	tracker := &statTrackingFS{FS: mem, dir: dir, t: t}

	// Reopen the DB — this triggers scanObsoleteFiles during Open.
	d, err = Open(dir, &Options{
		FS:                 tracker,
		FormatMajorVersion: FormatNewest,
		Logger:             testutils.Logger{T: t},
		DisableTableStats:  true,
	})
	require.NoError(t, err)
	d.TestOnlyWaitForCleaning()

	// Verify we actually observed some Stat calls (the test is meaningful).
	require.True(t, tracker.statCount > 0, "expected at least one Stat call")
	require.NoError(t, d.Close())
}

// statTrackingFS wraps a vfs.FS and verifies that all Stat calls for MANIFEST
// and OPTIONS files use directory-qualified paths.
type statTrackingFS struct {
	vfs.FS
	dir       string
	t         *testing.T
	statCount int
}

func (fs *statTrackingFS) Stat(name string) (vfs.FileInfo, error) {
	// Check if this is a MANIFEST or OPTIONS file by looking at the basename.
	baseName := fs.FS.PathBase(name)
	if strings.HasPrefix(baseName, "MANIFEST-") || strings.HasPrefix(baseName, "OPTIONS-") {
		fs.statCount++
		// The name must be prefixed with the database directory. A bare
		// filename like "MANIFEST-000001" indicates the bug.
		if !strings.HasPrefix(name, fs.dir+"/") {
			fs.t.Errorf("Stat called with bare filename %q; expected %s/%s", name, fs.dir, baseName)
		}
	}
	return fs.FS.Stat(name)
}

func TestCleaner(t *testing.T) {
	defer leaktest.AfterTest(t)()
	dbs := make(map[string]*DB)
	defer func() {
		for _, db := range dbs {
			require.NoError(t, db.Close())
		}
	}()

	mem := vfs.NewMem()
	var memLog base.InMemLogger
	fs := vfs.WithLogging(mem, memLog.Infof)
	datadriven.RunTest(t, "testdata/cleaner", func(t *testing.T, td *datadriven.TestData) string {
		memLog.Reset()
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
			return memLog.String()

		case "compact":
			if len(td.CmdArgs) != 1 {
				return "compact <db>"
			}
			d := dbs[td.CmdArgs[0].String()]
			if err := d.Compact(context.Background(), nil, []byte("\xff"), false); err != nil {
				return err.Error()
			}
			return memLog.String()

		case "flush":
			if len(td.CmdArgs) != 1 {
				return "flush <db>"
			}
			d := dbs[td.CmdArgs[0].String()]
			if err := d.Flush(); err != nil {
				return err.Error()
			}
			return memLog.String()

		case "close":
			if len(td.CmdArgs) != 1 {
				return "close <db>"
			}
			dbDir := td.CmdArgs[0].String()
			d := dbs[dbDir]
			if err := d.Close(); err != nil {
				return err.Error()
			}
			delete(dbs, dbDir)
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
			if len(td.CmdArgs) < 1 || len(td.CmdArgs) > 3 {
				return "open <dir> [archive] [readonly]"
			}
			dir := td.CmdArgs[0].String()
			opts := &Options{
				FS:     fs,
				WALDir: dir + "_wal",
				Logger: testutils.Logger{T: t},
			}
			opts.WithFSDefaults()

			for i := 1; i < len(td.CmdArgs); i++ {
				switch td.CmdArgs[i].String() {
				case "readonly":
					opts.ReadOnly = true
				case "archive":
					opts.Cleaner = ArchiveCleaner{}
				default:
					return "open <dir> [archive] [readonly]"
				}
			}
			// Asynchronous table stats retrieval makes the output flaky.
			opts.DisableTableStats = true
			opts.private.testingAlwaysWaitForCleanup = true
			d, err := Open(dir, opts)
			if err != nil {
				return err.Error()
			}
			d.TestOnlyWaitForCleaning()
			dbs[dir] = d
			return memLog.String()

		case "create-bogus-file":
			if len(td.CmdArgs) != 1 {
				return "create-bogus-file <db/file>"
			}
			dst, err := fs.Create(td.CmdArgs[0].String(), vfs.WriteCategoryUnspecified)
			require.NoError(t, err)
			_, err = dst.Write([]byte("bogus data"))
			require.NoError(t, err)
			require.NoError(t, dst.Sync())
			require.NoError(t, dst.Close())
			return memLog.String()

		default:
			return fmt.Sprintf("unknown command: %s", td.Cmd)
		}
	})
}
