// Copyright 2021 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package pebble

import (
	"fmt"
	"testing"

	"github.com/cockroachdb/pebble/sstable"
	"github.com/cockroachdb/pebble/vfs"
	"github.com/cockroachdb/pebble/vfs/atomicfs"
	"github.com/stretchr/testify/require"
)

func TestFormatMajorVersion_MigrationDefined(t *testing.T) {
	for v := FormatMostCompatible; v <= FormatNewest; v++ {
		if _, ok := formatMajorVersionMigrations[v]; !ok {
			t.Errorf("format major version %d has no migration defined", v)
		}
	}
}

func TestRatchetFormat(t *testing.T) {
	fs := vfs.NewMem()
	d, err := Open("", &Options{FS: fs})
	require.NoError(t, err)
	require.NoError(t, d.Set([]byte("foo"), []byte("bar"), Sync))
	require.Equal(t, FormatMostCompatible, d.FormatMajorVersion())
	require.NoError(t, d.RatchetFormatMajorVersion(FormatVersioned))
	require.Equal(t, FormatVersioned, d.FormatMajorVersion())
	require.NoError(t, d.RatchetFormatMajorVersion(FormatVersioned))
	require.Equal(t, FormatVersioned, d.FormatMajorVersion())
	require.NoError(t, d.RatchetFormatMajorVersion(FormatSetWithDelete))
	require.Equal(t, FormatSetWithDelete, d.FormatMajorVersion())
	require.NoError(t, d.RatchetFormatMajorVersion(FormatBlockPropertyCollector))
	require.Equal(t, FormatBlockPropertyCollector, d.FormatMajorVersion())
	require.NoError(t, d.Close())

	// If we Open the database again, leaving the default format, the
	// database should Open using the persisted FormatNewest.
	d, err = Open("", &Options{FS: fs})
	require.NoError(t, err)
	require.Equal(t, FormatNewest, d.FormatMajorVersion())
	require.NoError(t, d.Close())

	// Move the marker to a version that does not exist.
	m, _, err := atomicfs.LocateMarker(fs, "", formatVersionMarkerName)
	require.NoError(t, err)
	require.NoError(t, m.Move("999999"))
	require.NoError(t, m.Close())

	_, err = Open("", &Options{
		FS:                 fs,
		FormatMajorVersion: FormatVersioned,
	})
	require.Error(t, err)
	require.EqualError(t, err, `pebble: database "" written in format major version 999999`)
}

func testBasicDB(d *DB) error {
	key := []byte("a")
	value := []byte("b")
	if err := d.Set(key, value, nil); err != nil {
		return err
	}
	if err := d.Flush(); err != nil {
		return err
	}
	if err := d.Compact(nil, []byte("\xff"), false); err != nil {
		return err
	}

	iter := d.NewIter(nil)
	for valid := iter.First(); valid; valid = iter.Next() {
	}
	if err := iter.Close(); err != nil {
		return err
	}
	return nil
}

func TestFormatMajorVersions(t *testing.T) {
	for vers := FormatMostCompatible; vers <= FormatNewest; vers++ {
		t.Run(fmt.Sprintf("vers=%03d", vers), func(t *testing.T) {
			fs := vfs.NewStrictMem()
			opts := &Options{
				FS:                 fs,
				FormatMajorVersion: vers,
			}

			// Create a database at this format major version and perform
			// some very basic operations.
			d, err := Open("", opts)
			require.NoError(t, err)
			require.NoError(t, testBasicDB(d))
			require.NoError(t, d.Close())

			// Re-open the database at this format major version, and again
			// perform some basic operations.
			d, err = Open("", opts)
			require.NoError(t, err)
			require.NoError(t, testBasicDB(d))
			require.NoError(t, d.Close())

			t.Run("upgrade-at-open", func(t *testing.T) {
				for upgradeVers := vers + 1; upgradeVers <= FormatNewest; upgradeVers++ {
					t.Run(fmt.Sprintf("upgrade-vers=%03d", upgradeVers), func(t *testing.T) {
						// We use vfs.MemFS's option to ignore syncs so
						// that we can perform an upgrade on the current
						// database state in fs, and revert it when this
						// subtest is complete.
						fs.SetIgnoreSyncs(true)
						defer fs.ResetToSyncedState()

						// Re-open the database, passing a higher format
						// major version in the Options to automatically
						// ratchet the format major version. Ensure some
						// basic operations pass.
						opts := opts.Clone()
						opts.FormatMajorVersion = upgradeVers
						d, err = Open("", opts)
						require.NoError(t, err)
						require.Equal(t, upgradeVers, d.FormatMajorVersion())
						require.NoError(t, testBasicDB(d))
						require.NoError(t, d.Close())

						// Re-open to ensure the upgrade persisted.
						d, err = Open("", opts)
						require.NoError(t, err)
						require.Equal(t, upgradeVers, d.FormatMajorVersion())
						require.NoError(t, testBasicDB(d))
						require.NoError(t, d.Close())
					})
				}
			})

			t.Run("upgrade-while-open", func(t *testing.T) {
				for upgradeVers := vers + 1; upgradeVers <= FormatNewest; upgradeVers++ {
					t.Run(fmt.Sprintf("upgrade-vers=%03d", upgradeVers), func(t *testing.T) {
						// Ensure the previous tests don't overwrite our
						// options.
						require.Equal(t, vers, opts.FormatMajorVersion)

						// We use vfs.MemFS's option to ignore syncs so
						// that we can perform an upgrade on the current
						// database state in fs, and revert it when this
						// subtest is complete.
						fs.SetIgnoreSyncs(true)
						defer fs.ResetToSyncedState()

						// Re-open the database, still at the current format
						// major version. Perform some basic operations,
						// ratchet the format version up, and perform
						// additional basic operations.
						d, err = Open("", opts)
						require.NoError(t, err)
						require.NoError(t, testBasicDB(d))
						require.Equal(t, vers, d.FormatMajorVersion())
						require.NoError(t, d.RatchetFormatMajorVersion(upgradeVers))
						require.Equal(t, upgradeVers, d.FormatMajorVersion())
						require.NoError(t, testBasicDB(d))
						require.NoError(t, d.Close())

						// Re-open to ensure the upgrade persisted.
						d, err = Open("", opts)
						require.NoError(t, err)
						require.Equal(t, upgradeVers, d.FormatMajorVersion())
						require.NoError(t, testBasicDB(d))
						require.NoError(t, d.Close())
					})
				}
			})
		})
	}
}

func TestFormatMajorVersions_TableFormat(t *testing.T) {
	// NB: This test is intended to validate the mapping between every
	// FormatMajorVersion and sstable.TableFormat exhaustively. This serves as a
	// sanity check that new versions have a corresponding mapping. The test
	// fixture is intentionally verbose.

	m := map[FormatMajorVersion]sstable.TableFormat{
		FormatDefault:                 sstable.TableFormatRocksDBv2,
		FormatMostCompatible:          sstable.TableFormatRocksDBv2,
		formatVersionedManifestMarker: sstable.TableFormatRocksDBv2,
		FormatVersioned:               sstable.TableFormatRocksDBv2,
		FormatSetWithDelete:           sstable.TableFormatRocksDBv2,
		FormatBlockPropertyCollector:  sstable.TableFormatPebblev1,
	}

	// Valid versions.
	for fmv := FormatMostCompatible; fmv <= FormatNewest; fmv++ {
		f := fmv.MaxTableFormat()
		require.Equalf(t, m[fmv], f, "got %s; want %s", f, m[fmv])
	}

	// Invalid versions.
	fmv := FormatNewest + 1
	require.Panics(t, func() { _ = fmv.MaxTableFormat() })
}
