// Copyright 2021 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package pebble

import (
	"fmt"
	"strconv"

	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/pebble/internal/base"
	"github.com/cockroachdb/pebble/sstable"
	"github.com/cockroachdb/pebble/vfs"
	"github.com/cockroachdb/pebble/vfs/atomicfs"
)

// FormatMajorVersion is a constant controlling the format of persisted
// data. Backwards incompatible changes to durable formats are gated
// behind new format major versions.
//
// At any point, a database's format major version may be bumped.
// However, once a database's format major version is increased,
// previous versions of Pebble will refuse to open the database.
//
// The zero value format is the FormatDefault constant. The exact
// FormatVersion that the default corresponds to may change with time.
type FormatMajorVersion uint64

// String implements fmt.Stringer.
func (v FormatMajorVersion) String() string {
	// NB: This must not change. It's used as the value for the the
	// on-disk version marker file.
	//
	// Specifically, this value must always parse as a base 10 integer
	// that fits in a uint64. We format it as zero-padded, 3-digit
	// number today, but the padding may change.
	return fmt.Sprintf("%03d", v)
}

const (
	// FormatDefault leaves the format version unspecified. The
	// FormatDefault constant may be ratcheted upwards over time.
	FormatDefault FormatMajorVersion = iota
	// FormatMostCompatible maintains the most backwards compatibility,
	// maintaining bi-directional compatibility with RocksDB 6.2.1 in
	// the particular configuration described in the Pebble README.
	FormatMostCompatible
	// formatVersionedManifestMarker is the first
	// backwards-incompatible change made to Pebble, introducing the
	// format-version marker file for handling backwards-incompatible
	// changes more broadly, and replacing the `CURRENT` file with a
	// marker file.
	//
	// This format version is intended as an intermediary version state.
	// It is deliberately unexported to discourage direct use of this
	// format major version.  Clients should use FormatVersioned which
	// also ensures earlier versions of Pebble fail to open a database
	// written in a future format major version.
	formatVersionedManifestMarker
	// FormatVersioned is a new format major version that replaces the
	// old `CURRENT` file with a new 'marker' file scheme.  Previous
	// Pebble versions will be unable to open the database unless
	// they're aware of format versions.
	FormatVersioned
	// FormatSetWithDelete is a format major version that introduces a new key
	// kind, base.InternalKeyKindSetWithDelete. Previous Pebble versions will be
	// unable to open this database.
	FormatSetWithDelete
	// FormatBlockPropertyCollector is a format major version that introduces
	// BlockPropertyCollectors.
	FormatBlockPropertyCollector
	// FormatRangeKeys is a format major version that introduces range keys.
	FormatRangeKeys
	// FormatNewest always contains the most recent format major version.
	// NB: When adding new versions, the MaxTableFormat method should also be
	// updated to return the maximum allowable version for the new
	// FormatMajorVersion.
	FormatNewest FormatMajorVersion = FormatRangeKeys
)

// MaxTableFormat returns the maximum sstable.TableFormat that can be used at
// this FormatMajorVersion.
func (v FormatMajorVersion) MaxTableFormat() sstable.TableFormat {
	switch v {
	case FormatDefault, FormatMostCompatible, formatVersionedManifestMarker,
		FormatVersioned, FormatSetWithDelete:
		return sstable.TableFormatRocksDBv2
	case FormatBlockPropertyCollector:
		return sstable.TableFormatPebblev1
	case FormatRangeKeys:
		return sstable.TableFormatPebblev2
	default:
		panic(fmt.Sprintf("pebble: unsupported format major version: %s", v))
	}
}

// formatMajorVersionMigrations defines the migrations from one format
// major version to the next. Each migration is defined as a closure
// which will be invoked on the database before the new format major
// version is committed. Migrations must be idempotent. Migrations are
// invoked with d.mu locked.
//
// Each migration is responsible for invoking finalizeFormatVersUpgrade
// to set the new format major version.  RatchetFormatMajorVersion will
// panic if a migration returns a nil error but fails to finalize the
// new format major version.
var formatMajorVersionMigrations = map[FormatMajorVersion]func(*DB) error{
	FormatMostCompatible: func(d *DB) error { return nil },
	formatVersionedManifestMarker: func(d *DB) error {
		// formatVersionedManifestMarker introduces the use of a marker
		// file for pointing to the current MANIFEST file.

		// Lock the manifest.
		d.mu.versions.logLock()
		defer d.mu.versions.logUnlock()

		// Construct the filename of the currently active manifest and
		// move the manifest marker to that filename. The marker is
		// guaranteed to exist, because we unconditionally locate it
		// during Open.
		manifestFileNum := d.mu.versions.manifestFileNum
		filename := base.MakeFilename(fileTypeManifest, manifestFileNum)
		if err := d.mu.versions.manifestMarker.Move(filename); err != nil {
			return errors.Wrap(err, "moving manifest marker")
		}

		// Now that we have a manifest marker file in place and pointing
		// to the current MANIFEST, finalize the upgrade. If we fail for
		// some reason, a retry of this migration is guaranteed to again
		// move the manifest marker file to the latest manifest. If
		// we're unable to finalize the upgrade, a subsequent call to
		// Open will ignore the manifest marker.
		if err := d.finalizeFormatVersUpgrade(formatVersionedManifestMarker); err != nil {
			return err
		}

		// We've finalized the upgrade. All subsequent Open calls will
		// ignore the CURRENT file and instead read the manifest marker.
		// Before we unlock the manifest, we need to update versionSet
		// to use the manifest marker on future rotations.
		d.mu.versions.setCurrent = setCurrentFuncMarker(
			d.mu.versions.manifestMarker,
			d.mu.versions.fs,
			d.mu.versions.dirname)
		return nil
	},
	// The FormatVersioned version is split into two, each with their
	// own migration to ensure the post-migration cleanup happens even
	// if there's a crash immediately after finalizing the version. Once
	// a new format major version is finalized, its migration will never
	// run again. Post-migration cleanup like the one in the migration
	// below must be performed in a separate migration or every time the
	// database opens.
	FormatVersioned: func(d *DB) error {
		// Replace the `CURRENT` file with one that points to the
		// nonexistent `MANIFEST-000000` file. If an earlier Pebble
		// version that does not know about format major versions
		// attempts to open the database, it will error avoiding
		// accidental corruption.
		if err := setCurrentFile(d.mu.versions.dirname, d.mu.versions.fs, 0); err != nil {
			return err
		}
		return d.finalizeFormatVersUpgrade(FormatVersioned)
	},
	// As SetWithDelete is a new key kind, there is nothing to migrate. We can
	// simply finalize the format version and we're done.
	FormatSetWithDelete: func(d *DB) error {
		return d.finalizeFormatVersUpgrade(FormatSetWithDelete)
	},
	FormatBlockPropertyCollector: func(d *DB) error {
		return d.finalizeFormatVersUpgrade(FormatBlockPropertyCollector)
	},
	FormatRangeKeys: func(d *DB) error {
		return d.finalizeFormatVersUpgrade(FormatRangeKeys)
	},
}

const formatVersionMarkerName = `format-version`

func lookupFormatMajorVersion(
	fs vfs.FS, dirname string,
) (FormatMajorVersion, *atomicfs.Marker, error) {
	m, versString, err := atomicfs.LocateMarker(fs, dirname, formatVersionMarkerName)
	if err != nil {
		return 0, nil, err
	}
	if versString == "" {
		return FormatMostCompatible, m, nil
	}
	v, err := strconv.ParseUint(versString, 10, 64)
	if err != nil {
		return 0, nil, errors.Wrap(err, "parsing format major version")
	}
	vers := FormatMajorVersion(v)
	if vers == FormatDefault {
		return 0, nil, errors.Newf("pebble: default format major version should not persisted", vers)
	}
	if vers > FormatNewest {
		return 0, nil, errors.Newf("pebble: database %q written in format major version %d", dirname, vers)
	}
	return vers, m, nil
}

// FormatMajorVersion returns the database's active format major
// version. The format major version may be higher than the one
// provided in Options when the database was opened if the existing
// database was written with a higher format version.
func (d *DB) FormatMajorVersion() FormatMajorVersion {
	d.mu.Lock()
	defer d.mu.Unlock()
	return d.mu.formatVers.vers
}

// RatchetFormatMajorVersion ratchets the opened database's format major
// version to the provided version. It errors if the provided format
// major version is below the database's current version. Once a
// database's format major version is upgraded, previous Pebble versions
// that do not know of the format version will be unable to open the
// database.
func (d *DB) RatchetFormatMajorVersion(fmv FormatMajorVersion) error {
	if err := d.closed.Load(); err != nil {
		panic(err)
	}

	d.mu.Lock()
	defer d.mu.Unlock()
	return d.ratchetFormatMajorVersionLocked(fmv)
}

func (d *DB) ratchetFormatMajorVersionLocked(formatVers FormatMajorVersion) error {
	if d.opts.ReadOnly {
		return ErrReadOnly
	}
	if formatVers > FormatNewest {
		// Guard against accidentally forgetting to update FormatNewest.
		return errors.Errorf("pebble: unknown format version %d", formatVers)
	}
	if d.mu.formatVers.vers > formatVers {
		return errors.Newf("pebble: database already at format major version %d; cannot reduce to %d",
			d.mu.formatVers.vers, formatVers)
	}
	for nextVers := d.mu.formatVers.vers + 1; nextVers <= formatVers; nextVers++ {
		if err := formatMajorVersionMigrations[nextVers](d); err != nil {
			return errors.Wrapf(err, "migrating to version %d", nextVers)
		}

		// NB: The migration is responsible for calling
		// finalizeFormatVersUpgrade to finalize the upgrade. This
		// structure is necessary because some migrations may need to
		// update in-memory state (without ever dropping locks) after
		// the upgrade is finalized. Here we assert that the upgrade
		// did occur.
		if d.mu.formatVers.vers != nextVers {
			d.opts.Logger.Fatalf("pebble: successful migration to format version %d never finalized the upgrade", nextVers)
		}
	}
	return nil
}

// finalizeFormatVersUpgrade is typically only be called from within a
// format major version migration.
//
// See formatMajorVersionMigrations.
func (d *DB) finalizeFormatVersUpgrade(formatVers FormatMajorVersion) error {
	// We use the marker to encode the active format version in the
	// marker filename. Unlike other uses of the atomic marker, there is
	// no file with the filename `formatVers.String()` on the
	// filesystem.
	if err := d.mu.formatVers.marker.Move(formatVers.String()); err != nil {
		return err
	}
	d.mu.formatVers.vers = formatVers
	d.opts.EventListener.FormatUpgrade(formatVers)
	return nil
}
