// Copyright 2021 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package pebble

import (
	"fmt"
	"strconv"

	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/pebble/internal/manifest"
	"github.com/cockroachdb/pebble/objstorage/remote"
	"github.com/cockroachdb/pebble/sstable"
	"github.com/cockroachdb/pebble/sstable/blob"
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

// SafeValue implements redact.SafeValue.
func (v FormatMajorVersion) SafeValue() {}

// String implements fmt.Stringer.
func (v FormatMajorVersion) String() string {
	// NB: This must not change. It's used as the value for the on-disk
	// version marker file.
	//
	// Specifically, this value must always parse as a base 10 integer
	// that fits in a uint64. We format it as zero-padded, 3-digit
	// number today, but the padding may change.
	return fmt.Sprintf("%03d", v)
}

const (
	// FormatDefault leaves the format version unspecified. When used to create a
	// new store, Pebble will choose the earliest format version it supports.
	FormatDefault FormatMajorVersion = iota

	// 21.2 versions.

	// FormatMostCompatible maintains the most backwards compatibility,
	// maintaining bi-directional compatibility with RocksDB 6.2.1 in
	// the particular configuration described in the Pebble README.
	// Deprecated.
	_ // FormatMostCompatible

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
	// Deprecated.
	_ // formatVersionedManifestMarker

	// FormatVersioned is a new format major version that replaces the
	// old `CURRENT` file with a new 'marker' file scheme.  Previous
	// Pebble versions will be unable to open the database unless
	// they're aware of format versions.
	// Deprecated.
	_ // FormatVersioned

	// FormatSetWithDelete is a format major version that introduces a new key
	// kind, base.InternalKeyKindSetWithDelete. Previous Pebble versions will be
	// unable to open this database.
	// Deprecated.
	_ // FormatSetWithDelete

	// 22.1 versions.

	// FormatBlockPropertyCollector is a format major version that introduces
	// BlockPropertyCollectors.
	// Deprecated.
	_ // FormatBlockPropertyCollector

	// FormatSplitUserKeysMarked is a format major version that guarantees that
	// all files that share user keys with neighbors are marked for compaction
	// in the manifest. Ratcheting to FormatSplitUserKeysMarked will block
	// (without holding mutexes) until the scan of the LSM is complete and the
	// manifest has been rotated.
	// Deprecated.
	_ // FormatSplitUserKeysMarked

	// 22.2 versions.

	// FormatSplitUserKeysMarkedCompacted is a format major version that
	// guarantees that all files explicitly marked for compaction in the manifest
	// have been compacted. Combined with the FormatSplitUserKeysMarked format
	// major version, this version guarantees that there are no user keys split
	// across multiple files within a level L1+. Ratcheting to this format version
	// will block (without holding mutexes) until all necessary compactions for
	// files marked for compaction are complete.
	// Deprecated.
	_ // FormatSplitUserKeysMarkedCompacted

	// FormatRangeKeys is a format major version that introduces range keys.
	// Deprecated.
	_ // FormatRangeKeys

	// FormatMinTableFormatPebblev1 is a format major version that guarantees that
	// tables created by or ingested into the DB at or above this format major
	// version will have a table format version of at least Pebblev1 (Block
	// Properties).
	// Deprecated.
	_ // FormatMinTableFormatPebblev1

	// FormatPrePebblev1Marked is a format major version that guarantees that all
	// sstables with a table format version pre-Pebblev1 (i.e. those that are
	// guaranteed to not contain block properties) are marked for compaction in
	// the manifest. Ratcheting to FormatPrePebblev1Marked will block (without
	// holding mutexes) until the scan of the LSM is complete and the manifest has
	// been rotated.
	// Deprecated.
	_ // FormatPrePebblev1Marked

	// 23.1 versions.

	// formatUnusedPrePebblev1MarkedCompacted is an unused format major version.
	// This format major version was originally intended to ship in the 23.1
	// release. It was later decided that this should be deferred until a
	// subsequent release. The original ordering is preserved so as not to
	// introduce breaking changes in Cockroach.
	_ // formatUnusedPrePebblev1MarkedCompacted

	// FormatSSTableValueBlocks is a format major version that adds support for
	// storing values in value blocks in the sstable. Value block support is not
	// necessarily enabled when writing sstables, when running with this format
	// major version.
	_ // FormatSSTableValueBlocks

	// FormatFlushableIngest is a format major version that enables lazy
	// addition of ingested sstables into the LSM structure. When an ingest
	// overlaps with a memtable, a record of the ingest is written to the WAL
	// without waiting for a flush. Subsequent reads treat the ingested files as
	// a level above the overlapping memtable. Once the memtable is flushed, the
	// ingested files are moved into the lowest possible levels.
	//
	// This feature is behind a format major version because it required
	// breaking changes to the WAL format.
	FormatFlushableIngest

	// 23.2 versions.

	// FormatPrePebblev1MarkedCompacted is a format major version that guarantees
	// that all sstables explicitly marked for compaction in the manifest (see
	// FormatPrePebblev1Marked) have been compacted. Ratcheting to this format
	// version will block (without holding mutexes) until all necessary
	// compactions for files marked for compaction are complete.
	FormatPrePebblev1MarkedCompacted

	// FormatDeleteSizedAndObsolete is a format major version that adds support
	// for deletion tombstones that encode the size of the value they're
	// expected to delete. This format major version is required before the
	// associated key kind may be committed through batch applications or
	// ingests. It also adds support for keys that are marked obsolete (see
	// sstable/format.go for details).
	FormatDeleteSizedAndObsolete

	// FormatVirtualSSTables is a format major version that adds support for
	// virtual sstables that can reference a sub-range of keys in an underlying
	// physical sstable. This information is persisted through new,
	// backward-incompatible fields in the Manifest, and therefore requires
	// a format major version.
	FormatVirtualSSTables

	// FormatSyntheticPrefixSuffix is a format major version that adds support for
	// sstables to have their content exposed in a different prefix or suffix of
	// keyspace than the actual prefix/suffix persisted in the keys in such
	// sstables. The prefix and suffix replacement information is stored in new
	// fields in the Manifest and thus requires a format major version.
	FormatSyntheticPrefixSuffix

	// FormatFlushableIngestExcises is a format major version that adds support for
	// having excises unconditionally being written as flushable ingestions. This
	// is implemented through adding a new key kind that can go in the same batches
	// as flushable ingested sstables.
	FormatFlushableIngestExcises

	// FormatColumnarBlocks is a format major version enabling use of the
	// TableFormatPebblev5 table format, that encodes sstable data blocks, index
	// blocks and keyspan blocks by organizing the KVs into columns within the
	// block.
	FormatColumnarBlocks

	// FormatWALSyncChunks is a format major version enabling the writing of
	// WAL sync chunks. These new chunks are used to disambiguate between corruption
	// and logical EOF during WAL replay. This is implemented by adding a new
	// chunk wire format that encodes an additional "Synced Offset" field which acts
	// as a commitment that the WAL should have been synced up until the offset.
	FormatWALSyncChunks

	// FormatTableFormatV6 is a format major version enabling the sstable table
	// format TableFormatPebblev6.
	//
	// The TableFormatPebblev6 sstable format introduces a checksum within the
	// sstable footer, allows inclusion of blob handle references within the
	// value column of a sstable block, and supports columnar meta index +
	// properties blocks.
	//
	// This format major version does not yet enable use of value separation.
	FormatTableFormatV6

	// formatDeprecatedExperimentalValueSeparation was used to enable an
	// experimental version of value separation, separating values into external
	// blob files that do not participate in every compaction.
	//
	// Value separation now depends on TableFormatPebblev7 which this format
	// major version precedes. This format major version is deprecated and
	// unexported, and value separation now requires FormatValueSeparation.
	formatDeprecatedExperimentalValueSeparation

	// formatFooterAttributes is a format major version that adds support for
	// writing sstable.Attributes in the footer of sstables.
	formatFooterAttributes

	// FormatValueSeparation is a format major version that adds support for
	// value separation, separating values into external blob files that do not
	// participate in every compaction.
	FormatValueSeparation

	// FormatExciseBoundsRecord is a format major version that adds support for
	// persisting excise bounds records in the manifest (VersionEdit).
	FormatExciseBoundsRecord

	// FormatV2BlobFiles is a format major version that adds support for V2 blob
	// file format (which adds compression statistics).
	FormatV2BlobFiles

	// -- Add new versions here --

	// FormatNewest is the most recent format major version.
	FormatNewest FormatMajorVersion = iota - 1

	// Experimental versions, which are excluded by FormatNewest (but can be used
	// in tests) can be defined here.

	// -- Add experimental versions here --

	// internalFormatNewest is the most recent, possibly experimental format major
	// version.
	internalFormatNewest FormatMajorVersion = iota - 2
)

// FormatMinSupported is the minimum format version that is supported by this
// Pebble version.
const FormatMinSupported = FormatFlushableIngest

// FormatMinForSharedObjects it the minimum format version that supports shared
// objects (see CreateOnShared option).
const FormatMinForSharedObjects = FormatVirtualSSTables

// resolveDefault asserts that the given version is supported, and returns the
// given version, replacing FormatDefault with FormatMinSupported.
func (v FormatMajorVersion) resolveDefault() FormatMajorVersion {
	if v == FormatDefault {
		return FormatMinSupported
	}
	if v < FormatMinSupported || v > internalFormatNewest {
		panic(fmt.Sprintf("pebble: unsupported format major version: %s", v))
	}
	return v
}

// MaxTableFormat returns the maximum sstable.TableFormat that can be used at
// this FormatMajorVersion.
func (v FormatMajorVersion) MaxTableFormat() sstable.TableFormat {
	v = v.resolveDefault()
	switch {
	case v >= formatFooterAttributes:
		return sstable.TableFormatPebblev7
	case v >= FormatTableFormatV6:
		return sstable.TableFormatPebblev6
	case v >= FormatColumnarBlocks:
		return sstable.TableFormatPebblev5
	case v >= FormatDeleteSizedAndObsolete:
		return sstable.TableFormatPebblev4
	default:
		return sstable.TableFormatPebblev3
	}
}

// MinTableFormat returns the minimum sstable.TableFormat that can be used at
// this FormatMajorVersion.
func (v FormatMajorVersion) MinTableFormat() sstable.TableFormat {
	_ = v.resolveDefault()
	return sstable.TableFormatPebblev1
}

// MaxBlobFileFormat returns the maximum blob.FileFormat that can be used at
// this FormatMajorVersion. It can only be used on versions that support value
// separation.
func (v FormatMajorVersion) MaxBlobFileFormat() blob.FileFormat {
	v = v.resolveDefault()
	switch {
	case v >= FormatV2BlobFiles:
		return blob.FileFormatV2
	case v >= FormatValueSeparation:
		return blob.FileFormatV1
	default:
		panic(fmt.Sprintf("pebble: format major version %s does not support blob files", v))
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
	FormatFlushableIngest: func(d *DB) error { return nil },
	FormatPrePebblev1MarkedCompacted: func(d *DB) error {
		// Before finalizing the format major version, rewrite any sstables
		// still marked for compaction. Note all format major versions
		// migrations are invoked with DB.mu locked.
		if err := d.compactMarkedFilesLocked(); err != nil {
			return err
		}
		return d.finalizeFormatVersUpgrade(FormatPrePebblev1MarkedCompacted)
	},
	FormatDeleteSizedAndObsolete: func(d *DB) error {
		return d.finalizeFormatVersUpgrade(FormatDeleteSizedAndObsolete)
	},
	FormatVirtualSSTables: func(d *DB) error {
		return d.finalizeFormatVersUpgrade(FormatVirtualSSTables)
	},
	FormatSyntheticPrefixSuffix: func(d *DB) error {
		return d.finalizeFormatVersUpgrade(FormatSyntheticPrefixSuffix)
	},
	FormatFlushableIngestExcises: func(d *DB) error {
		return d.finalizeFormatVersUpgrade(FormatFlushableIngestExcises)
	},
	FormatColumnarBlocks: func(d *DB) error {
		return d.finalizeFormatVersUpgrade(FormatColumnarBlocks)
	},
	FormatWALSyncChunks: func(d *DB) error {
		return d.finalizeFormatVersUpgrade(FormatWALSyncChunks)
	},
	FormatTableFormatV6: func(d *DB) error {
		return d.finalizeFormatVersUpgrade(FormatTableFormatV6)
	},
	formatDeprecatedExperimentalValueSeparation: func(d *DB) error {
		return d.finalizeFormatVersUpgrade(formatDeprecatedExperimentalValueSeparation)
	},
	formatFooterAttributes: func(d *DB) error {
		return d.finalizeFormatVersUpgrade(formatFooterAttributes)
	},
	FormatValueSeparation: func(d *DB) error {
		return d.finalizeFormatVersUpgrade(FormatValueSeparation)
	},
	FormatExciseBoundsRecord: func(d *DB) error {
		return d.finalizeFormatVersUpgrade(FormatExciseBoundsRecord)
	},
	FormatV2BlobFiles: func(d *DB) error {
		return d.finalizeFormatVersUpgrade(FormatV2BlobFiles)
	},
}

const formatVersionMarkerName = `format-version`

// lookupFormatMajorVersion retrieves the format version from the format version
// marker file.
//
// If such a file does not exist, returns FormatDefault. Note that this case is
// only acceptable if we are creating a new store (we no longer support
// FormatMostCompatible which is the only one with no version marker file).
func lookupFormatMajorVersion(
	fs vfs.FS, dirname string, ls []string,
) (FormatMajorVersion, *atomicfs.Marker, error) {
	m, versString, err := atomicfs.LocateMarkerInListing(fs, dirname, formatVersionMarkerName, ls)
	if err != nil {
		return 0, nil, err
	}
	if versString == "" {
		return FormatDefault, m, nil
	}
	v, err := strconv.ParseUint(versString, 10, 64)
	if err != nil {
		return 0, nil, errors.Wrap(err, "parsing format major version")
	}
	vers := FormatMajorVersion(v)
	if vers == FormatDefault {
		return 0, nil, errors.Newf("pebble: default format major version should not persisted", vers)
	}
	if vers > internalFormatNewest {
		return 0, nil, errors.Newf("pebble: database %q written in unknown format major version %d", dirname, vers)
	}
	if vers < FormatMinSupported {
		return 0, nil, errors.Newf("pebble: database %q written in format major version %d which is no longer supported", dirname, vers)
	}
	return vers, m, nil
}

// FormatMajorVersion returns the database's active format major
// version. The format major version may be higher than the one
// provided in Options when the database was opened if the existing
// database was written with a higher format version.
func (d *DB) FormatMajorVersion() FormatMajorVersion {
	return FormatMajorVersion(d.mu.formatVers.vers.Load())
}

// TableFormat returns the TableFormat that the database is currently using when
// writing sstables. The table format is determined by the database's format
// major version, as well as experimental settings like EnableValueBlocks and
// EnableColumnarBlocks.
func (d *DB) TableFormat() sstable.TableFormat {
	// The table is typically written at the maximum allowable format implied by
	// the current format major version of the DB.
	f := d.FormatMajorVersion().MaxTableFormat()
	if f == sstable.TableFormatPebblev3 {
		// In format major versions with maximum table formats of Pebblev3,
		// value blocks were conditional on an experimental setting. In format
		// major versions with maximum table formats of Pebblev4 and higher,
		// value blocks are always enabled.
		if d.opts.Experimental.EnableValueBlocks == nil || !d.opts.Experimental.EnableValueBlocks() {
			f = sstable.TableFormatPebblev2
		}
	}
	return f
}

// BlobFileFormat returns the blob.FileFormat that the database is currently
// using when writing blob files.
func (d *DB) BlobFileFormat() blob.FileFormat {
	return d.FormatMajorVersion().MaxBlobFileFormat()
}

// shouldCreateShared returns true if the database should use shared objects
// when creating new objects on the given level.
func (d *DB) shouldCreateShared(targetLevel int) bool {
	return remote.ShouldCreateShared(d.opts.Experimental.CreateOnShared, targetLevel) &&
		d.FormatMajorVersion() >= FormatMinForSharedObjects
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
	if formatVers > internalFormatNewest {
		// Guard against accidentally forgetting to update internalFormatNewest.
		return errors.Errorf("pebble: unknown format version %d", formatVers)
	}
	if currentVers := d.FormatMajorVersion(); currentVers > formatVers {
		return errors.Newf("pebble: database already at format major version %d; cannot reduce to %d",
			currentVers, formatVers)
	}
	if d.mu.formatVers.ratcheting {
		return errors.Newf("pebble: database format major version upgrade is in-progress")
	}
	d.mu.formatVers.ratcheting = true
	defer func() { d.mu.formatVers.ratcheting = false }()

	for nextVers := d.FormatMajorVersion() + 1; nextVers <= formatVers; nextVers++ {
		if err := formatMajorVersionMigrations[nextVers](d); err != nil {
			return errors.Wrapf(err, "migrating to version %d", nextVers)
		}

		// NB: The migration is responsible for calling
		// finalizeFormatVersUpgrade to finalize the upgrade. This
		// structure is necessary because some migrations may need to
		// update in-memory state (without ever dropping locks) after
		// the upgrade is finalized. Here we assert that the upgrade
		// did occur.
		if d.FormatMajorVersion() != nextVers {
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
	if err := d.writeFormatVersionMarker(formatVers); err != nil {
		return err
	}
	d.mu.formatVers.vers.Store(uint64(formatVers))
	d.opts.EventListener.FormatUpgrade(formatVers)
	return nil
}

func (d *DB) writeFormatVersionMarker(formatVers FormatMajorVersion) error {
	// We use the marker to encode the active format version in the
	// marker filename. Unlike other uses of the atomic marker, there is
	// no file with the filename `formatVers.String()` on the
	// filesystem.
	return d.mu.formatVers.marker.Move(formatVers.String())
}

// compactMarkedFilesLocked performs a migration that schedules rewrite
// compactions to compact away any sstables marked for compaction.
// compactMarkedFilesLocked is run while ratcheting the database's format major
// version to FormatSplitUserKeysMarkedCompacted.
//
// Note that while this method is called with the DB.mu held, and will not
// return until all marked files have been compacted, the mutex is dropped while
// waiting for compactions to complete (or for slots to free up).
func (d *DB) compactMarkedFilesLocked() error {
	curr := d.mu.versions.currentVersion()
	if curr.Stats.MarkedForCompaction == 0 {
		return nil
	}
	// Attempt to schedule a compaction to rewrite a file marked for compaction.
	// We simply call maybeScheduleCompaction since it also picks rewrite
	// compactions. Note that we don't need to call this repeatedly in the for
	// loop below since the completion of a compaction either starts a new one
	// or ensures a compaction is queued for scheduling. By calling
	// maybeScheduleCompaction here we are simply kicking off this behavior.
	d.maybeScheduleCompaction()

	// The above attempt might succeed and schedule a rewrite compaction. Or
	// there might not be available compaction concurrency to schedule the
	// compaction.  Or compaction of the file might have already been in
	// progress. In any scenario, wait until there's some change in the
	// state of active compactions.
	for curr.Stats.MarkedForCompaction > 0 {
		// Before waiting, check that the database hasn't been closed. Trying to
		// schedule the compaction may have dropped d.mu while waiting for a
		// manifest write to complete. In that dropped interim, the database may
		// have been closed.
		if err := d.closed.Load(); err != nil {
			return err.(error)
		}

		// Some flush or compaction may have scheduled or completed while we waited
		// for the manifest lock in maybeScheduleCompactionPicker. Get the latest
		// Version before waiting on a compaction.
		curr = d.mu.versions.currentVersion()

		// Only wait on compactions if there are files still marked for compaction.
		// NB: Waiting on this condition variable drops d.mu while blocked.
		if curr.Stats.MarkedForCompaction > 0 {
			// NB: we cannot assert that d.mu.compact.compactingCount > 0, since
			// with a CompactionScheduler a DB may not have even one ongoing
			// compaction (if other competing activities are being preferred by the
			// scheduler).
			d.mu.compact.cond.Wait()
			// Refresh the current version again.
			curr = d.mu.versions.currentVersion()
		}
	}
	return nil
}

// findFilesFunc scans the LSM for files, returning true if at least one
// file was found. The returned array contains the matched files, if any, per
// level.
type findFilesFunc func(v *manifest.Version) (found bool, files [numLevels][]*manifest.TableMetadata, _ error)

// This method is not used currently, but it will be useful the next time we need
// to mark files for compaction.
var _ = (*DB)(nil).markFilesLocked

// markFilesLocked durably marks the files that match the given findFilesFunc for
// compaction.
func (d *DB) markFilesLocked(findFn findFilesFunc) error {
	jobID := d.newJobIDLocked()

	// Acquire a read state to have a view of the LSM and a guarantee that none
	// of the referenced files will be deleted until we've unreferenced the read
	// state. Some findFilesFuncs may read the files, requiring they not be
	// deleted.
	rs := d.loadReadState()
	var (
		found bool
		files [numLevels][]*manifest.TableMetadata
		err   error
	)
	func() {
		defer rs.unrefLocked()
		// Note the unusual locking: unlock, defer Lock(). The scan of the files in
		// the version does not need to block other operations that require the
		// DB.mu. Drop it for the scan, before re-acquiring it.
		d.mu.Unlock()
		defer d.mu.Lock()
		found, files, err = findFn(rs.current)
	}()
	if err != nil {
		return err
	}

	// The database lock has been acquired again by the defer within the above
	// anonymous function.
	if !found {
		// Nothing to do.
		return nil
	}

	// After scanning, if we found files to mark, we fetch the current state of
	// the LSM (which may have changed) and set MarkedForCompaction on the files,
	// and update the version's Stats.MarkedForCompaction count, which are both
	// protected by d.mu.

	// Lock the manifest for a coherent view of the LSM. The database lock has
	// been re-acquired by the defer within the above anonymous function.
	_, err = d.mu.versions.UpdateVersionLocked(func() (versionUpdate, error) {
		vers := d.mu.versions.currentVersion()
		for l, filesToMark := range files {
			if len(filesToMark) == 0 {
				continue
			}
			for _, f := range filesToMark {
				// Ignore files to be marked that have already been compacted or marked.
				if f.CompactionState == manifest.CompactionStateCompacted ||
					f.MarkedForCompaction {
					continue
				}
				// Else, mark the file for compaction in this version.
				vers.Stats.MarkedForCompaction++
				f.MarkedForCompaction = true
			}
			// The compaction picker uses the markedForCompactionAnnotator to
			// quickly find files marked for compaction, or to quickly determine
			// that there are no such files marked for compaction within a level.
			// A b-tree node may be annotated with an annotation recording that
			// there are no files marked for compaction within the node's subtree,
			// based on the assumption that it's static.
			//
			// Since we're marking files for compaction, these b-tree nodes'
			// annotations will be out of date. Clear the compaction-picking
			// annotation, so that it's recomputed the next time the compaction
			// picker looks for a file marked for compaction.
			markedForCompactionAnnotator.InvalidateLevelAnnotation(vers.Levels[l])
		}
		// The 'marked-for-compaction' bit is persisted in the MANIFEST file
		// metadata. We've already modified the in-memory table metadata, but the
		// manifest hasn't been updated. Force rotation to a new MANIFEST file,
		// which will write every table metadata to the new manifest file and ensure
		// that the now marked-for-compaction table metadata are persisted as marked.
		return versionUpdate{
			VE:                      &manifest.VersionEdit{},
			JobID:                   jobID,
			ForceManifestRotation:   true,
			InProgressCompactionsFn: func() []compactionInfo { return d.getInProgressCompactionInfoLocked(nil) },
		}, nil
	})
	return err
}
