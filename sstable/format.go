// Copyright 2022 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package sstable

import (
	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/pebble/internal/base"
)

// TableFormat specifies the format version for sstables. The legacy LevelDB
// format is format version 1.
type TableFormat uint32

// The available table formats, representing the tuple (magic number, version
// number). Note that these values are not (and should not) be serialized to
// disk. The ordering should follow the order the versions were introduced to
// Pebble (i.e. the history is linear).
const (
	TableFormatUnspecified TableFormat = iota
	TableFormatLevelDB
	TableFormatRocksDBv2
	TableFormatPebblev1 // Block properties.
	TableFormatPebblev2 // Range keys.
	TableFormatPebblev3 // Value blocks.
	TableFormatPebblev4 // DELSIZED tombstones.

	TableFormatMax = TableFormatPebblev4
)

// TableFormatPebblev4, in addition to DELSIZED, introduces the use of
// InternalKeyKindSSTableInternalObsoleteBit. See
// https://github.com/cockroachdb/pebble/issues/2465.
//
// TODO(sumeer): write a long comment using the text in that issue, including
// the definition of obsolete and the additional key-kind transformations that
// need to go with it. And that obsolete bit is not permitted for Merge kinds.
//
// Setting the obsolete bit on point keys is advanced usage, so we support 2
// modes, both of which must be truthful when setting the obsolete bit, but
// vary in when they don't set the obsolete bit.
//
// - Non-strict: In this mode, the bit does not need to be set for keys that
//   are obsolete. Additionally, any sstable containing MERGE keys can only
//   use this mode. An iterator over such an sstable, when configured to
//   hideObsoletePoints, can expose multiple internal keys per user key, and
//   can expose keys that are deleted by rangedels in the same sstable. This
//   is the mode that non-advanced users should use. Pebble without
//   disaggregated storage will also use this mode and will best-effort set
//   the obsolete bit, to optimize iteration when snapshots have retained many
//   obsolete keys.
//
// - Strict: In this mode, every obsolete key must have the obsolete bit set,
//   and no MERGE keys are permitted. An iterator over such an sstable, when
//   configured to hideObsoletePoints satisfies two properties:
//   - S1: will expose at most one internal key per user key, which is the
//     most recent one.
//   - S2: will never expose keys that are deleted by rangedels in the same
//     sstable.
//
//   This is the mode for two use cases in disaggregated storage (which will
//   exclude parts of the key space that has MERGEs), for levels that contain
//   sstables that can become foreign sstables.
//
//   - Pebble compaction output to these levels that can become foreign
//     sstables.
//
//   - CockroachDB ingest operations that can ingest into the levels that can
//     become foreign sstables. Note, these are not sstables corresponding to
//     copied data for CockroachDB range snapshots. This case occurs for
//     operations like index backfills: these trivially satisfy the strictness
//     criteria since they only write one key per userkey.

// ParseTableFormat parses the given magic bytes and version into its
// corresponding internal TableFormat.
func ParseTableFormat(magic []byte, version uint32) (TableFormat, error) {
	switch string(magic) {
	case levelDBMagic:
		return TableFormatLevelDB, nil
	case rocksDBMagic:
		if version != rocksDBFormatVersion2 {
			return TableFormatUnspecified, base.CorruptionErrorf(
				"pebble/table: unsupported rocksdb format version %d", errors.Safe(version),
			)
		}
		return TableFormatRocksDBv2, nil
	case pebbleDBMagic:
		switch version {
		case 1:
			return TableFormatPebblev1, nil
		case 2:
			return TableFormatPebblev2, nil
		case 3:
			return TableFormatPebblev3, nil
		case 4:
			return TableFormatPebblev4, nil
		default:
			return TableFormatUnspecified, base.CorruptionErrorf(
				"pebble/table: unsupported pebble format version %d", errors.Safe(version),
			)
		}
	default:
		return TableFormatUnspecified, base.CorruptionErrorf(
			"pebble/table: invalid table (bad magic number: 0x%x)", magic,
		)
	}
}

// AsTuple returns the TableFormat's (Magic String, Version) tuple.
func (f TableFormat) AsTuple() (string, uint32) {
	switch f {
	case TableFormatLevelDB:
		return levelDBMagic, 0
	case TableFormatRocksDBv2:
		return rocksDBMagic, 2
	case TableFormatPebblev1:
		return pebbleDBMagic, 1
	case TableFormatPebblev2:
		return pebbleDBMagic, 2
	case TableFormatPebblev3:
		return pebbleDBMagic, 3
	case TableFormatPebblev4:
		return pebbleDBMagic, 4
	default:
		panic("sstable: unknown table format version tuple")
	}
}

// String returns the TableFormat (Magic String,Version) tuple.
func (f TableFormat) String() string {
	switch f {
	case TableFormatLevelDB:
		return "(LevelDB)"
	case TableFormatRocksDBv2:
		return "(RocksDB,v2)"
	case TableFormatPebblev1:
		return "(Pebble,v1)"
	case TableFormatPebblev2:
		return "(Pebble,v2)"
	case TableFormatPebblev3:
		return "(Pebble,v3)"
	case TableFormatPebblev4:
		return "(Pebble,v4)"
	default:
		panic("sstable: unknown table format version tuple")
	}
}
