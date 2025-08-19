// Copyright 2018 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package sstable

import (
	"encoding/binary"
	"maps"
	"math"
	"slices"
	"unsafe"

	"github.com/cockroachdb/pebble/sstable/colblk"
	"github.com/cockroachdb/pebble/sstable/rowblk"
)

//go:generate go run ./internal/genprops

const propertiesBlockRestartInterval = math.MaxInt32

// Properties holds the sstable property values. The properties are
// automatically populated during sstable creation and load from the properties
// meta block when an sstable is opened.
type Properties struct {
	// The number of entries in this table.
	NumEntries uint64 `prop:"rocksdb.num.entries" options:"encodeempty"`
	// Total raw key size.
	RawKeySize uint64 `prop:"rocksdb.raw.key.size" options:"encodeempty"`
	// Total raw value size. If values are separated, this includes the size of
	// the separated value, NOT the value handle.
	RawValueSize uint64 `prop:"rocksdb.raw.value.size" options:"encodeempty"`
	// Total raw key size of point deletion tombstones. This value is comparable
	// to RawKeySize.
	RawPointTombstoneKeySize uint64 `prop:"pebble.raw.point-tombstone.key.size"`
	// Sum of the raw value sizes carried by point deletion tombstones
	// containing size estimates. See the DeleteSized key kind. This value is
	// comparable to Raw{Key,Value}Size.
	RawPointTombstoneValueSize uint64 `prop:"pebble.raw.point-tombstone.value.size"`
	// The number of point deletion entries ("tombstones") in this table that
	// carry a size hint indicating the size of the value the tombstone deletes.
	NumSizedDeletions uint64 `prop:"pebble.num.deletions.sized"`
	// The number of deletion entries in this table, including both point and
	// range deletions.
	NumDeletions uint64 `prop:"rocksdb.deleted.keys" options:"encodeempty"`
	// The number of range deletions in this table.
	NumRangeDeletions uint64 `prop:"rocksdb.num.range-deletions" options:"encodeempty"`
	// The number of RANGEKEYDELs in this table.
	NumRangeKeyDels uint64 `prop:"pebble.num.range-key-dels"`
	// The number of RANGEKEYSETs in this table.
	NumRangeKeySets uint64 `prop:"pebble.num.range-key-sets"`
	// Total size of value blocks and value index block. Only serialized if > 0.
	ValueBlocksSize uint64 `prop:"pebble.value-blocks.size"`
	// NumDataBlocks is the number of data blocks in this table.
	NumDataBlocks uint64 `prop:"rocksdb.num.data.blocks" options:"encodeempty"`
	// NumTombstoneDenseBlocks is the number of data blocks in this table that are
	// considered tombstone-dense.
	//
	// A block is considered tombstone dense if at least one of the following:
	//  1. The block contains at least options.Experimental.NumDeletionsThreshold
	//     point tombstones.
	//  2. The ratio of the uncompressed size of point tombstones to the
	//     uncompressed size of the block is at least
	//     options.Experimental.DeletionSizeRatioThreshold.
	//
	// This statistic is used to determine eligibility for a tombstone density
	// compaction.
	NumTombstoneDenseBlocks uint64 `prop:"pebble.num.tombstone-dense-blocks"`

	// The name of the comparer used in this table.
	ComparerName string `prop:"rocksdb.comparator" options:"encodeempty,intern"`
	// The total size of all data blocks.
	DataSize uint64 `prop:"rocksdb.data.size" options:"encodeempty"`
	// The name of the filter policy used in this table. Empty if no filter
	// policy is used.
	FilterPolicyName string `prop:"rocksdb.filter.policy" options:"intern"`
	// The size of filter block.
	FilterSize uint64 `prop:"rocksdb.filter.size" options:"encodeempty"`
	// Total number of index partitions if kTwoLevelIndexSearch is used.
	IndexPartitions uint64 `prop:"rocksdb.index.partitions"`
	// The size (uncompressed) of index block.
	IndexSize uint64 `prop:"rocksdb.index.size"`
	// The index type. TODO(peter): add a more detailed description.
	IndexType uint32 `prop:"rocksdb.block.based.table.index.type" options:"encodeempty"`
	// For formats >= TableFormatPebblev4, this is set to true if the obsolete
	// bit is strict for all the point keys.
	IsStrictObsolete bool `prop:"pebble.obsolete.is_strict"`
	// The name of the key schema used in this table. Empty for formats <=
	// TableFormatPebblev4.
	KeySchemaName string `prop:"pebble.colblk.schema" options:"intern"`
	// The name of the merger used in this table. Empty if no merger is used.
	MergerName string `prop:"rocksdb.merge.operator" options:"encodeempty,intern"`
	// The number of merge operands in the table.
	NumMergeOperands uint64 `prop:"rocksdb.merge.operands" options:"encodeempty"`
	// The number of RANGEKEYUNSETs in this table.
	NumRangeKeyUnsets uint64 `prop:"pebble.num.range-key-unsets"`
	// The number of value blocks in this table. Only serialized if > 0.
	NumValueBlocks uint64 `prop:"pebble.num.value-blocks"`
	// The number of values stored in value blocks. Only serialized if > 0.
	NumValuesInValueBlocks uint64 `prop:"pebble.num.values.in.value-blocks"`
	// The number of values stored in blob files. Only serialized if > 0.
	NumValuesInBlobFiles uint64 `prop:"pebble.num.values.in.blob-files"`
	// A comma separated list of names of the property collectors used in this
	// table.
	PropertyCollectorNames string `prop:"rocksdb.property.collectors" options:"encodeempty,intern"`
	// Total raw rangekey key size.
	RawRangeKeyKeySize uint64 `prop:"pebble.raw.range-key.key.size"`
	// Total raw rangekey value size.
	RawRangeKeyValueSize uint64 `prop:"pebble.raw.range-key.value.size"`
	// The total number of keys in this table that were pinned by open snapshots.
	SnapshotPinnedKeys uint64 `prop:"pebble.num.snapshot-pinned-keys"`
	// The cumulative bytes of keys in this table that were pinned by
	// open snapshots. This value is comparable to RawKeySize.
	SnapshotPinnedKeySize uint64 `prop:"pebble.raw.snapshot-pinned-keys.size"`
	// The cumulative bytes of values in this table that were pinned by
	// open snapshots. This value is comparable to RawValueSize.
	SnapshotPinnedValueSize uint64 `prop:"pebble.raw.snapshot-pinned-values.size"`
	// Size (uncompressed) of the top-level index if kTwoLevelIndexSearch is used.
	TopLevelIndexSize uint64 `prop:"rocksdb.top-level.index.size"`
	// The compression algorithm used to compress blocks.
	CompressionName string `prop:"rocksdb.compression"`
	// The compression statistics encoded as a string. The format is:
	// "<setting1>:<compressed1>/<uncompressed1>,<setting2>:<compressed2>/<uncompressed2>,..."
	CompressionStats string `prop:"pebble.compression_stats"`
	// User collected properties. Currently, we only use them to store block
	// properties aggregated at the table level.
	UserProperties map[string]string

	// Loaded is a bitfield indicating which fields have been loaded from disk. It
	// is used only for string output, to avoid printing zero fields unless they
	// were present. It is only exported for testing purposes.
	Loaded uint64
}

// NumPointDeletions returns the number of point deletions in this table.
func (p *Properties) NumPointDeletions() uint64 {
	return p.NumDeletions - p.NumRangeDeletions
}

// NumRangeKeys returns a count of the number of range keys in this table.
func (p *Properties) NumRangeKeys() uint64 {
	return p.NumRangeKeyDels + p.NumRangeKeySets + p.NumRangeKeyUnsets
}

func (p *Properties) accumulateProps(tblFormat TableFormat) map[string][]byte {
	m := p.encodeAll()
	for k, v := range p.UserProperties {
		m[k] = []byte(v)
	}

	if tblFormat < TableFormatPebblev1 {
		// NB: Omit some properties for Pebble formats. This isn't strictly
		// necessary because unrecognized properties are interpreted as user-defined
		// properties, however writing them prevents byte-for-byte equivalence with
		// RocksDB files that some of our testing requires.
		delete(m, "pebble.raw.point-tombstone.key.size")

		m["rocksdb.column.family.id"] = maxInt32Slice
		m["rocksdb.fixed.key.length"] = singleZeroSlice
		m["rocksdb.index.key.is.user.key"] = singleZeroSlice
		m["rocksdb.index.value.is.delta.encoded"] = singleZeroSlice
		m["rocksdb.oldest.key.time"] = singleZeroSlice
		m["rocksdb.creation.time"] = singleZeroSlice
		m["rocksdb.format.version"] = singleZeroSlice
		m["rocksdb.compression_options"] = rocksDBCompressionOptions
	}
	return m
}

func (p *Properties) saveToRowWriter(tblFormat TableFormat, w *rowblk.Writer) error {
	m := p.accumulateProps(tblFormat)
	for _, key := range slices.Sorted(maps.Keys(m)) {
		if err := w.AddRawString(key, m[key]); err != nil {
			return err
		}
	}
	return nil
}

func (p *Properties) saveToColWriter(tblFormat TableFormat, w *colblk.KeyValueBlockWriter) {
	m := p.accumulateProps(tblFormat)
	for _, key := range slices.Sorted(maps.Keys(m)) {
		// Zero-length keys are unsupported. See below about StringData.
		if len(key) == 0 {
			continue
		}
		// Use an unsafe conversion to avoid allocating. AddKV is not
		// supposed to modify the given slice, so the unsafe conversion
		// is okay. Note that unsafe.StringData panics if len(key) == 0,
		// so we explicitly skip zero-length keys above. They shouldn't
		// occur in practice.
		w.AddKV(unsafe.Slice(unsafe.StringData(key), len(key)), m[key])
	}
}

func (p *Properties) toAttributes() Attributes {
	var attributes Attributes

	if p.NumValueBlocks > 0 || p.NumValuesInValueBlocks > 0 {
		attributes.Add(AttributeValueBlocks)
	}
	if p.NumRangeKeySets > 0 {
		attributes.Add(AttributeRangeKeySets)
	}
	if p.NumRangeKeyUnsets > 0 {
		attributes.Add(AttributeRangeKeyUnsets)
	}
	if p.NumRangeKeyDels > 0 {
		attributes.Add(AttributeRangeKeyDels)
	}
	if p.NumRangeDeletions > 0 {
		attributes.Add(AttributeRangeDels)
	}
	if p.IndexType == twoLevelIndex {
		attributes.Add(AttributeTwoLevelIndex)
	}
	if p.NumValuesInBlobFiles > 0 {
		attributes.Add(AttributeBlobValues)
	}
	if p.NumDataBlocks > 0 {
		attributes.Add(AttributePointKeys)
	}

	return attributes
}

var (
	singleZeroSlice = []byte{0x00}
	maxInt32Slice   = binary.AppendUvarint([]byte(nil), math.MaxInt32)

	// RocksDB always includes this in the properties block, so we include it for
	// formats below TableFormatPebble.
	rocksDBCompressionOptions = []byte("window_bits=-14; level=32767; strategy=0; max_dict_bytes=0; zstd_max_train_bytes=0; enabled=0; ")
)
