// Copyright 2019 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package sstable

import (
	"fmt"

	"github.com/cockroachdb/pebble/internal/base"
	"github.com/cockroachdb/pebble/internal/sstableinternal"
	"github.com/cockroachdb/pebble/sstable/block"
	"github.com/cockroachdb/pebble/sstable/colblk"
	"github.com/cockroachdb/pebble/sstable/rowblk"
)

const (
	// MaximumRestartOffset is the maximum permissible value for a restart
	// offset within a block. That is, the maximum block size that allows adding
	// an additional restart point.
	MaximumRestartOffset = rowblk.MaximumRestartOffset
	// DefaultNumDeletionsThreshold defines the minimum number of point
	// tombstones that must be present in a data block for it to be
	// considered tombstone-dense.
	DefaultNumDeletionsThreshold = 100
	// DefaultDeletionSizeRatioThreshold defines the minimum ratio of the size
	// of point tombstones to the size of the data block in order to consider the
	// block as tombstone-dense.
	DefaultDeletionSizeRatioThreshold = 0.5
)

var ignoredInternalProperties = map[string]struct{}{
	"rocksdb.column.family.id":             {},
	"rocksdb.fixed.key.length":             {},
	"rocksdb.index.key.is.user.key":        {},
	"rocksdb.index.value.is.delta.encoded": {},
	"rocksdb.oldest.key.time":              {},
	"rocksdb.creation.time":                {},
	"rocksdb.file.creation.time":           {},
	"rocksdb.format.version":               {},
	"rocksdb.compression_options":          {},
}

// Comparers is a map from comparer name to comparer. It is used for debugging
// tools which may be used on multiple databases configured with different
// comparers.
type Comparers map[string]*base.Comparer

// Mergers is a map from merger name to merger. It is used for debugging tools
// which may be used on multiple databases configured with different
// mergers.
type Mergers map[string]*base.Merger

// KeySchemas is a map from key schema name to key schema. A single database may
// contain sstables with multiple key schemas.
type KeySchemas map[string]*colblk.KeySchema

// MakeKeySchemas constructs a KeySchemas from a slice of key schemas.
func MakeKeySchemas(keySchemas ...*colblk.KeySchema) KeySchemas {
	m := make(KeySchemas, len(keySchemas))
	for _, keySchema := range keySchemas {
		if _, ok := m[keySchema.Name]; ok {
			panic(fmt.Sprintf("duplicate key schemas with name %q", keySchema.Name))
		}
		m[keySchema.Name] = keySchema
	}
	return m
}

// ReaderOptions holds the parameters needed for reading an sstable.
type ReaderOptions struct {
	block.ReaderOptions

	// Comparer defines a total ordering over the space of []byte keys: a 'less
	// than' relationship. The same comparison algorithm must be used for reads
	// and writes over the lifetime of the DB.
	//
	// The default value uses the same ordering as bytes.Compare.
	Comparer *Comparer

	// Merger defines the Merge function in use for this keyspace.
	Merger *Merger

	Comparers Comparers
	Mergers   Mergers
	// KeySchemas contains the set of known key schemas to use when interpreting
	// columnar data blocks. Only used for sstables encoded in format
	// TableFormatPebblev5 or higher.
	KeySchemas KeySchemas

	// FilterDecoders contains decoders for supported table filter families. If
	// the filter family does not have a decoder in this list, the filter block
	// will not be used.
	FilterDecoders []base.TableFilterDecoder

	// FilterMetricsTracker is optionally used to track filter metrics.
	FilterMetricsTracker *FilterMetricsTracker

	// InitFileReadStats is to only be used for reads in NewReader and forgotten
	// after.
	InitFileReadStats block.InitFileReadStats
}

func (o ReaderOptions) ensureDefaults() ReaderOptions {
	if o.Comparer == nil {
		o.Comparer = base.DefaultComparer
	}
	if o.Merger == nil {
		o.Merger = base.DefaultMerger
	}
	if o.LoggerAndTracer == nil {
		o.LoggerAndTracer = base.NoopLoggerAndTracer{}
	}
	if o.KeySchemas == nil {
		o.KeySchemas = defaultKeySchemas
	}
	return o
}

var defaultKeySchema = colblk.DefaultKeySchema(base.DefaultComparer, 16)
var defaultKeySchemas = MakeKeySchemas(&defaultKeySchema)

type CompressionProfile = block.CompressionProfile

// Exported CompressionProfile constants.
var (
	DefaultCompression = block.DefaultCompression
	NoCompression      = block.NoCompression
	SnappyCompression  = block.SnappyCompression
	ZstdCompression    = block.ZstdCompression
	// MinLZCompression is only supported with table formats v6+. Older formats
	// fall back to snappy.
	MinLZCompression    = block.MinLZCompression
	FastestCompression  = block.FastestCompression
	FastCompression     = block.FastCompression
	BalancedCompression = block.BalancedCompression
	GoodCompression     = block.GoodCompression
)

// WriterOptions holds the parameters used to control building an sstable.
type WriterOptions struct {
	// BlockRestartInterval is the number of keys between restart points
	// for delta encoding of keys.
	//
	// The default value is 16.
	BlockRestartInterval int

	// BlockSize is the target uncompressed size in bytes of each table block.
	//
	// The default value is 4096.
	BlockSize int

	// BlockSizeThreshold finishes a block if the block size is larger than the
	// specified percentage of the target block size and adding the next entry
	// would cause the block to be larger than the target block size.
	//
	// The default value is 90.
	BlockSizeThreshold int

	// SizeClassAwareThreshold imposes a minimum block size restriction for blocks
	// to be flushed, that is computed as the percentage of the target block size.
	// Note that this threshold takes precedence over BlockSizeThreshold when
	// valid AllocatorSizeClasses are specified.
	//
	// The default value is 60.
	SizeClassAwareThreshold int

	// Comparer defines a total ordering over the space of []byte keys: a 'less
	// than' relationship. The same comparison algorithm must be used for reads
	// and writes over the lifetime of the DB.
	//
	// The default value uses the same ordering as bytes.Compare.
	Comparer *Comparer

	// Compression defines the per-block compression to use.
	//
	// The default value uses snappy compression.
	Compression *CompressionProfile

	// FilterPolicy defines a filter algorithm (such as a Bloom filter) that can
	// reduce disk reads for SeekPrefixGE calls.
	//
	// One such implementation is in the sstable/tablefilters/bloom package.
	//
	// The default value is NoFilterPolicy.
	FilterPolicy base.TableFilterPolicy

	// IndexBlockSize is the target uncompressed size in bytes of each index
	// block. When the index block size is larger than this target, two-level
	// indexes are automatically enabled. Setting this option to a large value
	// (such as math.MaxInt32) disables the automatic creation of two-level
	// indexes.
	//
	// The default value is the value of BlockSize.
	IndexBlockSize int

	// KeySchema describes the schema to use for sstable formats that make use
	// of columnar blocks, decomposing keys into their constituent components.
	// Ignored if TableFormat <= TableFormatPebblev4.
	KeySchema *colblk.KeySchema

	// Merger defines the associative merge operation to use for merging values
	// written with {Batch,DB}.Merge. The MergerName is checked for consistency
	// with the value stored in the sstable when it was written.
	MergerName string

	// TableFormat specifies the format version for writing sstables. The default
	// is TableFormatMinSupported.
	TableFormat TableFormat

	// IsStrictObsolete is only relevant for >= TableFormatPebblev4. See comment
	// in format.go. Must be false if format < TableFormatPebblev4.
	//
	// TODO(bilal): set this when writing shared ssts.
	IsStrictObsolete bool

	// WritingToLowestLevel is only relevant for >= TableFormatPebblev4. It is
	// used to set the obsolete bit on DEL/DELSIZED/SINGLEDEL if they are the
	// youngest for a userkey.
	WritingToLowestLevel bool

	// BlockPropertyCollectors is a list of BlockPropertyCollector creation
	// functions. A new BlockPropertyCollector is created for each sstable
	// built and lives for the lifetime of writing that table.
	BlockPropertyCollectors []func() BlockPropertyCollector

	// Checksum specifies which checksum to use.
	Checksum block.ChecksumType

	// ShortAttributeExtractor mirrors
	// Options.Experimental.ShortAttributeExtractor.
	ShortAttributeExtractor base.ShortAttributeExtractor

	// TieringSpanIDGetter returns the tiering span ID for a key. The span ID
	// is determined by policy (e.g., which logical partition the key belongs to).
	// Used to populate tiering histograms when WriteTieringHistograms is true.
	TieringSpanIDGetter func(key []byte) base.TieringSpanID

	// TieringAttributeExtractor extracts the tiering attribute from a key-value
	// pair. The attribute is data-driven metadata (e.g., access frequency).
	// Used to populate tiering histograms when WriteTieringHistograms is true.
	TieringAttributeExtractor func(key []byte, keyPrefixLen int, value []byte) (base.TieringAttribute, error)

	// WriteTieringHistograms enables writing tiering histogram blocks to the
	// sstable. Requires TieringSpanIDGetter and TieringAttributeExtractor to be set.
	WriteTieringHistograms bool

	// TieringThreshold is the tiering attribute threshold used to categorize keys
	// as below or above threshold in the histogram summary.
	TieringThreshold base.TieringAttribute

	// DisableValueBlocks is only used for TableFormat >= TableFormatPebblev3,
	// and if set to true, does not write any values to value blocks. This is
	// only intended for cases where the in-memory buffering of all value blocks
	// while writing a sstable is too expensive and likely to cause an OOM. It
	// is never set to true by a Pebble DB, and can be set to true when some
	// external code is directly generating huge sstables using Pebble's
	// sstable.Writer (for example, CockroachDB backups can sometimes write
	// 750MB sstables -- see
	// https://github.com/cockroachdb/cockroach/issues/117113).
	DisableValueBlocks bool

	// AllocatorSizeClasses provides a sorted list containing the supported size
	// classes of the underlying memory allocator. This provides hints to the
	// writer's flushing policy to select block sizes that preemptively reduce
	// internal fragmentation when loaded into the block cache.
	AllocatorSizeClasses []int

	// CompressionCounters are updated by the writer (if not nil).
	CompressionCounters *block.ByKind[block.LogicalBytesCompressed]

	// internal options can only be used from within the pebble package.
	internal sstableinternal.WriterOptions

	// NumDeletionsThreshold mirrors Options.Experimental.NumDeletionsThreshold.
	NumDeletionsThreshold int

	// DeletionSizeRatioThreshold mirrors
	// Options.Experimental.DeletionSizeRatioThreshold.
	DeletionSizeRatioThreshold float32

	// disableObsoleteCollector is used to disable the obsolete key block property
	// collector automatically added by sstable block writers.
	disableObsoleteCollector bool
}

// UserKeyPrefixBound represents a [Lower,Upper) bound of user key prefixes.
// If both are nil, there is no bound specified. Else, Compare(Lower,Upper)
// must be < 0.
type UserKeyPrefixBound struct {
	// Lower is a lower bound user key prefix.
	Lower []byte
	// Upper is an upper bound user key prefix.
	Upper []byte
}

// IsEmpty returns true iff the bound is empty.
func (ukb *UserKeyPrefixBound) IsEmpty() bool {
	return len(ukb.Lower) == 0 && len(ukb.Upper) == 0
}

// JemallocSizeClasses are a subset of available size classes in jemalloc[1],
// suitable for the AllocatorSizeClasses option.
//
// The size classes are used when writing sstables for determining target block
// sizes for flushes, with the goal of reducing internal memory fragmentation
// when the blocks are later loaded into the block cache. We only use the size
// classes between 16KiB - 256KiB as block limits fall in that range.
//
// [1] https://jemalloc.net/jemalloc.3.html#size_classes
var JemallocSizeClasses = []int{
	16 * 1024,
	20 * 1024, 24 * 1024, 28 * 1024, 32 * 1024, // 4KiB spacing
	40 * 1024, 48 * 1024, 56 * 1024, 64 * 1024, // 8KiB spacing
	80 * 1024, 96 * 1024, 112 * 1024, 128 * 1024, // 16KiB spacing.
	160 * 1024, 192 * 1024, 224 * 1024, 256 * 1024, // 32KiB spacing.
	320 * 1024,
}

// SetInternal sets the internal writer options. Note that even though this
// method is public, a caller outside the pebble package can't construct a value
// to pass to it.
func (o *WriterOptions) SetInternal(internalOpts sstableinternal.WriterOptions) {
	o.internal = internalOpts
}

func (o WriterOptions) ensureDefaults() WriterOptions {
	if o.BlockRestartInterval <= 0 {
		o.BlockRestartInterval = base.DefaultBlockRestartInterval
	}
	if o.BlockSize <= 0 {
		o.BlockSize = base.DefaultBlockSize
	}
	if o.BlockSizeThreshold <= 0 {
		o.BlockSizeThreshold = base.DefaultBlockSizeThreshold
	}
	if o.SizeClassAwareThreshold <= 0 {
		o.SizeClassAwareThreshold = base.SizeClassAwareBlockSizeThreshold
	}
	if o.Comparer == nil {
		o.Comparer = base.DefaultComparer
	}
	if o.IndexBlockSize <= 0 {
		o.IndexBlockSize = o.BlockSize
	}
	if o.MergerName == "" {
		o.MergerName = base.DefaultMerger.Name
	}
	if o.Checksum == block.ChecksumTypeNone {
		o.Checksum = block.ChecksumTypeCRC32c
	}
	// By default, if the table format is not specified, fall back to using the
	// most compatible format that is supported by Pebble.
	if o.TableFormat == TableFormatUnspecified {
		o.TableFormat = TableFormatMinSupported
	}
	if o.NumDeletionsThreshold == 0 {
		o.NumDeletionsThreshold = DefaultNumDeletionsThreshold
	}
	if o.DeletionSizeRatioThreshold == 0 {
		o.DeletionSizeRatioThreshold = DefaultDeletionSizeRatioThreshold
	}
	if o.KeySchema == nil && o.TableFormat.BlockColumnar() {
		s := colblk.DefaultKeySchema(o.Comparer, 16 /* bundle size */)
		o.KeySchema = &s
	}
	if o.Compression == nil || !tableFormatSupportsCompressionProfile(o.TableFormat, o.Compression) {
		o.Compression = block.SnappyCompression
	}
	if o.FilterPolicy == nil {
		o.FilterPolicy = base.NoFilterPolicy
	}
	return o
}

func tableFormatSupportsCompressionProfile(tf TableFormat, profile *CompressionProfile) bool {
	// MinLZ is only supported in TableFormatPebblev6 and higher.
	if tf < TableFormatPebblev6 && profile.UsesMinLZ() {
		return false
	}
	return true
}
