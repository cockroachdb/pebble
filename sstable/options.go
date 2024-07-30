// Copyright 2019 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package sstable

import (
	"github.com/cockroachdb/fifo"
	"github.com/cockroachdb/pebble/internal/base"
	"github.com/cockroachdb/pebble/internal/sstableinternal"
	"github.com/cockroachdb/pebble/sstable/block"
	"github.com/cockroachdb/pebble/sstable/rowblk"
)

// MaximumBlockSize is the maximum permissible size of a block.
const MaximumBlockSize = rowblk.MaximumSize

var ignoredInternalProperties = map[string]struct{}{
	"rocksdb.column.family.id":             {},
	"rocksdb.fixed.key.length":             {},
	"rocksdb.index.key.is.user.key":        {},
	"rocksdb.index.value.is.delta.encoded": {},
	"rocksdb.oldest.key.time":              {},
	"rocksdb.creation.time":                {},
	"rocksdb.file.creation.time":           {},
	"rocksdb.format.version":               {},
}

// FilterType exports the base.FilterType type.
type FilterType = base.FilterType

// Exported TableFilter constants.
const (
	TableFilter = base.TableFilter
)

// FilterWriter exports the base.FilterWriter type.
type FilterWriter = base.FilterWriter

// FilterPolicy exports the base.FilterPolicy type.
type FilterPolicy = base.FilterPolicy

// Comparers is a map from comparer name to comparer. It is used for debugging
// tools which may be used on multiple databases configured with different
// comparers.
type Comparers map[string]*base.Comparer

// Mergers is a map from merger name to merger. It is used for debugging tools
// which may be used on multiple databases configured with different
// mergers.
type Mergers map[string]*base.Merger

// ReaderOptions holds the parameters needed for reading an sstable.
type ReaderOptions struct {
	// LoadBlockSema, if set, is used to limit the number of blocks that can be
	// loaded (i.e. read from the filesystem) in parallel. Each load acquires one
	// unit from the semaphore for the duration of the read.
	LoadBlockSema *fifo.Semaphore

	// User properties specified in this map will not be added to sst.Properties.UserProperties.
	DeniedUserProperties map[string]struct{}

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

	// Filters is a map from filter policy name to filter policy. Filters with
	// policies that are not in this map will be ignored.
	Filters map[string]FilterPolicy

	// Logger is an optional logger and tracer.
	LoggerAndTracer base.LoggerAndTracer

	// FilterMetricsTracker is optionally used to track filter metrics.
	FilterMetricsTracker *FilterMetricsTracker

	// internal options can only be used from within the pebble package.
	internal sstableinternal.ReaderOptions
}

// SetInternal sets the internal reader options. Note that even though this
// method is public, a caller outside the pebble package can't construct a value
// to pass to it.
func (o *ReaderOptions) SetInternal(internalOpts sstableinternal.ReaderOptions) {
	o.internal = internalOpts
}

// SetInternalCacheOpts sets the internal cache options. Note that even though
// this method is public, a caller outside the pebble package can't construct a
// value to pass to it.
func (o *ReaderOptions) SetInternalCacheOpts(cacheOpts sstableinternal.CacheOptions) {
	o.internal.CacheOpts = cacheOpts
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
	if o.DeniedUserProperties == nil {
		o.DeniedUserProperties = ignoredInternalProperties
	}
	return o
}

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
	// The default value (DefaultCompression) uses snappy compression.
	Compression block.Compression

	// FilterPolicy defines a filter algorithm (such as a Bloom filter) that can
	// reduce disk reads for Get calls.
	//
	// One such implementation is bloom.FilterPolicy(10) from the pebble/bloom
	// package.
	//
	// The default value means to use no filter.
	FilterPolicy FilterPolicy

	// FilterType defines whether an existing filter policy is applied at a
	// block-level or table-level. Block-level filters use less memory to create,
	// but are slower to access as a check for the key in the index must first be
	// performed to locate the filter block. A table-level filter will require
	// memory proportional to the number of keys in an sstable to create, but
	// avoids the index lookup when determining if a key is present. Table-level
	// filters should be preferred except under constrained memory situations.
	FilterType FilterType

	// IndexBlockSize is the target uncompressed size in bytes of each index
	// block. When the index block size is larger than this target, two-level
	// indexes are automatically enabled. Setting this option to a large value
	// (such as math.MaxInt32) disables the automatic creation of two-level
	// indexes.
	//
	// The default value is the value of BlockSize.
	IndexBlockSize int

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

	// Parallelism is used to indicate that the sstable Writer is allowed to
	// compress data blocks and write datablocks to disk in parallel with the
	// Writer client goroutine.
	Parallelism bool

	// ShortAttributeExtractor mirrors
	// Options.Experimental.ShortAttributeExtractor.
	ShortAttributeExtractor base.ShortAttributeExtractor

	// RequiredInPlaceValueBound mirrors
	// Options.Experimental.RequiredInPlaceValueBound.
	RequiredInPlaceValueBound UserKeyPrefixBound

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

	// internal options can only be used from within the pebble package.
	internal sstableinternal.WriterOptions
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
	if o.Compression <= block.DefaultCompression || o.Compression >= block.NCompression {
		o.Compression = block.SnappyCompression
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
	return o
}
