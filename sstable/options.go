// Copyright 2019 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package sstable

import (
	"github.com/cockroachdb/pebble/internal/base"
	"github.com/cockroachdb/pebble/internal/cache"
)

// Compression exports the base.Compression type.
type Compression = base.Compression

// Exported Compression constants.
const (
	DefaultCompression = base.DefaultCompression
	NoCompression      = base.NoCompression
	SnappyCompression  = base.SnappyCompression
)

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

// TableFormat exports the base.TableFormat type.
type TableFormat = base.TableFormat

// Exported TableFormat constants.
const (
	TableFormatRocksDBv2 = base.TableFormatRocksDBv2
	TableFormatLevelDB   = base.TableFormatLevelDB
)

// TablePropertyCollector exports the base.TablePropertyCollector type.
type TablePropertyCollector = base.TablePropertyCollector

// Options holds the parameters needed for reading an sstable.
type Options struct {
	// Cache is used to cache uncompressed blocks from sstables.
	//
	// The default cache size is a zero-size cache.
	Cache *cache.Cache

	// Comparer defines a total ordering over the space of []byte keys: a 'less
	// than' relationship. The same comparison algorithm must be used for reads
	// and writes over the lifetime of the DB.
	//
	// The default value uses the same ordering as bytes.Compare.
	Comparer *Comparer

	// Filters is a map from filter policy name to filter policy. It is used for
	// debugging tools which may be used on multiple databases configured with
	// different filter policies. It is not necessary to populate this filters
	// map during normal usage of a DB.
	Filters map[string]FilterPolicy

	// Merger defines the associative merge operation to use for merging values
	// written with {Batch,DB}.Merge.
	//
	// The default merger concatenates values.
	Merger *Merger
}

func (o Options) ensureDefaults() Options {
	if o.Cache == nil {
		o.Cache = cache.New(0)
	}
	if o.Comparer == nil {
		o.Comparer = base.DefaultComparer
	}
	if o.Merger == nil {
		o.Merger = base.DefaultMerger
	}
	return o
}

// TableOptions holds the parameters used to control building an sstable.
type TableOptions struct {
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
	// The default value is 90
	BlockSizeThreshold int

	// Cache is used to cache uncompressed blocks from sstables.
	//
	// The default is a nil cache.
	Cache *cache.Cache

	// Comparer defines a total ordering over the space of []byte keys: a 'less
	// than' relationship. The same comparison algorithm must be used for reads
	// and writes over the lifetime of the DB.
	//
	// The default value uses the same ordering as bytes.Compare.
	Comparer *Comparer

	// Compression defines the per-block compression to use.
	//
	// The default value (DefaultCompression) uses snappy compression.
	Compression Compression

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
	// written with {Batch,DB}.Merge.
	//
	// The default merger concatenates values.
	Merger *Merger

	// TableFormat specifies the format version for writing sstables. The default
	// is TableFormatRocksDBv2 which creates RocksDB compatible sstables. Use
	// TableFormatLevelDB to create LevelDB compatible sstable which can be used
	// by a wider range of tools and libraries.
	TableFormat TableFormat

	// TablePropertyCollectors is a list of TablePropertyCollector creation
	// functions. A new TablePropertyCollector is created for each sstable built
	// and lives for the lifetime of the table.
	TablePropertyCollectors []func() TablePropertyCollector
}

var defaultLevelOptions base.LevelOptions

func init() {
	defaultLevelOptions.EnsureDefaults()
}

func (o TableOptions) ensureDefaults() TableOptions {
	if o.BlockRestartInterval <= 0 {
		o.BlockRestartInterval = defaultLevelOptions.BlockRestartInterval
	}
	if o.BlockSize <= 0 {
		o.BlockSize = defaultLevelOptions.BlockSize
	}
	if o.BlockSizeThreshold <= 0 {
		o.BlockSizeThreshold = defaultLevelOptions.BlockSizeThreshold
	}
	if o.Comparer == nil {
		o.Comparer = base.DefaultComparer
	}
	if o.Compression <= DefaultCompression || o.Compression >= base.NCompression {
		o.Compression = defaultLevelOptions.Compression
	}
	if o.IndexBlockSize <= 0 {
		o.IndexBlockSize = o.BlockSize
	}
	if o.Merger == nil {
		o.Merger = base.DefaultMerger
	}
	return o
}
