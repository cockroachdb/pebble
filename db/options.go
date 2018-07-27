// Copyright 2011 The LevelDB-Go Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package db

import (
	"github.com/petermattis/pebble/cache"
	"github.com/petermattis/pebble/storage"
)

// Compression is the per-block compression algorithm to use.
type Compression int

const (
	DefaultCompression Compression = iota
	NoCompression
	SnappyCompression
	nCompression
)

func (c Compression) String() string {
	switch c {
	case DefaultCompression:
		return "Default"
	case NoCompression:
		return "NoCompression"
	case SnappyCompression:
		return "Snappy"
	default:
		return "Unknown"
	}
}

// FilterPolicy is an algorithm for probabilistically encoding a set of keys.
// The canonical implementation is a Bloom filter.
//
// Every FilterPolicy has a name. This names the algorithm itself, not any one
// particular instance. Aspects specific to a particular instance, such as the
// set of keys or any other parameters, will be encoded in the []byte filter
// returned by NewFilter.
//
// The name may be written to files on disk, along with the filter data. To use
// these filters, the FilterPolicy name at the time of writing must equal the
// name at the time of reading. If they do not match, the filters will be
// ignored, which will not affect correctness but may affect performance.
type FilterPolicy interface {
	// Name names the filter policy.
	Name() string

	// AppendFilter appends to dst an encoded filter that holds a set of []byte
	// keys.
	AppendFilter(dst []byte, keys [][]byte) []byte

	// MayContain returns whether the encoded filter may contain given key.
	// False positives are possible, where it returns true for keys not in the
	// original set.
	MayContain(filter, key []byte) bool
}

// FilterType is the level at which to apply a filter: block or table.
type FilterType int

// The available filter types.
const (
	BlockFilter FilterType = iota
	TableFilter
)

// LevelOptions ...
// TODO(peter):
type LevelOptions struct {
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

	// The maximum number of bytes for the level. When the maximum number of
	// bytes for a level is exceeded, compaction is requested.
	MaxBytes int64

	// The target file size for the level.
	TargetFileSize int64
}

// EnsureDefaults ...
func (o *LevelOptions) EnsureDefaults() *LevelOptions {
	if o == nil {
		o = &LevelOptions{}
	}
	if o.BlockRestartInterval <= 0 {
		o.BlockRestartInterval = 16
	}
	if o.BlockSize <= 0 {
		o.BlockSize = 4096
	}
	if o.BlockSizeThreshold <= 0 {
		o.BlockSizeThreshold = 90
	}
	if o.Compression <= DefaultCompression || o.Compression >= nCompression {
		o.Compression = SnappyCompression
	}
	if o.MaxBytes <= 0 {
		o.MaxBytes = 64 << 20 // 64 MB
	}
	if o.TargetFileSize <= 0 {
		o.TargetFileSize = 4 << 20 // 4 MB
	}
	return o
}

// Options holds the optional parameters for configuring pebble. These options
// apply to the DB at large; per-query options are defined by the ReadOptions
// and WriteOptions types.
type Options struct {
	// Sync sstables and the WAL periodically in order to smooth out writes to
	// disk. This option does not provide any persistency guarantee, but is used
	// to avoid latency spikes if the OS automatically decides to write out a
	// large chunk of dirty filesystem buffers.
	//
	// The default value is 512KB.
	BytesPerSync int

	// TODO(peter): provide a cache interface.
	Cache *cache.Cache

	// Comparer defines a total ordering over the space of []byte keys: a 'less
	// than' relationship. The same comparison algorithm must be used for reads
	// and writes over the lifetime of the DB.
	//
	// The default value uses the same ordering as bytes.Compare.
	Comparer *Comparer

	// ErrorIfDBExists is whether it is an error if the database already exists.
	//
	// The default value is false.
	ErrorIfDBExists bool

	// The number of files necessary to trigger an L0 compaction.
	L0CompactionThreshold int

	// Soft limit on the number of L0 files. Writes are slowed down when this
	// threshold is reached.
	L0SlowdownWritesThreshold int

	// Hard limit on the number of L0 files. Writes are stopped when this
	// threshold is reached.
	L0StopWritesThreshold int

	// Per-level options. Options for at least one level must be specified. The
	// options for the last level are used for all subsequent levels.
	Levels []LevelOptions

	// MaxOpenFiles is a soft limit on the number of open files that can be
	// used by the DB.
	//
	// The default value is 1000.
	MaxOpenFiles int

	// The size of a MemTable. Note that more than one MemTable can be in
	// existence since flushing a MemTable involves creating a new one and
	// writing the contents of the old one in the
	// background. MemTableStopWritesThreshold places a hard limit on the number
	// of MemTables allowed at once.
	MemTableSize int

	// Hard limit on the number of MemTables. Writes are stopped when this number
	// is reached. This value should be at least 2 or writes will stop whenever
	// the MemTable is being flushed.
	MemTableStopWritesThreshold int

	// Storage maps file names to byte storage.
	//
	// The default value uses the underlying operating system's file system.
	Storage storage.Storage
}

// EnsureDefaults ensures that the default values for all options are set if a
// valid value was not already specified. Returns the new options.
func (o *Options) EnsureDefaults() *Options {
	if o == nil {
		o = &Options{}
	}
	if o.BytesPerSync <= 0 {
		o.BytesPerSync = 512 << 10
	}
	if o.Comparer == nil {
		o.Comparer = DefaultComparer
	}
	if o.L0CompactionThreshold <= 0 {
		o.L0CompactionThreshold = 4
	}
	if o.L0SlowdownWritesThreshold <= 0 {
		o.L0SlowdownWritesThreshold = 8
	}
	if o.L0StopWritesThreshold <= 0 {
		o.L0StopWritesThreshold = 12
	}
	if o.Levels == nil {
		o.Levels = make([]LevelOptions, 1)
		for i := range o.Levels {
			if i > 0 {
				l := &o.Levels[i]
				if l.MaxBytes <= 0 {
					l.MaxBytes = o.Levels[i-1].MaxBytes * 10
				}
				if l.TargetFileSize <= 0 {
					l.TargetFileSize = o.Levels[i-1].TargetFileSize * 2
				}
			}
			o.Levels[i] = *o.Levels[i].EnsureDefaults()
		}
	}
	if o.MaxOpenFiles == 0 {
		o.MaxOpenFiles = 1000
	}
	if o.MemTableSize <= 0 {
		o.MemTableSize = 4 << 20
	}
	if o.MemTableStopWritesThreshold <= 0 {
		o.MemTableStopWritesThreshold = 2
	}
	if o.Storage == nil {
		o.Storage = storage.Default
	}
	return o
}

// Level ...
func (o *Options) Level(level int) LevelOptions {
	if level < len(o.Levels) {
		return o.Levels[level]
	}
	n := len(o.Levels) - 1
	l := o.Levels[n]
	for i := n; i < level; i++ {
		l.MaxBytes *= 10
		l.TargetFileSize *= 2
	}
	return l
}

// ReadOptions hold the optional per-query parameters for Get and Find
// operations.
//
// Like Options, a nil *ReadOptions is valid and means to use the default
// values.
type ReadOptions struct {
	// TODO(peter): document
	LowerBound []byte
	UpperBound []byte
	// TableFilter func(_ TableProperties) bool
}

// WriteOptions hold the optional per-query parameters for Set and Delete
// operations.
//
// Like Options, a nil *WriteOptions is valid and means to use the default
// values.
type WriteOptions struct {
	// Sync is whether to sync underlying writes from the OS buffer cache
	// through to actual disk, if applicable. Setting Sync can result in
	// slower writes.
	//
	// If false, and the machine crashes, then some recent writes may be lost.
	// Note that if it is just the process that crashes (and the machine does
	// not) then no writes will be lost.
	//
	// In other words, Sync being false has the same semantics as a write
	// system call. Sync being true means write followed by fsync.
	//
	// The default value is true.
	Sync bool
}

var Sync = &WriteOptions{Sync: true}
var NoSync = &WriteOptions{Sync: false}

func (o *WriteOptions) GetSync() bool {
	return o == nil || o.Sync
}
