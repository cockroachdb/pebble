// Copyright 2011 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package db

import (
	"bytes"
	"fmt"
	"strings"

	"github.com/petermattis/pebble/cache"
	"github.com/petermattis/pebble/vfs"
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

// FilterType is the level at which to apply a filter: block or table.
type FilterType int

// The available filter types.
const (
	TableFilter FilterType = iota
)

func (t FilterType) String() string {
	switch t {
	case TableFilter:
		return "table"
	}
	return "unknown"
}

// FilterWriter provides an interface for creating filter blocks. See
// FilterPolicy for more details about filters.
type FilterWriter interface {
	// AddKey adds a key to the current filter block.
	AddKey(key []byte)

	// Finish appends to dst an encoded filter tha holds the current set of
	// keys. The writer state is reset after the call to Finish allowing the
	// writer to be reused for the creation of additional filters.
	Finish(dst []byte) []byte
}

// FilterPolicy is an algorithm for probabilistically encoding a set of keys.
// The canonical implementation is a Bloom filter.
//
// Every FilterPolicy has a name. This names the algorithm itself, not any one
// particular instance. Aspects specific to a particular instance, such as the
// set of keys or any other parameters, will be encoded in the []byte filter
// returned by NewWriter.
//
// The name may be written to files on disk, along with the filter data. To use
// these filters, the FilterPolicy name at the time of writing must equal the
// name at the time of reading. If they do not match, the filters will be
// ignored, which will not affect correctness but may affect performance.
type FilterPolicy interface {
	// Name names the filter policy.
	Name() string

	// MayContain returns whether the encoded filter may contain given key.
	// False positives are possible, where it returns true for keys not in the
	// original set.
	MayContain(ftype FilterType, filter, key []byte) bool

	// NewWriter creates a new FilterWriter.
	NewWriter(ftype FilterType) FilterWriter
}

func filterPolicyName(p FilterPolicy) string {
	if p == nil {
		return "none"
	}
	return p.Name()
}

// TableFormat specifies the format version for sstables. The legacy LevelDB
// format is format version 0.
type TableFormat uint32

// The available table formats. Note that these values are not (and should not)
// be serialized to disk. TableFormatRocksDBv2 is the default if otherwise
// unspecified.
const (
	TableFormatRocksDBv2 TableFormat = iota
	TableFormatLevelDB
)

// LevelOptions holds the optional per-level parameters.
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

	// The target file size for the level.
	TargetFileSize int64
}

// EnsureDefaults ensures that the default values for all of the options have
// been initialized. It is valid to call EnsureDefaults on a nil receiver. A
// non-nil result will always be returned.
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
	if o.TargetFileSize <= 0 {
		o.TargetFileSize = 2 << 20 // 2 MB
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

	// Disable the write-ahead log (WAL). Disabling the write-ahead log prohibits
	// crash recovery, but can improve performance if crash recovery is not
	// needed (e.g. when only temporary state is being stored in the database).
	//
	// TODO(peter): untested
	DisableWAL bool

	// ErrorIfDBExists is whether it is an error if the database already exists.
	//
	// The default value is false.
	ErrorIfDBExists bool

	// EventListener provides hooks to listening to significant DB events such as
	// flushes, compactions, and table deletion.
	EventListener EventListener

	// FS provides the interface for persistent file storage.
	//
	// The default value uses the underlying operating system's file system.
	FS vfs.FS

	// The number of files necessary to trigger an L0 compaction.
	L0CompactionThreshold int

	// Soft limit on the number of L0 files. Writes are slowed down when this
	// threshold is reached.
	L0SlowdownWritesThreshold int

	// Hard limit on the number of L0 files. Writes are stopped when this
	// threshold is reached.
	L0StopWritesThreshold int

	// The maximum number of bytes for LBase. The base level is the level which
	// L0 is compacted into. The base level is determined dynamically based on
	// the existing data in the LSM. The maximum number of bytes for other levels
	// is computed dynamically based on the base level's maximum size. When the
	// maximum number of bytes for a level is exceeded, compaction is requested.
	LBaseMaxBytes int64

	// Per-level options. Options for at least one level must be specified. The
	// options for the last level are used for all subsequent levels.
	Levels []LevelOptions

	// Logger used to write log messages.
	//
	// The default logger uses the Go standard library log package.
	Logger Logger

	// MaxManifestFileSize is the maximum size the MANIFEST file is allowed to
	// become. When the MANIFEST exceeds this size it is rolled over and a new
	// MANIFEST is created.
	MaxManifestFileSize int64

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

	// Merger defines the associative merge operation to use for merging values
	// written with {Batch,DB}.Merge.
	//
	// The default merger concatenates values.
	Merger *Merger

	// TableFormat specifies the format version for sstables. The default is
	// TableFormatRocksDBv2 which creates RocksDB compatible sstables. Use
	// TableFormatLevelDB to create LevelDB compatible sstable which can be used
	// by a wider range of tools and libraries.
	//
	// TODO(peter): TableFormatLevelDB does not support all of the functionality
	// of TableFormatRocksDBv2. We should ensure it is only used when writing an
	// sstable directly, and not used when opening a database.
	TableFormat TableFormat

	// WALDir specifies the directory to store write-ahead logs (WALs) in. If
	// empty (the default), WALs will be stored in the same directory as sstables
	// (i.e. the directory passed to pebble.Open).
	WALDir string
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
	if o.LBaseMaxBytes <= 0 {
		o.LBaseMaxBytes = 64 << 20 // 64 MB
	}
	if o.Levels == nil {
		o.Levels = make([]LevelOptions, 1)
		for i := range o.Levels {
			if i > 0 {
				l := &o.Levels[i]
				if l.TargetFileSize <= 0 {
					l.TargetFileSize = o.Levels[i-1].TargetFileSize * 2
				}
			}
			o.Levels[i].EnsureDefaults()
		}
	} else {
		for i := range o.Levels {
			o.Levels[i].EnsureDefaults()
		}
	}
	if o.Logger == nil {
		o.Logger = defaultLogger{}
	}
	o.EventListener.EnsureDefaults(o.Logger)
	if o.MaxManifestFileSize == 0 {
		o.MaxManifestFileSize = 128 << 20 // 128 MB
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
	if o.Merger == nil {
		o.Merger = DefaultMerger
	}
	if o.FS == nil {
		o.FS = vfs.Default
	}
	return o
}

// Level returns the LevelOptions for the specified level.
func (o *Options) Level(level int) LevelOptions {
	if level < len(o.Levels) {
		return o.Levels[level]
	}
	n := len(o.Levels) - 1
	l := o.Levels[n]
	for i := n; i < level; i++ {
		l.TargetFileSize *= 2
	}
	return l
}

func (o *Options) String() string {
	var buf bytes.Buffer

	fmt.Fprintf(&buf, "[Version]\n")
	fmt.Fprintf(&buf, "  pebble_version=0.1\n")
	fmt.Fprintf(&buf, "\n")
	fmt.Fprintf(&buf, "[Options]\n")
	fmt.Fprintf(&buf, "  bytes_per_sync=%d\n", o.BytesPerSync)
	fmt.Fprintf(&buf, "  cache_size=%d\n", o.Cache.MaxSize())
	fmt.Fprintf(&buf, "  comparer=%s\n", o.Comparer.Name)
	fmt.Fprintf(&buf, "  disable_wal=%t\n", o.DisableWAL)
	fmt.Fprintf(&buf, "  l0_compaction_threshold=%d\n", o.L0CompactionThreshold)
	fmt.Fprintf(&buf, "  l0_slowdown_writes_threshold=%d\n", o.L0SlowdownWritesThreshold)
	fmt.Fprintf(&buf, "  l0_stop_writes_threshold=%d\n", o.L0StopWritesThreshold)
	fmt.Fprintf(&buf, "  lbase_max_bytes=%d\n", o.LBaseMaxBytes)
	fmt.Fprintf(&buf, "  max_manifest_file_size=%d\n", o.MaxManifestFileSize)
	fmt.Fprintf(&buf, "  max_open_files=%d\n", o.MaxOpenFiles)
	fmt.Fprintf(&buf, "  mem_table_size=%d\n", o.MemTableSize)
	fmt.Fprintf(&buf, "  mem_table_stop_writes_threshold=%d\n", o.MemTableStopWritesThreshold)
	fmt.Fprintf(&buf, "  merger=%s\n", o.Merger.Name)
	fmt.Fprintf(&buf, "  wal_dir=%s\n", o.WALDir)

	for i := range o.Levels {
		l := &o.Levels[i]
		fmt.Fprintf(&buf, "\n")
		fmt.Fprintf(&buf, "[Level \"%d\"]\n", i)
		fmt.Fprintf(&buf, "  block_restart_interval=%d\n", l.BlockRestartInterval)
		fmt.Fprintf(&buf, "  block_size=%d\n", l.BlockSize)
		fmt.Fprintf(&buf, "  compression=%s\n", l.Compression)
		fmt.Fprintf(&buf, "  filter_policy=%s\n", filterPolicyName(l.FilterPolicy))
		fmt.Fprintf(&buf, "  filter_type=%s\n", l.FilterType)
		fmt.Fprintf(&buf, "  target_file_size=%d\n", l.TargetFileSize)
	}

	return buf.String()
}

// Check verifies the options are compatible with the previous options
// serialized by Options.String(). For example, the Comparer and Merger must be
// the same, or data will not be able to be properly read from the DB.
func (o *Options) Check(s string) error {
	var section string
	for _, line := range strings.Split(s, "\n") {
		line = strings.TrimSpace(line)
		if len(line) == 0 {
			// Skip blank lines.
			continue
		}
		if line[0] == ';' || line[0] == '#' {
			// Skip comments.
			continue
		}
		n := len(line)
		if line[0] == '[' && line[n-1] == ']' {
			// Parse section.
			section = line[1 : n-1]
			continue
		}

		pos := strings.Index(line, "=")
		if pos < 0 {
			return fmt.Errorf("pebble: invalid key=value syntax: %s", line)
		}

		key := strings.TrimSpace(line[:pos])
		value := strings.TrimSpace(line[pos+1:])
		path := section + "." + key

		// RocksDB uses a similar (INI-style) syntax for the OPTIONS file, but
		// different section names and keys. The "CFOptions ..." paths below are
		// the RocksDB versions.
		switch path {
		case "Options.comparer", `CFOptions "default".comparator`:
			if value != o.Comparer.Name {
				return fmt.Errorf("pebble: comparer name from file %q != comparer name from options %q",
					value, o.Comparer.Name)
			}
		case "Options.merger", `CFOptions "default".merge_operator`:
			// RocksDB allows the merge operator to be unspecified, in which case it
			// shows up as "nullptr".
			if value != "nullptr" && value != o.Merger.Name {
				return fmt.Errorf("pebble: merger name from file %q != meger name from options %q",
					value, o.Merger.Name)
			}
		}
	}
	return nil
}

// IterOptions hold the optional per-query parameters for NewIter.
//
// Like Options, a nil *IterOptions is valid and means to use the default
// values.
type IterOptions struct {
	// LowerBound specifies the smallest key (inclusive) that the iterator will
	// return during iteration. If the iterator is seeked or iterated past this
	// boundary the iterator will return Valid()==false. Setting LowerBound
	// effectively truncates the key space visible to the iterator.
	LowerBound []byte
	// UpperBound specifies the largest key (exclusive) that the iterator will
	// return during iteration. If the iterator is seeked or iterated past this
	// boundary the iterator will return Valid()==false. Setting UpperBound
	// effectively truncates the key space visible to the iterator.
	UpperBound []byte
	// TableFilter can be used to filter the tables that are scanned during
	// iteration based on the user properties. Return true to scan the table and
	// false to skip scanning.
	//
	// TODO(peter): unimplemented.
	// TableFilter func(userProps map[string]string) bool

	// If Prefix is true, the iterator will only be used to iterate over keys
	// matching that of the key it is first positioned at. If the Comparer was
	// supplied with a user-defined Split function and bloom filters are
	// enabled, this allows for improved performance by skipping SSTables known
	// not to contain the given prefix. The iterator will not properly observe
	// keys not matching the prefix.
	//
	// TODO(tbg): should an assertion trip if the first key's prefix is unstable?
	// TODO(tbg): should Prefix override (or sharpen) {Lower,Upper}Bound? When
	// we see the first key, we get the prefix and a separator which should be
	// a good {Lower,Upper}Bound.
	// TODO(tbg): unimplemented.
	// Prefix bool
}

// GetLowerBound returns the LowerBound or nil if the receiver is nil.
func (o *IterOptions) GetLowerBound() []byte {
	if o == nil {
		return nil
	}
	return o.LowerBound
}

// GetUpperBound returns the UpperBound or nil if the receiver is nil.
func (o *IterOptions) GetUpperBound() []byte {
	if o == nil {
		return nil
	}
	return o.UpperBound
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

// Sync specifies the default write options for writes which synchronize to
// disk.
var Sync = &WriteOptions{Sync: true}

// NoSync specifies the default write options for writes which do not
// synchronize to disk.
var NoSync = &WriteOptions{Sync: false}

// GetSync returns the Sync value or true if the receiver is nil.
func (o *WriteOptions) GetSync() bool {
	return o == nil || o.Sync
}
