// Copyright 2011 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package base

// SSTable block defaults.
const (
	DefaultBlockRestartInterval = 16
	DefaultBlockSize            = 4096
	DefaultBlockSizeThreshold   = 90
)

// Compression is the per-block compression algorithm to use.
type Compression int

// The available compression types.
const (
	DefaultCompression Compression = iota
	NoCompression
	SnappyCompression
	NCompression
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

// TablePropertyCollector provides a hook for collecting user-defined
// properties based on the keys and values stored in an sstable. A new
// TablePropertyCollector is created for an sstable when the sstable is being
// written.
type TablePropertyCollector interface {
	// Add is called with each new entry added to the sstable. While the sstable
	// is itself sorted by key, do not assume that the entries are added in any
	// order. In particular, the ordering of point entries and range tombstones
	// is unspecified.
	Add(key InternalKey, value []byte) error

	// Finish is called when all entries have been added to the sstable. The
	// collected properties (if any) should be added to the specified map. Note
	// that in case of an error during sstable construction, Finish may not be
	// called.
	Finish(userProps map[string]string) error

	// The name of the property collector.
	Name() string
}
