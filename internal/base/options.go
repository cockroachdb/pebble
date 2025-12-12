// Copyright 2011 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package base

// SSTable block defaults.
const (
	DefaultBlockRestartInterval      = 16
	DefaultBlockSize                 = 4096
	DefaultBlockSizeThreshold        = 90
	SizeClassAwareBlockSizeThreshold = 60
)

// TableFilterFamily identifies the table filter family. Each family has an
// associated TableFilterDecoder implementation.
type TableFilterFamily string

// TableFilterWriter provides an interface for creating table filter blocks.
//
// A TableFilterWriter implementation can decide at runtime what family of
// filter it creates.
type TableFilterWriter interface {
	// AddKey adds a key to the current filter block.
	AddKey(key []byte)

	// Finish returns an encoded filter for the current set of keys and the filter
	// family.
	//
	// If the filter should not be created (for example, if no keys were added or
	// it the filter would be bigger than a set limit), returns ok=false.
	//
	// The resulting filter data can only be decoded by the TableFilterDecoder
	// which is associated with the TableFilterFamily. The caller has to persist
	// the family along with the filter data.
	//
	// The writer state is reset after the call to Finish allowing the writer to
	// be reused for the creation of additional filters.
	Finish() (_ []byte, _ TableFilterFamily, ok bool)
}

// TableFilterPolicy is an algorithm for creating an approximate membership
// query filter. The canonical implementation is a Bloom filter.
//
// The name may be written to files on disk, along with the filter data. To use
// these filters, the FilterPolicy name at the time of writing must equal the
// name at the time of reading. If they do not match, the filters will be
// ignored, which will not affect correctness but may affect performance.
type TableFilterPolicy interface {
	// Name is the name of the filter policy. It is used in option files.
	Name() string

	// NewWriter creates a new TableFilterWriter.
	NewWriter() TableFilterWriter
}

// TableFilterDecoder provides an interface for using table filter blocks. Each
// decoder is associated with a TableFilterFamily. See TableFilterPolicy.
type TableFilterDecoder interface {
	Family() TableFilterFamily

	// MayContain returns whether the encoded filter may contain given key.
	// False positives are possible, where it returns true for keys not in the
	// original set.
	MayContain(filter, key []byte) bool
}

// NoFilterPolicy implements the "none" filter policy.
var NoFilterPolicy TableFilterPolicy = noFilter{}

type noFilter struct{}

func (noFilter) Name() string                 { return "none" }
func (noFilter) NewWriter() TableFilterWriter { panic("not implemented") }

// BlockPropertyFilter is used in an Iterator to filter sstables and blocks
// within the sstable. It should not maintain any per-sstable state, and must
// be thread-safe.
type BlockPropertyFilter interface {
	// Name returns the name of the block property collector.
	Name() string
	// Intersects returns true if the set represented by prop intersects with
	// the set in the filter.
	Intersects(prop []byte) (bool, error)
	// SyntheticSuffixIntersects runs Intersects, but only after using the passed in
	// suffix arg to modify a decoded copy of the passed in prop. This method only
	// needs to be implemented for filters which that will be used with suffix
	// replacement.
	SyntheticSuffixIntersects(prop []byte, suffix []byte) (bool, error)
}
