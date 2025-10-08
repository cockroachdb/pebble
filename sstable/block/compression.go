// Copyright 2024 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package block

import (
	"runtime"
	"strings"
	"sync/atomic"

	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/pebble/internal/base"
	"github.com/cockroachdb/pebble/internal/compression"
	"github.com/cockroachdb/pebble/sstable/block/blockkind"
)

// CompressionProfile contains the parameters for compressing blocks in an
// sstable or blob file.
//
// CompressionProfile is a more advanced successor to Compression.
//
// Some blocks (like rangedel) never use compression; this is at the
// discretion of the sstable or blob file writer.
//
// Note that MinLZ is only supported with table formats v6+. Older formats
// fall back to Snappy.
type CompressionProfile struct {
	Name string

	Settings ByKind[compression.Setting]

	// Blocks that are reduced by less than this percentage are stored
	// uncompressed.
	MinReductionPercent uint8

	// AdaptiveReductionCutoffPercent (when set to a non-zero value) enables
	// adaptive compressors for data and value blocks which fall back to the
	// OtherBlocks setting. The OtherBlocks setting is used when the
	// DataBlocks/ValueBlocks setting cannot achieve a further data reduction of
	// at least AdaptiveReductionCutoffPercent%.
	AdaptiveReductionCutoffPercent uint8
}

// UsesMinLZ returns true if the profile uses the MinLZ compression algorithm
// (for any block kind).
func (p *CompressionProfile) UsesMinLZ() bool {
	return p.Settings.DataBlocks.Algorithm == compression.MinLZ ||
		p.Settings.ValueBlocks.Algorithm == compression.MinLZ ||
		p.Settings.OtherBlocks.Algorithm == compression.MinLZ
}

var (
	NoCompression     = simpleCompressionProfile("NoCompression", compression.NoCompression)
	SnappyCompression = simpleCompressionProfile("Snappy", compression.SnappySetting)
	ZstdCompression   = simpleCompressionProfile("ZSTD", compression.ZstdLevel3)
	MinLZCompression  = simpleCompressionProfile("MinLZ", compression.MinLZFastest)

	// DefaultCompression is a legacy profile that uses Snappy.
	DefaultCompression = SnappyCompression

	// FastestCompression uses Snappy or MinLZ1.
	FastestCompression = simpleCompressionProfile("Fastest", fastestCompression)

	// FastCompression automatically chooses between Snappy/MinLZ1 and Zstd1 for
	// sstable and blob file value blocks.
	FastCompression = registerCompressionProfile(CompressionProfile{
		Name: "Fast",
		Settings: ByKind[compression.Setting]{
			DataBlocks:  fastestCompression,
			ValueBlocks: compression.ZstdLevel1,
			OtherBlocks: fastestCompression,
		},
		MinReductionPercent:            10,
		AdaptiveReductionCutoffPercent: 30,
	})

	// BalancedCompression automatically chooses between Snappy/MinLZ1 and Zstd1
	// for data and value blocks.
	BalancedCompression = registerCompressionProfile(CompressionProfile{
		Name: "Balanced",
		Settings: ByKind[compression.Setting]{
			DataBlocks:  compression.ZstdLevel1,
			ValueBlocks: compression.ZstdLevel1,
			OtherBlocks: fastestCompression,
		},
		MinReductionPercent:            5,
		AdaptiveReductionCutoffPercent: 15,
	})

	// GoodCompression uses Zstd1 for data and value blocks.
	GoodCompression = registerCompressionProfile(CompressionProfile{
		Name: "Good",
		// In practice, we have observed very little size benefit to using higher
		// zstd levels like ZstdLevel3 while paying a significant compression
		// performance cost.
		Settings: ByKind[compression.Setting]{
			DataBlocks:  compression.ZstdLevel1,
			ValueBlocks: compression.ZstdLevel1,
			OtherBlocks: fastestCompression,
		},
		MinReductionPercent: 3,
	})
)

// fastestCompression is either Snappy or MinLZ1, depending on the architecture.
var fastestCompression = func() compression.Setting {
	if runtime.GOARCH == "arm64" {
		// MinLZ is generally faster and better than Snappy except for arm64: Snappy
		// has an arm64 assembly implementation and MinLZ does not.
		return compression.SnappySetting
	}
	return compression.MinLZFastest
}()

// simpleCompressionProfile returns a CompressionProfile that uses the same
// compression setting for all blocks and which uses the uncompressed block if
// compression reduces it by less than 12%. This is similar to older Pebble
// versions which used Compression.
//
// It should only be used during global initialization.
func simpleCompressionProfile(name string, setting compression.Setting) *CompressionProfile {
	return registerCompressionProfile(CompressionProfile{
		Name: name,
		Settings: ByKind[compression.Setting]{
			DataBlocks:  setting,
			ValueBlocks: setting,
			OtherBlocks: setting,
		},
		MinReductionPercent: 12,
	})
}

// CompressionProfileByName returns the built-in compression profile with the
// given name, or nil if there is no such profile. It is case-insensitive.
//
// The caller must gracefully handle the nil return case as an unknown
// (user-defined or deprecated) profile.
func CompressionProfileByName(name string) *CompressionProfile {
	return compressionProfileMap[strings.ToLower(name)]
}

var compressionProfileMap = make(map[string]*CompressionProfile)

func registerCompressionProfile(p CompressionProfile) *CompressionProfile {
	key := strings.ToLower(p.Name)
	if _, ok := compressionProfileMap[key]; ok {
		panic(errors.AssertionFailedf("duplicate compression profile: %s", p.Name))
	}
	compressionProfileMap[key] = &p
	return &p
}

// CompressionIndicator is the byte stored physically within the block.Trailer
// to indicate the compression type.
//
// TODO(jackson): Avoid exporting once all compression and decompression is
// delegated to the block package.
type CompressionIndicator byte

// The block type gives the per-block compression format.
// These constants are part of the file format and should not be changed.
// They are different from the Compression constants because the latter
// are designed so that the zero value of the Compression type means to
// use the default compression (which is snappy).
// Not all compression types listed here are supported.
const (
	NoCompressionIndicator     CompressionIndicator = 0
	SnappyCompressionIndicator CompressionIndicator = 1
	ZlibCompressionIndicator   CompressionIndicator = 2
	Bzip2CompressionIndicator  CompressionIndicator = 3
	Lz4CompressionIndicator    CompressionIndicator = 4
	Lz4hcCompressionIndicator  CompressionIndicator = 5
	XpressCompressionIndicator CompressionIndicator = 6
	ZstdCompressionIndicator   CompressionIndicator = 7
	MinLZCompressionIndicator  CompressionIndicator = 8
)

// String implements fmt.Stringer.
func (i CompressionIndicator) String() string {
	switch i {
	case 0:
		return "none"
	case 1:
		return "snappy"
	case 2:
		return "zlib"
	case 3:
		return "bzip2"
	case 4:
		return "lz4"
	case 5:
		return "lz4hc"
	case 6:
		return "xpress"
	case 7:
		return "zstd"
	case 8:
		return "minlz"
	default:
		panic(errors.Newf("sstable: unknown block type: %d", i))
	}
}

func (i CompressionIndicator) Algorithm() compression.Algorithm {
	switch i {
	case NoCompressionIndicator:
		return compression.NoAlgorithm
	case SnappyCompressionIndicator:
		return compression.Snappy
	case ZstdCompressionIndicator:
		return compression.Zstd
	case MinLZCompressionIndicator:
		return compression.MinLZ
	default:
		panic("Invalid compression type.")
	}
}

func compressionIndicatorFromAlgorithm(algo compression.Algorithm) CompressionIndicator {
	switch algo {
	case compression.NoAlgorithm:
		return NoCompressionIndicator
	case compression.Snappy:
		return SnappyCompressionIndicator
	case compression.Zstd:
		return ZstdCompressionIndicator
	case compression.MinLZ:
		return MinLZCompressionIndicator
	default:
		panic("invalid algorithm")
	}
}

// CompressionCounters holds running counters for the number of bytes compressed
// and decompressed. Counters are separated by L5 vs L6 vs other levels, and by
// data blocks vs value blocks vs other blocks. These are the same categories
// for which compression profiles can vary.
//
// The main purpose for these metrics is to allow estimating how overall CPU
// usage would change with a different compression algorithm (in conjunction
// with performance information about the algorithms, like that produced by the
// compression analyzer).
//
// In all cases, the figures refer to the uncompressed ("logical") size; i.e.
// the *input* size for compression and the *output* size for decompression.
//
// Note that even if the compressor does not use the result of a compression
// (because the block didn't compress), those bytes are still counted (they are
// relevant for CPU usage).
//
// Blocks for which compression is disabled upfront (e.g. filter blocks) are not
// counted.
type CompressionCounters struct {
	Compressed   ByLevel[ByKind[LogicalBytesCompressed]]
	Decompressed ByLevel[ByKind[LogicalBytesDecompressed]]
}

func (c *CompressionCounters) LoadCompressed() ByLevel[ByKind[uint64]] {
	return mapByLevel(&c.Compressed, func(k *ByKind[LogicalBytesCompressed]) ByKind[uint64] {
		return mapByKind(k, func(v *LogicalBytesCompressed) uint64 {
			return v.Load()
		})
	})
}

func (c *CompressionCounters) LoadDecompressed() ByLevel[ByKind[uint64]] {
	return mapByLevel(&c.Decompressed, func(k *ByKind[LogicalBytesDecompressed]) ByKind[uint64] {
		return mapByKind(k, func(v *LogicalBytesDecompressed) uint64 {
			return v.Load()
		})
	})
}

// LogicalBytesCompressed keeps a count of the logical bytes that were compressed.
type LogicalBytesCompressed struct {
	atomic.Uint64
}

// LogicalBytesDecompressed keeps a count of the logical bytes that were decompressed.
type LogicalBytesDecompressed struct {
	atomic.Uint64
}

// ByKind stores three different instances of T, one for sstable data
// blocks, one for sstable and blob file value blocks, and one for all other
// blocks.
type ByKind[T any] struct {
	DataBlocks  T
	ValueBlocks T
	OtherBlocks T
}

func (b *ByKind[T]) ForKind(kind Kind) *T {
	switch kind {
	case blockkind.SSTableValue, blockkind.BlobValue:
		return &b.ValueBlocks
	case blockkind.SSTableData:
		return &b.DataBlocks
	default:
		return &b.OtherBlocks
	}
}

func mapByKind[T, U any](b *ByKind[T], f func(*T) U) ByKind[U] {
	return ByKind[U]{
		DataBlocks:  f(&b.DataBlocks),
		ValueBlocks: f(&b.ValueBlocks),
		OtherBlocks: f(&b.OtherBlocks),
	}
}

// ByLevel stores three different instance of T, one for L5, one for L6, and one
// for all other levels.
type ByLevel[T any] struct {
	L5          T
	L6          T
	OtherLevels T
}

func (b *ByLevel[T]) ForLevel(level base.Level) *T {
	if l, ok := level.Get(); ok {
		switch l {
		case 5:
			return &b.L5
		case 6:
			return &b.L6
		}
	}
	return &b.OtherLevels
}

func mapByLevel[T, U any](b *ByLevel[T], f func(*T) U) ByLevel[U] {
	return ByLevel[U]{
		L5:          f(&b.L5),
		L6:          f(&b.L6),
		OtherLevels: f(&b.OtherLevels),
	}
}
