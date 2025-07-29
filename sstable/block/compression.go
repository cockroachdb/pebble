// Copyright 2024 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package block

import (
	"runtime"
	"strings"

	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/pebble/internal/base"
	"github.com/cockroachdb/pebble/internal/compression"
)

// CompressionProfile contains the parameters for compressing blocks in an
// sstable or blob file.
//
// CompressionProfile is a more advanced successor to Compression.
type CompressionProfile struct {
	Name string

	// DataBlocks applies to sstable data blocks.
	// ValueBlocks applies to sstable value blocks and blob file value blocks.
	// OtherBlocks applies to all other blocks (such as index, filter, metadata
	// blocks).
	//
	// Some blocks (like rangedel) never use compression; this is at the
	// discretion of the sstable or blob file writer.
	//
	// Note that MinLZ is only supported with table formats v6+. Older formats
	// fall back to Snappy.
	DataBlocks  compression.Setting
	ValueBlocks compression.Setting
	OtherBlocks compression.Setting

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
	return p.DataBlocks.Algorithm == compression.MinLZ ||
		p.ValueBlocks.Algorithm == compression.MinLZ ||
		p.OtherBlocks.Algorithm == compression.MinLZ
}

var (
	NoCompression     = simpleCompressionProfile("NoCompression", compression.None)
	SnappyCompression = simpleCompressionProfile("Snappy", compression.Snappy)
	ZstdCompression   = simpleCompressionProfile("ZSTD", compression.ZstdLevel3)
	MinLZCompression  = simpleCompressionProfile("MinLZ", compression.MinLZFastest)

	DefaultCompression = SnappyCompression
	FastestCompression = simpleCompressionProfile("Fastest", fastestCompression)

	FastCompression = registerCompressionProfile(CompressionProfile{
		Name:                           "Fast",
		DataBlocks:                     fastestCompression,
		ValueBlocks:                    compression.ZstdLevel1,
		OtherBlocks:                    fastestCompression,
		MinReductionPercent:            10,
		AdaptiveReductionCutoffPercent: 30,
	})

	BalancedCompression = registerCompressionProfile(CompressionProfile{
		Name:                           "Balanced",
		DataBlocks:                     compression.ZstdLevel1,
		ValueBlocks:                    compression.ZstdLevel1,
		OtherBlocks:                    fastestCompression,
		MinReductionPercent:            5,
		AdaptiveReductionCutoffPercent: 20,
	})

	GoodCompression = registerCompressionProfile(CompressionProfile{
		Name:                           "Good",
		DataBlocks:                     compression.ZstdLevel3,
		ValueBlocks:                    compression.ZstdLevel3,
		OtherBlocks:                    fastestCompression,
		MinReductionPercent:            5,
		AdaptiveReductionCutoffPercent: 10,
	})
)

var fastestCompression = func() compression.Setting {
	if runtime.GOARCH == "arm64" {
		// MinLZ is generally faster and better than Snappy except for arm64: Snappy
		// has an arm64 assembly implementation and MinLZ does not.
		return compression.Snappy
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
		Name:                name,
		DataBlocks:          setting,
		ValueBlocks:         setting,
		OtherBlocks:         setting,
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
		return compression.NoCompression
	case SnappyCompressionIndicator:
		return compression.SnappyAlgorithm
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
	case compression.NoCompression:
		return NoCompressionIndicator
	case compression.SnappyAlgorithm:
		return SnappyCompressionIndicator
	case compression.Zstd:
		return ZstdCompressionIndicator
	case compression.MinLZ:
		return MinLZCompressionIndicator
	default:
		panic("invalid algorithm")
	}
}

// DecompressedLen returns the length of the provided block once decompressed,
// allowing the caller to allocate a buffer exactly sized to the decompressed
// payload.
func DecompressedLen(ci CompressionIndicator, b []byte) (decompressedLen int, err error) {
	decompressor := GetDecompressor(ci)
	defer decompressor.Close()
	return decompressor.DecompressedLen(b)
}

// DecompressInto decompresses compressed into buf. The buf slice must have the
// exact size as the decompressed value. Callers may use DecompressedLen to
// determine the correct size.
func DecompressInto(ci CompressionIndicator, compressed []byte, buf []byte) error {
	decompressor := GetDecompressor(ci)
	defer decompressor.Close()
	err := decompressor.DecompressInto(buf, compressed)
	if err != nil {
		return base.MarkCorruptionError(err)
	}
	return nil
}
