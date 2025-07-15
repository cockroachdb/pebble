// Copyright 2025 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package block

import (
	"cmp"
	"fmt"
	"iter"
	"slices"
	"strings"

	"github.com/cockroachdb/pebble/internal/compression"
	"github.com/cockroachdb/pebble/internal/invariants"
)

// CompressionStats collects compression statistics (either for a single file or
// for a collection of files).
//
// Compression statistics consist of the total compressed and uncompressed sizes for
// each distinct compression.Setting used.
type CompressionStats struct {
	// We inline common values to avoid allocating the map in most cases.

	// Total number of bytes that are not compressed.
	noCompressionBytes uint64
	// Compression stats for fastestCompression.
	fastest CompressionStatsForSetting

	others map[compression.Setting]CompressionStatsForSetting
}

type CompressionStatsForSetting struct {
	CompressedBytes   uint64
	UncompressedBytes uint64
}

func (cs *CompressionStatsForSetting) Add(other CompressionStatsForSetting) {
	cs.CompressedBytes += other.CompressedBytes
	cs.UncompressedBytes += other.UncompressedBytes
}

// add updates the stats to reflect a block that was compressed with the given setting.
func (c *CompressionStats) add(setting compression.Setting, stats CompressionStatsForSetting) {
	switch setting {
	case compression.None:
		c.noCompressionBytes += stats.UncompressedBytes
		if invariants.Enabled && stats.UncompressedBytes != stats.CompressedBytes {
			panic("invalid stats for no-compression")
		}
	case fastestCompression:
		c.fastest.Add(stats)
	default:
		if c.others == nil {
			c.others = make(map[compression.Setting]CompressionStatsForSetting, 2)
		}
		prev := c.others[setting]
		prev.Add(stats)
		c.others[setting] = prev
	}
}

// MergeWith updates the receiver stats to include the other stats.
func (c *CompressionStats) MergeWith(other *CompressionStats) {
	for s, cs := range other.All() {
		c.add(s, cs)
	}
}

// All returns an iterator over the collected stats, in arbitrary order.
func (c *CompressionStats) All() iter.Seq2[compression.Setting, CompressionStatsForSetting] {
	return func(yield func(s compression.Setting, cs CompressionStatsForSetting) bool) {
		if c.noCompressionBytes != 0 && !yield(compression.None, CompressionStatsForSetting{
			UncompressedBytes: c.noCompressionBytes,
			CompressedBytes:   c.noCompressionBytes,
		}) {
			return
		}
		if c.fastest.UncompressedBytes != 0 && !yield(fastestCompression, c.fastest) {
			return
		}
		for s, cs := range c.others {
			if !yield(s, cs) {
				return
			}
		}
	}
}

// String returns a string representation of the stats, in the format:
// "<setting1>:<compressed1>/<uncompressed1>,<setting2>:<compressed2>/<uncompressed2>,..."
//
// The settings are ordered alphabetically.
func (c CompressionStats) String() string {
	n := len(c.others)
	if c.noCompressionBytes != 0 {
		n++
	}
	if c.fastest.UncompressedBytes != 0 {
		n++
	}

	type entry struct {
		s  compression.Setting
		cs CompressionStatsForSetting
	}
	entries := make([]entry, 0, n)
	for s, cs := range c.All() {
		entries = append(entries, entry{s, cs})
	}
	slices.SortFunc(entries, func(x, y entry) int {
		if x.s.Algorithm != y.s.Algorithm {
			return cmp.Compare(x.s.Algorithm.String(), y.s.Algorithm.String())
		}
		return cmp.Compare(x.s.Level, y.s.Level)
	})

	var buf strings.Builder
	buf.Grow(n * 64)
	for _, e := range entries {
		if buf.Len() > 0 {
			buf.WriteString(",")
		}
		fmt.Fprintf(&buf, "%s:%d/%d", e.s.String(), e.cs.CompressedBytes, e.cs.UncompressedBytes)
	}
	return buf.String()
}
