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

	"github.com/cockroachdb/crlib/crmath"
	"github.com/cockroachdb/errors"
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

// CompressionRatio returns the compression ratio for the setting. Returns 0 if
// the stats are empty.
func (cs CompressionStatsForSetting) CompressionRatio() float64 {
	if cs.CompressedBytes == 0 {
		return 0
	}
	return float64(cs.UncompressedBytes) / float64(cs.CompressedBytes)
}

func (cs *CompressionStatsForSetting) Add(other CompressionStatsForSetting) {
	cs.CompressedBytes += other.CompressedBytes
	cs.UncompressedBytes += other.UncompressedBytes
}

func (c *CompressionStats) IsEmpty() bool {
	return c.noCompressionBytes == 0 && c.fastest.CompressedBytes == 0 && len(c.others) == 0
}

func (c *CompressionStats) Reset() {
	c.noCompressionBytes = 0
	c.fastest = CompressionStatsForSetting{}
	clear(c.others)
}

// addOne updates the stats to reflect a block that was compressed with the given setting.
func (c *CompressionStats) addOne(setting compression.Setting, stats CompressionStatsForSetting) {
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

// Add updates the receiver stats to include the other stats.
func (c *CompressionStats) Add(other *CompressionStats) {
	for s, cs := range other.All() {
		c.addOne(s, cs)
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

// Scale the stats by (size/backingSize). Used to obtain an approximation of the
// stats for a virtual table.
func (c *CompressionStats) Scale(size uint64, backingSize uint64) {
	// Make sure the sizes are sane, just in case.
	size = max(size, 1)
	backingSize = max(backingSize, size)

	c.noCompressionBytes = crmath.ScaleUint64(c.noCompressionBytes, size, backingSize)
	c.fastest.CompressedBytes = crmath.ScaleUint64(c.fastest.CompressedBytes, size, backingSize)
	c.fastest.UncompressedBytes = crmath.ScaleUint64(c.fastest.UncompressedBytes, size, backingSize)

	for s, cs := range c.others {
		cs.CompressedBytes = crmath.ScaleUint64(cs.CompressedBytes, size, backingSize)
		cs.UncompressedBytes = crmath.ScaleUint64(cs.UncompressedBytes, size, backingSize)
		c.others[s] = cs
	}
}

// ParseCompressionStats parses the output of CompressionStats.String back into CompressionStats.
//
// If the string contains statistics for unknown compression settings, these are
// accumulated under a special "unknown" setting.
func ParseCompressionStats(s string) (CompressionStats, error) {
	if s == "" {
		return CompressionStats{}, nil
	}
	var stats CompressionStats
	for _, a := range strings.Split(s, ",") {
		b := strings.Split(a, ":")
		if len(b) != 2 {
			return CompressionStats{}, errors.Errorf("cannot parse compression stats %q", s)
		}
		setting, ok := compression.ParseSetting(b[0])
		if !ok {
			setting = compression.Setting{Algorithm: compression.Unknown, Level: 0}
		}
		var cs CompressionStatsForSetting
		if _, err := fmt.Sscanf(b[1], "%d/%d", &cs.CompressedBytes, &cs.UncompressedBytes); err != nil {
			return CompressionStats{}, errors.Errorf("cannot parse compression stats %q", s)
		}
		stats.addOne(setting, cs)
	}
	return stats, nil
}
