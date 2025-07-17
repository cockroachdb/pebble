// Copyright 2025 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package block

import (
	"fmt"
	"iter"
	"strings"

	"github.com/cockroachdb/pebble/internal/compression"
)

// CompressionStats collects compression statistics for a single file - the
// total compressed and uncompressed sizes for each distinct compression.Setting
// used.
type CompressionStats struct {
	n int
	// Compression profiles have three settings (data, value, other) and
	// NoCompression can also be used for data that didn't compress.
	buf [4]CompressionStatsForSetting
}

type CompressionStatsForSetting struct {
	Setting           compression.Setting
	UncompressedBytes uint64
	CompressedBytes   uint64
}

// add updates the stats to reflect a block that was compressed with the given setting.
func (c *CompressionStats) add(
	setting compression.Setting, sizeUncompressed, sizeCompressed uint64,
) {
	for i := 0; i < c.n; i++ {
		if c.buf[i].Setting == setting {
			c.buf[i].UncompressedBytes += sizeUncompressed
			c.buf[i].CompressedBytes += sizeCompressed
			return
		}
	}
	if c.n >= len(c.buf)-1 {
		panic("too many compression settings")
	}
	c.buf[c.n] = CompressionStatsForSetting{
		Setting:           setting,
		UncompressedBytes: sizeUncompressed,
		CompressedBytes:   sizeCompressed,
	}
	c.n++
}

// MergeWith updates the receiver stats to include the other stats.
func (c *CompressionStats) MergeWith(other *CompressionStats) {
	for i := 0; i < other.n; i++ {
		c.add(other.buf[i].Setting, other.buf[i].UncompressedBytes, other.buf[i].CompressedBytes)
	}
}

// All returns an iterator over the collected stats, in arbitrary order.
func (c CompressionStats) All() iter.Seq[CompressionStatsForSetting] {
	return func(yield func(cs CompressionStatsForSetting) bool) {
		for i := 0; i < c.n; i++ {
			if !yield(c.buf[i]) {
				return
			}
		}
	}
}

// String returns a string representation of the stats, in the format:
// "<setting1>:<compressed1>/<uncompressed1>,<setting2>:<compressed2>/<uncompressed2>,..."
func (c CompressionStats) String() string {
	var buf strings.Builder
	buf.Grow(c.n * 64)
	for i := 0; i < c.n; i++ {
		if i > 0 {
			buf.WriteString(",")
		}
		fmt.Fprintf(&buf, "%s:%d/%d", c.buf[i].Setting.String(), c.buf[i].CompressedBytes, c.buf[i].UncompressedBytes)
	}
	return buf.String()
}
