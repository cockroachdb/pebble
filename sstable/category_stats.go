// Copyright 2018 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package sstable

import (
	"cmp"
	"slices"
	"sync"
	"time"

	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/redact"
)

// Category is a user-understandable string, where stats are aggregated for
// each category. The cardinality of this should be low, say < 20. The prefix
// "pebble-" is reserved for internal Pebble categories.
//
// Examples of categories that can be useful in the CockroachDB context are:
// sql-user, sql-stats, raft, rangefeed, mvcc-gc, range-snapshot.
type Category string

// QoSLevel describes whether the read is latency-sensitive or not. Each
// category must map to a single QoSLevel. While category strings are opaque
// to Pebble, the QoSLevel may be internally utilized in Pebble to better
// optimize future reads.
type QoSLevel int

const (
	// LatencySensitiveQoSLevel is the default when QoSLevel is not specified,
	// and represents reads that are latency-sensitive.
	LatencySensitiveQoSLevel QoSLevel = iota
	// NonLatencySensitiveQoSLevel represents reads that are not
	// latency-sensitive.
	NonLatencySensitiveQoSLevel
)

// SafeFormat implements the redact.SafeFormatter interface.
func (q QoSLevel) SafeFormat(p redact.SafePrinter, verb rune) {
	switch q {
	case LatencySensitiveQoSLevel:
		p.Printf("latency")
	case NonLatencySensitiveQoSLevel:
		p.Printf("non-latency")
	default:
		p.Printf("<unknown-qos>")
	}
}

// StringToQoSForTesting returns the QoSLevel for the string, or panics if the
// string is not known.
func StringToQoSForTesting(s string) QoSLevel {
	switch s {
	case "latency":
		return LatencySensitiveQoSLevel
	case "non-latency":
		return NonLatencySensitiveQoSLevel
	}
	panic(errors.AssertionFailedf("unknown QoS %s", s))
}

// CategoryAndQoS specifies both the Category and the QoSLevel.
type CategoryAndQoS struct {
	Category Category
	QoSLevel QoSLevel
}

// CategoryStats provides stats about a category of reads.
type CategoryStats struct {
	// BlockBytes is the bytes in the loaded blocks. If the block was
	// compressed, this is the compressed bytes. Currently, only the index
	// blocks, data blocks containing points, and filter blocks are included.
	// Additionally, value blocks read after the corresponding iterator is
	// closed are not included.
	BlockBytes uint64
	// BlockBytesInCache is the subset of BlockBytes that were in the block
	// cache.
	BlockBytesInCache uint64
	// BlockReadDuration is the total duration to read the bytes not in the
	// cache, i.e., BlockBytes-BlockBytesInCache.
	BlockReadDuration time.Duration
}

func (s *CategoryStats) aggregate(a CategoryStats) {
	s.BlockBytes += a.BlockBytes
	s.BlockBytesInCache += a.BlockBytesInCache
	s.BlockReadDuration += a.BlockReadDuration
}

// CategoryStatsAggregate is the aggregate for the given category.
type CategoryStatsAggregate struct {
	Category      Category
	QoSLevel      QoSLevel
	CategoryStats CategoryStats
}

type categoryStatsWithMu struct {
	mu sync.Mutex
	// Protected by mu.
	stats CategoryStatsAggregate
}

// CategoryStatsCollector collects and aggregates the stats per category.
type CategoryStatsCollector struct {
	// mu protects additions to statsMap.
	mu sync.Mutex
	// Category => categoryStatsWithMu.
	statsMap sync.Map
}

func (c *CategoryStatsCollector) reportStats(
	category Category, qosLevel QoSLevel, stats CategoryStats,
) {
	v, ok := c.statsMap.Load(category)
	if !ok {
		c.mu.Lock()
		v, _ = c.statsMap.LoadOrStore(category, &categoryStatsWithMu{
			stats: CategoryStatsAggregate{Category: category, QoSLevel: qosLevel},
		})
		c.mu.Unlock()
	}
	aggStats := v.(*categoryStatsWithMu)
	aggStats.mu.Lock()
	aggStats.stats.CategoryStats.aggregate(stats)
	aggStats.mu.Unlock()
}

// GetStats returns the aggregated stats.
func (c *CategoryStatsCollector) GetStats() []CategoryStatsAggregate {
	var stats []CategoryStatsAggregate
	c.statsMap.Range(func(_, v any) bool {
		aggStats := v.(*categoryStatsWithMu)
		aggStats.mu.Lock()
		s := aggStats.stats
		aggStats.mu.Unlock()
		if len(s.Category) == 0 {
			s.Category = "_unknown"
		}
		stats = append(stats, s)
		return true
	})
	slices.SortFunc(stats, func(a, b CategoryStatsAggregate) int {
		return cmp.Compare(a.Category, b.Category)
	})
	return stats
}

// iterStatsAccumulator is a helper for a sstable iterator to accumulate
// stats, which are reported to the CategoryStatsCollector when the
// accumulator is closed.
type iterStatsAccumulator struct {
	Category
	QoSLevel
	stats     CategoryStats
	collector *CategoryStatsCollector
}

func (accum *iterStatsAccumulator) init(
	categoryAndQoS CategoryAndQoS, collector *CategoryStatsCollector,
) {
	accum.Category = categoryAndQoS.Category
	accum.QoSLevel = categoryAndQoS.QoSLevel
	accum.collector = collector
}

func (accum *iterStatsAccumulator) reportStats(
	blockBytes, blockBytesInCache uint64, blockReadDuration time.Duration,
) {
	accum.stats.BlockBytes += blockBytes
	accum.stats.BlockBytesInCache += blockBytesInCache
	accum.stats.BlockReadDuration += blockReadDuration
}

func (accum *iterStatsAccumulator) close() {
	if accum.collector != nil {
		accum.collector.reportStats(accum.Category, accum.QoSLevel, accum.stats)
	}
}
