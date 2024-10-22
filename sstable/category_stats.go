// Copyright 2018 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package sstable

import (
	"cmp"
	"slices"
	"sync"
	"time"
	"unsafe"

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

const numCategoryStatsShards = 16

type categoryStatsWithMu struct {
	mu sync.Mutex
	// Protected by mu.
	stats CategoryStats
}

// Accumulate implements the IterStatsAccumulator interface.
func (c *categoryStatsWithMu) Accumulate(stats CategoryStats) {
	c.mu.Lock()
	c.stats.aggregate(stats)
	c.mu.Unlock()
}

// CategoryStatsCollector collects and aggregates the stats per category.
type CategoryStatsCollector struct {
	// mu protects additions to statsMap.
	mu sync.Mutex
	// Category => *shardedCategoryStats.
	statsMap sync.Map
}

// shardedCategoryStats accumulates stats for a category, splitting its stats
// across multiple shards to prevent mutex contention. In high-read workloads,
// contention on the category stats mutex has been observed.
type shardedCategoryStats struct {
	Category Category
	QoSLevel QoSLevel
	shards   [numCategoryStatsShards]struct {
		categoryStatsWithMu
		// Pad each shard to 64 bytes so they don't share a cache line.
		_ [64 - unsafe.Sizeof(categoryStatsWithMu{})]byte
	}
}

// getStats retrieves the aggregated stats for the category, summing across all
// shards.
func (s *shardedCategoryStats) getStats() CategoryStatsAggregate {
	agg := CategoryStatsAggregate{
		Category: s.Category,
		QoSLevel: s.QoSLevel,
	}
	for i := range s.shards {
		s.shards[i].mu.Lock()
		agg.CategoryStats.aggregate(s.shards[i].stats)
		s.shards[i].mu.Unlock()
	}
	return agg
}

// Accumulator returns a stats accumulator for the given category. The provided
// p is used to detrmine which shard to write stats to.
func (c *CategoryStatsCollector) Accumulator(p uint64, caq CategoryAndQoS) IterStatsAccumulator {
	v, ok := c.statsMap.Load(caq.Category)
	if !ok {
		c.mu.Lock()
		v, _ = c.statsMap.LoadOrStore(caq.Category, &shardedCategoryStats{
			Category: caq.Category,
			QoSLevel: caq.QoSLevel,
		})
		c.mu.Unlock()
	}
	s := v.(*shardedCategoryStats)
	// This equation is taken from:
	// https://en.wikipedia.org/wiki/Linear_congruential_generator#Parameters_in_common_use
	shard := ((p * 25214903917) >> 32) & (numCategoryStatsShards - 1)
	return &s.shards[shard].categoryStatsWithMu
}

// GetStats returns the aggregated stats.
func (c *CategoryStatsCollector) GetStats() []CategoryStatsAggregate {
	var stats []CategoryStatsAggregate
	c.statsMap.Range(func(_, v any) bool {
		s := v.(*shardedCategoryStats).getStats()
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

type IterStatsAccumulator interface {
	// Accumulate accumulates the provided stats.
	Accumulate(cas CategoryStats)
}

// iterStatsAccumulator is a helper for a sstable iterator to accumulate stats,
// which are reported to the CategoryStatsCollector when the accumulator is
// closed.
type iterStatsAccumulator struct {
	stats  CategoryStats
	parent IterStatsAccumulator
}

func (a *iterStatsAccumulator) init(parent IterStatsAccumulator) {
	a.parent = parent
}

func (a *iterStatsAccumulator) reportStats(
	blockBytes, blockBytesInCache uint64, blockReadDuration time.Duration,
) {
	a.stats.BlockBytes += blockBytes
	a.stats.BlockBytesInCache += blockBytesInCache
	a.stats.BlockReadDuration += blockReadDuration
}

func (a *iterStatsAccumulator) close() {
	if a.parent != nil {
		a.parent.Accumulate(a.stats)
	}
}
