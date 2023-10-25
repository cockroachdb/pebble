// Copyright 2018 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package sstable

import (
	"context"
	"sync"
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
	LatencySensitiveQoSLevel QoSLevel = iota
	NonLatencySensitiveQosLevel
)

type categoryKey struct{}
type qosLevelKey struct{}

func ContextWithCategory(
	ctx context.Context, category Category, qosLevel QoSLevel,
) context.Context {
	// Doing two allocations is not ideal, but we can't simply wrap ctx in our
	// custom context.Context implementation since it may get wrapped again and
	// then we will not have the ability to extract these values.
	ctx = context.WithValue(ctx, categoryKey{}, category)
	if qosLevel != LatencySensitiveQoSLevel {
		ctx = context.WithValue(ctx, qosLevelKey{}, qosLevel)
	}
	return ctx
}
func CategoryFromContext(ctx context.Context) (category Category, qosLevel QoSLevel) {
	val := ctx.Value(categoryKey{})
	if val != nil {
		category, _ = val.(Category)
	}
	qosLevel = LatencySensitiveQoSLevel
	val = ctx.Value(qosLevelKey{})
	if val != nil {
		level, ok := val.(QoSLevel)
		if ok {
			qosLevel = level
		}
	}
	return category, qosLevel
}

// CategoryStats provides stats about a category of reads.
type CategoryStats struct {
	// Bytes in the loaded blocks. If the block was compressed, this is the
	// compressed bytes. Currently, only the index blocks, data blocks
	// containing points, and filter blocks are included. Additionally, value
	// blocks read after the corresponding iterator is closed are not included.
	BlockBytes uint64
	// Subset of BlockBytes that were in the block cache.
	BlockBytesInCache uint64
}

func (s *CategoryStats) aggregate(a CategoryStats) {
	s.BlockBytes += a.BlockBytes
	s.BlockBytesInCache += a.BlockBytesInCache
}

type categoryStatsWithMu struct {
	mu sync.Mutex
	// Protected by mu.
	stats CategoryStatsAggregate
}

type CategoryStatsAggregate struct {
	Category
	QoSLevel
	CategoryStats
}
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

func (c *CategoryStatsCollector) GetStats() []CategoryStatsAggregate {
	var stats []CategoryStatsAggregate
	c.statsMap.Range(func(_, v any) bool {
		aggStats := v.(*categoryStatsWithMu)
		aggStats.mu.Lock()
		s := aggStats.stats
		aggStats.mu.Unlock()
		stats = append(stats, s)
		return true
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

func (accum *iterStatsAccumulator) init(ctx context.Context, collector *CategoryStatsCollector) {
	accum.Category, accum.QoSLevel = CategoryFromContext(ctx)
	accum.collector = collector
}

func (accum *iterStatsAccumulator) reportStats(blockBytes, blockBytesInCache uint64) {
	accum.stats.BlockBytes += blockBytes
	accum.stats.BlockBytesInCache += blockBytesInCache
}

func (accum *iterStatsAccumulator) close() {
	if accum.collector != nil {
		accum.collector.reportStats(accum.Category, accum.QoSLevel, accum.stats)
	}
}
