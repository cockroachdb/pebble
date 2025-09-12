// Copyright 2025 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package cache

import (
	"fmt"

	"github.com/cockroachdb/crlib/crtime"
)

// Category is used to maintain granular cache hit/miss statistics.
type Category uint8

const (
	// CategoryBackground is used for cache accesses made by compactions or
	// downloads.
	CategoryBackground Category = iota
	CategorySSTableData
	CategorySSTableValue
	CategoryBlobValue
	CategoryFilter
	// CategoryIndex includes index blocks and other metadata blocks (for both
	// sstables and blob files).
	CategoryIndex

	// Categories can be used with the range keyword.
	Categories
)

const NumCategories = int(Categories)

func (c Category) String() string {
	switch c {
	case CategoryBackground:
		return "background"
	case CategorySSTableData:
		return "sstdata"
	case CategorySSTableValue:
		return "sstval"
	case CategoryBlobValue:
		return "blobval"
	case CategoryFilter:
		return "filter"
	case CategoryIndex:
		return "index"
	default:
		return fmt.Sprintf("invalid(%d)", c)
	}
}

// Metrics holds metrics for the cache.
type Metrics struct {
	// Hits and misse since the cache was created.
	HitsAndMisses HitsAndMisses
	// The current number of bytes inuse by the cache.
	Size int64
	// The current count of objects (blocks or tables) in the cache.
	Count int64
	// Recent contains cache hit metrics covering two recent periods (last ~10
	// minutes and last ~1 hour).
	Recent [2]struct {
		HitsAndMisses
		Since crtime.Mono
	}
}

// HitsAndMisses contains the number of cache hits and misses across a period of
// time.
type HitsAndMisses [NumCategories]struct {
	Hits   int64
	Misses int64
}

// Aggregate returns the total hits and misses across all categories.
func (hm *HitsAndMisses) Aggregate() (hits, misses int64) {
	for i := range *hm {
		hits += hm[i].Hits
		misses += hm[i].Misses
	}
	return hits, misses
}

// ToRecent changes the receiver to reflect recent hits and misses, given the
// current metrics.
// At a high level, hm.ToRecent(current) means hm = current - hm.
func (hm *HitsAndMisses) ToRecent(current *HitsAndMisses) {
	for i := range *hm {
		hm[i].Hits = current[i].Hits - hm[i].Hits
		hm[i].Misses = current[i].Misses - hm[i].Misses
	}
}

// Metrics returns the current metrics for the cache.
func (c *Cache) Metrics() Metrics {
	var m Metrics
	m.HitsAndMisses = c.hitsAndMisses()
	for i := range c.shards {
		s := &c.shards[i]
		s.mu.RLock()
		m.Count += int64(s.blocks.Len())
		m.Size += s.sizeHot + s.sizeCold
		s.mu.RUnlock()
	}
	m.Recent[0].HitsAndMisses, m.Recent[0].Since = c.metricsWindow.TenMinutesAgo()
	m.Recent[1].HitsAndMisses, m.Recent[1].Since = c.metricsWindow.OneHourAgo()
	for i := range m.Recent {
		m.Recent[i].ToRecent(&m.HitsAndMisses)
	}
	return m
}

func (c *Cache) hitsAndMisses() HitsAndMisses {
	var hm HitsAndMisses
	for i := range c.shards {
		shardCounters := &c.shards[i].counters
		for j := range hm {
			hm[j].Hits += shardCounters[j].hits.Load()
			hm[j].Misses += shardCounters[j].misses.Load()
		}
	}
	return hm
}
