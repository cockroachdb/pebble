// Copyright 2025 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package cache

import (
	"fmt"
	"iter"

	"github.com/cockroachdb/crlib/crtime"
	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/pebble/internal/invariants"
)

// Level is the LSM level associated with an accessed block. Used to maintain
// granular cache hit/miss statistics.
//
// The zero value indicates that there is no level (e.g. flushable ingests) or
// it is unknown.
type Level struct {
	levelPlusOne int8
}

func (l Level) String() string {
	if l.levelPlusOne <= 0 {
		return "n/a"
	}
	return fmt.Sprintf("L%d", l.levelPlusOne-1)
}

// index returns a value between [0, NumLevels).
func (l Level) index() int8 {
	return l.levelPlusOne
}

func MakeLevel(l int) Level {
	if invariants.Enabled && (l < 0 || l >= NumLevels-1) {
		panic(errors.AssertionFailedf("invalid level: %d", l))
	}
	return Level{levelPlusOne: int8(l + 1)}
}

const NumLevels = 1 /* unknown level */ + 7

// Levels is an iter.Seq[Level].
func Levels(yield func(l Level) bool) {
	for i := range NumLevels {
		if !yield(Level{levelPlusOne: int8(i)}) {
			return
		}
	}
}

var _ iter.Seq[Level] = Levels

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
type HitsAndMisses [NumLevels][NumCategories]struct {
	Hits   int64
	Misses int64
}

func (hm *HitsAndMisses) Get(level Level, category Category) (hits, misses int64) {
	v := hm[level.index()][category]
	return v.Hits, v.Misses
}

func (hm *HitsAndMisses) Hits(level Level, category Category) int64 {
	return hm[level.index()][category].Hits
}

func (hm *HitsAndMisses) Misses(level Level, category Category) int64 {
	return hm[level.index()][category].Misses
}

// Aggregate returns the total hits and misses across all categories and levels.
func (hm *HitsAndMisses) Aggregate() (hits, misses int64) {
	for i := range hm {
		for j := range hm[i] {
			hits += hm[i][j].Hits
			misses += hm[i][j].Misses
		}
	}
	return hits, misses
}

// AggregateLevel returns the total hits and misses for a specific level (across
// all categories).
func (hm *HitsAndMisses) AggregateLevel(level Level) (hits, misses int64) {
	for _, v := range hm[level.index()] {
		hits += v.Hits
		misses += v.Misses
	}
	return hits, misses
}

// AggregateCategory returns the total hits and misses for a specific category
// (across all levels).
func (hm *HitsAndMisses) AggregateCategory(category Category) (hits, misses int64) {
	for i := range hm {
		hits += hm[i][category].Hits
		misses += hm[i][category].Misses
	}
	return hits, misses
}

// ToRecent changes the receiver to reflect recent hits and misses, given the
// current metrics.
// At a high level, hm.ToRecent(current) means hm = current - hm.
func (hm *HitsAndMisses) ToRecent(current *HitsAndMisses) {
	for i := range hm {
		for j := range hm[i] {
			hm[i][j].Hits = current[i][j].Hits - hm[i][j].Hits
			hm[i][j].Misses = current[i][j].Misses - hm[i][j].Misses
		}
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
			for k := range hm[j] {
				hm[j][k].Hits += shardCounters[j][k].hits.Load()
				hm[j][k].Misses += shardCounters[j][k].misses.Load()
			}
		}
	}
	return hm
}
