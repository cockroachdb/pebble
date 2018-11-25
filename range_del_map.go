// Copyright 2018 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package pebble

import (
	"github.com/petermattis/pebble/db"
	"github.com/petermattis/pebble/internal/rangedel"
)

// rangeDelLevel holds the state for a single level in rangeDelMap. Levels come
// in two flavors: single-table levels and multi-table levels. Single table
// labels are initialized with a range-del iterator when the rangeDelMap is
// created. Multi-table levels are connected to levelIter and are lazily
// populated as tables are loaded.
type rangeDelLevel struct {
	m    *rangeDelMap
	iter internalIterator
}

func (l *rangeDelLevel) init(iter internalIterator) {
	l.iter = iter
}

// load the range-del iterator for the specified table.
func (l *rangeDelLevel) load(meta *fileMetadata) error {
	var err error
	l.iter, err = l.m.newIter(meta)
	return err
}

// rangeDelMap provides a merged view of the range tombstones from multiple
// levels. The map is composed of a series of levels, mirroring the levels in
// the LSM tree, though L0 is exploded into a level per table, and each
// memtable is on its own level.
type rangeDelMap struct {
	// The sequence number at which reads are being performed. Tombstones that
	// are newer than this sequence number are ignored.
	seqNum uint64
	// The callback for creating new range-del iterators.
	newIter tableNewIter
	levels  []rangeDelLevel
}

func (m *rangeDelMap) init(seqNum uint64, newIter tableNewIter) {
	m.seqNum = seqNum
	m.newIter = newIter
}

func (m *rangeDelMap) addLevel(iter internalIterator) {
	m.levels = append(m.levels, rangeDelLevel{
		m:    m,
		iter: iter,
	})
}

func (m *rangeDelMap) addLevels(n int) []rangeDelLevel {
	for i := 0; i < n; i++ {
		m.levels = append(m.levels, rangeDelLevel{
			m: m,
		})
	}
	return m.levels[len(m.levels)-n:]
}

// ClearCache clears any cached tombstone information so that the next call to
// Deleted or Get will have to repopulate the cache. This is useful when the
// next call to Deleted or Get is expected to not hit the cache due to a Seek
// being performed on the iterator using this rangeDelMap.
func (m *rangeDelMap) ClearCache() {
}

// Deleted returns true if the specified key is covered by a newer range
// tombstone.
func (m *rangeDelMap) Deleted(key db.InternalKey) bool {
	return m.Get(key).Start.SeqNum() >= key.SeqNum()
}

// Get the range tombstone at the specified key. A tombstone is always
// returned, though it may cover an empty range of keys or the sequence number
// may be 0 to indicate that no tombstone covers the specified key.
func (m *rangeDelMap) Get(key db.InternalKey) rangedel.Tombstone {
	return rangedel.Tombstone{}
}
