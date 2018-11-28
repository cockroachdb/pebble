// Copyright 2018 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package pebble

import (
	"github.com/petermattis/pebble/db"
)

// getIter is an internal iterator used to perform gets. It iterates through
// the values for a particular key, level by level. It is not a general purpose
// internalIterator, but specialized for Get operations so that it loads data
// lazily.
type getIter struct {
	cmp             db.Compare
	newIter         tableNewIter
	newRangeDelIter tableNewIter
	key             []byte
	iter            internalIterator
	rangeDelIter    internalIterator
	levelIter       levelIter
	level           int
	mem             []flushable
	l0              []fileMetadata
	version         *version
	err             error
}

// getIter implements the internalIterator interface.
var _ internalIterator = (*getIter)(nil)

func (g *getIter) SeekGE(key []byte) {
	panic("pebble: SeekGE unimplemented")
}

func (g *getIter) SeekLT(key []byte) {
	panic("pebble: SeekLT unimplemented")
}

func (g *getIter) First() {
	g.Next()
}

func (g *getIter) Last() {
	panic("pebble: Last unimplemented")
}

func (g *getIter) Next() bool {
	if g.iter != nil {
		g.iter.Next()
	}

	// TODO(peter,rangedel): add range-del checks.

	for {
		if g.iter != nil {
			if g.iter.Valid() && g.cmp(g.key, g.iter.Key().UserKey) == 0 {
				return true
			}
			// We've advanced the iterator passed the desired key. Move on to the
			// next memtable / level.
			g.err = g.iter.Close()
			g.iter = nil
			if g.err != nil {
				return false
			}
		}

		// Create iterators from memtables from newest to oldest.
		if n := len(g.mem); n > 0 {
			g.iter = g.mem[n-1].newIter(nil)
			g.rangeDelIter = g.mem[n-1].newRangeDelIter(nil)
			g.mem = g.mem[:n-1]
			g.iter.SeekGE(g.key)
			continue
		}

		if g.level == 0 {
			// Create iterators from L0 from newest to oldest.
			if n := len(g.l0); n > 0 {
				g.iter, g.err = g.newIter(&g.l0[n-1])
				if g.err != nil {
					return false
				}
				g.l0 = g.l0[:n-1]
				g.iter.SeekGE(g.key)
				continue
			}
			g.level++
		}

		if g.level >= numLevels {
			return false
		}

		g.levelIter.init(nil, g.cmp, g.newIter, g.version.files[g.level])
		g.levelIter.initRangeDel(g.newRangeDelIter, &g.rangeDelIter)
		g.level++
		g.iter = &g.levelIter
		g.iter.SeekGE(g.key)
	}
}

func (g *getIter) Prev() bool {
	panic("pebble: Prev unimplemented")
}

func (g *getIter) Key() db.InternalKey {
	return g.iter.Key()
}

func (g *getIter) Value() []byte {
	return g.iter.Value()
}

func (g *getIter) Valid() bool {
	return g.iter != nil
}

func (g *getIter) Error() error {
	return g.err
}

func (g *getIter) Close() error {
	var err error
	if g.iter != nil {
		err = g.iter.Close()
		g.iter = nil
	}
	return err
}
