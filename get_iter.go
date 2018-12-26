// Copyright 2018 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package pebble

import (
	"github.com/petermattis/pebble/db"
	"github.com/petermattis/pebble/internal/rangedel"
)

// getIter is an internal iterator used to perform gets. It iterates through
// the values for a particular key, level by level. It is not a general purpose
// internalIterator, but specialized for Get operations so that it loads data
// lazily.
type getIter struct {
	cmp          db.Compare
	equal        db.Equal
	newIters     tableNewIters
	snapshot     uint64
	key          []byte
	iter         internalIterator
	rangeDelIter internalIterator
	tombstone    rangedel.Tombstone
	levelIter    levelIter
	level        int
	batch        *Batch
	mem          []flushable
	l0           []fileMetadata
	version      *version
	valid        bool
	err          error
}

// getIter implements the internalIterator interface.
var _ internalIterator = (*getIter)(nil)

func (g *getIter) SeekGE(key []byte) bool {
	panic("pebble: SeekGE unimplemented")
}

func (g *getIter) SeekLT(key []byte) bool {
	panic("pebble: SeekLT unimplemented")
}

func (g *getIter) First() bool {
	g.valid = g.Next()
	return g.valid
}

func (g *getIter) Last() bool {
	panic("pebble: Last unimplemented")
}

func (g *getIter) Next() bool {
	if g.iter != nil {
		g.valid = g.iter.Next()
	}

	for {
		if g.iter != nil {
			// We have to check rangeDelIter on each iteration because a single
			// user-key can be spread across multiple tables in a level. A range
			// tombstone will appear in the table corresponding to its start
			// key. Every call to levelIter.Next() potentially switches to a new
			// table and thus reinitializes rangeDelIter.
			if g.rangeDelIter != nil {
				g.tombstone = rangedel.Get(g.cmp, g.rangeDelIter, g.key, g.snapshot)
				if g.err = g.rangeDelIter.Close(); g.err != nil {
					return false
				}
				g.rangeDelIter = nil
			}

			if g.valid {
				key := g.iter.Key()
				if g.tombstone.Deletes(key.SeqNum()) {
					// We have a range tombstone covering this key. Rather than return a
					// point or range deletion here, we return false and close our
					// internal iterator which will make Valid() return false,
					// effectively stopping iteration.
					g.err = g.iter.Close()
					g.iter = nil
					return false
				}
				if g.equal(g.key, key.UserKey) {
					if !key.Visible(g.snapshot) {
						g.valid = g.iter.Next()
						continue
					}
					return true
				}
			}
			// We've advanced the iterator passed the desired key. Move on to the
			// next memtable / level.
			g.err = g.iter.Close()
			g.iter = nil
			if g.err != nil {
				return false
			}
		}

		// Create an iterator from the batch.
		if g.batch != nil {
			g.iter = g.batch.newInternalIter(nil)
			g.rangeDelIter = g.batch.newRangeDelIter(nil)
			g.batch = nil
			g.valid = g.iter.SeekGE(g.key)
			continue
		}

		// If we have a tombstone from a previous level it is guaranteed to delete
		// keys in lower levels.
		if !g.tombstone.Empty() {
			return false
		}

		// Create iterators from memtables from newest to oldest.
		if n := len(g.mem); n > 0 {
			m := g.mem[n-1]
			g.iter = m.newIter(nil)
			g.rangeDelIter = m.newRangeDelIter(nil)
			g.mem = g.mem[:n-1]
			g.valid = g.iter.SeekGE(g.key)
			continue
		}

		if g.level == 0 {
			// Create iterators from L0 from newest to oldest.
			if n := len(g.l0); n > 0 {
				l := &g.l0[n-1]
				g.iter, g.rangeDelIter, g.err = g.newIters(l, nil /* iter options */)
				if g.err != nil {
					return false
				}
				g.l0 = g.l0[:n-1]
				g.valid = g.iter.SeekGE(g.key)
				continue
			}
			g.level++
		}

		if g.level >= numLevels {
			return false
		}
		if len(g.version.files[g.level]) == 0 {
			g.level++
			continue
		}

		g.levelIter.init(nil, g.cmp, g.newIters, g.version.files[g.level])
		g.levelIter.initRangeDel(&g.rangeDelIter)
		g.level++
		g.iter = &g.levelIter
		g.valid = g.iter.SeekGE(g.key)
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
	return g.valid && g.err == nil
}

func (g *getIter) Error() error {
	return g.err
}

func (g *getIter) Close() error {
	if g.iter != nil {
		if err := g.iter.Close(); err != nil && g.err == nil {
			g.err = err
		}
		g.iter = nil
	}
	return g.err
}
