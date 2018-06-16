// Copyright 2011 The LevelDB-Go Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package pebble // import "github.com/petermattis/pebble"

import (
	"github.com/petermattis/pebble/arenaskl"
	"github.com/petermattis/pebble/db"
)

// memTable is a memory-backed implementation of the db.Reader interface.
//
// It is safe to call Get, Set, and Find concurrently.
//
// A memTable's memory consumption increases monotonically, even if keys are
// deleted or values are updated with shorter slices. Users are responsible for
// explicitly compacting a memTable into a separate DB (whether in-memory or
// on-disk) when appropriate.
type memTable struct {
	skl       arenaskl.Skiplist
	emptySize uint32
}

// memTable implements the db.Reader interface.
var _ db.Reader = (*memTable)(nil)

// newMemTable returns a new MemTable.
func newMemTable(o *db.Options) *memTable {
	m := &memTable{}
	arena := arenaskl.NewArena(4 << 20 /* 4 MiB */)
	m.skl.Reset(arena, o.GetComparer().Compare)
	m.emptySize = m.skl.Size()
	return m
}

// Get implements Reader.Get, as documented in the pebble/db package.
func (m *memTable) Get(key []byte, o *db.ReadOptions) (value []byte, err error) {
	it := m.skl.NewIter()
	if !it.SeekGE(key) {
		return nil, db.ErrNotFound
	}
	return it.Value(), nil
}

// Set implements DB.Set, as documented in the pebble/db package.
func (m *memTable) Set(key, value []byte, o *db.WriteOptions) error {
	return m.skl.Add(key, value)
}

// Find implements Reader.Find, as documented in the pebble/db package.
func (m *memTable) Find(key []byte, o *db.ReadOptions) db.Iterator {
	t := m.NewIter(o)
	t.SeekGE(key)
	t.Prev()
	return t
}

// NewIter implements Reader.NewIter, as documented in the pebble/db package.
func (m *memTable) NewIter(o *db.ReadOptions) db.Iterator {
	return &memTableIter{
		Iterator: m.skl.NewIter(),
	}
}

// Close implements Reader.Close, as documented in the pebble/db package.
func (m *memTable) Close() error {
	return nil
}

// ApproximateMemoryUsage returns the approximate memory usage of the MemTable.
func (m *memTable) ApproximateMemoryUsage() int {
	return int(m.skl.Size())
}

// Empty returns whether the MemTable has no key/value pairs.
func (m *memTable) Empty() bool {
	return m.skl.Size() == m.emptySize
}

// memTableIter is a MemTable memTableIter that buffers upcoming results, so
// that it does not have to acquire the MemTable's mutex on each Next call.
type memTableIter struct {
	arenaskl.Iterator
}

// memTableIter implements the db.Iterator interface.
var _ db.Iterator = (*memTableIter)(nil)
