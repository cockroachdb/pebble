// Copyright 2011 The LevelDB-Go Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package pebble // import "github.com/petermattis/pebble"

import (
	"sync/atomic"

	"github.com/petermattis/pebble/arenaskl"
	"github.com/petermattis/pebble/db"
)

func memTableEntrySize(keyBytes, valueBytes int) uint32 {
	return arenaskl.MaxNodeSize(uint32(keyBytes)+8, uint32(valueBytes))
}

// memTable is a memory-backed implementation of the db.Reader interface.
//
// It is safe to call Get, Set, and Find concurrently.
//
// A memTable's memory consumption increases monotonically, even if keys are
// deleted or values are updated with shorter slices. Users are responsible for
// explicitly compacting a memTable into a separate DB (whether in-memory or
// on-disk) when appropriate.
type memTable struct {
	cmp       db.Compare
	skl       arenaskl.Skiplist
	emptySize uint32
	reserved  uint32
	refs      int32
}

// newMemTable returns a new MemTable.
func newMemTable(o *db.Options) *memTable {
	m := &memTable{
		cmp:  o.GetComparer().Compare,
		refs: 1,
	}
	arena := arenaskl.NewArena(4<<20 /* 4 MiB */, 0)
	m.skl.Reset(arena, m.cmp)
	m.emptySize = m.skl.Size()
	return m
}

// Get gets the value for the given key. It returns ErrNotFound if the DB does
// not contain the key.
func (m *memTable) get(key []byte) (value []byte, err error) {
	it := m.skl.NewIter()
	it.SeekGE(key)
	if !it.Valid() {
		return nil, db.ErrNotFound
	}
	ikey := it.Key()
	if m.cmp(key, ikey.UserKey) != 0 {
		return nil, db.ErrNotFound
	}
	if ikey.Kind() == db.InternalKeyKindDelete {
		return nil, db.ErrNotFound
	}
	return it.Value(), nil
}

// Set sets the value for the given key. It overwrites any previous value for
// that key; a DB is not a multi-map.
func (m *memTable) set(key db.InternalKey, value []byte) error {
	// TODO(peter): how does this interact with prepare/apply?
	return m.skl.Add(key, value)
}

// Prepare reserves space for the batch in the memtable and references the
// memtable preventing it from being deleted until the batch is applied. Note
// that prepare is not thread-safe, while apply is.
func (m *memTable) prepare(batch *Batch) error {
	a := m.skl.Arena()
	if atomic.LoadInt32(&m.refs) == 1 {
		// If there are no other concurrent apply operations, we can update the
		// reserved bytes setting to accurately reflect how many bytes of been
		// allocated vs the over-estimation present in memTableEntrySize.
		m.reserved = a.Size()
	}

	avail := a.Capacity() - m.reserved
	if batch.memTableSize > avail {
		return arenaskl.ErrArenaFull
	}
	m.reserved += batch.memTableSize

	atomic.AddInt32(&m.refs, 1)
	return nil
}

func (m *memTable) apply(batch *Batch, seqNum uint64) error {
	startSeqNum := seqNum
	for iter := batch.iter(); ; seqNum++ {
		kind, ukey, value, ok := iter.next()
		if !ok {
			break
		}
		if err := m.skl.Add(db.MakeInternalKey(ukey, seqNum, kind), value); err != nil {
			return err
		}
	}
	if seqNum != startSeqNum+uint64(batch.count()) {
		panic("pebble: inconsistent batch count")
	}
	switch v := atomic.AddInt32(&m.refs, -1); {
	case v < 0:
		panic("pebble: inconsistent reference count")
	case v == 0:
		// TODO(peter): flush the batch
	}
	return nil
}

// NewIter returns an iterator that is unpositioned (Iterator.Valid() will
// return false). The iterator can be positioned via a call to SeekGE,
// SeekLT, First or Last.
func (m *memTable) NewIter(o *db.ReadOptions) db.InternalIterator {
	return &memTableIter{
		cmp:  m.cmp,
		iter: m.skl.NewIter(),
	}
}

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
	cmp       db.Compare
	reverse   bool
	iter      arenaskl.Iterator
	prevStart arenaskl.Iterator
	prevEnd   arenaskl.Iterator
}

// memTableIter implements the db.InternalIterator interface.
var _ db.InternalIterator = (*memTableIter)(nil)

func (t *memTableIter) clearPrevCache() {
	if t.reverse {
		t.reverse = false
		t.prevStart = arenaskl.Iterator{}
		t.prevEnd = arenaskl.Iterator{}
	}
}

func (t *memTableIter) initPrevStart(key db.InternalKey) {
	t.reverse = true
	t.prevStart = t.iter
	for {
		iter := t.prevStart
		if !iter.Prev() {
			break
		}
		prevKey := iter.Key()
		if t.cmp(prevKey.UserKey, key.UserKey) != 0 {
			break
		}
		t.prevStart = iter
	}
}

func (t *memTableIter) initPrevEnd(key db.InternalKey) {
	t.prevEnd = t.iter
	for {
		iter := t.prevEnd
		if !iter.Next() {
			break
		}
		nextKey := iter.Key()
		if t.cmp(nextKey.UserKey, key.UserKey) != 0 {
			break
		}
		t.prevEnd = iter
	}
}

func (t *memTableIter) SeekGE(key []byte) {
	t.clearPrevCache()
	t.iter.SeekGE(key)
}

func (t *memTableIter) SeekLT(key []byte) {
	t.clearPrevCache()
	t.iter.SeekLT(key)
	if t.iter.Valid() {
		key := t.iter.Key()
		t.initPrevStart(key)
		t.initPrevEnd(key)
		t.iter = t.prevStart
	}
}

func (t *memTableIter) First() {
	t.clearPrevCache()
	t.iter.First()
}

func (t *memTableIter) Last() {
	t.clearPrevCache()
	t.iter.Last()
	if t.iter.Valid() {
		key := t.iter.Key()
		t.initPrevStart(key)
		t.prevEnd = t.iter
		t.iter = t.prevStart
	}
}

func (t *memTableIter) Next() bool {
	t.clearPrevCache()
	return t.iter.Next()
}

func (t *memTableIter) NextUserKey() bool {
	t.clearPrevCache()
	if t.iter.Tail() {
		return false
	}
	if t.iter.Head() {
		t.iter.First()
		return t.iter.Valid()
	}
	key := t.iter.Key()
	for t.iter.Next() {
		if t.cmp(key.UserKey, t.Key().UserKey) < 0 {
			return true
		}
	}
	return false
}

func (t *memTableIter) Prev() bool {
	// Reverse iteration is a bit funky in that it returns entries for identical
	// user-keys from larger to smaller sequence number even though they are not
	// stored that way in the skiplist. For example, the following shows the
	// ordering of keys in the skiplist:
	//
	//   a:2 a:1 b:2 b:1 c:2 c:1
	//
	// With reverse iteration we return them in the following order:
	//
	//   c:2 c:1 b:2 b:1 a:2 a:1
	//
	// This is accomplished via a bit of fancy footwork: if the iterator is
	// currently at a valid entry, see if the user-key for the next entry is the
	// same and if it is advance. Otherwise, move to the previous user key.
	//
	// Note that this makes reverse iteration a bit more expensive than forward
	// iteration, especially if there are a larger number of versions for a key
	// in the memtable, though that should be rare. In the normal case where
	// there is a single version for each key, reverse iteration consumes an
	// extra dereference and comparison.
	if t.iter.Head() {
		return false
	}
	if t.iter.Tail() {
		return t.PrevUserKey()
	}
	if !t.reverse {
		key := t.iter.Key()
		t.initPrevStart(key)
		t.initPrevEnd(key)
	}
	if t.iter != t.prevEnd {
		t.iter.Next()
		if !t.iter.Valid() {
			panic("expected valid node")
		}
		return true
	}
	t.iter = t.prevStart
	if !t.iter.Prev() {
		t.clearPrevCache()
		return false
	}
	t.prevEnd = t.iter
	t.initPrevStart(t.iter.Key())
	t.iter = t.prevStart
	return true
}

func (t *memTableIter) PrevUserKey() bool {
	if t.iter.Head() {
		return false
	}
	if t.iter.Tail() {
		t.Last()
		return t.iter.Valid()
	}
	if !t.reverse {
		t.initPrevStart(t.iter.Key())
	}
	t.iter = t.prevStart
	if !t.iter.Prev() {
		t.clearPrevCache()
		return false
	}
	t.prevEnd = t.iter
	t.initPrevStart(t.iter.Key())
	t.iter = t.prevStart
	return true
}

func (t *memTableIter) Key() db.InternalKey {
	return t.iter.Key()
}

func (t *memTableIter) Value() []byte {
	return t.iter.Value()
}

func (t *memTableIter) Valid() bool {
	return t.iter.Valid()
}

func (t *memTableIter) Error() error {
	return nil
}

func (t *memTableIter) Close() error {
	return t.iter.Close()
}
