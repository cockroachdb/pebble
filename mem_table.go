// Copyright 2011 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package pebble

import (
	"sync"
	"sync/atomic"
	"unsafe"

	"github.com/petermattis/pebble/db"
	"github.com/petermattis/pebble/internal/arenaskl"
	"github.com/petermattis/pebble/internal/rangedel"
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
	cmp         db.Compare
	equal       db.Equal
	skl         arenaskl.Skiplist
	rangeDelSkl arenaskl.Skiplist
	emptySize   uint32
	reserved    uint32
	refs        int32
	flushedCh   chan struct{}
	tombstones  rangeTombstoneCache
}

// newMemTable returns a new MemTable.
func newMemTable(o *db.Options) *memTable {
	o = o.EnsureDefaults()
	m := &memTable{
		cmp:       o.Comparer.Compare,
		equal:     o.Comparer.Equal,
		refs:      1,
		flushedCh: make(chan struct{}),
	}
	arena := arenaskl.NewArena(uint32(o.MemTableSize), 0)
	m.skl.Reset(arena, m.cmp)
	m.rangeDelSkl.Reset(arena, m.cmp)
	m.emptySize = arena.Size()
	return m
}

func (m *memTable) ref() {
	atomic.AddInt32(&m.refs, 1)
}

func (m *memTable) unref() bool {
	switch v := atomic.AddInt32(&m.refs, -1); {
	case v < 0:
		panic("pebble: inconsistent reference count")
	case v == 0:
		return true
	default:
		return false
	}
}

func (m *memTable) flushed() chan struct{} {
	return m.flushedCh
}

func (m *memTable) readyForFlush() bool {
	return atomic.LoadInt32(&m.refs) == 0
}

// Get gets the value for the given key. It returns ErrNotFound if the DB does
// not contain the key.
func (m *memTable) get(key []byte) (value []byte, err error) {
	it := m.skl.NewIter()
	if !it.SeekGE(key) {
		return nil, db.ErrNotFound
	}
	ikey := it.Key()
	if !m.equal(key, ikey.UserKey) {
		return nil, db.ErrNotFound
	}
	if ikey.Kind() == db.InternalKeyKindDelete {
		return nil, db.ErrNotFound
	}
	return it.Value(), nil
}

// Prepare reserves space for the batch in the memtable and references the
// memtable preventing it from being flushed until the batch is applied. Note
// that prepare is not thread-safe, while apply is. The caller must call
// unref() after the batch has been applied.
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

	m.ref()
	return nil
}

func (m *memTable) apply(batch *Batch, seqNum uint64) error {
	var ins arenaskl.Inserter
	var tombstoneCount uint32
	startSeqNum := seqNum
	for iter := batch.iter(); ; seqNum++ {
		kind, ukey, value, ok := iter.next()
		if !ok {
			break
		}
		var err error
		ikey := db.MakeInternalKey(ukey, seqNum, kind)
		if kind == db.InternalKeyKindRangeDelete {
			err = m.rangeDelSkl.Add(ikey, value)
			tombstoneCount++
		} else {
			err = ins.Add(&m.skl, ikey, value)
		}
		if err != nil {
			return err
		}
	}
	if seqNum != startSeqNum+uint64(batch.count()) {
		panic("pebble: inconsistent batch count")
	}
	if tombstoneCount != 0 {
		m.tombstones.invalidate(tombstoneCount)
	}
	return nil
}

// newIter returns an iterator that is unpositioned (Iterator.Valid() will
// return false). The iterator can be positioned via a call to SeekGE,
// SeekLT, First or Last.
func (m *memTable) newIter(*db.IterOptions) internalIterator {
	it := m.skl.NewIter()
	return &it
}

func (m *memTable) newRangeDelIter(*db.IterOptions) internalIterator {
	tombstones := m.tombstones.get(m)
	if tombstones == nil {
		return nil
	}
	return rangedel.NewIter(m.cmp, tombstones)
}

func (m *memTable) close() error {
	return nil
}

// empty returns whether the MemTable has no key/value pairs.
func (m *memTable) empty() bool {
	return m.skl.Size() == m.emptySize
}

// A rangeTombstoneFrags holds a set of fragmented range tombstones generated
// at a particular "sequence number" for a memtable. Rather than use actual
// sequence numbers, this cache uses a count of the number of range tombstones
// in the memTable. Note that the count of range tombstones in a memTable only
// ever increases, which provides a monotonically increasing sequence.
type rangeTombstoneFrags struct {
	count      uint32
	once       sync.Once
	tombstones []rangedel.Tombstone
}

// get retrieves the fragmented tombstones, populating them if necessary. Note
// that the populated tombstone fragments may be built from more than f.count
// memTable range tombstones, but that is ok for correctness. All we're
// requiring is that the memTable contains at least f.count range
// tombstones. This situation can occur if there are multiple concurrent
// additions of range tombstones and a concurrent reader. The reader can load a
// tombstoneFrags and populate it even though is has been invalidated
// (i.e. replaced with a newer tombstoneFrags).
func (f *rangeTombstoneFrags) get(m *memTable) []rangedel.Tombstone {
	f.once.Do(func() {
		frag := &rangedel.Fragmenter{
			Cmp: m.cmp,
			Emit: func(fragmented []rangedel.Tombstone) {
				f.tombstones = append(f.tombstones, fragmented...)
			},
		}
		for it := m.rangeDelSkl.NewIter(); it.Next(); {
			frag.Add(it.Key(), it.Value())
		}
		frag.Finish()
	})
	return f.tombstones
}

// A rangeTombstoneCache is used to cache a set of fragmented tombstones. The
// cache is invalidated whenever a tombstone is added to a memTable, and
// populated when empty when a range-del iterator is created.
type rangeTombstoneCache struct {
	count uint32
	frags unsafe.Pointer
}

// Invalidate the current set of cached tombstones, indicating the number of
// tombstones that were added.
func (c *rangeTombstoneCache) invalidate(count uint32) {
	newCount := atomic.AddUint32(&c.count, count)
	var frags *rangeTombstoneFrags

	for {
		oldPtr := atomic.LoadPointer(&c.frags)
		if oldPtr != nil {
			oldFrags := (*rangeTombstoneFrags)(oldPtr)
			if oldFrags.count >= newCount {
				// Someone else invalidated the cache before us and their invalidation
				// subsumes ours.
				break
			}
		}
		if frags == nil {
			frags = &rangeTombstoneFrags{count: newCount}
		}
		if atomic.CompareAndSwapPointer(&c.frags, oldPtr, unsafe.Pointer(frags)) {
			// We successfully invalidated the cache.
			break
		}
		// Someone else invalidated the cache. Loop and try again.
	}
}

func (c *rangeTombstoneCache) get(m *memTable) []rangedel.Tombstone {
	frags := (*rangeTombstoneFrags)(atomic.LoadPointer(&c.frags))
	if frags == nil {
		return nil
	}
	return frags.get(m)
}
