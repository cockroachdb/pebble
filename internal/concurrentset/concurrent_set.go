// Copyright 2025 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package concurrentset

import (
	"runtime"
	"slices"
	"sync"
	"sync/atomic"

	"github.com/cockroachdb/pebble/internal/invariants"
)

// Set is a high-throughput, low-contention container that supports lock-free
// fast paths for Add/Remove and a locked, consistent snapshot via Append.
//
// Supports addition of elements, removal of elements by Handle returned by Add,
// and listing of elements in the set.
//
// Semantics:
//   - Add returns the Handle for the inserted element.
//   - Remove removes by Handle.
//   - Append runs under a lock and visits the consolidated map; very recent
//     adds that haven’t been reloaded yet won’t be visible until the next reload.
//
// Implementation:
//   - Producers record operations (adds/dels) into a fixed-size "magazine" using
//     atomics (no mutex).
//   - A slow path ("reload") swaps in a fresh magazine and folds the completed
//     one into a centralized map.
//   - Append forces a reload under a lock to iterate a consistent snapshot.
type Set[T any] struct {
	mu struct {
		sync.Mutex
		m               map[Handle]T
		recent          atomic.Pointer[magazine[T]]
		nextStartHandle Handle
		magAlloc        []magazine[T]
	}
}

// Handle is a stable identifier for an element in the Set. It is monotonically
// increasing with each Add; two different Add operations on the same set will
// never return the same Handle.
type Handle uint64

// New creates a new empty Set.
func New[T any]() *Set[T] {
	s := &Set[T]{}
	s.mu.m = make(map[Handle]T)

	s.mu.magAlloc = make([]magazine[T], magAllocBatch)

	mag := &s.mu.magAlloc[0]
	mag.addsStartHandle = 1
	s.mu.magAlloc = s.mu.magAlloc[1:]
	s.mu.recent.Store(mag)

	s.mu.nextStartHandle = 1 + magazineSize
	return s
}

// Add inserts t and returns a Handle.
func (s *Set[T]) Add(t T) Handle {
	mag := s.mu.recent.Load()
	if idx := mag.TryAdd(t); idx != -1 {
		// Fast path.
		return mag.addsStartHandle + Handle(idx)
	}
	// Slow path.
	s.mu.Lock()
	defer s.mu.Unlock()

	for {
		mag = s.mu.recent.Load()
		if idx := mag.TryAdd(t); idx != -1 {
			return mag.addsStartHandle + Handle(idx)
		}
		s.reloadLocked()
	}
}

// Remove removes the element identified by h. The element must exist in the
// set.
func (s *Set[T]) Remove(h Handle) {
	if mag := s.mu.recent.Load(); mag.TryDel(h) {
		// Fast path.
		return
	}
	// Slow path.
	s.mu.Lock()
	defer s.mu.Unlock()

	for {
		if mag := s.mu.recent.Load(); mag.TryDel(h) {
			return
		}
		s.reloadLocked()
	}
}

const magAllocBatch = 16

// reloadLocked installs a fresh magazine and folds the finished magazine into
// the centralized map, applying deletes before adds.
func (s *Set[T]) reloadLocked() {
	if len(s.mu.magAlloc) == 0 {
		s.mu.magAlloc = make([]magazine[T], magAllocBatch)
	}
	newMag := &s.mu.magAlloc[0]
	s.mu.magAlloc = s.mu.magAlloc[1:]
	newMag.addsStartHandle = s.mu.nextStartHandle
	s.mu.nextStartHandle += magazineSize

	// Note: as soon as we swap in the new magazine, other goroutines can start
	// adding to it immediately.
	mag := s.mu.recent.Swap(newMag)
	nAdds, nDels := mag.Finish()

	for _, h := range mag.dels[:nDels] {
		if invariants.Enabled {
			if _, ok := s.mu.m[h]; !ok {
				panic("delete of unknown handle")
			}
		}
		delete(s.mu.m, h)
	}

	for i := range mag.adds[:nAdds] {
		if mag.adds[i].deleted.Load() {
			continue
		}
		h := mag.addsStartHandle + Handle(i)
		if invariants.Enabled {
			if _, ok := s.mu.m[h]; ok {
				panic("add of existing handle")
			}
		}
		s.mu.m[h] = mag.adds[i].t
	}
}

// AppendAll appends all elements in the set to the given slice, in arbitrary
// order.
func (s *Set[T]) AppendAll(dest []T) []T {
	s.mu.Lock()
	defer s.mu.Unlock()
	// Swap in a new magazine. This will give us the latest "view"  in s.mu.m and
	// also allow other goroutines to make as much progress as possible without
	// locking.
	s.reloadLocked()
	dest = slices.Grow(dest, len(s.mu.m))
	for _, t := range s.mu.m {
		dest = append(dest, t)
	}
	return dest
}

// AppendFiltered runs the filter over a consistent snapshot of the set and
// appends matching elements to dest (in arbitrary order). The filter must be
// very fast; the mutex is held for the entire iteration.
func (s *Set[T]) AppendFiltered(filter func(t T) bool, dest []T) []T {
	s.mu.Lock()
	defer s.mu.Unlock()
	// Swap in a new magazine. This will give us the latest "view"  in s.mu.m and
	// also allow other goroutines to make as much progress as possible without
	// locking.
	s.reloadLocked()
	for _, t := range s.mu.m {
		if filter(t) {
			dest = append(dest, t)
		}
	}
	return dest
}

// --- Magazine internals ---

const magazineSize = 128

// A magazine can hold up to magazineSize new elements. These elements map to a
// contiguous range of Handles. A magazine can also hold up to magazineSize
// Handles for deletions of objects that correspond to previous magazines.
//
// A magazine can be Finish()ed, at which point no further adds or deletes are
// possible.
type magazine[T any] struct {
	// adds[0] through adds[min(addsReserved,magazineSize)-1] store added elements.
	// Only addsInitialized elements out of these have actually been set, the rest
	// are in the process of being written.
	// Note that instead of using CAS to reserve a slot, we simply do an atomic
	// add; as such, addsReserved can exceed magazineSize in which case it should
	// be interpreted as equal to magazineSize.
	addsReserved    atomic.Uint32
	addsInitialized atomic.Uint32
	addsStartHandle Handle
	adds            [magazineSize]struct {
		t       T
		deleted atomic.Bool
	}
	// inMagDeletes counts the number of deletes of handles in this magazine.
	inMagDeletes atomic.Uint32

	// dels[0] through dels[min(delsReserved,magazineSize)-1] store deleted
	// out-of-magazine handles.
	delsReserved    atomic.Uint32
	delsInitialized atomic.Uint32
	dels            [magazineSize]Handle
}

func (m *magazine[T]) TryAdd(t T) int {
	idx := m.addsReserved.Add(1) - 1
	if idx >= magazineSize {
		return -1
	}
	m.adds[idx].t = t
	m.addsInitialized.Add(1)
	return int(idx)
}

func (m *magazine[T]) TryDel(h Handle) bool {
	if h >= m.addsStartHandle {
		if invariants.Enabled && h >= m.addsStartHandle+magazineSize {
			// This cannot happen. The only way for the user to pass us a handle from a
			// newer magazine implies the add operation loaded a newer magazine than we
			// did now.
			panic("delete newer than magazine")
		}
		if m.inMagDeletes.Add(1) > magazineSize {
			// Magazine has been finished.
			return false
		}
		wasDeleted := m.adds[h-m.addsStartHandle].deleted.Swap(true)
		if wasDeleted {
			// It's important to check this even in non-invariant builds; if we don't,
			// Finish() will hang waiting for the correct count of set flags.
			panic("handle deleted twice")
		}
		return true
	}
	idx := m.delsReserved.Add(1) - 1
	if idx >= magazineSize {
		return false
	}
	m.dels[idx] = h
	m.delsInitialized.Add(1)
	return true
}

// Finish causes any subsequent TryAdd/TryDel calls to fail and waits until any
// concurrent TryAdd/TryDel calls complete. Returns the adds and dels in the
// magazine.
func (m *magazine[T]) Finish() (nAdds, nDels uint32) {
	// We block further operations by swapping in recentSize. The old value
	// contains the number of elements that were inserted (or are in the process
	// of being inserted right now).
	nAdds = m.addsReserved.Swap(magazineSize)
	nDels = m.delsReserved.Swap(magazineSize)
	nInMagDels := m.inMagDeletes.Swap(magazineSize)
	// The previous reserved value can exceed recentSize if multiple goroutines
	// were trying (and failing) to add.
	nAdds = min(nAdds, magazineSize)
	nDels = min(nDels, magazineSize)
	nInMagDels = min(nInMagDels, magazineSize)
	// Make sure all adds finished writing.
	for m.addsInitialized.Load() < nAdds {
		runtime.Gosched()
	}
	// Make sure all out-of-magazine dels finished writing.
	for m.delsInitialized.Load() < nDels {
		runtime.Gosched()
	}
	// Make sure all inside-magazine dels finished writing.
	for {
		n := uint32(0)
		for i := range m.adds {
			if m.adds[i].deleted.Load() {
				n++
			}
		}
		if n >= nInMagDels {
			if invariants.Enabled && n > nInMagDels {
				panic("more in-magazine deletes than recorded")
			}
			break
		}
		runtime.Gosched()
	}
	return nAdds, nDels
}
