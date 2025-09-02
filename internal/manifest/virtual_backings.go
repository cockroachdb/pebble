// Copyright 2024 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package manifest

import (
	"bytes"
	stdcmp "cmp"
	"container/heap"
	"fmt"
	"maps"
	"slices"

	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/pebble/internal/base"
)

// VirtualBackings maintains information about the set of backings that support
// virtual tables in the latest version.
//
// The VirtualBackings set internally maintains for each backing the number of
// virtual tables that use that backing and the sum of their virtual sizes. When
// a backing is added to the set, it initially is not associated with any
// tables. AddTable/RemoveTable are used to maintain the set of tables that are
// associated with a backing. Finally, a backing can only be removed from the
// set when it is no longer in use.
//
// -- Protection API --
//
// VirtualBackings exposes a Protect/Unprotect API. This is used to allow
// external file ingestions to reuse existing virtual backings. Because
// ingestions can run in parallel with other operations like compactions, it is
// possible for a backing to "go away" in-between the time the ingestion decides
// to use it and the time the ingestion installs a new version. The protection
// API solves this problem by keeping backings alive, even if they become
// otherwise unused by any tables.
//
// Backing protection achieves two goals:
//   - it must prevent the removal of the backing from the latest version, where
//     removal means becoming part of a VersionEdit.RemovedBackingTables. This
//     is achieved by treating the backing as "in use", preventing Unused() from
//     reporting it.
//   - it must prevent the backing from becoming obsolete (i.e. reaching a ref
//     count of 0). To achieve this, VirtualBackings takes a ref on each backing
//     when it is added; this ref must be released after the backing is removed
//     (when it is ok for the backing to be reported as obsolete).
//
// For example, say we have virtual table T1 with backing B1 and an ingestion tries
// to reuse the file. This is what will usually happen (the happy case):
//   - latest version is V1 and it contains T1(B1).
//   - ingestion request comes for another virtual portion of B1. Ingestion process
//     finds B1 and calls Protect(B1).
//   - ingestion completes, installs version V2 which has T1(B1) and a new
//     T2(B1), and calls Unprotect(B1).
//
// In this path, the Protect/Unprotect calls do nothing. But here is what could
// happen (the corner case):
//   - latest version is V1 and it contains T1(B1).
//   - ingestion request comes for another virtual portion of B1. Ingestion process
//     finds B1 and calls Protect(B1).
//   - compaction completes and installs version V2 which no longer has T1.
//     But because B1 is protected, V2 still has B1.
//   - ingestion completes, installs version V3 which has a new T2(B1) and calls
//     Unprotect(B1).
//
// If instead the ingestion fails to complete, the last step becomes:
//   - ingestion fails, calls Unprotect(B1). B1 is now Unused() and the next
//     version (applied by whatever next operation is) will remove B1.
type VirtualBackings struct {
	m map[base.DiskFileNum]*backingWithMetadata
	// rewriteCandidates is a min heap of virtual backings ordered by
	// referencedDataPct. Used to pick a candidate for rewriting.
	rewriteCandidates virtualBackingRewriteCandidatesHeap

	// unused are all the backings in m that are not inUse(). Used for
	// implementing Unused() efficiently.
	unused map[*TableBacking]struct{}

	totalSize uint64
}

// MakeVirtualBackings returns empty initialized VirtualBackings.
func MakeVirtualBackings() VirtualBackings {
	return VirtualBackings{
		m:      make(map[base.DiskFileNum]*backingWithMetadata),
		unused: make(map[*TableBacking]struct{}),
	}
}

type backingWithMetadata struct {
	backing *TableBacking

	// isLocal is true if the backing's fileNum is local to the file system.
	isLocal bool

	// protectionCount is used by Protect to temporarily prevent a backing from
	// being reported as unused.
	protectionCount int32
	// virtualizedSize is the sum of the sizes of the virtual tables
	// associated with this backing.
	virtualizedSize uint64

	// virtualTables is the list of virtual tables that use this backing.
	// AddTable/RemoveTable maintain this map.
	virtualTables map[base.TableNum]tableAndLevel
	heapIndex     int
}

func (bm *backingWithMetadata) referencedDataPct() float64 {
	return float64(bm.virtualizedSize) /
		float64(bm.backing.Size+bm.backing.ReferencedBlobValueSizeTotal)
}

// AddAndRef adds a new backing to the set and takes a reference on it. Another
// backing for the same DiskFileNum must not exist.
//
// The added backing is unused until it is associated with a table via AddTable
// or protected via Protect.
func (bv *VirtualBackings) AddAndRef(backing *TableBacking, isLocal bool) {
	// We take a reference on the backing because in case of protected backings
	// (see Protect), we might be the only ones holding on to a backing.
	backing.Ref()
	bm := &backingWithMetadata{
		backing:       backing,
		isLocal:       isLocal,
		virtualTables: make(map[base.TableNum]tableAndLevel),
		heapIndex:     -1,
	}
	bv.mustAdd(bm)
	bv.unused[backing] = struct{}{}
	bv.totalSize += backing.Size
}

// Remove removes a backing. The backing must not be in use; normally backings
// are removed once they are reported by Unused().
//
// It is up to the caller to release the reference took by AddAndRef.
func (bv *VirtualBackings) Remove(n base.DiskFileNum) {
	v := bv.mustGet(n)
	if v.inUse() {
		panic(errors.AssertionFailedf(
			"backing %s still in use (useCount=%d protectionCount=%d)",
			v.backing.DiskFileNum, len(v.virtualTables), v.protectionCount,
		))
	}
	if v.heapIndex != -1 {
		panic(errors.AssertionFailedf("backing %s still in rewriteCandidates heap", v.backing.DiskFileNum))
	}
	delete(bv.m, n)
	delete(bv.unused, v.backing)
	bv.totalSize -= v.backing.Size
}

// AddTable is used when a new table is using an existing backing. The backing
// must be in the set already.
func (bv *VirtualBackings) AddTable(m *TableMetadata, level int) {
	if !m.Virtual {
		panic(errors.AssertionFailedf("table %s not virtual", m.TableNum))
	}
	v := bv.mustGet(m.TableBacking.DiskFileNum)
	if _, ok := v.virtualTables[m.TableNum]; ok {
		panic(errors.AssertionFailedf("table %s already uses backing %s", m.TableNum, v.backing.DiskFileNum))
	}
	if !v.inUse() {
		delete(bv.unused, v.backing)
	}
	v.virtualizedSize += m.Size
	v.virtualTables[m.TableNum] = tableAndLevel{
		meta:  m,
		level: level,
	}
	// Update candidates heap.
	if v.isLocal {
		if v.heapIndex == -1 {
			heap.Push(&bv.rewriteCandidates, v)
		} else {
			heap.Fix(&bv.rewriteCandidates, v.heapIndex)
		}
	}
}

// RemoveTable is used when a table using a backing is removed. The backing is
// not removed from the set, even if it becomes unused.
func (bv *VirtualBackings) RemoveTable(backing base.DiskFileNum, table base.TableNum) {
	v := bv.mustGet(backing)
	t, ok := v.virtualTables[table]
	if !ok {
		panic(errors.AssertionFailedf("table %s does not use backing %s", table, v.backing.DiskFileNum))
	}
	delete(v.virtualTables, table)
	v.virtualizedSize -= t.meta.Size
	if !v.inUse() {
		bv.unused[v.backing] = struct{}{}
	}
	// Update candidates heap. The backing should be in the heap if we
	// successfully removed a table, but we check the index to be defensive.
	if v.heapIndex != -1 {
		if v.virtualizedSize == 0 {
			heap.Remove(&bv.rewriteCandidates, v.heapIndex)
			v.heapIndex = -1
		} else {
			heap.Fix(&bv.rewriteCandidates, v.heapIndex)
		}
	}
}

// Protect prevents a backing from being reported as unused until a
// corresponding Unprotect call is made. The backing must be in the set.
//
// Multiple Protect calls can be made for the same backing; each must have a
// corresponding Unprotect call before the backing can become unused.
func (bv *VirtualBackings) Protect(n base.DiskFileNum) {
	v := bv.mustGet(n)
	if !v.inUse() {
		delete(bv.unused, v.backing)
	}
	v.protectionCount++
}

// Unprotect reverses a Protect call.
func (bv *VirtualBackings) Unprotect(n base.DiskFileNum) {
	v := bv.mustGet(n)

	if v.protectionCount <= 0 {
		panic(errors.AssertionFailedf("invalid protectionCount"))
	}
	v.protectionCount--
	if !v.inUse() {
		bv.unused[v.backing] = struct{}{}
	}
}

// Stats returns the number and total size of all the virtual backings.
func (bv *VirtualBackings) Stats() (count int, totalSize uint64) {
	return len(bv.m), bv.totalSize
}

// Usage returns information about the usage of a backing, specifically:
//   - useCount: the number of virtual tables that use this backing;
//   - virtualizedSize: the sum of sizes of virtual tables that use the
//     backing.
//
// During compaction picking, we compensate a virtual sstable file size by
// (TableBacking.Size - virtualizedSize) / useCount.
// The intuition is that if TableBacking.Size - virtualizedSize is high, then the
// space amplification due to virtual sstables is high, and we should pick the
// virtual sstable with a higher priority.
func (bv *VirtualBackings) Usage(n base.DiskFileNum) (useCount int, virtualizedSize uint64) {
	v := bv.mustGet(n)
	return len(v.virtualTables), v.virtualizedSize
}

// Unused returns all backings that are and no longer used by the latest version
// and are not protected, in DiskFileNum order.
func (bv *VirtualBackings) Unused() []*TableBacking {
	res := make([]*TableBacking, 0, len(bv.unused))
	for b := range bv.unused {
		res = append(res, b)
	}
	slices.SortFunc(res, func(a, b *TableBacking) int {
		return stdcmp.Compare(a.DiskFileNum, b.DiskFileNum)
	})
	return res
}

// Get returns the backing with the given DiskFileNum, if it is in the set.
func (bv *VirtualBackings) Get(n base.DiskFileNum) (_ *TableBacking, ok bool) {
	v, ok := bv.m[n]
	if ok {
		return v.backing, true
	}
	return nil, false
}

// ForEach calls fn on each backing, in unspecified order.
func (bv *VirtualBackings) ForEach(fn func(backing *TableBacking)) {
	for _, v := range bv.m {
		fn(v.backing)
	}
}

// DiskFileNums returns disk file nums of all the backing in the set, in sorted
// order.
func (bv *VirtualBackings) DiskFileNums() []base.DiskFileNum {
	res := make([]base.DiskFileNum, 0, len(bv.m))
	for n := range bv.m {
		res = append(res, n)
	}
	slices.Sort(res)
	return res
}

// Backings returns all backings in the set, in unspecified order.
func (bv *VirtualBackings) Backings() []*TableBacking {
	res := make([]*TableBacking, 0, len(bv.m))
	for _, v := range bv.m {
		res = append(res, v.backing)
	}
	return res
}

// ReplacementCandidate returns the backing with the lowest ratio of data
// referenced by virtual tables to total size, along with the list of virtual
// tables that use the backing. If there are no backings in the set, nil is
// returned.
func (bv *VirtualBackings) ReplacementCandidate() (*TableBacking, [NumLevels][]*TableMetadata) {
	if bv.rewriteCandidates.Len() == 0 {
		return nil, [NumLevels][]*TableMetadata{}
	}
	v := bv.rewriteCandidates.items[0]
	var tables [NumLevels][]*TableMetadata
	tableNums := slices.Sorted(maps.Keys(v.virtualTables))
	for _, t := range tableNums {
		tl := v.virtualTables[t]
		tables[tl.level] = append(tables[tl.level], tl.meta)
	}
	return v.backing, tables
}

func (bv *VirtualBackings) String() string {
	nums := bv.DiskFileNums()

	var buf bytes.Buffer
	count, totalSize := bv.Stats()
	if count == 0 {
		fmt.Fprintf(&buf, "no virtual backings\n")
	} else {
		fmt.Fprintf(&buf, "%d virtual backings, total size %d:\n", count, totalSize)
		for _, n := range nums {
			v := bv.m[n]
			fmt.Fprintf(&buf, "  %s:  size=%d  refBlobValueSize=%d  useCount=%d  protectionCount=%d  virtualizedSize=%d",
				n, v.backing.Size, v.backing.ReferencedBlobValueSizeTotal, len(v.virtualTables), v.protectionCount, v.virtualizedSize)
			if !v.isLocal {
				fmt.Fprintf(&buf, "  (remote)")
			}
			tableNums := slices.Sorted(maps.Keys(v.virtualTables))
			fmt.Fprintf(&buf, "  tables: %v\n", tableNums)
		}
		// Print heap state.
		if len(bv.rewriteCandidates.items) > 0 {
			fmt.Fprint(&buf, "rewrite candidates heap: ")
			for _, v := range bv.rewriteCandidates.items {
				fmt.Fprintf(&buf, "%s(%.1f%%) ", v.backing.DiskFileNum, v.referencedDataPct()*100)
			}
			fmt.Fprintf(&buf, "\n")
		}
	}
	unused := bv.Unused()
	if len(unused) > 0 {
		fmt.Fprintf(&buf, "unused virtual backings:")
		for _, b := range unused {
			fmt.Fprintf(&buf, " %s", b.DiskFileNum)
		}
		fmt.Fprintf(&buf, "\n")
	}
	return buf.String()
}

func (bv *VirtualBackings) mustAdd(v *backingWithMetadata) {
	_, ok := bv.m[v.backing.DiskFileNum]
	if ok {
		panic("pebble: trying to add an existing file backing")
	}
	bv.m[v.backing.DiskFileNum] = v
}

func (bv *VirtualBackings) mustGet(n base.DiskFileNum) *backingWithMetadata {
	v, ok := bv.m[n]
	if !ok {
		panic(fmt.Sprintf("unknown backing %s", n))
	}
	return v
}

// inUse returns true if b is used to back at least one virtual table.
func (v *backingWithMetadata) inUse() bool {
	return len(v.virtualTables) > 0 || v.protectionCount > 0
}

type virtualBackingRewriteCandidatesHeap struct {
	items []*backingWithMetadata
}

var _ heap.Interface = (*virtualBackingRewriteCandidatesHeap)(nil)

func (v *virtualBackingRewriteCandidatesHeap) Len() int {
	return len(v.items)
}

func (v *virtualBackingRewriteCandidatesHeap) Less(i, j int) bool {
	// We want to rewrite backings with a high percentage of garbage first,
	// so we order the heap by ratio of data referenced in virtual tables.
	return v.items[i].referencedDataPct() < v.items[j].referencedDataPct()
}

func (v *virtualBackingRewriteCandidatesHeap) Swap(i, j int) {
	v.items[i], v.items[j] = v.items[j], v.items[i]
	v.items[i].heapIndex = i
	v.items[j].heapIndex = j
}

func (v *virtualBackingRewriteCandidatesHeap) Push(x any) {
	x.(*backingWithMetadata).heapIndex = len(v.items)
	v.items = append(v.items, x.(*backingWithMetadata))
}

func (v *virtualBackingRewriteCandidatesHeap) Pop() any {
	old := v.items
	n := len(old)
	item := old[n-1]
	old[n-1] = nil
	v.items = old[0 : n-1]
	item.heapIndex = -1
	return item
}
