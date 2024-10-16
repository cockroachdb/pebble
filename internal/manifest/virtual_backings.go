// Copyright 2024 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package manifest

import (
	"bytes"
	stdcmp "cmp"
	"fmt"
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
	m map[base.DiskFileNum]backingWithMetadata

	// unused are all the backings in m that are not inUse(). Used for
	// implementing Unused() efficiently.
	unused map[*FileBacking]struct{}

	totalSize uint64
}

// MakeVirtualBackings returns empty initialized VirtualBackings.
func MakeVirtualBackings() VirtualBackings {
	return VirtualBackings{
		m:      make(map[base.DiskFileNum]backingWithMetadata),
		unused: make(map[*FileBacking]struct{}),
	}
}

type backingWithMetadata struct {
	backing *FileBacking

	// A backing initially has a useCount of 0. The useCount is increased by
	// AddTable and decreased by RemoveTable. Backings that have useCount=0 are

	useCount int32
	// protectionCount is used by Protect to temporarily prevent a backing from
	// being reported as unused.
	protectionCount int32
	// virtualizedSize is the sum of the sizes of the useCount virtual tables
	// associated with this backing.
	virtualizedSize uint64
}

// AddAndRef adds a new backing to the set and takes a reference on it. Another
// backing for the same DiskFileNum must not exist.
//
// The added backing is unused until it is associated with a table via AddTable
// or protected via Protect.
func (bv *VirtualBackings) AddAndRef(backing *FileBacking) {
	// We take a reference on the backing because in case of protected backings
	// (see Protect), we might be the only ones holding on to a backing.
	backing.Ref()
	bv.mustAdd(backingWithMetadata{
		backing: backing,
	})
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
			v.backing.DiskFileNum, v.useCount, v.protectionCount,
		))
	}
	delete(bv.m, n)
	delete(bv.unused, v.backing)
	bv.totalSize -= v.backing.Size
}

// AddTable is used when a new table is using an exiting backing. The backing
// must be in the set already.
func (bv *VirtualBackings) AddTable(m *FileMetadata) {
	if !m.Virtual {
		panic(errors.AssertionFailedf("table %s not virtual", m.FileNum))
	}
	v := bv.mustGet(m.FileBacking.DiskFileNum)
	if !v.inUse() {
		delete(bv.unused, v.backing)
	}
	v.useCount++
	v.virtualizedSize += m.Size
	bv.m[m.FileBacking.DiskFileNum] = v
}

// RemoveTable is used when a table using a backing is removed. The backing is
// not removed from the set, even if it becomes unused.
func (bv *VirtualBackings) RemoveTable(m *FileMetadata) {
	if !m.Virtual {
		panic(errors.AssertionFailedf("table %s not virtual", m.FileNum))
	}
	v := bv.mustGet(m.FileBacking.DiskFileNum)

	if v.useCount <= 0 {
		panic(errors.AssertionFailedf("invalid useCount"))
	}
	v.useCount--
	v.virtualizedSize -= m.Size
	bv.m[m.FileBacking.DiskFileNum] = v
	if !v.inUse() {
		bv.unused[v.backing] = struct{}{}
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
	bv.m[n] = v
}

// Unprotect reverses a Protect call.
func (bv *VirtualBackings) Unprotect(n base.DiskFileNum) {
	v := bv.mustGet(n)

	if v.protectionCount <= 0 {
		panic(errors.AssertionFailedf("invalid protectionCount"))
	}
	v.protectionCount--
	bv.m[n] = v
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
// (FileBacking.Size - virtualizedSize) / useCount.
// The intuition is that if FileBacking.Size - virtualizedSize is high, then the
// space amplification due to virtual sstables is high, and we should pick the
// virtual sstable with a higher priority.
func (bv *VirtualBackings) Usage(n base.DiskFileNum) (useCount int, virtualizedSize uint64) {
	v := bv.mustGet(n)
	return int(v.useCount), v.virtualizedSize
}

// Unused returns all backings that are and no longer used by the latest version
// and are not protected, in DiskFileNum order.
func (bv *VirtualBackings) Unused() []*FileBacking {
	res := make([]*FileBacking, 0, len(bv.unused))
	for b := range bv.unused {
		res = append(res, b)
	}
	slices.SortFunc(res, func(a, b *FileBacking) int {
		return stdcmp.Compare(a.DiskFileNum, b.DiskFileNum)
	})
	return res
}

// Get returns the backing with the given DiskFileNum, if it is in the set.
func (bv *VirtualBackings) Get(n base.DiskFileNum) (_ *FileBacking, ok bool) {
	v, ok := bv.m[n]
	if ok {
		return v.backing, true
	}
	return nil, false
}

// ForEach calls fn on each backing, in unspecified order.
func (bv *VirtualBackings) ForEach(fn func(backing *FileBacking)) {
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
func (bv *VirtualBackings) Backings() []*FileBacking {
	res := make([]*FileBacking, 0, len(bv.m))
	for _, v := range bv.m {
		res = append(res, v.backing)
	}
	return res
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
			fmt.Fprintf(&buf, "  %s:  size=%d  useCount=%d  protectionCount=%d  virtualizedSize=%d\n",
				n, v.backing.Size, v.useCount, v.protectionCount, v.virtualizedSize)
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

func (bv *VirtualBackings) mustAdd(v backingWithMetadata) {
	_, ok := bv.m[v.backing.DiskFileNum]
	if ok {
		panic("pebble: trying to add an existing file backing")
	}
	bv.m[v.backing.DiskFileNum] = v
}

func (bv *VirtualBackings) mustGet(n base.DiskFileNum) backingWithMetadata {
	v, ok := bv.m[n]
	if !ok {
		panic(fmt.Sprintf("unknown backing %s", n))
	}
	return v
}

// inUse returns true if b is used to back at least one virtual table.
func (v *backingWithMetadata) inUse() bool {
	return v.useCount > 0 || v.protectionCount > 0
}
