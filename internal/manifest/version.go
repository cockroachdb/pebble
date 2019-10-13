// Copyright 2012 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package manifest

import (
	"bytes"
	"fmt"
	"sort"
	"sync"
	"sync/atomic"

	"github.com/cockroachdb/pebble/internal/base"
)

// Compare exports the base.Compare type.
type Compare = base.Compare

// InternalKey exports the base.InternalKey type.
type InternalKey = base.InternalKey

// TableInfo contains the common information for table related events.
type TableInfo struct {
	// FileNum is the internal DB identifier for the table.
	FileNum uint64
	// Size is the size of the file in bytes.
	Size uint64
	// Smallest is the smallest internal key in the table.
	Smallest InternalKey
	// Largest is the largest internal key in the table.
	Largest InternalKey
	// SmallestSeqNum is the smallest sequence number in the table.
	SmallestSeqNum uint64
	// LargestSeqNum is the largest sequence number in the table.
	LargestSeqNum uint64
}

// FileMetadata holds the metadata for an on-disk table.
type FileMetadata struct {
	// reference count for the file: incremented when a file is added to a
	// version and decremented when the version is unreferenced. The file is
	// obsolete when the reference count falls to zero. This is a pointer because
	// fileMetadata is copied by value from version to version, but we want the
	// reference count to be shared.
	refs *int32
	// FileNum is the file number.
	FileNum uint64
	// Size is the Size of the file, in bytes.
	Size uint64
	// Smallest and Largest are the inclusive bounds for the internal keys
	// stored in the table.
	Smallest InternalKey
	Largest  InternalKey
	// Smallest and largest sequence numbers in the table.
	SmallestSeqNum uint64
	LargestSeqNum  uint64
	// true if client asked us nicely to compact this file.
	MarkedForCompaction bool
}

func (m FileMetadata) String() string {
	return fmt.Sprintf("%d:%s-%s", m.FileNum, m.Smallest, m.Largest)
}

// TableInfo returns a subset of the FileMetadata state formatted as a
// TableInfo.
func (m *FileMetadata) TableInfo() TableInfo {
	return TableInfo{
		FileNum:        m.FileNum,
		Size:           m.Size,
		Smallest:       m.Smallest,
		Largest:        m.Largest,
		SmallestSeqNum: m.SmallestSeqNum,
		LargestSeqNum:  m.LargestSeqNum,
	}
}

func (m *FileMetadata) lessSeqNum(b *FileMetadata) bool {
	// NB: This is the same ordering that RocksDB uses for L0 files.

	// Sort first by largest sequence number.
	if m.LargestSeqNum != b.LargestSeqNum {
		return m.LargestSeqNum < b.LargestSeqNum
	}
	// Then by smallest sequence number.
	if m.SmallestSeqNum != b.SmallestSeqNum {
		return m.SmallestSeqNum < b.SmallestSeqNum
	}
	// Break ties by file number.
	return m.FileNum < b.FileNum
}

func (m *FileMetadata) lessSmallestKey(b *FileMetadata, cmp Compare) bool {
	return base.InternalCompare(cmp, m.Smallest, b.Smallest) < 0
}

// KeyRange returns the minimum smallest and maximum largest internalKey for
// all the fileMetadata in f0 and f1.
func KeyRange(ucmp Compare, f0, f1 []FileMetadata) (smallest, largest InternalKey) {
	first := true
	for _, f := range [2][]FileMetadata{f0, f1} {
		for _, meta := range f {
			if first {
				first = false
				smallest, largest = meta.Smallest, meta.Largest
				continue
			}
			if base.InternalCompare(ucmp, meta.Smallest, smallest) < 0 {
				smallest = meta.Smallest
			}
			if base.InternalCompare(ucmp, meta.Largest, largest) > 0 {
				largest = meta.Largest
			}
		}
	}
	return smallest, largest
}

type bySeqNum []FileMetadata

func (b bySeqNum) Len() int { return len(b) }
func (b bySeqNum) Less(i, j int) bool {
	return b[i].lessSeqNum(&b[j])
}
func (b bySeqNum) Swap(i, j int) { b[i], b[j] = b[j], b[i] }

// SortBySeqNum sorts the specified files by increasing sequence number.
func SortBySeqNum(files []FileMetadata) {
	sort.Sort(bySeqNum(files))
}

type bySmallest struct {
	files []FileMetadata
	cmp   Compare
}

func (b bySmallest) Len() int { return len(b.files) }
func (b bySmallest) Less(i, j int) bool {
	return b.files[i].lessSmallestKey(&b.files[j], b.cmp)
}
func (b bySmallest) Swap(i, j int) { b.files[i], b.files[j] = b.files[j], b.files[i] }

// SortBySmallest sorts the specified files by smallest key using the supplied
// comparison function to order user keys.
func SortBySmallest(files []FileMetadata, cmp Compare) {
	sort.Sort(bySmallest{files, cmp})
}

// NumLevels is the number of levels a Version contains.
const NumLevels = 7

// Version is a collection of file metadata for on-disk tables at various
// levels. In-memory DBs are written to level-0 tables, and compactions
// migrate data from level N to level N+1. The tables map internal keys (which
// are a user key, a delete or set bit, and a sequence number) to user values.
//
// The tables at level 0 are sorted by largest sequence number. Due to file
// ingestion, there may be overlap in the ranges of sequence numbers contain in
// level 0 sstables. In particular, it is valid for one level 0 sstable to have
// the seqnum range [1,100] while an adjacent sstable has the seqnum range
// [50,50]. This occurs when the [50,50] table was ingested and given a global
// seqnum. The ingestion code will have ensured that the [50,50] sstable will
// not have any keys that overlap with the [1,100] in the seqnum range
// [1,49]. The range of internal keys [fileMetadata.smallest,
// fileMetadata.largest] in each level 0 table may overlap.
//
// The tables at any non-0 level are sorted by their internal key range and any
// two tables at the same non-0 level do not overlap.
//
// The internal key ranges of two tables at different levels X and Y may
// overlap, for any X != Y.
//
// Finally, for every internal key in a table at level X, there is no internal
// key in a higher level table that has both the same user key and a higher
// sequence number.
type Version struct {
	refs int32

	Files [NumLevels][]FileMetadata

	// The callback to invoke when the last reference to a version is
	// removed. Will be called with list.mu held.
	Deleted func(obsolete []uint64)

	// The list the version is linked into.
	list *VersionList

	// The next/prev link for the versionList doubly-linked list of versions.
	prev, next *Version
}

func (v *Version) String() string {
	return v.Pretty(base.DefaultFormatter)
}

// Pretty returns a string representation of the version.
func (v *Version) Pretty(format base.Formatter) string {
	var buf bytes.Buffer
	for level := 0; level < NumLevels; level++ {
		if len(v.Files[level]) == 0 {
			continue
		}
		fmt.Fprintf(&buf, "%d:\n", level)
		for j := range v.Files[level] {
			f := &v.Files[level][j]
			fmt.Fprintf(&buf, "  %d:[%s-%s]\n", f.FileNum,
				format(f.Smallest.UserKey), format(f.Largest.UserKey))
		}
	}
	return buf.String()
}

// DebugString returns an alternative format to String() which includes
// sequence number and kind information for the sstable boundaries.
func (v *Version) DebugString(format base.Formatter) string {
	var buf bytes.Buffer
	for level := 0; level < NumLevels; level++ {
		if len(v.Files[level]) == 0 {
			continue
		}
		fmt.Fprintf(&buf, "%d:\n", level)
		for j := range v.Files[level] {
			f := &v.Files[level][j]
			fmt.Fprintf(&buf, "  %d:[%s-%s]\n", f.FileNum,
				f.Smallest.Pretty(format), f.Largest.Pretty(format))
		}
	}
	return buf.String()
}

// Refs returns the number of references to the version.
func (v *Version) Refs() int32 {
	return atomic.LoadInt32(&v.refs)
}

// Ref increments the version refcount.
func (v *Version) Ref() {
	atomic.AddInt32(&v.refs, 1)
}

// Unref decrements the version refcount. If the last reference to the version
// was removed, the version is removed from the list of versions and the
// Deleted callback is invoked. Requires that the VersionList mutex is NOT
// locked.
func (v *Version) Unref() {
	if atomic.AddInt32(&v.refs, -1) == 0 {
		obsolete := v.unrefFiles()
		l := v.list
		l.mu.Lock()
		l.Remove(v)
		v.Deleted(obsolete)
		l.mu.Unlock()
	}
}

// UnrefLocked decrements the version refcount. If the last reference to the
// version was removed, the version is removed from the list of versions and
// the Deleted callback is invoked. Requires that the VersionList mutex is
// already locked.
func (v *Version) UnrefLocked() {
	if atomic.AddInt32(&v.refs, -1) == 0 {
		v.list.Remove(v)
		v.Deleted(v.unrefFiles())
	}
}

func (v *Version) unrefFiles() []uint64 {
	var obsolete []uint64
	for _, files := range v.Files {
		for i := range files {
			f := &files[i]
			if atomic.AddInt32(f.refs, -1) == 0 {
				obsolete = append(obsolete, f.FileNum)
			}
		}
	}
	return obsolete
}

// Next returns the next version in the list of versions.
func (v *Version) Next() *Version {
	return v.next
}

// Overlaps returns all elements of v.files[level] whose user key range
// intersects the inclusive range [start, end]. If level is non-zero then the
// user key ranges of v.files[level] are assumed to not overlap (although they
// may touch). If level is zero then that assumption cannot be made, and the
// [start, end] range is expanded to the union of those matching ranges so far
// and the computation is repeated until [start, end] stabilizes.
// The returned files are a subsequence of the input files, i.e., the ordering
// is not changed.
func (v *Version) Overlaps(
	level int, cmp Compare, start, end []byte,
) (ret []FileMetadata) {
	if level == 0 {
		// Indices that have been selected as overlapping.
		selectedIndices := make([]bool, len(v.Files[level]))
		numSelected := 0
		for {
			restart := false
			for i, selected := range selectedIndices {
				if selected {
					continue
				}
				meta := &v.Files[level][i]
				smallest := meta.Smallest.UserKey
				largest := meta.Largest.UserKey
				if cmp(largest, start) < 0 {
					// meta is completely before the specified range; skip it.
					continue
				}
				if cmp(smallest, end) > 0 {
					// meta is completely after the specified range; skip it.
					continue
				}
				// Overlaps.
				selectedIndices[i] = true
				numSelected++

				// Since level == 0, check if the newly added fileMetadata has
				// expanded the range. We expand the range immediately for files
				// we have remaining to check in this loop. All already checked
				// and unselected files will need to be rechecked via the
				// restart below.
				if cmp(smallest, start) < 0 {
					start = smallest
					restart = true
				}
				if cmp(largest, end) > 0 {
					end = largest
					restart = true
				}
			}

			if !restart {
				ret = make([]FileMetadata, 0, numSelected)
				for i, selected := range selectedIndices {
					if selected {
						ret = append(ret, v.Files[level][i])
					}
				}
				break
			}
			// Continue looping to retry the files that were not selected.
		}
		return
	}
	// Binary search to find the range of files which overlaps with our target
	// range.
	files := v.Files[level]
	lower := sort.Search(len(files), func(i int) bool {
		return cmp(files[i].Largest.UserKey, start) >= 0
	})
	upper := sort.Search(len(files), func(i int) bool {
		return cmp(files[i].Smallest.UserKey, end) > 0
	})
	if lower >= upper {
		return nil
	}
	return files[lower:upper]
}

// CheckOrdering checks that the files are consistent with respect to
// increasing file numbers (for level 0 files) and increasing and non-
// overlapping internal key ranges (for level non-0 files).
func (v *Version) CheckOrdering(cmp Compare, format base.Formatter) error {
	for level, files := range v.Files {
		if err := CheckOrdering(cmp, format, level, files); err != nil {
			return fmt.Errorf("%s\n%s", err, v.DebugString(format))
		}
	}
	return nil
}

// VersionList holds a list of versions. The versions are ordered from oldest
// to newest.
type VersionList struct {
	mu   *sync.Mutex
	root Version
}

// Init initializes the version list.
func (l *VersionList) Init(mu *sync.Mutex) {
	l.mu = mu
	l.root.next = &l.root
	l.root.prev = &l.root
}

// Empty returns true if the list is empty, and false otherwise.
func (l *VersionList) Empty() bool {
	return l.root.next == &l.root
}

// Front returns the oldest version in the list. Note that this version is only
// valid if Empty() returns true.
func (l *VersionList) Front() *Version {
	return l.root.next
}

// Back returns the newest version in the list. Note that this version is only
// valid if Empty() returns true.
func (l *VersionList) Back() *Version {
	return l.root.prev
}

// PushBack adds a new version to the back of the list. This new version
// becomes the "newest" version in the list.
func (l *VersionList) PushBack(v *Version) {
	if v.list != nil || v.prev != nil || v.next != nil {
		panic("pebble: version list is inconsistent")
	}
	v.prev = l.root.prev
	v.prev.next = v
	v.next = &l.root
	v.next.prev = v
	v.list = l
}

// Remove removes the specified version from the list.
func (l *VersionList) Remove(v *Version) {
	if v == &l.root {
		panic("pebble: cannot remove version list root node")
	}
	if v.list != l {
		panic("pebble: version list is inconsistent")
	}
	v.prev.next = v.next
	v.next.prev = v.prev
	v.next = nil // avoid memory leaks
	v.prev = nil // avoid memory leaks
	v.list = nil // avoid memory leaks
}

// CheckOrdering checks that the files are consistent with respect to
// seqnums (for level 0 files -- see detailed comment below) and increasing and non-
// overlapping internal key ranges (for non-level 0 files).
func CheckOrdering(cmp Compare, format base.Formatter, level int, files []FileMetadata) error {
	if level == 0 {
		// We have 2 kinds of files:
		// - Files with exactly one sequence number: these could be either ingested files
		//   or flushed files. We cannot tell the difference between them based on FileMetadata,
		//   so our consistency checking here uses the weaker checks assuming it is an ingested
		//   file (except for the 0 sequence number case below).
		// - Files with multiple sequence numbers: these are necessarily flushed files.
		//
		// Two cases of overlapping sequence numbers:
		// Case 1:
		// An ingested file contained in the sequence numbers of the flushed file -- it must be
		// fully contained (not coincident with either end of the flushed file) since the memtable
		// must have been at [a, b-1] (where b > a) when the ingested file was assigned sequence
		// num b, and the memtable got a subsequent update that was given sequence num b+1, before
		// being flushed.
		//
		// So a sequence [1000, 1000] [1002, 1002] [1000, 2000] is invalid since the first and
		// third file are inconsistent with each other. So comparing adjacent files is insufficient
		// for consistency checking.
		//
		// Visually we have something like
		// x------y x-----------yx-------------y (flushed files where x, y are the endpoints)
		//     y       y  y        y             (y's represent ingested files)
		// And these are ordered in increasing order of y. Note that y's must be unique.
		//
		// Case 2:
		// A flushed file that did not overlap in keys with any file in any level, but does overlap
		// in the file key intervals. This file is placed in L0 since it overlaps in the file
		// key intervals but since it has no overlapping data, it is assigned a sequence number
		// of 0 in RocksDB. We handle this case for compatibility with RocksDB.

		// The largest sequence number of a flushed file. Increasing.
		var largestFlushedSeqNum uint64

		// The largest sequence number of any file. Increasing.
		var largestSeqNum uint64

		// The ingested file sequence numbers that have not yet been checked to be compatible with
		// flushed files.
		// They are checked when largestFlushedSeqNum advances past them.
		var uncheckedIngestedSeqNums []uint64

		for i := 0; i < len(files); i++ {
			f := &files[i]
			if i > 0 {
				// Validate that the sorting is sane.
				prev := &files[i-1]
				if prev.LargestSeqNum == 0 && f.LargestSeqNum == prev.LargestSeqNum {
					// Multiple files satisfying case 2 mentioned above.
				} else if !prev.lessSeqNum(f) {
					return fmt.Errorf("L0 files %06d and %06d are not properly ordered: %d-%d vs %d-%d",
						prev.FileNum, f.FileNum,
						prev.SmallestSeqNum, prev.LargestSeqNum,
						f.SmallestSeqNum, f.LargestSeqNum)
				}
			}
			if f.LargestSeqNum == 0 && f.LargestSeqNum == f.SmallestSeqNum {
				// We don't add these to uncheckedIngestedSeqNums since there could be flushed
				// files of the form [0, N] where N > 0.
				continue
			}
			if i > 0 && largestSeqNum >= f.LargestSeqNum {
				return fmt.Errorf("L0 file %06d does not have strictly increasing "+
					"largest seqnum: %d-%d vs %d", f.FileNum, f.SmallestSeqNum, f.LargestSeqNum, largestSeqNum)
			}
			largestSeqNum = f.LargestSeqNum
			if f.SmallestSeqNum == f.LargestSeqNum {
				// Ingested file.
				uncheckedIngestedSeqNums = append(uncheckedIngestedSeqNums, f.LargestSeqNum)
			} else {
				// Flushed file.
				// Two flushed files cannot overlap.
				if largestFlushedSeqNum > 0 && f.SmallestSeqNum <= largestFlushedSeqNum {
					return fmt.Errorf("L0 flushed file %06d overlaps with the largest seqnum of a "+
						"preceding flushed file: %d-%d vs %d", f.FileNum, f.SmallestSeqNum, f.LargestSeqNum,
						largestFlushedSeqNum)
				}
				largestFlushedSeqNum = f.LargestSeqNum
				// Check that unchecked ingested sequence numbers are not coincident with f.SmallestSeqNum.
				// We do not need to check that they are not coincident with f.LargestSeqNum because we
				// have already confirmed that LargestSeqNums were increasing.
				for _, seq := range uncheckedIngestedSeqNums {
					if seq == f.SmallestSeqNum {
						return fmt.Errorf("L0 flushed file %06d has an ingested file coincident with "+
							"smallest seqnum: %d-%d", f.FileNum, f.SmallestSeqNum, f.LargestSeqNum)
					}
				}
				uncheckedIngestedSeqNums = uncheckedIngestedSeqNums[:0]
			}
		}
	} else {
		for i := 0; i < len(files); i++ {
			f := &files[i]
			if base.InternalCompare(cmp, f.Smallest, f.Largest) > 0 {
				return fmt.Errorf("L%d file %06d has inconsistent bounds: %s vs %s",
					level, f.FileNum, f.Smallest.Pretty(format), f.Largest.Pretty(format))
			}
			if i > 0 {
				prev := &files[i-1]
				if !prev.lessSmallestKey(f, cmp) {
					return fmt.Errorf("L%d files %06d and %06d are not properly ordered: %s-%s vs %s-%s",
						level, prev.FileNum, f.FileNum,
						prev.Smallest.Pretty(format), prev.Largest.Pretty(format),
						f.Smallest.Pretty(format), f.Largest.Pretty(format))
				}
				if base.InternalCompare(cmp, prev.Largest, f.Smallest) >= 0 {
					return fmt.Errorf("L%d files %06d and %06d have overlapping ranges: %s-%s vs %s-%s",
						level, prev.FileNum, f.FileNum,
						prev.Smallest.Pretty(format), prev.Largest.Pretty(format),
						f.Smallest.Pretty(format), f.Largest.Pretty(format))
				}
			}
		}
	}
	return nil
}
