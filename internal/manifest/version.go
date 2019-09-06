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

type Compare = base.Compare
type InternalKey = base.InternalKey
type Options = base.Options
type TableInfo = base.TableInfo

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

func (m *FileMetadata) String() string {
	return fmt.Sprintf("%d:%s-%s", m.FileNum, m.Smallest, m.Largest)
}

// TableInfo returns a subset of the FileMetadata state formatted as a
// TableInfo.
func (m *FileMetadata) TableInfo(dirname string) TableInfo {
	return TableInfo{
		Path:           base.MakeFilename(dirname, base.FileTypeTable, m.FileNum),
		FileNum:        m.FileNum,
		Size:           m.Size,
		Smallest:       m.Smallest,
		Largest:        m.Largest,
		SmallestSeqNum: m.SmallestSeqNum,
		LargestSeqNum:  m.LargestSeqNum,
	}
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
	// NB: This is the same ordering that RocksDB uses for L0 files.

	// Sort first by largest sequence number.
	if b[i].LargestSeqNum != b[j].LargestSeqNum {
		return b[i].LargestSeqNum < b[j].LargestSeqNum
	}
	// Then by smallest sequence number.
	if b[i].SmallestSeqNum != b[j].SmallestSeqNum {
		return b[i].SmallestSeqNum < b[j].SmallestSeqNum
	}
	// Break ties by file number.
	return b[i].FileNum < b[j].FileNum
}
func (b bySeqNum) Swap(i, j int) { b[i], b[j] = b[j], b[i] }

// SortBySeqNum sorts the specified files by decreasing sequence number.
func SortBySeqNum(files []FileMetadata) {
	sort.Sort(bySeqNum(files))
}

type bySmallest struct {
	dat []FileMetadata
	cmp Compare
}

func (b bySmallest) Len() int { return len(b.dat) }
func (b bySmallest) Less(i, j int) bool {
	return base.InternalCompare(b.cmp, b.dat[i].Smallest, b.dat[j].Smallest) < 0
}
func (b bySmallest) Swap(i, j int) { b.dat[i], b.dat[j] = b.dat[j], b.dat[i] }

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
// The tables at level 0 are sorted by increasing fileNum. If two level 0
// tables have fileNums i and j and i < j, then the sequence numbers of every
// internal key in table i are all less than those for table j. The range of
// internal keys [fileMetadata.smallest, fileMetadata.largest] in each level 0
// table may overlap.
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
	var buf bytes.Buffer
	for level := 0; level < NumLevels; level++ {
		if len(v.Files[level]) == 0 {
			continue
		}
		fmt.Fprintf(&buf, "%d:", level)
		for j := range v.Files[level] {
			f := &v.Files[level][j]
			fmt.Fprintf(&buf, " %s-%s", f.Smallest.UserKey, f.Largest.UserKey)
		}
		fmt.Fprintf(&buf, "\n")
	}
	return buf.String()
}

// DebugString returns an alternative format to String() which includes
// sequence number and kind information for the sstable boundaries.
func (v *Version) DebugString() string {
	var buf bytes.Buffer
	for level := 0; level < NumLevels; level++ {
		if len(v.Files[level]) == 0 {
			continue
		}
		fmt.Fprintf(&buf, "%d:", level)
		for j := range v.Files[level] {
			f := &v.Files[level][j]
			fmt.Fprintf(&buf, " %s-%s", f.Smallest, f.Largest)
		}
		fmt.Fprintf(&buf, "\n")
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
func (v *Version) Overlaps(
	level int, cmp Compare, start, end []byte,
) (ret []FileMetadata) {
	if level == 0 {
		// The sstables in level 0 can overlap with each other. As soon as we find
		// one sstable that overlaps with our target range, we need to expand the
		// range and find all sstables that overlap with the expanded range.
	loop:
		for {
			for _, meta := range v.Files[level] {
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
				ret = append(ret, meta)

				// If level == 0, check if the newly added fileMetadata has
				// expanded the range. If so, restart the search.
				restart := false
				if cmp(smallest, start) < 0 {
					start = smallest
					restart = true
				}
				if cmp(largest, end) > 0 {
					end = largest
					restart = true
				}
				if restart {
					ret = ret[:0]
					continue loop
				}
			}
			return ret
		}
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
func (v *Version) CheckOrdering(cmp Compare) error {
	for level, ff := range v.Files {
		if level == 0 {
			for i := 1; i < len(ff); i++ {
				prev := &ff[i-1]
				f := &ff[i]
				if prev.LargestSeqNum >= f.LargestSeqNum {
					return fmt.Errorf("level 0 files are not in increasing largest seqNum order: %d, %d",
						prev.LargestSeqNum, f.LargestSeqNum)
				}
				if prev.SmallestSeqNum >= f.SmallestSeqNum {
					return fmt.Errorf("level 0 files are not in increasing smallest seqNum order: %d, %d",
						prev.SmallestSeqNum, f.SmallestSeqNum)
				}
			}
		} else {
			for i := 1; i < len(ff); i++ {
				prev := &ff[i-1]
				f := &ff[i]
				if base.InternalCompare(cmp, prev.Largest, f.Smallest) >= 0 {
					return fmt.Errorf("level non-0 files are not in increasing ikey order: %s, %s\n%s",
						prev.Largest, f.Smallest, v.DebugString())
				}
				if base.InternalCompare(cmp, f.Smallest, f.Largest) > 0 {
					return fmt.Errorf("level non-0 file has inconsistent bounds: %s, %s",
						f.Smallest, f.Largest)
				}
			}
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
