// Copyright 2012 The LevelDB-Go Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package leveldb

import (
	"fmt"
	"sort"

	"code.google.com/p/leveldb-go/leveldb/db"
)

// fileMetadata holds the metadata for an on-disk table.
type fileMetadata struct {
	// fileNum is the file number.
	fileNum uint64
	// size is the size of the file, in bytes.
	size uint64
	// smallest and largest are the inclusive bounds for the internal keys
	// stored in the table.
	smallest, largest internalKey
}

// totalSize returns the total size of all the files in f.
func totalSize(f []fileMetadata) (size uint64) {
	for _, x := range f {
		size += x.size
	}
	return size
}

// ikeyRange returns the minimum smallest and maximum largest internalKey for
// all the fileMetadata in f0 and f1.
func ikeyRange(icmp db.Comparer, f0, f1 []fileMetadata) (smallest, largest internalKey) {
	first := true
	for _, f := range [2][]fileMetadata{f0, f1} {
		for _, meta := range f {
			if first {
				first = false
				smallest, largest = meta.smallest, meta.largest
				continue
			}
			if icmp.Compare(meta.smallest, smallest) < 0 {
				smallest = meta.smallest
			}
			if icmp.Compare(meta.largest, largest) > 0 {
				largest = meta.largest
			}
		}
	}
	return smallest, largest
}

type byFileNum []fileMetadata

func (b byFileNum) Len() int           { return len(b) }
func (b byFileNum) Less(i, j int) bool { return b[i].fileNum < b[j].fileNum }
func (b byFileNum) Swap(i, j int)      { b[i], b[j] = b[j], b[i] }

type bySmallest struct {
	dat []fileMetadata
	cmp db.Comparer
}

func (b bySmallest) Len() int { return len(b.dat) }
func (b bySmallest) Less(i, j int) bool {
	return b.cmp.Compare(b.dat[i].smallest, b.dat[j].smallest) < 0
}
func (b bySmallest) Swap(i, j int) { b.dat[i], b.dat[j] = b.dat[j], b.dat[i] }

const numLevels = 7

// version is a collection of file metadata for on-disk tables at various
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
type version struct {
	files [numLevels][]fileMetadata
	// Every version is part of a circular doubly-linked list of versions.
	// One of those versions is a versionSet.dummyVersion.
	prev, next *version

	// These fields are the level that should be compacted next and its
	// compaction score. A score < 1 means that compaction is not strictly
	// needed.
	compactionScore float64
	compactionLevel int
}

// updateCompactionScore updates v's compaction score and level.
func (v *version) updateCompactionScore() {
	// We treat level-0 specially by bounding the number of files instead of
	// number of bytes for two reasons:
	//
	// (1) With larger write-buffer sizes, it is nice not to do too many
	// level-0 compactions.
	//
	// (2) The files in level-0 are merged on every read and therefore we
	// wish to avoid too many files when the individual file size is small
	// (perhaps because of a small write-buffer setting, or very high
	// compression ratios, or lots of overwrites/deletions).
	v.compactionScore = float64(len(v.files[0])) / l0CompactionTrigger
	v.compactionLevel = 0

	maxBytes := float64(10 * 1024 * 1024)
	for level := 1; level < numLevels-1; level++ {
		score := float64(totalSize(v.files[level])) / maxBytes
		if score > v.compactionScore {
			v.compactionScore = score
			v.compactionLevel = level
		}
		maxBytes *= 10
	}
}

// overlaps returns all elements of v.files[level] whose user key range
// intersects the inclusive range [ukey0, ukey1]. If level is non-zero then the
// user key ranges of v.files[level] are assumed to not overlap (although they
// may touch). If level is zero then that assumption cannot be made, and the
// [ukey0, ukey1] range is expanded to the union of those matching ranges so
// far and the computation is repeated until [ukey0, ukey1] stabilizes.
func (v *version) overlaps(level int, ucmp db.Comparer, ukey0, ukey1 []byte) (ret []fileMetadata) {
loop:
	for {
		for _, meta := range v.files[level] {
			m0 := meta.smallest.ukey()
			m1 := meta.largest.ukey()
			if ucmp.Compare(m1, ukey0) < 0 {
				// meta is completely before the specified range; skip it.
				continue
			}
			if ucmp.Compare(m0, ukey1) > 0 {
				// meta is completely after the specified range; skip it.
				continue
			}
			ret = append(ret, meta)

			// If level == 0, check if the newly added fileMetadata has
			// expanded the range. If so, restart the search.
			if level != 0 {
				continue
			}
			restart := false
			if ucmp.Compare(m0, ukey0) < 0 {
				ukey0 = m0
				restart = true
			}
			if ucmp.Compare(m1, ukey1) > 0 {
				ukey1 = m1
				restart = true
			}
			if restart {
				ret = ret[:0]
				continue loop
			}
		}
		return ret
	}
	panic("unreachable")
}

// checkOrdering checks that the files are consistent with respect to
// increasing file numbers (for level 0 files) and increasing and non-
// overlapping internal key ranges (for level non-0 files).
func (v *version) checkOrdering(icmp db.Comparer) error {
	for level, ff := range v.files {
		if level == 0 {
			prevFileNum := uint64(0)
			for i, f := range ff {
				if i != 0 && prevFileNum >= f.fileNum {
					return fmt.Errorf("level 0 files are not in increasing fileNum order: %d, %d", prevFileNum, f.fileNum)
				}
				prevFileNum = f.fileNum
			}
		} else {
			prevLargest := internalKey(nil)
			for i, f := range ff {
				if i != 0 && icmp.Compare(prevLargest, f.smallest) >= 0 {
					return fmt.Errorf("level non-0 files are not in increasing ikey order: %q, %q", prevLargest, f.smallest)
				}
				if icmp.Compare(f.smallest, f.largest) > 0 {
					return fmt.Errorf("level non-0 file has inconsistent bounds: %q, %q", f.smallest, f.largest)
				}
				prevLargest = f.largest
			}
		}
	}
	return nil
}

// tableIkeyFinder finds the given ikey in the table of the given file number.
type tableIkeyFinder interface {
	find(fileNum uint64, ikey internalKey) (db.Iterator, error)
}

// get looks up the internal key ikey0 in v's tables such that ikey and ikey0
// have the same user key, and ikey0's sequence number is the highest such
// sequence number that is less than or equal to ikey's sequence number.
//
// If ikey0's kind is set, the value for that previous set action is returned.
// If ikey0's kind is delete, the db.ErrNotFound error is returned.
// If there is no such ikey0, the db.ErrNotFound error is returned.
func (v *version) get(ikey internalKey, tiFinder tableIkeyFinder, ucmp db.Comparer, ro *db.ReadOptions) ([]byte, error) {
	ukey := ikey.ukey()
	// Iterate through v's tables, calling internalGet if the table's bounds
	// might contain ikey. Due to the order in which we search the tables, and
	// the internalKeyComparer's ordering within a table, we stop after the
	// first conclusive result.

	// Search the level 0 files in decreasing fileNum order,
	// which is also decreasing sequence number order.
	icmp := internalKeyComparer{ucmp}
	for i := len(v.files[0]) - 1; i >= 0; i-- {
		f := v.files[0][i]
		// We compare user keys on the low end, as we do not want to reject a table
		// whose smallest internal key may have the same user key and a lower sequence
		// number. An internalKeyComparer sorts increasing by user key but then
		// descending by sequence number.
		if ucmp.Compare(ukey, f.smallest.ukey()) < 0 {
			continue
		}
		// We compare internal keys on the high end. It gives a tighter bound than
		// comparing user keys.
		if icmp.Compare(ikey, f.largest) > 0 {
			continue
		}
		iter, err := tiFinder.find(f.fileNum, ikey)
		if err != nil {
			return nil, fmt.Errorf("leveldb: could not open table %d: %v", f.fileNum, err)
		}
		value, conclusive, err := internalGet(iter, ucmp, ukey)
		if conclusive {
			return value, err
		}
	}

	// Search the remaining levels.
	for level := 1; level < len(v.files); level++ {
		n := len(v.files[level])
		if n == 0 {
			continue
		}
		// Find the earliest file at that level whose largest key is >= ikey.
		index := sort.Search(n, func(i int) bool {
			return icmp.Compare(v.files[level][i].largest, ikey) >= 0
		})
		if index == n {
			continue
		}
		f := v.files[level][index]
		if ucmp.Compare(ukey, f.smallest.ukey()) < 0 {
			continue
		}
		iter, err := tiFinder.find(f.fileNum, ikey)
		if err != nil {
			return nil, fmt.Errorf("leveldb: could not open table %d: %v", f.fileNum, err)
		}
		value, conclusive, err := internalGet(iter, ucmp, ukey)
		if conclusive {
			return value, err
		}
	}
	return nil, db.ErrNotFound
}

// internalGet looks up the first key/value pair whose (internal) key is >=
// ikey, according to the internal key ordering, and also returns whether or
// not that search was conclusive.
//
// If there is no such pair, or that pair's key and ikey do not share the same
// user key (according to ucmp), then conclusive will be false. Otherwise,
// conclusive will be true and:
//	* if that pair's key's kind is set, that pair's value will be returned,
//	* if that pair's key's kind is delete, db.ErrNotFound will be returned.
// If the returned error is non-nil then conclusive will be true.
func internalGet(t db.Iterator, ucmp db.Comparer, ukey []byte) (value []byte, conclusive bool, err error) {
	if !t.Next() {
		err = t.Close()
		return nil, err != nil, err
	}
	ikey0 := internalKey(t.Key())
	if !ikey0.valid() {
		t.Close()
		return nil, true, fmt.Errorf("leveldb: corrupt table: invalid internal key")
	}
	if ucmp.Compare(ukey, ikey0.ukey()) != 0 {
		err = t.Close()
		return nil, err != nil, err
	}
	if ikey0.kind() == internalKeyKindDelete {
		t.Close()
		return nil, true, db.ErrNotFound
	}
	return t.Value(), true, t.Close()
}
