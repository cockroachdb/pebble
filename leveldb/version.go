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

type tableOpener interface {
	openTable(fileNum uint64) (db.DB, error)
}

// get looks up the internal key k0 in v's tables such that k and k0 have the
// same user key, and k0's sequence number is the highest such value that is
// less than or equal to k's sequence number.
//
// If k0's kind is set, the user key for that previous set action is returned.
// If k0's kind is delete, the db.ErrNotFound error is returned.
// If there is no such k0, the db.ErrNotFound error is returned.
func (v *version) get(k internalKey, tOpener tableOpener, ucmp db.Comparer, ro *db.ReadOptions) ([]byte, error) {
	ukey := k.ukey()
	// get looks for k0 inside the on-disk table defined by f. Due to the order
	// in which we search the tables, and the internalKeyComparer's ordering
	// within a table, the first k0 we find is the one that we want.
	//
	// In addition to returning a []byte and an error, it returns whether or
	// not the search was conclusive: whether it found a k0, or encountered a
	// corruption error. If the search was not conclusive, we move on to the
	// next table.
	get := func(f fileMetadata) (val []byte, conclusive bool, err error) {
		b, err := tOpener.openTable(f.fileNum)
		if err != nil {
			return nil, true, err
		}
		defer b.Close()
		t := b.Find(k, ro)
		if !t.Next() {
			err = t.Close()
			return nil, err != nil, err
		}
		k0 := internalKey(t.Key())
		if !k0.valid() {
			return nil, true, fmt.Errorf("leveldb: corrupt table %d: invalid internal key", f.fileNum)
		}
		if ucmp.Compare(ukey, k0.ukey()) != 0 {
			err = t.Close()
			return nil, err != nil, err
		}
		if k0.kind() == internalKeyKindDelete {
			return nil, true, db.ErrNotFound
		}
		return t.Value(), true, t.Close()
	}

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
		if icmp.Compare(k, f.largest) > 0 {
			continue
		}
		val, conclusive, err := get(f)
		if conclusive {
			return val, err
		}
	}

	// Search the remaining levels.
	for level := 1; level < len(v.files); level++ {
		n := len(v.files[level])
		if n == 0 {
			continue
		}
		// Find the earliest file at that level whose largest key is >= k.
		index := sort.Search(n, func(i int) bool {
			return icmp.Compare(v.files[level][i].largest, k) >= 0
		})
		if index == n {
			continue
		}
		f := v.files[level][index]
		if ucmp.Compare(ukey, f.smallest.ukey()) < 0 {
			continue
		}
		val, conclusive, err := get(f)
		if conclusive {
			return val, err
		}
	}
	return nil, db.ErrNotFound
}
