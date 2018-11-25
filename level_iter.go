// Copyright 2018 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package pebble

import (
	"sort"

	"github.com/petermattis/pebble/db"
)

// tableNewIter creates a new iterator for the given file number.
type tableNewIter func(meta *fileMetadata) (internalIterator, error)

// levelIter provides a merged view of the sstables in a level.
//
// TODO(peter): Level iteration needs to "pause" at sstable if a range deletion
// tombstone is the cause of that boundary. We know if a range tombstone is the
// smallest or largest key in a file because the kind will be
// InternalKeyKindRangeDeletion. If the lower boundary is a range deletion
// tombstone, we materialize a fake point deletion at that key and sequence
// number. This is safe because the lower bounds of a range deletion tombstone
// is inclusive so that key is already deleted at that sequence number. For the
// upper bounds we return the range tombstone sentinel key. dbIter treats the
// range tombstone sentinel as a no-op.
type levelIter struct {
	opts     *db.IterOptions
	cmp      db.Compare
	index    int
	iter     internalIterator
	newIter  tableNewIter
	rangeDel *rangeDelLevel
	files    []fileMetadata
	err      error
}

// levelIter implements the internalIterator interface.
var _ internalIterator = (*levelIter)(nil)

func newLevelIter(
	opts *db.IterOptions, cmp db.Compare, newIter tableNewIter, files []fileMetadata,
) *levelIter {
	l := &levelIter{}
	l.init(opts, cmp, newIter, files)
	return l
}

func (l *levelIter) init(
	opts *db.IterOptions, cmp db.Compare, newIter tableNewIter, files []fileMetadata,
) {
	l.opts = opts
	l.cmp = cmp
	l.index = -1
	l.newIter = newIter
	l.files = files
}

func (l *levelIter) initRangeDel(rangeDel *rangeDelLevel) {
	l.rangeDel = rangeDel
}

func (l *levelIter) findFileGE(key []byte) int {
	// Find the earliest file whose largest key is >= ikey.
	index := sort.Search(len(l.files), func(i int) bool {
		return l.cmp(l.files[i].largest.UserKey, key) >= 0
	})
	if index == len(l.files) {
		return len(l.files) - 1
	}
	return index
}

func (l *levelIter) findFileLT(key []byte) int {
	// Find the last file whose smallest key is < ikey.
	index := sort.Search(len(l.files), func(i int) bool {
		return l.cmp(l.files[i].smallest.UserKey, key) >= 0
	})
	if index == 0 {
		return index
	}
	return index - 1
}

func (l *levelIter) loadFile(index, dir int) bool {
	if l.index == index {
		return l.iter != nil
	}
	if l.iter != nil {
		l.err = l.iter.Close()
		if l.err != nil {
			return false
		}
		l.iter = nil
	}

	for ; ; index += dir {
		l.index = index
		if l.index < 0 || l.index >= len(l.files) {
			return false
		}

		f := &l.files[l.index]
		if lowerBound := l.opts.GetLowerBound(); lowerBound != nil {
			if l.cmp(f.largest.UserKey, lowerBound) < 0 {
				// The largest key in the sstable is smaller than the lower bound.
				if dir < 0 {
					return false
				}
				continue
			}
		}
		if upperBound := l.opts.GetUpperBound(); upperBound != nil {
			if l.cmp(f.smallest.UserKey, upperBound) >= 0 {
				// The smallest key in the sstable is greater than or equal to the
				// lower bound.
				if dir > 0 {
					return false
				}
				continue
			}
		}

		// TODO(peter): If the table is entirely covered by a range deletion
		// tombstone, skip it.

		l.iter, l.err = l.newIter(f)
		if l.err != nil || l.iter == nil {
			return false
		}
		if l.rangeDel != nil {
			if err := l.rangeDel.load(f); err != nil {
				l.err = err
				return false
			}
		}
		return true
	}
}

func (l *levelIter) SeekGE(key []byte) {
	// NB: the top-level dbIter has already adjusted key based on
	// IterOptions.LowerBound.
	if l.loadFile(l.findFileGE(key), 1) {
		l.iter.SeekGE(key)
	}
}

func (l *levelIter) SeekLT(key []byte) {
	// NB: the top-level dbIter has already adjusted key based on
	// IterOptions.UpperBound.
	if l.loadFile(l.findFileLT(key), -1) {
		l.iter.SeekLT(key)
	}
}

func (l *levelIter) First() {
	// NB: the top-level dbIter will call SeekGE if IterOptions.LowerBound is
	// set.
	if l.loadFile(0, 1) {
		l.iter.First()
	}
}

func (l *levelIter) Last() {
	// NB: the top-level dbIter will call SeekLT if IterOptions.UpperBound is
	// set.
	if l.loadFile(len(l.files)-1, -1) {
		l.iter.Last()
	}
}

func (l *levelIter) Next() bool {
	if l.err != nil {
		return false
	}

	if l.iter == nil {
		if l.index == -1 && l.loadFile(0, 1) {
			// The iterator was positioned off the beginning of the level. Position
			// at the first entry.
			l.iter.First()
			return true
		}
		return false
	}

	if l.iter.Next() {
		return true
	}

	// TODO(peter): Determine if a boundary sentinel key needs to be returned.

	// Current file was exhausted. Move to the next file.
	if l.loadFile(l.index+1, 1) {
		l.iter.First()
		return true
	}
	return false
}

func (l *levelIter) Prev() bool {
	if l.err != nil {
		return false
	}

	if l.iter == nil {
		if n := len(l.files); l.index == n && l.loadFile(n-1, -1) {
			// The iterator was positioned off the end of the level. Position at the
			// last entry.
			l.iter.Last()
			return true
		}
		return false
	}

	if l.iter.Prev() {
		return true
	}

	// TODO(peter): Determine if a boundary sentinel key needs to be returned.

	// Current file was exhausted. Move to the previous file.
	if l.loadFile(l.index-1, -1) {
		l.iter.Last()
		return true
	}
	return false
}

func (l *levelIter) Key() db.InternalKey {
	if l.iter == nil {
		return db.InvalidInternalKey
	}
	return l.iter.Key()
}

func (l *levelIter) Value() []byte {
	if l.iter == nil {
		return nil
	}
	return l.iter.Value()
}

func (l *levelIter) Valid() bool {
	if l.iter == nil {
		return false
	}
	return l.iter.Valid()
}

func (l *levelIter) Error() error {
	if l.err != nil || l.iter == nil {
		return l.err
	}
	return l.iter.Error()
}

func (l *levelIter) Close() error {
	if l.iter != nil {
		l.err = l.iter.Close()
		l.iter = nil
	}
	return l.err
}
