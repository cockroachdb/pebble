// Copyright 2018 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package pebble

import (
	"sort"

	"github.com/petermattis/pebble/db"
)

type levelIter struct {
	cmp     db.Compare
	index   int
	iter    db.InternalIterator
	newIter tableNewIter
	files   []fileMetadata
	err     error
}

// levelIter implements the db.InternalIterator interface.
var _ db.InternalIterator = (*levelIter)(nil)

func newLevelIter(cmp db.Compare, newIter tableNewIter, files []fileMetadata) *levelIter {
	l := &levelIter{}
	l.init(cmp, newIter, files)
	return l
}

func (l *levelIter) init(cmp db.Compare, newIter tableNewIter, files []fileMetadata) {
	l.cmp = cmp
	l.index = -1
	l.newIter = newIter
	l.files = files
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

func (l *levelIter) loadFile(index int) bool {
	if l.index == index {
		return true
	}
	if l.iter != nil {
		l.err = l.iter.Close()
		if l.err != nil {
			return false
		}
		l.iter = nil
	}
	l.index = index
	if l.index < 0 || l.index >= len(l.files) {
		return false
	}
	l.iter, l.err = l.newIter(l.files[l.index].fileNum)
	return l.err == nil
}

func (l *levelIter) SeekGE(key []byte) {
	if l.loadFile(l.findFileGE(key)) {
		l.iter.SeekGE(key)
	}
}

func (l *levelIter) SeekLT(key []byte) {
	if l.loadFile(l.findFileLT(key)) {
		l.iter.SeekLT(key)
	}
}

func (l *levelIter) First() {
	if l.loadFile(0) {
		l.iter.First()
	}
}

func (l *levelIter) Last() {
	if l.loadFile(len(l.files) - 1) {
		l.iter.Last()
	}
}

func (l *levelIter) Next() bool {
	if l.err != nil {
		return false
	}
	if l.iter == nil {
		if l.index == -1 && l.loadFile(0) {
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
	// Current file was exhausted. Move to the next file.
	if l.loadFile(l.index + 1) {
		l.iter.First()
		return true
	}
	return false
}

func (l *levelIter) NextUserKey() bool {
	return l.Next()
}

func (l *levelIter) Prev() bool {
	if l.err != nil {
		return false
	}
	if l.iter == nil {
		if n := len(l.files); l.index == n && l.loadFile(n-1) {
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
	// Current file was exhausted. Move to the previous file.
	if l.loadFile(l.index - 1) {
		l.iter.Last()
		return true
	}
	return false
}

func (l *levelIter) PrevUserKey() bool {
	return l.Prev()
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
