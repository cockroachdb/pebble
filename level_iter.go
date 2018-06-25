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

func (l *levelIter) init(cmp db.Compare, newIter tableNewIter, files []fileMetadata) {
	l.cmp = cmp
	l.index = -1
	l.newIter = newIter
	l.files = files
}

func (l *levelIter) findFile(key *db.InternalKey) int {
	// Find the earliest file whose largest key is >= ikey.
	index := sort.Search(len(l.files), func(i int) bool {
		return db.InternalCompare(l.cmp, l.files[i].largest, *key) >= 0
	})
	if index == len(l.files) {
		return len(l.files) - 1
	}
	return index
}

func (l *levelIter) loadFile(index int) bool {
	if l.index == index {
		return true
	}
	l.iter = nil
	l.index = index
	if l.index < 0 || l.index >= len(l.files) {
		return false
	}
	l.iter, l.err = l.newIter(l.files[l.index].fileNum)
	return l.err == nil
}

func (l *levelIter) SeekGE(key *db.InternalKey) {
	if l.loadFile(l.findFile(key)) {
		l.iter.SeekGE(key)
	}
}

func (l *levelIter) SeekLE(key *db.InternalKey) {
	if l.loadFile(l.findFile(key)) {
		l.iter.SeekLE(key)
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
	// Current file was exhausted. Move to the nxt file.
	if l.loadFile(l.index + 1) {
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

func (l *levelIter) Key() *db.InternalKey {
	if l.iter == nil {
		return nil
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
