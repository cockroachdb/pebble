package pebble

import (
	"sort"

	"github.com/petermattis/pebble/db"
)

type levelIter struct {
	cmp       db.Compare
	index     int
	iter      db.InternalIterator
	iterMaker tableIterMaker
	files     []fileMetadata
	err       error
}

// levelIter implements the db.InternalIterator interface.
var _ db.InternalIterator = (*levelIter)(nil)

func (l *levelIter) findFileGE(key *db.InternalKey) int {
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
	l.index = index
	l.iter, l.err = l.iterMaker.newIter(l.files[l.index].fileNum)
	if l.err != nil {
		return false
	}
	return true
}

func (l *levelIter) SeekGE(key *db.InternalKey) {
	if l.loadFile(l.findFileGE(key)) {
		l.iter.SeekGE(key)
	}
}

func (l *levelIter) SeekLE(key *db.InternalKey) {
	// TODO(peter): findFileLE?
	if l.loadFile(l.findFileGE(key)) {
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
	if l.iter != nil {
		if l.iter.Next() {
			return true
		}
	}

	// TODO(peter): find the next file.
	panic("pebble: Next unimplemented")
}

func (l *levelIter) Prev() bool {
	if l.err != nil {
		return false
	}
	if l.iter != nil {
		if l.iter.Prev() {
			return true
		}
	}

	// TODO(peter): find the prev file.
	panic("pebble: Prev unimplemented")
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
	if l.err != nil {
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
