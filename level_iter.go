package pebble

import (
	"sort"

	"github.com/petermattis/pebble/db"
)

type levelIter struct {
	cmp     db.Compare
	titer   db.InternalIterator
	tiMaker tableIterMaker
	files   []fileMetadata
	err     error
}

// levelIter implements the db.InternalIterator interface.
var _ db.InternalIterator = (*levelIter)(nil)

func (l *levelIter) findFileGE(key *db.InternalKey) *fileMetadata {
	// Find the earliest file at that level whose largest key is >= ikey.
	index := sort.Search(len(l.files), func(i int) bool {
		return db.InternalCompare(l.cmp, l.files[i].largest, *key) >= 0
	})
	if index == len(l.files) {
		return &l.files[len(l.files)-1]
	}
	return &l.files[index]
}

func (l *levelIter) SeekGE(key *db.InternalKey) {
	f := l.findFileGE(key)
	l.titer, l.err = l.tiMaker.newIter(f.fileNum)
	if l.err != nil {
		return
	}
	l.titer.SeekGE(key)
}

func (l *levelIter) SeekLE(key *db.InternalKey) {
	panic("pebble: SeekLE unimplemented")
}

func (l *levelIter) First() {
	f := &l.files[0]
	l.titer, l.err = l.tiMaker.newIter(f.fileNum)
	if l.err != nil {
		return
	}
	l.titer.First()
}

func (l *levelIter) Last() {
	f := &l.files[len(l.files)-1]
	l.titer, l.err = l.tiMaker.newIter(f.fileNum)
	if l.err != nil {
		return
	}
	l.titer.Last()
}

func (l *levelIter) Next() bool {
	if l.err != nil {
		return false
	}
	if l.titer != nil {
		if l.titer.Next() {
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
	if l.titer != nil {
		if l.titer.Prev() {
			return true
		}
	}

	// TODO(peter): find the prev file.
	panic("pebble: Prev unimplemented")
}

func (l *levelIter) Key() *db.InternalKey {
	if l.titer == nil {
		return nil
	}
	return l.titer.Key()
}

func (l *levelIter) Value() []byte {
	if l.titer == nil {
		return nil
	}
	return l.titer.Value()
}

func (l *levelIter) Valid() bool {
	if l.titer == nil {
		return false
	}
	return l.titer.Valid()
}

func (l *levelIter) Error() error {
	if l.err != nil {
		return l.err
	}
	return l.titer.Error()
}

func (l *levelIter) Close() error {
	if l.titer != nil {
		l.err = l.titer.Close()
		l.titer = nil
	}
	return l.err
}
