package pebble

import (
	"fmt"

	"github.com/petermattis/pebble/db"
)

type dbIter struct {
	cmp    db.Compare
	iter   db.InternalIterator
	ikey   db.InternalKey
	seqNum uint64
	err    error
}

var _ db.Iterator = (*dbIter)(nil)

func (i *dbIter) findNextEntry() bool {
	for i.iter.Valid() {
		key := i.iter.Key()
		if key.SeqNum() > i.seqNum {
			// Ignore entries that are newer than our snapshot seqNum.
			i.iter.Next()
			continue
		}
		switch key.Kind() {
		case db.InternalKeyKindDelete:
			i.iter.NextUserKey()
			continue

		case db.InternalKeyKindSet:
			return true

		default:
			i.err = fmt.Errorf("invalid internal key kind: %d", key.Kind())
			return false
		}
	}
	return false
}

func (i *dbIter) findPrevEntry() bool {
	for i.iter.Valid() {
		key := i.iter.Key()
		if key.SeqNum() > i.seqNum {
			// Ignore entries that are newer than our snapshot seqNum.
			i.iter.Prev()
			continue
		}
		switch key.Kind() {
		case db.InternalKeyKindDelete:
			i.iter.PrevUserKey()
			continue

		case db.InternalKeyKindSet:
			return true

		default:
			i.err = fmt.Errorf("invalid internal key kind: %d", key.Kind())
			return false
		}
	}
	return false
}

func (i *dbIter) SeekGE(key []byte) {
	i.ikey = db.MakeInternalKey(key, db.InternalKeySeqNumMax, db.InternalKeyKindMax)
	i.iter.SeekGE(&i.ikey)
	i.ikey.UserKey = nil
	i.findNextEntry()
}

func (i *dbIter) SeekLT(key []byte) {
	i.ikey = db.MakeInternalKey(key, db.InternalKeySeqNumMax, db.InternalKeyKindMax)
	i.iter.SeekLT(&i.ikey)
	i.ikey.UserKey = nil
	i.findPrevEntry()
}

func (i *dbIter) First() {
	i.iter.First()
	i.findNextEntry()
}

func (i *dbIter) Last() {
	i.iter.Last()
	i.findPrevEntry()
}

func (i *dbIter) Next() bool {
	i.iter.NextUserKey()
	return i.findNextEntry()
}

func (i *dbIter) Prev() bool {
	i.iter.PrevUserKey()
	return i.findPrevEntry()
}

func (i *dbIter) Key() []byte {
	return i.iter.Key().UserKey
}

func (i *dbIter) Value() []byte {
	return i.iter.Value()
}

func (i *dbIter) Valid() bool {
	return i.iter.Valid()
}

func (i *dbIter) Error() error {
	return i.err
}

func (i *dbIter) Close() error {
	return i.err
}
