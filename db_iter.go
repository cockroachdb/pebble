package pebble

import (
	"fmt"

	"github.com/petermattis/pebble/db"
)

type dbIter struct {
	cmp    db.Compare
	iter   db.InternalIterator
	seqNum uint64
	err    error
}

var _ db.Iterator = (*dbIter)(nil)

func (i *dbIter) findNextEntry() bool {
	for i.iter.Valid() {
		key := i.iter.Key()
		if seqNum := key.SeqNum(); seqNum > i.seqNum {
			// Ignore entries that are newer than our snapshot sequence number,
			// except for batch sequence numbers which are always visible.
			if (seqNum & db.InternalKeySeqNumBatch) == 0 {
				i.iter.Next()
				continue
			}
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
		if seqNum := key.SeqNum(); seqNum > i.seqNum {
			// Ignore entries that are newer than our snapshot sequence number,
			// except for batch sequence numbers which are always visible.
			if (seqNum & db.InternalKeySeqNumBatch) == 0 {
				i.iter.Next()
				continue
			}
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
	if i.err != nil {
		return
	}
	i.iter.SeekGE(key)
	i.findNextEntry()
}

func (i *dbIter) SeekLT(key []byte) {
	if i.err != nil {
		return
	}
	i.iter.SeekLT(key)
	i.findPrevEntry()
}

func (i *dbIter) First() {
	if i.err != nil {
		return
	}
	i.iter.First()
	i.findNextEntry()
}

func (i *dbIter) Last() {
	if i.err != nil {
		return
	}
	i.iter.Last()
	i.findPrevEntry()
}

func (i *dbIter) Next() bool {
	if i.err != nil {
		return false
	}
	i.iter.NextUserKey()
	return i.findNextEntry()
}

func (i *dbIter) Prev() bool {
	if i.err != nil {
		return false
	}
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
