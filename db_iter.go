// Copyright 2018 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package pebble

import (
	"fmt"

	"github.com/petermattis/pebble/db"
)

type dbIterPos int8

const (
	dbIterCur  dbIterPos = 0
	dbIterNext dbIterPos = 1
	dbIterPrev dbIterPos = -1
)

type dbIter struct {
	opts     *db.IterOptions
	cmp      db.Compare
	merge    db.Merge
	iter     db.InternalIterator
	seqNum   uint64
	version  *version
	err      error
	key      []byte
	keyBuf   []byte
	value    []byte
	valueBuf []byte
	valid    bool
	pos      dbIterPos
}

var _ db.Iterator = (*dbIter)(nil)

func (i *dbIter) findNextEntry() bool {
	upperBound := i.opts.GetUpperBound()
	i.valid = false
	i.pos = dbIterCur

	for i.iter.Valid() {
		key := i.iter.Key()
		if upperBound != nil && i.cmp(key.UserKey, upperBound) >= 0 {
			break
		}

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
			i.key = key.UserKey
			i.value = i.iter.Value()
			i.valid = true
			return true

		case db.InternalKeyKindMerge:
			return i.mergeNext()

		default:
			i.err = fmt.Errorf("invalid internal key kind: %d", key.Kind())
			return false
		}
	}

	return false
}

func (i *dbIter) findPrevEntry() bool {
	lowerBound := i.opts.GetLowerBound()
	i.valid = false
	i.pos = dbIterCur

	for i.iter.Valid() {
		key := i.iter.Key()
		if lowerBound != nil && i.cmp(key.UserKey, lowerBound) < 0 {
			break
		}

		if seqNum := key.SeqNum(); seqNum > i.seqNum {
			// Ignore entries that are newer than our snapshot sequence number,
			// except for batch sequence numbers which are always visible.
			if (seqNum & db.InternalKeySeqNumBatch) == 0 {
				i.iter.Prev()
				continue
			}
		}
		switch key.Kind() {
		case db.InternalKeyKindDelete:
			i.iter.PrevUserKey()
			continue

		case db.InternalKeyKindSet:
			i.key = key.UserKey
			i.value = i.iter.Value()
			i.valid = true
			return true

		case db.InternalKeyKindMerge:
			return i.mergePrev()

		default:
			i.err = fmt.Errorf("invalid internal key kind: %d", key.Kind())
			return false
		}
	}

	return false
}

func (i *dbIter) mergeNext() bool {
	// Save the current key and value.
	i.keyBuf = append(i.keyBuf[:0], i.iter.Key().UserKey...)
	i.valueBuf = append(i.valueBuf[:0], i.iter.Value()...)
	i.key, i.value = i.keyBuf, i.valueBuf
	i.valid = true

	// Loop looking for older values for this key and merging them.
	for {
		i.iter.Next()
		if !i.iter.Valid() {
			i.pos = dbIterNext
			return true
		}
		key := i.iter.Key()
		if i.cmp(i.key, key.UserKey) != 0 {
			// We've advanced to the next key.
			i.pos = dbIterNext
			return true
		}
		switch key.Kind() {
		case db.InternalKeyKindDelete:
			// We've hit a deletion tombstone. Return everything up to this
			// point.
			return true

		case db.InternalKeyKindSet:
			// We've hit a Set value. Merge with the existing value and return.
			i.value = i.merge(i.key, i.value, i.iter.Value(), nil)
			return true

		case db.InternalKeyKindMerge:
			// We've hit another Merge value. Merge with the existing value and
			// continue looping.
			i.value = i.merge(i.key, i.value, i.iter.Value(), nil)

		default:
			i.err = fmt.Errorf("invalid internal key kind: %d", key.Kind())
			return false
		}
	}
}

func (i *dbIter) mergePrev() bool {
	// Save the current key and value.
	i.keyBuf = append(i.keyBuf[:0], i.iter.Key().UserKey...)
	i.valueBuf = append(i.valueBuf[:0], i.iter.Value()...)
	i.key, i.value = i.keyBuf, i.valueBuf
	i.valid = true

	// Loop looking for older values for this key and merging them.
	for {
		i.iter.Prev()
		if !i.iter.Valid() {
			i.pos = dbIterPrev
			return true
		}
		key := i.iter.Key()
		if i.cmp(i.key, key.UserKey) != 0 {
			// We've advanced to the previous key.
			i.pos = dbIterPrev
			return true
		}
		switch key.Kind() {
		case db.InternalKeyKindDelete:
			// We've hit a deletion tombstone. Return everything up to this
			// point.
			return true

		case db.InternalKeyKindSet:
			// We've hit a Set value. Merge with the existing value and return.
			i.value = i.merge(i.key, i.value, i.iter.Value(), nil)
			return true

		case db.InternalKeyKindMerge:
			// We've hit another Merge value. Merge with the existing value and
			// continue looping.
			i.value = i.merge(i.key, i.value, i.iter.Value(), nil)

		default:
			i.err = fmt.Errorf("invalid internal key kind: %d", key.Kind())
			return false
		}
	}
}

func (i *dbIter) SeekGE(key []byte) {
	if i.err != nil {
		return
	}

	if lowerBound := i.opts.GetLowerBound(); lowerBound != nil && i.cmp(key, lowerBound) < 0 {
		key = lowerBound
	}

	i.iter.SeekGE(key)
	i.findNextEntry()
}

func (i *dbIter) SeekLT(key []byte) {
	if i.err != nil {
		return
	}

	if upperBound := i.opts.GetUpperBound(); upperBound != nil && i.cmp(key, upperBound) >= 0 {
		key = upperBound
	}

	i.iter.SeekLT(key)
	i.findPrevEntry()
}

func (i *dbIter) First() {
	if i.err != nil {
		return
	}

	if lowerBound := i.opts.GetLowerBound(); lowerBound != nil {
		i.SeekGE(lowerBound)
		return
	}

	i.iter.First()
	i.findNextEntry()
}

func (i *dbIter) Last() {
	if i.err != nil {
		return
	}

	if upperBound := i.opts.GetUpperBound(); upperBound != nil {
		i.SeekLT(upperBound)
		return
	}

	i.iter.Last()
	i.findPrevEntry()
}

func (i *dbIter) Next() bool {
	if i.err != nil {
		return false
	}
	switch i.pos {
	case dbIterCur:
		i.iter.NextUserKey()
	case dbIterPrev:
		i.iter.NextUserKey()
		i.iter.NextUserKey()
	case dbIterNext:
	}
	return i.findNextEntry()
}

func (i *dbIter) Prev() bool {
	if i.err != nil {
		return false
	}
	switch i.pos {
	case dbIterCur:
		i.iter.PrevUserKey()
	case dbIterNext:
		i.iter.PrevUserKey()
		i.iter.PrevUserKey()
	case dbIterPrev:
	}
	return i.findPrevEntry()
}

func (i *dbIter) Key() []byte {
	return i.key
}

func (i *dbIter) Value() []byte {
	return i.value
}

func (i *dbIter) Valid() bool {
	return i.valid
}

func (i *dbIter) Error() error {
	return i.err
}

func (i *dbIter) Close() error {
	if i.version != nil {
		i.version.unref()
		i.version = nil
	}
	return i.err
}
