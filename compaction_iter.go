// Copyright 2018 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package pebble

import (
	"fmt"

	"github.com/petermattis/pebble/db"
)

type compactionIterPos int8

const (
	compactionIterCur  compactionIterPos = 0
	compactionIterNext compactionIterPos = 1
)

type compactionIter struct {
	cmp                db.Compare
	merge              db.Merge
	iter               db.InternalIterator
	err                error
	key                db.InternalKey
	keyBuf             []byte
	value              []byte
	valueBuf           []byte
	valid              bool
	pos                compactionIterPos
	isBaseLevelForUkey func(cmp db.Compare, ukey []byte) bool
}

// TODO(peter): Add support for snapshots. Snapshots complicate the rules for
// when to output entries. Rather than outputting a single entry per-user key,
// we have to output the newest entry that is older than a snapshot.
//
// Whenever an entries sequence number is older than a snapshot, but the
// previous sequence number for the same user key was newer than the snapshot,
// we need to stop accumulating the current entry. We're looking for the
// pattern "a#X, snap#Y, a#Z" where X > Y >= Z. Another way to look at this is
// that compaction compresses keys which are shadowed, yet snapshots prevent
// shadowing. For example, consider the sequence of entries for the key a:
//
//   put#10
//   del#8
//   put#7
//   del#4
//   put#3
//
// In the absence of any snapshots, during compaction this would compress to:
//
//   put#10
//
// What if we have a snapshot at sequence number 11? Nothing changes as all of
// the operations are older than the snapshot:
//
//   put#10
//
// There is similar behavior if there is a snapshot at sequence number 2. More
// interesting is what happens if there is a snapshot at sequence number 5. The
// series of operations between [inf, 5) are compressed, and so are the
// operations between [5, 0):
//
//   put#10
//   del#4
//
// compactionIter can perform this processing by maintaining a slice containing
// all of the snapshot sequence numbers ordered from newest to oldest. When an
// entry is being processed, a binary search is performed to find the "covering
// snapshot" (i.e. the oldest snapshot that is newer than entry's sequence
// number). If the covering snapshot differs from the previous entry's covering
// snapshot, then we'll need to emit a new entry and thus processing for the
// current entry is terminated.

func (i *compactionIter) findNextEntry() bool {
	i.valid = false
	i.pos = compactionIterCur

	for i.iter.Valid() {
		i.key = i.iter.Key()
		switch i.key.Kind() {
		case db.InternalKeyKindDelete:
			if i.isBaseLevelForUkey(i.cmp, i.key.UserKey) {
				i.iter.NextUserKey()
				continue
			}
			i.value = i.iter.Value()
			i.valid = true
			return true

		case db.InternalKeyKindSet:
			i.value = i.iter.Value()
			i.valid = true
			return true

		case db.InternalKeyKindMerge:
			return i.mergeNext()

		default:
			i.err = fmt.Errorf("invalid internal key kind: %d", i.key.Kind())
			return false
		}
	}

	return false
}

func (i *compactionIter) mergeNext() bool {
	// Save the current key and value.
	i.keyBuf = append(i.keyBuf[:0], i.iter.Key().UserKey...)
	i.valueBuf = append(i.valueBuf[:0], i.iter.Value()...)
	i.key.UserKey, i.value = i.keyBuf, i.valueBuf
	i.valid = true

	// Loop looking for older values for this key and merging them.
	for {
		i.iter.Next()
		if !i.iter.Valid() {
			i.pos = compactionIterNext
			return true
		}
		key := i.iter.Key()
		if i.cmp(i.key.UserKey, key.UserKey) != 0 {
			// We've advanced to the next key.
			i.pos = compactionIterNext
			return true
		}
		switch key.Kind() {
		case db.InternalKeyKindDelete:
			// We've hit a deletion tombstone. Return everything up to this
			// point.
			return true

		case db.InternalKeyKindSet:
			// We've hit a Set value. Merge with the existing value and return. We
			// change the kind of the resulting key to a Set so that it shadows keys
			// in lower levels. That is, MERGE+MERGE+SET -> SET.
			i.value = i.merge(i.key.UserKey, i.value, i.iter.Value(), nil)
			i.key.SetKind(db.InternalKeyKindSet)
			return true

		case db.InternalKeyKindMerge:
			// We've hit another Merge value. Merge with the existing value and
			// continue looping.
			i.value = i.merge(i.key.UserKey, i.value, i.iter.Value(), nil)

		default:
			i.err = fmt.Errorf("invalid internal key kind: %d", i.key.Kind())
			return false
		}
	}
}

func (i *compactionIter) First() {
	if i.err != nil {
		return
	}
	i.iter.First()
	i.findNextEntry()
}

func (i *compactionIter) Next() bool {
	if i.err != nil {
		return false
	}
	switch i.pos {
	case compactionIterCur:
		// TODO(peter): Rather than calling NextUserKey here, we should advance the
		// iterator manually to the next key looking for any entries which have
		// invalid keys and returning them.
		i.iter.NextUserKey()
	case compactionIterNext:
	}
	return i.findNextEntry()
}

func (i *compactionIter) Key() db.InternalKey {
	return i.key
}

func (i *compactionIter) Value() []byte {
	return i.value
}

func (i *compactionIter) Valid() bool {
	return i.valid
}

func (i *compactionIter) Error() error {
	return i.err
}

func (i *compactionIter) Close() error {
	return i.err
}
