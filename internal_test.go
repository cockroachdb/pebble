// Copyright 2019 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package pebble

import (
	"bytes"
	"fmt"

	"github.com/petermattis/pebble/db"
)

// internalIterAdapter adapts the new internalIterator interface which returns
// the key and value from positioning methods (Seek*, First, Last, Next, Prev)
// to the old interface which returned a boolean corresponding to Valid. Only
// used by test code.
type internalIterAdapter struct {
	internalIterator
}

func (i *internalIterAdapter) verify(key *db.InternalKey, val []byte) bool {
	valid := key != nil
	if valid != i.Valid() {
		panic(fmt.Sprintf("inconsistent valid: %t != %t", valid, i.Valid()))
	}
	if valid {
		if db.InternalCompare(bytes.Compare, *key, i.Key()) != 0 {
			panic(fmt.Sprintf("inconsistent key: %s != %s", *key, i.Key()))
		}
		if !bytes.Equal(val, i.Value()) {
			panic(fmt.Sprintf("inconsistent value: [% x] != [% x]", val, i.Value()))
		}
	}
	return valid
}

func (i *internalIterAdapter) SeekGE(key []byte) bool {
	return i.verify(i.internalIterator.SeekGE(key))
}

func (i *internalIterAdapter) SeekPrefixGE(prefix, key []byte) bool {
	return i.verify(i.internalIterator.SeekPrefixGE(prefix, key))
}

func (i *internalIterAdapter) SeekLT(key []byte) bool {
	return i.verify(i.internalIterator.SeekLT(key))
}

func (i *internalIterAdapter) First() bool {
	return i.verify(i.internalIterator.First())
}

func (i *internalIterAdapter) Last() bool {
	return i.verify(i.internalIterator.Last())
}

func (i *internalIterAdapter) Next() bool {
	return i.verify(i.internalIterator.Next())
}

func (i *internalIterAdapter) Prev() bool {
	return i.verify(i.internalIterator.Prev())
}

func (i *internalIterAdapter) Key() db.InternalKey {
	return *i.internalIterator.Key()
}
