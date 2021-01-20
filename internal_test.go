// Copyright 2019 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package pebble

// internalIterAdapter adapts the new internalIterator interface which returns
// the key and value from positioning methods (Seek*, First, Last, Next, Prev)
// to the old interface which returned a boolean corresponding to Valid. Only
// used by test code.
type internalIterAdapter struct {
	internalIterator
	key *InternalKey
	val []byte
}

func newInternalIterAdapter(iter internalIterator) *internalIterAdapter {
	return &internalIterAdapter{
		internalIterator: iter,
	}
}

func (i *internalIterAdapter) update(key *InternalKey, val []byte) bool {
	i.key = key
	i.val = val
	return i.key != nil
}

func (i *internalIterAdapter) String() string {
	return "internal-iter-adapter"
}

func (i *internalIterAdapter) SeekGE(key []byte) bool {
	return i.update(i.internalIterator.SeekGE(key))
}

func (i *internalIterAdapter) SeekPrefixGE(prefix, key []byte, trySeekUsingNext bool) bool {
	return i.update(i.internalIterator.SeekPrefixGE(prefix, key, trySeekUsingNext))
}

func (i *internalIterAdapter) SeekLT(key []byte) bool {
	return i.update(i.internalIterator.SeekLT(key))
}

func (i *internalIterAdapter) First() bool {
	return i.update(i.internalIterator.First())
}

func (i *internalIterAdapter) Last() bool {
	return i.update(i.internalIterator.Last())
}

func (i *internalIterAdapter) Next() bool {
	return i.update(i.internalIterator.Next())
}

func (i *internalIterAdapter) Prev() bool {
	return i.update(i.internalIterator.Prev())
}

func (i *internalIterAdapter) Key() *InternalKey {
	return i.key
}

func (i *internalIterAdapter) Value() []byte {
	return i.val
}

func (i *internalIterAdapter) Valid() bool {
	return i.key != nil
}
