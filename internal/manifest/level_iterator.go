// Copyright 2020 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package manifest

import "sort"

// SliceLevelIterator constructs a LevelIterator over the provided slice.  This
// function is expected to be a temporary adapter between interfaces.
// TODO(jackson): Revisit once the conversion of Version.Files to a btree is
// complete.
func SliceLevelIterator(files []*FileMetadata) LevelIterator {
	return LevelIterator{files: files, cur: 0}
}

// LevelIterator iterates over a set of files' metadata.
type LevelIterator struct {
	files []*FileMetadata
	cur   int
}

// Collect returns a slice of all the files in the iterator, irrespective of
// the current iterator position. The returned slice is owned by the iterator.
// This method is intended to be a temporary adatpter between interfaces, and
// callers should prefer using the iterator directory when possible.
//
// TODO(jackson): Revisit once the conversion of Version.Files to a btree is
// complete.
func (i LevelIterator) Collect() []*FileMetadata {
	return i.files
}

// SizeSum sums the size of all files in the iterator, irrespective of the
// current iterator position.
func (i LevelIterator) SizeSum() uint64 {
	var sum uint64
	for _, f := range i.files {
		sum += f.Size
	}
	return sum
}

// Empty indicates whether there are remaining files in the iterator.
func (i LevelIterator) Empty() bool {
	return i.cur >= len(i.files)
}

// First seeks to the first file in the iterator and returns it.
func (i *LevelIterator) First() *FileMetadata {
	i.cur = 0
	if i.cur >= len(i.files) {
		return nil
	}
	return i.files[i.cur]
}

// Next advances the iterator to the next file and returns it.
func (i *LevelIterator) Next() *FileMetadata {
	i.cur++
	if i.cur >= len(i.files) {
		return nil
	}
	return i.files[i.cur]
}

// SeekGE seeks to the first file in the iterator's file set with a largest
// user key less than or equal to the provided user key.
func (i *LevelIterator) SeekGE(cmp Compare, userKey []byte) *FileMetadata {
	i.cur = sort.Search(len(i.files), func(j int) bool {
		return cmp(userKey, i.files[j].Largest.UserKey) <= 0
	})
	if i.cur >= len(i.files) {
		return nil
	}
	return i.files[i.cur]
}
