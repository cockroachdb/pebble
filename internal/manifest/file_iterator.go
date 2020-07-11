// Copyright 2020 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package manifest

import "sort"

// SliceFileIterator constructs a FileIterator over the provided slice.  This
// function is expected to be a temporary adapter between interfaces.
// TODO(jackson): Revisit once the conversion of Version.Files to a btree is
// complete.
func SliceFileIterator(files []*FileMetadata) FileIterator {
	return FileIterator{files: files, cur: 0}
}

// FileIterator iterates over a set of files' metadata.
type FileIterator struct {
	files []*FileMetadata
	cur   int
}

// Collect returns a slice of all the files in the iterator, irrespective of
// the current iterator position. The returned slice is owned by the iterator.
func (i FileIterator) Collect() []*FileMetadata {
	return i.files
}

// Len returns the length of the iterator, from its current position to end.
func (i FileIterator) Len() int {
	return len(i.files) - i.cur
}

// First seeks to the first file in the iterator and returns it.
func (i *FileIterator) First() *FileMetadata {
	i.cur = 0
	if i.cur >= len(i.files) {
		return nil
	}
	return i.files[i.cur]
}

// Next advances the iterator to the next file and returns it.
func (i *FileIterator) Next() *FileMetadata {
	i.cur++
	if i.cur >= len(i.files) {
		return nil
	}
	return i.files[i.cur]
}

// SeekGE seeks to the first file in the iterator's file set with a largest
// user key less than or equal to the provided user key.
func (i *FileIterator) SeekGE(cmp Compare, userKey []byte) *FileMetadata {
	i.cur = sort.Search(len(i.files), func(j int) bool {
		return cmp(userKey, i.files[j].Largest.UserKey) <= 0
	})
	if i.cur >= len(i.files) {
		return nil
	}
	return i.files[i.cur]
}
