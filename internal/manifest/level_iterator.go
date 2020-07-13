// Copyright 2020 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package manifest

import "sort"

// LevelMetadata contains metadata for all of the files within
// a level of the LSM.
// TODO(jackson): Convert to an opaque struct.
type LevelMetadata []*FileMetadata

// Iter constructs a LevelIterator over the entire level.
func (lm LevelMetadata) Iter() LevelIterator {
	return LevelIterator{files: lm, end: len(lm)}
}

// SliceLevelIterator constructs a LevelIterator over the provided slice.  This
// function is expected to be a temporary adapter between interfaces.
// TODO(jackson): Revisit once the conversion of Version.Files to a btree is
// complete.
func SliceLevelIterator(files []*FileMetadata) LevelIterator {
	return LevelIterator{files: files, cur: 0, end: len(files)}
}

// LevelIterator iterates over a set of files' metadata. Its zero value is an
// empty iterator.
type LevelIterator struct {
	files []*FileMetadata
	cur   int
	start int
	end   int
}

// Collect returns a slice of all the files in the iterator, irrespective of
// the current iterator position. The returned slice is owned by the iterator.
// This method is intended to be a temporary adatpter between interfaces, and
// callers should prefer using the iterator directory when possible.
//
// TODO(jackson): Revisit once the conversion of Version.Files to a btree is
// complete.
func (i LevelIterator) Collect() []*FileMetadata {
	return i.files[i.start:i.end]
}

// Len returns the number of files in the iterator, irrespective of the
// current iterator position.
func (i LevelIterator) Len() int {
	return i.end - i.start
}

// SizeSum sums the size of all files in the iterator, irrespective of the
// current iterator position.
func (i LevelIterator) SizeSum() uint64 {
	var sum uint64
	for _, f := range i.files[i.start:i.end] {
		sum += f.Size
	}
	return sum
}

// Each invokes fn for each element in LevelIterator, irrespective of the
// current position of the iterator.
func (i LevelIterator) Each(fn func(*FileMetadata)) {
	for f := i.First(); f != nil; f = i.Next() {
		fn(f)
	}
}

// Empty indicates whether there are remaining files in the iterator.
func (i LevelIterator) Empty() bool {
	return i.cur >= i.end
}

// Current returns the item at the current iterator position.
func (i LevelIterator) Current() *FileMetadata {
	if i.cur < i.start || i.cur >= i.end {
		return nil
	}
	return i.files[i.cur]
}

// First seeks to the first file in the iterator and returns it.
func (i *LevelIterator) First() *FileMetadata {
	i.cur = i.start
	if i.cur < i.start || i.cur >= i.end {
		return nil
	}
	return i.files[i.cur]
}

// Last seeks to the last file in the iterator and returns it.
func (i *LevelIterator) Last() *FileMetadata {
	i.cur = i.end - 1
	if i.cur < i.start || i.cur >= i.end {
		return nil
	}
	return i.files[i.cur]
}

// Next advances the iterator to the next file and returns it.
func (i *LevelIterator) Next() *FileMetadata {
	i.cur++
	if i.cur < i.start || i.cur >= i.end {
		return nil
	}
	return i.files[i.cur]
}

// Prev moves the iterator the previous file and returns it.
func (i *LevelIterator) Prev() *FileMetadata {
	i.cur--
	if i.cur < i.start || i.cur >= i.end {
		return nil
	}
	return i.files[i.cur]
}

// PeekNext returns the next file without altering the current iterator
// position.
func (i LevelIterator) PeekNext() *FileMetadata {
	if i.cur+1 < i.start || i.cur+1 >= i.end {
		return nil
	}
	return i.files[i.cur+1]
}

// PeekPrev returns the previous file without altering the current iterator
// position.
func (i LevelIterator) PeekPrev() *FileMetadata {
	if i.cur-1 < i.start || i.cur-1 >= i.end {
		return nil
	}
	return i.files[i.cur-1]
}

// Reslice constructs a new LevelIterator backed by the same underlying level,
// with new start and end positions. Reslice invokes the provided function,
// passing two LevelIterators: one positioned to i's inclusive start and one
// positioned to i's inclusive end. The resliceFunc may move either iterator
// forward or backwards, including beyond i's original bounds to capture
// additional files from the underlying level. Reslice constructs and returns
// a new LevelIterator with the final bounds of the iterators after calling
// resliceFunc.
func (i *LevelIterator) Reslice(resliceFunc func(start, end *LevelIterator)) LevelIterator {
	start := LevelIterator{
		files: i.files,
		cur:   i.start,
		start: 0,
		end:   len(i.files),
	}
	end := LevelIterator{
		files: i.files,
		cur:   i.end - 1,
		start: 0,
		end:   len(i.files),
	}
	resliceFunc(&start, &end)
	return LevelIterator{
		files: i.files,
		start: start.cur,
		end:   end.cur + 1,
	}
}

// SeekGE seeks to the first file in the iterator's file set with a largest
// user key less than or equal to the provided user key. The iterator must
// have been constructed from L1+, because it requires the underlying files to
// be sorted by user keys and non-overlapping.
func (i *LevelIterator) SeekGE(cmp Compare, userKey []byte) *FileMetadata {
	files := i.files[i.start:i.end]
	i.cur = i.start + sort.Search(len(files), func(j int) bool {
		return cmp(userKey, files[j].Largest.UserKey) <= 0
	})
	if i.cur >= i.end {
		return nil
	}
	return i.files[i.cur]
}

// Take constructs a new LevelIterator containing only the file at the
// iterator's current position. If the iterator is not currently positioned
// over a file, the returned iterator is empty.
func (i LevelIterator) Take() LevelIterator {
	return LevelIterator{
		files: i.files,
		cur:   i.cur,
		start: i.cur,
		end:   i.cur + 1,
	}
}
