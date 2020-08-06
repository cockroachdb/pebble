// Copyright 2020 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package manifest

import "sort"

// LevelMetadata contains metadata for all of the files within
// a level of the LSM.
type LevelMetadata struct {
	files []*FileMetadata
}

// Len returns the number of files within the level.
func (lm *LevelMetadata) Len() int {
	return len(lm.files)
}

// Iter constructs a LevelIterator over the entire level.
func (lm *LevelMetadata) Iter() LevelIterator {
	return LevelIterator{files: lm.files, end: len(lm.files)}
}

// Slice constructs a slice containing the entire level.
func (lm *LevelMetadata) Slice() LevelSlice {
	return LevelSlice{files: lm.files, end: len(lm.files)}
}

// LevelFile holds a file's metadata along with its position
// within a level of the LSM.
type LevelFile struct {
	*FileMetadata
	slice LevelSlice
}

// Slice constructs a LevelSlice containing only this file.
func (lf LevelFile) Slice() LevelSlice {
	return lf.slice
}

// NewLevelSlice constructs a LevelSlice over the provided files. This
// function is expected to be a temporary adapter between interfaces.
// TODO(jackson): Revisit once the conversion of Version.Files to a btree is
// complete.
func NewLevelSlice(files []*FileMetadata) LevelSlice {
	return LevelSlice{files: files, start: 0, end: len(files)}
}

// LevelSlice contains a slice of the files within a level of the LSM.
type LevelSlice struct {
	files []*FileMetadata
	start int
	end   int
}

// Each invokes fn for each element in the slice.
func (ls LevelSlice) Each(fn func(*FileMetadata)) {
	iter := ls.Iter()
	for f := iter.First(); f != nil; f = iter.Next() {
		fn(f)
	}
}

// Empty indicates whether the slice contains any files.
func (ls LevelSlice) Empty() bool {
	return ls.start >= ls.end
}

// Iter constructs a LevelIterator that iterates over the slice.
func (ls LevelSlice) Iter() LevelIterator {
	return LevelIterator{
		files: ls.files,
		start: ls.start,
		end:   ls.end,
	}
}

// Len returns the number of files in the slice.
func (ls LevelSlice) Len() int {
	return ls.end - ls.start
}

// SizeSum sums the size of all files in the slice.
func (ls LevelSlice) SizeSum() uint64 {
	var sum uint64
	for _, f := range ls.files[ls.start:ls.end] {
		sum += f.Size
	}
	return sum
}

// Reslice constructs a new slice backed by the same underlying level, with
// new start and end positions. Reslice invokes the provided function, passing
// two LevelIterators: one positioned to i's inclusive start and one
// positioned to i's inclusive end. The resliceFunc may move either iterator
// forward or backwards, including beyond the callee's original bounds to
// capture additional files from the underlying level. Reslice constructs and
// returns a new LevelSlice with the final bounds of the iterators after
// calling resliceFunc.
func (ls LevelSlice) Reslice(resliceFunc func(start, end *LevelIterator)) LevelSlice {
	start := LevelIterator{
		files: ls.files,
		cur:   ls.start,
		start: 0,
		end:   len(ls.files),
	}
	end := LevelIterator{
		files: ls.files,
		cur:   ls.end - 1,
		start: 0,
		end:   len(ls.files),
	}
	resliceFunc(&start, &end)
	return LevelSlice{
		files: ls.files,
		start: start.cur,
		end:   end.cur + 1,
	}
}

// LevelIterator iterates over a set of files' metadata. Its zero value is an
// empty iterator.
type LevelIterator struct {
	files []*FileMetadata
	cur   int
	start int
	end   int
}

// Clone copies the iterator, returning an independent iterator at the same
// position.
func (i *LevelIterator) Clone() LevelIterator {
	return *i
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

// SeekLT seeks to the last file in the iterator's file set with a smallest
// user key less than the provided user key. The iterator must have been
// constructed from L1+, because it requries the underlying files to be sorted
// by user keys and non-overlapping.
func (i *LevelIterator) SeekLT(cmp Compare, userKey []byte) *FileMetadata {
	files := i.files[i.start:i.end]
	i.cur = i.start + sort.Search(len(files), func(j int) bool {
		return cmp(files[j].Smallest.UserKey, userKey) >= 0
	})
	if i.cur < i.start {
		return nil
	}
	return i.Prev()
}

// Take constructs a LevelFile containing the file at the iterator's current
// position. Take panics if the iterator is not currently positioned over a
// file.
func (i LevelIterator) Take() LevelFile {
	m := i.Current()
	if m == nil {
		panic("Take called on invalid LevelIterator")
	}
	return LevelFile{
		FileMetadata: m,
		slice: LevelSlice{
			files: i.files,
			start: i.cur,
			end:   i.cur + 1,
		},
	}
}
