// Copyright 2020 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package manifest

import (
	"bytes"
	"fmt"

	"github.com/cockroachdb/pebble/internal/base"
)

// LevelMetadata contains metadata for all of the files within
// a level of the LSM.
type LevelMetadata struct {
	tree btree
}

// clone makes a copy of the level metadata, implicitly increasing the ref
// count of every file contained within lm.
func (lm *LevelMetadata) clone() LevelMetadata {
	return LevelMetadata{
		tree: lm.tree.clone(),
	}
}

func (lm *LevelMetadata) release() (obsolete []base.FileNum) {
	return lm.tree.release()
}

func makeLevelMetadata(cmp Compare, level int, files []*FileMetadata) LevelMetadata {
	bcmp := btreeCmpSeqNum
	if level > 0 {
		bcmp = btreeCmpKeys(cmp)
	}
	var lm LevelMetadata
	lm.tree, _ = makeBTree(bcmp, files)
	return lm
}

func makeBTree(cmp btreeCmp, files []*FileMetadata) (btree, LevelSlice) {
	var t btree
	t.cmp = cmp
	for _, f := range files {
		t.insert(f)
	}
	return t, LevelSlice{iter: t.iter()}
}

// Empty indicates whether there are any files in the level.
func (lm *LevelMetadata) Empty() bool {
	return lm.tree.length == 0
}

// Len returns the number of files within the level.
func (lm *LevelMetadata) Len() int {
	return lm.tree.length
}

// Iter constructs a LevelIterator over the entire level.
func (lm *LevelMetadata) Iter() LevelIterator {
	return LevelIterator{iter: lm.tree.iter()}
}

// Slice constructs a slice containing the entire level.
func (lm *LevelMetadata) Slice() LevelSlice {
	return LevelSlice{iter: lm.tree.iter()}
}

// Find finds the provided file in the level if it exists.
func (lm *LevelMetadata) Find(cmp base.Compare, m *FileMetadata) *LevelFile {
	o := overlaps(lm.Iter(), cmp, m.Smallest.UserKey, m.Largest.UserKey)
	iter := o.Iter()
	for f := iter.First(); f != nil; f = iter.Next() {
		if f == m {
			lf := iter.Take()
			return &lf
		}
	}
	return nil
}

// Annotation lazily calculates and returns the annotation defined by
// Annotator. The Annotator is used as the key for pre-calculated
// values, so equal Annotators must be used to avoid duplicate computations
// and cached annotations.
func (lm *LevelMetadata) Annotation(annotator Annotator) interface{} {
	if lm.Empty() {
		return annotator.Zero()
	}
	v, _ := lm.tree.root.annotation(annotator)
	return v
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

// NewLevelSliceSeqSorted constructs a LevelSlice over the provided files,
// sorted by the L0 sequence number sort order.
// TODO(jackson): Can we improve this interface or avoid needing to export
// a slice constructor like this?
func NewLevelSliceSeqSorted(files []*FileMetadata) LevelSlice {
	tr, slice := makeBTree(btreeCmpSeqNum, files)
	tr.release()
	return slice
}

// NewLevelSliceKeySorted constructs a LevelSlice over the provided files,
// sorted by the files smallest keys.
func NewLevelSliceKeySorted(cmp base.Compare, files []*FileMetadata) LevelSlice {
	tr, slice := makeBTree(btreeCmpKeys(cmp), files)
	tr.release()
	return slice
}

// NewLevelSliceSpecificOrder constructs a LevelSlice over the provided files,
// ordering the files by their order in the provided slice. It's used in
// tests.
func NewLevelSliceSpecificOrder(files []*FileMetadata) LevelSlice {
	tr, slice := makeBTree(btreeCmpSpecificOrder(files), files)
	tr.release()
	return slice
}

// LevelSlice contains a slice of the files within a level of the LSM.
type LevelSlice struct {
	iter iterator
	// start and end form the inclusive bounds of a slice of files within a
	// level of the LSM. They may be nil if the entire B-Tree backing iter is
	// accessible.
	start *iterator
	end   *iterator
}

// Each invokes fn for each element in the slice.
func (ls LevelSlice) Each(fn func(*FileMetadata)) {
	iter := ls.Iter()
	for f := iter.First(); f != nil; f = iter.Next() {
		fn(f)
	}
}

func (ls LevelSlice) String() string {
	var buf bytes.Buffer
	ls.Each(func(f *FileMetadata) {
		if buf.Len() > 0 {
			fmt.Fprintf(&buf, " ")
		}
		fmt.Fprint(&buf, f)
	})
	return buf.String()
}

// Empty indicates whether the slice contains any files.
func (ls LevelSlice) Empty() bool {
	if ls.iter.r == nil {
		return true
	}
	return ls.start != nil && ls.end != nil && cmpIter(*ls.start, *ls.end) > 0
}

// Iter constructs a LevelIterator that iterates over the slice.
func (ls LevelSlice) Iter() LevelIterator {
	if ls.Empty() {
		return LevelIterator{}
	}

	return LevelIterator{
		start: ls.start,
		end:   ls.end,
		iter:  ls.iter.clone(),
	}
}

// Len returns the number of files in the slice.
func (ls LevelSlice) Len() int {
	var len int
	iter := ls.Iter()
	for f := iter.First(); f != nil; f = iter.Next() {
		len++
	}
	return len
}

// SizeSum sums the size of all files in the slice.
func (ls LevelSlice) SizeSum() uint64 {
	var sum uint64
	iter := ls.Iter()
	for f := iter.First(); f != nil; f = iter.Next() {
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
	if ls.iter.r == nil {
		return ls
	}
	var start, end LevelIterator
	if ls.start == nil {
		start.iter = ls.iter.clone()
		start.iter.first()
	} else {
		start.iter = ls.start.clone()
	}
	if ls.end == nil {
		end.iter = ls.iter.clone()
		end.iter.last()
	} else {
		end.iter = ls.end.clone()
	}
	resliceFunc(&start, &end)

	return LevelSlice{
		iter:  start.iter.clone(),
		start: &start.iter,
		end:   &end.iter,
	}
}

// LevelIterator iterates over a set of files' metadata. Its zero value is an
// empty iterator.
type LevelIterator struct {
	iter  iterator
	start *iterator
	end   *iterator
}

func (i LevelIterator) String() string {
	var buf bytes.Buffer
	iter := i.iter.clone()
	iter.first()
	iter.prev()
	if i.iter.pos == -1 {
		fmt.Fprint(&buf, "(<start>)*")
	}
	iter.next()
	for ; iter.valid(); iter.next() {
		if buf.Len() > 0 {
			fmt.Fprint(&buf, "   ")
		}

		if i.start != nil && cmpIter(iter, *i.start) == 0 {
			fmt.Fprintf(&buf, " [ ")
		}
		isCurrentPos := cmpIter(iter, i.iter) == 0
		if isCurrentPos {
			fmt.Fprint(&buf, " ( ")
		}
		fmt.Fprint(&buf, iter.cur().String())
		if isCurrentPos {
			fmt.Fprint(&buf, " )*")
		}
		if i.end != nil && cmpIter(iter, *i.end) == 0 {
			fmt.Fprintf(&buf, " ]")
		}
	}
	if i.iter.n != nil && i.iter.pos >= i.iter.n.count {
		if buf.Len() > 0 {
			fmt.Fprint(&buf, "   ")
		}
		fmt.Fprint(&buf, "(<end>)*")
	}
	return buf.String()
}

// Clone copies the iterator, returning an independent iterator at the same
// position.
func (i *LevelIterator) Clone() LevelIterator {
	if i.iter.r == nil {
		return *i
	}
	// The start and end iterators are not cloned and are treated as
	// immutable.
	return LevelIterator{
		iter:  i.iter.clone(),
		start: i.start,
		end:   i.end,
	}
}

// Current returns the item at the current iterator position.
func (i LevelIterator) Current() *FileMetadata {
	if !i.iter.valid() {
		return nil
	}
	return i.iter.cur()
}

func (i *LevelIterator) empty() bool {
	return i.iter.r == nil || (i.start != nil && i.end != nil && cmpIter(*i.end, *i.start) < 0)
}

// First seeks to the first file in the iterator and returns it.
func (i *LevelIterator) First() *FileMetadata {
	if i.empty() {
		return nil
	}
	if i.start != nil {
		i.iter = i.start.clone()
	} else {
		i.iter.first()
	}
	if !i.iter.valid() {
		return nil
	}
	return i.iter.cur()
}

// Last seeks to the last file in the iterator and returns it.
func (i *LevelIterator) Last() *FileMetadata {
	if i.empty() {
		return nil
	}
	if i.end != nil {
		i.iter = i.end.clone()
	} else {
		i.iter.last()
	}
	if !i.iter.valid() {
		return nil
	}
	return i.iter.cur()
}

// Next advances the iterator to the next file and returns it.
func (i *LevelIterator) Next() *FileMetadata {
	i.iter.next()
	if !i.iter.valid() {
		return nil
	}
	if i.end != nil && cmpIter(i.iter, *i.end) > 0 {
		return nil
	}
	return i.iter.cur()
}

// Prev moves the iterator the previous file and returns it.
func (i *LevelIterator) Prev() *FileMetadata {
	i.iter.prev()
	if !i.iter.valid() {
		return nil
	}
	if i.start != nil && cmpIter(i.iter, *i.start) < 0 {
		return nil
	}
	return i.iter.cur()
}

// SeekGE seeks to the first file in the iterator's file set with a largest
// user key less than or equal to the provided user key. The iterator must
// have been constructed from L1+, because it requires the underlying files to
// be sorted by user keys and non-overlapping.
func (i *LevelIterator) SeekGE(cmp Compare, userKey []byte) *FileMetadata {
	if i.empty() {
		return nil
	}
	i.iter.seekGE(cmp, userKey)

	// i.iter.seekGE seeked in the unbounded underyling B-Tree. If the
	// iterator has start or end bounds, we may have exceeded them. Reset to
	// the bound if necessary.
	if i.end != nil && cmpIter(i.iter, *i.end) > 0 {
		i.iter = i.end.clone()
		// Adjusting back to the end bound might've put us before the user key
		// searched. If so, advance once into the sentinel state.
		if i.iter.valid() && cmp(userKey, i.iter.cur().Largest.UserKey) > 0 {
			i.iter.next()
			return nil
		}
	} else if i.start != nil && cmpIter(i.iter, *i.start) < 0 {
		i.iter = i.start.clone()
	}

	if !i.iter.valid() {
		return nil
	}
	return i.iter.cur()
}

// SeekLT seeks to the last file in the iterator's file set with a smallest
// user key less than the provided user key. The iterator must have been
// constructed from L1+, because it requries the underlying files to be sorted
// by user keys and non-overlapping.
func (i *LevelIterator) SeekLT(cmp Compare, userKey []byte) *FileMetadata {
	if i.empty() {
		return nil
	}

	i.iter.seekLT(cmp, userKey)

	// i.iter.seekLT seeked in the unbounded underyling B-Tree. If the
	// iterator has start or end bounds, we may have exceeded them. Reset to
	// the bound if necessary.
	if i.start != nil && cmpIter(i.iter, *i.start) < 0 {
		i.iter = i.start.clone()
		// Adjusting forward to the start bound might've put us after the user
		// key searched. If so, step once back into the sentinel state.
		if i.iter.valid() && cmp(i.iter.cur().Smallest.UserKey, userKey) >= 0 {
			i.iter.prev()
			return nil
		}
	} else if i.end != nil && cmpIter(i.iter, *i.end) > 0 {
		i.iter = i.end.clone()
	}

	var v *FileMetadata
	if i.iter.valid() {
		v = i.iter.cur()
	}
	return v
}

// Take constructs a LevelFile containing the file at the iterator's current
// position. Take panics if the iterator is not currently positioned over a
// file.
func (i LevelIterator) Take() LevelFile {
	m := i.Current()
	if m == nil {
		panic("Take called on invalid LevelIterator")
	}
	// LevelSlice's start and end fields are immutable and are positioned to
	// the same position for a LevelFile because they're inclusive, so we can
	// share one iterator stack between the two bounds.
	boundsIter := i.iter.clone()
	return LevelFile{
		FileMetadata: m,
		slice: LevelSlice{
			iter:  i.iter.clone(),
			start: &boundsIter,
			end:   &boundsIter,
		},
	}
}
