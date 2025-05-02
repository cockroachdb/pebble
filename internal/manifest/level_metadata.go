// Copyright 2020 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package manifest

import (
	"bytes"
	"fmt"
	"iter"
	"reflect"

	"github.com/cockroachdb/pebble/internal/base"
	"github.com/cockroachdb/pebble/internal/invariants"
)

// LevelMetadata contains metadata for all of the files within
// a level of the LSM.
type LevelMetadata struct {
	level     int
	totalSize uint64
	// NumVirtual is the number of virtual sstables in the level.
	NumVirtual uint64
	// VirtualSize is the size of the virtual sstables in the level.
	VirtualSize uint64
	tree        btree
}

// clone makes a copy of the level metadata, implicitly increasing the ref
// count of every file contained within lm.
func (lm *LevelMetadata) clone() LevelMetadata {
	return LevelMetadata{
		level:       lm.level,
		totalSize:   lm.totalSize,
		NumVirtual:  lm.NumVirtual,
		VirtualSize: lm.VirtualSize,
		tree:        lm.tree.Clone(),
	}
}

func (lm *LevelMetadata) release(of ObsoleteFilesSet) {
	lm.tree.Release(of)
}

// MakeLevelMetadata creates a LevelMetadata with the given files.
func MakeLevelMetadata(cmp Compare, level int, files []*TableMetadata) LevelMetadata {
	bcmp := btreeCmpSeqNum
	if level > 0 {
		bcmp = btreeCmpSmallestKey(cmp)
	}
	var lm LevelMetadata
	lm.level = level
	lm.tree = makeBTree(cmp, bcmp, files)
	for _, f := range files {
		lm.totalSize += f.Size
		if f.Virtual {
			lm.NumVirtual++
			lm.VirtualSize += f.Size
		}
	}
	return lm
}

func makeBTree(cmp base.Compare, bcmp btreeCmp, files []*TableMetadata) btree {
	t := btree{cmp: cmp, bcmp: bcmp}
	for _, f := range files {
		if err := t.Insert(f); err != nil {
			panic(err)
		}
	}
	return t
}

func makeLevelSlice(cmp base.Compare, bcmp btreeCmp, files []*TableMetadata) LevelSlice {
	t := makeBTree(cmp, bcmp, files)
	slice := newLevelSlice(t.Iter())
	slice.verifyInvariants()
	// We can release the tree because the nodes that are referenced by the
	// LevelSlice are immutable and we never recycle them.
	t.Release(ignoreObsoleteFiles{})
	return slice
}

func (lm *LevelMetadata) insert(f *TableMetadata) error {
	if err := lm.tree.Insert(f); err != nil {
		return err
	}
	lm.totalSize += f.Size
	if f.Virtual {
		lm.NumVirtual++
		lm.VirtualSize += f.Size
	}
	return nil
}

func (lm *LevelMetadata) remove(f *TableMetadata) {
	lm.totalSize -= f.Size
	if f.Virtual {
		lm.NumVirtual--
		lm.VirtualSize -= f.Size
	}
	lm.tree.Delete(f, assertNoObsoleteFiles{})
}

// Empty indicates whether there are any files in the level.
func (lm *LevelMetadata) Empty() bool {
	return lm.tree.Count() == 0
}

// Len returns the number of files within the level.
func (lm *LevelMetadata) Len() int {
	return lm.tree.Count()
}

// Size returns the cumulative size of all the files within the level.
func (lm *LevelMetadata) Size() uint64 {
	return lm.totalSize
}

// Iter constructs a LevelIterator over the entire level.
func (lm *LevelMetadata) Iter() LevelIterator {
	return LevelIterator{iter: lm.tree.Iter()}
}

// All returns an iterator over all files in the level.
func (lm *LevelMetadata) All() iter.Seq[*TableMetadata] {
	return func(yield func(*TableMetadata) bool) {
		iter := lm.Iter()
		for f := iter.First(); f != nil; f = iter.Next() {
			if !yield(f) {
				break
			}
		}
	}
}

// Slice constructs a slice containing the entire level.
func (lm *LevelMetadata) Slice() LevelSlice {
	return newLevelSlice(lm.tree.Iter())
}

// Find finds the provided file in the level. If it exists, returns a LevelSlice
// that contains just that file; otherwise, returns an empty LevelSlice.
func (lm *LevelMetadata) Find(cmp base.Compare, m *TableMetadata) LevelSlice {
	iter := lm.Iter()
	if lm.level == 0 {
		// We only need to look at the portion of files that are "equal" to m with
		// respect to the L0 ordering.
		iter.iter.seekSeqNumL0(m)
		f := iter.constrainToIteratorBounds()
		for ; f != nil && f.cmpSeqNum(m) == 0; f = iter.Next() {
			if f == m {
				return iter.Take().slice
			}
		}
	} else {
		// For levels other than L0, UserKeyBounds in the level are non-overlapping
		// so we only need to check one file.
		if f := iter.SeekGE(cmp, m.Smallest().UserKey); f == m {
			return iter.Take().slice
		}
	}
	return LevelSlice{}
}

// LevelFile holds a file's metadata along with its position
// within a level of the LSM.
type LevelFile struct {
	*TableMetadata
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
func NewLevelSliceSeqSorted(files []*TableMetadata) LevelSlice {
	return makeLevelSlice(nil, btreeCmpSeqNum, files)
}

// NewLevelSliceKeySorted constructs a LevelSlice over the provided files,
// sorted by the files smallest keys.
// TODO(jackson): Can we improve this interface or avoid needing to export
// a slice constructor like this?
func NewLevelSliceKeySorted(cmp base.Compare, files []*TableMetadata) LevelSlice {
	return makeLevelSlice(cmp, btreeCmpSmallestKey(cmp), files)
}

// NewLevelSliceSpecificOrder constructs a LevelSlice over the provided files,
// ordering the files by their order in the provided slice. It's used in
// tests.
// TODO(jackson): Update tests to avoid requiring this and remove it.
func NewLevelSliceSpecificOrder(files []*TableMetadata) LevelSlice {
	slice := makeLevelSlice(nil, btreeCmpSpecificOrder(files), files)
	slice.verifyInvariants()
	return slice
}

// newLevelSlice constructs a new LevelSlice backed by iter.
func newLevelSlice(iter iterator) LevelSlice {
	s := LevelSlice{iter: iter}
	if iter.r != nil {
		s.length = iter.r.subtreeCount
	}
	s.verifyInvariants()
	return s
}

// newBoundedLevelSlice constructs a new LevelSlice backed by iter and bounded
// by the provided start and end bounds. The provided startBound and endBound
// iterators must be iterators over the same B-Tree. Both start and end bounds
// are inclusive.
func newBoundedLevelSlice(iter iterator, startBound, endBound *iterator) LevelSlice {
	s := LevelSlice{
		iter:  iter,
		start: startBound,
		end:   endBound,
	}
	if iter.valid() {
		s.length = endBound.countLeft() - startBound.countLeft()
		// NB: The +1 is a consequence of the end bound being inclusive.
		if endBound.valid() {
			s.length++
		}
		// NB: A slice that's empty due to its bounds may have an endBound
		// positioned before the startBound due to the inclusive bounds.
		// TODO(jackson): Consider refactoring the end boundary to be exclusive;
		// it would simplify some areas (eg, here) and complicate others (eg,
		// Reslice-ing to grow compactions).
		if s.length < 0 {
			s.length = 0
		}
	}
	s.verifyInvariants()
	return s
}

// LevelSlice contains a slice of the files within a level of the LSM.
// A LevelSlice is immutable once created, but may be used to construct a
// mutable LevelIterator over the slice's files.
//
// LevelSlices should be constructed through one of the existing constructors,
// not manually initialized.
type LevelSlice struct {
	iter   iterator
	length int
	// start and end form the inclusive bounds of a slice of files within a
	// level of the LSM. They may be nil if the entire B-Tree backing iter is
	// accessible.
	start *iterator
	end   *iterator
}

func (ls LevelSlice) verifyInvariants() {
	if invariants.Enabled {
		var length int
		for range ls.All() {
			length++
		}
		if ls.length != length {
			panic(fmt.Sprintf("LevelSlice %s has length %d value; actual length is %d", ls, ls.length, length))
		}
	}
}

// All returns an iterator over all files in the slice.
func (ls LevelSlice) All() iter.Seq[*TableMetadata] {
	return func(yield func(*TableMetadata) bool) {
		iter := ls.Iter()
		for f := iter.First(); f != nil; f = iter.Next() {
			if !yield(f) {
				break
			}
		}
	}
}

// String implements fmt.Stringer.
func (ls LevelSlice) String() string {
	var buf bytes.Buffer
	fmt.Fprintf(&buf, "%d files: ", ls.length)
	for f := range ls.All() {
		if buf.Len() > 0 {
			fmt.Fprintf(&buf, " ")
		}
		fmt.Fprint(&buf, f)
	}
	return buf.String()
}

// Empty indicates whether the slice contains any files.
func (ls *LevelSlice) Empty() bool {
	return emptyWithBounds(ls.iter, ls.start, ls.end)
}

// Iter constructs a LevelIterator that iterates over the slice.
func (ls *LevelSlice) Iter() LevelIterator {
	return LevelIterator{
		start: ls.start,
		end:   ls.end,
		iter:  ls.iter.clone(),
	}
}

// Len returns the number of files in the slice. Its runtime is constant.
func (ls *LevelSlice) Len() int {
	return ls.length
}

// SizeSum sums the size of all files in the slice. Its runtime is linear in
// the length of the slice.
func (ls *LevelSlice) SizeSum() uint64 {
	var sum uint64
	for f := range ls.All() {
		sum += f.Size
	}
	return sum
}

// NumVirtual returns the number of virtual sstables in the level. Its runtime is
// linear in the length of the slice.
func (ls *LevelSlice) NumVirtual() uint64 {
	var n uint64
	for f := range ls.All() {
		if f.Virtual {
			n++
		}
	}
	return n
}

// VirtualSizeSum returns the sum of the sizes of the virtual sstables in the
// level.
func (ls *LevelSlice) VirtualSizeSum() uint64 {
	var sum uint64
	for f := range ls.All() {
		if f.Virtual {
			sum += f.Size
		}
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
	return newBoundedLevelSlice(start.iter.clone(), &start.iter, &end.iter)
}

// Overlaps returns a new LevelSlice that reflects the portion of files with
// boundaries that overlap with the provided bounds.
func (ls LevelSlice) Overlaps(cmp Compare, bounds base.UserKeyBounds) LevelSlice {
	startIter := ls.Iter()
	startIter.SeekGE(cmp, bounds.Start)

	// Note: newBoundedLevelSlice uses inclusive bounds, so we need to position
	// endIter at the last overlapping file.
	endIter := ls.Iter()
	endIterFile := endIter.SeekGE(cmp, bounds.End.Key)
	// The first file that ends at/after bounds.End.Key might or might not overlap
	// the bounds; we need to check the start key.
	if endIterFile == nil || !bounds.End.IsUpperBoundFor(cmp, endIterFile.Smallest().UserKey) {
		endIter.Prev()
	}
	return newBoundedLevelSlice(startIter.iter.clone(), &startIter.iter, &endIter.iter)
}

// KeyType is used to specify the type of keys we're looking for in
// LevelIterator positioning operations. Files not containing any keys of the
// desired type are skipped.
type KeyType int8

const (
	// KeyTypePointAndRange denotes a search among the entire keyspace, including
	// both point keys and range keys. No sstables are skipped.
	KeyTypePointAndRange KeyType = iota
	// KeyTypePoint denotes a search among the point keyspace. SSTables with no
	// point keys will be skipped. Note that the point keyspace includes rangedels.
	KeyTypePoint
	// KeyTypeRange denotes a search among the range keyspace. SSTables with no
	// range keys will be skipped.
	KeyTypeRange
)

// LevelIterator iterates over a set of files' metadata. Its zero value is an
// empty iterator.
type LevelIterator struct {
	iter iterator
	// If set, start is an inclusive lower bound on the iterator.
	start *iterator
	// If set, end is an inclusive upper bound on the iterator.
	end    *iterator
	filter KeyType
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
		iter:   i.iter.clone(),
		start:  i.start,
		end:    i.end,
		filter: i.filter,
	}
}

func (i *LevelIterator) empty() bool {
	return emptyWithBounds(i.iter, i.start, i.end)
}

// Filter clones the iterator and sets the desired KeyType as the key to filter
// files on.
func (i *LevelIterator) Filter(keyType KeyType) LevelIterator {
	l := i.Clone()
	l.filter = keyType
	return l
}

func emptyWithBounds(i iterator, start, end *iterator) bool {
	// If i.r is nil, the iterator was constructed from an empty btree.
	// If the end bound is before the start bound, the bounds represent an
	// empty slice of the B-Tree.
	return i.r == nil || (start != nil && end != nil && cmpIter(*end, *start) < 0)
}

// First seeks to the first file in the iterator and returns it.
func (i *LevelIterator) First() *TableMetadata {
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
	return i.skipFilteredForward(i.iter.cur())
}

// Last seeks to the last file in the iterator and returns it.
func (i *LevelIterator) Last() *TableMetadata {
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
	return i.skipFilteredBackward(i.iter.cur())
}

// Next advances the iterator to the next file and returns it.
func (i *LevelIterator) Next() *TableMetadata {
	if i.iter.r == nil {
		return nil
	}
	if invariants.Enabled && (i.iter.pos >= i.iter.n.count || (i.end != nil && cmpIter(i.iter, *i.end) > 0)) {
		panic("pebble: cannot next forward-exhausted iterator")
	}
	i.iter.next()
	if !i.iter.valid() {
		return nil
	}
	return i.skipFilteredForward(i.iter.cur())
}

// Prev moves the iterator the previous file and returns it.
func (i *LevelIterator) Prev() *TableMetadata {
	if i.iter.r == nil {
		return nil
	}
	if invariants.Enabled && (i.iter.pos < 0 || (i.start != nil && cmpIter(i.iter, *i.start) < 0)) {
		panic("pebble: cannot prev backward-exhausted iterator")
	}
	i.iter.prev()
	if !i.iter.valid() {
		return nil
	}
	return i.skipFilteredBackward(i.iter.cur())
}

// SeekGE seeks to the first file with a largest key (of the desired type) that
// is an upper bound for the given user key. This is the first file that could
// contain a user key that is greater than or equal to userKey.
//
// More specifically, userKey is less than the file's largest.UserKey or they
// are equal and largest is not an exclusive sentinel.
//
// The iterator must have been constructed from L1+ or from a single sublevel of
// L0, because it requires the underlying files to be sorted by user keys and
// non-overlapping.
func (i *LevelIterator) SeekGE(cmp Compare, userKey []byte) *TableMetadata {
	if i.iter.r == nil {
		return nil
	}
	i.assertNotL0Cmp()
	i.iter.seekLargest(cmp, userKey)
	m := i.constrainToIteratorBounds()
	if i.filter != KeyTypePointAndRange && m != nil {
		b, ok := m.LargestBound(i.filter)
		if !ok || !b.IsUpperBoundFor(cmp, userKey) {
			// The file does not contain any keys of desired key types
			// that are >= userKey.
			return i.Next()
		}
	}
	return i.skipFilteredForward(m)
}

// SeekLT seeks to the last file with a smallest key (of the desired type) that
// is less than the given user key. This is the last file that could contain a
// key less than userKey.
//
// The iterator must have been constructed from L1+ or from a single sublevel of
// L0, because it requires the underlying files to be sorted by user keys and
// non-overlapping.
func (i *LevelIterator) SeekLT(cmp Compare, userKey []byte) *TableMetadata {
	if i.iter.r == nil {
		return nil
	}
	i.assertNotL0Cmp()
	i.iter.seekSmallest(cmp, userKey)
	_ = i.constrainToIteratorBounds()
	m := i.Prev()
	// Although i.Prev() guarantees that the current file contains keys of the
	// relevant type, it doesn't guarantee that the keys of the relevant type
	// are < userKey. For example, say that we have these two files:
	//   f1: [a, f) with keys of the desired type in the range [c, d)
	//   f2: [h, k)
	// and userKey is b. The seek call above will position us at f2 and Prev will
	// position us at f1.
	if i.filter != KeyTypePointAndRange && m != nil {
		b, ok := m.SmallestBound(i.filter)
		if !ok {
			panic("unreachable")
		}
		if cmp(b.UserKey, userKey) >= 0 {
			// This file does not contain any keys of desired key types
			// that are <= userKey.
			return i.Prev()
		}
	}
	return m
}

// assertNotL0Cmp verifies that the btree associated with the iterator is
// ordered by Smallest key (i.e. L1+ or L0 sublevel) and not by LargestSeqNum
// (L0).
func (i *LevelIterator) assertNotL0Cmp() {
	if invariants.Enabled {
		if reflect.ValueOf(i.iter.cmp).Pointer() == reflect.ValueOf(btreeCmpSeqNum).Pointer() {
			panic("Seek used with btreeCmpSeqNum")
		}
	}
}

// skipFilteredForward takes the table metadata at the iterator's current
// position, and skips forward if the current key-type filter (i.filter)
// excludes the file. It skips until it finds an unfiltered file or exhausts the
// level. If lower is != nil, skipFilteredForward skips any files that do not
// contain keys with the provided key-type ≥ lower.
//
// skipFilteredForward also enforces the upper bound, returning nil if at any
// point the upper bound is exceeded.
func (i *LevelIterator) skipFilteredForward(meta *TableMetadata) *TableMetadata {
	for meta != nil && !meta.ContainsKeyType(i.filter) {
		i.iter.next()
		if !i.iter.valid() {
			meta = nil
		} else {
			meta = i.iter.cur()
		}
	}
	if meta != nil && i.end != nil && cmpIter(i.iter, *i.end) > 0 {
		// Exceeded upper bound.
		meta = nil
	}
	return meta
}

// skipFilteredBackward takes the table metadata at the iterator's current
// position, and skips backward if the current key-type filter (i.filter)
// excludes the file. It skips until it finds an unfiltered file or exhausts the
// level. If upper is != nil, skipFilteredBackward skips any files that do not
// contain keys with the provided key-type < upper.
//
// skipFilteredBackward also enforces the lower bound, returning nil if at any
// point the lower bound is exceeded.
func (i *LevelIterator) skipFilteredBackward(meta *TableMetadata) *TableMetadata {
	for meta != nil && !meta.ContainsKeyType(i.filter) {
		i.iter.prev()
		if !i.iter.valid() {
			meta = nil
		} else {
			meta = i.iter.cur()
		}
	}
	if meta != nil && i.start != nil && cmpIter(i.iter, *i.start) < 0 {
		// Exceeded lower bound.
		meta = nil
	}
	return meta
}

// constrainToIteratorBounds adjusts the iterator position to ensure it's
// positioned within the iterator's bounds.
func (i *LevelIterator) constrainToIteratorBounds() *TableMetadata {
	// i.iter.{seekLargest,seekSmallest,seekSeqNumL0} all seek in the unbounded
	// underlying B-Tree. If the iterator has start or end bounds, we may have
	// exceeded them. Reset to the bounds if necessary.
	//
	// NB: The LevelIterator and LevelSlice semantics require that a bounded
	// LevelIterator/LevelSlice containing files x0, x1, ..., xn behave
	// identically to an unbounded LevelIterator/LevelSlice of a B-Tree
	// containing x0, x1, ..., xn. In other words, any files outside the
	// LevelIterator's bounds should not influence the iterator's behavior.
	// When seeking, this means a SeekGE that seeks beyond the end bound,
	// followed by a Prev should return the last element within bounds.
	if i.end != nil && cmpIter(i.iter, *i.end) > 0 {
		i.iter = i.end.clone()
		// Since seek(fn) positioned beyond i.end, we know there is nothing to
		// return within bounds.
		i.iter.next()
		return nil
	} else if i.start != nil && cmpIter(i.iter, *i.start) < 0 {
		i.iter = i.start.clone()
	}
	if !i.iter.valid() {
		return nil
	}
	return i.iter.cur()
}

// Take constructs a LevelFile containing the file at the iterator's current
// position. Take panics if the iterator is not currently positioned over a
// file.
func (i *LevelIterator) Take() LevelFile {
	if !i.iter.valid() ||
		(i.end != nil && cmpIter(i.iter, *i.end) > 0) ||
		(i.start != nil && cmpIter(i.iter, *i.start) < 0) {
		panic("Take called on invalid LevelIterator")
	}
	m := i.iter.cur()
	// LevelSlice's start and end fields are immutable and are positioned to
	// the same position for a LevelFile because they're inclusive, so we can
	// share one iterator stack between the two bounds.
	boundsIter := i.iter.clone()
	s := newBoundedLevelSlice(i.iter.clone(), &boundsIter, &boundsIter)
	return LevelFile{
		TableMetadata: m,
		slice:         s,
	}
}
