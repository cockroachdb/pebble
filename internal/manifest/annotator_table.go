// Copyright 2025 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package manifest

import (
	"sort"

	"github.com/cockroachdb/pebble/internal/base"
)

// The TableAnnotator type defined below is used by other packages to lazily
// compute a value over a B-Tree. Each node of the B-Tree stores one
// `annotation` per annotator, containing the result of the computation over
// the node's subtree.
//
// An annotation is marked as valid if it's current with the current subtree
// state. Annotations are marked as invalid whenever a node will be mutated
// (in mut).  Annotators may also return `false` from `Accumulate` to signal
// that a computation for a file is not stable and may change in the future.
// Annotations that include these unstable values are also marked as invalid
// on the node, ensuring that future queries for the annotation will recompute
// the value.

// A TableAnnotator defines a computation over a level's TableMetadata. If the
// computation is stable and uses inputs that are fixed for the lifetime of a
// TableMetadata, the LevelMetadata's internal data structures are annotated
// with the intermediary computations. This allows the computation to be
// computed incrementally as edits are applied to a level.
type TableAnnotator[T any] struct {
	annotator[T, *TableMetadata]

	partialOverlapFunc func(*TableMetadata, base.UserKeyBounds) T
}

func MakeTableAnnotator[T any](
	idx TableAnnotationIdx, funcs TableAnnotatorFuncs[T],
) TableAnnotator[T] {
	return TableAnnotator[T]{
		annotator: annotator[T, *TableMetadata]{
			idx:       annotationIdx(idx),
			mergeFunc: funcs.Merge,
			itemFunc:  funcs.Table,
		},
		partialOverlapFunc: funcs.PartialOverlap,
	}
}

// TableAnnotatorFuncs groups the functions that define a table annotator.
type TableAnnotatorFuncs[T any] struct {
	// Merge combines two values src and dst into dst.
	Merge func(dst *T, src T)

	// Table computes the annotation for a table in a tree. Returns the value and
	// a bool flag indicating whether the value is stable and okay to cache as an
	// annotation. If the file's value may change over the life of the file, the
	// annotator must return false.
	Table func(*TableMetadata) (_ T, cacheOK bool)

	// PartialOverlap computes the annotation for a table that only partially
	// overlaps the given bounds. PartialOverlap is optional and only used with
	// range annotation functionality.
	//
	// PartialOverlap results are never cached.
	PartialOverlap func(*TableMetadata, base.UserKeyBounds) T
}

// TableAnnotationIdx is an index associated with a specific annotator. Table
// annotators should have distinct TableAnnotationIdx values.
type TableAnnotationIdx int

// NewTableAnnotationIdx generates a unique AnnotationIdx that can be used with a
// table annotator. It should be called during global initialization.
func NewTableAnnotationIdx() TableAnnotationIdx {
	n := nextTableAnnotationIdx
	if n >= maxAnnotationsPerNode {
		panic("too many table annotations")
	}
	nextTableAnnotationIdx++
	return n
}

var nextTableAnnotationIdx TableAnnotationIdx

// accumulateRangeAnnotation computes this annotator's annotation across all
// files in the node's subtree which overlap with the range defined by bounds.
// The computed annotation is accumulated into a.scratch.
func (a *TableAnnotator[T]) accumulateRangeAnnotation(
	n *node[*TableMetadata],
	cmp base.Compare,
	bounds base.UserKeyBounds,
	// fullyWithinLowerBound and fullyWithinUpperBound indicate whether this
	// node's subtree is already known to be within each bound.
	fullyWithinLowerBound bool,
	fullyWithinUpperBound bool,
) T {
	// If this node's subtree is fully within the bounds, compute a regular
	// annotation.
	if fullyWithinLowerBound && fullyWithinUpperBound {
		v, _ := a.nodeAnnotation(n)
		return *v
	}

	// We will accumulate annotations from each item in the end-exclusive
	// range [leftItem, rightItem).
	leftItem, rightItem := 0, int(n.count)
	if !fullyWithinLowerBound {
		// leftItem is the index of the first item that overlaps the lower bound.
		leftItem = sort.Search(int(n.count), func(i int) bool {
			return cmp(bounds.Start, n.items[i].Largest().UserKey) <= 0
		})
	}
	if !fullyWithinUpperBound {
		// rightItem is the index of the first item that does not overlap the
		// upper bound.
		rightItem = sort.Search(int(n.count), func(i int) bool {
			return !bounds.End.IsUpperBoundFor(cmp, n.items[i].Smallest().UserKey)
		})
	}

	var result T
	// Accumulate annotations from every item that overlaps the bounds.
	for i := leftItem; i < rightItem; i++ {
		if i == leftItem || i == rightItem-1 {
			if a.partialOverlapFunc != nil {
				fb := n.items[i].UserKeyBounds()
				if cmp(bounds.Start, fb.Start) > 0 || bounds.End.CompareUpperBounds(cmp, fb.End) < 0 {
					v := a.partialOverlapFunc(n.items[i], bounds)
					a.mergeFunc(&result, v)
					continue
				}
			}
		}
		v, _ := a.itemFunc(n.items[i])
		a.mergeFunc(&result, v)
	}

	if !n.isLeaf() {
		// We will accumulate annotations from each child in the end-inclusive
		// range [leftChild, rightChild].
		leftChild, rightChild := leftItem, rightItem
		// If the lower bound overlaps with the child at leftItem, there is no
		// need to accumulate annotations from the child to its left.
		if leftItem < int(n.count) && cmp(bounds.Start, n.items[leftItem].Smallest().UserKey) >= 0 {
			leftChild++
		}
		// If the upper bound spans beyond the child at rightItem, we must also
		// accumulate annotations from the child to its right.
		if rightItem < int(n.count) && bounds.End.IsUpperBoundFor(cmp, n.items[rightItem].Largest().UserKey) {
			rightChild++
		}

		for i := leftChild; i <= rightChild; i++ {
			v := a.accumulateRangeAnnotation(
				n.child(int16(i)),
				cmp,
				bounds,
				// If this child is to the right of leftItem, then its entire
				// subtree is within the lower bound.
				fullyWithinLowerBound || i > leftItem,
				// If this child is to the left of rightItem, then its entire
				// subtree is within the upper bound.
				fullyWithinUpperBound || i < rightItem,
			)
			a.mergeFunc(&result, v)
		}
	}
	return result
}

// LevelAnnotation calculates the annotation defined by this TableAnnotator for all
// files in the given LevelMetadata. A pointer to the TableAnnotator is used as the
// key for pre-calculated values, so the same TableAnnotator must be used to avoid
// duplicate computation.
func (a *TableAnnotator[T]) LevelAnnotation(lm LevelMetadata) T {
	if lm.Empty() {
		var zero T
		return zero
	}

	v, _ := a.nodeAnnotation(lm.tree.root)
	return *v
}

// MultiLevelAnnotation calculates the annotation defined by this TableAnnotator for
// all files across the given levels. A pointer to the TableAnnotator is used as the
// key for pre-calculated values, so the same TableAnnotator must be used to avoid
// duplicate computation.
func (a *TableAnnotator[T]) MultiLevelAnnotation(lms []LevelMetadata) T {
	var result T
	for l := 0; l < len(lms); l++ {
		if !lms[l].Empty() {
			v, _ := a.nodeAnnotation(lms[l].tree.root)
			a.mergeFunc(&result, *v)
		}
	}
	return result
}

// LevelRangeAnnotation calculates the annotation defined by this TableAnnotator for
// the files within LevelMetadata which are within the range
// [lowerBound, upperBound). A pointer to the TableAnnotator is used as the key for
// pre-calculated values, so the same TableAnnotator must be used to avoid duplicate
// computation.
func (a *TableAnnotator[T]) LevelRangeAnnotation(
	cmp base.Compare, lm LevelMetadata, bounds base.UserKeyBounds,
) T {
	if lm.Empty() {
		var zero T
		return zero
	}

	return a.accumulateRangeAnnotation(lm.tree.root, cmp, bounds, false, false)
}

// VersionRangeAnnotation calculates the annotation defined by this TableAnnotator
// for all files within the given Version which are within the range
// defined by bounds.
func (a *TableAnnotator[T]) VersionRangeAnnotation(v *Version, bounds base.UserKeyBounds) T {
	var result T
	accumulateSlice := func(ls LevelSlice) {
		if ls.Empty() {
			return
		}
		val := a.accumulateRangeAnnotation(ls.iter.r, v.cmp.Compare, bounds, false, false)
		a.mergeFunc(&result, val)
	}
	for _, ls := range v.L0SublevelFiles {
		accumulateSlice(ls)
	}
	for _, lm := range v.Levels[1:] {
		accumulateSlice(lm.Slice())
	}
	return result
}

// MakePickFileAnnotator ... TODO implements the AnnotationAggregator interface. It defines
// an aggregator that picks a single file from a set of eligible files.
func MakePickFileAnnotator(
	idx TableAnnotationIdx, funcs PickFileAnnotatorFuncs,
) TableAnnotator[*TableMetadata] {
	return MakeTableAnnotator[*TableMetadata](idx, TableAnnotatorFuncs[*TableMetadata]{
		Merge: func(dst **TableMetadata, src *TableMetadata) {
			if src != nil && (*dst == nil || funcs.Compare(src, *dst)) {
				*dst = src
			}
		},
		Table: func(f *TableMetadata) (_ *TableMetadata, cacheOK bool) {
			eligible, cacheOK := funcs.Filter(f)
			if !eligible {
				return nil, cacheOK
			}
			return f, cacheOK
		},
	})
}

type PickFileAnnotatorFuncs struct {
	// Filter takes a TableMetadata and returns whether it is eligible to be
	// picked by this PickFileAggregator. The second return value indicates
	// whether this eligibility is stable and thus cacheable.
	Filter func(f *TableMetadata) (eligible bool, cacheOK bool)
	// Compare compares two instances of TableMetadata and returns true if the
	// first one should be picked over the second one. It may assume that both
	// arguments are non-nil.
	Compare func(f1 *TableMetadata, f2 *TableMetadata) bool
}
