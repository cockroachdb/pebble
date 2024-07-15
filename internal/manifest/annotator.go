// Copyright 2024 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package manifest

import (
	"sort"

	"github.com/cockroachdb/pebble/internal/base"
)

// The Annotator type defined below is used by other packages to lazily
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

// An Annotator defines a computation over a level's FileMetadata. If the
// computation is stable and uses inputs that are fixed for the lifetime of
// a FileMetadata, the LevelMetadata's internal data structures are annotated
// with the intermediary computations. This allows the computation to be
// computed incrementally as edits are applied to a level.
type Annotator[T any] struct {
	Aggregator AnnotationAggregator[T]
}

// An AnnotationAggregator defines how an annotation should be accumulated
// from a single FileMetadata and merged with other annotated values.
type AnnotationAggregator[T any] interface {
	// Zero returns the zero value of an annotation. This value is returned
	// when a LevelMetadata is empty. The dst argument, if non-nil, is an
	// obsolete value previously returned by this Annotator and may be
	// overwritten and reused to avoid a memory allocation.
	Zero(dst *T) *T

	// Accumulate computes the annotation for a single file in a level's
	// metadata. It merges the file's value into dst and returns a bool flag
	// indicating whether or not the value is stable and okay to cache as an
	// annotation. If the file's value may change over the life of the file,
	// the annotator must return false.
	//
	// Implementations may modify dst and return it to avoid an allocation.
	Accumulate(f *FileMetadata, dst *T) (v *T, cacheOK bool)

	// Merge combines two values src and dst, returning the result.
	// Implementations may modify dst and return it to avoid an allocation.
	Merge(src *T, dst *T) *T
}

type annotation struct {
	// annotator is a pointer to the Annotator that computed this annotation.
	// NB: This is untyped to allow AnnotationAggregator to use Go generics,
	// since annotations are stored in a slice on each node and a single
	// slice cannot contain elements with different type parameters.
	annotator interface{}
	// v is contains the annotation value, the output of either
	// AnnotationAggregator.Accumulate or AnnotationAggregator.Merge.
	// NB: This is untyped for the same reason as annotator above.
	v interface{}

	// scratch is used to hold the aggregated annotation value when computing
	// range annotations in order to avoid overwriting an already cached
	// annotation, and to also avoid additional allocations.
	scratch interface{}
	// valid indicates whether future reads of the annotation may use the
	// value as-is. If false, v will be zeroed and recalculated.
	valid bool
}

// findAnnotation finds this Annotator's annotation on a node, creating
// one if it doesn't already exist.
func (a *Annotator[T]) findAnnotation(n *node) *annotation {
	for i := range n.annot {
		if n.annot[i].annotator == a {
			return &n.annot[i]
		}
	}

	// This node has never been annotated by a. Create a new annotation.
	n.annot = append(n.annot, annotation{
		annotator: a,
		v:         a.Aggregator.Zero(nil),
	})
	return &n.annot[len(n.annot)-1]
}

// nodeRangeAnnotation computes this annotator's annotation of a node across
// all files in the range defined by lowerBound and upperBound. The second
// return value indicates whether the annotation is stable and thus cacheable.
func (a *Annotator[T]) nodeRangeAnnotation(
	n *node,
	cmp base.Compare,
	// lowerBound and upperBound may be nil to indicate no lower or upper bound.
	lowerBound []byte,
	// upperBound is a UserKeyBoundary that may be inclusive or exclusive.
	upperBound *base.UserKeyBoundary,
) (v *T, cacheOK bool) {
	annot := a.findAnnotation(n)
	// If the annotation is already marked as valid and this node's
	// subtree is fully within the bounds, we can return it without
	// recomputing anything.
	if lowerBound == nil && upperBound == nil && annot.valid {
		return annot.v.(*T), true
	}

	// We will accumulate annotations from each item in the end-exclusive
	// range [leftItem, rightItem).
	leftItem, rightItem := 0, int(n.count)
	if lowerBound != nil {
		// leftItem is the index of the first item that overlaps the lower bound.
		leftItem = sort.Search(int(n.count), func(i int) bool {
			return cmp(lowerBound, n.items[i].Largest.UserKey) <= 0
		})
	}
	if upperBound != nil {
		// rightItem is the index of the first item that does not overlap the
		// upper bound.
		rightItem = sort.Search(int(n.count), func(i int) bool {
			return !upperBound.IsUpperBoundFor(cmp, n.items[i].Smallest.UserKey)
		})
	}

	var result *T
	switch {
	// If there is no cached annotation, we can directly write to the node's
	// annotation value.
	case !annot.valid:
		result = a.Aggregator.Zero(annot.v.(*T))
	// Otherwise, use annot.scratch as scratch space to avoid allocations.
	// The allocation for annot.scratch is performed lazily here instead of
	// within findAnnotation to avoid an allocation when range annotations
	// are not used.
	case annot.scratch == nil:
		annot.scratch = a.Aggregator.Zero(nil)
		result = annot.scratch.(*T)
	default:
		result = a.Aggregator.Zero(annot.scratch.(*T))
	}

	valid := true
	// Accumulate annotations from every item that overlaps the bounds.
	for i := leftItem; i < rightItem; i++ {
		v, ok := a.Aggregator.Accumulate(n.items[i], result)
		result = v
		valid = valid && ok
	}

	if !n.leaf {
		// We will accumulate annotations from each child in the end-inclusive
		// range [leftChild, rightChild].
		leftChild, rightChild := leftItem, rightItem
		// If the lower bound overlaps with the child at leftItem, there is no
		// need to accumulate annotations from the child to its left.
		if leftItem < int(n.count) && cmp(lowerBound, n.items[leftItem].Smallest.UserKey) >= 0 {
			leftChild++
		}
		// If the upper bound spans beyond the child at rightItem, we must also
		// accumulate annotations from the child to its right.
		if rightItem < int(n.count) && upperBound.IsUpperBoundFor(cmp, n.items[rightItem].Largest.UserKey) {
			rightChild++
		}

		for i := leftChild; i <= rightChild; i++ {
			newLowerBound, newUpperBound := lowerBound, upperBound
			// If this child is to the right of leftItem, then its entire
			// subtree is within the lower bound.
			if i > leftItem {
				newLowerBound = nil
			}
			// If this child is to the left of rightItem, then its entire
			// subtree is within the upper bound.
			if i < rightItem {
				newUpperBound = nil
			}

			v, ok := a.nodeRangeAnnotation(
				n.children[i],
				cmp,
				newLowerBound,
				newUpperBound,
			)
			result = a.Aggregator.Merge(v, result)
			valid = valid && ok
		}
	}

	// Update this node's cached annotation only if we accumulated from its
	// entire subtree.
	if lowerBound == nil && upperBound == nil {
		annot.v = result
		annot.valid = valid
	}

	return result, valid
}

// InvalidateAnnotation removes any existing cached annotations from this
// annotator from a node's subtree.
func (a *Annotator[T]) invalidateNodeAnnotation(n *node) {
	annot := a.findAnnotation(n)
	annot.valid = false
	if !n.leaf {
		for i := int16(0); i <= n.count; i++ {
			a.invalidateNodeAnnotation(n.children[i])
		}
	}
}

// LevelAnnotation calculates the annotation defined by this Annotator for all
// files in the given LevelMetadata. A pointer to the Annotator is used as the
// key for pre-calculated values, so the same Annotator must be used to avoid
// duplicate computation. Annotation must not be called concurrently, and in
// practice this is achieved by requiring callers to hold DB.mu.
func (a *Annotator[T]) LevelAnnotation(lm LevelMetadata) *T {
	if lm.Empty() {
		return a.Aggregator.Zero(nil)
	}

	v, _ := a.nodeRangeAnnotation(lm.tree.root, lm.tree.cmp, nil, nil)
	return v
}

// LevelAnnotation calculates the annotation defined by this Annotator for all
// files across the given levels. A pointer to the Annotator is used as the
// key for pre-calculated values, so the same Annotator must be used to avoid
// duplicate computation. Annotation must not be called concurrently, and in
// practice this is achieved by requiring callers to hold DB.mu.
func (a *Annotator[T]) MultiLevelAnnotation(lms []LevelMetadata) *T {
	aggregated := a.Aggregator.Zero(nil)
	for l := 0; l < len(lms); l++ {
		if !lms[l].Empty() {
			v := a.LevelAnnotation(lms[l])
			aggregated = a.Aggregator.Merge(v, aggregated)
		}
	}
	return aggregated
}

// LevelRangeAnnotation calculates the annotation defined by this Annotator for
// the files within LevelMetadata which are within the range
// [lowerBound, upperBound). A pointer to the Annotator is used as the key for
// pre-calculated values, so the same Annotator must be used to avoid duplicate
// computation. Annotation must not be called concurrently, and in practice this
// is achieved by requiring callers to hold DB.mu.
func (a *Annotator[T]) LevelRangeAnnotation(lm LevelMetadata, bounds *base.UserKeyBounds) *T {
	if lm.Empty() {
		return a.Aggregator.Zero(nil)
	}

	v, _ := a.nodeRangeAnnotation(lm.tree.root, lm.tree.cmp, bounds.Start, &bounds.End)
	return v
}

// InvalidateAnnotation clears any cached annotations defined by Annotator. A
// pointer to the Annotator is used as the key for pre-calculated values, so
// the same Annotator must be used to clear the appropriate cached annotation.
// InvalidateAnnotation must not be called concurrently, and in practice this
// is achieved by requiring callers to hold DB.mu.
func (a *Annotator[T]) InvalidateLevelAnnotation(lm LevelMetadata) {
	if lm.Empty() {
		return
	}
	a.invalidateNodeAnnotation(lm.tree.root)
}

// sumAggregator defines an Aggregator which sums together a uint64 value
// across files.
type sumAggregator struct {
	accumulateFunc func(f *FileMetadata) (v uint64, cacheOK bool)
}

func (sa sumAggregator) Zero(dst *uint64) *uint64 {
	if dst == nil {
		return new(uint64)
	}
	*dst = 0
	return dst
}

func (sa sumAggregator) Accumulate(f *FileMetadata, dst *uint64) (v *uint64, cacheOK bool) {
	accumulated, ok := sa.accumulateFunc(f)
	*dst += accumulated
	return dst, ok
}

func (sa sumAggregator) Merge(src *uint64, dst *uint64) *uint64 {
	*dst += *src
	return dst
}

// SumAnnotator takes a function that computes a uint64 value from a single
// FileMetadata and returns an Annotator that sums together the values across
// files.
func SumAnnotator(accumulate func(f *FileMetadata) (v uint64, cacheOK bool)) *Annotator[uint64] {
	return &Annotator[uint64]{
		Aggregator: sumAggregator{
			accumulateFunc: accumulate,
		},
	}
}

// NumFilesAnnotator is an Annotator which computes an annotation value
// equal to the number of files included in the annotation. Particularly, it
// can be used to efficiently calculate the number of files in a given key
// range using range annotations.
var NumFilesAnnotator = SumAnnotator(func(f *FileMetadata) (uint64, bool) {
	return 1, true
})

// PickFileAggregator implements the AnnotationAggregator interface. It defines
// an aggregator that picks a single file from a set of eligible files.
type PickFileAggregator struct {
	// Filter takes a FileMetadata and returns whether it is eligible to be
	// picked by this PickFileAggregator. The second return value indicates
	// whether this eligibility is stable and thus cacheable.
	Filter func(f *FileMetadata) (eligible bool, cacheOK bool)
	// Compare compares two instances of FileMetadata and returns true if
	// the first one should be picked over the second one. It may assume
	// that both arguments are non-nil.
	Compare func(f1 *FileMetadata, f2 *FileMetadata) bool
}

// Zero implements AnnotationAggregator.Zero, returning nil as the zero value.
func (fa PickFileAggregator) Zero(dst *FileMetadata) *FileMetadata {
	return nil
}

func (fa PickFileAggregator) mergePickedFiles(src *FileMetadata, dst *FileMetadata) *FileMetadata {
	switch {
	case src == nil:
		return dst
	case dst == nil:
		return src
	case fa.Compare(src, dst):
		return src
	default:
		return dst
	}
}

// Accumulate implements AnnotationAggregator.Accumulate, accumulating a single
// file as long as it is eligible to be picked.
func (fa PickFileAggregator) Accumulate(
	f *FileMetadata, dst *FileMetadata,
) (v *FileMetadata, cacheOK bool) {
	eligible, ok := fa.Filter(f)
	if eligible {
		return fa.mergePickedFiles(f, dst), ok
	}
	return dst, ok
}

// Merge implements AnnotationAggregator.Merge by picking a single file based
// on the output of PickFileAggregator.Compare.
func (fa PickFileAggregator) Merge(src *FileMetadata, dst *FileMetadata) *FileMetadata {
	return fa.mergePickedFiles(src, dst)
}
