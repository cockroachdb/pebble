// Copyright 2024 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package manifest

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
	// when a LevelMetadata is empty.
	Zero() T

	// Accumulate computes the annotation for a single file in a level's
	// metadata. It also returns a bool flag indicating whether or not the
	// value is stable and okay to cache as an annotation, which must be false
	// if the file's value may change over the life of the file.
	Accumulate(f *FileMetadata) (v T, cacheOK bool)

	// Merge combines two annotated values src and dst, returning the result.
	Merge(v1 T, v2 T) T
}

type annotation struct {
	// annotator is a pointer to the Annotator that computed this annotation.
	// NB: This is untyped to allow AnnotationAggregator to use Go generics,
	// since annotations are stored in a slice on each node and a single
	// slice cannot contain elements with different type parameters.
	annotator interface{}
	// vptr is a pointer to the annotation value, the output of either
	// annotator.Value or annotator.Merge.
	// NB: This is untyped for the same reason as annotator above. We store
	// a pointer instead of the value itself in order to eliminate a heap
	// allocation every time an annotation is computed.
	vptr interface{}
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
		vptr:      new(T),
	})
	return &n.annot[len(n.annot)-1]
}

// nodeRangeAnnotation computes this annotator's annotation of this node across
// all files in the inclusive range [lowerBound, upperBound]. The second return
// value indicates whether the annotation is stable and thus cacheable.
func (a *Annotator[T]) nodeRangeAnnotation(
	n *node,
	cmp btreeCmp,
	// NB: lowerBound and upperBound are passed as FileMetadata pointers
	// because btreeCmp expects this type.
	lowerBound *FileMetadata,
	upperBound *FileMetadata,
	// fullyWithinLowerBound and fullyWithinUpperBound indicate whether
	// this node's subtree is already known to be within each bound.
	fullyWithinLowerBound bool,
	fullyWithinUpperBound bool,
) (v T, cacheOK bool) {
	annot := a.findAnnotation(n)
	// If the annotation is already marked as valid and this node's
	// subtree is fully within the bounds, we can return it without
	// recomputing anything.
	if fullyWithinLowerBound && fullyWithinUpperBound && annot.valid {
		return *annot.vptr.(*T), true
	}

	aggregated := a.Aggregator.Zero()
	valid := true

	left, right := 0, int(n.count)
	overlapsRight := false
	if !fullyWithinLowerBound {
		left, _ = n.find(cmp, lowerBound)
	}
	if !fullyWithinUpperBound {
		right, overlapsRight = n.find(cmp, upperBound)
	}

	// Iterate over every item and child node which could overlap
	// with the bounds.
	for i := left; i <= right; i++ {
		if !n.leaf {
			v, ok := a.nodeRangeAnnotation(
				n.children[i],
				cmp,
				lowerBound,
				upperBound,
				// The children at left and right may or may not be within
				// the bounds, but all other children must be due to the
				// btree's structure.
				fullyWithinLowerBound || i > left,
				fullyWithinUpperBound || i < right,
			)
			aggregated = a.Aggregator.Merge(v, aggregated)
			valid = valid && ok
		}

		// Accumulate annotations from every item in the range [left, right).
		// NB: We only accumulate from n.items[right] in the case that
		// its smallest key is exactly equal to the upper bound.
		if overlapsRight || i < right {
			v, ok := a.Aggregator.Accumulate(n.items[i])
			aggregated = a.Aggregator.Merge(v, aggregated)
			valid = valid && ok
		}
	}

	// Update this node's cached annotation only if we accumulated from
	// this node's entire subtree.
	if fullyWithinLowerBound && fullyWithinUpperBound {
		*annot.vptr.(*T) = aggregated
		annot.valid = valid
	}

	return aggregated, valid
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
func (a *Annotator[T]) LevelAnnotation(lm LevelMetadata) T {
	if lm.Empty() {
		return a.Aggregator.Zero()
	}

	v, _ := a.nodeRangeAnnotation(lm.tree.root, lm.tree.cmp, nil, nil, true, true)
	return v
}

// LevelAnnotation calculates the annotation defined by this Annotator for all
// files across the given levels. A pointer to the Annotator is used as the
// key for pre-calculated values, so the same Annotator must be used to avoid
// duplicate computation. Annotation must not be called concurrently, and in
// practice this is achieved by requiring callers to hold DB.mu.
func (a *Annotator[T]) MultiLevelAnnotation(lms []LevelMetadata) T {
	aggregated := a.Aggregator.Zero()
	for l := 0; l < len(lms); l++ {
		if !lms[l].Empty() {
			v := a.LevelAnnotation(lms[l])
			aggregated = a.Aggregator.Merge(v, aggregated)
		}
	}
	return aggregated
}

// LevelRangeAnnotation calculates the annotation defined by this Annotator for
// the files within LevelMetadata which are within the inclusive range
// [lowerBound, upperBound]. A pointer to the Annotator is used as the key for
// pre-calculated values, so the same Annotator must be used to avoid duplicate
// computation. Annotation must not be called concurrently, and in practice this
// is achieved by requiring callers to hold DB.mu.
func (a *Annotator[T]) LevelRangeAnnotation(
	lm LevelMetadata, lowerBound InternalKey, upperBound InternalKey,
) T {
	if lm.Empty() {
		return a.Aggregator.Zero()
	}

	// fake filemetadata
	lowerBoundMeta := &FileMetadata{
		Smallest: lowerBound,
	}
	upperBoundMeta := &FileMetadata{
		Smallest: upperBound,
	}

	// TODO: assert lm.tree.cmp is by smallest key (L0cmp)

	// we ignore ok because
	v, _ := a.nodeRangeAnnotation(lm.tree.root, lm.tree.cmp, lowerBoundMeta, upperBoundMeta, false, false)
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

func (sa sumAggregator) Zero() uint64 {
	return 0
}

func (sa sumAggregator) Accumulate(f *FileMetadata) (v uint64, cacheOK bool) {
	return sa.accumulateFunc(f)
}

func (sa sumAggregator) Merge(val1 uint64, val2 uint64) uint64 {
	return val1 + val2
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
func (fa PickFileAggregator) Zero() *FileMetadata {
	return nil
}

// Accumulate implements AnnotationAggregator.Accumulate, accumulating a single
// file as long as it is eligible to be picked.
func (fa PickFileAggregator) Accumulate(f *FileMetadata) (v *FileMetadata, cacheOK bool) {
	eligible, ok := fa.Filter(f)
	if eligible {
		return f, ok
	}
	return nil, ok
}

// Merge implements AnnotationAggregator.Merge by picking a single file based
// on the output of PickFileAggregator.Compare.
func (fa PickFileAggregator) Merge(f1 *FileMetadata, f2 *FileMetadata) *FileMetadata {
	if f1 == nil {
		return f2
	} else if f2 == nil {
		return f1
	}
	if fa.Compare(f1, f2) {
		return f1
	}
	return f2
}
