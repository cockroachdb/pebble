// Copyright 2024 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package manifest

import (
	"sort"
	"sync/atomic"

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
}

func NewTableAnnotator[T any](agg AnnotationAggregator[T, *TableMetadata]) *TableAnnotator[T] {
	return &TableAnnotator[T]{
		annotator: annotator[T, *TableMetadata]{Aggregator: agg},
	}
}

// A BlobFileAnnotator defines a computation over a version's set of blob files.
type BlobFileAnnotator[T any] struct {
	annotator[T, BlobFileMetadata]
}

func NewBlobFileAnnotator[T any](
	agg AnnotationAggregator[T, BlobFileMetadata],
) *BlobFileAnnotator[T] {
	return &BlobFileAnnotator[T]{
		annotator: annotator[T, BlobFileMetadata]{Aggregator: agg},
	}
}

type annotator[T any, M fileMetadata] struct {
	Aggregator AnnotationAggregator[T, M]
}

// An AnnotationAggregator defines how an annotation should be accumulated from
// a single TableMetadata and merged with other annotated values.
type AnnotationAggregator[T any, M fileMetadata] interface {
	// Zero returns the zero value of an annotation. This value is returned
	// when a LevelMetadata is empty. The dst argument, if non-nil, is an
	// obsolete value previously returned by this TableAnnotator and may be
	// overwritten and reused to avoid a memory allocation.
	Zero(dst *T) *T

	// Accumulate computes the annotation for a single file in a level's
	// metadata. It merges the file's value into dst and returns a bool flag
	// indicating whether or not the value is stable and okay to cache as an
	// annotation. If the file's value may change over the life of the file,
	// the annotator must return false.
	//
	// Implementations may modify dst and return it to avoid an allocation.
	Accumulate(f M, dst *T) (v *T, cacheOK bool)

	// Merge combines two values src and dst, returning the result.
	// Implementations may modify dst and return it to avoid an allocation.
	Merge(src *T, dst *T) *T
}

// A PartialOverlapAnnotationAggregator is an extension of AnnotationAggregator
// that allows for custom accumulation of range annotations for tables that only
// partially overlap with the range.
type PartialOverlapAnnotationAggregator[T any] interface {
	AnnotationAggregator[T, *TableMetadata]
	AccumulatePartialOverlap(f *TableMetadata, dst *T, bounds base.UserKeyBounds) *T
}

type annotation struct {
	// annotator is a pointer to the TableAnnotator that computed this annotation.
	// NB: This is untyped to allow AnnotationAggregator to use Go generics,
	// since annotations are stored in a slice on each node and a single
	// slice cannot contain elements with different type parameters.
	annotator interface{}
	// v is contains the annotation value, the output of either
	// AnnotationAggregator.Accumulate or AnnotationAggregator.Merge.
	// NB: This is untyped for the same reason as annotator above.
	v atomic.Value
	// valid indicates whether future reads of the annotation may use the
	// value as-is. If false, v will be zeroed and recalculated.
	valid atomic.Bool
}

func (a *annotator[T, M]) findExistingAnnotation(n *node[M]) *annotation {
	n.annotMu.RLock()
	defer n.annotMu.RUnlock()
	for i := range n.annot {
		if n.annot[i].annotator == a {
			return &n.annot[i]
		}
	}
	return nil
}

// findAnnotation finds this TableAnnotator's annotation on a node, creating
// one if it doesn't already exist.
func (a *annotator[T, M]) findAnnotation(n *node[M]) *annotation {
	if a := a.findExistingAnnotation(n); a != nil {
		return a
	}
	n.annotMu.Lock()
	defer n.annotMu.Unlock()

	// This node has never been annotated by a. Create a new annotation.
	n.annot = append(n.annot, annotation{
		annotator: a,
		v:         atomic.Value{},
	})
	n.annot[len(n.annot)-1].v.Store(a.Aggregator.Zero(nil))
	return &n.annot[len(n.annot)-1]
}

// nodeAnnotation computes this annotator's annotation of this node across all
// files in the node's subtree. The second return value indicates whether the
// annotation is stable and thus cacheable.
func (a *annotator[T, M]) nodeAnnotation(n *node[M]) (t *T, cacheOK bool) {
	annot := a.findAnnotation(n)
	// If the annotation is already marked as valid, we can return it without
	// recomputing anything.
	if annot.valid.Load() {
		// The load of these two atomics should be safe, as we don't need to
		// guarantee invalidations are concurrent-safe. See the comment on
		// InvalidateLevelAnnotation about why.
		return annot.v.Load().(*T), true
	}

	t = a.Aggregator.Zero(t)
	valid := true

	for i := int16(0); i <= n.count; i++ {
		if !n.isLeaf() {
			v, ok := a.nodeAnnotation(n.child(i))
			t = a.Aggregator.Merge(v, t)
			valid = valid && ok
		}

		if i < n.count {
			var ok bool
			t, ok = a.Aggregator.Accumulate(n.items[i], t)
			valid = valid && ok
		}
	}

	if valid {
		// Two valid annotations should be identical, so this is
		// okay.
		annot.v.Store(t)
		annot.valid.Store(valid)
	}

	return t, valid
}

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
	dst *T,
) *T {
	// If this node's subtree is fully within the bounds, compute a regular
	// annotation.
	if fullyWithinLowerBound && fullyWithinUpperBound {
		v, _ := a.nodeAnnotation(n)
		dst = a.Aggregator.Merge(v, dst)
		return dst
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

	// Accumulate annotations from every item that overlaps the bounds.
	for i := leftItem; i < rightItem; i++ {
		if i == leftItem || i == rightItem-1 {
			if agg, ok := a.Aggregator.(PartialOverlapAnnotationAggregator[T]); ok {
				fb := n.items[i].UserKeyBounds()
				if cmp(bounds.Start, fb.Start) > 0 || bounds.End.CompareUpperBounds(cmp, fb.End) < 0 {
					dst = agg.AccumulatePartialOverlap(n.items[i], dst, bounds)
					continue
				}
			}
		}
		v, _ := a.Aggregator.Accumulate(n.items[i], dst)
		dst = v
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
			dst = a.accumulateRangeAnnotation(
				n.child(int16(i)),
				cmp,
				bounds,
				// If this child is to the right of leftItem, then its entire
				// subtree is within the lower bound.
				fullyWithinLowerBound || i > leftItem,
				// If this child is to the left of rightItem, then its entire
				// subtree is within the upper bound.
				fullyWithinUpperBound || i < rightItem,
				dst,
			)
		}
	}
	return dst
}

// InvalidateAnnotation removes any existing cached annotations from this
// annotator from a node's subtree.
func (a *TableAnnotator[T]) invalidateNodeAnnotation(n *node[*TableMetadata]) {
	annot := a.findAnnotation(n)
	annot.valid.Store(false)
	if !n.isLeaf() {
		for i := int16(0); i <= n.count; i++ {
			a.invalidateNodeAnnotation(n.child(i))
		}
	}
}

// LevelAnnotation calculates the annotation defined by this TableAnnotator for all
// files in the given LevelMetadata. A pointer to the TableAnnotator is used as the
// key for pre-calculated values, so the same TableAnnotator must be used to avoid
// duplicate computation.
func (a *TableAnnotator[T]) LevelAnnotation(lm LevelMetadata) *T {
	if lm.Empty() {
		return a.Aggregator.Zero(nil)
	}

	v, _ := a.nodeAnnotation(lm.tree.root)
	return v
}

// MultiLevelAnnotation calculates the annotation defined by this TableAnnotator for
// all files across the given levels. A pointer to the TableAnnotator is used as the
// key for pre-calculated values, so the same TableAnnotator must be used to avoid
// duplicate computation.
func (a *TableAnnotator[T]) MultiLevelAnnotation(lms []LevelMetadata) *T {
	aggregated := a.Aggregator.Zero(nil)
	for l := 0; l < len(lms); l++ {
		if !lms[l].Empty() {
			v := a.LevelAnnotation(lms[l])
			aggregated = a.Aggregator.Merge(v, aggregated)
		}
	}
	return aggregated
}

// LevelRangeAnnotation calculates the annotation defined by this TableAnnotator for
// the files within LevelMetadata which are within the range
// [lowerBound, upperBound). A pointer to the TableAnnotator is used as the key for
// pre-calculated values, so the same TableAnnotator must be used to avoid duplicate
// computation.
func (a *TableAnnotator[T]) LevelRangeAnnotation(
	cmp base.Compare, lm LevelMetadata, bounds base.UserKeyBounds,
) *T {
	if lm.Empty() {
		return a.Aggregator.Zero(nil)
	}

	var dst *T
	dst = a.Aggregator.Zero(dst)
	dst = a.accumulateRangeAnnotation(lm.tree.root, cmp, bounds, false, false, dst)
	return dst
}

// VersionRangeAnnotation calculates the annotation defined by this TableAnnotator
// for all files within the given Version which are within the range
// defined by bounds.
func (a *TableAnnotator[T]) VersionRangeAnnotation(v *Version, bounds base.UserKeyBounds) *T {
	var dst *T
	dst = a.Aggregator.Zero(dst)
	accumulateSlice := func(ls LevelSlice) {
		if ls.Empty() {
			return
		}
		dst = a.accumulateRangeAnnotation(ls.iter.r, v.cmp.Compare, bounds, false, false, dst)
	}
	for _, ls := range v.L0SublevelFiles {
		accumulateSlice(ls)
	}
	for _, lm := range v.Levels[1:] {
		accumulateSlice(lm.Slice())
	}
	return dst
}

// InvalidateLevelAnnotation clears any cached annotations defined by TableAnnotator.
// A pointer to the TableAnnotator is used as the key for pre-calculated values, so
// the same TableAnnotator must be used to clear the appropriate cached annotation.
// Calls to InvalidateLevelAnnotation are *not* concurrent-safe with any other
// calls to TableAnnotator methods for the same TableAnnotator (concurrent calls from
// other annotators are fine). Any calls to this function must have some
// externally-guaranteed mutual exclusion.
func (a *TableAnnotator[T]) InvalidateLevelAnnotation(lm LevelMetadata) {
	if lm.Empty() {
		return
	}
	a.invalidateNodeAnnotation(lm.tree.root)
}

// Annotation calculates the annotation defined by this BlobFileAnnotator for
// all blob files in the given set.
// A pointer to the TableAnnotator is used as the key for pre-calculated values,
// so the same TableAnnotator must be used to avoid duplicate computation.
func (a *BlobFileAnnotator[T]) Annotation(blobFiles *BlobFileSet) *T {
	if blobFiles.tree.Count() == 0 {
		return a.Aggregator.Zero(nil)
	}

	v, _ := a.nodeAnnotation(blobFiles.tree.root)
	return v
}

// SumAggregator defines an Aggregator which sums together a uint64 value
// across files.
type SumAggregator struct {
	AccumulateFunc               func(f *TableMetadata) (v uint64, cacheOK bool)
	AccumulatePartialOverlapFunc func(f *TableMetadata, bounds base.UserKeyBounds) uint64
}

// Zero implements AnnotationAggregator.Zero, returning a new uint64 set to 0.
func (sa SumAggregator) Zero(dst *uint64) *uint64 {
	if dst == nil {
		return new(uint64)
	}
	*dst = 0
	return dst
}

// Accumulate implements AnnotationAggregator.Accumulate, accumulating a single
// file's uint64 value.
func (sa SumAggregator) Accumulate(f *TableMetadata, dst *uint64) (v *uint64, cacheOK bool) {
	accumulated, ok := sa.AccumulateFunc(f)
	*dst += accumulated
	return dst, ok
}

// AccumulatePartialOverlap implements
// PartialOverlapAnnotationAggregator.AccumulatePartialOverlap, accumulating a
// single file's uint64 value for a file which only partially overlaps with the
// range defined by bounds.
func (sa SumAggregator) AccumulatePartialOverlap(
	f *TableMetadata, dst *uint64, bounds base.UserKeyBounds,
) *uint64 {
	if sa.AccumulatePartialOverlapFunc == nil {
		v, _ := sa.Accumulate(f, dst)
		return v
	}
	*dst += sa.AccumulatePartialOverlapFunc(f, bounds)
	return dst
}

// Merge implements AnnotationAggregator.Merge by summing two uint64 values.
func (sa SumAggregator) Merge(src *uint64, dst *uint64) *uint64 {
	*dst += *src
	return dst
}

// SumAnnotator takes a function that computes a uint64 value from a single
// TableMetadata and returns an TableAnnotator that sums together the values across
// files.
func SumAnnotator(
	accumulate func(f *TableMetadata) (v uint64, cacheOK bool),
) *TableAnnotator[uint64] {
	return NewTableAnnotator[uint64](SumAggregator{AccumulateFunc: accumulate})
}

// NumFilesAnnotator is an TableAnnotator which computes an annotation value
// equal to the number of files included in the annotation. Particularly, it
// can be used to efficiently calculate the number of files in a given key
// range using range annotations.
var NumFilesAnnotator = SumAnnotator(func(f *TableMetadata) (uint64, bool) {
	return 1, true
})

// PickFileAggregator implements the AnnotationAggregator interface. It defines
// an aggregator that picks a single file from a set of eligible files.
type PickFileAggregator struct {
	// Filter takes a TableMetadata and returns whether it is eligible to be
	// picked by this PickFileAggregator. The second return value indicates
	// whether this eligibility is stable and thus cacheable.
	Filter func(f *TableMetadata) (eligible bool, cacheOK bool)
	// Compare compares two instances of TableMetadata and returns true if the
	// first one should be picked over the second one. It may assume that both
	// arguments are non-nil.
	Compare func(f1 *TableMetadata, f2 *TableMetadata) bool
}

// Zero implements AnnotationAggregator.Zero, returning nil as the zero value.
func (fa PickFileAggregator) Zero(dst *TableMetadata) *TableMetadata {
	return nil
}

func (fa PickFileAggregator) mergePickedFiles(
	src *TableMetadata, dst *TableMetadata,
) *TableMetadata {
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
	f *TableMetadata, dst *TableMetadata,
) (v *TableMetadata, cacheOK bool) {
	eligible, ok := fa.Filter(f)
	if eligible {
		return fa.mergePickedFiles(f, dst), ok
	}
	return dst, ok
}

// Merge implements AnnotationAggregator.Merge by picking a single file based
// on the output of PickFileAggregator.Compare.
func (fa PickFileAggregator) Merge(src *TableMetadata, dst *TableMetadata) *TableMetadata {
	return fa.mergePickedFiles(src, dst)
}
