// Copyright 2024 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package manifest

import "sync/atomic"

// annotator aggregates arbitrary values across B-Tree subtrees.
type annotator[T any, M fileMetadata] struct {
	// idx is an index into nodeAnnotation.cachedValues.
	idx annotationIdx

	// mergeFunc combines two values src and dst into dst. Merging with the zero T
	// value should be a no-op.
	mergeFunc func(dst *T, src T)

	// itemFunc computes the annotation for a single element in a tree. Returns
	// the value and a bool flag indicating whether the value is stable and okay
	// to cache as an annotation. If the file's value may change over the life of
	// the file, the annotator must return false.
	itemFunc func(M) (_ T, cacheOK bool)
}

// annotationIdx is an index associated with a specific annotator. Only
// annotators with distinct annotationIdx values can be used with the same tree.
type annotationIdx int

// nodeAnnotations stores the cached annotation values for a B-Tree node.
type nodeAnnotations struct {
	// cachedValues contains the aggregated value for a B-Tree node's subtree.
	// Only set when all information is available; once it is set, that value is
	// final for that subtree.
	cachedValues [maxAnnotationsPerNode]atomic.Value
}

// Reset removes all cached values. Must not run concurrently with any
// annotators.
func (a *nodeAnnotations) Reset() {
	// We are zeroing out atomic.Values; this is ok because there are no
	// concurrent uses.
	*a = nodeAnnotations{}
}

const maxAnnotationsPerNode = 4

//type annotator[T any, M fileMetadata] struct {
//	Aggregator AnnotationAggregator[T, M]
//}
//
//// An AnnotationAggregator defines how an annotation should be accumulated from
//// a single TableMetadata and merged with other annotated values.
//type AnnotationAggregator[T any, M fileMetadata] interface {
//	// Zero returns the zero value of an annotation.
//	Zero() *T
//
//	// Accumulate computes the annotation for a single file in a level's
//	// metadata. It merges the file's value into dst and returns a bool flag
//	// indicating whether or not the value is stable and okay to cache as an
//	// annotation. If the file's value may change over the life of the file,
//	// the annotator must return false.
//	//
//	// Implementations may modify dst and return it to avoid an allocation.
//	Accumulate(f M, dst *T) (v *T, cacheOK bool)
//
//	// Merge combines two values src and dst, returning the result.
//	// Implementations may modify dst and return it to avoid an allocation.
//	Merge(src *T, dst *T) *T
//}
//
//// A PartialOverlapAnnotationAggregator is an extension of AnnotationAggregator
//// that allows for custom accumulation of range annotations for tables that only
//// partially overlap with the range.
//type PartialOverlapAnnotationAggregator[T any] interface {
//	AnnotationAggregator[T, *TableMetadata]
//	AccumulatePartialOverlap(f *TableMetadata, dst *T, bounds base.UserKeyBounds) *T
//}
//
//type annotation struct {
//	// annotator is a pointer to the TableAnnotator that computed this annotation.
//	// NB: This is untyped to allow AnnotationAggregator to use Go generics,
//	// since annotations are stored in a slice on each node and a single
//	// slice cannot contain elements with different type parameters.
//	annotator interface{}
//	// v is contains the annotation value, the output of either
//	// AnnotationAggregator.Accumulate or AnnotationAggregator.Merge.
//	// NB: This is untyped for the same reason as annotator above.
//	v atomic.Value
//	// valid indicates whether future reads of the annotation may use the
//	// value as-is. If false, v will be zeroed and recalculated.
//	valid atomic.Bool
//}

//func (a *annotator[T, M]) findExistingAnnotation(n *node[M]) *annotation {
//	n.annotMu.RLock()
//	defer n.annotMu.RUnlock()
//	for i := range n.annot {
//		if n.annot[i].annotator == a {
//			return &n.annot[i]
//		}
//	}
//	return nil
//}
//
//// findAnnotation finds this TableAnnotator's annotation on a node, creating
//// one if it doesn't already exist.
//func (a *annotator[T, M]) findAnnotation(n *node[M]) *annotation {
//	if a := a.findExistingAnnotation(n); a != nil {
//		return a
//	}
//	n.annotMu.Lock()
//	defer n.annotMu.Unlock()
//
//	// This node has never been annotated by a. Create a new annotation.
//	n.annot = append(n.annot, annotation{
//		annotator: a,
//		v:         atomic.Value{},
//	})
//	n.annot[len(n.annot)-1].v.Store(a.Aggregator.Zero())
//	return &n.annot[len(n.annot)-1]
//}

// nodeAnnotation computes this annotator's annotation of this node across all
// files in the node's subtree. The second return value indicates whether the
// annotation is stable and thus cacheable.
func (a *annotator[T, M]) nodeAnnotation(n *node[M]) (t *T, cacheOK bool) {
	// If the annotation already has a cached value, we can return it without
	// recomputing anything.
	if val := n.annot.cachedValues[a.idx].Load(); val != nil {
		return val.(*T), true
	}

	t = new(T)
	cacheOK = true

	// Aggregate all items in this node.
	for i := range n.count {
		v, ok := a.itemFunc(n.items[i])
		a.mergeFunc(t, v)
		cacheOK = cacheOK && ok
	}

	// Aggregate all subtrees.
	if !n.isLeaf() {
		for i := range n.count + 1 {
			v, ok := a.nodeAnnotation(n.child(i))
			a.mergeFunc(t, *v)
			cacheOK = cacheOK && ok
		}
	}

	if cacheOK {
		// It is possible that multiple goroutines do this at the same time; this is
		// ok, as the results should be the same.
		n.annot.cachedValues[a.idx].Store(t)
	}

	return t, cacheOK
}

//// SumAggregator defines an Aggregator which sums together a T value
//// across files.
//type SumAggregator[T any] struct {
//	AddFunc                      func(src, dst *T)
//	AccumulateFunc               func(f *TableMetadata) (v T, cacheOK bool)
//	AccumulatePartialOverlapFunc func(f *TableMetadata, bounds base.UserKeyBounds) T
//}
//
//// Zero implements AnnotationAggregator.Zero, returning a new uint64 set to 0.
//func (sa SumAggregator[T]) Zero() *T {
//	return new(T)
//}
//
//// Accumulate implements AnnotationAggregator.Accumulate, accumulating a single
//// file's uint64 value.
//func (sa SumAggregator[T]) Accumulate(f *TableMetadata, dst *T) (v *T, cacheOK bool) {
//	accumulated, ok := sa.AccumulateFunc(f)
//	sa.AddFunc(&accumulated, dst)
//	return dst, ok
//}
//
//// AccumulatePartialOverlap implements
//// PartialOverlapAnnotationAggregator.AccumulatePartialOverlap, accumulating a
//// single file's uint64 value for a file which only partially overlaps with the
//// range defined by bounds.
//func (sa SumAggregator[T]) AccumulatePartialOverlap(
//	f *TableMetadata, dst *T, bounds base.UserKeyBounds,
//) *T {
//	if sa.AccumulatePartialOverlapFunc == nil {
//		v, _ := sa.Accumulate(f, dst)
//		return v
//	}
//	accumulated := sa.AccumulatePartialOverlapFunc(f, bounds)
//	sa.AddFunc(&accumulated, dst)
//	return dst
//}
//
//// Merge implements AnnotationAggregator.Merge by summing two uint64 values.
//func (sa SumAggregator[T]) Merge(src *T, dst *T) *T {
//	sa.AddFunc(src, dst)
//	return dst
//}
