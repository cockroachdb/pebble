// Copyright 2024 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package manifest

import "sync/atomic"

// annotator aggregates arbitrary values across B-Tree subtrees. It is used to
// lazily compute a value over a B-Tree. Each node of the B-Tree stores a value
// per annotator, containing the result of the computation over the node's
// subtree.
//
// The annotation for an item in the tree can be !cacheOK if some data was not
// available (e.g. relevant table statistics were not collected yet). In this
// case, the value is not cached and it will be recomputed.
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
