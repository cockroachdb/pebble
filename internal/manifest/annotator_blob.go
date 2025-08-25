// Copyright 2025 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package manifest

// A BlobFileAnnotator defines a computation over a version's set of blob files.
type BlobFileAnnotator[T any] struct {
	annotator[T, BlobFileMetadata]
}

func MakeBlobFileAnnotator[T any](
	idx BlobFileAnnotationIdx, funcs BlobFileAnnotatorFuncs[T],
) BlobFileAnnotator[T] {
	return BlobFileAnnotator[T]{
		annotator: annotator[T, BlobFileMetadata]{
			idx:       annotationIdx(idx),
			mergeFunc: funcs.Merge,
			itemFunc:  funcs.BlobFile,
		},
	}
}

// BlobFileAnnotatorFuncs groups the functions that define a table annotator.
type BlobFileAnnotatorFuncs[T any] struct {
	// Merge combines two values src and dst into dst.
	Merge func(dst *T, src T)

	// BlobFile computes the annotation for a blob file in a tree. Returns the
	// value and a bool flag indicating whether the value is stable and okay to
	// cache as an annotation. If the file's value may change over the life of the
	// file, the annotator must return false.
	BlobFile func(BlobFileMetadata) (_ T, cacheOK bool)
}

// Annotation calculates the annotation defined by this BlobFileAnnotator for
// all blob files in the given set.
// A pointer to the TableAnnotator is used as the key for pre-calculated values,
// so the same TableAnnotator must be used to avoid duplicate computation.
func (a *BlobFileAnnotator[T]) Annotation(blobFiles *BlobFileSet) T {
	if blobFiles.tree.Count() == 0 {
		var zero T
		return zero
	}

	v, _ := a.nodeAnnotation(blobFiles.tree.root)
	return *v
}

// BlobFileAnnotationIdx is an index associated with a specific blob file
// annotator. Blob file annotators should have distinct BlobFileAnnotationIdx
// values.
type BlobFileAnnotationIdx int

// NewBlobAnnotationIdx generates a unique AnnotationIdx that can be used with
// blob file annotators. It should be called during global initialization.
func NewBlobAnnotationIdx() BlobFileAnnotationIdx {
	n := nextBlobAnnotationID
	if n >= maxAnnotationsPerNode {
		panic("too many blob annotations")
	}
	nextBlobAnnotationID++
	return n
}

var nextBlobAnnotationID BlobFileAnnotationIdx
