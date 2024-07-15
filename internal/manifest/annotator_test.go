// Copyright 2024 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package manifest

import (
	"testing"

	"github.com/cockroachdb/pebble/internal/base"
	"github.com/stretchr/testify/require"
)

func makeTestLevelMetadata(count int) (LevelMetadata, []*FileMetadata) {
	files := make([]*FileMetadata, count)
	for i := 0; i < count; i++ {
		files[i] = newItem(key(i))
	}

	lm := MakeLevelMetadata(base.DefaultComparer.Compare, 6, files)
	return lm, files
}

// NumFilesAnnotator is an Annotator which computes an annotation value
// equal to the number of files included in the annotation.
var NumFilesAnnotator = SumAnnotator(func(f *FileMetadata) (uint64, bool) {
	return 1, true
})

func TestNumFilesAnnotator(t *testing.T) {
	const count = 1000
	lm, _ := makeTestLevelMetadata(0)

	for i := 1; i <= count; i++ {
		lm.tree.Insert(newItem(key(i)))
		numFiles := *NumFilesAnnotator.LevelAnnotation(lm)
		require.EqualValues(t, i, numFiles)
	}

	numFiles := *NumFilesAnnotator.LevelAnnotation(lm)
	require.EqualValues(t, count, numFiles)

	numFiles = *NumFilesAnnotator.LevelAnnotation(lm)
	require.EqualValues(t, count, numFiles)

	lm.tree.Delete(newItem(key(count / 2)))
	numFiles = *NumFilesAnnotator.LevelAnnotation(lm)
	require.EqualValues(t, count-1, numFiles)
}

func BenchmarkNumFilesAnnotator(b *testing.B) {
	lm, _ := makeTestLevelMetadata(0)
	for i := 1; i <= b.N; i++ {
		lm.tree.Insert(newItem(key(i)))
		numFiles := *NumFilesAnnotator.LevelAnnotation(lm)
		require.EqualValues(b, uint64(i), numFiles)
	}
}
