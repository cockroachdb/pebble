// Copyright 2024 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package manifest

import (
	"math/rand"
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

func TestNumFilesAnnotator(t *testing.T) {
	const count = 1000
	lm, _ := makeTestLevelMetadata(0)

	for i := 1; i <= count; i++ {
		lm.tree.Insert(newItem(key(i)))
		numFiles := NumFilesAnnotator.LevelAnnotation(lm)
		require.EqualValues(t, i, numFiles)
	}

	numFiles := NumFilesAnnotator.LevelAnnotation(lm)
	require.EqualValues(t, count, numFiles)

	numFiles = NumFilesAnnotator.LevelAnnotation(lm)
	require.EqualValues(t, count, numFiles)
}

func BenchmarkNumFilesAnnotator(b *testing.B) {
	lm, _ := makeTestLevelMetadata(0)
	for i := 1; i <= b.N; i++ {
		lm.tree.Insert(newItem(key(i)))
		numFiles := NumFilesAnnotator.LevelAnnotation(lm)
		require.EqualValues(b, i, numFiles)
	}
}

func TestNumFilesRangeAnnotationEmptyRanges(t *testing.T) {
	lm, files := makeTestLevelMetadata(5_000)

	// Delete key ranges in the beginning and middle.
	for i := 0; i < 100; i++ {
		lm.tree.Delete(files[i])
	}
	for i := 2400; i < 2600; i++ {
		lm.tree.Delete(files[i])
	}

	// Ranges that are completely empty.
	v := NumFilesAnnotator.LevelRangeAnnotation(lm, key(1), key(99))
	require.EqualValues(t, 0, v)
	v = NumFilesAnnotator.LevelRangeAnnotation(lm, key(5001), key(6000))
	require.EqualValues(t, 0, v)
	v = NumFilesAnnotator.LevelRangeAnnotation(lm, key(2450), key(2550))
	require.EqualValues(t, 0, v)

	// Partial overlaps with empty ranges.
	v = NumFilesAnnotator.LevelRangeAnnotation(lm, key(0), key(100))
	require.EqualValues(t, 1, v)
	v = NumFilesAnnotator.LevelRangeAnnotation(lm, key(2300), key(2700))
	require.EqualValues(t, 201, v)
	v = NumFilesAnnotator.LevelRangeAnnotation(lm, key(2500), key(4000))
	require.EqualValues(t, 1401, v)

	// Range which only spans a single table.
	v = NumFilesAnnotator.LevelRangeAnnotation(lm, key(2300), key(2300))
	require.EqualValues(t, 1, v)
}

func TestNumFilesRangeAnnotationRandomized(t *testing.T) {
	const count = 10_000
	const numIterations = 100_000
	lm, _ := makeTestLevelMetadata(count)

	rng := rand.New(rand.NewSource(int64(0)))
	for i := 0; i < numIterations; i++ {
		left := rng.Intn(count)
		right := left + rng.Intn(count-left)

		v := NumFilesAnnotator.LevelRangeAnnotation(lm, key(left), key(right))

		// There are right - left + 1 files overlapping the range
		// [left, right] inclusive.
		require.EqualValues(t, right-left+1, v)
	}
}

func BenchmarkNumFilesRangeAnnotation(b *testing.B) {
	const count = 10_000
	lm, files := makeTestLevelMetadata(count)

	rng := rand.New(rand.NewSource(int64(0)))
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		left := rng.Intn(count)
		right := left + rng.Intn(count-left)

		// Randomly delete and reinsert a key within the range to verify
		// that range annotations are still fast despite small mutations.
		toDelete := rng.Intn(count)
		lm.tree.Delete(files[toDelete])
		NumFilesAnnotator.LevelRangeAnnotation(lm, key(left), key(right))
		lm.tree.Insert(files[toDelete])
	}
}
