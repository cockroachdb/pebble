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

// Creates a version with numFiles files in level 6.
func makeTestVersion(numFiles int) (*Version, []*FileMetadata) {
	files := make([]*FileMetadata, numFiles)
	for i := 0; i < numFiles; i++ {
		// Each file spans 10 keys, e.g. [0->9], [10->19], etc.
		files[i] = (&FileMetadata{}).ExtendPointKeyBounds(
			base.DefaultComparer.Compare, key(i*10), key(i*10+9),
		)
		files[i].InitPhysicalBacking()
	}

	var levelFiles [7][]*FileMetadata
	levelFiles[6] = files

	v := NewVersion(base.DefaultComparer, 0, levelFiles)
	return v, files
}

func TestNumFilesAnnotator(t *testing.T) {
	const count = 1000
	v, _ := makeTestVersion(0)

	for i := 1; i <= count; i++ {
		v.Levels[6].tree.Insert(newItem(key(i)))
		numFiles := *NumFilesAnnotator.LevelAnnotation(v.Levels[6])
		require.EqualValues(t, i, numFiles)
	}
}

func BenchmarkNumFilesAnnotator(b *testing.B) {
	v, _ := makeTestVersion(0)
	for i := 1; i <= b.N; i++ {
		v.Levels[6].tree.Insert(newItem(key(i)))
		numFiles := *NumFilesAnnotator.LevelAnnotation(v.Levels[6])
		require.EqualValues(b, uint64(i), numFiles)
	}
}

func TestPickFileAggregator(t *testing.T) {
	const count = 1000
	a := Annotator[FileMetadata]{
		Aggregator: PickFileAggregator{
			Filter: func(f *FileMetadata) (eligible bool, cacheOK bool) {
				return true, true
			},
			Compare: func(f1 *FileMetadata, f2 *FileMetadata) bool {
				return base.DefaultComparer.Compare(f1.Smallest.UserKey, f2.Smallest.UserKey) < 0
			},
		},
	}

	v, files := makeTestVersion(1)

	for i := 1; i <= count; i++ {
		v.Levels[6].tree.Insert(newItem(key(i)))
		pickedFile := a.LevelAnnotation(v.Levels[6])
		// The picked file should always be the one with the smallest key.
		require.Same(t, files[0], pickedFile)
	}
}

func bounds(i int, j int, exclusive bool) base.UserKeyBounds {
	b := base.UserKeyBoundsEndExclusiveIf(key(i).UserKey, key(j).UserKey, exclusive)
	return b
}

func randomBounds(rng *rand.Rand, count int) base.UserKeyBounds {
	first := rng.Intn(count)
	second := rng.Intn(count)
	exclusive := rng.Intn(2) == 0
	return bounds(min(first, second), max(first, second), exclusive)
}

func requireMatchOverlaps(t *testing.T, v *Version, bounds base.UserKeyBounds) {
	overlaps := v.Overlaps(6, bounds)
	numFiles := *NumFilesAnnotator.LevelRangeAnnotation(v.Levels[6], bounds)
	require.EqualValues(t, overlaps.length, numFiles)
}

func TestNumFilesRangeAnnotationEmptyRanges(t *testing.T) {
	const count = 5_000
	v, files := makeTestVersion(count)

	// Delete files containing key ranges [0, 999] and [24_000, 25_999].
	for i := 0; i < 100; i++ {
		v.Levels[6].tree.Delete(files[i])
	}
	for i := 2400; i < 2600; i++ {
		v.Levels[6].tree.Delete(files[i])
	}

	// Ranges that are completely empty.
	requireMatchOverlaps(t, v, bounds(1, 999, false))
	requireMatchOverlaps(t, v, bounds(0, 1000, true))
	requireMatchOverlaps(t, v, bounds(50_000, 60_000, false))
	requireMatchOverlaps(t, v, bounds(24_500, 25_500, false))
	requireMatchOverlaps(t, v, bounds(24_000, 26_000, true))

	// Partial overlaps with empty ranges.
	requireMatchOverlaps(t, v, bounds(0, 1000, false))
	requireMatchOverlaps(t, v, bounds(20, 1001, true))
	requireMatchOverlaps(t, v, bounds(20, 1010, true))
	requireMatchOverlaps(t, v, bounds(23_000, 27_000, true))
	requireMatchOverlaps(t, v, bounds(25_000, 40_000, false))
	requireMatchOverlaps(t, v, bounds(25_500, 26_001, true))

	// Ranges which only spans a single table.
	requireMatchOverlaps(t, v, bounds(45_000, 45_000, true))
	requireMatchOverlaps(t, v, bounds(30_000, 30_001, true))
	requireMatchOverlaps(t, v, bounds(23_000, 23_000, false))
}

func TestNumFilesRangeAnnotationRandomized(t *testing.T) {
	const count = 10_000
	const numIterations = 10_000

	v, _ := makeTestVersion(count)

	rng := rand.New(rand.NewSource(int64(0)))
	for i := 0; i < numIterations; i++ {
		requireMatchOverlaps(t, v, randomBounds(rng, count*11))
	}
}

func BenchmarkNumFilesRangeAnnotation(b *testing.B) {
	const count = 100_000
	v, files := makeTestVersion(count)

	rng := rand.New(rand.NewSource(int64(0)))
	b.Run("annotator", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			b := randomBounds(rng, count*11)
			// Randomly delete and reinsert a file to verify that range
			// annotations are still fast despite small mutations.
			toDelete := rng.Intn(count)
			v.Levels[6].tree.Delete(files[toDelete])

			NumFilesAnnotator.LevelRangeAnnotation(v.Levels[6], b)

			v.Levels[6].tree.Insert(files[toDelete])
		}
	})

	// Also benchmark an equivalent aggregation using version.Overlaps to show
	// the difference in performance.
	b.Run("overlaps", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			b := randomBounds(rng, count*11)
			toDelete := rng.Intn(count)
			v.Levels[6].tree.Delete(files[toDelete])

			overlaps := v.Overlaps(6, b)
			iter := overlaps.Iter()
			numFiles := 0
			for f := iter.First(); f != nil; f = iter.Next() {
				numFiles++
			}

			v.Levels[6].tree.Insert(files[toDelete])
		}
	})

}
