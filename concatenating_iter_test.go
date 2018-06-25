package pebble

import (
	"math/rand"
	"testing"
)

func TestConcatenatingIter(t *testing.T) {
	testIterator(t, newConcatenatingIter, func(r *rand.Rand) [][]string {
		// Partition testKeyValuePairs into one or more splits. Each individual
		// split is in increasing order, and different splits may not overlap
		// in range. Some of the splits may be empty.
		splits, remainder := [][]string{}, testKeyValuePairs
		for r.Intn(4) != 0 {
			i := r.Intn(1 + len(remainder))
			splits = append(splits, remainder[:i])
			remainder = remainder[i:]
		}
		if len(remainder) > 0 {
			splits = append(splits, remainder)
		}
		return splits
	})
}
