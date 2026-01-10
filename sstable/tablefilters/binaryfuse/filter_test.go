// Copyright 2026 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package binaryfuse

import (
	"math/rand/v2"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestBuildFilter(t *testing.T) {
	for range 20 {
		var sizeRange int
		switch x := rand.IntN(100); {
		case x < 10:
			sizeRange = 100
		case x < 20:
			sizeRange = 1000
		case x < 80:
			sizeRange = 10_000
		case x < 95:
			sizeRange = 100_000
		case x < 99:
			sizeRange = 1_000_000
		default:
			sizeRange = maxSize
		}
		size := 1 + rand.IntN(sizeRange)
		hashes := make([]uint64, size)
		for i := range hashes {
			hashes[i] = rand.Uint64()
		}
		testBuild(t, hashes)
	}
}

func testBuild(t *testing.T, hashes []uint64) {
	var hc hashCollector
	for _, hash := range hashes {
		hc.Add(hash)
	}
	fpVals := []int{4, 8, 12, 16}
	filter, ok := buildFilter(&hc, fpVals[rand.IntN(len(fpVals))])
	require.True(t, ok)
	for range min(len(hashes), 100_000) {
		hash := hashes[rand.IntN(len(hashes))]
		require.True(t, mayContain(filter, hash))
	}
}
