// Copyright 2026 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package pebble

import (
	"fmt"
	"math/rand/v2"
	"slices"
	"testing"

	"github.com/cockroachdb/pebble/internal/base"
	"github.com/cockroachdb/pebble/internal/iterv2"
	"github.com/cockroachdb/pebble/internal/testkeys"
)

// TestMergingIterV2Rand generates random levels and cross-checks the
// mergingIterV2 against the reference FakeIter using forward iteration
// (First + Next).
func TestMergingIterV2Rand(t *testing.T) {
	// To reproduce a failure:
	// runMergingIterV2RandomTest(t, <seed>)
	for range 200 {
		runMergingIterV2RandomTest(t, rand.Uint64())
	}
}

func runMergingIterV2RandomTest(t *testing.T, seed uint64) {
	cmp := testkeys.Comparer
	rng := rand.New(rand.NewPCG(seed, seed))
	cfg := iterv2.RandKeyConfig(rng)
	cfg.MinSeqNum = 1
	cfg.MaxSeqNum = 100
	levels := randMergingTestLevels(rng, cfg, 5, 20, 5)

	// Randomly choose snapshot: sometimes make all keys visible, sometimes
	// use a value within the seqnum range to exercise snapshot filtering.
	snapshot := cfg.MaxSeqNum + 1
	if rng.IntN(3) == 0 {
		snapshot = cfg.MinSeqNum + base.SeqNum(rng.Uint64N(uint64(cfg.MaxSeqNum-cfg.MinSeqNum+1)))
	}

	// TODO(radu): support initial lower/upper bounds.
	iter := newMergingIterV2FromLevels(rng, cmp, levels, snapshot)
	expectedPoints := mergeLevels(cmp, levels, snapshot)

	// On failure, log seed and configuration.
	defer func() {
		if t.Failed() {
			fmt.Printf("seed: %d\n", seed)
			fmt.Printf("snapshot: %d\n", snapshot)
			fmt.Printf("cfg: %+v\n", cfg)
			for levelIdx, l := range levels {
				fmt.Printf("L%d:\n", levelIdx)
				fmt.Printf("  points: %s\n", base.FormatKeys(l.Points))
				fmt.Printf("  spans: %v\n", l.Spans)
			}
		}
	}()

	// The merging iterator is a base.InternalIterator but not an iterv2.Iter;
	// wrap it in an InterleavingIter so we can use the iterv2 testing
	// infrastructure.
	var interleaving iterv2.InterleavingIter
	interleaving.Init(cmp, iter, nil, nil, nil, nil, nil)
	checkCfg := iterv2.CheckIterConfig{
		Comparer:     cmp,
		KeyGenConfig: cfg,
		OpWeights:    iterv2.AllTestOps,
		NumOps:       500,
	}
	expected := iterv2.TestIterData{
		Points: expectedPoints,
	}
	iterv2.CheckIter(t, rng, checkCfg, expected, &interleaving)
}

// randMergingTestLevels generates random merging test levels with seqnum ranges
// partitioned so that level 0 (topmost) gets the highest seqnums.
func randMergingTestLevels(
	rng *rand.Rand, cfg iterv2.KeyGenConfig, maxLevels, maxPointsPerLevel, maxSpansPerLevel int,
) []iterv2.TestIterData {
	numLevels := 1 + rng.IntN(maxLevels)

	// Partition [cfg.MinSeqNum, cfg.MaxSeqNum] into numLevels non-overlapping
	// sub-ranges. Level 0 gets the highest seqnums.
	totalSeqNums := int(cfg.MaxSeqNum - cfg.MinSeqNum + 1)
	if totalSeqNums < numLevels {
		numLevels = totalSeqNums
	}

	// Generate numLevels-1 cut points in [1, totalSeqNums-1] to create
	// numLevels partitions.
	cuts := make([]int, 0, numLevels+1)
	cuts = append(cuts, 0)
	if numLevels > 1 {
		// Pick numLevels-1 distinct cut points.
		cutSet := make(map[int]struct{})
		for len(cutSet) < numLevels-1 {
			c := 1 + rng.IntN(totalSeqNums-1)
			cutSet[c] = struct{}{}
		}
		for c := range cutSet {
			cuts = append(cuts, c)
		}
	}
	cuts = append(cuts, totalSeqNums)
	slices.Sort(cuts)

	levels := make([]iterv2.TestIterData, numLevels)
	for i := range numLevels {
		// Level 0 is the topmost (highest seqnums). Partitions are assigned in
		// reverse order: level 0 gets the last partition.
		partIdx := numLevels - 1 - i
		minSeq := cfg.MinSeqNum + base.SeqNum(cuts[partIdx])
		maxSeq := cfg.MinSeqNum + base.SeqNum(cuts[partIdx+1]) - 1

		lvlCfg := cfg
		lvlCfg.MinSeqNum = minSeq
		lvlCfg.MaxSeqNum = maxSeq

		// Generate random spurious boundaries (0-3 per level).
		numExtra := rng.IntN(4)
		var extraBoundaries [][]byte
		if numExtra > 0 {
			extraBoundaries = make([][]byte, numExtra)
			for j := range numExtra {
				extraBoundaries[j] = cfg.RandKey(rng)
			}
		}

		levels[i] = iterv2.TestIterData{
			Points:          iterv2.RandPointKeys(rng, lvlCfg, maxPointsPerLevel),
			Spans:           iterv2.RandSpans(rng, lvlCfg, maxSpansPerLevel),
			ExtraBoundaries: extraBoundaries,
		}
	}
	return levels
}
