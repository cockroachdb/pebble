// Copyright 2019 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package metamorphic

import (
	"fmt"
	"math/rand/v2"
	"testing"
	"time"

	"github.com/cockroachdb/pebble/internal/randvar"
	"github.com/pmezard/go-difflib/difflib"
	"github.com/stretchr/testify/require"
)

func TestGenerator(t *testing.T) {
	rng := randvar.NewRand()
	g := newGenerator(rng, DefaultOpConfig(), newKeyManager(1 /* numInstances */))

	g.newBatch()
	g.newBatch()
	g.newBatch()
	require.EqualValues(t, 3, len(g.liveBatches))
	require.EqualValues(t, 0, len(g.batches))

	g.newIndexedBatch()
	g.newIndexedBatch()
	g.newIndexedBatch()
	require.EqualValues(t, 6, len(g.liveBatches))
	require.EqualValues(t, 3, len(g.batches))
	require.EqualValues(t, 3, len(g.readers))

	g.newIter()
	g.newIter()
	g.newIter()
	require.EqualValues(t, 3, len(g.liveIters))

	g.batchAbort()
	g.batchAbort()
	g.batchAbort()
	g.batchAbort()
	g.batchAbort()
	g.batchAbort()

	require.EqualValues(t, 0, len(g.liveBatches))
	require.EqualValues(t, 0, len(g.batches))
	require.EqualValues(t, 0, len(g.iters))
	require.EqualValues(t, 1, len(g.liveReaders))
	require.EqualValues(t, 0, len(g.readers))
	require.EqualValues(t, 0, len(g.liveSnapshots))
	require.EqualValues(t, 0, len(g.snapshots))
	require.EqualValues(t, 1, len(g.liveWriters))

	g.randIter(g.iterClose)()
	g.randIter(g.iterClose)()
	g.randIter(g.iterClose)()
	require.EqualValues(t, 0, len(g.liveIters))

	if testing.Verbose() {
		t.Logf("\n%s", g)
	}

	g = newGenerator(rng, DefaultOpConfig(), newKeyManager(1 /* numInstances */))

	g.newSnapshot()
	g.newSnapshot()
	g.newSnapshot()
	require.EqualValues(t, 3, len(g.liveSnapshots))
	require.EqualValues(t, 3, len(g.snapshots))

	g.newIter()
	g.newIter()
	g.newIter()
	g.snapshotClose()
	g.snapshotClose()
	g.snapshotClose()

	require.EqualValues(t, 0, len(g.liveBatches))
	require.EqualValues(t, 0, len(g.batches))
	require.EqualValues(t, 0, len(g.iters))
	require.EqualValues(t, 1, len(g.liveReaders))
	require.EqualValues(t, 0, len(g.readers))
	require.EqualValues(t, 0, len(g.liveSnapshots))
	require.EqualValues(t, 0, len(g.snapshots))
	require.EqualValues(t, 1, len(g.liveWriters))

	g.randIter(g.iterClose)()
	g.randIter(g.iterClose)()
	g.randIter(g.iterClose)()
	require.EqualValues(t, 0, len(g.liveIters))

	if testing.Verbose() {
		t.Logf("\n%s", g)
	}

	g = newGenerator(rng, DefaultOpConfig(), newKeyManager(1 /* numInstances */))

	g.newIndexedBatch()
	g.newIndexedBatch()
	g.newIndexedBatch()
	g.newIter()
	g.newIter()
	g.newIter()
	g.writerApply()
	g.writerApply()
	g.writerApply()
	g.randIter(g.iterClose)()
	g.randIter(g.iterClose)()
	g.randIter(g.iterClose)()

	require.EqualValues(t, 0, len(g.liveBatches))
	require.EqualValues(t, 0, len(g.batches))
	require.EqualValues(t, 0, len(g.liveIters))
	require.EqualValues(t, 0, len(g.iters))
	require.EqualValues(t, 1, len(g.liveReaders))
	require.EqualValues(t, 0, len(g.readers))
	require.EqualValues(t, 0, len(g.liveSnapshots))
	require.EqualValues(t, 0, len(g.snapshots))
	require.EqualValues(t, 1, len(g.liveWriters))

	if testing.Verbose() {
		t.Logf("\n%s", g)
	}
}

func TestGeneratorRandom(t *testing.T) {
	seed := uint64(time.Now().UnixNano())
	ops := randvar.NewUniform(1000, 10000)
	cfgs := []string{"default", "multiInstance"}
	generateFromSeed := func(cfg OpConfig) string {
		rng := rand.New(rand.NewPCG(0, seed))
		count := ops.Uint64(rng)
		return formatOps(generate(rng, count, cfg, newKeyManager(cfg.numInstances)))
	}

	for i := range cfgs {
		t.Run(fmt.Sprintf("config=%s", cfgs[i]), func(t *testing.T) {
			cfg := DefaultOpConfig
			if cfgs[i] == "multiInstance" {
				cfg = func() OpConfig {
					cfg := multiInstanceConfig()
					cfg.numInstances = 2
					return cfg
				}
			}
			// Ensure that generate doesn't use any other source of randomness other
			// than rng.
			referenceOps := generateFromSeed(cfg())
			for i := 0; i < 10; i++ {
				regeneratedOps := generateFromSeed(cfg())
				diff, err := difflib.GetUnifiedDiffString(difflib.UnifiedDiff{
					A:       difflib.SplitLines(referenceOps),
					B:       difflib.SplitLines(regeneratedOps),
					Context: 1,
				})
				require.NoError(t, err)
				if len(diff) > 0 {
					t.Fatalf("Diff:\n%s", diff)
				}
			}
			if testing.Verbose() {
				t.Logf("\nOps:\n%s", referenceOps)
			}
		})
	}
}

func TestGenerateDisjointKeyRanges(t *testing.T) {
	rng := randvar.NewRand()
	g := newGenerator(rng, DefaultOpConfig(), newKeyManager(1 /* numInstances */))

	for i := 0; i < 10; i++ {
		keyRanges := g.generateDisjointKeyRanges(5)
		for j := range keyRanges {
			t.Logf("%d: %d: %s", i, j, keyRanges[j])
			for k := range keyRanges {
				if j == k {
					continue
				}
				if g.cmp(keyRanges[j].End, keyRanges[k].Start) <= 0 {
					require.Less(t, j, k)
					continue
				}
				if g.cmp(keyRanges[j].Start, keyRanges[k].End) >= 0 {
					require.Greater(t, j, k)
					continue
				}
				t.Fatalf("generated key ranges %q and %q overlap", keyRanges[j], keyRanges[k])
			}
		}
	}
}
