// Copyright 2026 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package pebble

import (
	"context"
	"fmt"
	"math/rand/v2"
	"testing"

	"github.com/cockroachdb/pebble/internal/base"
	"github.com/cockroachdb/pebble/vfs"
	"github.com/stretchr/testify/require"
)

// deletableSumMerger wires base.NewDeletableSumValueMerger (a
// DeletableValueMerger whose merges resolving to zero report delete=true)
// into a *pebble.Merger usable by a DB.
var deletableSumMerger = &Merger{
	Name:  "deletable-sum-merger",
	Merge: base.NewDeletableSumValueMerger,
}

// TestCompactionDeletableMergerStress exercises DeletableValueMerger results
// under compaction with randomized-but-deterministic sequences of
// Set/Merge/Flush/Snapshot/Compact. Open snapshots split key histories into
// snapshot stripes, reaching the compaction iterator's needDelete handling in
// a variety of iterator states.
//
// Regression test: the needDelete path used to elide the merge result via
// `continue` without the skip/position bookkeeping the returned-key path
// performs, tripping the "compaction iterator has skip=true, but iterator is
// at iterPosNext" assertion (a panic on the compaction goroutine).
func TestCompactionDeletableMergerStress(t *testing.T) {
	ctx := context.Background()
	for seed := uint64(0); seed < 100; seed++ {
		t.Run(fmt.Sprintf("seed=%d", seed), func(t *testing.T) {
			rng := rand.New(rand.NewPCG(0, seed))
			db, err := Open("", &Options{
				FS:     vfs.NewMem(),
				Merger: deletableSumMerger,
			})
			require.NoError(t, err)
			var snaps []*Snapshot
			defer func() {
				for _, s := range snaps {
					require.NoError(t, s.Close())
				}
				require.NoError(t, db.Close())
			}()

			keys := [][]byte{[]byte("a"), []byte("b"), []byte("c")}
			for op := 0; op < 400; op++ {
				key := keys[rng.IntN(len(keys))]
				switch rng.IntN(10) {
				case 0, 1:
					require.NoError(t, db.Set(key, []byte("1"), NoSync))
				case 2, 3, 4:
					require.NoError(t, db.Merge(key, []byte("2"), NoSync))
				case 5, 6:
					// Drives merges toward sums of zero, which the deletable
					// merger reports as delete=true.
					require.NoError(t, db.Merge(key, []byte("-2"), NoSync))
				case 7:
					require.NoError(t, db.Flush())
				case 8:
					if len(snaps) < 4 {
						snaps = append(snaps, db.NewSnapshot())
					} else {
						require.NoError(t, snaps[0].Close())
						snaps = snaps[1:]
					}
				case 9:
					require.NoError(t, db.Compact(ctx, []byte("a"), []byte("d"), false))
				}
			}
			for _, s := range snaps {
				require.NoError(t, s.Close())
			}
			snaps = nil
			require.NoError(t, db.Flush())
			require.NoError(t, db.Compact(ctx, []byte("a"), []byte("d"), false))
		})
	}
}
