// Copyright 2026 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package pebble

import (
	"context"
	"testing"

	"github.com/cockroachdb/crlib/testutils/leaktest"
	"github.com/cockroachdb/crlib/testutils/require"
	"github.com/cockroachdb/pebble/internal/manifest"
)

// TestSuffixMaskClearedAfterCompaction verifies the one novel
// compaction-time invariant the SuffixMask feature introduces: when a
// compaction physically rewrites a masked file, the output table must
// not carry the mask forward.
//
// The reasoning is correct by construction (the compaction iterator
// applies the mask as it reads the inputs, so the output is post-filter
// data), but if compaction were to ever blindly copy `SuffixMasks` from
// an input to its output we would silently re-mask already-filtered
// keys — a correctness bug that affects exactly the keys outside the
// mask range that happen to share a file with masked ones. This test
// pins the contract.
//
// Trivial-move compactions DO preserve the mask (and should — no
// rewrite happens, so the mask is still needed). To exercise a real
// merge we anchor a file in L6 before flushing the masked L0 file, so
// the subsequent compaction can't trivial-move.
func TestSuffixMaskClearedAfterCompaction(t *testing.T) {
	defer leaktest.AfterTest(t)()

	db, _ := suffixMaskTestDB(t)
	defer func() { require.NoError(t, db.Close()) }()
	ctx := context.Background()

	aStart := testMakeEngineKey([]byte("a"), 0, 0)
	bStart := testMakeEngineKey([]byte("b"), 0, 0)

	// Anchor a single old version (wall=1) at L6. Any subsequent L0 file
	// that shares the user key "a@1" will overlap this anchor in user-key
	// space and force a real merging compaction (not a trivial move).
	require.NoError(t, db.Set(testMakeEngineKey([]byte("a"), 1, 0), []byte("anchor"), nil))
	require.NoError(t, db.Flush())
	require.NoError(t, db.Compact(ctx, aStart, bStart, false))

	// Flush six versions to L0. The wall=1 entry forces user-key overlap
	// with the L6 anchor; the remaining walls populate the file with keys
	// both inside and outside the mask range.
	for _, wall := range []uint64{1, 5, 10, 20, 50, 100} {
		require.NoError(t, db.Set(testMakeEngineKey([]byte("a"), wall, 0), []byte("v"), nil))
	}
	require.NoError(t, db.Flush())

	// Attach a mask covering walls (10, 50]: hides a@20 and a@50.
	require.NoError(t, db.DeleteSuffixRange(ctx,
		KeyRange{Start: aStart, End: bStart},
		testMakeSuffix(50, 0), // lower bound = newest wall to hide
		testMakeSuffix(10, 0), // upper bound = oldest wall to keep visible
	))

	// Sanity: at least one file carries the mask before compaction.
	var maskedBefore int
	for level := 0; level < manifest.NumLevels; level++ {
		for f := range db.DebugCurrentVersion().Levels[level].All() {
			if len(f.SuffixMasks) > 0 {
				maskedBefore++
			}
		}
	}
	require.True(t, maskedBefore >= 1)

	// Compact: must merge the masked L0 file with the L6 anchor.
	require.NoError(t, db.Compact(ctx, aStart, bStart, false))

	// No file in any level may carry a SuffixMask after the rewrite.
	for level := 0; level < manifest.NumLevels; level++ {
		for f := range db.DebugCurrentVersion().Levels[level].All() {
			require.Equal(t, 0, len(f.SuffixMasks))
		}
	}

	// Iteration returns only the unmasked walls plus the anchor.
	// cockroachkvs sorts MVCC keys newest-first: 100, 10, 5, 1.
	iter, err := db.NewIter(nil)
	require.NoError(t, err)
	defer func() { require.NoError(t, iter.Close()) }()
	var got []uint64
	for iter.First(); iter.Valid(); iter.Next() {
		wall, _ := parseEngineKeyWallLogical(iter.Key())
		got = append(got, wall)
	}
	require.Equal(t, []uint64{100, 10, 5, 1}, got)
}
