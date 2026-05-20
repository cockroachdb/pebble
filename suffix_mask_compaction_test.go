// Copyright 2026 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package pebble

import (
	"context"
	"math"
	"testing"

	"github.com/cockroachdb/crlib/testutils/leaktest"
	"github.com/cockroachdb/crlib/testutils/require"
	"github.com/cockroachdb/pebble/cockroachkvs"
	"github.com/cockroachdb/pebble/internal/manifest"
	"github.com/cockroachdb/pebble/internal/testutils"
	"github.com/cockroachdb/pebble/sstable"
	"github.com/cockroachdb/pebble/vfs"
)

// TestDeleteSuffixRangeCancelsOverlappingCompaction pins the fix for the
// race documented at the top of `DB.DeleteSuffixRange` in `suffix_mask.go`:
//
//	A compaction picked against the pre-DSR version holds a *TableMetadata
//	pointer for each input file. When that compaction later applies its
//	version edit, the manifest's blob-reference tracker (keyed by pointer
//	in `internal/manifest/blob_metadata.go`) looks up each deleted table's
//	blob references. If DSR has, in the meantime, replaced the input file
//	with a virtual table — a different *TableMetadata pointer — the
//	tracker no longer has an entry for the original pointer and fires:
//
//	    pebble: deleted table NNNN's reference to blob file BNNN not known
//
//	The fix: inside DSR's `UpdateVersionLocked` updateFn (which holds the
//	manifest log lock), iterate `d.mu.compact.inProgress` and `Cancel()`
//	any compaction whose `Bounds()` overlap the DSR span. The cancelled
//	compaction's own apply step checks `c.cancel` under the same log lock
//	and returns `ErrCancelledCompaction` instead of applying its VE.
//
// Test design:
//   - Insert keys with values large enough to land in a blob file. Flush
//     so the L0 table carries a blob reference.
//   - Anchor an L6 file so the subsequent manual compaction must merge
//     (not trivial-move).
//   - Trigger a manual compaction in a goroutine. Pause it inside the I/O
//     phase via `testingDuringCompactionIOFunc` — the compaction has
//     constructed its `VersionEdit` deleting the L0 input but has not yet
//     re-acquired DB.mu to apply it.
//   - From the test goroutine, call `DeleteSuffixRange` over the same
//     user-key span. With the fix, DSR enters its updateFn, sees the
//     in-progress compaction overlapping its span, and calls
//     `Cancel()`. DSR's VE applies (replacing the L0 input with a
//     virtual table).
//   - Release the compaction. Its apply step observes `c.cancel == true`
//     and returns `ErrCancelledCompaction` gracefully.
//
// On unfixed code (which instead waited for `compactingCount == 0` before
// DSR ran), the test deadlocks: the paused compaction holds
// `compactingCount > 0` while DSR's wait blocks indefinitely, and the
// compaction can't proceed past the hook until DSR returns. The wait was
// the buggy mechanism the fix replaces; removing it requires the cancel
// pattern to prevent the manifest assertion the metamorphic test surfaced.
func TestDeleteSuffixRangeCancelsOverlappingCompaction(t *testing.T) {
	defer leaktest.AfterTest(t)()

	// hookReady fires when the compaction's I/O phase has produced its
	// VersionEdit and is about to re-acquire DB.mu. release tells the
	// hook to return (allowing the compaction's apply step to proceed).
	hookReady := make(chan struct{})
	release := make(chan struct{})

	fs := vfs.NewMem()
	opts := &Options{
		Comparer:                    &cockroachkvs.Comparer,
		FS:                          fs,
		FormatMajorVersion:          FormatNewest,
		KeySchema:                   cockroachkvs.KeySchema.Name,
		KeySchemas:                  sstable.MakeKeySchemas(&cockroachkvs.KeySchema),
		L0CompactionThreshold:       100,
		L0StopWritesThreshold:       100,
		DisableAutomaticCompactions: true,
		Logger:                      testutils.Logger{T: t},
	}
	// Force value separation so the flushed L0 table will carry a blob
	// reference; the manifest's tracker is what surfaces the bug.
	opts.ValueSeparationPolicy = func() ValueSeparationPolicy {
		return ValueSeparationPolicy{
			Enabled:                true,
			MinimumSize:            1,
			MinimumMVCCGarbageSize: 1,
			MaxBlobReferenceDepth:  10,
		}
	}
	// The hook fires from inside the compaction's I/O phase; only the
	// specific compaction we trigger below should pause. Use sync.Once-style
	// gating via the channels themselves (closed-channel detection).
	fired := make(chan struct{}, 1)
	opts.private.testingDuringCompactionIOFunc = func() {
		select {
		case fired <- struct{}{}:
			// First (and only) compaction we want to pause.
			close(hookReady)
			<-release
		default:
			// Subsequent compactions pass straight through.
		}
	}
	db, err := Open("", opts)
	require.NoError(t, err)
	defer func() { require.NoError(t, db.Close()) }()
	ctx := context.Background()

	aStart := testMakeEngineKey([]byte("a"), 0, 0)
	bStart := testMakeEngineKey([]byte("b"), 0, 0)

	// Anchor at L6 with several user keys spanning [a, az] so any L0 file
	// in that range overlaps the L6 anchor in user-key space and forces a
	// merging compaction (not a trivial move). Each anchor entry uses a
	// wall=1 suffix that's outside the mask range used below.
	for _, k := range []string{"a", "ab", "ac", "ad", "ae", "af", "ag"} {
		require.NoError(t, db.Set(testMakeEngineKey([]byte(k), 1, 0),
			[]byte("anchor-value-large-enough-for-separation"), nil))
	}
	require.NoError(t, db.Flush())
	require.NoError(t, db.Compact(ctx, aStart, bStart, false))

	// Flush an L0 table whose user-key range [a, ag] overlaps the L6 anchor
	// (forcing a real merge). The values are large enough to be
	// value-separated into a blob file, so the L0 table carries blob
	// references.
	for i, k := range []string{"a", "ab", "ac", "ad", "ae", "af", "ag"} {
		// Vary the wall time so different keys land at different versions.
		// The wall is > 10 so it falls within the mask range (10, MaxUint64].
		wall := uint64(20 + 10*i)
		require.NoError(t, db.Set(testMakeEngineKey([]byte(k), wall, 0),
			[]byte("value-large-enough-for-blob-separation-aaaaaa"), nil))
	}
	require.NoError(t, db.Flush())

	// Verify the L0 table picked up at least one blob reference.
	var blobRefsBefore int
	for level := 0; level < manifest.NumLevels; level++ {
		for f := range db.DebugCurrentVersion().Levels[level].All() {
			blobRefsBefore += len(f.BlobReferences)
		}
	}
	if blobRefsBefore == 0 {
		t.Fatalf("setup: expected the flushed L0 table to carry at least one blob reference")
	}

	// Trigger a manual compaction; it will pause in
	// `testingDuringCompactionIOFunc` after constructing its VersionEdit.
	compactDone := make(chan error, 1)
	go func() {
		compactDone <- db.Compact(ctx, aStart, bStart, false)
	}()

	// Wait for the compaction to be paused inside the hook.
	<-hookReady

	// While the compaction is paused, run DSR over the same span. With the
	// fix, DSR enters its updateFn (acquiring d.mu since the compaction
	// released it during I/O), finds the in-progress compaction overlapping
	// its span, and cancels it. Without the fix, DSR's pre-updateFn
	// "wait for compactingCount == 0" deadlocks here.
	require.NoError(t, db.DeleteSuffixRange(ctx,
		KeyRange{Start: aStart, End: bStart},
		testMakeSuffix(math.MaxUint64, 0),
		testMakeSuffix(10, 0),
	))

	// Release the paused compaction; it should observe `c.cancel == true`
	// and exit gracefully (Compact retries internally, but with the L0
	// input now replaced by a virtual table, the next compaction attempt
	// either succeeds against the masked file or finds nothing to do).
	close(release)

	// The manual Compact returns nil if any retry succeeded, or an error
	// indicating cancellation. Either is acceptable; the critical property
	// is that no manifest assertion fires.
	require.NoError(t, <-compactDone)
}

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

	// Disable the metamorphic skip bypass so the BPF skip's behavior
	// (and therefore which files get masks) is deterministic here.
	prev := suffixMaskSkipBypassDisabled
	suffixMaskSkipBypassDisabled = true
	defer func() { suffixMaskSkipBypassDisabled = prev }()

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

	// Exactly one file should carry the mask: the L0 file whose key-range
	// intersects the span AND whose wall-range intersects the mask. The L6
	// anchor (wall=1) has no walls in the mask range, so the BPF skip
	// keeps it mask-free.
	var maskedBefore int
	for level := 0; level < manifest.NumLevels; level++ {
		for f := range db.DebugCurrentVersion().Levels[level].All() {
			if len(f.SuffixMasks) > 0 {
				maskedBefore++
			}
		}
	}
	require.Equal(t, 1, maskedBefore)

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
