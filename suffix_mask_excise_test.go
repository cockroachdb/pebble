// Copyright 2026 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

// Suffix-mask tests: excise / file-splitting / middle-table machinery. These
// tests cover the interaction between SuffixMask and IngestAndExcise, the
// looseMiddleTableBounds helper that computes bounds for the middle virtual
// table when DeleteSuffixRange splits an existing table, the size estimator
// for that middle table, and the SyntheticSuffix path through exciseOverlap.

package pebble

import (
	"context"
	"fmt"
	"math"
	"testing"

	"github.com/cockroachdb/crlib/testutils/leaktest"
	"github.com/cockroachdb/crlib/testutils/require"
	"github.com/cockroachdb/pebble/cockroachkvs"
	"github.com/cockroachdb/pebble/internal/base"
	"github.com/cockroachdb/pebble/internal/manifest"
)

// TestSuffixMaskExcisePreservation verifies that when a table with a SuffixMask
// is excised (split by IngestAndExcise), the resulting left and right virtual
// tables inherit the SuffixMask, and masked keys remain invisible.
//
// Setup:
//   - Write keys for roach keys "a" through "e" at various wall times.
//   - Flush to produce one SST.
//   - Apply a suffix mask that hides wall times > 100.
//   - Ingest-and-excise an SST covering roach key "c" to "d", which splits
//     the masked table.
//
// After excise the LSM should contain: the left piece [a, c), the ingested
// table [c, d), and the right piece [d, e]. The left and right pieces should
// still carry the SuffixMask. Masked keys (wall > 100) should remain invisible
// through a normal iterator.
func TestSuffixMaskExcisePreservation(t *testing.T) {
	defer leaktest.AfterTest(t)()

	db, fs := suffixMaskTestDB(t)
	defer db.Close()

	// Write keys spanning roach keys a-e at various wall times.
	type kv struct {
		roachKey string
		wall     uint64
		value    string
	}
	keys := []kv{
		{"a", 200, "a@200"},
		{"a", 100, "a@100"},
		{"b", 150, "b@150"},
		{"b", 50, "b@50"},
		{"c", 300, "c@300"},
		{"c", 80, "c@80"},
		{"d", 250, "d@250"},
		{"d", 90, "d@90"},
		{"e", 120, "e@120"},
		{"e", 60, "e@60"},
	}
	for _, k := range keys {
		engineKey := testMakeEngineKey([]byte(k.roachKey), k.wall, 0)
		require.NoError(t, db.Set(engineKey, []byte(k.value), nil))
	}
	require.NoError(t, db.Flush())

	// Apply suffix mask: hide wall times in (100, MaxUint64].
	lower := testMakeSuffix(math.MaxUint64, 0)
	upper := testMakeSuffix(100, 0)
	spanStart := testMakeEngineKey([]byte("a"), 0, 0)
	spanEnd := testMakeEngineKey([]byte("f"), 0, 0)
	require.NoError(t, db.DeleteSuffixRange(
		context.Background(),
		KeyRange{Start: spanStart, End: spanEnd},
		lower, upper,
	))

	t.Logf("LSM before excise:\n%s", db.DebugString())

	// This test focuses on whether `IngestAndExcise` propagates `SuffixMask`
	// to the resulting virtual tables; end-to-end iterator visibility is
	// covered by TestDeleteSuffixRangeOracle.

	// Record which tables have SuffixMasks set before excise.
	ver := db.DebugCurrentVersion()
	var maskedCountBefore int
	for level := 0; level < manifest.NumLevels; level++ {
		for f := range ver.Levels[level].All() {
			if len(f.SuffixMasks) > 0 {
				maskedCountBefore++
			}
		}
	}
	t.Logf("masked tables before excise: %d", maskedCountBefore)
	if maskedCountBefore == 0 {
		t.Fatal("expected at least one masked table before excise")
	}

	// Write an SST to ingest that covers [c, d). This will excise the masked
	// table, splitting it into pieces.
	suffixMaskWriteIngestSST(t, fs, "ingest.sst", []suffixMaskTestEntry{
		{"c", 500, "c@500-ingested"},
	})

	exciseStart := testMakeEngineKey([]byte("c"), 0, 0)
	exciseEnd := testMakeEngineKey([]byte("d"), 0, 0)
	_, err := db.IngestAndExcise(
		context.Background(),
		[]string{"ingest.sst"},
		nil, nil,
		KeyRange{Start: exciseStart, End: exciseEnd},
	)
	require.NoError(t, err)

	t.Logf("LSM after excise:\n%s", db.DebugString())

	// Check that the excised pieces retain the SuffixMasks.
	ver = db.DebugCurrentVersion()
	var maskedCountAfter int
	for level := 0; level < manifest.NumLevels; level++ {
		for f := range ver.Levels[level].All() {
			if len(f.SuffixMasks) > 0 {
				maskedCountAfter++
				require.Equal(t, 1, len(f.SuffixMasks))
				require.Equal(t, lower, f.SuffixMasks[0].Lower)
				require.Equal(t, upper, f.SuffixMasks[0].Upper)
			}
		}
	}
	t.Logf("masked tables after excise: %d", maskedCountAfter)
	// We expect at least two masked pieces (left and right of the excise span).
	// The ingested table should not have a mask.
	if maskedCountAfter < 2 {
		t.Fatalf("expected at least 2 masked tables after excise, got %d", maskedCountAfter)
	}

	// Verify masked keys are still invisible after excise.
	postExcise := suffixMaskCollectVisible(t, db)
	t.Logf("post-excise visible: %v", postExcise)
	for _, val := range postExcise {
		switch val {
		case "a@200", "b@150", "d@250", "e@120":
			t.Errorf("masked key %q should not be visible after excise", val)
		}
	}
	// The ingested key c@500-ingested should be visible (it replaced the
	// excised range).
	found := false
	for _, val := range postExcise {
		if val == "c@500-ingested" {
			found = true
			break
		}
	}
	if !found {
		t.Fatal("expected ingested key c@500-ingested to be visible")
	}
}

// TestSuffixMaskLooseMiddleTableBounds systematically tests looseMiddleTableBounds
// across the cross-product of point key and range key positions relative to the
// span. Each key type can be: absent, fully inside, straddle-left, straddle-right,
// straddle-both, fully-outside-above, or fully-outside-below.
func TestSuffixMaskLooseMiddleTableBounds(t *testing.T) {
	defer leaktest.AfterTest(t)()

	cmp := cockroachkvs.Comparer.Compare
	mk := func(roachKey string) []byte {
		return testMakeEngineKey([]byte(roachKey), 0, 0)
	}

	// Span is always [d, p).
	spanStart, spanEnd := mk("d"), mk("p")

	type bounds struct {
		name       string
		start, end string // empty = absent
	}

	// Positions relative to span [d, p):
	positions := []bounds{
		{"absent", "", ""},
		{"inside", "e", "n"},         // fully inside [d, p)
		{"straddle-left", "a", "h"},  // starts before d, ends inside
		{"straddle-right", "k", "z"}, // starts inside, ends after p
		{"straddle-both", "a", "z"},  // straddles both sides
		{"outside-above", "q", "z"},  // entirely after p
		{"outside-below", "a", "c"},  // entirely before d
		{"boundary-exact", "d", "p"}, // bounds exactly at span start/end
		{"boundary-start", "d", "n"}, // start at span start, end inside
		{"boundary-end", "e", "p"},   // start inside, end at span end
	}

	for _, pt := range positions {
		for _, rk := range positions {
			if pt.name == "absent" && rk.name == "absent" {
				continue
			}
			name := fmt.Sprintf("pt=%s_rk=%s", pt.name, rk.name)
			t.Run(name, func(t *testing.T) {
				original := &manifest.TableMetadata{}
				if pt.name != "absent" {
					original.ExtendPointKeyBounds(cmp,
						base.MakeInternalKey(mk(pt.start), 0, base.InternalKeyKindSet),
						base.MakeInternalKey(mk(pt.end), 0, base.InternalKeyKindSet),
					)
				}
				if rk.name != "absent" {
					original.ExtendRangeKeyBounds(cmp, manifest.AnyRangeKeys,
						base.MakeInternalKey(mk(rk.start), 0, base.InternalKeyKindRangeKeySet),
						base.MakeExclusiveSentinelKey(base.InternalKeyKindRangeKeySet, mk(rk.end)),
					)
				}

				exciseBounds := base.UserKeyBoundsEndExclusive(spanStart, spanEnd)
				middle := &manifest.TableMetadata{}
				looseMiddleTableBounds(cmp, original, middle, exciseBounds)

				// Core invariants that must hold for ALL combinations:
				checkBounds := func(label string, s, l base.InternalKey) {
					if base.InternalCompare(cmp, s, l) >= 0 {
						t.Fatalf("%s bounds inverted: %s >= %s", label, s, l)
					}
					if cmp(s.UserKey, spanStart) < 0 {
						t.Fatalf("%s smallest %s before span start", label, s)
					}
					if cmp(l.UserKey, spanEnd) > 0 {
						t.Fatalf("%s largest %s after span end", label, l)
					}
				}
				if middle.HasPointKeys {
					checkBounds("point", middle.PointKeyBounds.Smallest(), middle.PointKeyBounds.Largest())
				}
				if middle.HasRangeKeys {
					checkBounds("range", middle.RangeKeyBounds.Smallest(), middle.RangeKeyBounds.Largest())
				}

				// Keys fully outside the span should produce no bounds for that type.
				if pt.name == "outside-above" || pt.name == "outside-below" {
					if middle.HasPointKeys {
						t.Fatal("point keys outside span should be absent")
					}
				}
				if rk.name == "outside-above" || rk.name == "outside-below" {
					if middle.HasRangeKeys {
						t.Fatal("range keys outside span should be absent")
					}
				}

				// Keys with any overlap should produce bounds.
				overlapping := []string{"inside", "straddle-left", "straddle-right", "straddle-both",
					"boundary-exact", "boundary-start", "boundary-end"}
				isOverlapping := func(name string) bool {
					for _, n := range overlapping {
						if n == name {
							return true
						}
					}
					return false
				}
				if isOverlapping(pt.name) {
					if !middle.HasPointKeys {
						t.Fatal("overlapping point keys should be present")
					}
				}
				if isOverlapping(rk.name) {
					if !middle.HasRangeKeys {
						t.Fatal("overlapping range keys should be present")
					}
				}

				// When the original largest extends beyond the span end,
				// the clamped largest must be a valid exclusive sentinel.
				// This catches the sentinel-kind bug (fatal A) where
				// MakeExclusiveSentinelKey(SET, ...) wasn't recognized.
				clampedEnd := []string{"straddle-right", "straddle-both", "boundary-exact"}
				isClamped := func(name string) bool {
					for _, n := range clampedEnd {
						if n == name {
							return true
						}
					}
					return false
				}
				if isClamped(pt.name) && middle.HasPointKeys {
					if !middle.PointKeyBounds.Largest().IsExclusiveSentinel() {
						t.Fatalf("clamped point largest should be exclusive sentinel, got %s",
							middle.PointKeyBounds.Largest())
					}
				}
				if isClamped(rk.name) && middle.HasRangeKeys {
					if !middle.RangeKeyBounds.Largest().IsExclusiveSentinel() {
						t.Fatalf("clamped range largest should be exclusive sentinel, got %s",
							middle.RangeKeyBounds.Largest())
					}
				}

			})
		}
	}
}

// TestSuffixMaskSmallestBoundIsSafe verifies that the smallest bound
// synthesized by looseMiddleTableBounds (for the middle virtual table) and
// looseRightTableBounds (for the right virtual table after excise) is large
// enough in InternalCompare order that no real key in the file can violate
// it.
//
// This is a regression test for a bug where the synthesized smallest was
// `(userKey, 0, InternalKeyKindMaxForSSTable)` — a tiny trailer. A real
// range tombstone at the same user key with any non-zero seqnum has a much
// larger trailer, and therefore sorts BEFORE the synthesized bound in
// internal-key order, violating the lower-bound check in
// `keyspan.AssertBounds` (gated by invariants.Sometimes in
// `file_cache.newRangeDelIter`). The fix synthesizes with seqnum
// `SeqNumMax-1` (the largest non-sentinel seqnum); this test pins that
// choice rather than re-checking it via the randomly-gated assertion path.
//
// The bug was originally surfaced by the metamorphic test wiring (commit 8)
// with DSR weight > 0. This focused test makes a future regression
// immediately apparent without needing a metamorphic stress run.
func TestSuffixMaskSmallestBoundIsSafe(t *testing.T) {
	defer leaktest.AfterTest(t)()

	cmp := cockroachkvs.Comparer.Compare
	startKey := testMakeEngineKey([]byte("a"), 0, 0)
	endKey := testMakeEngineKey([]byte("z"), 0, 0)
	bounds := KeyRange{Start: startKey, End: endKey}.UserKeyBounds()

	// An original table whose smallest point key has a small trailer
	// (seqnum=0, kind=DeleteSized) — the exact shape that triggered the bug.
	original := &manifest.TableMetadata{}
	original.ExtendPointKeyBounds(cmp,
		base.MakeInternalKey(startKey, 0, base.InternalKeyKindDeleteSized),
		base.MakeInternalKey(endKey, 0, base.InternalKeyKindSet),
	)
	original.ExtendRangeKeyBounds(cmp, manifest.AnyRangeKeys,
		base.MakeInternalKey(startKey, 0, base.InternalKeyKindRangeKeySet),
		base.MakeInternalKey(endKey, 0, base.InternalKeyKindRangeKeySet),
	)

	t.Run("looseMiddleTableBounds", func(t *testing.T) {
		var middle manifest.TableMetadata
		looseMiddleTableBounds(cmp, original, &middle, bounds)

		// Any real internal key at startKey has trailer <= ((SeqNumMax-1)<<8)|kind.
		// The synthesized bound's trailer must be >= that.
		gotPoint := middle.PointKeyBounds.Smallest()
		if got := gotPoint.SeqNum(); got != base.SeqNumMax-1 {
			t.Fatalf("point smallest seqnum: got %d, want %d", got, base.SeqNumMax-1)
		}
		gotRange := middle.RangeKeyBounds.Smallest()
		if got := gotRange.SeqNum(); got != base.SeqNumMax-1 {
			t.Fatalf("range smallest seqnum: got %d, want %d", got, base.SeqNumMax-1)
		}
	})

	t.Run("looseRightTableBounds", func(t *testing.T) {
		var right manifest.TableMetadata
		looseRightTableBounds(cmp, original, &right, startKey)

		gotPoint := right.PointKeyBounds.Smallest()
		if got := gotPoint.SeqNum(); got != base.SeqNumMax-1 {
			t.Fatalf("point smallest seqnum: got %d, want %d", got, base.SeqNumMax-1)
		}
		gotRange := right.RangeKeyBounds.Smallest()
		if got := gotRange.SeqNum(); got != base.SeqNumMax-1 {
			t.Fatalf("range smallest seqnum: got %d, want %d", got, base.SeqNumMax-1)
		}
	})
}

// TestSuffixMaskMiddleTableSize verifies the size computation for the middle
// table doesn't underflow when left + right size estimates exceed the original.
func TestSuffixMaskMiddleTableSize(t *testing.T) {
	defer leaktest.AfterTest(t)()

	type testCase struct {
		name                string
		originalSize        uint64
		leftSize, rightSize uint64
		expectMiddleSize    uint64
	}
	for _, tc := range []testCase{
		{"normal", 1000, 300, 400, 300},
		{"exact", 1000, 500, 500, 1},     // left+right == original → clamp to 1
		{"overshoot", 1000, 600, 500, 1}, // left+right > original → clamp to 1
		{"left-only", 1000, 400, 0, 600},
		{"right-only", 1000, 0, 700, 300},
		{"both-zero", 1000, 0, 0, 1000},
		{"tiny-original", 1, 1, 1, 1},    // overshoot with tiny file
		{"loose-bounds", 101, 51, 51, 1}, // (101+1)/2 = 51 each → overshoot
	} {
		t.Run(tc.name, func(t *testing.T) {
			require.Equal(t, tc.expectMiddleSize,
				middleTableSize(tc.originalSize, tc.leftSize+tc.rightSize))
		})
	}
}

// TestLooseExciseTableBoundsSkipDisjointKeyType is a regression test for a bug
// in `looseLeftTableBounds` / `looseRightTableBounds`: when the original
// table's range keys (or point keys) sit entirely inside the excise span, the
// helpers were still calling `ExtendRangeKeyBounds` / `ExtendPointKeyBounds`
// on the left / right virtual table with a clamped largest (or smallest) that
// sorts before (or after) the original's preserved smallest (or largest).
// That produced a table with `smallest > largest` and tripped
// `TableMetadata.Validate`'s inconsistent-bounds check, surfaced by the
// metamorphic test as `file NNN has inconsistent range key bounds`.
//
// The fix gates each key type's contribution on whether keys of that type
// actually extend past the excise boundary in the relevant direction. This
// test pins both the left and right helpers across point keys and range keys
// using the cases that triggered the metamorphic failure (range keys / point
// keys entirely inside the excise span).
func TestLooseExciseTableBoundsSkipDisjointKeyType(t *testing.T) {
	defer leaktest.AfterTest(t)()

	cmp := base.DefaultComparer.Compare
	// Original table: range keys at [c, d), point keys at [c, d].
	original := &manifest.TableMetadata{}
	original.ExtendPointKeyBounds(cmp,
		base.MakeInternalKey([]byte("c"), 1, base.InternalKeyKindSet),
		base.MakeInternalKey([]byte("d"), 1, base.InternalKeyKindSet),
	)
	original.ExtendRangeKeyBounds(cmp, manifest.AnyRangeKeys,
		base.MakeInternalKey([]byte("c"), 2, base.InternalKeyKindRangeKeySet),
		base.MakeExclusiveSentinelKey(base.InternalKeyKindRangeKeySet, []byte("d")),
	)

	// looseLeftTableBounds with exciseSpanStart="b": both point and range
	// keys start at or after "b", so neither extends to the left. The left
	// table must have no point keys and no range keys.
	t.Run("looseLeftTableBounds/disjoint", func(t *testing.T) {
		var left manifest.TableMetadata
		looseLeftTableBounds(cmp, original, &left, []byte("b"))
		if left.HasPointKeys {
			t.Errorf("left table should not have point keys; got %s..%s",
				left.PointKeyBounds.Smallest(), left.PointKeyBounds.Largest())
		}
		if left.HasRangeKeys {
			t.Errorf("left table should not have range keys; got %s..%s",
				left.RangeKeyBounds.Smallest(), left.RangeKeyBounds.Largest())
		}
	})

	// looseRightTableBounds with exciseSpanEnd="e": both point and range
	// keys end at or before "e", so neither extends to the right. The right
	// table must have no point keys and no range keys.
	t.Run("looseRightTableBounds/disjoint", func(t *testing.T) {
		var right manifest.TableMetadata
		looseRightTableBounds(cmp, original, &right, []byte("e"))
		if right.HasPointKeys {
			t.Errorf("right table should not have point keys; got %s..%s",
				right.PointKeyBounds.Smallest(), right.PointKeyBounds.Largest())
		}
		if right.HasRangeKeys {
			t.Errorf("right table should not have range keys; got %s..%s",
				right.RangeKeyBounds.Smallest(), right.RangeKeyBounds.Largest())
		}
	})

	// Where one key type extends and the other does not, only the extending
	// type should populate the resulting table. Use a fresh original where
	// point keys extend left of "b" but range keys do not.
	t.Run("looseLeftTableBounds/mixed", func(t *testing.T) {
		mixed := &manifest.TableMetadata{}
		mixed.ExtendPointKeyBounds(cmp,
			base.MakeInternalKey([]byte("a"), 1, base.InternalKeyKindSet),
			base.MakeInternalKey([]byte("d"), 1, base.InternalKeyKindSet),
		)
		mixed.ExtendRangeKeyBounds(cmp, manifest.AnyRangeKeys,
			base.MakeInternalKey([]byte("c"), 2, base.InternalKeyKindRangeKeySet),
			base.MakeExclusiveSentinelKey(base.InternalKeyKindRangeKeySet, []byte("d")),
		)
		var left manifest.TableMetadata
		looseLeftTableBounds(cmp, mixed, &left, []byte("b"))
		if !left.HasPointKeys {
			t.Error("left table should have point keys (point keys extend left of b)")
		}
		if left.HasRangeKeys {
			t.Errorf("left table should not have range keys; got %s..%s",
				left.RangeKeyBounds.Smallest(), left.RangeKeyBounds.Largest())
		}
		// The point bounds, if present, must be well-ordered (smallest
		// <= largest). The TableBacking is not set here so we can't run
		// the full Validate; assert the bounds directly.
		if left.HasPointKeys && base.InternalCompare(cmp,
			left.PointKeyBounds.Smallest(), left.PointKeyBounds.Largest()) > 0 {
			t.Fatalf("left table has inconsistent point bounds: %s vs %s",
				left.PointKeyBounds.Smallest(), left.PointKeyBounds.Largest())
		}
	})
	t.Run("looseRightTableBounds/mixed", func(t *testing.T) {
		mixed := &manifest.TableMetadata{}
		mixed.ExtendPointKeyBounds(cmp,
			base.MakeInternalKey([]byte("c"), 1, base.InternalKeyKindSet),
			base.MakeInternalKey([]byte("h"), 1, base.InternalKeyKindSet),
		)
		mixed.ExtendRangeKeyBounds(cmp, manifest.AnyRangeKeys,
			base.MakeInternalKey([]byte("c"), 2, base.InternalKeyKindRangeKeySet),
			base.MakeExclusiveSentinelKey(base.InternalKeyKindRangeKeySet, []byte("d")),
		)
		var right manifest.TableMetadata
		looseRightTableBounds(cmp, mixed, &right, []byte("e"))
		if !right.HasPointKeys {
			t.Error("right table should have point keys (point keys extend right of e)")
		}
		if right.HasRangeKeys {
			t.Errorf("right table should not have range keys; got %s..%s",
				right.RangeKeyBounds.Smallest(), right.RangeKeyBounds.Largest())
		}
		if right.HasPointKeys && base.InternalCompare(cmp,
			right.PointKeyBounds.Smallest(), right.PointKeyBounds.Largest()) > 0 {
			t.Fatalf("right table has inconsistent point bounds: %s vs %s",
				right.PointKeyBounds.Smallest(), right.PointKeyBounds.Largest())
		}
	})
}

// TestDetermineExcisedTableBlobReferencesCapped is a regression test for a
// bug in `determineExcisedTableBlobReferences`: the scaling factor
// `excisedTable.Size / originalSize` can exceed 1 when size estimates diverge
// (e.g. the excised virtual is sized by `fc.estimateSize` while the original
// virtual's size was clamped to `1` by `determineExcisedTableSize` on a zero
// estimate, or loose-bound halving causes drift across repeated splits).
// Without a cap, the scaled `ValueSize` could exceed the physical blob file's
// `ValueSize`, tripping `MakeBlobReference`'s invariant check on manifest
// replay (`blob reference value size N > blob file's value size M`).
//
// The fix caps the per-reference scaled `ValueSize` at the original blob
// reference's `ValueSize` (excised is a subset of the original, so its share
// cannot exceed the original's). This test pins the cap with the smallest
// reproducer of the bug — an original blob reference of `ValueSize=14` and
// scaling ratios that, naïvely, produce values larger than `14`.
func TestDetermineExcisedTableBlobReferencesCapped(t *testing.T) {
	defer leaktest.AfterTest(t)()

	type testCase struct {
		name          string
		originalRefVS uint64
		originalSize  uint64
		excisedSize   uint64
		expectExcisVS uint64
	}
	for _, tc := range []testCase{
		// Estimate noise: excisedSize > originalSize. Without the cap the
		// scaled value size would exceed the physical blob file's value
		// size and trip MakeBlobReference's invariant. With the cap it is
		// clamped to the original ref's value size (the upper bound of any
		// subset's share).
		{"excised-bigger", 14, 10, 100, 14},
		// Pathological: the originalSize was clamped to 1 by a prior excise
		// (e.g. determineExcisedTableSize's zero-estimate guard), so any
		// non-trivial excisedSize multiplies the value size by orders of
		// magnitude. The cap keeps it at the original ref's value size.
		{"original-clamped-to-one", 14, 1, 50, 14},
		// Normal subset: excisedSize < originalSize, scale down.
		{"subset-half", 100, 1000, 500, 50},
		// Subset that scales to zero: clamp to 1.
		{"subset-tiny", 100, 1000, 1, 1},
	} {
		t.Run(tc.name, func(t *testing.T) {
			originalRefs := manifest.BlobReferences{{
				FileID:    base.BlobFileID(1),
				ValueSize: tc.originalRefVS,
			}}
			excised := &manifest.TableMetadata{Size: tc.excisedSize}
			determineExcisedTableBlobReferences(originalRefs, tc.originalSize, excised, FormatSuffixMask)
			if len(excised.BlobReferences) != 1 {
				t.Fatalf("expected 1 blob reference, got %d", len(excised.BlobReferences))
			}
			got := excised.BlobReferences[0].ValueSize
			if got != tc.expectExcisVS {
				t.Errorf("ValueSize: got %d, want %d", got, tc.expectExcisVS)
			}
			if got > tc.originalRefVS {
				t.Errorf("scaled ValueSize %d exceeds original ref ValueSize %d "+
					"(would trip MakeBlobReference invariant on manifest replay)",
					got, tc.originalRefVS)
			}
		})
	}
}
