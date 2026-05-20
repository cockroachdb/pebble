// Copyright 2026 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

// TestDeleteSuffixRangeOracle is an end-to-end integration test: it
// confirms that the iterator stack, manifest edits, virtual-table
// construction, blob-reference attribution, file-cache invalidation, and
// compaction output all combine to produce the user-visible behavior
// `DeleteSuffixRange` specifies. Lower-level units have their own
// targeted tests (see the index below); this test exists to catch any
// composition bug those isolated tests would miss.
//
// The "oracle" is just a dumb in-memory map used as the specification:
// a DeleteSuffixRange call with span `[lo, hi)` and mask
// `(maskOldest, maskNewest]` means "for every key whose prefix lies in
// `[lo, hi)` and whose wall time lies in `(maskOldest, maskNewest]`,
// mark it deleted." Subsequent Set at a key reinstates it.
//
// The test interleaves random Sets, Flushes, Compactions, and
// DeleteSuffixRange calls, re-scanning the DB and the oracle after every
// few operations (and at the end of each trial) and asserting equality.
// Any divergence indicates a composition bug somewhere in the mask
// machinery.
//
// This file holds the integration test (TestDeleteSuffixRangeOracle), the
// random-input helpers it needs, and most of the focused DSR tests added in
// the same series — input validation, the expandSuffixMask helper, data-
// driven scenarios under testdata/delete_suffix_range, the BPF skip
// optimization, the SyntheticSuffix + empty-suffix-range-key regression,
// and the wire-format round-trip for SuffixMasks on ExternalFile/
// SharedSSTMeta.
//
// A few concerns live in separate files because their content is large or
// stands cleanly on its own:
//
//   - suffix_mask_excise_test.go      Excise / file-splitting / middle-
//                                     table bounds and size; SyntheticSuffix
//                                     excise path.
//   - suffix_mask_compaction_test.go  DSR + compaction interactions
//                                     (cancellation, post-compaction mask
//                                     clearing).
//   - suffix_mask_sst_test.go         SST-direct iteration: range keys
//                                     forward/backward, columnar vs rowblk
//                                     parity, fallback path, no-keys-masked
//                                     optimization.
//   - suffix_mask_testutils_test.go   Shared helpers: key/suffix encoding,
//                                     SST/DB builders, oracle key parsing.

package pebble

import (
	"bytes"
	"context"
	"fmt"
	"math"
	"math/rand"
	"slices"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/cockroachdb/crlib/crstrings"
	"github.com/cockroachdb/crlib/testutils/leaktest"
	"github.com/cockroachdb/crlib/testutils/require"
	"github.com/cockroachdb/datadriven"
	"github.com/cockroachdb/pebble/cockroachkvs"
	"github.com/cockroachdb/pebble/internal/base"
	"github.com/cockroachdb/pebble/internal/invariants"
	"github.com/cockroachdb/pebble/internal/keyspan"
	"github.com/cockroachdb/pebble/internal/manifest"
	"github.com/cockroachdb/pebble/internal/testkeys"
	"github.com/cockroachdb/pebble/objstorage/objstorageprovider"
	"github.com/cockroachdb/pebble/objstorage/remote"
	"github.com/cockroachdb/pebble/sstable"
	"github.com/cockroachdb/pebble/vfs"
)

// TestDeleteSuffixRangeOracle is the headline correctness test for
// DeleteSuffixRange. It interleaves random Sets, Flushes, Compactions, and
// DeleteSuffixRange calls, and after each operation verifies that the DB's
// observable point keys match an in-memory oracle.
//
// The oracle is "dumb": it maintains a map from raw engine key to latest
// value, and on DeleteSuffixRange it marks every matching entry as deleted.
// This is the natural specification of the operation, and any divergence
// between the oracle and the real DB indicates a correctness bug somewhere
// in the mask machinery (manifest edit, virtual table construction, blob
// reference attribution, row-level filter, range-key filter, file-cache
// invalidation, or compaction output).
//
// The test would have caught the IsExclusiveSentinel boundary bug and would
// catch any future regression in iterator visibility under masks.
func TestDeleteSuffixRangeOracle(t *testing.T) {
	defer leaktest.AfterTest(t)()

	seed := int64(time.Now().UnixNano())
	t.Logf("seed: %d", seed)

	// With multi-mask support DSR always succeeds (no disjoint skip branch),
	// so each trial exercises many disjoint mask scenarios that accumulate
	// per file. Increased trials/ops density catches regressions.
	const trials = 12
	const opsPerTrial = 80

	for trial := 0; trial < trials; trial++ {
		t.Run(fmt.Sprintf("trial%d", trial), func(t *testing.T) {
			rng := rand.New(rand.NewSource(seed + int64(trial)))

			db, _ := suffixMaskTestDB(t)
			defer func() { require.NoError(t, db.Close()) }()
			cmp := cockroachkvs.Comparer.Compare

			// Build a random pool of distinct roach prefixes.
			prefixes := func() []string {
				n := 4 + rng.Intn(6)
				seen := map[string]struct{}{}
				for len(seen) < n {
					p := string(rune('a'+rng.Intn(8))) + string(rune('a'+rng.Intn(8)))
					seen[p] = struct{}{}
				}
				out := make([]string, 0, len(seen))
				for p := range seen {
					out = append(out, p)
				}
				slices.Sort(out)
				return out
			}()
			// Bracket past the end of the prefix space so span.End is always
			// > any used prefix and we don't have to special-case the last
			// element.
			endBracket := string(rune('z'))

			// Oracle: map from raw engine key (as string) -> latest value.
			// Empty value means the key is currently deleted (either via a
			// DeleteSuffixRange that matched it, or because we never wrote
			// it). A subsequent Set replaces an empty value with the new
			// value, modelling that a write after a mask is visible.
			type oracleVal struct{ value string }
			oracle := map[string]oracleVal{}

			randSuffix := func() uint64 { return uint64(1 + rng.Intn(100)) }
			randPrefix := func() string { return prefixes[rng.Intn(len(prefixes))] }
			// randSpan returns a [start, end) pair drawn from the prefix pool.
			randSpan := func() ([]byte, []byte) {
				lo := rng.Intn(len(prefixes))
				var hiPrefix string
				if hi := lo + 1 + rng.Intn(len(prefixes)-lo); hi >= len(prefixes) {
					hiPrefix = endBracket
				} else {
					hiPrefix = prefixes[hi]
				}
				return testMakeEngineKey([]byte(prefixes[lo]), 0, 0),
					testMakeEngineKey([]byte(hiPrefix), 0, 0)
			}

			scanDB := func() []string {
				iter, err := db.NewIter(nil)
				require.NoError(t, err)
				defer func() { require.NoError(t, iter.Close()) }()
				var got []string
				for iter.First(); iter.Valid(); iter.Next() {
					got = append(got, string(iter.Value()))
				}
				return got
			}
			expectedScan := func() []string {
				type kv struct {
					key, value []byte
				}
				var kvs []kv
				for k, v := range oracle {
					if v.value == "" {
						continue
					}
					kvs = append(kvs, kv{key: []byte(k), value: []byte(v.value)})
				}
				slices.SortFunc(kvs, func(a, b kv) int { return cmp(a.key, b.key) })
				out := make([]string, len(kvs))
				for i, p := range kvs {
					out[i] = string(p.value)
				}
				return out
			}
			validate := func(after string) {
				t.Helper()
				want := expectedScan()
				got := scanDB()
				if !slices.Equal(got, want) {
					t.Fatalf("scan mismatch %s\n got: %v\nwant: %v", after, got, want)
				}
			}

			for op := 0; op < opsPerTrial; op++ {
				switch r := rng.Intn(10); {
				case r < 5:
					// Set.
					p := randPrefix()
					wall := randSuffix()
					value := fmt.Sprintf("%s@%d#op%d", p, wall, op)
					key := testMakeEngineKey([]byte(p), wall, 0)
					require.NoError(t, db.Set(key, []byte(value), nil))
					oracle[string(key)] = oracleVal{value: value}
				case r == 5:
					require.NoError(t, db.Flush())
				case r == 6:
					// Best-effort compact; the underlying call may error if
					// the range has no data and that's fine here.
					lo, hi := randSpan()
					_ = db.Compact(context.Background(), lo, hi, false)
				default:
					// DeleteSuffixRange. Pick the suffix bounds as two random
					// wall times; the API takes (newest, oldest) in comparer
					// order, which is (larger, smaller) in numeric order.
					a, b := randSuffix(), randSuffix()
					if a == b {
						continue
					}
					maskOldest, maskNewest := a, b
					if maskOldest > maskNewest {
						maskOldest, maskNewest = maskNewest, maskOldest
					}
					lo, hi := randSpan()
					span := KeyRange{Start: lo, End: hi}
					err := db.DeleteSuffixRange(
						context.Background(), span,
						testMakeSuffix(maskNewest, 0),
						testMakeSuffix(maskOldest, 0),
					)
					require.NoError(t, err)
					// Apply to oracle: any visible key whose prefix is in
					// [lo, hi) and whose wall is in (maskOldest, maskNewest]
					// in magnitude becomes deleted.
					for k, v := range oracle {
						if v.value == "" {
							continue
						}
						kb := []byte(k)
						prefix := kb[:cockroachkvs.Split(kb)]
						if cmp(prefix, span.Start) < 0 || cmp(prefix, span.End) >= 0 {
							continue
						}
						wall, _ := parseEngineKeyWallLogical(kb)
						if wall > maskOldest && wall <= maskNewest {
							oracle[k] = oracleVal{} // tombstoned
						}
					}
				}
				// Spot-check every few ops to keep runtime down; full check
				// at the end.
				if op%5 == 4 {
					validate(fmt.Sprintf("after op %d", op))
				}
			}
			validate("final")
		})
	}
}

// TestDeleteSuffixRangeValidation covers the early error/validation paths of
// DeleteSuffixRange: empty bounds, invalid key range, ReadOnly, FMV gating.
func TestDeleteSuffixRangeValidation(t *testing.T) {
	defer leaktest.AfterTest(t)()

	validSpan := KeyRange{
		Start: testMakeEngineKey([]byte("a"), 0, 0),
		End:   testMakeEngineKey([]byte("z"), 0, 0),
	}
	lower := testMakeSuffix(math.MaxUint64, 0)
	upper := testMakeSuffix(100, 0)
	baseOpts := func() *Options {
		return &Options{
			Comparer:           &cockroachkvs.Comparer,
			FormatMajorVersion: FormatSuffixMask,
			FS:                 vfs.NewMem(),
			KeySchema:          cockroachkvs.KeySchema.Name,
			KeySchemas:         sstable.MakeKeySchemas(&cockroachkvs.KeySchema),
		}
	}

	// errSubstr is matched against the returned error's message; "" means
	// any non-nil error is acceptable.
	for _, tc := range []struct {
		name      string
		opts      func() *Options
		span      KeyRange
		lower     []byte
		upper     []byte
		errSubstr string
	}{
		{
			name: "format major version too low",
			opts: func() *Options {
				o := baseOpts()
				o.FormatMajorVersion = FormatColumnarBlocks
				return o
			},
			span: validSpan, lower: lower, upper: upper,
			errSubstr: "format major version",
		},
		{
			name: "empty lower bound",
			opts: baseOpts, span: validSpan, lower: nil, upper: upper,
			errSubstr: "lower bound is empty",
		},
		{
			name: "empty upper bound",
			opts: baseOpts, span: validSpan, lower: lower, upper: nil,
			errSubstr: "upper bound is empty",
		},
		{
			name:  "invalid KeyRange (nil Start)",
			opts:  baseOpts,
			span:  KeyRange{Start: nil, End: testMakeEngineKey([]byte("z"), 0, 0)},
			lower: lower, upper: upper,
			errSubstr: "invalid key range",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			d, err := Open("", tc.opts())
			require.NoError(t, err)
			defer func() { require.NoError(t, d.Close()) }()
			err = d.DeleteSuffixRange(context.Background(), tc.span, tc.lower, tc.upper)
			if err == nil {
				t.Fatalf("expected non-nil error")
			}
			if tc.errSubstr != "" && !strings.Contains(err.Error(), tc.errSubstr) {
				t.Fatalf("error %q does not contain %q", err.Error(), tc.errSubstr)
			}
		})
	}

	t.Run("ReadOnly", func(t *testing.T) {
		// Create then reopen read-only to surface ErrReadOnly.
		fs := vfs.NewMem()
		opts := baseOpts()
		opts.FS = fs
		d, err := Open("", opts)
		require.NoError(t, err)
		require.NoError(t, d.Close())

		ro := baseOpts()
		ro.FS = fs
		ro.ReadOnly = true
		d, err = Open("", ro)
		require.NoError(t, err)
		defer func() { require.NoError(t, d.Close()) }()

		err = d.DeleteSuffixRange(context.Background(), validSpan, lower, upper)
		require.Equal(t, ErrReadOnly, err)
	})
}

func TestExpandSuffixMask(t *testing.T) {
	defer leaktest.AfterTest(t)()

	// expandSuffixMask returns (merged, true) when the union of two suffix
	// masks is itself a single contiguous range, and (zero, false) when it
	// is not. Suffix masks use [Lower, Upper) under the suffix comparator
	// passed in. The test cases below use single-byte synthetic suffixes
	// with a reverse-bytes comparator, simulating cockroachkvs-style MVCC
	// where newer walls (smaller wall_time) sort before older ones.
	reverseBytesCmp := func(a, b []byte) int { return bytes.Compare(b, a) }
	tests := []struct {
		name      string
		a, b      sstable.SuffixMask
		wantLower []byte // ignored if wantOK is false
		wantUpper []byte
		wantOK    bool
	}{
		{
			name:   "disjoint with gap",
			a:      sstable.SuffixMask{Lower: []byte{0x10}, Upper: []byte{0x05}},
			b:      sstable.SuffixMask{Lower: []byte{0x30}, Upper: []byte{0x20}},
			wantOK: false,
		},
		{
			name:      "adjacent (no gap)",
			a:         sstable.SuffixMask{Lower: []byte{0x20}, Upper: []byte{0x10}},
			b:         sstable.SuffixMask{Lower: []byte{0x10}, Upper: []byte{0x05}},
			wantLower: []byte{0x20},
			wantUpper: []byte{0x05},
			wantOK:    true,
		},
		{
			name:      "one range contains the other",
			a:         sstable.SuffixMask{Lower: []byte{0x40}, Upper: []byte{0x01}},
			b:         sstable.SuffixMask{Lower: []byte{0x30}, Upper: []byte{0x10}},
			wantLower: []byte{0x40},
			wantUpper: []byte{0x01},
			wantOK:    true,
		},
		{
			name:      "identical ranges",
			a:         sstable.SuffixMask{Lower: []byte{0x20}, Upper: []byte{0x10}},
			b:         sstable.SuffixMask{Lower: []byte{0x20}, Upper: []byte{0x10}},
			wantLower: []byte{0x20},
			wantUpper: []byte{0x10},
			wantOK:    true,
		},
		{
			name:      "partially overlapping",
			a:         sstable.SuffixMask{Lower: []byte{0x30}, Upper: []byte{0x10}},
			b:         sstable.SuffixMask{Lower: []byte{0x40}, Upper: []byte{0x20}},
			wantLower: []byte{0x40},
			wantUpper: []byte{0x10},
			wantOK:    true,
		},
		{
			name:      "multi-byte suffixes (overlap)",
			a:         sstable.SuffixMask{Lower: []byte{0x00, 0x20}, Upper: []byte{0x00, 0x05}},
			b:         sstable.SuffixMask{Lower: []byte{0x00, 0x30}, Upper: []byte{0x00, 0x10}},
			wantLower: []byte{0x00, 0x30},
			wantUpper: []byte{0x00, 0x05},
			wantOK:    true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Expansion is commutative; verify both orderings.
			for _, args := range []struct{ a, b sstable.SuffixMask }{{tt.a, tt.b}, {tt.b, tt.a}} {
				got, ok := expandSuffixMask(reverseBytesCmp, args.a, args.b)
				if !tt.wantOK {
					if ok {
						t.Fatalf("want ok=false, got merged=%v", got)
					}
					continue
				}
				if !ok {
					t.Fatalf("want ok=true, got ok=false")
				}
				require.Equal(t, tt.wantLower, got.Lower)
				require.Equal(t, tt.wantUpper, got.Upper)
			}
		})
	}

	// Cross-length suffixes (variable-length decimals) where the byte
	// ordering disagrees with the suffix comparator. For testkeys, lower
	// timestamps sort *after* higher ones in suffix order, so the mask
	// `[@9, @5)` covers walls {6,7,8,9} and `[@99, @50)` covers walls
	// {51..99}. The two are disjoint with a gap at walls {10..50}, but
	// byte ordering puts "@50"<"@9" and "@99">"@9", which causes the
	// previous bytes.Compare-based implementation to spuriously merge
	// them into `[@99, @5)` = walls {6..99} (filling the gap).
	t.Run("variable-length-decimal disjoint with gap", func(t *testing.T) {
		a := sstable.SuffixMask{Lower: []byte("@9"), Upper: []byte("@5")}
		b := sstable.SuffixMask{Lower: []byte("@99"), Upper: []byte("@50")}
		suffixCmp := testkeys.Comparer.ComparePointSuffixes
		for _, args := range []struct{ a, b sstable.SuffixMask }{{a, b}, {b, a}} {
			got, ok := expandSuffixMask(suffixCmp, args.a, args.b)
			if ok {
				t.Fatalf("a=%q,%q b=%q,%q want ok=false, got merged=%q,%q",
					args.a.Lower, args.a.Upper, args.b.Lower, args.b.Upper,
					got.Lower, got.Upper)
			}
		}
	})

	// Same scenario but contiguous: `[@9, @5)` and `[@50, @9)` are
	// adjacent (no gap) — together they cover walls {6..49}. Under
	// suffix comparator they should merge to `[@50, @5)`. Under
	// bytes.Compare they were spuriously refused: maxUpper="@9" >
	// minLower="@50" in bytes, so the contiguity check fails.
	t.Run("variable-length-decimal contiguous", func(t *testing.T) {
		a := sstable.SuffixMask{Lower: []byte("@9"), Upper: []byte("@5")}
		b := sstable.SuffixMask{Lower: []byte("@50"), Upper: []byte("@9")}
		suffixCmp := testkeys.Comparer.ComparePointSuffixes
		// Sanity-check via the actual testkeys suffix comparator that the
		// expected merged bounds are well-formed: @50 sorts < @5 in suffix
		// order.
		require.True(t, suffixCmp([]byte("@50"), []byte("@5")) < 0)
		for _, args := range []struct{ a, b sstable.SuffixMask }{{a, b}, {b, a}} {
			got, ok := expandSuffixMask(suffixCmp, args.a, args.b)
			if !ok {
				t.Fatalf("a=%q,%q b=%q,%q want ok=true, got ok=false",
					args.a.Lower, args.a.Upper, args.b.Lower, args.b.Upper)
			}
			require.Equal(t, []byte("@50"), got.Lower)
			require.Equal(t, []byte("@5"), got.Upper)
		}
	})
}

func TestDeleteSuffixRangeDataDriven(t *testing.T) {
	defer leaktest.AfterTest(t)()

	var d *DB
	cleanup := func() {
		if d != nil {
			require.NoError(t, d.Close())
			d = nil
		}
	}
	defer cleanup()

	var enableBlobStorage bool
	openDB := func() {
		cleanup()
		opts := &Options{
			Comparer:                    &cockroachkvs.Comparer,
			FormatMajorVersion:          FormatSuffixMask,
			FS:                          vfs.NewMem(),
			KeySchema:                   cockroachkvs.KeySchema.Name,
			KeySchemas:                  sstable.MakeKeySchemas(&cockroachkvs.KeySchema),
			DisableAutomaticCompactions: true,
		}
		if enableBlobStorage {
			opts.FormatMajorVersion = FormatSuffixMask
			opts.ValueSeparationPolicy = func() ValueSeparationPolicy {
				return ValueSeparationPolicy{
					Enabled:                true,
					MinimumSize:            1,
					MinimumMVCCGarbageSize: 1,
					MaxBlobReferenceDepth:  10,
				}
			}
		}
		var err error
		d, err = Open("", opts)
		require.NoError(t, err)
	}
	openDB()

	// parseMVCCKey parses "roachKey@wallTime" or "roachKey" (bare).
	parseMVCCKey := func(s string) []byte {
		if i := strings.Index(s, "@"); i >= 0 {
			roachKey := s[:i]
			wall, err := strconv.ParseUint(s[i+1:], 10, 64)
			if err != nil {
				panic(fmt.Sprintf("bad wall time in %q: %v", s, err))
			}
			return testMakeEngineKey([]byte(roachKey), wall, 0)
		}
		return testMakeEngineKey([]byte(s), 0, 0)
	}

	parseWallTime := func(s string) uint64 {
		if s == "max" {
			return math.MaxUint64
		}
		v, err := strconv.ParseUint(s, 10, 64)
		if err != nil {
			panic(fmt.Sprintf("bad wall time %q: %v", s, err))
		}
		return v
	}

	datadriven.RunTest(t, "testdata/delete_suffix_range", func(t *testing.T, td *datadriven.TestData) string {
		switch td.Cmd {
		case "reset":
			enableBlobStorage = false
			for _, arg := range td.CmdArgs {
				if arg.Key == "blob-storage" {
					enableBlobStorage = true
				}
			}
			openDB()
			return ""
		case "batch":
			b := d.NewBatch()
			for line := range crstrings.LinesSeq(td.Input) {
				parts := strings.Fields(line)
				if len(parts) == 0 {
					continue
				}
				switch parts[0] {
				case "set":
					if len(parts) != 3 {
						return "set <key> <value>\n"
					}
					require.NoError(t, b.Set(parseMVCCKey(parts[1]), []byte(parts[2]), nil))
				case "range-key-set":
					if len(parts) != 5 {
						return "range-key-set <start> <end> @<wallTime> <value>\n"
					}
					start := testMakeEngineKey([]byte(parts[1]), 0, 0)
					end := testMakeEngineKey([]byte(parts[2]), 0, 0)
					suffix := parts[3]
					if !strings.HasPrefix(suffix, "@") {
						return "range-key-set suffix must start with @\n"
					}
					wall := parseWallTime(suffix[1:])
					require.NoError(t, b.RangeKeySet(start, end, testMakeSuffix(wall, 0), []byte(parts[4]), nil))
				default:
					return fmt.Sprintf("unknown batch op: %s\n", parts[0])
				}
			}
			require.NoError(t, b.Commit(nil))
			return ""
		case "flush":
			require.NoError(t, d.Flush())
			return ""
		case "compact":
			if len(td.CmdArgs) != 2 {
				return "compact <start> <end>\n"
			}
			require.NoError(t, d.Compact(
				context.Background(),
				testMakeEngineKey([]byte(td.CmdArgs[0].String()), 0, 0),
				testMakeEngineKey([]byte(td.CmdArgs[1].String()), 0, 0),
				false,
			))
			return ""
		case "delete-suffix-range":
			parts := strings.Fields(td.CmdArgs[0].String() + " " + td.CmdArgs[1].String())
			start := testMakeEngineKey([]byte(parts[0]), 0, 0)
			end := testMakeEngineKey([]byte(parts[1]), 0, 0)
			var lowerWall, upperWall uint64
			for _, arg := range td.CmdArgs[2:] {
				switch arg.Key {
				case "lower":
					lowerWall = parseWallTime(arg.Vals[0])
				case "upper":
					upperWall = parseWallTime(arg.Vals[0])
				}
			}
			span := KeyRange{Start: start, End: end}
			err := d.DeleteSuffixRange(context.Background(), span, testMakeSuffix(lowerWall, 0), testMakeSuffix(upperWall, 0))
			if err != nil {
				return err.Error()
			}
			return ""
		case "iter":
			iter, err := d.NewIter(nil)
			require.NoError(t, err)
			defer iter.Close()
			var buf bytes.Buffer
			for line := range crstrings.LinesSeq(td.Input) {
				parts := strings.Fields(line)
				if len(parts) == 0 {
					continue
				}
				switch parts[0] {
				case "first":
					iter.First()
				case "next":
					iter.Next()
				case "prev":
					iter.Prev()
				case "last":
					iter.Last()
				case "seek-ge":
					iter.SeekGE(parseMVCCKey(parts[1]))
				case "seek-lt":
					iter.SeekLT(parseMVCCKey(parts[1]))
				}
				if iter.Valid() {
					fmt.Fprintf(&buf, "%s\n", iter.Value())
				} else {
					fmt.Fprintf(&buf, ".\n")
				}
			}
			return buf.String()
		default:
			return fmt.Sprintf("unknown command: %s\n", td.Cmd)
		}
	})
}

// TestDeleteSuffixRangeSyntheticSuffixWithEmptyRangeKey reproduces a bug where
// DeleteSuffixRange's SyntheticSuffix excise shortcut incorrectly dropped
// range-key entries whose original suffix was empty.
//
// Background: a file ingested with SyntheticSuffix has every point key's
// suffix replaced at iteration time. RangeKeySet entries also have their
// suffix replaced — but only if the original suffix is non-empty. A
// RangeKeySet with an empty original suffix retains the empty suffix.
//
// DSR's per-file shortcut checked whether the file's SyntheticSuffix falls in
// the mask range; if so, it excised the file's overlapping portion entirely.
// This was incorrect for files containing RangeKeySet entries with empty
// original suffix: those entries' effective suffix is empty, empty suffixes
// are never masked (per DSR's documented contract), and they should remain
// visible after DSR. The shortcut excised them anyway.
//
// The fix gates the shortcut on `!m.HasRangeKeys`, falling through to the
// per-row mask attachment path for files containing range keys.
func TestDeleteSuffixRangeSyntheticSuffixWithEmptyRangeKey(t *testing.T) {
	defer leaktest.AfterTest(t)()

	remoteStorage := remote.NewInMem()
	opts := &Options{
		Comparer:                    &cockroachkvs.Comparer,
		FormatMajorVersion:          FormatSuffixMask,
		FS:                          vfs.NewMem(),
		KeySchema:                   cockroachkvs.KeySchema.Name,
		KeySchemas:                  sstable.MakeKeySchemas(&cockroachkvs.KeySchema),
		DisableAutomaticCompactions: true,
	}
	opts.RemoteStorage = remote.MakeSimpleFactory(map[remote.Locator]remote.Storage{
		remote.MakeLocator("ext"): remoteStorage,
	})
	d, err := Open("", opts)
	require.NoError(t, err)
	defer d.Close()

	// Write an SST that contains a RangeKeySet with an EMPTY suffix.
	writeOpts := d.opts.MakeWriterOptions(0, d.TableFormat())
	obj, err := remoteStorage.CreateObject("ext1")
	require.NoError(t, err)
	w := sstable.NewWriter(objstorageprovider.NewRemoteWritable(obj), writeOpts)
	// A single point key, plus a range key with empty suffix spanning [a, z).
	require.NoError(t, w.Set(testMakeEngineKey([]byte("c"), 0, 0), []byte("v-c")))
	require.NoError(t, w.Raw().EncodeSpan(keyspan.Span{
		Start: testMakeEngineKey([]byte("a"), 0, 0),
		End:   testMakeEngineKey([]byte("z"), 0, 0),
		Keys: []keyspan.Key{
			{
				Trailer: base.MakeTrailer(0, base.InternalKeyKindRangeKeySet),
				Suffix:  nil,
				Value:   []byte("rk-empty-suffix"),
			},
		},
	}))
	require.NoError(t, w.Close())

	sz, err := remoteStorage.Size("ext1")
	require.NoError(t, err)

	// Ingest with a SyntheticSuffix at wall=50. This file's effective point
	// keys all have suffix @50; range-key entry's original suffix is empty
	// (nil), so its effective suffix stays empty.
	synthSuffix := testMakeSuffix(50, 0)
	_, err = d.IngestExternalFiles(context.Background(), []ExternalFile{{
		Locator:           remote.MakeLocator("ext"),
		ObjName:           "ext1",
		Size:              uint64(sz),
		StartKey:          testMakeEngineKey([]byte("a"), 0, 0),
		EndKey:            testMakeEngineKey([]byte("z"), 0, 0),
		EndKeyIsInclusive: false,
		HasPointKey:       true,
		HasRangeKey:       true,
		SyntheticSuffix:   synthSuffix,
	}})
	require.NoError(t, err)

	// Sanity check: range key with empty suffix is visible before DSR.
	rangeKeyVisible := func() bool {
		it, err := d.NewIter(&IterOptions{
			KeyTypes: IterKeyTypeRangesOnly,
		})
		require.NoError(t, err)
		defer it.Close()
		for valid := it.First(); valid; valid = it.Next() {
			for _, rk := range it.RangeKeys() {
				if bytes.Equal(rk.Suffix, nil) || len(rk.Suffix) == 0 {
					return true
				}
			}
		}
		return false
	}
	require.True(t, rangeKeyVisible())

	// DSR with mask range covering @50 (the synthetic suffix). Per the
	// SyntheticSuffix optimization, the file's point keys are all masked.
	// But the range-key entry's effective suffix is empty, so it must
	// remain visible.
	span := KeyRange{
		Start: testMakeEngineKey([]byte("a"), 0, 0),
		End:   testMakeEngineKey([]byte("z"), 0, 0),
	}
	require.NoError(t, d.DeleteSuffixRange(context.Background(), span,
		testMakeSuffix(math.MaxUint64, 0), // newer-side, inclusive
		testMakeSuffix(10, 0),             // older-side, exclusive (covers @50)
	))

	// After DSR, the range-key entry with empty suffix must still be visible.
	if !rangeKeyVisible() {
		t.Fatal("empty-suffix range-key entry incorrectly hidden by DSR's SyntheticSuffix shortcut")
	}
}

// NOTE: a regression test for the runCopyCompaction SuffixMask propagation
// fix lives in the metamorphic suite — running shared-storage + DSR + Download
// reproduces it. A direct unit test proved tricky to author because the copy-
// compaction is only chosen for specific (external<->local, virtual) file
// configurations that a self-contained test couldn't easily produce in
// isolation.

// TestDeleteSuffixRangeSkipsNonOverlappingFiles verifies the BPF-based
// per-file skip optimization in DeleteSuffixRange. Several tables are
// constructed at disjoint wall-time ranges, and DSR is called with a
// suffix range that matches only the middle band. Files that don't
// overlap the mask range must not have a SuffixMask attached.
//
// The metamorphic bypass (invariants.Sometimes inside DeleteSuffixRange)
// is disabled for this test so the skip behavior is deterministic.
func TestDeleteSuffixRangeSkipsNonOverlappingFiles(t *testing.T) {
	defer leaktest.AfterTest(t)()

	// Disable the metamorphic bypass: we want the skip to be deterministic.
	prev := suffixMaskSkipBypassDisabled
	suffixMaskSkipBypassDisabled = true
	defer func() { suffixMaskSkipBypassDisabled = prev }()

	db, _ := suffixMaskTestDB(t)
	defer func() { require.NoError(t, db.Close()) }()
	ctx := context.Background()

	// Build three disjoint files by writing keys in three distinct wall-time
	// bands and flushing between writes.
	//
	//   file A: keys "a".."c" with walls 10..30
	//   file B: keys "d".."f" with walls 100..130
	//   file C: keys "g".."i" with walls 500..530
	type fileGroup struct {
		prefixes []string
		walls    []uint64
	}
	groups := []fileGroup{
		{prefixes: []string{"a", "b", "c"}, walls: []uint64{10, 20, 30}},
		{prefixes: []string{"d", "e", "f"}, walls: []uint64{100, 110, 130}},
		{prefixes: []string{"g", "h", "i"}, walls: []uint64{500, 520, 530}},
	}
	for i, g := range groups {
		for _, p := range g.prefixes {
			for _, w := range g.walls {
				val := fmt.Sprintf("%s@%d/file%d", p, w, i)
				require.NoError(t, db.Set(testMakeEngineKey([]byte(p), w, 0), []byte(val), nil))
			}
		}
		require.NoError(t, db.Flush())
	}

	// Sanity: three files in L0.
	ver := db.DebugCurrentVersion()
	var total int
	for level := 0; level < manifest.NumLevels; level++ {
		for range ver.Levels[level].All() {
			total++
		}
	}
	require.Equal(t, 3, total)

	// Call DSR with a wall range (200, 400] — i.e. lower=suffix(400),
	// upper=suffix(200). This wall band overlaps no file. Span the full
	// key range so the spatial overlap test would otherwise match every
	// file; the BPF skip is the only thing that can elide them.
	lower := testMakeSuffix(400, 0)
	upper := testMakeSuffix(200, 0)
	spanStart := testMakeEngineKey([]byte("a"), 0, 0)
	spanEnd := testMakeEngineKey([]byte("z"), 0, 0)
	require.NoError(t, db.DeleteSuffixRange(ctx,
		KeyRange{Start: spanStart, End: spanEnd}, lower, upper))

	// No file should have received a mask: every file's wall band lies
	// entirely outside (200, 400].
	ver = db.DebugCurrentVersion()
	var masked, after int
	for level := 0; level < manifest.NumLevels; level++ {
		for f := range ver.Levels[level].All() {
			after++
			if len(f.SuffixMasks) > 0 {
				masked++
			}
		}
	}
	require.Equal(t, 3, after)
	require.Equal(t, 0, masked)

	// Now DSR a band that matches only file B's wall range (90, 140]:
	// lower=suffix(140), upper=suffix(90). Only file B should be
	// masked; files A (walls 10..30) and C (walls 500..530) must be
	// skipped.
	lower = testMakeSuffix(140, 0)
	upper = testMakeSuffix(90, 0)
	require.NoError(t, db.DeleteSuffixRange(ctx,
		KeyRange{Start: spanStart, End: spanEnd}, lower, upper))

	ver = db.DebugCurrentVersion()
	masked = 0
	after = 0
	for level := 0; level < manifest.NumLevels; level++ {
		for f := range ver.Levels[level].All() {
			after++
			if len(f.SuffixMasks) > 0 {
				masked++
			}
		}
	}
	require.Equal(t, 3, after)
	require.Equal(t, 1, masked)
}

// TestDeleteSuffixRangeSkipBypassExercisesPath verifies that with the
// metamorphic bypass enabled (the default in invariants builds), at
// least one file occasionally has a no-op mask attached even when its
// block-property aggregate would normally let it be skipped. This is a
// statistical assertion across many DSR calls; with bypass probability
// 25%, the chance that 100 calls all skip is (1-0.25)^100 ≈ 3*10^-13.
func TestDeleteSuffixRangeSkipBypassExercisesPath(t *testing.T) {
	defer leaktest.AfterTest(t)()
	if !invariants.Enabled {
		t.Skip("metamorphic bypass only fires under invariants/race builds")
	}

	// Reset the bypass to its default (enabled) for this test.
	prev := suffixMaskSkipBypassDisabled
	suffixMaskSkipBypassDisabled = false
	defer func() { suffixMaskSkipBypassDisabled = prev }()

	db, _ := suffixMaskTestDB(t)
	defer func() { require.NoError(t, db.Close()) }()
	ctx := context.Background()

	// One file with walls in [10, 30].
	for _, w := range []uint64{10, 20, 30} {
		require.NoError(t, db.Set(testMakeEngineKey([]byte("a"), w, 0),
			[]byte(fmt.Sprintf("a@%d", w)), nil))
	}
	require.NoError(t, db.Flush())

	// DSR with a wall band that doesn't intersect: lower=suffix(400),
	// upper=suffix(200). The BPF would normally skip. We loop until
	// the bypass triggers and a mask is attached, or give up after
	// many tries (extraordinarily unlikely).
	spanStart := testMakeEngineKey([]byte("a"), 0, 0)
	spanEnd := testMakeEngineKey([]byte("z"), 0, 0)
	lower := testMakeSuffix(400, 0)
	upper := testMakeSuffix(200, 0)

	const maxCalls = 200
	for i := 0; i < maxCalls; i++ {
		require.NoError(t, db.DeleteSuffixRange(ctx,
			KeyRange{Start: spanStart, End: spanEnd}, lower, upper))
		ver := db.DebugCurrentVersion()
		for level := 0; level < manifest.NumLevels; level++ {
			for f := range ver.Levels[level].All() {
				if len(f.SuffixMasks) > 0 {
					// The bypass triggered at least once: the mask was
					// attached even though no key matches. Visible scan
					// should still see all rows.
					iter, err := db.NewIter(nil)
					require.NoError(t, err)
					var visible int
					for iter.First(); iter.Valid(); iter.Next() {
						visible++
					}
					require.NoError(t, iter.Close())
					require.Equal(t, 3, visible)
					return
				}
			}
		}
	}
	t.Fatalf("expected metamorphic bypass to attach a no-op mask within %d calls", maxCalls)
}
