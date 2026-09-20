// Copyright 2026 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package pebble

import (
	"bytes"
	"context"
	"fmt"
	"math"
	"math/bits"
	"math/rand/v2"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/cockroachdb/crlib/crhumanize"
	"github.com/cockroachdb/crlib/crstrings"
	"github.com/cockroachdb/crlib/testutils/leaktest"
	"github.com/cockroachdb/datadriven"
	"github.com/cockroachdb/pebble/internal/base"
	"github.com/cockroachdb/pebble/internal/manifest"
	"github.com/cockroachdb/pebble/internal/problemspans"
	"github.com/cockroachdb/pebble/internal/testutils"
	"github.com/cockroachdb/pebble/vfs"
	"github.com/stretchr/testify/require"
)

func TestCompactionPickerSmallTables(t *testing.T) {
	defer leaktest.AfterTest(t)()
	var opts *Options
	var picker *compactionPickerByScore
	var inProgressCompactions []compactionInfo
	var problemSpans *problemspans.ByLevel

	datadriven.RunTest(t, "testdata/compaction_picker_small_tables", func(t *testing.T, td *datadriven.TestData) string {
		switch td.Cmd {
		case "define":
			opts = DefaultOptions()
			targetFileSize := int64(1000)
			td.MaybeScanArgs(t, "target-file-size", &targetFileSize)
			for i := range opts.TargetFileSizes {
				opts.TargetFileSizes[i] = targetFileSize
			}
			minRunLength := 4
			td.MaybeScanArgs(t, "min-run-length", &minRunLength)
			opts.SmallTableCompactionMinRunLength = func() int { return minRunLength }
			problemSpans = nil

			fileMetas, baseLevel, compactions, err := parseLevelsAndCompactions(t, opts.Comparer.Compare, td.Input)
			if err != nil {
				return err.Error()
			}
			inProgressCompactions = compactions
			picker = newPickerForTesting(opts, fileMetas, baseLevel, inProgressCompactions)

			var buf bytes.Buffer
			fmt.Fprint(&buf, picker.vers.String())
			if len(inProgressCompactions) > 0 {
				fmt.Fprintln(&buf, "compactions")
				for _, c := range inProgressCompactions {
					fmt.Fprintf(&buf, "  %s\n", c.String())
				}
			}
			return buf.String()

		case "span-policy-boundaries":
			// Each argument is a key at which a new span policy region starts.
			var boundaries [][]byte
			for _, arg := range td.CmdArgs {
				boundaries = append(boundaries, []byte(arg.String()))
			}
			cmp := opts.Comparer.Compare
			opts.Experimental.SpanPolicyFunc = func(bounds UserKeyBounds) (SpanPolicy, error) {
				var policy SpanPolicy
				for _, b := range boundaries {
					if cmp(b, bounds.Start) <= 0 {
						policy.KeyRange.Start = b
					} else {
						policy.KeyRange.End = b
						break
					}
				}
				return policy, nil
			}
			return ""

		case "problem-spans":
			problemSpans = &problemspans.ByLevel{}
			problemSpans.Init(manifest.NumLevels, opts.Comparer.Compare)
			for line := range crstrings.LinesSeq(td.Input) {
				var level int
				var span1, span2 string
				n, err := fmt.Sscanf(line, "L%d %s %s", &level, &span1, &span2)
				if err != nil || n != 3 {
					td.Fatalf(t, "malformed problem span %q", line)
				}
				problemSpans.Add(level, base.ParseUserKeyBounds(span1+" "+span2), time.Hour*10)
			}
			return ""

		case "pick", "pick-auto":
			env := compactionEnv{
				diskAvailBytes:          math.MaxUint64,
				earliestUnflushedSeqNum: math.MaxUint64,
				earliestSnapshotSeqNum:  math.MaxUint64,
				inProgressCompactions:   inProgressCompactions,
				problemSpans:            problemSpans,
			}
			// The picker remembers a fruitless scan for its lifetime (a new picker
			// is created for every version); the test changes span policies and
			// problem spans between picks on the same picker, so reset that memo
			// unless the test case wants to exercise it.
			if !td.HasArg("keep-memo") {
				picker.noSmallTableRunsForK = 0
			}
			if minRunLength := 0; td.MaybeScanArgs(t, "min-run-length", &minRunLength) {
				opts.SmallTableCompactionMinRunLength = func() int { return minRunLength }
			}
			var pc *pickedTableCompaction
			if td.Cmd == "pick" {
				pc = picker.pickSmallTableCompaction(env)
			} else if picked := picker.pickAutoNonScore(env); picked != nil {
				pc = picked.(*pickedTableCompaction)
			}
			if pc == nil {
				if opts.SmallTableCompactionMinRunLength() > 0 && picker.noSmallTableRunsForK == 0 {
					// The scan ran into tables that are compacting or overlap a
					// problem span, so its result was not memoized.
					return "nil (inconclusive)"
				}
				return "nil"
			}
			checkClone(t, pc)
			c := newCompaction(context.TODO(), pc, opts, time.Now(), nil /* provider */, noopGrantHandle{}, noSharedStorage, neverSeparateValues)
			if pc.kind == compactionKindSmallTables && pc.startLevel.level != pc.outputLevel.level {
				// A run with nothing underneath is moved down a level.
				require.Equal(t, compactionKindMove, c.kind)
				require.True(t, pc.outputLevel.files.Empty())
				return fmt.Sprintf("%s L%d -> L%d (%s) %s: %s (%s)", pc.kind, pc.startLevel.level, pc.outputLevel.level,
					c.kind, pc.bounds, tableNums(pc.startLevel.files), crhumanize.Bytes(pc.startLevel.files.AggregateSizeSum()))
			}
			if pc.kind == compactionKindSmallTables {
				// Small-table compactions must produce a single output table.
				require.True(t, c.grandparents.Empty())
				require.Equal(t, uint64(math.MaxUint64), c.maxOutputFileSize)
				require.Equal(t, uint64(math.MaxUint64), c.maxOverlapBytes)
				require.Equal(t, pc.kind, c.kind)
			}
			return fmt.Sprintf("%s L%d %s: %s (%s)", pc.kind, pc.startLevel.level, pc.bounds,
				tableNums(pc.startLevel.files), crhumanize.Bytes(pc.startLevel.files.AggregateSizeSum()))
		}
		return fmt.Sprintf("unrecognized command: %s", td.Cmd)
	})
}

// TestFindSmallTableRun cross-checks findSmallTableRun against a brute-force
// search on random levels: the level is partitioned into regions (maximal runs
// of adjacent tables that lie within the same next-level table or the same gap
// between next-level tables, and within the same span policy region), and
// every window of every region is considered.
func TestFindSmallTableRun(t *testing.T) {
	defer leaktest.AfterTest(t)()
	seed := uint64(time.Now().UnixNano())
	t.Logf("seed: %d", seed)
	rng := rand.New(rand.NewPCG(seed, 0))
	opts := DefaultOptions()
	cmp := opts.Comparer.Compare
	key := func(k int) string { return fmt.Sprintf("%04d", k) }

	// genTables generates n adjacent, non-overlapping tables over integer keys
	// (some with an exclusive end, like a range deletion), with keys spaced by
	// up to gap and spans of up to width.
	genTables := func(n, firstNum, gap, width int, size func() uint64) []*manifest.TableMetadata {
		var tables []*manifest.TableMetadata
		pos := rng.IntN(gap + 1)
		for i := 0; i < n; i++ {
			start := pos + rng.IntN(gap+1)
			var spec string
			if rng.IntN(4) == 0 {
				end := start + 1 + rng.IntN(width)
				spec = fmt.Sprintf("%06d:%s#1,RANGEDEL-%s#inf,RANGEDEL size=%d", firstNum+i, key(start), key(end), size())
				// The next table may start at the exclusive end key.
				pos = end
			} else {
				end := start + rng.IntN(width+1)
				spec = fmt.Sprintf("%06d:%s#1,SET-%s#1,SET size=%d", firstNum+i, key(start), key(end), size())
				pos = end + 1
			}
			m, err := parseTableMetaSpec(cmp, spec)
			require.NoError(t, err)
			tables = append(tables, m)
		}
		return tables
	}

	for iter := 0; iter < 1000; iter++ {
		minRunLength := 3 + rng.IntN(4)
		byteCap := uint64(100 + rng.IntN(1000))
		levelNum := 5
		if rng.IntN(4) == 0 {
			levelNum = 6
		}
		var fileMetas [manifest.NumLevels][]*manifest.TableMetadata
		fileMetas[levelNum] = genTables(1+rng.IntN(60), 1, 2, 3, func() uint64 {
			switch rng.IntN(8) {
			case 0:
				// A table too large for any run.
				return byteCap + uint64(rng.IntN(int(byteCap)))
			case 1, 2:
				return 1 + uint64(rng.IntN(int(byteCap)))
			default:
				return 1 + uint64(rng.IntN(int(byteCap)/minRunLength+1))
			}
		})
		if levelNum < manifest.NumLevels-1 {
			fileMetas[levelNum+1] = genTables(rng.IntN(8), 1000, 6, 40, func() uint64 { return 10000 })
		}
		var boundaries [][]byte
		for i := rng.IntN(5); i > 0; i-- {
			boundaries = append(boundaries, []byte(key(rng.IntN(200))))
		}
		slices.SortFunc(boundaries, cmp)
		boundaries = slices.CompactFunc(boundaries, func(a, b []byte) bool { return cmp(a, b) == 0 })
		spanPolicyFunc := func(bounds UserKeyBounds) (SpanPolicy, error) {
			var policy SpanPolicy
			for _, b := range boundaries {
				if cmp(b, bounds.Start) <= 0 {
					policy.KeyRange.Start = b
				} else {
					policy.KeyRange.End = b
					break
				}
			}
			return policy, nil
		}
		version, _ := newVersionWithLatest(opts, fileMetas)
		describe := func() string {
			var buf strings.Builder
			fmt.Fprintf(&buf, "minRunLength=%d byteCap=%d span-policy-boundaries=%q\n", minRunLength, byteCap, boundaries)
			for l := levelNum; l < manifest.NumLevels; l++ {
				fmt.Fprintf(&buf, "L%d\n", l)
				for _, m := range fileMetas[l] {
					fmt.Fprintf(&buf, "  %s size=%d\n", m.String(), m.Size)
				}
			}
			return buf.String()
		}

		// Brute force: assign each table to a region (or to none, if it
		// straddles a boundary).
		type regionID struct {
			// below is the index of the next-level table containing the table,
			// or -1; gap is the number of next-level tables before the table, or
			// -1; policy is the index of the span policy region.
			below, gap, policy int
			straddles          bool
		}
		tables := fileMetas[levelNum]
		ids := make([]regionID, len(tables))
		for i, m := range tables {
			id := regionID{below: -1, gap: 0}
			b := m.UserKeyBounds()
			if levelNum < manifest.NumLevels-1 {
				for j, below := range fileMetas[levelNum+1] {
					bb := below.UserKeyBounds()
					switch {
					case bb.ContainsBounds(cmp, b):
						id.below, id.gap = j, -1
					case bb.Overlaps(cmp, b):
						id.straddles = true
					case !bb.End.IsUpperBoundFor(cmp, b.Start):
						id.gap++
					}
				}
			}
			for _, boundary := range boundaries {
				if cmp(boundary, b.Start) <= 0 {
					id.policy++
				} else if b.End.CompareUpperBounds(cmp, base.UserKeyExclusive(boundary)) > 0 {
					id.straddles = true
				}
			}
			ids[i] = id
		}
		// Find the best window in each region; the longest wins, then the
		// smallest total size, then the earliest.
		var expected []*manifest.TableMetadata
		var expectedNothingUnderneath bool
		var expectedSize uint64
		for i := 0; i < len(tables); {
			j := i + 1
			for j < len(tables) && ids[j] == ids[i] {
				j++
			}
			if !ids[i].straddles {
				if s, e, size := bruteForceSmallTableRun(tables[i:j], minRunLength, byteCap); s >= 0 &&
					(e-s+1 > len(expected) || (e-s+1 == len(expected) && size < expectedSize)) {
					expected, expectedSize = tables[i+s:i+e+1], size
					expectedNothingUnderneath = ids[i].below < 0
				}
			}
			i = j
		}

		var nextLevel manifest.LevelIterator
		if levelNum < manifest.NumLevels-1 {
			nextLevel = version.Levels[levelNum+1].Iter()
		}
		var helper findSmallTableRunHelper
		got, err := helper.findSmallTableRun(cmp, spanPolicyFunc, version.Levels[levelNum].Iter(), nextLevel,
			levelNum, nil /* problemSpans */, minRunLength, byteCap)
		require.NoError(t, err)
		require.False(t, got.sawUnavailable)
		if expected == nil {
			require.Nil(t, got.tables, describe())
			continue
		}
		require.Equal(t, expected, got.tables, describe())
		require.Equal(t, expectedNothingUnderneath, got.nothingUnderneath, describe())
	}
}

// bruteForceSmallTableRun considers every window of tables and returns the
// best one for a small-table compaction (see findSmallTableRunInRegion),
// preferring the longest, then the smallest total size, then the earliest. It
// returns start = -1 if no window qualifies.
func bruteForceSmallTableRun(
	tables []*manifest.TableMetadata, minRunLength int, byteCap uint64,
) (start, end int, size uint64) {
	start, end = -1, -1
	for s := 0; s < len(tables); s++ {
		var sum, largest uint64
		for e := s; e < len(tables) && e-s+1 <= smallTableCompactionMaxFiles; e++ {
			sum += tables[e].EstimatedDataSize()
			largest = max(largest, tables[e].EstimatedDataSize())
			if sum > byteCap {
				break
			}
			if cnt := e - s + 1; cnt >= minRunLength && 2*largest <= sum &&
				(cnt > end-start+1 || (cnt == end-start+1 && sum < size)) {
				start, end, size = s, e, sum
			}
		}
	}
	return start, end, size
}

// TestFindSmallTableRunInRegion cross-checks findSmallTableRunInRegion against
// a brute-force search over all windows.
func TestFindSmallTableRunInRegion(t *testing.T) {
	defer leaktest.AfterTest(t)()
	seed := uint64(time.Now().UnixNano())
	t.Logf("seed: %d", seed)
	rng := rand.New(rand.NewPCG(seed, 0))

	for iter := 0; iter < 2000; iter++ {
		n := 1 + rng.IntN(2*smallTableCompactionMaxFiles)
		byteCap := uint64(100 + rng.IntN(10000))
		minRunLength := 3 + rng.IntN(5)
		tables := make([]*manifest.TableMetadata, n)
		for i := range tables {
			var size uint64
			switch rng.IntN(10) {
			case 0:
				// Occasionally a table larger than the cap.
				size = byteCap + 1 + uint64(rng.IntN(int(byteCap)))
			case 1, 2:
				size = 1 + uint64(rng.IntN(int(byteCap)))
			default:
				size = 1 + uint64(rng.IntN(int(byteCap)/20+1))
			}
			tables[i] = &manifest.TableMetadata{TableNum: base.TableNum(i + 1), Size: size}
		}
		bestStart, bestEnd, _ := bruteForceSmallTableRun(tables, minRunLength, byteCap)

		var helper findSmallTableRunHelper
		got := helper.findSmallTableRunInRegion(tables, minRunLength, byteCap)
		describe := func() string {
			var sizes []string
			for _, t := range tables {
				sizes = append(sizes, fmt.Sprint(t.Size))
			}
			return fmt.Sprintf("minRunLength=%d byteCap=%d sizes=[%s]", minRunLength, byteCap, strings.Join(sizes, " "))
		}
		if bestStart < 0 {
			require.Nil(t, got, describe())
			continue
		}
		require.NotNil(t, got, describe())
		require.Equal(t, tables[bestStart:bestEnd+1], got, describe())
	}
}

// TestCompactionSmallTables simulates the workload motivating small-table
// compactions: repeated flushes of small amounts of data into key regions that
// don't overlap existing data. Each flushed table is moved intact down to L6,
// where small-table compactions must keep the file count bounded.
func TestCompactionSmallTables(t *testing.T) {
	defer leaktest.AfterTest(t)()
	const numFlushes = 200
	const minRunLength = 4

	// run returns the maximum number of L6 tables observed after any flush (once
	// compactions have quiesced) and the number of small-table compactions.
	run := func(t *testing.T, minRunLength int) (maxFiles int, numSmallTableCompactions int64) {
		opts := &Options{
			FS:                 vfs.NewMem(),
			FormatMajorVersion: FormatNewest,
			Logger:             testutils.Logger{T: t},
			DebugCheck:         DebugCheckLevels,
			// Compact each flushed table as soon as it lands in L0, so that it
			// is moved down to L6 on its own.
			L0CompactionThreshold:            1,
			SmallTableCompactionMinRunLength: func() int { return minRunLength },
		}
		opts.WithFSDefaults()
		d, err := Open("", opts)
		require.NoError(t, err)
		defer func() { require.NoError(t, d.Close()) }()

		for i := 0; i < numFlushes; i++ {
			b := d.NewBatch()
			for j := 0; j < 4; j++ {
				require.NoError(t, b.Set([]byte(fmt.Sprintf("%03d/%d", i, j)), []byte("value"), nil))
			}
			require.NoError(t, b.Commit(Sync))
			require.NoError(t, d.Flush())
			drainCompactions(d)
			m := d.Metrics()
			for l := range m.Levels {
				if l != numLevels-1 {
					require.Zero(t, m.Levels[l].Tables.Count, "L%d", l)
				}
			}
			maxFiles = max(maxFiles, int(m.Levels[numLevels-1].Tables.Count))
			numSmallTableCompactions = m.Compact.SmallTablesCount
		}
		t.Logf("max L6 tables: %d; small-table compactions: %d", maxFiles, numSmallTableCompactions)
		return maxFiles, numSmallTableCompactions
	}

	t.Run("disabled", func(t *testing.T) {
		maxFiles, numCompactions := run(t, 0)
		require.Equal(t, numFlushes, maxFiles)
		require.Zero(t, numCompactions)
	})
	t.Run("enabled", func(t *testing.T) {
		maxFiles, numCompactions := run(t, minRunLength)
		// Consolidation is size-tiered (a run's largest table is at most half of
		// the run), so the tables form tiers of doubling sizes: at any time there
		// are fewer than minRunLength tables in the smallest tier and at most one
		// table per larger tier.
		require.LessOrEqual(t, maxFiles, minRunLength-1+bits.Len(uint(numFlushes)))
		require.Greater(t, numCompactions, int64(0))
	})
}

// BenchmarkPickSmallTableCompaction measures the cost of scanning an LSM for a
// small-table compaction. The LSM is well-formed: L6 has 100K tables, each
// table in L1-L5 spans exactly five tables of the level below, and the size of
// a table is proportional to the number of keys it spans (a full-width table
// has the level's target file size). Then, in each level, ten random tables are
// broken up into a random number (up to maxPieces) of one-key small tables,
// followed by a larger leftover table. With maxPieces below K no run of small
// tables exists, but the small tables defeat the per-level annotator check, so
// the whole LSM is scanned: the worst case for a pick. With larger values, L6
// has qualifying runs (possibly very long ones) and the scan stops there.
func BenchmarkPickSmallTableCompaction(b *testing.B) {
	const numL6Tables = 100_000
	const l6TableWidth = 100_000 // keys
	const baseLevel = 1
	const brokenPerLevel = 10
	key := func(i int) []byte { return []byte(fmt.Sprintf("%012d", i)) }
	for _, k := range []int{3, 4, 5} {
		for _, maxPieces := range []int{2, 10, 1000, 10000} {
			b.Run(fmt.Sprintf("k=%d/max-pieces=%d", k, maxPieces), func(b *testing.B) {
				opts := DefaultOptions()
				opts.SmallTableCompactionMinRunLength = func() int { return k }
				rng := rand.New(rand.NewPCG(uint64(k), uint64(maxPieces)))
				var fileMetas [manifest.NumLevels][]*manifest.TableMetadata
				var tableNum base.TableNum
				// addTable adds a table spanning keys [start, start+width) to a
				// level, with a size proportional to its width.
				addTable := func(level, start, width int, bytesPerKey float64) {
					tableNum++
					m := &manifest.TableMetadata{TableNum: tableNum, Size: uint64(float64(width) * bytesPerKey)}
					m.ExtendPointKeyBounds(opts.Comparer.Compare,
						base.MakeInternalKey(key(start), 1, base.InternalKeyKindSet),
						base.MakeInternalKey(key(start+width-1), 1, base.InternalKeyKindSet))
					m.SeqNums.Low, m.SeqNums.High, m.LargestSeqNumAbsolute = 1, 1, 1
					m.InitPhysicalBacking()
					fileMetas[level] = append(fileMetas[level], m)
				}
				n, tableWidth := numL6Tables, l6TableWidth
				for l := manifest.NumLevels - 1; l >= baseLevel; l-- {
					bytesPerKey := float64(opts.TargetFileSize(l, baseLevel)) / float64(tableWidth)
					broken := make(map[int]bool, brokenPerLevel)
					for len(broken) < brokenPerLevel {
						broken[rng.IntN(n)] = true
					}
					for i := range n {
						start := i * tableWidth
						if !broken[i] {
							addTable(l, start, tableWidth, bytesPerKey)
							continue
						}
						pieces := 1 + rng.IntN(maxPieces)
						for j := range pieces {
							addTable(l, start+j, 1, bytesPerKey)
						}
						addTable(l, start+pieces, tableWidth-pieces, bytesPerKey)
					}
					n /= 5
					tableWidth *= 5
				}
				picker := newPickerForTesting(opts, fileMetas, baseLevel, nil /* inProgressCompactions */)
				env := compactionEnv{
					diskAvailBytes:          math.MaxUint64,
					earliestUnflushedSeqNum: math.MaxUint64,
					earliestSnapshotSeqNum:  math.MaxUint64,
				}
				// Populate the smallest-table-size annotations, which persist
				// across picks (and across versions, for unchanged B-Tree nodes).
				pc := picker.pickSmallTableCompaction(env)
				if (pc != nil) != (maxPieces >= k) {
					b.Fatalf("expected a compaction to be found iff max-pieces >= K; got %v", pc)
				}
				// The scan proceeds bottom-up and stops at the level where the
				// compaction is found.
				scannedTables := 0
				for l := manifest.NumLevels - 1; l >= baseLevel && (pc == nil || l >= pc.startLevel.level); l-- {
					scannedTables += len(fileMetas[l])
				}
				b.ReportAllocs()
				for b.Loop() {
					// The picker remembers a fruitless scan; we want to measure the
					// scan itself.
					picker.noSmallTableRunsForK = 0
					picker.pickSmallTableCompaction(env)
				}
				b.ReportMetric(float64(b.Elapsed().Nanoseconds())/float64(b.N)/float64(scannedTables), "ns/table")
			})
		}
	}
}
