// Copyright 2020 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package manifest

import (
	"bytes"
	"fmt"
	"io"
	"math"
	"math/rand/v2"
	"os"
	"slices"
	"sort"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/cockroachdb/datadriven"
	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/pebble/internal/base"
	"github.com/cockroachdb/pebble/internal/testkeys"
	"github.com/cockroachdb/pebble/record"
	"github.com/stretchr/testify/require"
)

func readManifest(filename string) (*Version, error) {
	f, err := os.Open(filename)
	if err != nil {
		return nil, err
	}
	defer f.Close()
	rr := record.NewReader(f, 0 /* logNum */)

	l0Organizer := NewL0Organizer(base.DefaultComparer, 10<<20 /* flushSplitBytes */)
	v := NewInitialVersion(base.DefaultComparer)
	addedByFileNum := make(map[base.FileNum]*TableMetadata)
	for {
		r, err := rr.Next()
		if err == io.EOF {
			break
		}
		if err != nil {
			return nil, err
		}
		var ve VersionEdit
		if err = ve.Decode(r); err != nil {
			return nil, err
		}
		var bve BulkVersionEdit
		bve.AllAddedTables = addedByFileNum
		if err := bve.Accumulate(&ve); err != nil {
			return nil, err
		}
		if v, err = bve.Apply(v, 32000); err != nil {
			return nil, err
		}
		l0Organizer.PerformUpdate(l0Organizer.PrepareUpdate(&bve, v), v)
	}
	return v, nil
}

func visualizeSublevels(
	s *l0Sublevels, compactionFiles bitSet, otherLevels [][]*TableMetadata,
) string {
	var buf strings.Builder
	if compactionFiles == nil {
		compactionFiles = newBitSet(s.levelMetadata.Len())
	}
	largestChar := byte('a')
	printLevel := func(files []*TableMetadata, level string, isL0 bool) {
		lastChar := byte('a')
		fmt.Fprintf(&buf, "L%s:", level)
		for i := 0; i < 5-len(level); i++ {
			buf.WriteByte(' ')
		}
		for j, f := range files {
			for lastChar < f.Smallest().UserKey[0] {
				buf.WriteString("   ")
				lastChar++
			}
			buf.WriteByte(f.Smallest().UserKey[0])
			middleChar := byte('-')
			if isL0 {
				if compactionFiles[f.L0Index] {
					middleChar = '+'
				} else if f.IsCompacting() {
					if f.IsIntraL0Compacting {
						middleChar = '^'
					} else {
						middleChar = 'v'
					}
				}
			} else if f.IsCompacting() {
				middleChar = '='
			}
			if largestChar < f.Largest().UserKey[0] {
				largestChar = f.Largest().UserKey[0]
			}
			if f.Smallest().UserKey[0] == f.Largest().UserKey[0] {
				buf.WriteByte(f.Largest().UserKey[0])
				if compactionFiles[f.L0Index] {
					buf.WriteByte('+')
				} else if j < len(files)-1 {
					buf.WriteByte(' ')
				}
				lastChar++
				continue
			}
			buf.WriteByte(middleChar)
			buf.WriteByte(middleChar)
			lastChar++
			for lastChar < f.Largest().UserKey[0] {
				buf.WriteByte(middleChar)
				buf.WriteByte(middleChar)
				buf.WriteByte(middleChar)
				lastChar++
			}
			if f.Largest().IsExclusiveSentinel() &&
				j < len(files)-1 && files[j+1].Smallest().UserKey[0] == f.Largest().UserKey[0] {
				// This case happens where two successive files have
				// matching end/start user keys but where the left-side file
				// has the sentinel key as its end key trailer. In this case
				// we print the sstables as:
				//
				// a------d------g
				//
				continue
			}
			buf.WriteByte(middleChar)
			buf.WriteByte(f.Largest().UserKey[0])
			if j < len(files)-1 {
				buf.WriteByte(' ')
			}
			lastChar++
		}
		fmt.Fprintf(&buf, "\n")
	}
	for i := len(s.levelFiles) - 1; i >= 0; i-- {
		printLevel(s.levelFiles[i], fmt.Sprintf("0.%d", i), true)
	}
	for i := range otherLevels {
		if len(otherLevels[i]) == 0 {
			continue
		}
		printLevel(otherLevels[i], strconv.Itoa(i+1), false)
	}
	buf.WriteString("       ")
	for b := byte('a'); b <= largestChar; b++ {
		buf.WriteByte(b)
		buf.WriteByte(b)
		if b < largestChar {
			buf.WriteByte(' ')
		}
	}
	buf.WriteByte('\n')
	return buf.String()
}

func TestL0Sublevels(t *testing.T) {
	parseMeta := func(s string) (*TableMetadata, error) {
		parts := strings.Split(s, ":")
		if len(parts) != 2 {
			t.Fatalf("malformed table spec: %s", s)
		}
		fileNum, err := strconv.Atoi(strings.TrimSpace(parts[0]))
		if err != nil {
			return nil, err
		}
		fields := strings.Fields(parts[1])
		keyRange := strings.Split(strings.TrimSpace(fields[0]), "-")
		m := (&TableMetadata{}).ExtendPointKeyBounds(
			base.DefaultComparer.Compare,
			base.ParseInternalKey(strings.TrimSpace(keyRange[0])),
			base.ParseInternalKey(strings.TrimSpace(keyRange[1])),
		)
		m.SmallestSeqNum = m.Smallest().SeqNum()
		m.LargestSeqNum = m.Largest().SeqNum()
		if m.Largest().IsExclusiveSentinel() {
			m.LargestSeqNum = m.SmallestSeqNum
		}
		m.LargestSeqNumAbsolute = m.LargestSeqNum
		m.TableNum = base.FileNum(fileNum)
		m.Size = uint64(256)
		m.InitPhysicalBacking()
		if len(fields) > 1 {
			for _, field := range fields[1:] {
				parts := strings.Split(field, "=")
				switch parts[0] {
				case "base_compacting":
					m.IsIntraL0Compacting = false
					m.CompactionState = CompactionStateCompacting
				case "intra_l0_compacting":
					m.IsIntraL0Compacting = true
					m.CompactionState = CompactionStateCompacting
				case "compacting":
					m.CompactionState = CompactionStateCompacting
				case "size":
					sizeInt, err := strconv.Atoi(parts[1])
					if err != nil {
						return nil, err
					}
					m.Size = uint64(sizeInt)
				}
			}
		}

		return m, nil
	}

	var err error
	var fileMetas [NumLevels][]*TableMetadata
	var explicitSublevels [][]*TableMetadata
	var activeCompactions []L0Compaction
	var sublevels *l0Sublevels
	baseLevel := NumLevels - 1

	datadriven.RunTest(t, "testdata/l0_sublevels", func(t *testing.T, td *datadriven.TestData) string {
		pickBaseCompaction := false
		level := 0
		addL0FilesOpt := false
		switch td.Cmd {
		case "add-l0-files":
			addL0FilesOpt = true
			level = 0
			fallthrough
		case "define":
			if !addL0FilesOpt {
				fileMetas = [NumLevels][]*TableMetadata{}
				baseLevel = NumLevels - 1
				activeCompactions = nil
			}
			explicitSublevels = [][]*TableMetadata{}
			sublevel := -1
			addedL0Files := make(map[base.FileNum]*TableMetadata)
			for _, data := range strings.Split(td.Input, "\n") {
				data = strings.TrimSpace(data)
				switch data[:2] {
				case "L0", "L1", "L2", "L3", "L4", "L5", "L6":
					level, err = strconv.Atoi(data[1:2])
					if err != nil {
						return err.Error()
					}
					if level == 0 && len(data) > 3 {
						// Sublevel was specified.
						sublevel, err = strconv.Atoi(data[3:])
						if err != nil {
							return err.Error()
						}
					} else {
						sublevel = -1
					}
				default:
					meta, err := parseMeta(data)
					if err != nil {
						return err.Error()
					}
					if level != 0 && level < baseLevel {
						baseLevel = level
					}
					fileMetas[level] = append(fileMetas[level], meta)
					if level == 0 {
						addedL0Files[meta.TableNum] = meta
					}
					if sublevel != -1 {
						for len(explicitSublevels) <= sublevel {
							explicitSublevels = append(explicitSublevels, []*TableMetadata{})
						}
						explicitSublevels[sublevel] = append(explicitSublevels[sublevel], meta)
					}
				}
			}

			flushSplitMaxBytes := 64
			initialize := true
			for _, arg := range td.CmdArgs {
				switch arg.Key {
				case "flush_split_max_bytes":
					flushSplitMaxBytes, err = strconv.Atoi(arg.Vals[0])
					if err != nil {
						t.Fatal(err)
					}
				case "no_initialize":
					// This case is for use with explicitly-specified sublevels
					// only.
					initialize = false
				}
			}
			sortBySeqNum(fileMetas[0])
			for i := 1; i < NumLevels; i++ {
				SortBySmallest(fileMetas[i], base.DefaultComparer.Compare)
			}

			levelMetadata := MakeLevelMetadata(base.DefaultComparer.Compare, 0, fileMetas[0])
			if initialize {
				if addL0FilesOpt {
					files, ok := sublevels.canUseAddL0Files(addedL0Files, &levelMetadata)
					if !ok {
						err = errors.Newf("pebble: L0 sublevel generation optimization cannot be used")
					} else {
						sublevels = sublevels.addL0Files(files, int64(flushSplitMaxBytes), &levelMetadata)
						// Check if the output matches a full initialization.
						sublevels2, _ := newL0Sublevels(&levelMetadata, base.DefaultComparer.Compare, base.DefaultFormatter, int64(flushSplitMaxBytes))
						require.Equal(t, sublevels.flushSplitUserKeys, sublevels2.flushSplitUserKeys)
						require.Equal(t, sublevels.levelFiles, sublevels2.levelFiles)
					}
				} else {
					sublevels, err = newL0Sublevels(
						&levelMetadata,
						base.DefaultComparer.Compare,
						base.DefaultFormatter,
						int64(flushSplitMaxBytes))
				}
				if err != nil {
					return err.Error()
				}
				sublevels.InitCompactingFileInfo(nil)
			} else {
				// This case is for use with explicitly-specified sublevels
				// only.
				sublevels = &l0Sublevels{
					levelFiles:    explicitSublevels,
					cmp:           base.DefaultComparer.Compare,
					formatKey:     base.DefaultFormatter,
					levelMetadata: levelMetadata,
				}
				for _, files := range explicitSublevels {
					sublevels.Levels = append(sublevels.Levels, NewLevelSliceSpecificOrder(files))
				}
			}

			if err != nil {
				t.Fatal(err)
			}

			var builder strings.Builder
			builder.WriteString(sublevels.describe(true))
			builder.WriteString(visualizeSublevels(sublevels, nil, fileMetas[1:]))
			return builder.String()
		case "pick-base-compaction":
			pickBaseCompaction = true
			fallthrough
		case "pick-intra-l0-compaction":
			minCompactionDepth := 3
			earliestUnflushedSeqNum := base.SeqNum(math.MaxUint64)
			for _, arg := range td.CmdArgs {
				switch arg.Key {
				case "min_depth":
					minCompactionDepth, err = strconv.Atoi(arg.Vals[0])
					if err != nil {
						t.Fatal(err)
					}
				case "earliest_unflushed_seqnum":
					earliestUnflushedSeqNum = base.ParseSeqNum(arg.Vals[0])
				}
			}

			var lcf *L0CompactionFiles
			if pickBaseCompaction {
				baseFiles := NewLevelSliceKeySorted(base.DefaultComparer.Compare, fileMetas[baseLevel])
				lcf = sublevels.PickBaseCompaction(base.DefaultLogger, minCompactionDepth, baseFiles, 0, nil)
				if lcf != nil {
					// Try to extend the base compaction into a more rectangular
					// shape, using the smallest/largest keys of the files before
					// and after overlapping base files. This mimics the logic
					// the compactor is expected to implement.
					baseFiles := fileMetas[baseLevel]
					firstFile := sort.Search(len(baseFiles), func(i int) bool {
						return sublevels.cmp(baseFiles[i].Largest().UserKey, sublevels.orderedIntervals[lcf.minIntervalIndex].startKey.key) >= 0
					})
					lastFile := sort.Search(len(baseFiles), func(i int) bool {
						return sublevels.cmp(baseFiles[i].Smallest().UserKey, sublevels.orderedIntervals[lcf.maxIntervalIndex+1].startKey.key) >= 0
					})
					startKey := base.InvalidInternalKey
					endKey := base.InvalidInternalKey
					if firstFile > 0 {
						startKey = baseFiles[firstFile-1].Largest()
					}
					if lastFile < len(baseFiles) {
						endKey = baseFiles[lastFile].Smallest()
					}
					sublevels.ExtendL0ForBaseCompactionTo(
						startKey,
						endKey,
						lcf)
				}
			} else {
				lcf = sublevels.PickIntraL0Compaction(earliestUnflushedSeqNum, minCompactionDepth, nil /* problemSpans */)
			}
			if lcf == nil {
				return "no compaction picked"
			}
			var builder strings.Builder
			builder.WriteString(fmt.Sprintf("compaction picked with stack depth reduction %d\n", lcf.seedIntervalStackDepthReduction))
			for i, file := range lcf.Files {
				builder.WriteString(file.TableNum.String())
				if i < len(lcf.Files)-1 {
					builder.WriteByte(',')
				}
			}
			startKey := sublevels.orderedIntervals[lcf.seedInterval].startKey
			endKey := sublevels.orderedIntervals[lcf.seedInterval+1].startKey
			builder.WriteString(fmt.Sprintf("\nseed interval: %s-%s\n", startKey.key, endKey.key))
			builder.WriteString(visualizeSublevels(sublevels, lcf.FilesIncluded, fileMetas[1:]))

			return builder.String()
		case "read-amp":
			return strconv.Itoa(sublevels.ReadAmplification())
		case "in-use-key-ranges":
			var buf bytes.Buffer
			for _, data := range strings.Split(strings.TrimSpace(td.Input), "\n") {
				keyRange := strings.Split(strings.TrimSpace(data), "-")
				smallest := []byte(strings.TrimSpace(keyRange[0]))
				largest := []byte(strings.TrimSpace(keyRange[1]))

				keyRanges := sublevels.InUseKeyRanges(smallest, largest)
				for i, r := range keyRanges {
					fmt.Fprintf(&buf, "%s-%s", sublevels.formatKey(r.Start), sublevels.formatKey(r.End.Key))
					if i < len(keyRanges)-1 {
						fmt.Fprint(&buf, ", ")
					}
				}
				if len(keyRanges) == 0 {
					fmt.Fprint(&buf, ".")
				}
				fmt.Fprintln(&buf)
			}
			return buf.String()
		case "flush-split-keys":
			var builder strings.Builder
			builder.WriteString("flush user split keys: ")
			flushSplitKeys := sublevels.FlushSplitKeys()
			for i, key := range flushSplitKeys {
				builder.Write(key)
				if i < len(flushSplitKeys)-1 {
					builder.WriteString(", ")
				}
			}
			if len(flushSplitKeys) == 0 {
				builder.WriteString("none")
			}
			return builder.String()
		case "max-depth-after-ongoing-compactions":
			return strconv.Itoa(sublevels.MaxDepthAfterOngoingCompactions())
		case "l0-check-ordering":
			for sublevel, files := range sublevels.levelFiles {
				slice := NewLevelSliceSpecificOrder(files)
				err := CheckOrdering(base.DefaultComparer, L0Sublevel(sublevel), slice.Iter())
				if err != nil {
					return err.Error()
				}
			}
			return "OK"
		case "update-state-for-compaction":
			var fileNums []base.FileNum
			for _, arg := range td.CmdArgs {
				switch arg.Key {
				case "files":
					for _, val := range arg.Vals {
						fileNum, err := strconv.ParseUint(val, 10, 64)
						if err != nil {
							return err.Error()
						}
						fileNums = append(fileNums, base.FileNum(fileNum))
					}
				}
			}
			files := make([]*TableMetadata, 0, len(fileNums))
			for _, num := range fileNums {
				for _, f := range fileMetas[0] {
					if f.TableNum == num {
						f.CompactionState = CompactionStateCompacting
						files = append(files, f)
						break
					}
				}
			}
			slice := NewLevelSliceSeqSorted(files)
			bounds := KeyRange(base.DefaultComparer.Compare, slice.All())
			activeCompactions = append(activeCompactions, L0Compaction{Bounds: bounds})
			if err := sublevels.UpdateStateForStartedCompaction([]LevelSlice{slice}, true); err != nil {
				return err.Error()
			}
			return "OK"
		case "describe":
			var builder strings.Builder
			builder.WriteString(sublevels.describe(true))
			builder.WriteString(visualizeSublevels(sublevels, nil, fileMetas[1:]))
			return builder.String()
		}
		return fmt.Sprintf("unrecognized command: %s", td.Cmd)
	})
}

// sortBySeqNum sorts the specified files by increasing sequence number.
func sortBySeqNum(files []*TableMetadata) {
	slices.SortFunc(files, func(a, b *TableMetadata) int {
		return a.cmpSeqNum(b)
	})
}

func TestAddL0FilesEquivalence(t *testing.T) {
	seed := uint64(time.Now().UnixNano())
	rng := rand.New(rand.NewPCG(0, seed))
	t.Logf("seed: %d", seed)

	var inUseKeys [][]byte
	const keyReusePct = 0.15
	var fileMetas []*TableMetadata
	var s, s2 *l0Sublevels
	keySpace := testkeys.Alpha(8)

	flushSplitMaxBytes := rng.Int64N(1 << 20)

	// The outer loop runs once for each version edit. The inner loop(s) run
	// once for each file, or each file bound.
	for i := 0; i < 100; i++ {
		filesToAdd := make(map[base.FileNum]*TableMetadata)
		numFiles := 1 + rng.IntN(9)
		keys := make([][]byte, 0, 2*numFiles)
		for j := 0; j < 2*numFiles; j++ {
			if rng.Float64() <= keyReusePct && len(inUseKeys) > 0 {
				keys = append(keys, inUseKeys[rng.IntN(len(inUseKeys))])
			} else {
				newKey := testkeys.Key(keySpace, rng.Int64N(keySpace.Count()))
				inUseKeys = append(inUseKeys, newKey)
				keys = append(keys, newKey)
			}
		}
		slices.SortFunc(keys, bytes.Compare)
		for j := 0; j < numFiles; j++ {
			startKey := keys[j*2]
			endKey := keys[j*2+1]
			if bytes.Equal(startKey, endKey) {
				continue
			}
			meta := (&TableMetadata{
				TableNum:              base.FileNum(i*10 + j + 1),
				Size:                  rng.Uint64N(1 << 20),
				SmallestSeqNum:        base.SeqNum(2*i + 1),
				LargestSeqNum:         base.SeqNum(2*i + 2),
				LargestSeqNumAbsolute: base.SeqNum(2*i + 2),
			}).ExtendPointKeyBounds(
				base.DefaultComparer.Compare,
				base.MakeInternalKey(startKey, base.SeqNum(2*i+1), base.InternalKeyKindSet),
				base.MakeRangeDeleteSentinelKey(endKey),
			)
			meta.InitPhysicalBacking()
			fileMetas = append(fileMetas, meta)
			filesToAdd[meta.TableNum] = meta
		}
		if len(filesToAdd) == 0 {
			continue
		}

		levelMetadata := MakeLevelMetadata(testkeys.Comparer.Compare, 0, fileMetas)
		var err error

		if s2 == nil {
			s2, err = newL0Sublevels(&levelMetadata, testkeys.Comparer.Compare, testkeys.Comparer.FormatKey, flushSplitMaxBytes)
			require.NoError(t, err)
		} else {
			// addL0Files relies on the indices in TableMetadatas pointing to
			// that of the previous L0Sublevels. So it must be called before
			// newL0Sublevels; calling it the other way around results in
			// out-of-bounds panics.
			files, ok := s2.canUseAddL0Files(filesToAdd, &levelMetadata)
			require.True(t, ok)
			s2 = s2.addL0Files(files, flushSplitMaxBytes, &levelMetadata)
		}

		s, err = newL0Sublevels(&levelMetadata, testkeys.Comparer.Compare, testkeys.Comparer.FormatKey, flushSplitMaxBytes)
		require.NoError(t, err)

		// Check for equivalence.
		require.Equal(t, s.flushSplitUserKeys, s2.flushSplitUserKeys)
		require.Equal(t, s.orderedIntervals, s2.orderedIntervals)
		require.Equal(t, s.levelFiles, s2.levelFiles)
	}
}

func BenchmarkManifestApplyWithL0Sublevels(b *testing.B) {
	b.ResetTimer()
	for n := 0; n < b.N; n++ {
		v, err := readManifest("testdata/MANIFEST_import")
		require.NotNil(b, v)
		require.NoError(b, err)
	}
}

func BenchmarkL0SublevelsInit(b *testing.B) {
	v, err := readManifest("testdata/MANIFEST_import")
	if err != nil {
		b.Fatal(err)
	}
	b.ResetTimer()
	for n := 0; n < b.N; n++ {
		sl, err := newL0Sublevels(&v.Levels[0],
			base.DefaultComparer.Compare, base.DefaultFormatter, 5<<20)
		require.NoError(b, err)
		if sl == nil {
			b.Fatal("expected non-nil L0Sublevels to be generated")
		}
	}
}

func BenchmarkL0SublevelsInitAndPick(b *testing.B) {
	v, err := readManifest("testdata/MANIFEST_import")
	if err != nil {
		b.Fatal(err)
	}
	b.ResetTimer()
	for n := 0; n < b.N; n++ {
		sl, err := newL0Sublevels(&v.Levels[0],
			base.DefaultComparer.Compare, base.DefaultFormatter, 5<<20)
		require.NoError(b, err)
		if sl == nil {
			b.Fatal("expected non-nil L0Sublevels to be generated")
		}
		c := sl.PickBaseCompaction(base.DefaultLogger, 2, LevelSlice{}, 0, nil)
		if c == nil {
			b.Fatal("expected non-nil compaction to be generated")
		}
	}
}
