// Copyright 2018 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package pebble

import (
	"bytes"
	"cmp"
	"fmt"
	"maps"
	"math"
	"slices"
	"sort"
	"strconv"
	"strings"
	"sync"
	"testing"
	"text/tabwriter"
	"time"

	"github.com/cockroachdb/crlib/crstrings"
	"github.com/cockroachdb/datadriven"
	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/pebble/internal/base"
	"github.com/cockroachdb/pebble/internal/humanize"
	"github.com/cockroachdb/pebble/internal/manifest"
	"github.com/cockroachdb/pebble/internal/problemspans"
	"github.com/cockroachdb/pebble/internal/testkeys"
	"github.com/cockroachdb/pebble/internal/testutils"
	"github.com/cockroachdb/pebble/sstable"
	"github.com/cockroachdb/pebble/vfs"
	"github.com/stretchr/testify/require"
)

func loadVersion(
	t *testing.T, d *datadriven.TestData,
) (*manifest.Version, *latestVersionState, *Options, string) {
	var sizes [numLevels]int64
	opts := &Options{}
	opts.randomizeForTesting(t)
	opts.EnsureDefaults()
	d.ScanArgs(t, "l-base-max-bytes", &opts.LBaseMaxBytes)
	var files [numLevels][]*manifest.TableMetadata
	if len(d.Input) > 0 {
		// Parse each line as
		//
		// <level>: <size> [compensation]
		//
		// Creating sstables within the level whose file sizes total to `size`
		// and whose compensated file sizes total to `size`+`compensation`. If
		// size is sufficiently large, only one single file is created. See
		// the TODO below.
		for _, data := range strings.Split(d.Input, "\n") {
			parts := strings.Split(data, " ")
			parts[0] = strings.TrimSuffix(strings.TrimSpace(parts[0]), ":")
			if len(parts) < 2 {
				return nil, nil, nil, fmt.Sprintf("malformed test:\n%s", d.Input)
			}
			level, err := strconv.Atoi(parts[0])
			if err != nil {
				return nil, nil, nil, err.Error()
			}
			if files[level] != nil {
				return nil, nil, nil, fmt.Sprintf("level %d already filled", level)
			}
			size, err := strconv.ParseUint(strings.TrimSpace(parts[1]), 10, 64)
			if err != nil {
				return nil, nil, nil, err.Error()
			}
			var compensation uint64
			if len(parts) == 3 {
				compensation, err = strconv.ParseUint(strings.TrimSpace(parts[2]), 10, 64)
				if err != nil {
					return nil, nil, nil, err.Error()
				}
			}

			for i := uint64(1); sizes[level] < int64(size); i++ {
				var key InternalKey
				if level == 0 {
					// For L0, make `size` overlapping files.
					key = base.MakeInternalKey([]byte(fmt.Sprintf("%04d", 1)), base.SeqNum(i), InternalKeyKindSet)
				} else {
					key = base.MakeInternalKey([]byte(fmt.Sprintf("%04d", i)), base.SeqNum(i), InternalKeyKindSet)
				}
				m := &manifest.TableMetadata{
					TableNum:              base.TableNum(uint64(level)*100_000 + i),
					SmallestSeqNum:        key.SeqNum(),
					LargestSeqNum:         key.SeqNum(),
					LargestSeqNumAbsolute: key.SeqNum(),
					Size:                  1,
				}
				m.ExtendPointKeyBounds(opts.Comparer.Compare, key, key)
				m.InitPhysicalBacking()
				if size >= 100 {
					// If the requested size of the level is very large only add a single
					// file in order to avoid massive blow-up in the number of files in
					// the Version.
					//
					// TODO(peter): There is tension between the testing in
					// TestCompactionPickerLevelMaxBytes and
					// TestCompactionPickerTargetLevel. Clean this up somehow.
					m.Size = size
					if level != 0 {
						endKey := base.MakeInternalKey([]byte(fmt.Sprintf("%04d", size)), base.SeqNum(i), InternalKeyKindSet)
						m.ExtendPointKeyBounds(opts.Comparer.Compare, key, endKey)
					}
				}
				if compensation > 0 {
					m.PopulateStats(&manifest.TableStats{
						RangeDeletionsBytesEstimate: compensation,
					})
				}
				files[level] = append(files[level], m)
				sizes[level] += int64(m.Size)
			}
		}
	}

	vers, latest := newVersionWithLatest(opts, files)
	return vers, latest, opts, ""
}

// parseTableMeta parses a table number from a string. The string is expected to
// be in the form of one of the following:
//
//	start.SET.1-end.SET.2
//	<table-num>:start.SET.1-end.SET.2
func parseTableMeta(t *testing.T, s string, opts *Options) (*manifest.TableMetadata, error) {
	parts := strings.Split(s, ":")
	var tableNum base.TableNum
	if len(parts) > 1 {
		tableNum = parseTableNum(t, strings.TrimSpace(parts[0]))
		parts = parts[1:]
	}
	fields := strings.Fields(parts[0])
	parts = strings.Split(fields[0], "-")
	if len(parts) != 2 {
		return nil, errors.Errorf("malformed table spec: %s. usage: <optional-file-num>:start.SET.1-end.SET.2", s)
	}
	m := &manifest.TableMetadata{
		TableNum: tableNum,
		Size:     1028,
	}
	m.InitPhysicalBacking()
	m.ExtendPointKeyBounds(
		opts.Comparer.Compare,
		base.ParseInternalKey(strings.TrimSpace(parts[0])),
		base.ParseInternalKey(strings.TrimSpace(parts[1])),
	)
	for _, p := range fields {
		if strings.HasPrefix(p, "size=") {
			v, err := strconv.Atoi(strings.TrimPrefix(p, "size="))
			if err != nil {
				return nil, err
			}
			m.Size = uint64(v)
		}
		if strings.HasPrefix(p, "range-deletions-bytes-estimate=") {
			v, err := strconv.Atoi(strings.TrimPrefix(p, "range-deletions-bytes-estimate="))
			if err != nil {
				return nil, err
			}
			m.TableBacking.PopulateProperties(&sstable.Properties{
				NumDeletions: 1, // At least one range del responsible for the deletion bytes.
			})
			m.PopulateStats(&manifest.TableStats{
				RangeDeletionsBytesEstimate: uint64(v),
			})
		}
	}
	m.SmallestSeqNum = m.Smallest().SeqNum()
	m.LargestSeqNum = m.Largest().SeqNum()
	if m.SmallestSeqNum > m.LargestSeqNum {
		m.SmallestSeqNum, m.LargestSeqNum = m.LargestSeqNum, m.SmallestSeqNum
	}
	m.LargestSeqNumAbsolute = m.LargestSeqNum
	return m, nil
}

// parseCompactionLines parse in-progress compactions in the form of:
// L0 000001 -> L2 000005
func parseCompactionLines(
	t *testing.T, compactionLines []string, fileMetas [manifest.NumLevels][]*manifest.TableMetadata,
) ([]compactionInfo, error) {
	var inProgressCompactions []compactionInfo
	for len(compactionLines) > 0 {
		parts := strings.Fields(compactionLines[0])
		compactionLines = compactionLines[1:]

		var level int
		var info compactionInfo
		compactionFiles := map[int][]*manifest.TableMetadata{}
		for _, p := range parts {
			switch p {
			case "L0", "L1", "L2", "L3", "L4", "L5", "L6":
				var err error
				level, err = strconv.Atoi(p[1:])
				if err != nil {
					return nil, err
				}
				if len(info.inputs) > 0 && info.inputs[len(info.inputs)-1].level == level {
					// eg, L0 -> L0 compaction or L6 -> L6 compaction
					continue
				}
				if info.outputLevel < level {
					info.outputLevel = level
				}
				info.inputs = append(info.inputs, compactionLevel{level: level})
			case "->":
				continue
			default:
				tableNum := parseTableNum(t, p)
				var compactFile *manifest.TableMetadata
				for _, m := range fileMetas[level] {
					if m.TableNum == tableNum {
						compactFile = m
					}
				}
				if compactFile == nil {
					return nil, errors.Errorf("cannot find compaction file %s", tableNum)
				}
				compactFile.CompactionState = manifest.CompactionStateCompacting
				var bounds base.UserKeyBounds
				if info.bounds != nil {
					bounds = info.bounds.Union(DefaultComparer.Compare, compactFile.UserKeyBounds())
				} else {
					bounds = compactFile.UserKeyBounds()
				}
				info.bounds = &bounds
				compactionFiles[level] = append(compactionFiles[level], compactFile)
			}
		}
		for i, cl := range info.inputs {
			files := compactionFiles[cl.level]
			if cl.level == 0 {
				info.inputs[i].files = manifest.NewLevelSliceSeqSorted(files)
			} else {
				info.inputs[i].files = manifest.NewLevelSliceKeySorted(DefaultComparer.Compare, files)
			}
			// Mark as intra-L0 compacting if the compaction is
			// L0 -> L0.
			if info.outputLevel == 0 {
				for _, f := range files {
					f.IsIntraL0Compacting = true
				}
			}
		}
		inProgressCompactions = append(inProgressCompactions, info)
	}

	return inProgressCompactions, nil
}

func TestCompactionPickerByScoreLevelMaxBytes(t *testing.T) {
	datadriven.RunTest(t, "testdata/compaction_picker_level_max_bytes",
		func(t *testing.T, d *datadriven.TestData) string {
			switch d.Cmd {
			case "init":
				vers, latest, opts, errMsg := loadVersion(t, d)
				if errMsg != "" {
					return errMsg
				}
				p := newCompactionPickerByScore(vers, latest, opts, nil)
				var buf bytes.Buffer
				for level := p.getBaseLevel(); level < numLevels; level++ {
					fmt.Fprintf(&buf, "%d: %d\n", level, p.levelMaxBytes[level])
				}
				return buf.String()

			default:
				return fmt.Sprintf("unknown command: %s", d.Cmd)
			}
		})
}

func TestCompactionPickerTargetLevel(t *testing.T) {
	var vers *manifest.Version
	var latest *latestVersionState
	var opts *Options
	var pickerByScore *compactionPickerByScore

	parseInProgress := func(vals []string) ([]compactionInfo, error) {
		var levels []int
		for _, s := range vals {
			l, err := strconv.ParseInt(s, 10, 8)
			if err != nil {
				return nil, err
			}
			levels = append(levels, int(l))
		}
		if len(levels)%2 != 0 {
			return nil, errors.New("odd number of levels with ongoing compactions")
		}
		var inProgress []compactionInfo
		for i := 0; i < len(levels); i += 2 {
			inProgress = append(inProgress, compactionInfo{
				inputs: []compactionLevel{
					{level: levels[i]},
					{level: levels[i+1]},
				},
				outputLevel: levels[i+1],
			})
		}
		return inProgress, nil
	}

	resetCompacting := func() {
		for _, files := range vers.Levels {
			for f := range files.All() {
				f.CompactionState = manifest.CompactionStateNotCompacting
			}
		}
		latest.l0Organizer.ResetForTesting(vers)
	}

	pickAuto := func(env compactionEnv, pickerByScore *compactionPickerByScore) (ptc *pickedTableCompaction, allowedCompactions int) {
		inProgressCompactions := len(env.inProgressCompactions)
		allowedCompactions = pickerByScore.getCompactionConcurrency()
		if inProgressCompactions >= allowedCompactions {
			return nil, allowedCompactions
		}
		pc := pickerByScore.pickAutoScore(env)
		if pc == nil {
			return nil, allowedCompactions
		}
		return pc.(*pickedTableCompaction), allowedCompactions
	}

	datadriven.RunTest(t, "testdata/compaction_picker_target_level",
		func(t *testing.T, d *datadriven.TestData) string {
			switch d.Cmd {
			case "init":
				// loadVersion expects a single datadriven argument that it
				// sets as Options.LBaseMaxBytes. It parses the input as
				// newline-separated levels, specifying the level's file size
				// and optionally additional compensation to be added during
				// compensated file size calculations. Eg:
				//
				// init l-base-max-bytes=<LBaseMaxBytes>
				// <level>: <size> [compensation]
				// <level>: <size> [compensation]
				var errMsg string
				vers, latest, opts, errMsg = loadVersion(t, d)
				// This test limits the count based on the L0 read amp, compaction
				// debt, and deleted garbage, so we would like to return math.MaxInt
				// for most test cases. But we don't since it is also used in
				// expandedCompactionByteSizeLimit, and causes the expanded bytes to
				// reduce. The test cases never pick more than 4 compactions, so we
				// use 4 as the default.
				maxConcurrentCompactions := 4
				if d.HasArg("max-concurrent-compactions") {
					d.ScanArgs(t, "max-concurrent-compactions", &maxConcurrentCompactions)
				}
				opts.CompactionConcurrencyRange = func() (int, int) {
					return 1, maxConcurrentCompactions
				}
				if d.HasArg("compaction-debt-concurrency") {
					d.ScanArgs(t, "compaction-debt-concurrency", &opts.Experimental.CompactionDebtConcurrency)
				}
				if errMsg != "" {
					return errMsg
				}
				return runVersionFileSizes(vers)
			case "init_cp":
				resetCompacting()

				var inProgress []compactionInfo
				if arg, ok := d.Arg("ongoing"); ok {
					var err error
					inProgress, err = parseInProgress(arg.Vals)
					if err != nil {
						return err.Error()
					}
				}

				pickerByScore = newCompactionPickerByScore(vers, latest, opts, inProgress)
				return fmt.Sprintf("base: %d", pickerByScore.baseLevel)
			case "queue":
				var b strings.Builder
				var inProgress []compactionInfo
				printConcurrency := false
				if d.HasArg("print-compaction-concurrency") {
					printConcurrency = true
				}
				for {
					env := compactionEnv{
						diskAvailBytes:          math.MaxUint64,
						earliestUnflushedSeqNum: base.SeqNumMax,
						inProgressCompactions:   inProgress,
					}
					pc, concurrency := pickAuto(env, pickerByScore)
					if printConcurrency {
						fmt.Fprintf(&b, "compaction concurrency: %d\n", concurrency)
					}
					if pc == nil {
						break
					}
					fmt.Fprintf(&b, "L%d->L%d: %.1f\n", pc.startLevel.level, pc.outputLevel.level, pc.score)
					bounds := pc.bounds.Clone()
					inProgress = append(inProgress, compactionInfo{
						inputs:      pc.inputs,
						outputLevel: pc.outputLevel.level,
						bounds:      &bounds,
					})

					if pc.outputLevel.level == 0 {
						// Once we pick one L0->L0 compaction, we'll keep on doing so
						// because the test isn't marking files as Compacting.
						break
					}
					if pc.startLevel != nil && pc.startLevel.level == 0 {
						require.NoError(t, latest.l0Organizer.UpdateStateForStartedCompaction(
							[]manifest.LevelSlice{pc.startLevel.files}, true /* isBase */))
					}
					for _, cl := range pc.inputs {
						for f := range cl.files.All() {
							f.CompactionState = manifest.CompactionStateCompacting
							fmt.Fprintf(&b, "  %s marked as compacting\n", f)
						}
					}
				}

				resetCompacting()
				return b.String()
			case "pick":
				resetCompacting()

				var inProgress []compactionInfo
				if len(d.CmdArgs) == 1 {
					arg := d.CmdArgs[0]
					if arg.Key != "ongoing" {
						return "unknown arg: " + arg.Key
					}
					var err error
					inProgress, err = parseInProgress(arg.Vals)
					if err != nil {
						return err.Error()
					}
				}

				var l0InProgress []manifest.L0Compaction

				// Mark files as compacting for each in-progress compaction.
				for i := range inProgress {
					c := &inProgress[i]
					for j, cl := range c.inputs {
						iter := vers.Levels[cl.level].Iter()
						for f := iter.First(); ; f = iter.Next() {
							if f == nil {
								d.Fatalf(t, "could not find any non-compacting files in L%d", cl.level)
							}
							if !f.IsCompacting() {
								f.CompactionState = manifest.CompactionStateCompacting
								c.inputs[j].files = iter.Take().Slice()
								break
							}
						}
					}
					if c.inputs[0].level == 0 {
						l0InProgress = append(l0InProgress, manifest.L0Compaction{
							Bounds:    manifest.KeyRange(opts.Comparer.Compare, c.inputs[0].files.All()),
							IsIntraL0: c.outputLevel == 0,
						})
					}
					if c.inputs[0].level == 0 && c.outputLevel != 0 {
						// L0->Lbase: mark all of Lbase as compacting.
						c.inputs[1].files = vers.Levels[c.outputLevel].Slice()
						for _, in := range c.inputs {
							for f := range in.files.All() {
								f.CompactionState = manifest.CompactionStateCompacting
							}
						}
					}
				}

				// Make sure the L0Organizer reflects the current in-progress compactions.
				latest.l0Organizer.InitCompactingFileInfo(l0InProgress)

				var b strings.Builder
				fmt.Fprintf(&b, "Initial state before pick:\n%s", runVersionFileSizes(vers))
				pc, _ := pickAuto(compactionEnv{
					earliestUnflushedSeqNum: base.SeqNumMax,
					inProgressCompactions:   inProgress,
				}, pickerByScore)
				if pc != nil {
					fmt.Fprintf(&b, "Picked: L%d->L%d: %0.1f\n", pc.startLevel.level, pc.outputLevel.level, pc.score)
				}
				if pc == nil {
					fmt.Fprintln(&b, "Picked: no compaction")
				}
				return b.String()
			case "pick_manual":
				var startLevel int
				var start, end string
				d.MaybeScanArgs(t, "level", &startLevel)
				d.MaybeScanArgs(t, "start", &start)
				d.MaybeScanArgs(t, "end", &end)

				iStart := base.MakeInternalKey([]byte(start), base.SeqNumMax, InternalKeyKindMax)
				iEnd := base.MakeInternalKey([]byte(end), 0, 0)
				manual := &manualCompaction{
					done:  make(chan error, 1),
					level: startLevel,
					start: iStart.UserKey,
					end:   iEnd.UserKey,
				}

				pc, retryLater := newPickedManualCompaction(
					pickerByScore.vers,
					pickerByScore.latestVersionState.l0Organizer,
					opts,
					compactionEnv{
						earliestUnflushedSeqNum: base.SeqNumMax,
					},
					pickerByScore.getBaseLevel(),
					manual)
				if pc == nil {
					return fmt.Sprintf("nil, retryLater = %v", retryLater)
				}

				return fmt.Sprintf("L%d->L%d, retryLater = %v", pc.startLevel.level, pc.outputLevel.level, retryLater)
			default:
				return fmt.Sprintf("unknown command: %s", d.Cmd)
			}
		})
}

func TestCompactionPickerEstimatedCompactionDebt(t *testing.T) {
	datadriven.RunTest(t, "testdata/compaction_picker_estimated_debt",
		func(t *testing.T, d *datadriven.TestData) string {
			switch d.Cmd {
			case "init":
				vers, latest, opts, errMsg := loadVersion(t, d)
				if errMsg != "" {
					return errMsg
				}
				opts.MemTableSize = 1000

				p := newCompactionPickerByScore(vers, latest, opts, nil)
				return fmt.Sprintf("%d\n", p.estimatedCompactionDebt())

			default:
				return fmt.Sprintf("unknown command: %s", d.Cmd)
			}
		})
}

func TestCompactionPickerL0(t *testing.T) {
	opts := DefaultOptions()
	opts.Experimental.L0CompactionConcurrency = 1

	parseMeta := func(s string) (*manifest.TableMetadata, error) {
		parts := strings.Split(s, ":")
		tableNum := parseTableNum(t, parts[0])
		fields := strings.Fields(parts[1])
		parts = strings.Split(fields[0], "-")
		if len(parts) != 2 {
			return nil, errors.Errorf("malformed table spec: %s", s)
		}
		m := (&manifest.TableMetadata{
			TableNum: tableNum,
		}).ExtendPointKeyBounds(
			opts.Comparer.Compare,
			base.ParseInternalKey(strings.TrimSpace(parts[0])),
			base.ParseInternalKey(strings.TrimSpace(parts[1])),
		)
		m.SmallestSeqNum = m.Smallest().SeqNum()
		m.LargestSeqNum = m.Largest().SeqNum()
		if m.SmallestSeqNum > m.LargestSeqNum {
			m.SmallestSeqNum, m.LargestSeqNum = m.LargestSeqNum, m.SmallestSeqNum
		}
		m.LargestSeqNumAbsolute = m.LargestSeqNum
		m.InitPhysicalBacking()
		return m, nil
	}

	var picker *compactionPickerByScore
	var inProgressCompactions []compactionInfo
	var ptc *pickedTableCompaction

	datadriven.RunTest(t, "testdata/compaction_picker_L0", func(t *testing.T, td *datadriven.TestData) string {
		inProgressCompactions = nil
		switch td.Cmd {
		case "define":
			fileMetas := [manifest.NumLevels][]*manifest.TableMetadata{}
			baseLevel := manifest.NumLevels - 1
			level := 0
			var err error
			lines := strings.Split(td.Input, "\n")

			for len(lines) > 0 {
				data := strings.TrimSpace(lines[0])
				lines = lines[1:]
				switch data {
				case "L0", "L1", "L2", "L3", "L4", "L5", "L6":
					level, err = strconv.Atoi(data[1:])
					if err != nil {
						return err.Error()
					}
				case "compactions":
					inProgressCompactions, err = parseCompactionLines(t, lines, fileMetas)
					if err != nil {
						return err.Error()
					}
					// Compactions should be the last definition.
					lines = nil
				default:
					meta, err := parseMeta(data)
					if err != nil {
						return err.Error()
					}
					if level != 0 && level < baseLevel {
						baseLevel = level
					}
					fileMetas[level] = append(fileMetas[level], meta)
				}
			}

			version, latest := newVersionWithLatest(opts, fileMetas)
			latest.l0Organizer.InitCompactingFileInfo(inProgressL0Compactions(inProgressCompactions))
			vs := &versionSet{
				opts:   opts,
				latest: latest,
				cmp:    DefaultComparer,
			}
			vs.versions.Init(nil)
			vs.append(version)
			picker = &compactionPickerByScore{
				opts:               opts,
				vers:               version,
				latestVersionState: vs.latest,
				baseLevel:          baseLevel,
			}
			vs.picker = picker
			picker.initLevelMaxBytes(inProgressCompactions)

			var buf bytes.Buffer
			fmt.Fprint(&buf, version.String())
			if len(inProgressCompactions) > 0 {
				fmt.Fprintln(&buf, "compactions")
				for _, c := range inProgressCompactions {
					fmt.Fprintf(&buf, "  %s\n", c.String())
				}
			}
			return buf.String()
		case "pick-auto":
			td.MaybeScanArgs(t, "l0_compaction_threshold", &opts.L0CompactionThreshold)
			td.MaybeScanArgs(t, "l0_compaction_file_threshold", &opts.L0CompactionFileThreshold)

			score := true
			if td.HasArg("non-score") {
				score = false
			}
			env := compactionEnv{
				diskAvailBytes:          math.MaxUint64,
				earliestUnflushedSeqNum: math.MaxUint64,
				inProgressCompactions:   inProgressCompactions,
			}
			ptc = nil
			var pc pickedCompaction
			if score {
				pc = picker.pickAutoScore(env)
			} else {
				pc = picker.pickAutoNonScore(env)
			}
			if pc != nil {
				ptc = pc.(*pickedTableCompaction)
			}
			var result strings.Builder
			if ptc != nil {
				checkClone(t, ptc)
				c := newCompaction(ptc, opts, time.Now(), nil /* provider */, noopGrantHandle{}, noSharedStorage, neverSeparateValues)
				fmt.Fprintf(&result, "L%d -> L%d\n", ptc.startLevel.level, ptc.outputLevel.level)
				fmt.Fprintf(&result, "L%d: %s\n", ptc.startLevel.level, tableNums(ptc.startLevel.files))
				if !ptc.outputLevel.files.Empty() {
					fmt.Fprintf(&result, "L%d: %s\n", ptc.outputLevel.level, tableNums(ptc.outputLevel.files))
				}
				if !c.grandparents.Empty() {
					fmt.Fprintf(&result, "grandparents: %s\n", tableNums(c.grandparents))
				}
			} else {
				return "nil"
			}
			return result.String()
		case "mark-for-compaction":
			var tableNum uint64
			td.ScanArgs(t, "file", &tableNum)
			for l, lm := range picker.vers.Levels {
				iter := lm.Iter()
				for f := iter.First(); f != nil; f = iter.Next() {
					if f.TableNum != base.TableNum(tableNum) {
						continue
					}
					picker.vers.MarkedForCompaction.Insert(f, l)
					return fmt.Sprintf("marked L%d.%s", l, f.TableNum)
				}
			}
			return "not-found"
		case "max-output-file-size":
			if ptc == nil {
				return "no compaction"
			}
			return fmt.Sprintf("%d", ptc.maxOutputFileSize)
		case "max-overlap-bytes":
			if ptc == nil {
				return "no compaction"
			}
			return fmt.Sprintf("%d", ptc.maxOverlapBytes)
		}
		return fmt.Sprintf("unrecognized command: %s", td.Cmd)
	})
}

const noSharedStorage = false

func TestCompactionPickerConcurrency(t *testing.T) {
	opts := DefaultOptions()
	opts.Experimental.L0CompactionConcurrency = 1
	lowerConcurrencyLimit, upperConcurrencyLimit := 1, 4
	opts.CompactionConcurrencyRange = func() (int, int) { return lowerConcurrencyLimit, upperConcurrencyLimit }

	var picker *compactionPickerByScore
	var inProgressCompactions []compactionInfo

	datadriven.RunTest(t, "testdata/compaction_picker_concurrency", func(t *testing.T, td *datadriven.TestData) string {

		switch td.Cmd {
		case "define":
			inProgressCompactions = nil
			tableMetas := [manifest.NumLevels][]*manifest.TableMetadata{}
			level := 0
			var err error
			lines := strings.Split(td.Input, "\n")
			for len(lines) > 0 {
				data := strings.TrimSpace(lines[0])
				lines = lines[1:]
				switch data {
				case "L0", "L1", "L2", "L3", "L4", "L5", "L6":
					level, err = strconv.Atoi(data[1:])
					if err != nil {
						return err.Error()
					}
				case "compactions":
					inProgressCompactions, err = parseCompactionLines(t, lines, tableMetas)
					if err != nil {
						return err.Error()
					}
					// Compactions should be the last definition.
					lines = nil
				default:
					meta, err := parseTableMeta(t, data, opts)
					if err != nil {
						return err.Error()
					}
					tableMetas[level] = append(tableMetas[level], meta)
				}
			}

			version, latest := newVersionWithLatest(opts, tableMetas)
			latest.l0Organizer.InitCompactingFileInfo(inProgressL0Compactions(inProgressCompactions))
			vs := &versionSet{
				opts: opts,
				cmp:  DefaultComparer,
			}
			vs.versions.Init(nil)
			vs.append(version)

			picker = newCompactionPickerByScore(version, latest, opts, inProgressCompactions)
			vs.picker = picker

			var buf bytes.Buffer
			fmt.Fprint(&buf, version.String())
			if len(inProgressCompactions) > 0 {
				fmt.Fprintln(&buf, "compactions")
				for _, c := range inProgressCompactions {
					fmt.Fprintf(&buf, "  %s\n", c.String())
				}
			}
			return buf.String()

		case "pick-auto":
			td.MaybeScanArgs(t, "l0_compaction_threshold", &opts.L0CompactionThreshold)
			td.MaybeScanArgs(t, "l0_compaction_concurrency", &opts.Experimental.L0CompactionConcurrency)
			td.MaybeScanArgs(t, "compaction_debt_concurrency", &opts.Experimental.CompactionDebtConcurrency)
			td.MaybeScanArgs(t, "concurrency", &lowerConcurrencyLimit, &upperConcurrencyLimit)

			env := compactionEnv{
				earliestUnflushedSeqNum: math.MaxUint64,
				inProgressCompactions:   inProgressCompactions,
			}
			inProgressCount := len(env.inProgressCompactions)
			allowedCompactions := picker.getCompactionConcurrency()
			var pc pickedCompaction
			var ptc *pickedTableCompaction
			if inProgressCount < allowedCompactions {
				pc = picker.pickAutoScore(env)
				if pc != nil {
					// TODO(jackson): Expand the interface and adapt the test so
					// we don't need to cast.
					ptc = pc.(*pickedTableCompaction)
				}
			}
			var result strings.Builder
			fmt.Fprintf(&result, "picker.getCompactionConcurrency: %d\n", allowedCompactions)
			if pc != nil {
				c := newCompaction(ptc, opts, time.Now(), nil /* provider */, noopGrantHandle{}, noSharedStorage, neverSeparateValues)
				fmt.Fprintf(&result, "L%d -> L%d\n", ptc.startLevel.level, ptc.outputLevel.level)
				fmt.Fprintf(&result, "L%d: %s\n", ptc.startLevel.level, tableNums(ptc.startLevel.files))
				if !ptc.outputLevel.files.Empty() {
					fmt.Fprintf(&result, "L%d: %s\n", ptc.outputLevel.level, tableNums(ptc.outputLevel.files))
				}
				if !c.grandparents.Empty() {
					fmt.Fprintf(&result, "grandparents: %s\n", tableNums(c.grandparents))
				}
			} else {
				fmt.Fprintf(&result, "nil")
			}
			return result.String()

		default:
			return fmt.Sprintf("unrecognized command: %s", td.Cmd)
		}
	})
}

func TestCompactionPickerPickReadTriggered(t *testing.T) {
	opts := DefaultOptions()
	var picker *compactionPickerByScore
	var rcList readCompactionQueue

	datadriven.RunTest(t, "testdata/compaction_picker_read_triggered", func(t *testing.T, td *datadriven.TestData) string {
		switch td.Cmd {
		case "define":
			rcList = readCompactionQueue{}
			fileMetas := [manifest.NumLevels][]*manifest.TableMetadata{}
			level := 0
			var err error
			lines := strings.Split(td.Input, "\n")

			for len(lines) > 0 {
				data := strings.TrimSpace(lines[0])
				lines = lines[1:]
				switch data {
				case "L0", "L1", "L2", "L3", "L4", "L5", "L6":
					level, err = strconv.Atoi(data[1:])
					if err != nil {
						return err.Error()
					}
				default:
					meta, err := parseTableMeta(t, data, opts)
					if err != nil {
						return err.Error()
					}
					fileMetas[level] = append(fileMetas[level], meta)
				}
			}

			vers, latest := newVersionWithLatest(opts, fileMetas)
			vs := &versionSet{
				opts:   opts,
				cmp:    DefaultComparer,
				latest: latest,
			}
			vs.versions.Init(nil)
			vs.append(vers)
			var inProgressCompactions []compactionInfo
			picker = newCompactionPickerByScore(vers, latest, opts, inProgressCompactions)
			vs.picker = picker

			var buf bytes.Buffer
			fmt.Fprint(&buf, vers.String())
			return buf.String()

		case "add-read-compaction":
			for _, line := range crstrings.Lines(td.Input) {
				parts := strings.Split(line, " ")
				if len(parts) != 3 {
					return "error: malformed data for add-read-compaction. usage: <level>: <start>-<end> <filenum>"
				}
				if l, err := strconv.Atoi(parts[0][:1]); err == nil {
					keys := strings.Split(parts[1], "-")
					tableNum := parseTableNum(t, parts[2])

					rc := readCompaction{
						level:    l,
						start:    []byte(keys[0]),
						end:      []byte(keys[1]),
						tableNum: tableNum,
					}
					rcList.add(&rc, DefaultComparer.Compare)
				} else {
					return err.Error()
				}
			}
			return ""

		case "show-read-compactions":
			var sb strings.Builder
			if rcList.size == 0 {
				sb.WriteString("(none)")
			}
			for i := 0; i < rcList.size; i++ {
				rc := rcList.at(i)
				sb.WriteString(fmt.Sprintf("(level: %d, start: %s, end: %s)\n", rc.level, string(rc.start), string(rc.end)))
			}
			return sb.String()

		case "pick-auto":
			var result strings.Builder
			var pc pickedCompaction
			var ptc *pickedTableCompaction
			env := compactionEnv{
				earliestUnflushedSeqNum: math.MaxUint64,
				readCompactionEnv: readCompactionEnv{
					readCompactions: &rcList,
					flushing:        false,
				},
			}
			if pc = picker.pickAutoScore(env); pc != nil {
				fmt.Fprintf(&result, "picked score-based compaction:\n")
			} else if pc = picker.pickAutoNonScore(env); pc != nil {
				fmt.Fprintf(&result, "picked non-score-based compaction:\n")
			}
			if pc != nil {
				ptc = pc.(*pickedTableCompaction)
			}
			if ptc != nil {
				fmt.Fprintf(&result, "L%d -> L%d\n", ptc.startLevel.level, ptc.outputLevel.level)
				fmt.Fprintf(&result, "L%d: %s\n", ptc.startLevel.level, tableNums(ptc.startLevel.files))
				if !ptc.outputLevel.files.Empty() {
					fmt.Fprintf(&result, "L%d: %s\n", ptc.outputLevel.level, tableNums(ptc.outputLevel.files))
				}
			} else {
				return "nil"
			}
			return result.String()
		}
		return fmt.Sprintf("unrecognized command: %s", td.Cmd)
	})
}

type alwaysMultiLevel struct{}

func optionAlwaysMultiLevel() MultiLevelHeuristic {
	return alwaysMultiLevel{}
}

func (d alwaysMultiLevel) pick(
	pcOrig *pickedTableCompaction, opts *Options, env compactionEnv,
) *pickedTableCompaction {
	pcMulti := pcOrig.clone()
	if !pcMulti.setupMultiLevelCandidate(opts, env) {
		return pcOrig
	}
	return pcMulti
}

func (d alwaysMultiLevel) allowL0() bool  { return false }
func (d alwaysMultiLevel) String() string { return "always" }

func TestPickedCompactionSetupInputs(t *testing.T) {
	opts := DefaultOptions()

	setupInputTest := func(t *testing.T, d *datadriven.TestData) string {
		var inProgressCompactions []compactionInfo
		switch d.Cmd {
		case "setup-inputs":
			var availBytes uint64 = math.MaxUint64
			var maxLevelBytes [7]int64
			args := d.CmdArgs

			if len(args) > 0 && args[0].Key == "avail-bytes" {
				require.Equal(t, 1, len(args[0].Vals))
				var err error
				availBytes, err = strconv.ParseUint(args[0].Vals[0], 10, 64)
				require.NoError(t, err)
				args = args[1:]
			}

			if len(args) != 2 {
				return "setup-inputs [avail-bytes=XXX] <start> <end>"
			}

			pc := &pickedTableCompaction{
				baseLevel: 1,
				inputs:    []compactionLevel{{level: -1}, {level: -1}},
			}
			pc.startLevel, pc.outputLevel = &pc.inputs[0], &pc.inputs[1]
			var currentLevel int
			var files [numLevels][]*manifest.TableMetadata
			tableNum := base.TableNum(1)

			lines := strings.Split(d.Input, "\n")
			for len(lines) > 0 {
				data := strings.TrimSpace(lines[0])
				dataArgs := strings.Fields(strings.TrimSpace(lines[0]))
				lines = lines[1:]
				switch dataArgs[0] {
				case "L0", "L1", "L2", "L3", "L4", "L5", "L6":
					level, err := strconv.Atoi(dataArgs[0][1:])
					if err != nil {
						return err.Error()
					}
					currentLevel = level
					if len(dataArgs) > 1 {
						maxSizeArg := strings.Replace(dataArgs[1], "max-size=", "", 1)
						maxSize, err := strconv.ParseInt(maxSizeArg, 10, 64)
						if err != nil {
							return err.Error()
						}
						maxLevelBytes[level] = maxSize
					} else {
						maxLevelBytes[level] = math.MaxInt64
					}
					if pc.startLevel.level == -1 {
						pc.startLevel.level = level

					} else if pc.outputLevel.level == -1 {
						if pc.startLevel.level >= level {
							return fmt.Sprintf("startLevel=%d >= outputLevel=%d\n", pc.startLevel.level, level)
						}
						pc.outputLevel.level = level
					}
				case "compactions":
					var err error
					inProgressCompactions, err = parseCompactionLines(t, lines, files)
					if err != nil {
						return err.Error()
					}
					// Compactions should be the last definition.
					lines = nil
				default:
					meta, err := parseTableMeta(t, data, opts)
					if err != nil {
						return err.Error()
					}
					if meta.TableNum == 0 {
						// Auto-assign table num.
						meta.TableNum = tableNum
					}
					tableNum++
					files[currentLevel] = append(files[currentLevel], meta)
				}
			}

			for _, levelFiles := range files {
				for i := 1; i < len(levelFiles); i++ {
					if !checkTableBoundary(levelFiles[i-1], levelFiles[i], opts.Comparer.Compare) {
						d.Fatalf(t, "overlapping tables: %s and %s", levelFiles[i-1], levelFiles[i])
					}
				}
			}

			if pc.outputLevel.level == -1 {
				pc.outputLevel.level = pc.startLevel.level + 1
			}
			pc.version = newVersion(opts, files)
			pc.startLevel.files = pc.version.Overlaps(
				pc.startLevel.level,
				base.UserKeyBoundsInclusive([]byte(args[0].String()), []byte(args[1].String())),
			)

			var isCompacting bool
			if !pc.setupInputs(opts, availBytes, inProgressCompactions, pc.startLevel, nil /* problemSpans */) {
				isCompacting = true
			}
			origPC := pc
			env := compactionEnv{
				diskAvailBytes:        availBytes,
				inProgressCompactions: inProgressCompactions,
			}
			pc = pc.maybeAddLevel(opts, env)
			// If pc points to a new pickedCompaction, a new multi level compaction
			// was initialized.
			initMultiLevel := pc != origPC
			checkClone(t, pc)
			var buf bytes.Buffer
			for _, cl := range pc.inputs {
				if cl.files.Empty() {
					continue
				}

				fmt.Fprintf(&buf, "L%d\n", cl.level)
				for f := range cl.files.All() {
					fmt.Fprintf(&buf, "  %s\n", f)
				}
			}
			if isCompacting {
				fmt.Fprintf(&buf, "is-compacting\n")
			}

			if len(inProgressCompactions) > 0 {
				fmt.Fprintln(&buf, "compactions")
				for _, c := range inProgressCompactions {
					fmt.Fprintf(&buf, "  %s\n", c.String())
				}
			}

			if initMultiLevel {
				extraLevel := pc.inputs[1].level
				fmt.Fprintf(&buf, "init-multi-level(%d,%d,%d)\n", pc.startLevel.level, extraLevel,
					pc.outputLevel.level)
				fmt.Fprintf(&buf, "Original WriteAmp %.2f; ML WriteAmp %.2f\n", origPC.predictedWriteAmp(), pc.predictedWriteAmp())
				fmt.Fprintf(&buf, "Original OverlappingRatio %.2f; ML OverlappingRatio %.2f\n", origPC.overlappingRatio(), pc.overlappingRatio())
			}
			return buf.String()

		default:
			return fmt.Sprintf("unknown command: %s", d.Cmd)
		}
	}

	t.Logf("Test basic setup inputs behavior without multi level compactions")
	opts.Experimental.MultiLevelCompactionHeuristic = OptionNoMultiLevel
	datadriven.RunTest(t, "testdata/compaction_setup_inputs",
		setupInputTest)

	t.Logf("Turning multi level compaction on")
	opts.CompactionConcurrencyRange = func() (int, int) {
		// Setting an upper limit greater than 1 is necessary to allow
		// multilevel compactions to be picked.
		return 1, 2
	}
	opts.Experimental.MultiLevelCompactionHeuristic = optionAlwaysMultiLevel
	datadriven.RunTest(t, "testdata/compaction_setup_inputs_multilevel_dummy",
		setupInputTest)

	t.Logf("Try Write-Amp Heuristic")
	opts.Experimental.MultiLevelCompactionHeuristic = OptionWriteAmpHeuristic
	datadriven.RunTest(t, "testdata/compaction_setup_inputs_multilevel_write_amp",
		setupInputTest)
}

func TestPickedCompactionExpandInputs(t *testing.T) {
	opts := DefaultOptions()
	cmp := DefaultComparer.Compare
	var files []*manifest.TableMetadata

	parseMeta := func(s string) *manifest.TableMetadata {
		parts := strings.Split(s, "-")
		if len(parts) != 2 {
			t.Fatalf("malformed table spec: %s", s)
		}
		m := (&manifest.TableMetadata{}).ExtendPointKeyBounds(
			opts.Comparer.Compare,
			base.ParseInternalKey(parts[0]),
			base.ParseInternalKey(parts[1]),
		)
		m.InitPhysicalBacking()
		return m
	}

	datadriven.RunTest(t, "testdata/compaction_expand_inputs",
		func(t *testing.T, d *datadriven.TestData) string {
			switch d.Cmd {
			case "define":
				files = nil
				if len(d.Input) == 0 {
					return ""
				}
				for _, data := range strings.Split(d.Input, "\n") {
					meta := parseMeta(data)
					meta.TableNum = base.TableNum(len(files))
					files = append(files, meta)
				}
				manifest.SortBySmallest(files, cmp)
				// Verify that the tables have no user key overlap.
				for i := 1; i < len(files); i++ {
					if !checkTableBoundary(files[i-1], files[i], cmp) {
						d.Fatalf(t, "overlapping tables: %s and %s", files[i-1], files[i])
					}
				}
				return ""

			case "expand-inputs":
				pc := &pickedTableCompaction{
					inputs: []compactionLevel{{level: 1}},
				}
				pc.startLevel = &pc.inputs[0]

				var filesLevelled [numLevels][]*manifest.TableMetadata
				filesLevelled[pc.startLevel.level] = files
				pc.version = newVersion(opts, filesLevelled)

				if len(d.CmdArgs) != 1 {
					return fmt.Sprintf("%s expects 1 argument", d.Cmd)
				}
				index, err := strconv.ParseInt(d.CmdArgs[0].String(), 10, 64)
				if err != nil {
					return err.Error()
				}

				// Advance the iterator to position `index`.
				iter := pc.version.Levels[pc.startLevel.level].Iter()
				_ = iter.First()
				for i := int64(0); i < index; i++ {
					_ = iter.Next()
				}

				var buf bytes.Buffer
				for f := range iter.Take().Slice().All() {
					fmt.Fprintf(&buf, "%d: %s-%s\n", f.TableNum, f.Smallest(), f.Largest())
				}
				return buf.String()

			default:
				return fmt.Sprintf("unknown command: %s", d.Cmd)
			}
		})
}

func TestCompactionOutputFileSize(t *testing.T) {
	opts := DefaultOptions()
	var picker *compactionPickerByScore

	datadriven.RunTest(t, "testdata/compaction_output_file_size", func(t *testing.T, td *datadriven.TestData) string {
		switch td.Cmd {
		case "define":
			fileMetas := [manifest.NumLevels][]*manifest.TableMetadata{}
			level := 0
			var err error
			lines := strings.Split(td.Input, "\n")

			for len(lines) > 0 {
				data := strings.TrimSpace(lines[0])
				lines = lines[1:]
				switch data {
				case "L0", "L1", "L2", "L3", "L4", "L5", "L6":
					level, err = strconv.Atoi(data[1:])
					if err != nil {
						return err.Error()
					}
				default:
					meta, err := parseTableMeta(t, data, opts)
					if err != nil {
						return err.Error()
					}
					fileMetas[level] = append(fileMetas[level], meta)
				}
			}

			vers, latest := newVersionWithLatest(opts, fileMetas)
			vs := &versionSet{
				opts:   opts,
				cmp:    DefaultComparer,
				latest: latest,
			}
			vs.versions.Init(nil)
			vs.append(vers)
			var inProgressCompactions []compactionInfo
			picker = newCompactionPickerByScore(vers, latest, opts, inProgressCompactions)
			vs.picker = picker

			var buf bytes.Buffer
			fmt.Fprint(&buf, vers.String())
			return buf.String()

		case "pick-auto":
			pc := picker.pickAutoNonScore(compactionEnv{
				earliestUnflushedSeqNum: math.MaxUint64,
				earliestSnapshotSeqNum:  math.MaxUint64,
			})
			var buf bytes.Buffer
			if pc != nil {
				ptc := pc.(*pickedTableCompaction)
				fmt.Fprintf(&buf, "L%d -> L%d\n", ptc.startLevel.level, ptc.outputLevel.level)
				fmt.Fprintf(&buf, "L%d: %s\n", ptc.startLevel.level, tableNums(ptc.startLevel.files))
				fmt.Fprintf(&buf, "maxOutputFileSize: %d\n", ptc.maxOutputFileSize)
			} else {
				return "nil"
			}
			return buf.String()

		default:
			return fmt.Sprintf("unrecognized command: %s", td.Cmd)
		}
	})
}

func TestCompactionPickerCompensatedSize(t *testing.T) {
	testCases := []struct {
		size                  uint64
		pointDelEstimateBytes uint64
		rangeDelEstimateBytes uint64
		wantBytes             uint64
	}{
		{
			size:                  100,
			pointDelEstimateBytes: 0,
			rangeDelEstimateBytes: 0,
			wantBytes:             100,
		},
		{
			size:                  100,
			pointDelEstimateBytes: 10,
			rangeDelEstimateBytes: 0,
			wantBytes:             100 + 10,
		},
		{
			size:                  100,
			pointDelEstimateBytes: 10,
			rangeDelEstimateBytes: 5,
			wantBytes:             100 + 10 + 5,
		},
	}

	for _, tc := range testCases {
		t.Run("", func(t *testing.T) {
			f := &manifest.TableMetadata{Size: tc.size}
			f.InitPhysicalBacking()
			f.PopulateStats(&manifest.TableStats{
				PointDeletionsBytesEstimate: tc.pointDelEstimateBytes,
				RangeDeletionsBytesEstimate: tc.rangeDelEstimateBytes,
			})
			gotBytes := tableCompensatedSize(f)
			require.Equal(t, tc.wantBytes, gotBytes)
		})
	}
}

func TestCompactionPickerPickFile(t *testing.T) {
	var problemSpans *problemspans.ByLevel
	var d *DB
	defer func() {
		if d != nil {
			require.NoError(t, d.Close())
		}
	}()

	datadriven.RunTest(t, "testdata/compaction_picker_pick_file", func(t *testing.T, td *datadriven.TestData) string {
		switch td.Cmd {
		case "define":
			problemSpans = nil
			if d != nil {
				require.NoError(t, d.Close())
			}

			opts := &Options{
				Comparer:                    testkeys.Comparer,
				FormatMajorVersion:          FormatNewest,
				Logger:                      testutils.Logger{T: t},
				DisableAutomaticCompactions: true,
			}
			opts.Experimental.CompactionScheduler = func() CompactionScheduler {
				return NewConcurrencyLimitSchedulerWithNoPeriodicGrantingForTest()
			}

			var err error
			d, err = runDBDefineCmd(td, opts)
			if err != nil {
				return err.Error()
			}
			d.mu.Lock()
			s := d.mu.versions.currentVersion().String()
			d.mu.Unlock()
			return s

		case "file-sizes":
			return runTableFileSizesCmd(td, d)

		case "build":
			if err := runBuildCmd(td, d, d.opts.FS); err != nil {
				return err.Error()
			}
			return ""

		case "ingest-and-excise":
			if err := runIngestAndExciseCmd(td, d); err != nil {
				return err.Error()
			}
			return ""

		case "lsm":
			d.mu.Lock()
			s := d.mu.versions.currentVersion().String()
			d.mu.Unlock()
			return s

		case "problem-spans":
			problemSpans = &problemspans.ByLevel{}
			problemSpans.Init(manifest.NumLevels, d.opts.Comparer.Compare)
			for _, line := range crstrings.Lines(td.Input) {
				var level int
				var span1, span2 string
				n, err := fmt.Sscanf(line, "L%d %s %s", &level, &span1, &span2)
				if err != nil || n != 3 {
					td.Fatalf(t, "malformed problem span %q", line)
				}
				bounds := base.ParseUserKeyBounds(span1 + " " + span2)
				problemSpans.Add(level, bounds, time.Hour*10)
			}
			return ""

		case "pick-file":
			s := strings.TrimPrefix(td.CmdArgs[0].String(), "L")
			level, err := strconv.Atoi(s)
			if err != nil {
				return fmt.Sprintf("unable to parse arg %q as level", td.CmdArgs[0].String())
			}
			if level == 0 {
				panic("L0 picking unimplemented")
			}
			d.mu.Lock()
			defer d.mu.Unlock()

			// Use maybeScheduleCompactionPicker to take care of all of the
			// initialization of the compaction-picking environment, but never
			// pick a compaction; just call pickFile using the user-provided
			// level.
			var lf manifest.LevelFile
			var ok bool
			func() {
				d.mu.versions.logLock()
				defer d.mu.versions.logUnlock()
				env := d.makeCompactionEnvLocked()
				if env == nil {
					return
				}
				p := d.mu.versions.picker.(*compactionPickerByScore)
				lf, ok = pickCompactionSeedFile(p.vers, &p.latestVersionState.virtualBackings,
					d.opts, level, level+1, env.earliestSnapshotSeqNum, problemSpans)
			}()
			if !ok {
				return "(none)"
			}
			return lf.TableMetadata.String()

		default:
			return fmt.Sprintf("unknown command: %s", td.Cmd)
		}
	})
}

type pausableCleaner struct {
	mu      sync.Mutex
	cond    sync.Cond
	paused  bool
	cleaner Cleaner
}

func newPausableCleaner(cleaner Cleaner) *pausableCleaner {
	p := &pausableCleaner{cleaner: cleaner}
	p.cond.L = &p.mu
	return p
}

func (c *pausableCleaner) Clean(fs vfs.FS, fileType base.FileType, path string) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	for c.paused {
		c.cond.Wait()
	}
	return c.cleaner.Clean(fs, fileType, path)
}

func (c *pausableCleaner) pause() {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.paused = true
}

func (c *pausableCleaner) resume() {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.paused = false
	c.cond.Broadcast()
}

func TestCompactionPickerScores(t *testing.T) {
	cleaner := newPausableCleaner(DeleteCleaner{})

	var d *DB
	defer func() {
		if d != nil {
			cleaner.resume()
			require.NoError(t, closeAllSnapshots(d))
			require.NoError(t, d.Close())
		}
	}()

	var buf bytes.Buffer
	datadriven.RunTest(t, "testdata/compaction_picker_scores", func(t *testing.T, td *datadriven.TestData) string {
		switch td.Cmd {
		case "batch":
			b := d.NewBatch()
			err := runBatchDefineCmd(td, b)
			if err != nil {
				return err.Error()
			}
			if err = b.Commit(Sync); err != nil {
				return err.Error()
			}
			return ""

		case "define":
			if d != nil {
				require.NoError(t, closeAllSnapshots(d))
				require.NoError(t, d.Close())
			}

			if td.HasArg("pause-cleaning") {
				cleaner.pause()
			}
			opts := &Options{
				Cleaner:                     cleaner,
				Comparer:                    testkeys.Comparer,
				DisableAutomaticCompactions: true,
				FormatMajorVersion:          FormatNewest,
				Logger:                      testutils.Logger{T: t},
			}
			opts.Experimental.CompactionScheduler = func() CompactionScheduler {
				return NewConcurrencyLimitSchedulerWithNoPeriodicGrantingForTest()
			}
			var err error
			d, err = runDBDefineCmd(td, opts)
			if err != nil {
				return err.Error()
			}
			d.mu.Lock()
			s := d.mu.versions.currentVersion().String()
			d.mu.Unlock()
			return s

		case "disable-table-stats":
			d.mu.Lock()
			d.opts.DisableTableStats = true
			d.mu.Unlock()
			return ""

		case "enable-table-stats":
			d.mu.Lock()
			d.opts.DisableTableStats = false
			d.maybeCollectTableStatsLocked()
			d.mu.Unlock()
			return ""

		case "flush":
			if err := d.Flush(); err != nil {
				return err.Error()
			}
			return runLSMCmd(td, d)

		case "resume-cleaning":
			cleaner.resume()
			return ""

		case "ingest":
			if err := runBuildCmd(td, d, d.opts.FS); err != nil {
				return err.Error()
			}
			if err := runIngestCmd(td, d); err != nil {
				return err.Error()
			}
			d.mu.Lock()
			s := d.mu.versions.currentVersion().String()
			d.mu.Unlock()
			return s

		case "lsm":
			return runLSMCmd(td, d)

		case "maybe-compact":
			buf.Reset()
			d.mu.Lock()
			d.opts.DisableAutomaticCompactions = false
			d.maybeScheduleCompaction()
			if v := d.mu.compact.burstConcurrency.Load(); v > 0 {
				fmt.Fprintf(&buf, "%d burst concurrency active\n", v)
			}
			fmt.Fprintf(&buf, "%d compactions in progress:", d.mu.compact.compactingCount)

			runningCompactions := slices.SortedFunc(maps.Keys(d.mu.compact.inProgress), func(a, b compaction) int {
				return cmp.Compare(a.Info().String(), b.Info().String())
			})
			for _, c := range runningCompactions {
				fmt.Fprintf(&buf, "\n%s", c)
			}
			d.opts.DisableAutomaticCompactions = true
			d.mu.Unlock()
			return buf.String()

		case "scores":
			waitFor := "completion"
			td.MaybeScanArgs(t, "wait-for-compaction", &waitFor)

			// Wait for any running compactions to complete before calculating
			// scores. Otherwise, the output of this command is
			// nondeterministic.
			switch waitFor {
			case "completion":
				d.mu.Lock()
				for d.mu.compact.compactingCount > 0 {
					d.mu.compact.cond.Wait()
				}
				d.mu.Unlock()
			case "version-edit":
				func() {
					for {
						d.mu.Lock()
						wait := len(d.mu.compact.inProgress) > 0
						for c := range d.mu.compact.inProgress {
							wait = wait && !c.VersionEditApplied()
						}
						d.mu.Unlock()
						if !wait {
							return
						}
						// d.mu.compact.cond isn't notified until the compaction
						// is removed from inProgress, so we need to just sleep
						// and check again soon.
						time.Sleep(10 * time.Millisecond)
					}
				}()
			default:
				panic(fmt.Sprintf("unrecognized `wait-for-compaction` value: %q", waitFor))
			}

			buf.Reset()
			tw := tabwriter.NewWriter(&buf, 2, 1, 4, ' ', 0)
			fmt.Fprintf(tw, "Level\tSize\tScore\tFill factor\tCompensated fill factor\n")
			for l, lm := range d.Metrics().Levels {
				fmt.Fprintf(tw, "L%d\t%s\t%.2f\t%.2f\t%.2f\n", l, humanize.Bytes.Uint64(lm.AggregateSize()).String(),
					lm.Score, lm.FillFactor, lm.CompensatedFillFactor)
			}
			tw.Flush()
			return buf.String()

		case "wait-compactions":
			d.mu.Lock()
			for d.mu.compact.compactingCount > 0 {
				d.mu.compact.cond.Wait()
			}
			d.mu.Unlock()
			return ""

		case "wait-pending-table-stats":
			return runWaitForTableStatsCmd(td, d)

		default:
			return fmt.Sprintf("unknown command: %s", td.Cmd)
		}
	})
}

func tableNums(tables manifest.LevelSlice) string {
	var ss []string
	for f := range tables.All() {
		ss = append(ss, f.TableNum.String())
	}
	sort.Strings(ss)
	return strings.Join(ss, ",")
}

func checkClone(t *testing.T, pc *pickedTableCompaction) {
	pcClone := pc.clone()
	require.Equal(t, pc.String(), pcClone.String())

	// ensure all input files are in new address
	for i := range pc.inputs {
		// Len could be zero if setup inputs rejected a level
		if pc.inputs[i].files.Len() > 0 {
			require.NotEqual(t, &pc.inputs[i], &pcClone.inputs[i])
		}
	}
	for i := range pc.startLevel.l0SublevelInfo {
		if pc.startLevel.l0SublevelInfo[i].Len() > 0 {
			require.NotEqual(t, &pc.startLevel.l0SublevelInfo[i], &pcClone.startLevel.l0SublevelInfo[i])
		}
	}
}

func checkTableBoundary(a, b *manifest.TableMetadata, cmp base.Compare) (ok bool) {
	c := cmp(a.PointKeyBounds.LargestUserKey(), b.PointKeyBounds.SmallestUserKey())
	return c < 0 || (c == 0 && a.PointKeyBounds.Largest().IsExclusiveSentinel())
}

func parseTableNum(t *testing.T, s string) base.TableNum {
	n, err := strconv.Atoi(s)
	if err != nil {
		t.Fatalf("error parsing %q as table number: %v", s, err)
	}
	return base.TableNum(n)
}
