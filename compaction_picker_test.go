// Copyright 2018 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package pebble

import (
	"bytes"
	"fmt"
	"math"
	"sort"
	"strconv"
	"strings"
	"testing"

	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/pebble/internal/base"
	"github.com/cockroachdb/pebble/internal/datadriven"
	"github.com/cockroachdb/pebble/internal/manifest"
	"github.com/stretchr/testify/require"
)

func loadVersion(d *datadriven.TestData) (*version, *Options, [numLevels]int64, string) {
	var sizes [numLevels]int64
	opts := &Options{}
	opts.EnsureDefaults()

	if len(d.CmdArgs) != 1 {
		return nil, nil, sizes, fmt.Sprintf("%s expects 1 argument", d.Cmd)
	}
	var err error
	opts.LBaseMaxBytes, err = strconv.ParseInt(d.CmdArgs[0].Key, 10, 64)
	if err != nil {
		return nil, nil, sizes, err.Error()
	}

	var files [numLevels][]*fileMetadata
	if len(d.Input) > 0 {
		for _, data := range strings.Split(d.Input, "\n") {
			parts := strings.Split(data, " ")
			parts[0] = strings.TrimSuffix(strings.TrimSpace(parts[0]), ":")
			if len(parts) < 2 {
				return nil, nil, sizes, fmt.Sprintf("malformed test:\n%s", d.Input)
			}
			level, err := strconv.Atoi(parts[0])
			if err != nil {
				return nil, nil, sizes, err.Error()
			}
			if files[level] != nil {
				return nil, nil, sizes, fmt.Sprintf("level %d already filled", level)
			}
			size, err := strconv.ParseUint(strings.TrimSpace(parts[1]), 10, 64)
			if err != nil {
				return nil, nil, sizes, err.Error()
			}
			for i := uint64(1); sizes[level] < int64(size); i++ {
				var key InternalKey
				if level == 0 {
					// For L0, make `size` overlapping files.
					key = base.MakeInternalKey([]byte(fmt.Sprintf("%04d", 1)), i, InternalKeyKindSet)
				} else {
					key = base.MakeInternalKey([]byte(fmt.Sprintf("%04d", i)), i, InternalKeyKindSet)
				}
				m := &fileMetadata{
					Smallest:       key,
					Largest:        key,
					SmallestSeqNum: key.SeqNum(),
					LargestSeqNum:  key.SeqNum(),
					Size:           1,
				}
				if size >= 100 {
					// If the requested size of the level is very large only add a single
					// file in order to avoid massive blow-up in the number of files in
					// the Version.
					//
					// TODO(peter): There is tension between the testing in
					// TestCompactionPickerLevelMaxBytes and
					// TestCompactionPickerTargetLevel. Clean this up somehow.
					m.Size = size
				}
				files[level] = append(files[level], m)
				sizes[level] += int64(m.Size)
			}
		}
	}

	vers := newVersion(opts, files)
	return vers, opts, sizes, ""
}

func TestCompactionPickerByScoreLevelMaxBytes(t *testing.T) {
	datadriven.RunTest(t, "testdata/compaction_picker_level_max_bytes",
		func(d *datadriven.TestData) string {
			switch d.Cmd {
			case "init":
				vers, opts, sizes, errMsg := loadVersion(d)
				if errMsg != "" {
					return errMsg
				}

				p, ok := newCompactionPicker(vers, opts, nil, sizes).(*compactionPickerByScore)
				require.True(t, ok)
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
	var vers *version
	var opts *Options
	var sizes [numLevels]int64
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
			files.Slice().Each(func(f *fileMetadata) {
				f.Compacting = false
			})
		}
	}

	datadriven.RunTest(t, "testdata/compaction_picker_target_level",
		func(d *datadriven.TestData) string {
			switch d.Cmd {
			case "init":
				var errMsg string
				vers, opts, sizes, errMsg = loadVersion(d)
				if errMsg != "" {
					return errMsg
				}
				return ""
			case "init_cp":
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

				p := newCompactionPicker(vers, opts, inProgress, sizes)
				var ok bool
				pickerByScore, ok = p.(*compactionPickerByScore)
				require.True(t, ok)
				return fmt.Sprintf("base: %d", pickerByScore.baseLevel)
			case "queue":
				var b strings.Builder
				var inProgress []compactionInfo
				for {
					env := compactionEnv{
						earliestUnflushedSeqNum: InternalKeySeqNumMax,
						inProgressCompactions:   inProgress,
					}
					pc := pickerByScore.pickAuto(env)
					if pc == nil {
						break
					}
					fmt.Fprintf(&b, "L%d->L%d: %.1f\n", pc.startLevel.level, pc.outputLevel.level, pc.score)
					inProgress = append(inProgress, compactionInfo{
						inputs:      pc.inputs,
						outputLevel: pc.outputLevel.level,
					})
					if pc.outputLevel.level == 0 {
						// Once we pick one L0->L0 compaction, we'll keep on doing so
						// because the test isn't marking files as Compacting.
						break
					}
					for _, cl := range pc.inputs {
						cl.files.Each(func(f *fileMetadata) {
							f.Compacting = true
						})
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

				// Mark files as compacting for each in-progress compaction.
				for i := range inProgress {
					c := &inProgress[i]
					iter := vers.Levels[c.inputs[0].level].Iter()
					for f := iter.First(); f != nil; f = iter.Next() {
						if !f.Compacting {
							f.Compacting = true
							c.inputs[0].files = iter.Take().Slice()
							break
						}
					}

					switch {
					case c.inputs[0].level == 0 && c.outputLevel != 0:
						// L0->Lbase: mark all of Lbase as compacting.
						c.inputs[1].files = vers.Levels[c.outputLevel].Slice()
						for _, in := range c.inputs {
							in.files.Each(func(f *fileMetadata) {
								f.Compacting = true
							})
						}
					case c.inputs[0].level != c.outputLevel:
						// Ln->Ln+1: mark 1 file in Ln+1 as compacting.
						iter := vers.Levels[c.outputLevel].Iter()
						for f := iter.First(); f != nil; f = iter.Next() {
							if !f.Compacting {
								f.Compacting = true
								c.inputs[1].files = iter.Take().Slice()
								break
							}
						}
					}
				}

				pc := pickerByScore.pickAuto(compactionEnv{
					earliestUnflushedSeqNum: InternalKeySeqNumMax,
					inProgressCompactions:   inProgress,
				})
				if pc == nil {
					return "no compaction"
				}
				return fmt.Sprintf("L%d->L%d: %0.1f", pc.startLevel.level, pc.outputLevel.level, pc.score)
			case "pick_manual":
				startLevel := 0
				start := ""
				end := ""

				if len(d.CmdArgs) > 0 {
					for _, arg := range d.CmdArgs {
						switch arg.Key {
						case "level":
							startLevel64, err := strconv.ParseInt(arg.Vals[0], 10, 64)
							if err != nil {
								return err.Error()
							}
							startLevel = int(startLevel64)
						case "start":
							start = arg.Vals[0]
						case "end":
							end = arg.Vals[0]
						default:
							return "unknown arg: " + arg.Key
						}
					}
				}

				iStart := base.MakeInternalKey([]byte(start), InternalKeySeqNumMax, InternalKeyKindMax)
				iEnd := base.MakeInternalKey([]byte(end), 0, 0)
				manual := &manualCompaction{
					done:  make(chan error, 1),
					level: startLevel,
					start: iStart,
					end:   iEnd,
				}

				pc, retryLater := pickerByScore.pickManual(compactionEnv{
					earliestUnflushedSeqNum: InternalKeySeqNumMax,
				}, manual)
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
		func(d *datadriven.TestData) string {
			switch d.Cmd {
			case "init":
				vers, opts, sizes, errMsg := loadVersion(d)
				if errMsg != "" {
					return errMsg
				}
				opts.MemTableSize = 1000

				p := newCompactionPicker(vers, opts, nil, sizes)
				return fmt.Sprintf("%d\n", p.estimatedCompactionDebt(0))

			default:
				return fmt.Sprintf("unknown command: %s", d.Cmd)
			}
		})
}

func TestCompactionPickerIntraL0(t *testing.T) {
	opts := &Options{}
	opts = opts.EnsureDefaults()
	var files []*fileMetadata

	parseMeta := func(s string) *fileMetadata {
		index := strings.Index(s, ":")
		if index == -1 {
			t.Fatalf("malformed table spec: %s", s)
		}
		m := &fileMetadata{}
		fn, err := strconv.ParseUint(s[:index], 10, 64)
		require.NoError(t, err)
		m.FileNum = FileNum(fn)

		fields := strings.Fields(s[index+1:])
		if len(fields) != 2 && len(fields) != 3 {
			t.Fatalf("malformed table spec: %s", s)
		}

		parts := strings.Split(fields[0], "-")
		if len(parts) != 2 {
			t.Fatalf("malformed table spec: %s", s)
		}
		m.Smallest = base.ParseInternalKey(strings.TrimSpace(parts[0]))
		m.Largest = base.ParseInternalKey(strings.TrimSpace(parts[1]))
		m.SmallestSeqNum = m.Smallest.SeqNum()
		m.LargestSeqNum = m.Largest.SeqNum()
		if m.SmallestSeqNum > m.LargestSeqNum {
			m.SmallestSeqNum, m.LargestSeqNum = m.LargestSeqNum, m.SmallestSeqNum
		}

		m.Size, err = strconv.ParseUint(fields[1], 10, 64)
		require.NoError(t, err)

		if len(fields) == 3 {
			if fields[2] != "compacting" {
				t.Fatalf("malformed table spec: %s", s)
			}
			m.Compacting = true
		}

		return m
	}

	datadriven.RunTest(t, "testdata/compaction_picker_intra_L0",
		func(d *datadriven.TestData) string {
			switch d.Cmd {
			case "define":
				files = nil
				if len(d.Input) == 0 {
					return ""
				}
				for _, data := range strings.Split(d.Input, "\n") {
					files = append(files, parseMeta(data))
				}
				manifest.SortBySeqNum(files)
				return ""

			case "pick-intra-L0":
				env := compactionEnv{}
				env.earliestUnflushedSeqNum = InternalKeySeqNumMax

				if len(d.CmdArgs) == 1 {
					if d.CmdArgs[0].Key != "earliest-unflushed" {
						return fmt.Sprintf("unknown argument: %s", d.CmdArgs[0])
					}
					if len(d.CmdArgs[0].Vals) != 1 {
						return fmt.Sprintf("%s expects 1 value: %s", d.CmdArgs[0].Key, d.CmdArgs[0])
					}
					var err error
					env.earliestUnflushedSeqNum, err = strconv.ParseUint(d.CmdArgs[0].Vals[0], 10, 64)
					if err != nil {
						return err.Error()
					}
				}

				c := pickIntraL0(env, opts, newVersion(opts, [numLevels][]*fileMetadata{0: files}))
				if c == nil {
					return "<nil>\n"
				}

				var buf bytes.Buffer
				c.startLevel.files.Each(func(f *fileMetadata) {
					fmt.Fprintf(&buf, "%s\n", f)
				})
				return buf.String()

			default:
				return fmt.Sprintf("unknown command: %s", d.Cmd)
			}
		})
}

func TestCompactionPickerL0(t *testing.T) {
	parseMeta := func(s string) (*fileMetadata, error) {
		parts := strings.Split(s, ":")
		fileNum, err := strconv.Atoi(parts[0])
		if err != nil {
			return nil, err
		}
		fields := strings.Fields(parts[1])
		parts = strings.Split(fields[0], "-")
		if len(parts) != 2 {
			return nil, errors.Errorf("malformed table spec: %s", s)
		}
		m := &fileMetadata{
			FileNum:  base.FileNum(fileNum),
			Smallest: base.ParseInternalKey(strings.TrimSpace(parts[0])),
			Largest:  base.ParseInternalKey(strings.TrimSpace(parts[1])),
		}
		m.SmallestSeqNum = m.Smallest.SeqNum()
		m.LargestSeqNum = m.Largest.SeqNum()
		return m, nil
	}

	opts := (*Options)(nil).EnsureDefaults()
	opts.Experimental.L0CompactionConcurrency = 1
	var picker *compactionPickerByScore
	var inProgressCompactions []compactionInfo
	var pc *pickedCompaction

	datadriven.RunTest(t, "testdata/compaction_picker_L0", func(td *datadriven.TestData) string {
		switch td.Cmd {
		case "define":
			fileMetas := [manifest.NumLevels][]*fileMetadata{}
			baseLevel := manifest.NumLevels - 1
			level := 0
			var err error
			lines := strings.Split(td.Input, "\n")
			var compactionLines []string

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
					compactionLines, lines = lines, nil
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

			// Parse in-progress compactions in the form of:
			//   L0 000001 -> L2 000005
			inProgressCompactions = nil
			for len(compactionLines) > 0 {
				parts := strings.Fields(compactionLines[0])
				compactionLines = compactionLines[1:]

				var level int
				var info compactionInfo
				first := true
				compactionFiles := map[int][]*fileMetadata{}
				for _, p := range parts {
					switch p {
					case "L0", "L1", "L2", "L3", "L4", "L5", "L6":
						var err error
						level, err = strconv.Atoi(p[1:])
						if err != nil {
							return err.Error()
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
						fileNum, err := strconv.Atoi(p)
						if err != nil {
							return err.Error()
						}
						var compactFile *fileMetadata
						for _, m := range fileMetas[level] {
							if m.FileNum == FileNum(fileNum) {
								compactFile = m
							}
						}
						if compactFile == nil {
							return fmt.Sprintf("cannot find compaction file %s", FileNum(fileNum))
						}
						compactFile.Compacting = true
						if first || base.InternalCompare(DefaultComparer.Compare, info.largest, compactFile.Largest) < 0 {
							info.largest = compactFile.Largest
						}
						if first || base.InternalCompare(DefaultComparer.Compare, info.smallest, compactFile.Smallest) > 0 {
							info.smallest = compactFile.Smallest
						}
						first = false
						compactionFiles[level] = append(compactionFiles[level], compactFile)
					}
				}
				for i, cl := range info.inputs {
					files := compactionFiles[cl.level]
					info.inputs[i].files = manifest.NewLevelSliceSeqSorted(files)
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

			version := newVersion(opts, fileMetas)
			version.L0Sublevels.InitCompactingFileInfo(inProgressL0Compactions(inProgressCompactions))
			vs := &versionSet{
				opts:    opts,
				cmp:     DefaultComparer.Compare,
				cmpName: DefaultComparer.Name,
			}
			vs.versions.Init(nil)
			vs.append(version)
			picker = &compactionPickerByScore{
				opts:      opts,
				vers:      version,
				baseLevel: baseLevel,
			}
			vs.picker = picker
			for l := 0; l < len(picker.levelSizes); l++ {
				version.Levels[l].Slice().Each(func(m *fileMetadata) {
					picker.levelSizes[l] += int64(m.Size)
				})
			}
			picker.initLevelMaxBytes(inProgressCompactions)

			var buf bytes.Buffer
			fmt.Fprint(&buf, version.DebugString(base.DefaultFormatter))
			if len(inProgressCompactions) > 0 {
				fmt.Fprintln(&buf, "compactions")
				for _, c := range inProgressCompactions {
					fmt.Fprintf(&buf, "  %s\n", c.String())
				}
			}
			return buf.String()
		case "pick-auto":
			for _, arg := range td.CmdArgs {
				var err error
				switch arg.Key {
				case "l0_compaction_threshold":
					opts.L0CompactionThreshold, err = strconv.Atoi(arg.Vals[0])
					if err != nil {
						return err.Error()
					}
				}
			}

			pc = picker.pickAuto(compactionEnv{
				bytesCompacted:          new(uint64),
				earliestUnflushedSeqNum: math.MaxUint64,
				inProgressCompactions:   inProgressCompactions,
			})
			var result strings.Builder
			if pc != nil {
				c := newCompaction(pc, opts, new(uint64))
				fmt.Fprintf(&result, "L%d -> L%d\n", pc.startLevel.level, pc.outputLevel.level)
				fmt.Fprintf(&result, "L%d: %s\n", pc.startLevel.level, fileNums(pc.startLevel.files))
				if !pc.outputLevel.files.Empty() {
					fmt.Fprintf(&result, "L%d: %s\n", pc.outputLevel.level, fileNums(pc.outputLevel.files))
				}
				if !c.grandparents.Empty() {
					fmt.Fprintf(&result, "grandparents: %s\n", fileNums(c.grandparents))
				}
			} else {
				return "nil"
			}
			return result.String()
		case "max-output-file-size":
			if pc == nil {
				return "no compaction"
			}
			return fmt.Sprintf("%d", pc.maxOutputFileSize)
		case "max-overlap-bytes":
			if pc == nil {
				return "no compaction"
			}
			return fmt.Sprintf("%d", pc.maxOverlapBytes)
		case "max-expanded-bytes":
			if pc == nil {
				return "no compaction"
			}
			return fmt.Sprintf("%d", pc.maxExpandedBytes)
		}
		return fmt.Sprintf("unrecognized command: %s", td.Cmd)
	})
}

func TestCompactionPickerConcurrency(t *testing.T) {
	parseMeta := func(s string) (*fileMetadata, error) {
		parts := strings.Split(s, ":")
		fileNum, err := strconv.Atoi(parts[0])
		if err != nil {
			return nil, err
		}
		fields := strings.Fields(parts[1])
		parts = strings.Split(fields[0], "-")
		if len(parts) != 2 {
			return nil, errors.Errorf("malformed table spec: %s", s)
		}
		m := &fileMetadata{
			FileNum:  base.FileNum(fileNum),
			Size:     1028,
			Smallest: base.ParseInternalKey(strings.TrimSpace(parts[0])),
			Largest:  base.ParseInternalKey(strings.TrimSpace(parts[1])),
		}
		for _, p := range fields[1:] {
			if strings.HasPrefix(p, "size=") {
				v, err := strconv.Atoi(strings.TrimPrefix(p, "size="))
				if err != nil {
					return nil, err
				}
				m.Size = uint64(v)
			}
		}
		m.SmallestSeqNum = m.Smallest.SeqNum()
		m.LargestSeqNum = m.Largest.SeqNum()
		return m, nil
	}

	opts := (*Options)(nil).EnsureDefaults()
	opts.Experimental.L0CompactionConcurrency = 1
	var picker *compactionPickerByScore
	var inProgressCompactions []compactionInfo

	datadriven.RunTest(t, "testdata/compaction_picker_concurrency", func(td *datadriven.TestData) string {
		switch td.Cmd {
		case "define":
			fileMetas := [manifest.NumLevels][]*fileMetadata{}
			level := 0
			var err error
			lines := strings.Split(td.Input, "\n")
			var compactionLines []string

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
					compactionLines, lines = lines, nil
				default:
					meta, err := parseMeta(data)
					if err != nil {
						return err.Error()
					}
					fileMetas[level] = append(fileMetas[level], meta)
				}
			}

			// Parse in-progress compactions in the form of:
			//   L0 000001 -> L2 000005
			inProgressCompactions = nil
			for len(compactionLines) > 0 {
				parts := strings.Fields(compactionLines[0])
				compactionLines = compactionLines[1:]

				var level int
				var info compactionInfo
				first := true
				compactionFiles := map[int][]*fileMetadata{}
				for _, p := range parts {
					switch p {
					case "L0", "L1", "L2", "L3", "L4", "L5", "L6":
						var err error
						level, err = strconv.Atoi(p[1:])
						if err != nil {
							return err.Error()
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
						fileNum, err := strconv.Atoi(p)
						if err != nil {
							return err.Error()
						}
						var compactFile *fileMetadata
						for _, m := range fileMetas[level] {
							if m.FileNum == FileNum(fileNum) {
								compactFile = m
							}
						}
						if compactFile == nil {
							return fmt.Sprintf("cannot find compaction file %s", FileNum(fileNum))
						}
						compactFile.Compacting = true
						if first || base.InternalCompare(DefaultComparer.Compare, info.largest, compactFile.Largest) < 0 {
							info.largest = compactFile.Largest
						}
						if first || base.InternalCompare(DefaultComparer.Compare, info.smallest, compactFile.Smallest) > 0 {
							info.smallest = compactFile.Smallest
						}
						first = false
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

			version := newVersion(opts, fileMetas)
			version.L0Sublevels.InitCompactingFileInfo(inProgressL0Compactions(inProgressCompactions))
			vs := &versionSet{
				opts:    opts,
				cmp:     DefaultComparer.Compare,
				cmpName: DefaultComparer.Name,
			}
			vs.versions.Init(nil)
			vs.append(version)
			var sizes [numLevels]int64
			for l := 0; l < len(sizes); l++ {
				version.Levels[l].Slice().Each(func(m *fileMetadata) {
					sizes[l] += int64(m.Size)
				})
			}
			picker = newCompactionPicker(version, opts, inProgressCompactions, sizes).(*compactionPickerByScore)
			vs.picker = picker

			var buf bytes.Buffer
			fmt.Fprint(&buf, version.DebugString(base.DefaultFormatter))
			if len(inProgressCompactions) > 0 {
				fmt.Fprintln(&buf, "compactions")
				for _, c := range inProgressCompactions {
					fmt.Fprintf(&buf, "  %s\n", c.String())
				}
			}
			return buf.String()

		case "pick-auto":
			for _, arg := range td.CmdArgs {
				var err error
				switch arg.Key {
				case "l0_compaction_threshold":
					opts.L0CompactionThreshold, err = strconv.Atoi(arg.Vals[0])
					if err != nil {
						return err.Error()
					}
				case "l0_compaction_concurrency":
					opts.Experimental.L0CompactionConcurrency, err = strconv.Atoi(arg.Vals[0])
					if err != nil {
						return err.Error()
					}
				case "compaction_debt_concurrency":
					opts.Experimental.CompactionDebtConcurrency, err = strconv.Atoi(arg.Vals[0])
					if err != nil {
						return err.Error()
					}
				}
			}

			pc := picker.pickAuto(compactionEnv{
				bytesCompacted:          new(uint64),
				earliestUnflushedSeqNum: math.MaxUint64,
				inProgressCompactions:   inProgressCompactions,
			})
			var result strings.Builder
			if pc != nil {
				c := newCompaction(pc, opts, new(uint64))
				fmt.Fprintf(&result, "L%d -> L%d\n", pc.startLevel.level, pc.outputLevel.level)
				fmt.Fprintf(&result, "L%d: %s\n", pc.startLevel.level, fileNums(pc.startLevel.files))
				if !pc.outputLevel.files.Empty() {
					fmt.Fprintf(&result, "L%d: %s\n", pc.outputLevel.level, fileNums(pc.outputLevel.files))
				}
				if !c.grandparents.Empty() {
					fmt.Fprintf(&result, "grandparents: %s\n", fileNums(c.grandparents))
				}
			} else {
				return "nil"
			}
			return result.String()
		}
		return fmt.Sprintf("unrecognized command: %s", td.Cmd)
	})
}

func TestCompactionPickerPickReadTriggered(t *testing.T) {
	opts := (*Options)(nil).EnsureDefaults()
	var picker *compactionPickerByScore
	var rcList []readCompaction
	var vers *version

	parseMeta := func(s string) (*fileMetadata, error) {
		parts := strings.Split(s, ":")
		fileNum, err := strconv.Atoi(parts[0])
		if err != nil {
			return nil, err
		}
		fields := strings.Fields(parts[1])
		parts = strings.Split(fields[0], "-")
		if len(parts) != 2 {
			return nil, errors.Errorf("malformed table spec: %s. usage: <file-num>:start.SET.1-end.SET.2", s)
		}
		m := &fileMetadata{
			FileNum:  base.FileNum(fileNum),
			Size:     1028,
			Smallest: base.ParseInternalKey(strings.TrimSpace(parts[0])),
			Largest:  base.ParseInternalKey(strings.TrimSpace(parts[1])),
		}
		for _, p := range fields[1:] {
			if strings.HasPrefix(p, "size=") {
				v, err := strconv.Atoi(strings.TrimPrefix(p, "size="))
				if err != nil {
					return nil, err
				}
				m.Size = uint64(v)
			}
		}
		m.SmallestSeqNum = m.Smallest.SeqNum()
		m.LargestSeqNum = m.Largest.SeqNum()
		return m, nil
	}

	datadriven.RunTest(t, "testdata/compaction_picker_read_triggered", func(td *datadriven.TestData) string {
		switch td.Cmd {
		case "define":
			rcList = []readCompaction{}
			fileMetas := [manifest.NumLevels][]*fileMetadata{}
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
					meta, err := parseMeta(data)
					if err != nil {
						return err.Error()
					}
					fileMetas[level] = append(fileMetas[level], meta)
				}
			}

			vers = newVersion(opts, fileMetas)
			vs := &versionSet{
				opts:    opts,
				cmp:     DefaultComparer.Compare,
				cmpName: DefaultComparer.Name,
			}
			vs.versions.Init(nil)
			vs.append(vers)
			var sizes [numLevels]int64
			for l := 0; l < len(sizes); l++ {
				vers.Levels[l].Slice().Each(func(m *fileMetadata) {
					sizes[l] += int64(m.Size)
				})
			}
			var inProgressCompactions []compactionInfo
			picker = newCompactionPicker(vers, opts, inProgressCompactions, sizes).(*compactionPickerByScore)
			vs.picker = picker

			var buf bytes.Buffer
			fmt.Fprint(&buf, vers.DebugString(base.DefaultFormatter))
			return buf.String()

		case "add-read-compaction":
			for _, line := range strings.Split(td.Input, "\n") {
				if line == "" {
					continue
				}
				parts := strings.Split(line, " ")
				if len(parts) != 2 {
					return "error: malformed data for add-read-compaction. usage: <level>: <start>-<end>"
				}
				if l, err := strconv.Atoi(parts[0][:1]); err == nil {
					keys := strings.Split(parts[1], "-")

					rc := readCompaction{
						level: l,
						start: []byte(keys[0]),
						end:   []byte(keys[1]),
					}
					rcList = append(rcList, rc)
				} else {
					return err.Error()
				}
			}
			return ""

		case "show-read-compactions":
			var sb strings.Builder
			if len(rcList) == 0 {
				sb.WriteString("(none)")
			}
			for _, rc := range rcList {
				sb.WriteString(fmt.Sprintf("(level: %d, start: %s, end: %s)\n", rc.level, string(rc.start), string(rc.end)))
			}
			return sb.String()

		case "pick-auto":
			pc := picker.pickAuto(compactionEnv{
				bytesCompacted:          new(uint64),
				earliestUnflushedSeqNum: math.MaxUint64,
				readCompactionEnv: readCompactionEnv{
					readCompactions: &rcList,
					flushing:        false,
				},
			})
			var result strings.Builder
			if pc != nil {
				fmt.Fprintf(&result, "L%d -> L%d\n", pc.startLevel.level, pc.outputLevel.level)
				fmt.Fprintf(&result, "L%d: %s\n", pc.startLevel.level, fileNums(pc.startLevel.files))
				if !pc.outputLevel.files.Empty() {
					fmt.Fprintf(&result, "L%d: %s\n", pc.outputLevel.level, fileNums(pc.outputLevel.files))
				}
			} else {
				return "nil"
			}
			return result.String()
		}
		return fmt.Sprintf("unrecognized command: %s", td.Cmd)
	})
}

func TestPickedCompactionSetupInputs(t *testing.T) {
	opts := &Options{}
	opts.EnsureDefaults()
	parseMeta := func(s string) *fileMetadata {
		parts := strings.Split(strings.TrimSpace(s), " ")
		compacting := false
		if len(parts) == 2 {
			compacting = parts[1] == "compacting"
		}
		tableParts := strings.Split(parts[0], "-")
		if len(tableParts) != 2 {
			t.Fatalf("malformed table spec: %s", s)
		}
		m := &fileMetadata{
			Smallest:   base.ParseInternalKey(strings.TrimSpace(tableParts[0])),
			Largest:    base.ParseInternalKey(strings.TrimSpace(tableParts[1])),
			Compacting: compacting,
		}
		m.SmallestSeqNum = m.Smallest.SeqNum()
		m.LargestSeqNum = m.Largest.SeqNum()
		return m
	}

	datadriven.RunTest(t, "testdata/compaction_setup_inputs",
		func(d *datadriven.TestData) string {
			switch d.Cmd {
			case "setup-inputs":
				if len(d.CmdArgs) != 2 {
					return "setup-inputs <start> <end>"
				}

				pc := &pickedCompaction{
					cmp:              DefaultComparer.Compare,
					inputs:           []compactionLevel{{level: -1}, {level: -1}},
					maxExpandedBytes: 1 << 30,
				}
				pc.startLevel, pc.outputLevel = &pc.inputs[0], &pc.inputs[1]
				var currentLevel int
				var files [numLevels][]*fileMetadata
				fileNum := FileNum(1)

				for _, data := range strings.Split(d.Input, "\n") {
					switch data {
					case "L0", "L1", "L2", "L3", "L4", "L5", "L6":
						level, err := strconv.Atoi(data[1:])
						if err != nil {
							return err.Error()
						}
						if pc.startLevel.level == -1 {
							pc.startLevel.level = level
							currentLevel = level
						} else if pc.outputLevel.level == -1 {
							if pc.startLevel.level >= level {
								return fmt.Sprintf("startLevel=%d >= outputLevel=%d\n", pc.startLevel.level, level)
							}
							pc.outputLevel.level = level
							currentLevel = level
						} else {
							return "outputLevel already set\n"
						}

					default:
						meta := parseMeta(data)
						meta.FileNum = fileNum
						fileNum++
						files[currentLevel] = append(files[currentLevel], meta)
					}
				}

				if pc.outputLevel.level == -1 {
					pc.outputLevel.level = pc.startLevel.level + 1
				}
				pc.version = newVersion(opts, files)
				pc.startLevel.files = pc.version.Overlaps(pc.startLevel.level, pc.cmp,
					[]byte(d.CmdArgs[0].String()), []byte(d.CmdArgs[1].String()))

				var isCompacting bool
				if !pc.setupInputs() {
					isCompacting = true
				}

				var buf bytes.Buffer
				for _, cl := range pc.inputs {
					if cl.files.Empty() {
						continue
					}

					fmt.Fprintf(&buf, "L%d\n", cl.level)
					cl.files.Each(func(f *fileMetadata) {
						fmt.Fprintf(&buf, "  %s\n", f)
					})
				}
				if isCompacting {
					fmt.Fprintf(&buf, "is-compacting")
				}
				return buf.String()

			default:
				return fmt.Sprintf("unknown command: %s", d.Cmd)
			}
		})
}

func TestPickedCompactionExpandInputs(t *testing.T) {
	opts := &Options{}
	opts.EnsureDefaults()
	cmp := DefaultComparer.Compare
	var files []*fileMetadata

	parseMeta := func(s string) *fileMetadata {
		parts := strings.Split(s, "-")
		if len(parts) != 2 {
			t.Fatalf("malformed table spec: %s", s)
		}
		return &fileMetadata{
			Smallest: base.ParseInternalKey(parts[0]),
			Largest:  base.ParseInternalKey(parts[1]),
		}
	}

	datadriven.RunTest(t, "testdata/compaction_expand_inputs",
		func(d *datadriven.TestData) string {
			switch d.Cmd {
			case "define":
				files = nil
				if len(d.Input) == 0 {
					return ""
				}
				for _, data := range strings.Split(d.Input, "\n") {
					meta := parseMeta(data)
					meta.FileNum = FileNum(len(files))
					files = append(files, meta)
				}
				manifest.SortBySmallest(files, cmp)
				return ""

			case "expand-inputs":
				pc := &pickedCompaction{
					cmp:    cmp,
					inputs: []compactionLevel{{level: 1}},
				}
				pc.startLevel = &pc.inputs[0]

				var filesLevelled [numLevels][]*fileMetadata
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

				inputs, _ := expandToAtomicUnit(cmp, iter.Take().Slice(), true /* disableIsCompacting */)

				var buf bytes.Buffer
				inputs.Each(func(f *fileMetadata) {
					fmt.Fprintf(&buf, "%d: %s-%s\n", f.FileNum, f.Smallest, f.Largest)
				})
				return buf.String()

			default:
				return fmt.Sprintf("unknown command: %s", d.Cmd)
			}
		})
}

func TestCompactionOutputFileSize(t *testing.T) {
	opts := (*Options)(nil).EnsureDefaults()
	var picker *compactionPickerByScore
	var vers *version

	parseMeta := func(s string) (*fileMetadata, error) {
		parts := strings.Split(s, ":")
		fileNum, err := strconv.Atoi(parts[0])
		if err != nil {
			return nil, err
		}
		fields := strings.Fields(parts[1])
		parts = strings.Split(fields[0], "-")
		if len(parts) != 2 {
			return nil, errors.Errorf("malformed table spec: %s. usage: <file-num>:start.SET.1-end.SET.2", s)
		}
		m := &fileMetadata{
			FileNum:  base.FileNum(fileNum),
			Size:     1028,
			Smallest: base.ParseInternalKey(strings.TrimSpace(parts[0])),
			Largest:  base.ParseInternalKey(strings.TrimSpace(parts[1])),
		}
		for _, p := range fields[1:] {
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
				m.Stats.Valid = true
				m.Stats.RangeDeletionsBytesEstimate = uint64(v)
			}
		}
		m.SmallestSeqNum = m.Smallest.SeqNum()
		m.LargestSeqNum = m.Largest.SeqNum()
		return m, nil
	}

	datadriven.RunTest(t, "testdata/compaction_output_file_size", func(td *datadriven.TestData) string {
		switch td.Cmd {
		case "define":
			fileMetas := [manifest.NumLevels][]*fileMetadata{}
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
					meta, err := parseMeta(data)
					if err != nil {
						return err.Error()
					}
					fileMetas[level] = append(fileMetas[level], meta)
				}
			}

			vers = newVersion(opts, fileMetas)
			vs := &versionSet{
				opts:    opts,
				cmp:     DefaultComparer.Compare,
				cmpName: DefaultComparer.Name,
			}
			vs.versions.Init(nil)
			vs.append(vers)
			var sizes [numLevels]int64
			for l := 0; l < len(sizes); l++ {
				slice := vers.Levels[l].Slice()
				sizes[l] = int64(slice.SizeSum())
			}
			var inProgressCompactions []compactionInfo
			picker = newCompactionPicker(vers, opts, inProgressCompactions, sizes).(*compactionPickerByScore)
			vs.picker = picker

			var buf bytes.Buffer
			fmt.Fprint(&buf, vers.DebugString(base.DefaultFormatter))
			return buf.String()

		case "pick-auto":
			pc := picker.pickAuto(compactionEnv{
				bytesCompacted:          new(uint64),
				earliestUnflushedSeqNum: math.MaxUint64,
				earliestSnapshotSeqNum:  math.MaxUint64,
			})
			var buf bytes.Buffer
			if pc != nil {
				fmt.Fprintf(&buf, "L%d -> L%d\n", pc.startLevel.level, pc.outputLevel.level)
				fmt.Fprintf(&buf, "L%d: %s\n", pc.startLevel.level, fileNums(pc.startLevel.files))
				fmt.Fprintf(&buf, "maxOutputFileSize: %d\n", pc.maxOutputFileSize)
			} else {
				return "nil"
			}
			return buf.String()

		default:
			return fmt.Sprintf("unrecognized command: %s", td.Cmd)
		}
	})
}

func fileNums(files manifest.LevelSlice) string {
	var ss []string
	files.Each(func(f *fileMetadata) {
		ss = append(ss, f.FileNum.String())
	})
	sort.Strings(ss)
	return strings.Join(ss, ",")
}
