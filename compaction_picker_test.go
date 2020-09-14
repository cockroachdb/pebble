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

func loadVersion(d *datadriven.TestData) (*version, *Options, string) {
	opts := &Options{}
	opts.EnsureDefaults()

	if len(d.CmdArgs) != 1 {
		return nil, nil, fmt.Sprintf("%s expects 1 argument", d.Cmd)
	}
	var err error
	opts.LBaseMaxBytes, err = strconv.ParseInt(d.CmdArgs[0].Key, 10, 64)
	if err != nil {
		return nil, nil, err.Error()
	}

	var files [numLevels][]*fileMetadata
	if len(d.Input) > 0 {
		for _, data := range strings.Split(d.Input, "\n") {
			parts := strings.Split(data, ":")
			if len(parts) != 2 {
				return nil, nil, fmt.Sprintf("malformed test:\n%s", d.Input)
			}
			level, err := strconv.Atoi(parts[0])
			if err != nil {
				return nil, nil, err.Error()
			}
			if files[level] != nil {
				return nil, nil, fmt.Sprintf("level %d already filled", level)
			}
			size, err := strconv.ParseUint(strings.TrimSpace(parts[1]), 10, 64)
			if err != nil {
				return nil, nil, err.Error()
			}
			for i := uint64(1); i <= size; i++ {
				key := base.MakeInternalKey([]byte(fmt.Sprintf("%04d", i)), i, InternalKeyKindSet)
				files[level] = append(files[level], &fileMetadata{
					Smallest:       key,
					Largest:        key,
					SmallestSeqNum: key.SeqNum(),
					LargestSeqNum:  key.SeqNum(),
					Size:           1,
				})
				if size >= 100 {
					// If the requested size of the level is very large only add a single
					// file in order to avoid massive blow-up in the number of files in
					// the Version.
					//
					// TODO(peter): There is tension between the testing in
					// TestCompactionPickerLevelMaxBytes and
					// TestCompactionPickerTargetLevel. Clean this up somehow.
					files[level][len(files[level])-1].Size = size
					break
				}
			}
		}
	}

	vers := newVersion(opts, files)
	return vers, opts, ""
}

func TestCompactionPickerByScoreLevelMaxBytes(t *testing.T) {
	datadriven.RunTest(t, "testdata/compaction_picker_level_max_bytes",
		func(d *datadriven.TestData) string {
			switch d.Cmd {
			case "init":
				vers, opts, errMsg := loadVersion(d)
				if errMsg != "" {
					return errMsg
				}

				p, ok := newCompactionPicker(vers, opts, nil).(*compactionPickerByScore)
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
				vers, opts, errMsg = loadVersion(d)
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

				p := newCompactionPicker(vers, opts, inProgress)
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
				vers, opts, errMsg := loadVersion(d)
				if errMsg != "" {
					return errMsg
				}
				opts.MemTableSize = 1000

				p := newCompactionPicker(vers, opts, nil)
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
	fileNums := func(files manifest.LevelSlice) string {
		var ss []string
		files.Each(func(f *fileMetadata) {
			ss = append(ss, f.FileNum.String())
		})
		sort.Strings(ss)
		return strings.Join(ss, ",")
	}

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

		for _, field := range fields[1:] {
			switch field {
			case "intra_l0_compacting":
				m.IsIntraL0Compacting = true
				m.Compacting = true
			case "base_compacting":
				fallthrough
			case "compacting":
				m.Compacting = true
			}
		}
		return m, nil
	}

	opts := (*Options)(nil).EnsureDefaults()
	opts.Experimental.L0SublevelCompactions = true
	var picker *compactionPickerByScore

	datadriven.RunTest(t, "testdata/compaction_picker_L0", func(td *datadriven.TestData) string {
		switch td.Cmd {
		case "define":
			fileMetas := [manifest.NumLevels][]*fileMetadata{}
			baseLevel := manifest.NumLevels - 1
			level := 0
			var err error
			for _, data := range strings.Split(td.Input, "\n") {
				data = strings.TrimSpace(data)
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
					if level != 0 && level < baseLevel {
						baseLevel = level
					}
					fileMetas[level] = append(fileMetas[level], meta)
				}
			}

			version := newVersion(opts, fileMetas)
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
			inProgressCompactions := []compactionInfo{}
			for level, files := range version.Levels {
				files.Slice().Each(func(f *fileMetadata) {
					if f.Compacting {
						c := compactionInfo{
							inputs: []compactionLevel{
								{level: level},
								{level: level + 1, files: manifest.NewLevelSlice([]*fileMetadata{f})},
							},
							outputLevel: level + 1,
						}
						if f.IsIntraL0Compacting {
							c.outputLevel = c.inputs[0].level
						}
						inProgressCompactions = append(inProgressCompactions, c)
					}
				})
			}
			picker.initLevelMaxBytes(inProgressCompactions)
			return version.DebugString(base.DefaultFormatter)
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

			pc := picker.pickAuto(compactionEnv{
				bytesCompacted:          new(uint64),
				earliestUnflushedSeqNum: math.MaxUint64,
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
			Smallest: base.ParseInternalKey(strings.TrimSpace(tableParts[0])),
			Largest:  base.ParseInternalKey(strings.TrimSpace(tableParts[1])),
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
					return fmt.Sprintf("setup-inputs <start> <end>")
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
							return fmt.Sprintf("outputLevel already set\n")
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

				inputs, _ := expandToAtomicUnit(cmp, iter.Take().Slice())

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
