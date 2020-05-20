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

	vers := &version{}
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
			if vers.Files[level] != nil {
				return nil, nil, fmt.Sprintf("level %d already filled", level)
			}
			size, err := strconv.ParseUint(strings.TrimSpace(parts[1]), 10, 64)
			if err != nil {
				return nil, nil, err.Error()
			}
			for i := uint64(1); i <= size; i++ {
				key := base.MakeInternalKey([]byte(fmt.Sprintf("%04d", i)), i, InternalKeyKindSet)
				vers.Files[level] = append(vers.Files[level], &fileMetadata{
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
					vers.Files[level][len(vers.Files[level])-1].Size = size
					break
				}
			}
		}
	}

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
				startLevel:  levels[i],
				outputLevel: levels[i+1],
			})
		}
		return inProgress, nil
	}

	resetCompacting := func() {
		for _, files := range vers.Files {
			for _, f := range files {
				f.Compacting = false
			}
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
					c := pickerByScore.pickAuto(env)
					if c == nil {
						break
					}
					fmt.Fprintf(&b, "L%d->L%d: %.1f\n", c.startLevel, c.outputLevel, c.score)
					inProgress = append(inProgress, compactionInfo{
						startLevel:  c.startLevel,
						outputLevel: c.outputLevel,
						inputs:      c.inputs,
					})
					if c.outputLevel == 0 {
						// Once we pick one L0->L0 compaction, we'll keep on doing so
						// because the test isn't marking files as Compacting.
						break
					}
					for _, files := range c.inputs {
						for _, f := range files {
							f.Compacting = true
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

				// Mark files as compacting for each in-progress compaction.
				for i := range inProgress {
					c := &inProgress[i]
					for j, f := range vers.Files[c.startLevel] {
						if !f.Compacting {
							f.Compacting = true
							c.inputs[0] = vers.Files[c.startLevel][j : j+1]
							break
						}
					}

					switch {
					case c.startLevel == 0 && c.outputLevel != 0:
						// L0->Lbase: mark all of Lbase as compacting.
						c.inputs[1] = vers.Files[c.outputLevel]
						for _, f := range c.inputs[1] {
							f.Compacting = true
						}
					case c.startLevel != c.outputLevel:
						// Ln->Ln+1: mark 1 file in Ln+1 as compacting.
						for j, f := range vers.Files[c.outputLevel] {
							if !f.Compacting {
								f.Compacting = true
								c.inputs[1] = vers.Files[c.outputLevel][j : j+1]
								break
							}
						}
					}
				}

				c := pickerByScore.pickAuto(compactionEnv{
					earliestUnflushedSeqNum: InternalKeySeqNumMax,
					inProgressCompactions:   inProgress,
				})
				if c == nil {
					return "no compaction"
				}
				return fmt.Sprintf("L%d->L%d: %0.1f", c.startLevel, c.outputLevel, c.score)
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

				c, retryLater := pickerByScore.pickManual(compactionEnv{
					earliestUnflushedSeqNum: InternalKeySeqNumMax,
				}, manual)
				if c == nil {
					return fmt.Sprintf("nil, retryLater = %v", retryLater)
				}

				return fmt.Sprintf("L%d->L%d, retryLater = %v", c.startLevel, c.outputLevel, retryLater)
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

				c := pickIntraL0(env, opts, &version{
					Files: [7][]*fileMetadata{
						0: files,
					},
				})
				if c == nil {
					return "<nil>\n"
				}

				var buf bytes.Buffer
				for _, f := range c.inputs[0] {
					fmt.Fprintf(&buf, "%s\n", f)
				}
				return buf.String()

			default:
				return fmt.Sprintf("unknown command: %s", d.Cmd)
			}
		})
}

func TestCompactionPickerL0(t *testing.T) {
	fileNums := func(f []*fileMetadata) string {
		ss := make([]string, 0, len(f))
		for _, meta := range f {
			ss = append(ss, meta.FileNum.String())
		}
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
			fileMetas := [manifest.NumLevels][]*manifest.FileMetadata{}
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

			version := &version{
				Files: fileMetas,
			}
			if err := version.InitL0Sublevels(DefaultComparer.Compare, base.DefaultFormatter, 0); err != nil {
				t.Fatal(err)
			}

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
			for level, files := range version.Files {
				for _, f := range files {
					if f.Compacting {
						c := compactionInfo{
							startLevel:  level,
							outputLevel: level + 1,
							inputs:      [2][]*fileMetadata{{f}},
						}
						if f.IsIntraL0Compacting {
							c.outputLevel = c.startLevel
						}
						inProgressCompactions = append(inProgressCompactions, c)
					}
				}
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
			c := picker.pickAuto(compactionEnv{
				bytesCompacted:          new(uint64),
				earliestUnflushedSeqNum: math.MaxUint64,
			})
			var result strings.Builder
			if c != nil {
				fmt.Fprintf(&result, "L%d -> L%d\n", c.startLevel, c.outputLevel)
				fmt.Fprintf(&result, "L%d: %s\n", c.startLevel, fileNums(c.inputs[0]))
				if len(c.inputs[1]) > 0 {
					fmt.Fprintf(&result, "L%d: %s\n", c.outputLevel, fileNums(c.inputs[1]))
				}
				if len(c.grandparents) > 0 {
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
