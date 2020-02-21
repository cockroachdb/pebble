// Copyright 2018 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package pebble

import (
	"bytes"
	"fmt"
	"strconv"
	"strings"
	"testing"

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
			if level == 0 {
				for i := uint64(0); i < size; i++ {
					vers.Files[level] = append(vers.Files[level], &fileMetadata{
						Size: 1,
					})
				}
			} else {
				vers.Files[level] = append(vers.Files[level], &fileMetadata{
					Size: size,
				})
			}
		}
	}

	return vers, opts, ""
}

func TestCompactionPickerLevelMaxBytes(t *testing.T) {
	datadriven.RunTest(t, "testdata/compaction_picker_level_max_bytes",
		func(d *datadriven.TestData) string {
			switch d.Cmd {
			case "init":
				vers, opts, errMsg := loadVersion(d)
				if errMsg != "" {
					return errMsg
				}

				p := newCompactionPicker(vers, opts, nil)
				var buf bytes.Buffer
				levelMaxBytes := p.getLevelMaxBytes()
				for level := p.getBaseLevel(); level < numLevels; level++ {
					fmt.Fprintf(&buf, "%d: %d\n", level, levelMaxBytes[level])
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
			return nil, fmt.Errorf("odd number of levels with ongoing compactions")
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

				for _, files := range vers.Files {
					for _, f := range files {
						f.Compacting = false
					}
				}
				return b.String()
			case "pick":
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

				c := pickerByScore.pickAuto(compactionEnv{
					bytesCompacted:          new(uint64),
					inProgressCompactions:   inProgress,
					earliestUnflushedSeqNum: InternalKeySeqNumMax,
				})
				if c == nil {
					return "no compaction"
				}
				return fmt.Sprintf("L%d->L%d", c.startLevel, c.outputLevel)
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
		var err error
		m.FileNum, err = strconv.ParseUint(s[:index], 10, 64)
		if err != nil {
			t.Fatal(err)
		}

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
		if err != nil {
			t.Fatal(err)
		}

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
