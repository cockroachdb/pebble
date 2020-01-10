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

	"github.com/cockroachdb/pebble/internal/datadriven"
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
					vers.Files[level] = append(vers.Files[level], fileMetadata{
						Size: 1,
					})
				}
			} else {
				vers.Files[level] = append(vers.Files[level], fileMetadata{
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

type compactionInfoTesting struct {
	startLevel  int
	outputLevel int
}

func (c compactionInfoTesting) startLevelNum() int       { return c.startLevel }
func (c compactionInfoTesting) outputLevelNum() int      { return c.outputLevel }
func (c compactionInfoTesting) smallestKey() InternalKey { return InternalKey{} }
func (c compactionInfoTesting) largestKey() InternalKey  { return InternalKey{} }

func TestCompactionPickerTargetLevel(t *testing.T) {
	var vers *version
	var opts *Options
	var pickerByScore *compactionPickerByScore
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
				p := newCompactionPicker(vers, opts, nil)
				var ok bool
				pickerByScore, ok = p.(*compactionPickerByScore)
				require.True(t, ok)
				return ""
			case "queue":
				var b strings.Builder
				for _, c := range pickerByScore.compactionQueue {
					fmt.Fprintf(&b, "%d: %.1f,", c.level, c.score)
				}
				return b.String()
			case "pick":
				var levels []int
				if len(d.CmdArgs) == 1 {
					arg := d.CmdArgs[0]
					if arg.Key != "ongoing" {
						return "unknown arg: " + arg.Key
					}
					for _, s := range arg.Vals {
						l, err := strconv.ParseInt(s, 10, 8)
						if err != nil {
							return err.Error()
						}
						levels = append(levels, int(l))
					}
				}
				if len(levels)%2 != 0 {
					return "odd number of levels with ongoing compactions"
				}
				var inProgress []compactionInfo
				for i := 0; i < len(levels); i += 2 {
					inProgress = append(inProgress,
						compactionInfoTesting{startLevel: levels[i], outputLevel: levels[i+1]})
				}
				c := pickerByScore.pickAuto(opts, new(uint64), inProgress)
				if c == nil {
					return "no compaction"
				}
				return fmt.Sprintf("startLevel: %d, outputLevel: %d", c.startLevel, c.outputLevel)
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
