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

	"github.com/petermattis/pebble/internal/datadriven"
)

func load(d *datadriven.TestData) (*version, *Options, string) {
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
			if vers.files[level] != nil {
				return nil, nil, fmt.Sprintf("level %d already filled", level)
			}
			size, err := strconv.ParseUint(strings.TrimSpace(parts[1]), 10, 64)
			if err != nil {
				return nil, nil, err.Error()
			}
			if level == 0 {
				for i := uint64(0); i < size; i++ {
					vers.files[level] = append(vers.files[level], fileMetadata{
						size: 1,
					})
				}
			} else {
				vers.files[level] = append(vers.files[level], fileMetadata{
					size: size,
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
				vers, opts, errMsg := load(d)
				if errMsg != "" {
					return errMsg
				}

				p := newCompactionPicker(vers, opts)
				var buf bytes.Buffer
				for level := p.baseLevel; level < numLevels; level++ {
					fmt.Fprintf(&buf, "%d: %d\n", level, p.levelMaxBytes[level])
				}
				return buf.String()

			default:
				return fmt.Sprintf("unknown command: %s", d.Cmd)
			}
		})
}

func TestCompactionPickerTargetLevel(t *testing.T) {
	datadriven.RunTest(t, "testdata/compaction_picker_target_level",
		func(d *datadriven.TestData) string {
			switch d.Cmd {
			case "pick":
				vers, opts, errMsg := load(d)
				if errMsg != "" {
					return errMsg
				}

				p := newCompactionPicker(vers, opts)
				return fmt.Sprintf("%d: %.1f\n", p.level, p.score)

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
				vers, opts, errMsg := load(d)
				if errMsg != "" {
					return errMsg
				}
				opts.MemTableSize = 1000

				p := newCompactionPicker(vers, opts)
				return fmt.Sprintf("%d: %d\n",
					p.estimatedCompactionDebt(),
					p.compactionDebtMultiplier())

			default:
				return fmt.Sprintf("unknown command: %s", d.Cmd)
			}
		})
}
