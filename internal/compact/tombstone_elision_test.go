// Copyright 2024 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package compact

import (
	"bytes"
	"fmt"
	"strconv"
	"strings"
	"testing"

	"github.com/cockroachdb/crlib/crstrings"
	"github.com/cockroachdb/datadriven"
	"github.com/cockroachdb/pebble/internal/base"
	"github.com/cockroachdb/pebble/internal/manifest"
	"github.com/cockroachdb/pebble/internal/testkeys"
)

func TestTombstoneElider(t *testing.T) {
	var elision TombstoneElision
	toStr := map[bool]string{true: "elide", false: "don't elide"}
	datadriven.RunTest(t, "testdata/tombstone_elider", func(t *testing.T, d *datadriven.TestData) string {
		switch d.Cmd {
		case "init":
			if len(d.CmdArgs) != 1 {
				d.Fatalf(t, "init requires one arg: elide-nothing or in-use-ranges")
			}
			switch arg := d.CmdArgs[0].Key; arg {
			case "elide-nothing":
				elision = NoTombstoneElision()

			case "in-use-ranges":
				var inUseRanges []base.UserKeyBounds
				if d.Input != "" {
					for line := range crstrings.LinesSeq(d.Input) {
						fields := strings.FieldsFunc(line, func(r rune) bool { return r == '-' })
						if len(fields) != 2 {
							d.Fatalf(t, "invalid range %q", line)
						}
						inUseRanges = append(inUseRanges, base.UserKeyBoundsEndExclusive([]byte(fields[0]), []byte(fields[1])))
					}
				}
				elision = ElideTombstonesOutsideOf(inUseRanges)

			default:
				d.Fatalf(t, "unknown arg: %s", arg)
			}

		case "points":
			var e pointTombstoneElider
			e.Init(base.DefaultComparer.Compare, elision)
			var results []string
			for line := range crstrings.LinesSeq(d.Input) {
				fields := strings.FieldsFunc(line, func(r rune) bool { return r == '-' })
				if len(fields) != 1 {
					d.Fatalf(t, "invalid point %q", line)
				}
				key := []byte(fields[0])
				results = append(results, fmt.Sprintf("%s: %s", key, toStr[e.ShouldElide(key)]))
			}
			return strings.Join(results, "\n")

		case "ranges":
			var e rangeTombstoneElider
			e.Init(base.DefaultComparer.Compare, elision)
			var results []string
			for line := range crstrings.LinesSeq(d.Input) {
				fields := strings.FieldsFunc(line, func(r rune) bool { return r == '-' })
				if len(fields) != 2 {
					d.Fatalf(t, "invalid range %q", line)
				}
				start := []byte(fields[0])
				end := []byte(fields[1])
				results = append(results, fmt.Sprintf("%s-%s: %s", start, end, toStr[e.ShouldElide(start, end)]))
			}
			return strings.Join(results, "\n")

		default:
			d.Fatalf(t, "unknown command: %s", d.Cmd)
		}
		return ""
	})
}

func TestSetupTombstoneElision(t *testing.T) {
	var v *manifest.Version
	var l0Organizer *manifest.L0Organizer
	datadriven.RunTest(t, "testdata/tombstone_elision_setup", func(t *testing.T, td *datadriven.TestData) string {
		switch td.Cmd {
		case "define":
			var err error
			l0Organizer = manifest.NewL0Organizer(base.DefaultComparer, 64*1024 /* flushSplitBytes */)
			v, err = manifest.ParseVersionDebug(base.DefaultComparer, l0Organizer, td.Input)
			if err != nil {
				td.Fatalf(t, "%v", err)
			}
			if err := v.CheckOrdering(); err != nil {
				td.Fatalf(t, "%v", err)
			}
			return v.String()

		case "inuse-key-ranges":
			var buf bytes.Buffer
			for line := range crstrings.LinesSeq(td.Input) {
				parts := strings.Fields(line)
				if len(parts) != 3 {
					fmt.Fprintf(&buf, "expected <output-level> <smallest> <largest>: %q\n", line)
					continue
				}
				outputLevel, err := strconv.Atoi(strings.TrimPrefix(parts[0], "L"))
				if err != nil {
					fmt.Fprintf(&buf, "expected <output-level> <smallest> <largest>: %q: %v\n", line, err)
					continue
				}
				bounds := base.UserKeyBoundsInclusive([]byte(parts[1]), []byte(parts[2]))
				res, _ := SetupTombstoneElision(base.DefaultComparer.Compare, v, l0Organizer, outputLevel, bounds)
				fmt.Fprintf(&buf, "L%d %s: %s\n", outputLevel, bounds, res)
			}
			return buf.String()

		default:
			return fmt.Sprintf("unknown command: %s", td.Cmd)
		}
	})
}

func TestTombstoneElision(t *testing.T) {
	var v *manifest.Version
	var l0Organizer *manifest.L0Organizer
	datadriven.RunTest(t, "testdata/tombstone_elision", func(t *testing.T, td *datadriven.TestData) string {
		switch td.Cmd {
		case "define":
			var err error
			l0Organizer = manifest.NewL0Organizer(base.DefaultComparer, 64*1024 /* flushSplitBytes */)
			v, err = manifest.ParseVersionDebug(base.DefaultComparer, l0Organizer, td.Input)
			if err != nil {
				td.Fatalf(t, "%v", err)
			}
			if err := v.CheckOrdering(); err != nil {
				td.Fatalf(t, "%v", err)
			}
			if td.HasArg("verbose") {
				return v.DebugString()
			}
			return v.String()
		case "elide":
			var startLevel int
			td.ScanArgs(t, "start-level", &startLevel)
			del, _ := SetupTombstoneElision(testkeys.Comparer.Compare, v, l0Organizer, startLevel+1, base.UserKeyBoundsInclusive([]byte("a"), []byte("z")))
			var p pointTombstoneElider
			p.Init(testkeys.Comparer.Compare, del)
			var r rangeTombstoneElider
			r.Init(testkeys.Comparer.Compare, del)
			var buf bytes.Buffer
			for line := range crstrings.LinesSeq(td.Input) {
				switch f := strings.FieldsFunc(line, func(r rune) bool { return r == '-' }); len(f) {
				case 1:
					fmt.Fprintf(&buf, "elideTombstone(%q) = %t\n", f[0], p.ShouldElide([]byte(f[0])))
				case 2:
					fmt.Fprintf(&buf, "elideRangeTombstone(%q, %q) = %t\n", f[0], f[1], r.ShouldElide([]byte(f[0]), []byte(f[1])))
				default:
					td.Fatalf(t, "invalid line %q", line)
				}
			}
			return buf.String()
		default:
			return fmt.Sprintf("unknown command: %s", td.Cmd)
		}
	})
}
