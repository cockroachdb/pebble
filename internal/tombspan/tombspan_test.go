// Copyright 2025 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package tombspan

import (
	"fmt"
	"strconv"
	"testing"

	"github.com/cockroachdb/crlib/crstrings"
	"github.com/cockroachdb/datadriven"
	"github.com/cockroachdb/pebble/internal/base"
	"github.com/cockroachdb/pebble/internal/manifest"
	"github.com/cockroachdb/pebble/internal/strparse"
	"github.com/cockroachdb/pebble/internal/testkeys"
)

func TestSet(t *testing.T) {
	tables := make(map[base.DiskFileNum]*manifest.TableMetadata)
	set := Make(testkeys.Comparer)
	var versions []*manifest.Version

	parseWideTombstone := func(line string) WideTombstone {
		// Example: L1.1 [a,b) seqnums{point=[1-2], range=[3-4]}
		var h WideTombstone
		p := strparse.MakeParser(`-[]()={}.,`, line)
		h.Level = p.Level()
		p.Expect(".")
		h.Table = tables[p.DiskFileNum()]
		p.Expect("[")
		h.Bounds.Start = []byte(p.Next())
		p.Expect(",")
		h.Bounds.End.Key = []byte(p.Next())
		h.Bounds.End.Kind = base.Exclusive
		p.Expect(")")
		p.Expect("seqnums")
		p.Expect("{")
		if p.Peek() == "point" {
			p.Expect("point")
			p.Expect("=")
			h.PointSeqNums = p.SeqNumRange()
		}
		if p.Peek() == "," {
			p.Expect(",")
		}
		if p.Peek() == "range" {
			p.Expect("range")
			h.RangeSeqNums = p.SeqNumRange()
		}
		p.Expect("}")
		return h
	}

	datadriven.RunTest(t, "testdata/set", func(t *testing.T, td *datadriven.TestData) string {
		switch td.Cmd {
		case "define-version":
			l0Organizer := manifest.NewL0Organizer(testkeys.Comparer, 64*1024 /* flushSplitBytes */)
			v, err := manifest.ParseVersionDebug(testkeys.Comparer, l0Organizer, td.Input)
			if err != nil {
				return err.Error()
			}
			for _, table := range v.AllTables() {
				dfn := base.DiskFileNum(table.TableNum)
				if _, ok := tables[dfn]; !ok {
					tables[dfn] = table
				}
			}
			versions = append(versions, v)
			return fmt.Sprintf("v%d:\n%s", len(versions)-1, v.DebugString())
		case "add":
			var tombs []WideTombstone
			for line := range crstrings.LinesSeq(td.Input) {
				tombs = append(tombs, parseWideTombstone(line))
			}
			set.AddTombstones(tombs...)
			return set.String()
		case "update-with-earliest-snapshot":
			v, err := strconv.ParseInt(td.CmdArgs[0].String(), 10, 64)
			if err != nil {
				return err.Error()
			}
			set.UpdateWithEarliestSnapshot(base.SeqNum(v))
			return set.String()
		case "pick-compaction":
			var versionIndex int
			td.ScanArgs(t, "v", &versionIndex)
			excise := td.HasArg("exciseAllowed")
			picked, ok := set.PickCompaction(versions[versionIndex], excise)
			if !ok {
				return fmt.Sprintf("none\n%s", set.String())
			}
			return fmt.Sprintf("%s\n%s", picked.String(), set.String())
		case "mark-as-compacting":
			var tableNum uint64
			td.ScanArgs(t, "table", &tableNum)
			tables[base.DiskFileNum(tableNum)].CompactionState = manifest.CompactionStateCompacting
			return ""
		default:
			return fmt.Sprintf("unknown command: %s", td.Cmd)
		}
	})
}
