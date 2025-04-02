// Copyright 2024 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package compact

import (
	"fmt"
	"strings"
	"testing"

	"github.com/cockroachdb/datadriven"
	"github.com/cockroachdb/pebble/internal/base"
	"github.com/cockroachdb/pebble/internal/manifest"
	"github.com/cockroachdb/pebble/internal/testutils"
)

func TestTableSplitLimit(t *testing.T) {
	var v *manifest.Version
	datadriven.RunTest(t, "testdata/table_split_limit", func(t *testing.T, d *datadriven.TestData) string {
		var buf strings.Builder
		switch d.Cmd {
		case "define":
			var flushSplitBytes int64
			d.MaybeScanArgs(t, "flush-split-bytes", &flushSplitBytes)
			l0Organizer := manifest.NewL0Organizer(base.DefaultComparer, flushSplitBytes)
			v = testutils.CheckErr(manifest.ParseVersionDebug(base.DefaultComparer, l0Organizer, d.Input))
			buf.WriteString(v.String())
			if v.Levels[0].Len() != 0 {
				buf.WriteString("flush split keys:\n")
				for _, key := range v.L0Sublevels.FlushSplitKeys() {
					fmt.Fprintf(&buf, "\t%s\n", base.DefaultFormatter(key))
				}
			}

		case "split-limit":
			var maxOverlap uint64
			d.MaybeScanArgs(t, "max-overlap", &maxOverlap)
			r := &Runner{
				cmp: base.DefaultComparer.Compare,
				cfg: RunnerConfig{
					L0SplitKeys:                v.L0Sublevels.FlushSplitKeys(),
					Grandparents:               v.Levels[1].Slice(),
					MaxGrandparentOverlapBytes: maxOverlap,
				},
			}
			for _, k := range strings.Fields(d.Input) {
				res := r.TableSplitLimit([]byte(k))
				if res == nil {
					fmt.Fprintf(&buf, "%s: no limit\n", k)
				} else {
					fmt.Fprintf(&buf, "%s: %s\n", k, string(res))
				}
			}

		default:
			d.Fatalf(t, "unknown command: %s", d.Cmd)
		}
		return buf.String()
	})
}
