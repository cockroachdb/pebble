// Copyright 2019 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package rangedel

import (
	"fmt"
	"strings"
	"testing"

	"github.com/cockroachdb/pebble/internal/base"
	"github.com/cockroachdb/pebble/internal/datadriven"
)

func TestTruncate(t *testing.T) {
	cmp := base.DefaultComparer.Compare
	var iter iterator

	datadriven.RunTest(t, "testdata/truncate", func(d *datadriven.TestData) string {
		switch d.Cmd {
		case "build":
			tombstones := buildTombstones(t, cmp, d.Input)
			iter = NewIter(cmp, tombstones)
			return formatTombstones(tombstones)

		case "truncate":
			if len(d.Input) > 0 {
				t.Fatalf("unexpected input: %s", d.Input)
			}
			if len(d.CmdArgs) != 1 {
				t.Fatalf("expected 1 argument: %s", d.CmdArgs)
			}
			parts := strings.Split(d.CmdArgs[0].String(), "-")
			if len(parts) != 2 {
				t.Fatalf("malformed arg: %s", d.CmdArgs[0])
			}
			lower := []byte(parts[0])
			upper := []byte(parts[1])

			truncated := Truncate(cmp, iter, lower, upper)
			return formatTombstones(truncated.tombstones)

		default:
			return fmt.Sprintf("unknown command: %s", d.Cmd)
		}
	})
}
