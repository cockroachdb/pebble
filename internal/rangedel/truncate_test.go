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
	fmtKey := base.DefaultComparer.FormatKey
	var iter base.InternalIterator

	datadriven.RunTest(t, "testdata/truncate", func(d *datadriven.TestData) string {
		switch d.Cmd {
		case "build":
			tombstones := buildTombstones(t, cmp, fmtKey, d.Input)
			iter = NewIter(cmp, tombstones)
			return formatTombstones(tombstones)

		case "truncate":
			if len(d.Input) > 0 {
				t.Fatalf("unexpected input: %s", d.Input)
			}
			if len(d.CmdArgs) < 1 || len(d.CmdArgs) > 3 {
				t.Fatalf("expected 1-3 arguments: %s", d.CmdArgs)
			}
			parts := strings.Split(d.CmdArgs[0].String(), "-")
			var startKey, endKey *base.InternalKey
			if len(d.CmdArgs) > 1 {
				for _, arg := range d.CmdArgs[1:] {
					switch arg.Key {
					case "startKey":
						startKey = &base.InternalKey{}
						*startKey = base.ParseInternalKey(arg.Vals[0])
					case "endKey":
						endKey = &base.InternalKey{}
						*endKey = base.ParseInternalKey(arg.Vals[0])
					}
				}
			}
			if len(parts) != 2 {
				t.Fatalf("malformed arg: %s", d.CmdArgs[0])
			}
			lower := []byte(parts[0])
			upper := []byte(parts[1])

			truncated := Truncate(cmp, iter, lower, upper, startKey, endKey)
			return formatTombstones(truncated.tombstones)

		default:
			return fmt.Sprintf("unknown command: %s", d.Cmd)
		}
	})
}
