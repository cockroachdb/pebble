// Copyright 2019 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package keyspan

import (
	"fmt"
	"strings"
	"testing"

	"github.com/cockroachdb/datadriven"
	"github.com/cockroachdb/pebble/internal/base"
)

func TestTruncate(t *testing.T) {
	cmp := base.DefaultComparer.Compare
	fmtKey := base.DefaultComparer.FormatKey
	var iter FragmentIterator

	datadriven.RunTest(t, "testdata/truncate", func(t *testing.T, d *datadriven.TestData) string {
		switch d.Cmd {
		case "build":
			tombstones := buildSpans(t, cmp, fmtKey, d.Input, base.InternalKeyKindRangeDelete)
			iter = NewIter(cmp, tombstones)
			return formatAlphabeticSpans(tombstones)

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

			tIter := Truncate(cmp, iter, lower, upper, startKey, endKey)
			defer tIter.Close()
			var truncated []Span
			for s := tIter.First(); s != nil; s = tIter.Next() {
				truncated = append(truncated, s.ShallowClone())
			}
			return formatAlphabeticSpans(truncated)

		default:
			return fmt.Sprintf("unknown command: %s", d.Cmd)
		}
	})
}
