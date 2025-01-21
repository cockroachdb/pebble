// Copyright 2019 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package keyspan

import (
	"bytes"
	"fmt"
	"strings"
	"testing"

	"github.com/cockroachdb/datadriven"
	"github.com/cockroachdb/pebble/v2/internal/base"
	"github.com/stretchr/testify/require"
)

func TestTruncate(t *testing.T) {
	cmp := base.DefaultComparer.Compare
	fmtKey := base.DefaultComparer.FormatKey
	var iter FragmentIterator
	var savedIter FragmentIterator
	defer func() {
		if savedIter != nil {
			savedIter.Close()
			savedIter = nil
		}
	}()

	datadriven.RunTest(t, "testdata/truncate", func(t *testing.T, d *datadriven.TestData) string {
		doTruncate := func() FragmentIterator {
			if len(d.Input) > 0 {
				t.Fatalf("unexpected input: %s", d.Input)
			}
			if len(d.CmdArgs) < 1 || len(d.CmdArgs) > 3 {
				t.Fatalf("expected 1-3 arguments: %s", d.CmdArgs)
			}
			parts := strings.Split(d.CmdArgs[0].String(), "-")
			if len(parts) != 2 {
				t.Fatalf("malformed arg: %s", d.CmdArgs[0])
			}
			lower := []byte(parts[0])
			upper := []byte(parts[1])

			return Truncate(cmp, iter, base.UserKeyBoundsEndExclusive(lower, upper))
		}

		switch d.Cmd {
		case "build":
			tombstones := buildSpans(t, cmp, fmtKey, d.Input, base.InternalKeyKindRangeDelete)
			iter = NewIter(cmp, tombstones)
			return formatAlphabeticSpans(tombstones)

		case "truncate":
			tIter := doTruncate()
			defer tIter.Close()
			var truncated []Span
			var s *Span
			var err error
			for s, err = tIter.First(); s != nil; s, err = tIter.Next() {
				truncated = append(truncated, s.Clone())
			}
			require.NoError(t, err)
			return formatAlphabeticSpans(truncated)

		case "truncate-and-save-iter":
			if savedIter != nil {
				savedIter.Close()
			}
			savedIter = doTruncate()
			return "ok"

		case "saved-iter":
			var buf bytes.Buffer
			RunIterCmd(d.Input, savedIter, &buf)
			return buf.String()

		default:
			return fmt.Sprintf("unknown command: %s", d.Cmd)
		}
	})
}
