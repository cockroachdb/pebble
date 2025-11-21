// Copyright 2023 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package itertest

import (
	"fmt"
	"strings"
	"testing"

	"github.com/cockroachdb/crlib/crstrings"
	"github.com/cockroachdb/datadriven"
	"github.com/cockroachdb/pebble/internal/base"
	"github.com/cockroachdb/pebble/internal/testkeys"
)

func TestProbes(t *testing.T) {
	var sb strings.Builder
	parser := NewParser()
	var iter base.InternalIterator
	datadriven.RunTest(t, "testdata/probes", func(t *testing.T, td *datadriven.TestData) string {
		sb.Reset()
		switch td.Cmd {
		case "new":
			iter = nil
			// new expects each line to describe a probe in the DSL. Each probe
			// is used to wrap the iterator resulting from attaching the probe
			// on the previous line.
			for l := range crstrings.LinesSeq(td.Input) {
				probe, err := parser.Parse(l)
				if err != nil {
					return fmt.Sprintf("parsing err: %s\n", err)
				}
				iter = Attach(iter, ProbeState{
					Comparer: testkeys.Comparer,
				}, probe)
			}
			return sb.String()
		case "iter":
			return RunInternalIterCmd(t, td, iter, Verbose)
		default:
			return fmt.Sprintf("unrecognized command %q", td.Cmd)
		}
	})
}
