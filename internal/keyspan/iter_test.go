// Copyright 2018 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package keyspan

import (
	"fmt"
	"testing"

	"github.com/cockroachdb/crlib/crstrings"
	"github.com/cockroachdb/datadriven"
	"github.com/cockroachdb/pebble/internal/base"
)

func TestIter(t *testing.T) {
	var spans []Span
	datadriven.RunTest(t, "testdata/iter", func(t *testing.T, d *datadriven.TestData) string {
		switch d.Cmd {
		case "define":
			spans = nil
			for line := range crstrings.LinesSeq(d.Input) {
				spans = append(spans, ParseSpan(line))
			}
			return ""

		case "iter":
			iter := NewIter(base.DefaultComparer.Compare, spans)
			defer iter.Close()
			return RunFragmentIteratorCmd(iter, d.Input, nil)
		default:
			return fmt.Sprintf("unknown command: %s", d.Cmd)
		}
	})
}
