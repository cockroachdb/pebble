// Copyright 2023 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package keyspan

import (
	"fmt"
	"regexp"
	"strings"
	"testing"

	"github.com/cockroachdb/datadriven"
	"github.com/cockroachdb/pebble/internal/base"
	"github.com/stretchr/testify/require"
)

func TestLoggingIter(t *testing.T) {
	var spans []Span
	datadriven.RunTest(t, "testdata/logging_iter", func(t *testing.T, d *datadriven.TestData) string {
		switch d.Cmd {
		case "define":
			spans = nil
			for _, line := range strings.Split(d.Input, "\n") {
				spans = append(spans, ParseSpan(line))
			}
			return ""

		case "iter":
			l := &base.InMemLogger{}
			var iter FragmentIterator
			iter = NewIter(base.DefaultComparer.Compare, spans)
			// Wrap with an assert as a very simple "stack" example.
			iter = Assert(iter, base.DefaultComparer.Compare)
			iter = InjectLogging(iter, l)
			RunFragmentIteratorCmd(iter, d.Input, nil)
			require.NoError(t, iter.Close())
			out := l.String()
			// Hide pointer values.
			r := regexp.MustCompile(`\(0x[0-9a-f]+\)`)
			return r.ReplaceAllString(out, "")

		default:
			return fmt.Sprintf("unknown command: %s", d.Cmd)
		}
	})
}
