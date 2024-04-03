// Copyright 2021 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package rangekey

import (
	"bytes"
	"fmt"
	"testing"

	"github.com/cockroachdb/datadriven"
	"github.com/cockroachdb/pebble/internal/keyspan"
	"github.com/cockroachdb/pebble/internal/testkeys"
)

func TestCoalesce(t *testing.T) {
	var buf bytes.Buffer
	eq := testkeys.Comparer.Equal
	cmp := testkeys.Comparer.Compare

	datadriven.RunTest(t, "testdata/coalesce", func(t *testing.T, td *datadriven.TestData) string {
		switch td.Cmd {
		case "coalesce":
			buf.Reset()
			span := keyspan.ParseSpan(td.Input)
			coalesced := keyspan.Span{
				Start: span.Start,
				End:   span.End,
			}
			Coalesce(cmp, eq, span.Keys, &coalesced.Keys)
			fmt.Fprintln(&buf, coalesced)
			return buf.String()
		default:
			return fmt.Sprintf("unrecognized command %q", td.Cmd)
		}
	})
}
