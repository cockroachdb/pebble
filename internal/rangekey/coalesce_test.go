// Copyright 2021 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package rangekey

import (
	"fmt"
	"testing"

	"github.com/cockroachdb/datadriven"
	"github.com/cockroachdb/pebble/v2/internal/keyspan"
	"github.com/cockroachdb/pebble/v2/internal/testkeys"
)

func TestCoalesce(t *testing.T) {
	datadriven.RunTest(t, "testdata/coalesce", func(t *testing.T, td *datadriven.TestData) string {
		switch td.Cmd {
		case "coalesce":
			span := keyspan.ParseSpan(td.Input)
			coalesced := keyspan.Span{
				Start: span.Start,
				End:   span.End,
			}
			Coalesce(testkeys.Comparer.CompareRangeSuffixes, span.Keys, &coalesced.Keys)
			return coalesced.String()

		default:
			return fmt.Sprintf("unrecognized command %q", td.Cmd)
		}
	})
}
