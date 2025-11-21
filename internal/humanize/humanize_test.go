// Copyright 2023 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package humanize

import (
	"bytes"
	"fmt"
	"strconv"
	"testing"

	"github.com/cockroachdb/crlib/crstrings"
	"github.com/cockroachdb/datadriven"
)

func TestHumanize(t *testing.T) {
	datadriven.RunTest(t, "testdata/humanize", func(t *testing.T, td *datadriven.TestData) string {
		var c config
		switch td.Cmd {
		case "bytes":
			c = Bytes
		case "count":
			c = Count
		default:
			td.Fatalf(t, "invalid command %q", td.Cmd)
		}
		var buf bytes.Buffer
		for row := range crstrings.LinesSeq(td.Input) {
			val, err := strconv.ParseInt(row, 10, 64)
			if err != nil {
				td.Fatalf(t, "error parsing %q: %v", row, err)
			}
			fmt.Fprintf(&buf, "%s\n", c.Int64(val))
		}
		return buf.String()
	})
}
