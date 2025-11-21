// Copyright 2023 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package errorfs

import (
	"fmt"
	"strings"
	"testing"

	"github.com/cockroachdb/crlib/crstrings"
	"github.com/cockroachdb/datadriven"
)

func TestErrorFS(t *testing.T) {
	var sb strings.Builder
	datadriven.RunTest(t, "testdata/errorfs", func(t *testing.T, td *datadriven.TestData) string {
		sb.Reset()
		switch td.Cmd {
		case "parse-dsl":
			for l := range crstrings.LinesSeq(td.Input) {
				inj, err := ParseDSL(l)
				if err != nil {
					fmt.Fprintf(&sb, "parsing err: %s\n", err)
				} else {
					fmt.Fprintf(&sb, "%s\n", inj.String())
				}
			}
			return sb.String()
		default:
			return fmt.Sprintf("unrecognized command %q", td.Cmd)
		}
	})
}
