// Copyright 2023 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package sstable

import (
	"bytes"
	"fmt"
	"strings"
	"testing"

	"github.com/cockroachdb/datadriven"
	"github.com/cockroachdb/pebble/internal/base"
)

func TestIndexBlockWriter(t *testing.T) {
	ibw := &indexBlockWriter{}
	datadriven.RunTest(t, "testdata/index_block_writer", func(t *testing.T, td *datadriven.TestData) string {
		switch td.Cmd {
		case "build":
			ibw.format = indexBlockFormatDefault
			if td.HasArg("condensed") {
				ibw.format = indexBlockFormatCondensed
			}

			for _, line := range strings.Split(td.Input, "\n") {
				parts := strings.Split(line, ":")
				ik := base.ParseInternalKey(parts[0])
				ibw.add(ik, []byte(parts[1]))
			}
			const printWidth = 30
			data := ibw.finish()
			var buf bytes.Buffer
			for len(data) > 0 {
				if len(data) > printWidth {
					fmt.Fprintf(&buf, "%x\n", data[:printWidth])
					data = data[printWidth:]
				} else {
					fmt.Fprintf(&buf, "%x\n", data)
					data = nil
				}
			}
			ibw.clear()
			return buf.String()
		default:
			return fmt.Sprintf("unrecognized command %q", td.Cmd)
		}
	})
}
