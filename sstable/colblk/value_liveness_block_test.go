// Copyright 2025 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package colblk

import (
	"bytes"
	"fmt"
	"strings"
	"testing"

	"github.com/cockroachdb/datadriven"
)

func TestValueLivenessBlock(t *testing.T) {
	var decoder ValueLivenessBlockDecoder
	var buf bytes.Buffer
	datadriven.RunTest(t, "testdata/value_liveness_block", func(t *testing.T, d *datadriven.TestData) string {
		buf.Reset()
		switch d.Cmd {
		case "build":
			var w BlobRefValueLivenessIndexBlockEncoder
			w.Init()
			for _, line := range strings.Split(d.Input, "\n") {
				fields := strings.Fields(line)
				value := fields[0]
				w.AddValue([]byte(value))
			}

			data := w.Finish()
			decoder.Init(data)
			fmt.Fprint(&buf, decoder.DebugString())
			return buf.String()
		default:
			return fmt.Sprintf("unknown command: %s", d.Cmd)
		}
	})
}
