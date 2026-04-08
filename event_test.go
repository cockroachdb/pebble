// Copyright 2026 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package pebble

import (
	"encoding/hex"
	"strings"
	"testing"

	"github.com/cockroachdb/datadriven"
)

func TestFormatBlockDataAsHex(t *testing.T) {
	datadriven.RunTest(t, "testdata/format_block_data_as_hex", func(t *testing.T, td *datadriven.TestData) string {
		switch td.Cmd {
		case "format":
			input := strings.ReplaceAll(td.Input, "\n", "")
			input = strings.ReplaceAll(input, " ", "")
			data, err := hex.DecodeString(input)
			if err != nil {
				t.Fatalf("invalid hex input: %v", err)
			}
			info := DataCorruptionInfo{CorruptedBlockData: data}
			return info.FormatBlockDataAsHex()

		default:
			t.Fatalf("unknown command: %s", td.Cmd)
			return ""
		}
	})
}
