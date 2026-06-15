// Copyright 2026 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package pebble

import (
	"encoding/hex"
	"strings"
	"testing"

	"github.com/cockroachdb/datadriven"
	"github.com/cockroachdb/pebble/internal/base"
	"github.com/cockroachdb/pebble/internal/manifest"
	"github.com/stretchr/testify/require"
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

// tableInfoWithRefSize builds a TableInfo carrying a single blob reference with
// the given estimated physical size.
func tableInfoWithRefSize(num base.FileNum, size, refSize uint64) TableInfo {
	m := &manifest.TableMetadata{TableNum: num, Size: size}
	if refSize > 0 {
		m.BlobReferences = manifest.BlobReferences{
			{FileID: base.BlobFileID(num), EstimatedPhysicalSize: refSize},
		}
	}
	return m.TableInfo()
}

func TestLevelInfoSafeFormat(t *testing.T) {
	t.Run("no blob references", func(t *testing.T) {
		li := LevelInfo{
			Level:  1,
			Tables: []TableInfo{tableInfoWithRefSize(1, 3<<10, 0)},
			Score:  1.31,
		}
		require.Equal(t, "L1 [000001] (3.0KB) Score=1.31", li.String())
	})

	t.Run("with blob references", func(t *testing.T) {
		li := LevelInfo{
			Level: 0,
			Tables: []TableInfo{
				tableInfoWithRefSize(1, 3<<10, 1<<20),
				tableInfoWithRefSize(2, 1<<10, 200<<10),
			},
			Score: 1.31,
		}
		require.Equal(t, "L0 [000001 000002] (4.0KB + 1.2MB) Score=1.31", li.String())
	})
}
