// Copyright 2026 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package pebble

import (
	"encoding/hex"
	"strings"
	"testing"
	"time"

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

func TestFormatTablesWithSizes(t *testing.T) {
	require.Equal(t, "", FormatTablesWithSizes(nil))
	require.Equal(t, "000001(3.0KB)",
		FormatTablesWithSizes([]TableInfo{tableInfoWithRefSize(1, 3<<10, 0)}))
	require.Equal(t, "000001(3.0KB+1.0MB)",
		FormatTablesWithSizes([]TableInfo{tableInfoWithRefSize(1, 3<<10, 1<<20)}))
	// Multiple tables exercise the separator and the per-table refSize omission.
	require.Equal(t, "000001(3.0KB+1.0MB) 000002(1.0KB)",
		FormatTablesWithSizes([]TableInfo{
			tableInfoWithRefSize(1, 3<<10, 1<<20),
			tableInfoWithRefSize(2, 1<<10, 0),
		}))
}

func TestFormatBlobsWithSizes(t *testing.T) {
	require.Equal(t, "", FormatBlobsWithSizes(nil))
	require.Equal(t, "000006(5.0MB)",
		FormatBlobsWithSizes([]BlobFileInfo{{DiskFileNum: base.DiskFileNum(6), Size: 5 << 20}}))
	// Multiple blobs; the second carries MVCC garbage (1/4 of its values).
	require.Equal(t, "000006(5.0MB) 000007(2.0MB, MVCCGarbage: 25%)",
		FormatBlobsWithSizes([]BlobFileInfo{
			{DiskFileNum: base.DiskFileNum(6), Size: 5 << 20},
			{DiskFileNum: base.DiskFileNum(7), Size: 2 << 20, ValueSize: 4 << 20, MVCCGarbageSize: 1 << 20},
		}))
}

func TestFormatTotalSize(t *testing.T) {
	require.Equal(t, "3.0KB", formatTotalSize(3<<10, 0))
	require.Equal(t, "3.0KB+1.0MB", formatTotalSize(3<<10, 1<<20))
}

// TestCompactionInfoSafeFormatBlobs verifies the compaction "done" line renders
// per-table output sizes and the output-blob segment. This is the only direct
// coverage of the compaction output-blob branch; golden testdata flushes blobs
// but never compacts them.
func TestCompactionInfoSafeFormatBlobs(t *testing.T) {
	ci := CompactionInfo{
		JobID:  1,
		Reason: "default",
		Input:  []LevelInfo{{Level: 0, Tables: []TableInfo{tableInfoWithRefSize(1, 1<<20, 0)}}},
		Output: LevelInfo{
			Level: 6,
			Tables: []TableInfo{
				tableInfoWithRefSize(10, 8<<20, 2<<20),
				tableInfoWithRefSize(11, 2<<20, 0),
			},
			Blobs: []BlobFileInfo{
				{DiskFileNum: base.DiskFileNum(12), Size: 3 << 20},
				{DiskFileNum: base.DiskFileNum(13), Size: 1 << 20},
			},
		},
		Duration:      time.Second,
		TotalDuration: time.Second,
		Done:          true,
	}
	require.Contains(t, ci.String(),
		"-> L6 [000010(8.0MB+2.0MB) 000011(2.0MB)] (10MB+2.0MB) blobs [000012(3.0MB) 000013(1.0MB)] (4.0MB)")
}
