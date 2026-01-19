// Copyright 2026 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package tool

import (
	"bytes"
	"testing"

	"github.com/cockroachdb/datadriven"
	"github.com/cockroachdb/pebble/internal/manifest"
)

func TestPrintMetadataStats(t *testing.T) {
	datadriven.RunTest(t, "testdata/analyze_metadata", func(t *testing.T, td *datadriven.TestData) string {
		switch td.Cmd {
		case "print-metadata-stats":
			var stats metadataStats

			// L0: Normal level with sampled files and two-level index files.
			stats.levels[0].numTotalFiles = 100
			stats.levels[0].numSampledFiles = 50
			stats.levels[0].numFilesWithTwoLevelIndex = 10
			for i := 0; i < 50; i++ {
				stats.levels[0].commonPrefix.Add(float64(10 + i%5))
				stats.levels[0].sstableFileSize.Add(float64(1024 * 1024 * (1 + i%3)))          // 1-3 MB
				stats.levels[0].sstableFileSizePlusBlobs.Add(float64(1024 * 1024 * (2 + i%3))) // 2-4 MB
				stats.levels[0].numKVsPerFile.Add(float64(1000 + i*10))
				stats.levels[0].bytesPerKV.Add(float64(100 + i%20))
				stats.levels[0].bytesPerKVWithBlobs.Add(float64(150 + i%30))
				stats.levels[0].indexSize.Add(float64(10000 + i*100))
				stats.levels[0].numEntriesPerIndexBlock.Add(float64(100 + i%10))
				stats.levels[0].numDataBlocks.Add(float64(50 + i%20))
				stats.levels[0].filterBlockSize.Add(float64(5000 + i*50))
			}

			// L1: Empty level (no files at all).
			// All fields remain zero.

			// L2: Files exist but none sampled yet (numSampledFiles = 0).
			stats.levels[2].numTotalFiles = 20
			// numSampledFiles remains 0, all Welford stats empty.

			// L3: Sampled files but no two-level index (all single-level index).
			stats.levels[3].numTotalFiles = 30
			stats.levels[3].numSampledFiles = 15
			stats.levels[3].numFilesWithTwoLevelIndex = 0 // No two-level index files
			for i := 0; i < 15; i++ {
				stats.levels[3].commonPrefix.Add(float64(5 + i%3))
				stats.levels[3].sstableFileSize.Add(float64(512 * 1024 * (1 + i%2))) // 512KB - 1MB
				stats.levels[3].sstableFileSizePlusBlobs.Add(float64(512 * 1024 * (1 + i%2)))
				stats.levels[3].numKVsPerFile.Add(float64(500 + i*5))
				stats.levels[3].bytesPerKV.Add(float64(80 + i%10))
				stats.levels[3].bytesPerKVWithBlobs.Add(float64(80 + i%10))
				stats.levels[3].indexSize.Add(float64(5000 + i*50))
				stats.levels[3].numEntriesPerIndexBlock.Add(float64(50 + i%5))
				stats.levels[3].numDataBlocks.Add(float64(30 + i%10))
				stats.levels[3].filterBlockSize.Add(float64(2000 + i*20))
			}

			// L4: Very small files (edge case with small values).
			stats.levels[4].numTotalFiles = 5
			stats.levels[4].numSampledFiles = 5
			stats.levels[4].numFilesWithTwoLevelIndex = 5 // All have two-level index
			for i := 0; i < 5; i++ {
				stats.levels[4].commonPrefix.Add(float64(1))
				stats.levels[4].sstableFileSize.Add(float64(1024)) // 1KB
				stats.levels[4].sstableFileSizePlusBlobs.Add(float64(2048))
				stats.levels[4].numKVsPerFile.Add(float64(10))
				stats.levels[4].bytesPerKV.Add(float64(100))
				stats.levels[4].bytesPerKVWithBlobs.Add(float64(200))
				stats.levels[4].indexSize.Add(float64(100))
				stats.levels[4].numEntriesPerIndexBlock.Add(float64(5))
				stats.levels[4].numDataBlocks.Add(float64(2))
				stats.levels[4].filterBlockSize.Add(float64(50))
			}

			// L5: Large files (edge case with large values).
			stats.levels[5].numTotalFiles = 10
			stats.levels[5].numSampledFiles = 3
			stats.levels[5].numFilesWithTwoLevelIndex = 3
			for i := 0; i < 3; i++ {
				stats.levels[5].commonPrefix.Add(float64(100))
				stats.levels[5].sstableFileSize.Add(float64(1024 * 1024 * 1024)) // 1GB
				stats.levels[5].sstableFileSizePlusBlobs.Add(float64(2 * 1024 * 1024 * 1024))
				stats.levels[5].numKVsPerFile.Add(float64(10000000))
				stats.levels[5].bytesPerKV.Add(float64(100))
				stats.levels[5].bytesPerKVWithBlobs.Add(float64(200))
				stats.levels[5].indexSize.Add(float64(100 * 1024 * 1024)) // 100MB
				stats.levels[5].numEntriesPerIndexBlock.Add(float64(10000))
				stats.levels[5].numDataBlocks.Add(float64(1000000))
				stats.levels[5].filterBlockSize.Add(float64(50 * 1024 * 1024)) // 50MB
			}

			// L6: Zero values for all stats (sampled files but all zero data).
			stats.levels[6].numTotalFiles = 10
			stats.levels[6].numSampledFiles = 10
			stats.levels[6].numFilesWithTwoLevelIndex = 0
			for i := 0; i < 10; i++ {
				stats.levels[6].commonPrefix.Add(0)
				stats.levels[6].sstableFileSize.Add(0)
				stats.levels[6].sstableFileSizePlusBlobs.Add(0)
				stats.levels[6].numKVsPerFile.Add(0)
				// Don't add to bytesPerKV since numKVsPerFile is 0 (matching real behavior)
				stats.levels[6].indexSize.Add(0)
				stats.levels[6].numEntriesPerIndexBlock.Add(0)
				stats.levels[6].numDataBlocks.Add(0)
				stats.levels[6].filterBlockSize.Add(0)
			}

			var sampledFiles, totalFiles int
			for i := range manifest.NumLevels {
				sampledFiles += int(stats.levels[i].numSampledFiles)
				totalFiles += int(stats.levels[i].numTotalFiles)
			}

			var buf bytes.Buffer
			printMetadataStats(&buf, &stats, sampledFiles, totalFiles)
			return buf.String()

		default:
			t.Fatalf("unknown command: %s", td.Cmd)
			return ""
		}
	})
}
