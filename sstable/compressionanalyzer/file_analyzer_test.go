// Copyright 2025 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package compressionanalyzer

import (
	"context"
	"testing"

	"github.com/cockroachdb/crlib/crstrings"
	"github.com/cockroachdb/datadriven"
	"github.com/cockroachdb/pebble/objstorage"
	"github.com/cockroachdb/pebble/sstable"
	"github.com/cockroachdb/pebble/vfs"
)

func TestFileAnalyzer(t *testing.T) {
	datadriven.RunTest(t, "testdata/file_analyzer", func(t *testing.T, td *datadriven.TestData) string {
		switch td.Cmd {
		case "sst":
			fa := NewFileAnalyzer(nil, sstable.ReaderOptions{})
			defer fa.Close()
			for path := range crstrings.LinesSeq(td.Input) {
				file, err := vfs.Default.Open(path)
				if err != nil {
					td.Fatalf(t, "%v", err)
				}
				readable, err := objstorage.NewSimpleReadable(file)
				if err != nil {
					td.Fatalf(t, "%v", err)
				}
				if err := fa.SSTable(context.Background(), readable); err != nil {
					td.Fatalf(t, "%v", err)
				}
			}
			// Clear out values that are not reliable.
			b := fa.Buckets()
			for i := range b {
				for j := range b[i] {
					for k := range b[i][j] {
						bucket := &b[i][j][k]
						for l := range bucket.Experiments {
							// Snappy always has the same output in all configurations and on
							// all platforms.
							if Profiles[l].Name != "Snappy" {
								bucket.Experiments[l].CompressionRatio = WeightedWelford{}
							}
							bucket.Experiments[l].CompressionTime = WeightedWelford{}
							bucket.Experiments[l].DecompressionTime = WeightedWelford{}
						}
					}
				}
			}
			return b.String(1)

		default:
			td.Fatalf(t, "unknown command %s", td.Cmd)
			return ""
		}
	})
}
