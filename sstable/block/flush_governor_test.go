// Copyright 2024 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package block

import (
	"testing"

	"github.com/cockroachdb/datadriven"
)

func TestFlushGovernor(t *testing.T) {
	var fg FlushGovernor
	datadriven.RunTest(t, "testdata/flush_governor", func(t *testing.T, td *datadriven.TestData) string {
		switch td.Cmd {
		case "init":
			targetBlockSize := 32768
			blockSizeThreshold := 90
			sizeClassAwareThreshold := 60
			var classes []int
			td.ScanArgs(t, "target-block-size", &targetBlockSize)
			td.MaybeScanArgs(t, "threshold", &blockSizeThreshold)
			td.MaybeScanArgs(t, "size-class-aware-threshold", &sizeClassAwareThreshold)
			td.MaybeScanArgs(t, "size-classes", &classes)
			fg = MakeFlushGovernor(targetBlockSize, blockSizeThreshold, sizeClassAwareThreshold, classes)
			return fg.String()

		case "should-flush":
			var sizeBefore, sizeAfter int
			td.ScanArgs(t, "size-before", &sizeBefore)
			td.ScanArgs(t, "size-after", &sizeAfter)
			if fg.ShouldFlush(sizeBefore, sizeAfter) {
				return "should flush"
			}
			return "should not flush"

		default:
			td.Fatalf(t, "unknown command: %s", td.Cmd)
		}
		return ""
	})
}
