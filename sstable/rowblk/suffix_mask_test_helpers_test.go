// Copyright 2026 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package rowblk

import (
	"fmt"
	"testing"

	"github.com/cockroachdb/datadriven"
	"github.com/cockroachdb/pebble/sstable/blockiter"
)

// parseSuffixMaskArgs parses suffix-mask-lower/suffix-mask-upper pairs from a
// datadriven test data block. The first pair uses the unsuffixed names; the
// Nth pair (N >= 2) uses names with an integer suffix:
//
//	iter suffix-mask-lower=@200 suffix-mask-upper=@5 \
//	     suffix-mask-lower2=@2  suffix-mask-upper2=@1 \
//	     suffix-mask-lower3=...
//
// Returns the masks in the order they appear (positional, not numeric).
func parseSuffixMaskArgs(t *testing.T, td *datadriven.TestData) []blockiter.SuffixMask {
	var masks []blockiter.SuffixMask
	for i := 1; ; i++ {
		lowerKey, upperKey := "suffix-mask-lower", "suffix-mask-upper"
		if i > 1 {
			lowerKey = fmt.Sprintf("suffix-mask-lower%d", i)
			upperKey = fmt.Sprintf("suffix-mask-upper%d", i)
		}
		var lower, upper string
		td.MaybeScanArgs(t, lowerKey, &lower)
		td.MaybeScanArgs(t, upperKey, &upper)
		if lower == "" && upper == "" {
			break
		}
		masks = append(masks, blockiter.SuffixMask{Lower: []byte(lower), Upper: []byte(upper)})
	}
	return masks
}
