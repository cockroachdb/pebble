// Copyright 2025 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package cockroachkvs

import (
	"testing"

	"github.com/cockroachdb/pebble/internal/base"
	"github.com/stretchr/testify/require"
)

// TestMaxMVCCTimestampPropertyExtract verifies that
// MaxMVCCTimestampProperty.Extract returns a suffix that encodes the largest
// wall time actually present in the block, not Upper of the half-open
// BlockInterval. The returned suffix powers the sstable iterator's
// synthetic-key optimization, so it must sort no later (under MVCC's
// descending suffix order) than the largest real key's suffix; otherwise the
// synthetic key sorts ahead of keys it is meant to stand in for.
func TestMaxMVCCTimestampPropertyExtract(t *testing.T) {
	collector := BlockPropertyCollectors[0]()

	// Add a few MVCC keys with distinct wall times to a single data block.
	walls := []uint64{100, 250, 175, 300, 50}
	var maxWall uint64
	for _, w := range walls {
		key := EncodeMVCCKey(nil, []byte("roach"), w, 0)
		ik := base.MakeInternalKey(key, base.SeqNumMax, base.InternalKeyKindSet)
		require.NoError(t, collector.AddPointKey(ik, nil))
		if w > maxWall {
			maxWall = w
		}
	}

	encodedInterval, err := collector.FinishDataBlock(nil)
	require.NoError(t, err)

	// Extract skips the first byte (shortID prefix added by the block-property
	// framework in real sstables), so prepend a placeholder.
	encoded := append([]byte{0x00}, encodedInterval...)

	suffix, ok, err := MaxMVCCTimestampProperty{}.Extract(nil, encoded)
	require.NoError(t, err)
	require.True(t, ok)

	wall, logical, err := DecodeMVCCTimestampSuffix(suffix)
	require.NoError(t, err)
	require.Equal(t, uint32(0), logical)
	require.Equal(t, maxWall, wall,
		"extracted wall time should match the largest wall time in the block")

	// Stronger ordering check: the synthetic suffix must sort no later than
	// the real largest suffix (ComparePointSuffixes <= 0).
	realMax := NewTimestampSuffix(maxWall, 0)
	require.LessOrEqual(t, ComparePointSuffixes(suffix, realMax), 0,
		"extracted suffix must not sort after the largest real suffix")
}
