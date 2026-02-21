// Copyright 2026 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package pebble

import (
	"context"
	"crypto/rand"
	"fmt"
	"testing"

	"github.com/cockroachdb/pebble/vfs"
	"github.com/stretchr/testify/require"
)

func TestApproximateMidKey(t *testing.T) {
	opts := &Options{
		FS:                          vfs.NewMem(),
		DisableAutomaticCompactions: true,
	}
	opts.ApplyCompressionSettings(func() DBCompressionSettings {
		return DBCompressionNone
	})
	d, err := Open("", opts)
	require.NoError(t, err)
	defer d.Close()

	// Use 4KB random values. Each KV pair exceeds the default 4KB block
	// size, so each gets its own data block. This gives SSTs many index
	// entries for the merge-walk to bisect within.
	value := make([]byte, 4096)
	rand.Read(value)

	// Set up a single LSM with distinct key ranges that have different
	// structures. After setup the LSM looks like:
	//
	//         a       b        c          d        e
	//
	// L0.0          |b000---b063|                          ~260KB
	//               |c000-c015|                           ~65KB each
	//                 |c016-c031|
	//                   |c032-c047|
	//                     |c048-c063|
	//                                 |d032-d063|         ~130KB
	//                                             |e0-e3| ~0.5KB
	// L6                              |d000-d031|         ~130KB
	//
	// Key ranges and what they exercise:
	//   "a___": no data (empty span)
	//   "b___": single SST — forces merge-walk into one SST's index
	//   "c___": four L0 SSTs — coarse walk may find the boundary
	//   "d___": data across L0 and L6
	//   "e___": tiny data — below epsilon, returns nil

	// "b" range: one flush → single SST.
	for i := 0; i < 64; i++ {
		require.NoError(t, d.Set([]byte(fmt.Sprintf("b%03d", i)), value, nil))
	}
	require.NoError(t, d.Flush())

	// "c" range: flush every 16 keys → four SSTs.
	for i := 0; i < 64; i++ {
		require.NoError(t, d.Set([]byte(fmt.Sprintf("c%03d", i)), value, nil))
		if (i+1)%16 == 0 {
			require.NoError(t, d.Flush())
		}
	}

	// "d" range: write + flush + compact to lower level, then write more to L0.
	for i := 0; i < 32; i++ {
		require.NoError(t, d.Set([]byte(fmt.Sprintf("d%03d", i)), value, nil))
	}
	require.NoError(t, d.Flush())
	require.NoError(t, d.Compact(context.Background(), []byte("d000"), []byte("d999"), false))
	for i := 32; i < 64; i++ {
		require.NoError(t, d.Set([]byte(fmt.Sprintf("d%03d", i)), value, nil))
	}
	require.NoError(t, d.Flush())

	// "e" range: a few small values.
	for _, k := range []string{"e000", "e001", "e002", "e003"} {
		require.NoError(t, d.Set([]byte(k), []byte("v"), nil))
	}
	require.NoError(t, d.Flush())

	for _, tc := range []struct {
		name    string
		kr      KeyRange
		epsilon uint64
		wantNil bool // true if we expect no mid key
	}{
		{
			name:    "empty-span",
			kr:      KeyRange{Start: []byte("a000"), End: []byte("a999")},
			epsilon: 64 << 10,
			wantNil: true,
		},
		{
			name:    "too-small-for-epsilon",
			kr:      KeyRange{Start: []byte("e000"), End: []byte("e999")},
			epsilon: 64 << 10,
			wantNil: true,
		},
		{
			name:    "single-sst",
			kr:      KeyRange{Start: []byte("b000"), End: []byte("b999")},
			epsilon: 64 << 10,
		},
		{
			name:    "multiple-ssts",
			kr:      KeyRange{Start: []byte("c000"), End: []byte("c999")},
			epsilon: 64 << 10,
		},
		{
			name:    "multiple-levels",
			kr:      KeyRange{Start: []byte("d000"), End: []byte("d999")},
			epsilon: 64 << 10,
		},
		{
			name:    "tight-epsilon",
			kr:      KeyRange{Start: []byte("c000"), End: []byte("c999")},
			epsilon: 1 << 10,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			midKey, _, err := d.ApproximateMidKey(context.Background(), tc.kr, tc.epsilon)
			require.NoError(t, err)

			if tc.wantNil {
				require.Nil(t, midKey)
				return
			}

			require.NotNil(t, midKey, "expected a mid key")
			require.True(t, d.cmp(midKey, tc.kr.Start) > 0, "mid key %s <= start", midKey)
			require.True(t, d.cmp(midKey, tc.kr.End) < 0, "mid key %s >= end", midKey)

			leftSize, err := d.EstimateDiskUsage(tc.kr.Start, midKey)
			require.NoError(t, err)
			rightSize, err := d.EstimateDiskUsage(midKey, tc.kr.End)
			require.NoError(t, err)
			total := leftSize + rightSize
			if total > 0 {
				leftPct := float64(leftSize) / float64(total)
				require.Greater(t, leftPct, 0.15,
					"left side too small: left=%d right=%d", leftSize, rightSize)
				require.Less(t, leftPct, 0.85,
					"left side too large: left=%d right=%d", leftSize, rightSize)
			}
		})
	}
}
