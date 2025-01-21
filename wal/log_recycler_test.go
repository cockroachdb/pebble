// Copyright 2019 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package wal

import (
	"testing"

	"github.com/cockroachdb/pebble/v2/internal/base"
	"github.com/stretchr/testify/require"
)

func TestLogRecycler(t *testing.T) {
	r := LogRecycler{limit: 3, minRecycleLogNum: 4}

	// Logs below the min-recycle number are not recycled.
	require.False(t, r.Add(base.FileInfo{FileNum: base.DiskFileNum(1), FileSize: 0}))
	require.False(t, r.Add(base.FileInfo{FileNum: base.DiskFileNum(2), FileSize: 0}))
	require.False(t, r.Add(base.FileInfo{FileNum: base.DiskFileNum(3), FileSize: 0}))

	// Logs are recycled up to the limit.
	require.True(t, r.Add(base.FileInfo{FileNum: base.DiskFileNum(4), FileSize: 0}))
	require.EqualValues(t, []base.DiskFileNum{4}, r.LogNumsForTesting())
	require.EqualValues(t, 4, r.maxLogNumForTesting())
	fi, ok := r.Peek()
	require.True(t, ok)
	require.EqualValues(t, uint64(4), uint64(fi.FileNum))
	require.True(t, r.Add(base.FileInfo{FileNum: base.DiskFileNum(5), FileSize: 0}))
	require.EqualValues(t, []base.DiskFileNum{4, 5}, r.LogNumsForTesting())
	require.EqualValues(t, 5, r.maxLogNumForTesting())
	require.True(t, r.Add(base.FileInfo{FileNum: base.DiskFileNum(6), FileSize: 0}))
	require.EqualValues(t, []base.DiskFileNum{4, 5, 6}, r.LogNumsForTesting())
	require.EqualValues(t, 6, r.maxLogNumForTesting())

	// Trying to add a file past the limit fails.
	require.False(t, r.Add(base.FileInfo{FileNum: base.DiskFileNum(7), FileSize: 0}))
	require.EqualValues(t, []base.DiskFileNum{4, 5, 6}, r.LogNumsForTesting())
	require.EqualValues(t, 7, r.maxLogNumForTesting())

	// Trying to add a previously recycled file returns success, but the internal
	// state is unchanged.
	require.True(t, r.Add(base.FileInfo{FileNum: base.DiskFileNum(4), FileSize: 0}))
	require.EqualValues(t, []base.DiskFileNum{4, 5, 6}, r.LogNumsForTesting())
	require.EqualValues(t, 7, r.maxLogNumForTesting())

	// An error is returned if we try to pop an element other than the first.
	require.Regexp(t, `invalid 000005 vs \[000004 000005 000006\]`, r.Pop(5))

	require.NoError(t, r.Pop(4))
	require.EqualValues(t, []base.DiskFileNum{5, 6}, r.LogNumsForTesting())

	// Log number 7 was already considered, so it won't be recycled.
	require.True(t, r.Add(base.FileInfo{FileNum: base.DiskFileNum(7), FileSize: 0}))
	require.EqualValues(t, []base.DiskFileNum{5, 6}, r.LogNumsForTesting())

	require.True(t, r.Add(base.FileInfo{FileNum: base.DiskFileNum(8), FileSize: 0}))
	require.EqualValues(t, []base.DiskFileNum{5, 6, 8}, r.LogNumsForTesting())
	require.EqualValues(t, 8, r.maxLogNumForTesting())

	require.NoError(t, r.Pop(5))
	require.EqualValues(t, []base.DiskFileNum{6, 8}, r.LogNumsForTesting())
	require.NoError(t, r.Pop(6))
	require.EqualValues(t, []base.DiskFileNum{8}, r.LogNumsForTesting())
	require.NoError(t, r.Pop(8))
	require.EqualValues(t, []base.DiskFileNum(nil), r.LogNumsForTesting())

	require.Regexp(t, `empty`, r.Pop(9))
}
