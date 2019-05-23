// Copyright 2019 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package pebble

import (
	"testing"

	"github.com/petermattis/pebble/vfs"
	"github.com/stretchr/testify/require"
)

func (r *logRecycler) logNums() []uint64 {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([]uint64(nil), r.mu.logNums...)
}

func (r *logRecycler) maxLogNum() uint64 {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.mu.maxLogNum
}

func TestLogRecycler(t *testing.T) {
	r := logRecycler{limit: 3}

	// Logs are recycled up to the limit.
	require.True(t, r.add(1))
	require.EqualValues(t, []uint64{1}, r.logNums())
	require.EqualValues(t, 1, r.maxLogNum())
	require.EqualValues(t, 1, r.peek())
	require.True(t, r.add(2))
	require.EqualValues(t, []uint64{1, 2}, r.logNums())
	require.EqualValues(t, 2, r.maxLogNum())
	require.True(t, r.add(3))
	require.EqualValues(t, []uint64{1, 2, 3}, r.logNums())
	require.EqualValues(t, 3, r.maxLogNum())

	// Trying to add a file past the limit fails.
	require.False(t, r.add(4))
	require.EqualValues(t, []uint64{1, 2, 3}, r.logNums())
	require.EqualValues(t, 4, r.maxLogNum())

	// Trying to add a previously recycled file returns success, but the internal
	// state is unchanged.
	require.True(t, r.add(1))
	require.EqualValues(t, []uint64{1, 2, 3}, r.logNums())
	require.EqualValues(t, 4, r.maxLogNum())

	// An error is returned if we try to pop an element other than the first.
	require.Regexp(t, `invalid 2 vs \[1 2 3\]`, r.pop(2))

	require.NoError(t, r.pop(1))
	require.EqualValues(t, []uint64{2, 3}, r.logNums())

	// Log number 4 was already considered, so it won't be recycled.
	require.True(t, r.add(4))
	require.EqualValues(t, []uint64{2, 3}, r.logNums())

	require.True(t, r.add(5))
	require.EqualValues(t, []uint64{2, 3, 5}, r.logNums())
	require.EqualValues(t, 5, r.maxLogNum())

	require.NoError(t, r.pop(2))
	require.EqualValues(t, []uint64{3, 5}, r.logNums())
	require.NoError(t, r.pop(3))
	require.EqualValues(t, []uint64{5}, r.logNums())
	require.NoError(t, r.pop(5))
	require.EqualValues(t, []uint64(nil), r.logNums())

	require.Regexp(t, `empty`, r.pop(6))
}

func TestRecycleLogs(t *testing.T) {
	d, err := Open("", &Options{
		FS: vfs.NewMem(),
	})
	if err != nil {
		t.Fatal(err)
	}

	logNum := func() uint64 {
		d.mu.Lock()
		defer d.mu.Unlock()
		return d.mu.log.queue[len(d.mu.log.queue)-1]
	}

	// Flush the memtable a few times, forcing rotation of the WAL. We should see
	// the recycled logs change as expected.
	require.EqualValues(t, []uint64(nil), d.logRecycler.logNums())
	curLog := logNum()

	if err := d.Flush(); err != nil {
		t.Fatal(err)
	}

	require.EqualValues(t, []uint64{curLog}, d.logRecycler.logNums())
	curLog = logNum()

	if err := d.Flush(); err != nil {
		t.Fatal(err)
	}

	require.EqualValues(t, []uint64{curLog}, d.logRecycler.logNums())

	if err := d.Close(); err != nil {
		t.Fatal(err)
	}
}
