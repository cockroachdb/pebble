// Copyright 2019 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package pebble

import (
	"testing"

	"github.com/cockroachdb/pebble/vfs"
	"github.com/stretchr/testify/require"
)

func (r *logRecycler) logNums() []FileNum {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([]FileNum(nil), r.mu.logNums...)
}

func (r *logRecycler) maxLogNum() FileNum {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.mu.maxLogNum
}

func TestLogRecycler(t *testing.T) {
	r := logRecycler{limit: 3, minRecycleLogNum: 4}

	// Logs below the min-recycle number are not recycled.
	require.False(t, r.add(1))
	require.False(t, r.add(2))
	require.False(t, r.add(3))

	// Logs are recycled up to the limit.
	require.True(t, r.add(4))
	require.EqualValues(t, []FileNum{4}, r.logNums())
	require.EqualValues(t, 4, r.maxLogNum())
	require.EqualValues(t, 4, r.peek())
	require.True(t, r.add(5))
	require.EqualValues(t, []FileNum{4, 5}, r.logNums())
	require.EqualValues(t, 5, r.maxLogNum())
	require.True(t, r.add(6))
	require.EqualValues(t, []FileNum{4, 5, 6}, r.logNums())
	require.EqualValues(t, 6, r.maxLogNum())

	// Trying to add a file past the limit fails.
	require.False(t, r.add(7))
	require.EqualValues(t, []FileNum{4, 5, 6}, r.logNums())
	require.EqualValues(t, 7, r.maxLogNum())

	// Trying to add a previously recycled file returns success, but the internal
	// state is unchanged.
	require.True(t, r.add(4))
	require.EqualValues(t, []FileNum{4, 5, 6}, r.logNums())
	require.EqualValues(t, 7, r.maxLogNum())

	// An error is returned if we try to pop an element other than the first.
	require.Regexp(t, `invalid 5 vs \[4 5 6\]`, r.pop(5))

	require.NoError(t, r.pop(4))
	require.EqualValues(t, []FileNum{5, 6}, r.logNums())

	// Log number 7 was already considered, so it won't be recycled.
	require.True(t, r.add(7))
	require.EqualValues(t, []FileNum{5, 6}, r.logNums())

	require.True(t, r.add(8))
	require.EqualValues(t, []FileNum{5, 6, 8}, r.logNums())
	require.EqualValues(t, 8, r.maxLogNum())

	require.NoError(t, r.pop(5))
	require.EqualValues(t, []FileNum{6, 8}, r.logNums())
	require.NoError(t, r.pop(6))
	require.EqualValues(t, []FileNum{8}, r.logNums())
	require.NoError(t, r.pop(8))
	require.EqualValues(t, []FileNum(nil), r.logNums())

	require.Regexp(t, `empty`, r.pop(9))
}

func TestRecycleLogs(t *testing.T) {
	mem := vfs.NewMem()
	d, err := Open("", &Options{
		FS: mem,
	})
	require.NoError(t, err)

	logNum := func() FileNum {
		d.mu.Lock()
		defer d.mu.Unlock()
		return d.mu.log.queue[len(d.mu.log.queue)-1]
	}
	logCount := func() int {
		d.mu.Lock()
		defer d.mu.Unlock()
		return len(d.mu.log.queue)
	}

	// Flush the memtable a few times, forcing rotation of the WAL. We should see
	// the recycled logs change as expected.
	require.EqualValues(t, []FileNum(nil), d.logRecycler.logNums())
	curLog := logNum()

	require.NoError(t, d.Flush())

	require.EqualValues(t, []FileNum{curLog}, d.logRecycler.logNums())
	curLog = logNum()

	require.NoError(t, d.Flush())

	require.EqualValues(t, []FileNum{curLog}, d.logRecycler.logNums())

	require.NoError(t, d.Close())

	d, err = Open("", &Options{
		FS: mem,
	})
	require.NoError(t, err)
	metrics := d.Metrics()
	if n := logCount(); n != int(metrics.WAL.Files) {
		t.Fatalf("expected %d WAL files, but found %d", n, metrics.WAL.Files)
	}
	if n := d.logRecycler.count(); n != int(metrics.WAL.ObsoleteFiles) {
		t.Fatalf("expected %d obsolete WAL files, but found %d", n, metrics.WAL.ObsoleteFiles)
	}
	if recycled := d.logRecycler.logNums(); len(recycled) != 0 {
		t.Fatalf("expected no recycled WAL files after recovery, but found %d", recycled)
	}
	require.NoError(t, d.Close())
}
