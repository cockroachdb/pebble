// Copyright 2018 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package db

import (
	"testing"
)

func TestLevelOptions(t *testing.T) {
	var opts *Options
	opts = opts.EnsureDefaults()

	testCases := []struct {
		level          int
		targetFileSize int64
	}{
		{0, 4 << 20},
		{1, (2 * 4) << 20},
		{2, (4 * 4) << 20},
		{3, (8 * 4) << 20},
		{4, (16 * 4) << 20},
		{5, (32 * 4) << 20},
		{6, (64 * 4) << 20},
	}
	for _, c := range testCases {
		l := opts.Level(c.level)
		if c.targetFileSize != l.TargetFileSize {
			t.Fatalf("%d: expected target-file-size %d, but found %d",
				c.level, c.targetFileSize, l.TargetFileSize)
		}
	}
}

func TestOptionsString(t *testing.T) {
	const expected = `[Version]
  pebble_version=0.1

[Options]
  bytes_per_sync=524288
  cache_size=0
  comparer=leveldb.BytewiseComparator
  l0_compaction_threshold=4
  l0_slowdown_writes_threshold=8
  l0_stop_writes_threshold=12
  l1_max_bytes=67108864
  max_open_files=1000
  mem_table_size=4194304
  mem_table_stop_writes_threshold=2
  merger=pebble.concatenate

[Level "0"]
  block_restart_interval=16
  block_size=4096
  compression=Snappy
  filter_policy=none
  filter_type=block
  target_file_size=4194304
`

	var opts *Options
	opts = opts.EnsureDefaults()
	if v := opts.String(); expected != v {
		t.Fatalf("expected\n%s\nbut found\n%s", expected, v)
	}
}
