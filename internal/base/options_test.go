// Copyright 2018 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package base

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestLevelOptions(t *testing.T) {
	var opts *Options
	opts = opts.EnsureDefaults()

	testCases := []struct {
		level          int
		targetFileSize int64
	}{
		{0, 2 << 20},
		{1, (2 * 2) << 20},
		{2, (4 * 2) << 20},
		{3, (8 * 2) << 20},
		{4, (16 * 2) << 20},
		{5, (32 * 2) << 20},
		{6, (64 * 2) << 20},
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
  disable_wal=false
  l0_compaction_threshold=4
  l0_stop_writes_threshold=12
  lbase_max_bytes=67108864
  max_manifest_file_size=134217728
  max_open_files=1000
  mem_table_size=4194304
  mem_table_stop_writes_threshold=2
  min_compaction_rate=4194304
  min_flush_rate=4194304
  merger=pebble.concatenate
  table_property_collectors=[]
  wal_dir=

[Level "0"]
  block_restart_interval=16
  block_size=4096
  compression=Snappy
  filter_policy=none
  filter_type=table
  target_file_size=2097152
`

	var opts *Options
	opts = opts.EnsureDefaults()
	if v := opts.String(); expected != v {
		t.Fatalf("expected\n%s\nbut found\n%s", expected, v)
	}
}

func TestOptionsCheck(t *testing.T) {
	var opts *Options
	opts = opts.EnsureDefaults()
	s := opts.String()
	require.NoError(t, opts.Check(s))
	require.Regexp(t, `invalid key=value syntax`, opts.Check("foo\n"))

	tmp := *opts
	tmp.Comparer = &Comparer{Name: "foo"}
	require.Regexp(t, `comparer name from file.*!=.*`, tmp.Check(s))

	tmp = *opts
	tmp.Merger = &Merger{Name: "foo"}
	require.Regexp(t, `merger name from file.*!=.*`, tmp.Check(s))

	// RocksDB uses a similar (INI-style) syntax for the OPTIONS file, but
	// different section names and keys.
	s = `
[CFOptions "default"]
  comparator=rocksdb-comparer
  merge_operator=rocksdb-merger
`
	tmp = *opts
	tmp.Comparer = &Comparer{Name: "foo"}
	require.Regexp(t, `comparer name from file.*!=.*`, tmp.Check(s))

	tmp.Comparer = &Comparer{Name: "rocksdb-comparer"}
	tmp.Merger = &Merger{Name: "foo"}
	require.Regexp(t, `merger name from file.*!=.*`, tmp.Check(s))

	tmp.Merger = &Merger{Name: "rocksdb-merger"}
	require.NoError(t, tmp.Check(s))

	// RocksDB allows the merge operator to be unspecified, in which case it
	// shows up as "nullptr".
	s = `
[CFOptions "default"]
  merge_operator=nullptr
`
	tmp = *opts
	require.NoError(t, tmp.Check(s))
}
