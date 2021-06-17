// Copyright 2018 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package pebble

import (
	"testing"
	"time"

	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/pebble/internal/base"
	"github.com/cockroachdb/pebble/vfs"
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
  cache_size=8388608
  cleaner=delete
  compaction_debt_concurrency=1073741824
  comparer=leveldb.BytewiseComparator
  delete_range_flush_delay=0s
  disable_wal=false
  flush_split_bytes=4194304
  l0_compaction_concurrency=10
  l0_compaction_threshold=4
  l0_stop_writes_threshold=12
  lbase_max_bytes=67108864
  max_concurrent_compactions=1
  max_manifest_file_size=134217728
  max_open_files=1000
  mem_table_size=4194304
  mem_table_stop_writes_threshold=2
  min_compaction_rate=4194304
  min_deletion_rate=0
  min_flush_rate=1048576
  merger=pebble.concatenate
  read_compaction_rate=16000
  read_sampling_multiplier=16
  strict_wal_tail=true
  table_property_collectors=[]
  wal_dir=
  wal_bytes_per_sync=0

[Level "0"]
  block_restart_interval=16
  block_size=4096
  compression=Snappy
  filter_policy=none
  filter_type=table
  index_block_size=4096
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

type testCleaner struct{}

func (testCleaner) Clean(fs vfs.FS, fileType base.FileType, path string) error {
	return nil
}

func (testCleaner) String() string {
	return "test-cleaner"
}

func TestOptionsParse(t *testing.T) {
	testComparer := *DefaultComparer
	testComparer.Name = "test-comparer"
	testMerger := *DefaultMerger
	testMerger.Name = "test-merger"
	var newCacheSize int64

	hooks := &ParseHooks{
		NewCache: func(size int64) *Cache {
			newCacheSize = size
			return nil
		},
		NewCleaner: func(name string) (Cleaner, error) {
			if name == (testCleaner{}).String() {
				return testCleaner{}, nil
			}
			return nil, errors.Errorf("unknown cleaner: %q", name)
		},
		NewComparer: func(name string) (*Comparer, error) {
			if name == testComparer.Name {
				return &testComparer, nil
			}
			return nil, errors.Errorf("unknown comparer: %q", name)
		},
		NewMerger: func(name string) (*Merger, error) {
			if name == testMerger.Name {
				return &testMerger, nil
			}
			return nil, errors.Errorf("unknown merger: %q", name)
		},
	}

	testCases := []struct {
		cleaner  Cleaner
		comparer *Comparer
		merger   *Merger
	}{
		{testCleaner{}, nil, nil},
		{nil, &testComparer, nil},
		{nil, nil, &testMerger},
	}
	for _, c := range testCases {
		t.Run("", func(t *testing.T) {
			var opts Options
			opts.Comparer = c.comparer
			opts.Merger = c.merger
			opts.WALDir = "wal"
			opts.Levels = make([]LevelOptions, 3)
			opts.Levels[0].BlockSize = 1024
			opts.Levels[1].BlockSize = 2048
			opts.Levels[2].BlockSize = 4096
			opts.Experimental.CompactionDebtConcurrency = 100
			opts.Experimental.DeleteRangeFlushDelay = 10 * time.Second
			opts.Experimental.MinDeletionRate = 200
			opts.Experimental.ReadCompactionRate = 300
			opts.Experimental.ReadSamplingMultiplier = 400
			opts.EnsureDefaults()
			str := opts.String()

			newCacheSize = 0
			var parsedOptions Options
			require.NoError(t, parsedOptions.Parse(str, hooks))
			parsedStr := parsedOptions.String()
			if str != parsedStr {
				t.Fatalf("expected\n%s\nbut found\n%s", str, parsedStr)
			}
			require.Nil(t, parsedOptions.Cache)
			require.NotEqual(t, newCacheSize, 0)
		})
	}
}

func TestOptionsValidate(t *testing.T) {
	testCases := []struct {
		options  string
		expected string
	}{
		{``, ``},
		{`
[Options]
  l0_compaction_concurrency=0
`,
			`L0CompactionConcurrency \(0\) must be >= 1`,
		},
		{`
[Options]
  l0_compaction_threshold=2
  l0_stop_writes_threshold=1
`,
			`L0StopWritesThreshold .* must be >= L0CompactionThreshold .*`,
		},
		{`
[Options]
  mem_table_size=4294967296
`,
			`MemTableSize \(4\.0 G\) must be < 4\.0 G`,
		},
		{`
[Options]
  mem_table_stop_writes_threshold=1
`,
			`MemTableStopWritesThreshold .* must be >= 2`,
		},
	}

	for _, c := range testCases {
		t.Run("", func(t *testing.T) {
			var opts Options
			opts.EnsureDefaults()
			require.NoError(t, opts.Parse(c.options, nil))
			err := opts.Validate()
			if c.expected == "" {
				require.NoError(t, err)
			} else {
				require.Error(t, err)
				require.Regexp(t, c.expected, err.Error())
			}
		})
	}
}
