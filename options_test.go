// Copyright 2018 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package pebble

import (
	"math/rand/v2"
	"runtime"
	"strings"
	"testing"
	"time"

	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/pebble/internal/base"
	"github.com/cockroachdb/pebble/internal/testkeys"
	"github.com/cockroachdb/pebble/sstable/block"
	"github.com/cockroachdb/pebble/vfs"
	"github.com/cockroachdb/pebble/wal"
	"github.com/stretchr/testify/require"
)

// testingRandomized randomizes some default options. Currently, it's
// used for testing under a random format major version in some tests.
func (o *Options) testingRandomized(t testing.TB) *Options {
	if o == nil {
		o = &Options{}
	}
	if o.Logger == nil {
		o.Logger = testLogger{t: t}
	}
	if o.FormatMajorVersion == FormatDefault {
		// Pick a random format major version from the range
		// [FormatMinSupported, FormatNewest].
		n := rand.IntN(int(internalFormatNewest - FormatMinSupported + 1))
		o.FormatMajorVersion = FormatMinSupported + FormatMajorVersion(n)
		t.Logf("Running %s with format major version %s", t.Name(), o.FormatMajorVersion.String())
	}
	// Enable columnar blocks if using a format major version that supports it.
	if o.FormatMajorVersion >= FormatColumnarBlocks && o.Experimental.EnableColumnarBlocks == nil && rand.Int64N(4) > 0 {
		o.Experimental.EnableColumnarBlocks = func() bool { return true }
	}
	// Enable value separation if using a format major version that supports it.
	if o.FormatMajorVersion >= FormatValueSeparation && o.Experimental.ValueSeparationPolicy == nil && rand.Int64N(4) > 0 {
		policy := ValueSeparationPolicy{
			Enabled:               true,
			MinimumSize:           1 << rand.IntN(10), // [1, 512]
			MaxBlobReferenceDepth: rand.IntN(10) + 1,  // [1, 10)
			// Constrain the rewrite minimum age to [0, 15s).
			RewriteMinimumAge: time.Duration(rand.IntN(15)) * time.Second,
		}
		o.Experimental.ValueSeparationPolicy = func() ValueSeparationPolicy { return policy }
	}
	o.EnsureDefaults()
	return o
}

func testingRandomized(t testing.TB, o *Options) *Options {
	o.testingRandomized(t)
	return o
}

func TestTargetFileSize(t *testing.T) {
	opts := DefaultOptions()

	testCases := []struct {
		level          int
		baseLevel      int
		targetFileSize int64
	}{
		{level: 0, baseLevel: 1, targetFileSize: 2 << 20},
		{level: 1, baseLevel: 1, targetFileSize: 2 * 2 << 20},
		{level: 2, baseLevel: 1, targetFileSize: 4 * 2 << 20},
		{level: 3, baseLevel: 1, targetFileSize: 8 * 2 << 20},
		{level: 4, baseLevel: 1, targetFileSize: 16 * 2 << 20},
		{level: 5, baseLevel: 1, targetFileSize: 32 * 2 << 20},
		{level: 6, baseLevel: 1, targetFileSize: 64 * 2 << 20},

		{level: 3, baseLevel: 3, targetFileSize: 2 * 2 << 20},
		{level: 4, baseLevel: 3, targetFileSize: 4 * 2 << 20},
		{level: 5, baseLevel: 3, targetFileSize: 8 * 2 << 20},
	}
	for _, c := range testCases {
		require.Equal(t, c.targetFileSize, opts.TargetFileSize(c.level, c.baseLevel))
	}
}

func TestDefaultOptionsString(t *testing.T) {
	n := runtime.GOMAXPROCS(8)
	defer runtime.GOMAXPROCS(n)

	const expected = `[Version]
  pebble_version=0.1

[Options]
  bytes_per_sync=524288
  cache_size=8388608
  cleaner=delete
  compaction_debt_concurrency=1073741824
  compaction_garbage_fraction_for_max_concurrency=0.40
  comparer=leveldb.BytewiseComparator
  disable_wal=false
  enable_columnar_blocks=true
  flush_delay_delete_range=0s
  flush_delay_range_key=0s
  flush_split_bytes=4194304
  format_major_version=13
  key_schema=DefaultKeySchema(leveldb.BytewiseComparator,16)
  l0_compaction_concurrency=10
  l0_compaction_file_threshold=500
  l0_compaction_threshold=4
  l0_stop_writes_threshold=12
  lbase_max_bytes=67108864
  concurrent_compactions=1
  max_concurrent_compactions=1
  max_concurrent_downloads=1
  max_manifest_file_size=134217728
  max_open_files=1000
  mem_table_size=4194304
  mem_table_stop_writes_threshold=2
  min_deletion_rate=0
  free_space_threshold_bytes=17179869184
  free_space_timeframe=10s
  obsolete_bytes_max_ratio=0.200000
  obsolete_bytes_timeframe=5m0s
  merger=pebble.concatenate
  multilevel_compaction_heuristic=wamp(0.00, false)
  read_compaction_rate=16000
  read_sampling_multiplier=16
  num_deletions_threshold=100
  deletion_size_ratio_threshold=0.500000
  tombstone_dense_compaction_threshold=0.100000
  strict_wal_tail=true
  table_cache_shards=8
  validate_on_ingest=false
  wal_dir=
  wal_bytes_per_sync=0
  secondary_cache_size_bytes=0
  create_on_shared=0

[Level "0"]
  block_restart_interval=16
  block_size=4096
  block_size_threshold=90
  compression=Snappy
  filter_policy=none
  filter_type=table
  index_block_size=4096
  target_file_size=2097152

[Level "1"]
  block_restart_interval=16
  block_size=4096
  block_size_threshold=90
  compression=Snappy
  filter_policy=none
  filter_type=table
  index_block_size=4096
  target_file_size=4194304

[Level "2"]
  block_restart_interval=16
  block_size=4096
  block_size_threshold=90
  compression=Snappy
  filter_policy=none
  filter_type=table
  index_block_size=4096
  target_file_size=8388608

[Level "3"]
  block_restart_interval=16
  block_size=4096
  block_size_threshold=90
  compression=Snappy
  filter_policy=none
  filter_type=table
  index_block_size=4096
  target_file_size=16777216

[Level "4"]
  block_restart_interval=16
  block_size=4096
  block_size_threshold=90
  compression=Snappy
  filter_policy=none
  filter_type=table
  index_block_size=4096
  target_file_size=33554432

[Level "5"]
  block_restart_interval=16
  block_size=4096
  block_size_threshold=90
  compression=Snappy
  filter_policy=none
  filter_type=table
  index_block_size=4096
  target_file_size=67108864

[Level "6"]
  block_restart_interval=16
  block_size=4096
  block_size_threshold=90
  compression=Snappy
  filter_policy=none
  filter_type=table
  index_block_size=4096
  target_file_size=134217728
`

	require.Equal(t, expected, DefaultOptions().String())
}

func TestOptionsCheckCompatibility(t *testing.T) {
	storeDir := "/mnt/foo"
	opts := DefaultOptions()
	s := opts.String()
	require.NoError(t, opts.CheckCompatibility(storeDir, s))
	require.Regexp(t, `invalid key=value syntax`, opts.CheckCompatibility(storeDir, "foo\n"))

	tmp := *opts
	tmp.Comparer = &Comparer{Name: "foo"}
	require.Regexp(t, `comparer name from file.*!=.*`, tmp.CheckCompatibility(storeDir, s))

	tmp = *opts
	tmp.Merger = &Merger{Name: "foo"}
	require.Regexp(t, `merger name from file.*!=.*`, tmp.CheckCompatibility(storeDir, s))

	// RocksDB uses a similar (INI-style) syntax for the OPTIONS file, but
	// different section names and keys.
	s = `
[CFOptions "default"]
  comparator=rocksdb-comparer
  merge_operator=rocksdb-merger
`
	tmp = *opts
	tmp.Comparer = &Comparer{Name: "foo"}
	require.Regexp(t, `comparer name from file.*!=.*`, tmp.CheckCompatibility(storeDir, s))

	tmp.Comparer = &Comparer{Name: "rocksdb-comparer"}
	tmp.Merger = &Merger{Name: "foo"}
	require.Regexp(t, `merger name from file.*!=.*`, tmp.CheckCompatibility(storeDir, s))

	tmp.Merger = &Merger{Name: "rocksdb-merger"}
	require.NoError(t, tmp.CheckCompatibility(storeDir, s))

	// RocksDB allows the merge operator to be unspecified, in which case it
	// shows up as "nullptr".
	s = `
[CFOptions "default"]
  merge_operator=nullptr
`
	tmp = *opts
	require.NoError(t, tmp.CheckCompatibility(storeDir, s))

	// Check that an OPTIONS file that configured an explicit WALDir that will
	// no longer be used errors if it's not also present in WALRecoveryDirs.
	//require.Equal(t, ErrMissingWALRecoveryDir{Dir: "external-wal-dir"},
	err := DefaultOptions().CheckCompatibility(storeDir, `
[Options]
  wal_dir=external-wal-dir
`)
	var missingWALRecoveryDirErr ErrMissingWALRecoveryDir
	require.True(t, errors.As(err, &missingWALRecoveryDirErr))
	require.Equal(t, "external-wal-dir", missingWALRecoveryDirErr.Dir)

	// But not if it's configured as a WALRecoveryDir or current WALDir.
	opts = &Options{WALRecoveryDirs: []wal.Dir{{Dirname: "external-wal-dir"}}}
	opts.EnsureDefaults()
	require.NoError(t, opts.CheckCompatibility(storeDir, `
[Options]
  wal_dir=external-wal-dir
`))
	opts = &Options{WALDir: "external-wal-dir"}
	opts.EnsureDefaults()
	require.NoError(t, opts.CheckCompatibility(storeDir, `
[Options]
  wal_dir=external-wal-dir
`))

	// Check that an OPTIONS file that was configured without an explicit WALDir
	// errors out if the store path is not in WALRecoveryDirs.
	opts = &Options{WALDir: "external-wal-dir"}
	opts.EnsureDefaults()
	err = opts.CheckCompatibility(storeDir, `
[Options]
  wal_dir=
`)
	require.True(t, errors.As(err, &missingWALRecoveryDirErr))
	require.Equal(t, storeDir, missingWALRecoveryDirErr.Dir)

	// Should be the same as above.
	err = opts.CheckCompatibility(storeDir, ``)
	require.True(t, errors.As(err, &missingWALRecoveryDirErr))
	require.Equal(t, storeDir, missingWALRecoveryDirErr.Dir)

	opts.WALRecoveryDirs = []wal.Dir{{Dirname: storePathIdentifier}}
	require.NoError(t, DefaultOptions().CheckCompatibility(storeDir, `
[Options]
  wal_dir=
`))

	// Should be the same as above.
	require.NoError(t, DefaultOptions().CheckCompatibility(storeDir, ``))

	// Check that an OPTIONS file that configured a secondary failover WAL dir
	// that will no longer be used errors if it's not also present in
	// WALRecoveryDirs.
	err = DefaultOptions().CheckCompatibility(storeDir, `
[Options]

[WAL Failover]
  secondary_dir=failover-wal-dir
`)
	require.True(t, errors.As(err, &missingWALRecoveryDirErr))
	require.Equal(t, "failover-wal-dir", missingWALRecoveryDirErr.Dir)

	// But not if it's configured as a WALRecoveryDir or current failover
	// secondary dir.
	opts = &Options{WALRecoveryDirs: []wal.Dir{{Dirname: "failover-wal-dir"}}}
	opts.EnsureDefaults()
	require.NoError(t, opts.CheckCompatibility(storeDir, `
[Options]

[WAL Failover]
  secondary_dir=failover-wal-dir
`))
	opts = &Options{WALFailover: &WALFailoverOptions{Secondary: wal.Dir{Dirname: "failover-wal-dir"}}}
	opts.EnsureDefaults()
	require.NoError(t, opts.CheckCompatibility(storeDir, `
[Options]

[WAL Failover]
  secondary_dir=failover-wal-dir
`))
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

	hooks := &ParseHooks{
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
			opts.Levels[0].BlockSize = 1024
			opts.Levels[1].BlockSize = 2048
			opts.Levels[2].BlockSize = 4096
			opts.Experimental.CompactionDebtConcurrency = 100
			opts.FlushDelayDeleteRange = 10 * time.Second
			opts.FlushDelayRangeKey = 11 * time.Second
			opts.Experimental.LevelMultiplier = 5
			opts.TargetByteDeletionRate = 200
			opts.WALFailover = &WALFailoverOptions{
				Secondary: wal.Dir{Dirname: "wal_secondary", FS: vfs.Default},
			}
			opts.Experimental.ReadCompactionRate = 300
			opts.Experimental.ReadSamplingMultiplier = 400
			opts.Experimental.NumDeletionsThreshold = 500
			opts.Experimental.DeletionSizeRatioThreshold = 0.7
			opts.Experimental.TombstoneDenseCompactionThreshold = 0.2
			opts.Experimental.FileCacheShards = 500
			opts.Experimental.SecondaryCacheSizeBytes = 1024
			opts.Experimental.ValueSeparationPolicy = func() ValueSeparationPolicy {
				return ValueSeparationPolicy{
					Enabled:               true,
					MinimumSize:           1024,
					MaxBlobReferenceDepth: 10,
					RewriteMinimumAge:     15 * time.Minute,
				}
			}
			opts.EnsureDefaults()
			str := opts.String()

			var parsedOptions Options
			require.NoError(t, parsedOptions.Parse(str, hooks))
			parsedStr := parsedOptions.String()
			if str != parsedStr {
				t.Fatalf("expected\n%s\nbut found\n%s", str, parsedStr)
			}
			require.Nil(t, parsedOptions.Cache)
		})
	}
}

func TestOptionsParseLevelNoQuotes(t *testing.T) {
	withQuotes := `
[Options]
[Level "1"]
  block_restart_interval=8
  block_size=10
[Level "6"]
  block_restart_interval=8
  block_size=10
`
	withoutQuotes := `
[Options]
[Level 1]
  block_restart_interval=8
  block_size=10
[Level 6]
  block_restart_interval=8
  block_size=10
`
	o1 := &Options{}
	require.NoError(t, o1.Parse(withQuotes, nil))
	o1.EnsureDefaults()

	o2 := &Options{}
	require.NoError(t, o2.Parse(withoutQuotes, nil))
	o2.EnsureDefaults()

	require.Equal(t, o1.String(), o2.String())
}

func TestOptionsParseComparerOverwrite(t *testing.T) {
	// Test that an unrecognized comparer in the OPTIONS file does not nil out
	// the Comparer field.
	o := &Options{Comparer: testkeys.Comparer}
	err := o.Parse(`[Options]
comparer=unrecognized`, nil)
	require.NoError(t, err)
	require.Equal(t, testkeys.Comparer, o.Comparer)
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
			`MemTableSize \(4\.0GB\) must be < [2|4]\.0GB`,
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

func TestKeyCategories(t *testing.T) {
	kc := MakeUserKeyCategories(base.DefaultComparer.Compare, []UserKeyCategory{
		{Name: "b", UpperBound: []byte("b")},
		{Name: "dd", UpperBound: []byte("dd")},
		{Name: "e", UpperBound: []byte("e")},
		{Name: "h", UpperBound: []byte("h")},
		{Name: "o", UpperBound: nil},
	}...)

	for _, tc := range []struct {
		key      string
		expected string
	}{
		{key: "a", expected: "b"},
		{key: "a123", expected: "b"},
		{key: "az", expected: "b"},
		{key: "b", expected: "dd"},
		{key: "b123", expected: "dd"},
		{key: "c", expected: "dd"},
		{key: "d", expected: "dd"},
		{key: "dd", expected: "e"},
		{key: "dd0", expected: "e"},
		{key: "de", expected: "e"},
		{key: "e", expected: "h"},
		{key: "e1", expected: "h"},
		{key: "f", expected: "h"},
		{key: "h", expected: "o"},
		{key: "h1", expected: "o"},
		{key: "z", expected: "o"},
	} {
		t.Run(tc.key, func(t *testing.T) {
			res := kc.CategorizeKey([]byte(tc.key))
			require.Equal(t, tc.expected, res)
		})
	}

	for _, tc := range []struct {
		rng      string
		expected string
	}{
		{rng: "a-a", expected: "b"},
		{rng: "a-a1", expected: "b"},
		{rng: "a-b", expected: "b-dd"},
		{rng: "b1-b5", expected: "dd"},
		{rng: "b1-dd0", expected: "dd-e"},
		{rng: "b1-g", expected: "dd-h"},
		{rng: "b1-j", expected: "dd-o"},
		{rng: "b1-z", expected: "dd-o"},
		{rng: "dd-e", expected: "e-h"},
		{rng: "h-i", expected: "o"},
		{rng: "i-i", expected: "o"},
		{rng: "i-j", expected: "o"},
		{rng: "a-z", expected: "b-o"},
	} {
		t.Run(tc.rng, func(t *testing.T) {
			keys := strings.SplitN(tc.rng, "-", 2)
			res := kc.CategorizeKeyRange([]byte(keys[0]), []byte(keys[1]))
			require.Equal(t, tc.expected, res)
		})
	}
}

func TestApplyDBCompressionSettings(t *testing.T) {
	var o Options
	o.testingRandomized(t)

	var profile DBCompressionSettings
	o.ApplyCompressionSettings(func() DBCompressionSettings {
		return profile
	})

	profile = UniformDBCompressionSettings("Test", block.FastestCompression)
	profile.Levels[1] = block.FastestCompression
	profile.Levels[2] = block.BalancedCompression
	profile.Levels[3] = block.GoodCompression
	require.Equal(t, block.FastestCompression, o.Levels[1].Compression())
	require.Equal(t, block.BalancedCompression, o.Levels[2].Compression())
	require.Equal(t, block.GoodCompression, o.Levels[3].Compression())

	profile = UniformDBCompressionSettings("Test2", block.FastestCompression)
	require.Equal(t, block.FastestCompression, o.Levels[1].Compression())
	require.Equal(t, block.FastestCompression, o.Levels[2].Compression())
	require.Equal(t, block.FastestCompression, o.Levels[3].Compression())
}
