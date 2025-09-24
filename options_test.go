// Copyright 2018 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package pebble

import (
	"bytes"
	"fmt"
	"io"
	"math/rand/v2"
	"runtime"
	"strings"
	"testing"
	"time"

	"github.com/cockroachdb/crlib/crstrings"
	"github.com/cockroachdb/datadriven"
	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/pebble/internal/base"
	"github.com/cockroachdb/pebble/internal/strparse"
	"github.com/cockroachdb/pebble/internal/testkeys"
	"github.com/cockroachdb/pebble/internal/testutils"
	"github.com/cockroachdb/pebble/sstable/block"
	"github.com/cockroachdb/pebble/sstable/tablefilters/bloom"
	"github.com/cockroachdb/pebble/vfs"
	"github.com/cockroachdb/pebble/wal"
	"github.com/stretchr/testify/require"
)

// randomizeForTesting randomizes some default options. Currently, it's
// used for testing under a random format major version in some tests.
func (o *Options) randomizeForTesting(t testing.TB) {
	if o.Logger == nil {
		o.Logger = testutils.Logger{T: t}
	}
	if o.FormatMajorVersion == FormatDefault {
		// Pick a random format major version from the range
		// [FormatMinSupported, FormatNewest].
		n := rand.IntN(int(internalFormatNewest - FormatMinSupported + 1))
		o.FormatMajorVersion = FormatMinSupported + FormatMajorVersion(n)
		t.Logf("Running %s with format major version %s", t.Name(), o.FormatMajorVersion.String())
	}
	// Randomize value separation if using a format major version that supports it.
	if o.FormatMajorVersion >= FormatValueSeparation && o.Experimental.ValueSeparationPolicy == nil {
		switch rand.IntN(4) {
		case 0:
			// 25% of the time, use defaults (leave nil for EnsureDefaults).
		case 1:
			// 25% of the time, disable value separation.
			o.Experimental.ValueSeparationPolicy = func() ValueSeparationPolicy {
				return ValueSeparationPolicy{Enabled: false}
			}
		default:
			// 50% of the time, enable with random parameters.
			lowPri := 0.1 + rand.Float64()*0.9 // [0.1, 1.0)
			policy := ValueSeparationPolicy{
				Enabled:                true,
				MinimumSize:            1 << rand.IntN(10), // [1, 512]
				MinimumMVCCGarbageSize: 5 + rand.IntN(11),  // [5, 15]
				MaxBlobReferenceDepth:  1 + rand.IntN(10),  // [1, 10]
				// Constrain the rewrite minimum age to [0, 15s).
				RewriteMinimumAge:        time.Duration(rand.IntN(15)) * time.Second,
				GarbageRatioLowPriority:  lowPri,
				GarbageRatioHighPriority: lowPri + rand.Float64()*(1.0-lowPri), // [lowPri, 1.0)
			}
			o.Experimental.ValueSeparationPolicy = func() ValueSeparationPolicy { return policy }
		}
	}
	if rand.IntN(2) == 0 {
		o.Experimental.IteratorTracking.PollInterval = 100 * time.Millisecond
		o.Experimental.IteratorTracking.MaxAge = 10 * time.Second
	}
	o.EnsureDefaults()
}

func testingRandomized(t testing.TB, o *Options) *Options {
	o.randomizeForTesting(t)
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

[Value Separation]
  enabled=true
  minimum_size=256
  minimum_mvcc_garbage_size=1024
  max_blob_reference_depth=10
  rewrite_minimum_age=5m0s
  garbage_ratio_low_priority=0.10
  garbage_ratio_high_priority=0.20

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

func TestWALRecoveryDirValidation(t *testing.T) {
	storeDir := "/mnt/foo"
	mem := vfs.NewMem()

	// Test that when a WALRecoveryDir is NOT the current secondary
	// (i.e., it's an old secondary we're recovering from), we should NOT
	// validate its identifier, even if it has one.
	oldSecondaryDir := "/mnt/old-secondary"
	err := mem.MkdirAll(oldSecondaryDir, 0755)
	require.NoError(t, err)

	// Create stable_identifier file in the old secondary with its own ID.
	oldIdentifierFile := mem.PathJoin(oldSecondaryDir, wal.StableIdentifierFilename)
	err = writeTestIdentifierToFile(mem, oldIdentifierFile, "11111111111111111111111111111111")
	require.NoError(t, err)

	currentSecondaryDir := "/mnt/current-secondary"
	err = mem.MkdirAll(currentSecondaryDir, 0755)
	require.NoError(t, err)

	opts := &Options{
		FS: mem,
		WALFailover: &WALFailoverOptions{
			Secondary: wal.Dir{
				FS:      mem,
				Dirname: currentSecondaryDir,
				ID:      "22222222222222222222222222222222",
			},
		},
		WALRecoveryDirs: []wal.Dir{
			{
				// Old secondary with an ID from when it was the current secondary.
				// This ID doesn't match the identifier file in oldSecondaryDir,
				// but we shouldn't validate because it's not the current secondary.
				FS:      mem,
				Dirname: oldSecondaryDir,
				ID:      "99999999999999999999999999999999",
			},
		},
	}
	opts.EnsureDefaults()

	// This should succeed because oldSecondaryDir is not the current secondary,
	// so we don't validate its identifier.
	err = opts.checkWALDir(storeDir, oldSecondaryDir, "test context")
	require.NoError(t, err)
}

// writeTestIdentifierToFile is a helper function to write an identifier to a file
func writeTestIdentifierToFile(fs vfs.FS, filename, identifier string) error {
	f, err := fs.Create(filename, "pebble-wal")
	if err != nil {
		return err
	}
	defer f.Close()

	_, err = io.WriteString(f, identifier)
	if err != nil {
		return err
	}

	return f.Sync()
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
			opts.DeletionPacing.BaselineRate = func() uint64 { return 200 }
			opts.WALFailover = &WALFailoverOptions{
				Secondary: wal.Dir{Dirname: "wal_secondary", FS: vfs.Default},
			}
			opts.Experimental.ReadCompactionRate = 300
			opts.Experimental.ReadSamplingMultiplier = 400
			opts.Experimental.NumDeletionsThreshold = 500
			opts.Experimental.DeletionSizeRatioThreshold = 0.7
			opts.Experimental.TombstoneDenseCompactionThreshold = func() float64 { return 0.2 }
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

func TestOptionsParseInvalidLevel(t *testing.T) {
	str := `
[Options]
[Level 1]
  block_restart_interval=8
  block_size=10
[Level 10]
  block_restart_interval=8
  block_size=10
`
	o := &Options{}
	require.Error(t, o.Parse(str, nil))

	str = `
[Options]
[Level -1]
  block_restart_interval=8
  block_size=10
`
	require.Error(t, o.Parse(str, nil))

	str = `
[Options]
[Level 100000000000000000000000000]
  block_restart_interval=8
  block_size=10
`
	require.Error(t, o.Parse(str, nil))
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
	t.Run("wal-failover", func(t *testing.T) {
		var opts Options
		opts.EnsureDefaults()
		opts.WALFailover = &WALFailoverOptions{}
		err := opts.Validate()
		require.Error(t, err)
		require.Regexp(t, "Secondary.FS is required", err.Error())
	})
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
	o.randomizeForTesting(t)

	var profile DBCompressionSettings
	o.ApplyCompressionSettings(func() DBCompressionSettings {
		return profile
	})

	profile = UniformDBCompressionSettings(block.FastestCompression)
	profile.Levels[1] = block.FastestCompression
	profile.Levels[2] = block.BalancedCompression
	profile.Levels[3] = block.GoodCompression
	require.Equal(t, block.FastestCompression, o.Levels[1].Compression())
	require.Equal(t, block.BalancedCompression, o.Levels[2].Compression())
	require.Equal(t, block.GoodCompression, o.Levels[3].Compression())

	profile = UniformDBCompressionSettings(block.FastestCompression)
	require.Equal(t, block.FastestCompression, o.Levels[1].Compression())
	require.Equal(t, block.FastestCompression, o.Levels[2].Compression())
	require.Equal(t, block.FastestCompression, o.Levels[3].Compression())
}

func TestApplyDBTableFilterPolicy(t *testing.T) {
	var o Options
	o.randomizeForTesting(t)

	var profile DBTableFilterPolicy
	o.ApplyTableFilterPolicy(func() DBTableFilterPolicy {
		return profile
	})

	profile = UniformDBTableFilterPolicy(bloom.FilterPolicy(10))
	profile[1] = bloom.FilterPolicy(1)
	profile[2] = bloom.FilterPolicy(2)
	profile[3] = bloom.FilterPolicy(3)
	require.Equal(t, bloom.FilterPolicy(1), o.Levels[1].TableFilterPolicy())
	require.Equal(t, bloom.FilterPolicy(2), o.Levels[2].TableFilterPolicy())
	require.Equal(t, bloom.FilterPolicy(3), o.Levels[3].TableFilterPolicy())

	profile = UniformDBTableFilterPolicy(bloom.FilterPolicy(15))
	require.Equal(t, bloom.FilterPolicy(15), o.Levels[1].TableFilterPolicy())
	require.Equal(t, bloom.FilterPolicy(15), o.Levels[2].TableFilterPolicy())
	require.Equal(t, bloom.FilterPolicy(15), o.Levels[3].TableFilterPolicy())
}

func TestStaticSpanPolicyFunc(t *testing.T) {
	var buf bytes.Buffer
	datadriven.RunTest(t, "testdata/static_span_policy_func", func(t *testing.T, td *datadriven.TestData) string {
		buf.Reset()
		var inputSpanPolicies []SpanPolicy
		for _, cmdArg := range td.CmdArgs {
			p := strparse.MakeParser("-:", cmdArg.String())
			var sap SpanPolicy
			sap.KeyRange.Start = []byte(p.Next())
			p.Expect("-")
			sap.KeyRange.End = []byte(p.Next())
			p.Expect(":")
			switch tok := p.Next(); tok {
			case "lowlatency":
				sap.ValueStoragePolicy = ValueStorageLowReadLatency
			case "latencytolerant":
				sap.ValueStoragePolicy = ValueStorageLatencyTolerant
			default:
				t.Fatalf("unknown policy: %s", tok)
			}
			inputSpanPolicies = append(inputSpanPolicies, sap)
		}

		spf := MakeStaticSpanPolicyFunc(testkeys.Comparer.Compare, inputSpanPolicies...)
		for l := range crstrings.LinesSeq(td.Input) {
			b := base.ParseUserKeyBounds(l)
			policy, err := spf(b)
			require.NoError(t, err)
			fmt.Fprintf(&buf, "%s\n", policy.String())
		}
		return buf.String()
	})
}
