// Copyright 2019 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package metamorphic

import (
	"bytes"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"strconv"
	"time"

	"github.com/cockroachdb/pebble"
	"github.com/cockroachdb/pebble/bloom"
	"github.com/cockroachdb/pebble/internal/cache"
	"github.com/cockroachdb/pebble/internal/testkeys"
	"github.com/cockroachdb/pebble/sstable"
	"github.com/cockroachdb/pebble/vfs"
	"golang.org/x/exp/rand"
)

func parseOptions(opts *testOptions, data string) error {
	hooks := &pebble.ParseHooks{
		NewCache: pebble.NewCache,
		NewFilterPolicy: func(name string) (pebble.FilterPolicy, error) {
			if name == "none" {
				return nil, nil
			}
			return bloom.FilterPolicy(10), nil
		},
		SkipUnknown: func(name, value string) bool {
			switch name {
			case "TestOptions":
				return true
			case "TestOptions.strictfs":
				opts.strictFS = true
				return true
			case "TestOptions.ingest_using_apply":
				opts.ingestUsingApply = true
				return true
			case "TestOptions.replace_single_delete":
				opts.replaceSingleDelete = true
				return true
			case "TestOptions.use_disk":
				opts.useDisk = true
				return true
			case "TestOptions.initial_state_desc":
				opts.initialStateDesc = value
				return true
			case "TestOptions.initial_state_path":
				opts.initialStatePath = value
				return true
			case "TestOptions.threads":
				v, err := strconv.Atoi(value)
				if err != nil {
					panic(err)
				}
				opts.threads = v
				return true
			case "TestOptions.use_block_property_collector":
				opts.useBlockPropertyCollector = true
				opts.opts.BlockPropertyCollectors = blockPropertyCollectorConstructors
				return true
			case "TestOptions.enable_value_blocks":
				opts.opts.Experimental.EnableValueBlocks = func() bool { return true }
				return true
			default:
				return false
			}
		},
	}
	err := opts.opts.Parse(data, hooks)
	return err
}

func optionsToString(opts *testOptions) string {
	var buf bytes.Buffer
	if opts.strictFS {
		fmt.Fprint(&buf, "  strictfs=true\n")
	}
	if opts.ingestUsingApply {
		fmt.Fprint(&buf, "  ingest_using_apply=true\n")
	}
	if opts.replaceSingleDelete {
		fmt.Fprint(&buf, "  replace_single_delete=true\n")
	}
	if opts.useDisk {
		fmt.Fprint(&buf, "  use_disk=true\n")
	}
	if opts.initialStatePath != "" {
		fmt.Fprintf(&buf, "  initial_state_path=%s\n", opts.initialStatePath)
	}
	if opts.initialStateDesc != "" {
		fmt.Fprintf(&buf, "  initial_state_desc=%s\n", opts.initialStateDesc)
	}
	if opts.threads != 0 {
		fmt.Fprintf(&buf, "  threads=%d\n", opts.threads)
	}
	if opts.useBlockPropertyCollector {
		fmt.Fprintf(&buf, "  use_block_property_collector=%t\n", opts.useBlockPropertyCollector)
	}

	s := opts.opts.String()
	if buf.Len() == 0 {
		return s
	}
	return s + "\n[TestOptions]\n" + buf.String()
}

func defaultTestOptions() *testOptions {
	return &testOptions{
		opts:                      defaultOptions(),
		useBlockPropertyCollector: true,
		threads:                   16,
	}
}

func defaultOptions() *pebble.Options {
	opts := &pebble.Options{
		Comparer:           testkeys.Comparer,
		FS:                 vfs.NewMem(),
		FormatMajorVersion: pebble.FormatNewest,
		Levels: []pebble.LevelOptions{{
			FilterPolicy: bloom.FilterPolicy(10),
		}},
	}
	opts.EnsureDefaults()
	return opts
}

type testOptions struct {
	opts     *pebble.Options
	useDisk  bool
	strictFS bool
	threads  int
	// Use Batch.Apply rather than DB.Ingest.
	ingestUsingApply bool
	// Replace a SINGLEDEL with a DELETE.
	replaceSingleDelete bool
	// The path on the local filesystem where the initial state of the database
	// exists.  Empty if the test run begins from an empty database state.
	initialStatePath string
	// A human-readable string describing the initial state of the database.
	// Empty if the test run begins from an empty database state.
	initialStateDesc string
	// Use a block property collector, which may be used by block property
	// filters.
	useBlockPropertyCollector bool
}

func standardOptions() []*testOptions {
	// The index labels are not strictly necessary, but they make it easier to
	// find which options correspond to a failure.
	stdOpts := []string{
		0: "", // default options
		1: `
[Options]
  cache_size=1
`,
		2: `
[Options]
  disable_wal=true
`,
		3: `
[Options]
  l0_compaction_threshold=1
`,
		4: `
[Options]
  l0_compaction_threshold=1
  l0_stop_writes_threshold=1
`,
		5: `
[Options]
  lbase_max_bytes=1
`,
		6: `
[Options]
  max_manifest_file_size=1
`,
		7: `
[Options]
  max_open_files=1
`,
		8: `
[Options]
  mem_table_size=2000
`,
		9: `
[Options]
  mem_table_stop_writes_threshold=2
`,
		10: `
[Options]
  wal_dir=data/wal
`,
		11: `
[Level "0"]
  block_restart_interval=1
`,
		12: `
[Level "0"]
  block_size=1
`,
		13: `
[Level "0"]
  compression=NoCompression
`,
		14: `
[Level "0"]
  index_block_size=1
`,
		15: `
[Level "0"]
  target_file_size=1
`,
		16: `
[Level "0"]
  filter_policy=none
`,
		// 1GB
		17: `
[Options]
  bytes_per_sync=1073741824
[TestOptions]
  strictfs=true
`,
		18: `
[Options]
  max_concurrent_compactions=2
`,
		19: `
[TestOptions]
  ingest_using_apply=true
`,
		20: `
[TestOptions]
  replace_single_delete=true
`,
		21: `
[TestOptions]
 use_disk=true
`,
		22: `
[Options]
  max_writer_concurrency=2
  force_writer_parallelism=true
`,
		23: `
[TestOptions]
  use_block_property_collector=false
`,
		24: `
[TestOptions]
  threads=1
`,
		25: `
[TestOptions]
  enable_value_blocks=true
`,
	}

	opts := make([]*testOptions, len(stdOpts))
	for i := range opts {
		opts[i] = defaultTestOptions()
		if err := parseOptions(opts[i], stdOpts[i]); err != nil {
			panic(err)
		}
	}
	return opts
}

func randomOptions(rng *rand.Rand) *testOptions {
	var testOpts = &testOptions{}
	opts := defaultOptions()
	testOpts.opts = opts

	// There are some private options, which we don't want users to fiddle with.
	// There's no way to set it through the public interface. The only method is
	// through Parse.
	{
		var privateOpts bytes.Buffer
		fmt.Fprintln(&privateOpts, `[Options]`)
		if rng.Intn(3) == 0 /* 33% */ {
			fmt.Fprintln(&privateOpts, `  disable_delete_only_compactions=true`)
		}
		if rng.Intn(3) == 0 /* 33% */ {
			fmt.Fprintln(&privateOpts, `  disable_elision_only_compactions=true`)
		}
		if rng.Intn(5) == 0 /* 20% */ {
			fmt.Fprintln(&privateOpts, `  disable_lazy_combined_iteration=true`)
		}
		if privateOptsStr := privateOpts.String(); privateOptsStr != `[Options]\n` {
			parseOptions(testOpts, privateOptsStr)
		}
	}

	opts.BytesPerSync = 1 << uint(rng.Intn(28))     // 1B - 256MB
	opts.Cache = cache.New(1 << uint(rng.Intn(30))) // 1B - 1GB
	opts.DisableWAL = rng.Intn(2) == 0
	opts.FlushDelayDeleteRange = time.Millisecond * time.Duration(5*rng.Intn(245)) // 5-250ms
	opts.FlushDelayRangeKey = time.Millisecond * time.Duration(5*rng.Intn(245))    // 5-250ms
	opts.FlushSplitBytes = 1 << rng.Intn(20)                                       // 1B - 1MB
	// The metamorphic test exercise range keys, so we cannot use an older
	// FormatMajorVersion than pebble.FormatRangeKeys.
	opts.FormatMajorVersion = pebble.FormatRangeKeys
	n := int(pebble.FormatNewest - opts.FormatMajorVersion)
	if n > 0 {
		opts.FormatMajorVersion += pebble.FormatMajorVersion(rng.Intn(n))
	}
	opts.Experimental.L0CompactionConcurrency = 1 + rng.Intn(4)    // 1-4
	opts.Experimental.LevelMultiplier = 5 << rng.Intn(7)           // 5 - 320
	opts.Experimental.MinDeletionRate = 1 << uint(20+rng.Intn(10)) // 1MB - 1GB
	opts.Experimental.ValidateOnIngest = rng.Intn(2) != 0
	opts.L0CompactionThreshold = 1 + rng.Intn(100)     // 1 - 100
	opts.L0CompactionFileThreshold = 1 << rng.Intn(11) // 1 - 1024
	opts.L0StopWritesThreshold = 1 + rng.Intn(100)     // 1 - 100
	if opts.L0StopWritesThreshold < opts.L0CompactionThreshold {
		opts.L0StopWritesThreshold = opts.L0CompactionThreshold
	}
	opts.LBaseMaxBytes = 1 << uint(rng.Intn(30)) // 1B - 1GB
	maxConcurrentCompactions := rng.Intn(3) + 1  // 1-3
	opts.MaxConcurrentCompactions = func() int {
		return maxConcurrentCompactions
	}
	opts.MaxManifestFileSize = 1 << uint(rng.Intn(30)) // 1B  - 1GB
	opts.MemTableSize = 2 << (10 + uint(rng.Intn(16))) // 2KB - 256MB
	opts.MemTableStopWritesThreshold = 2 + rng.Intn(5) // 2 - 5
	if rng.Intn(2) == 0 {
		opts.WALDir = "data/wal"
	}
	if rng.Intn(4) == 0 {
		// Enable Writer parallelism for 25% of the random options. Setting
		// MaxWriterConcurrency to any value greater than or equal to 1 has the
		// same effect currently.
		opts.Experimental.MaxWriterConcurrency = 2
		opts.Experimental.ForceWriterParallelism = true
	}
	var lopts pebble.LevelOptions
	lopts.BlockRestartInterval = 1 + rng.Intn(64)  // 1 - 64
	lopts.BlockSize = 1 << uint(rng.Intn(24))      // 1 - 16MB
	lopts.BlockSizeThreshold = 50 + rng.Intn(50)   // 50 - 100
	lopts.IndexBlockSize = 1 << uint(rng.Intn(24)) // 1 - 16MB
	lopts.TargetFileSize = 1 << uint(rng.Intn(28)) // 1 - 256MB
	opts.Levels = []pebble.LevelOptions{lopts}
	opts.Experimental.PointTombstoneWeight = 1 + 10*rng.Float64() // 1 - 10

	// Explicitly disable disk-backed FS's for the random configurations. The
	// single standard test configuration that uses a disk-backed FS is
	// sufficient.
	testOpts.useDisk = false
	testOpts.strictFS = rng.Intn(2) != 0 // Only relevant for MemFS.
	testOpts.threads = rng.Intn(runtime.GOMAXPROCS(0)) + 1
	if testOpts.strictFS {
		opts.DisableWAL = false
	}
	testOpts.ingestUsingApply = rng.Intn(2) != 0
	testOpts.replaceSingleDelete = rng.Intn(2) != 0
	testOpts.useBlockPropertyCollector = rng.Intn(2) != 0
	if testOpts.useBlockPropertyCollector {
		testOpts.opts.BlockPropertyCollectors = blockPropertyCollectorConstructors
	}
	return testOpts
}

func setupInitialState(dataDir string, testOpts *testOptions) error {
	// Copy (vfs.Default,<initialStatePath>/data) to (testOpts.opts.FS,<dataDir>).
	ok, err := vfs.Clone(
		vfs.Default,
		testOpts.opts.FS,
		vfs.Default.PathJoin(testOpts.initialStatePath, "data"),
		dataDir,
		vfs.CloneSync,
		vfs.CloneSkip(func(filename string) bool {
			// Skip the archive of historical files, any checkpoints created by
			// operations and files staged for ingest in tmp.
			b := filepath.Base(filename)
			return b == "archive" || b == "checkpoints" || b == "tmp"
		}))
	if err != nil {
		return err
	} else if !ok {
		return os.ErrNotExist
	}

	// Tests with wal_dir set store their WALs in a `wal` directory. The source
	// database (initialStatePath) could've had wal_dir set, or the current test
	// options (testOpts) could have wal_dir set, or both.
	fs := testOpts.opts.FS
	walDir := fs.PathJoin(dataDir, "wal")
	if err := fs.MkdirAll(walDir, os.ModePerm); err != nil {
		return err
	}

	// Copy <dataDir>/wal/*.log -> <dataDir>.
	src, dst := walDir, dataDir
	if testOpts.opts.WALDir != "" {
		// Copy <dataDir>/*.log -> <dataDir>/wal.
		src, dst = dst, src
	}
	return moveLogs(fs, src, dst)
}

func moveLogs(fs vfs.FS, srcDir, dstDir string) error {
	ls, err := fs.List(srcDir)
	if err != nil {
		return err
	}
	for _, f := range ls {
		if filepath.Ext(f) != ".log" {
			continue
		}
		src := fs.PathJoin(srcDir, f)
		dst := fs.PathJoin(dstDir, f)
		if err := fs.Rename(src, dst); err != nil {
			return err
		}
	}
	return nil
}

var blockPropertyCollectorConstructors = []func() pebble.BlockPropertyCollector{
	sstable.NewTestKeysBlockPropertyCollector,
}
