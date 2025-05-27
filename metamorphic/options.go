// Copyright 2019 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package metamorphic

import (
	"bytes"
	"fmt"
	"math"
	"math/rand/v2"
	"os"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"sync/atomic"
	"time"

	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/pebble"
	"github.com/cockroachdb/pebble/bloom"
	"github.com/cockroachdb/pebble/internal/base"
	"github.com/cockroachdb/pebble/objstorage/remote"
	"github.com/cockroachdb/pebble/sstable"
	"github.com/cockroachdb/pebble/sstable/block"
	"github.com/cockroachdb/pebble/vfs"
	"github.com/cockroachdb/pebble/wal"
)

const (
	minimumFormatMajorVersion = pebble.FormatMinSupported
	// The format major version to use in the default options configurations. We
	// default to the minimum supported format so we exercise the runtime version
	// ratcheting that a cluster upgrading would experience. The randomized
	// options may still use format major versions that are less than
	// defaultFormatMajorVersion but are at least minimumFormatMajorVersion.
	defaultFormatMajorVersion = pebble.FormatMinSupported
	// newestFormatMajorVersionToTest is the most recent format major version
	// the metamorphic tests should use. This may be greater than
	// pebble.FormatNewest when some format major versions are marked as
	// experimental.
	newestFormatMajorVersionToTest = pebble.FormatNewest
)

func parseOptions(
	opts *TestOptions, data string, customOptionParsers map[string]func(string) (CustomOption, bool),
) error {
	hooks := &pebble.ParseHooks{
		NewComparer: func(name string) (*pebble.Comparer, error) {
			for _, kf := range knownKeyFormats {
				if kf.Comparer.Name == name {
					return kf.Comparer, nil
				}
			}
			return nil, errors.Newf("unknown comparer %q", name)

		},
		NewFilterPolicy: filterPolicyFromName,
		NewKeySchema: func(name string) (pebble.KeySchema, error) {
			for _, kf := range knownKeyFormats {
				if kf.KeySchema.Name == name {
					return *kf.KeySchema, nil
				}
			}
			return pebble.KeySchema{}, errors.Newf("unknown key schema %q", name)
		},
		SkipUnknown: func(name, value string) bool {
			if strings.EqualFold(value, "false") {
				// TODO(radu): audit all settings and use ParseBool wherever necessary.
				panic(fmt.Sprintf("%s: boolean options can only be set to true", name))
			}
			switch name {
			case "TestOptions":
				return true
			case "TestOptions.strictfs":
				opts.strictFS = true
				opts.Opts.FS = vfs.NewCrashableMem()
				return true
			case "TestOptions.key_format":
				opts.KeyFormat = keyFormatsByName[value]
				return true
			case "TestOptions.ingest_using_apply":
				opts.ingestUsingApply = true
				return true
			case "TestOptions.delete_sized":
				opts.deleteSized = true
				return true
			case "TestOptions.replace_single_delete":
				opts.replaceSingleDelete = true
				return true
			case "TestOptions.use_disk":
				opts.useDisk = true
				opts.Opts.FS = vfs.Default
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
				opts.Threads = v
				return true
			case "TestOptions.disable_block_property_collector":
				v, err := strconv.ParseBool(value)
				if err != nil {
					panic(err)
				}
				opts.disableBlockPropertyCollector = v
				if v {
					opts.Opts.BlockPropertyCollectors = nil
				}
				return true
			case "TestOptions.enable_value_blocks":
				opts.enableValueBlocks = true
				opts.Opts.Experimental.EnableValueBlocks = func() bool { return true }
				return true
			case "TestOptions.disable_value_blocks_for_ingest_sstables":
				opts.disableValueBlocksForIngestSSTables = true
				return true
			case "TestOptions.async_apply_to_db":
				opts.asyncApplyToDB = true
				return true
			case "TestOptions.shared_storage_enabled":
				opts.sharedStorageEnabled = true
				if opts.Opts.Experimental.CreateOnShared == remote.CreateOnSharedNone {
					opts.Opts.Experimental.CreateOnShared = remote.CreateOnSharedAll
				}
				return true
			case "TestOptions.external_storage_enabled":
				opts.externalStorageEnabled = true
				return true
			case "TestOptions.secondary_cache_enabled":
				opts.secondaryCacheEnabled = true
				opts.Opts.Experimental.SecondaryCacheSizeBytes = 1024 * 1024 * 32 // 32 MBs
				return true
			case "TestOptions.seed_efos":
				v, err := strconv.ParseUint(value, 10, 64)
				if err != nil {
					panic(err)
				}
				opts.seedEFOS = v
				return true
			case "TestOptions.io_latency_mean":
				v, err := time.ParseDuration(value)
				if err != nil {
					panic(err)
				}
				opts.ioLatencyMean = v
				return true
			case "TestOptions.io_latency_probability":
				v, err := strconv.ParseFloat(value, 64)
				if err != nil {
					panic(err)
				}
				opts.ioLatencyProbability = v
				return true
			case "TestOptions.io_latency_seed":
				v, err := strconv.ParseInt(value, 10, 64)
				if err != nil {
					panic(err)
				}
				opts.ioLatencySeed = v
				return true
			case "TestOptions.ingest_split":
				opts.ingestSplit = true
				opts.Opts.Experimental.IngestSplit = func() bool {
					return true
				}
				return true
			case "TestOptions.use_shared_replicate":
				opts.useSharedReplicate = true
				return true
			case "TestOptions.use_external_replicate":
				opts.useExternalReplicate = true
				return true
			case "TestOptions.use_excise":
				opts.useExcise = true
				return true
			case "TestOptions.use_delete_only_compaction_excises":
				opts.useDeleteOnlyCompactionExcises = true
				opts.Opts.Experimental.EnableDeleteOnlyCompactionExcises = func() bool {
					return opts.useDeleteOnlyCompactionExcises
				}
				return true
			case "TestOptions.disable_downloads":
				opts.disableDownloads = true
				return true
			case "TestOptions.use_jemalloc_size_classes":
				opts.Opts.AllocatorSizeClasses = pebble.JemallocSizeClasses
				return true
			case "TestOptions.disable_value_separation":
				v, err := strconv.ParseBool(value)
				if err != nil {
					panic(err)
				}
				if v {
					opts.Opts.Experimental.ValueSeparationPolicy = func() pebble.ValueSeparationPolicy {
						return pebble.ValueSeparationPolicy{
							Enabled: false,
						}
					}
				}
				return true
			default:
				if customOptionParsers == nil {
					return false
				}
				name = strings.TrimPrefix(name, "TestOptions.")
				if p, ok := customOptionParsers[name]; ok {
					if customOpt, ok := p(value); ok {
						opts.CustomOpts = append(opts.CustomOpts, customOpt)
						return true
					}
				}
				return false
			}
		},
	}
	err := opts.Opts.Parse(data, hooks)
	// Ensure that the WAL failover FS agrees with the primary FS. They're
	// separate options, but in the metamorphic tests we keep them in sync.
	if opts.Opts.WALFailover != nil {
		opts.Opts.WALFailover.Secondary.FS = opts.Opts.FS
	}
	opts.Opts.EnsureDefaults()
	return err
}

func optionsToString(opts *TestOptions) string {
	var buf bytes.Buffer
	fmt.Fprintf(&buf, "  key_format=%s\n", opts.KeyFormat.Name)
	if opts.strictFS {
		fmt.Fprint(&buf, "  strictfs=true\n")
	}
	if opts.ingestUsingApply {
		fmt.Fprint(&buf, "  ingest_using_apply=true\n")
	}
	if opts.deleteSized {
		fmt.Fprint(&buf, "  delete_sized=true\n")
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
	if opts.Threads != 0 {
		fmt.Fprintf(&buf, "  threads=%d\n", opts.Threads)
	}
	if opts.disableBlockPropertyCollector {
		fmt.Fprintf(&buf, "  disable_block_property_collector=%t\n", opts.disableBlockPropertyCollector)
	}
	if opts.enableValueBlocks {
		fmt.Fprintf(&buf, "  enable_value_blocks=%t\n", opts.enableValueBlocks)
	}
	if opts.disableValueBlocksForIngestSSTables {
		fmt.Fprintf(&buf, "  disable_value_blocks_for_ingest_sstables=%t\n", opts.disableValueBlocksForIngestSSTables)
	}
	if opts.asyncApplyToDB {
		fmt.Fprint(&buf, "  async_apply_to_db=true\n")
	}
	if opts.sharedStorageEnabled {
		fmt.Fprint(&buf, "  shared_storage_enabled=true\n")
	}
	if opts.externalStorageEnabled {
		fmt.Fprint(&buf, "  external_storage_enabled=true\n")
	}
	if opts.secondaryCacheEnabled {
		fmt.Fprint(&buf, "  secondary_cache_enabled=true\n")
	}
	if opts.seedEFOS != 0 {
		fmt.Fprintf(&buf, "  seed_efos=%d\n", opts.seedEFOS)
	}
	if opts.ingestSplit {
		fmt.Fprintf(&buf, "  ingest_split=%v\n", opts.ingestSplit)
	}
	if opts.ioLatencyProbability > 0 {
		fmt.Fprintf(&buf, "  io_latency_mean=%s\n", opts.ioLatencyMean)
		fmt.Fprintf(&buf, "  io_latency_probability=%.10f\n", opts.ioLatencyProbability)
		fmt.Fprintf(&buf, "  io_latency_seed=%d\n", opts.ioLatencySeed)
	}
	if opts.useSharedReplicate {
		fmt.Fprintf(&buf, "  use_shared_replicate=%v\n", opts.useSharedReplicate)
	}
	if opts.useExternalReplicate {
		fmt.Fprintf(&buf, "  use_external_replicate=%v\n", opts.useExternalReplicate)
	}
	if opts.useExcise {
		fmt.Fprintf(&buf, "  use_excise=%v\n", opts.useExcise)
	}
	if opts.useDeleteOnlyCompactionExcises {
		fmt.Fprintf(&buf, "  use_delete_only_compaction_excises=%v\n", opts.useDeleteOnlyCompactionExcises)
	}
	if opts.disableDownloads {
		fmt.Fprintf(&buf, "  disable_downloads=%v\n", opts.disableDownloads)
	}
	if opts.Opts.AllocatorSizeClasses != nil {
		if fmt.Sprint(opts.Opts.AllocatorSizeClasses) != fmt.Sprint(pebble.JemallocSizeClasses) {
			panic(fmt.Sprintf("unexpected AllocatorSizeClasses %v", opts.Opts.AllocatorSizeClasses))
		}
		fmt.Fprint(&buf, "  use_jemalloc_size_classes=true\n")
	}
	for _, customOpt := range opts.CustomOpts {
		fmt.Fprintf(&buf, "  %s=%s\n", customOpt.Name(), customOpt.Value())
	}

	s := opts.Opts.String()
	if buf.Len() == 0 {
		return s
	}
	return s + "\n[TestOptions]\n" + buf.String()
}

func defaultTestOptions(kf KeyFormat) *TestOptions {
	return &TestOptions{
		Opts:        defaultOptions(kf),
		Threads:     16,
		RetryPolicy: NeverRetry,
		KeyFormat:   kf,
	}
}

func defaultOptions(kf KeyFormat) *pebble.Options {
	opts := &pebble.Options{
		// Use an archive cleaner to ease post-mortem debugging.
		Cleaner: base.ArchiveCleaner{},
		// Always use our custom comparer which provides a Split method.
		Comparer:           kf.Comparer,
		KeySchema:          kf.KeySchema.Name,
		KeySchemas:         sstable.MakeKeySchemas(kf.KeySchema),
		FS:                 vfs.NewMem(),
		FormatMajorVersion: defaultFormatMajorVersion,
		Levels: []pebble.LevelOptions{{
			FilterPolicy: bloom.FilterPolicy(10),
		}},
		BlockPropertyCollectors: kf.BlockPropertyCollectors,
	}
	opts.Experimental.EnableColumnarBlocks = func() bool { return true }

	opts.Experimental.ValueSeparationPolicy = func() pebble.ValueSeparationPolicy {
		return pebble.ValueSeparationPolicy{
			Enabled:               true,
			MinimumSize:           5,
			MaxBlobReferenceDepth: 3,
		}
	}

	// The level checker runs every time a new read state is installed: every
	// compaction, flush, ingest completion, etc. It runs while the database
	// mutex DB.mu is held, preventing the scheduling of new compactions or
	// flushes.
	//
	// We only consider running the level checker 50% of the time (to ensure
	// we're not obscuring races post-read state installation).
	//
	// Additionally, some option configurations can create pathological numbers
	// of sstables, causing the level checker to consume an excessive amount of
	// time. To prevent pathological cases, we limit the cumulative time spent
	// in the level checker to 25% of the test's runtime. If DebugCheck is
	// invoked but we've spent more than 25% of our total test time wihtin the
	// level checker, we skip invoking DebugCheckLevels.
	startTime := time.Now()
	var cumulativeTime atomic.Int64
	opts.DebugCheck = func(db *pebble.DB) error {
		if rand.Float64() < 0.50 {
			return nil
		}
		now := time.Now()
		testDur := now.Sub(startTime)
		if checkerDur := time.Duration(cumulativeTime.Load()); checkerDur > testDur/4 {
			return nil
		}
		err := pebble.DebugCheckLevels(db)
		cumulativeTime.Add(int64(time.Since(now)))
		return err
	}

	return opts
}

// TestOptions describes the options configuring an individual run of the
// metamorphic tests.
type TestOptions struct {
	// Opts holds the *pebble.Options for the test.
	Opts *pebble.Options
	// Threads configures the parallelism of the test. Each thread will run in
	// an independent goroutine and be responsible for executing operations
	// against an independent set of objects. The outcome of any individual
	// operation will still be deterministic, with the metamorphic test
	// inserting synchronization where necessary.
	Threads int
	// RetryPolicy configures which errors should be retried.
	RetryPolicy RetryPolicy
	// KeyFormat defines the format of keys used within the test.
	KeyFormat KeyFormat
	// CustomOptions holds custom test options that are defined outside of this
	// package.
	CustomOpts []CustomOption

	// internal

	useDisk  bool
	strictFS bool
	// Use Batch.Apply rather than DB.Ingest.
	ingestUsingApply bool
	// Use Batch.DeleteSized rather than Batch.Delete.
	deleteSized bool
	// Replace a SINGLEDEL with a DELETE.
	replaceSingleDelete bool
	// The path on the local filesystem where the initial state of the database
	// exists.  Empty if the test run begins from an empty database state.
	initialStatePath string
	// A human-readable string describing the initial state of the database.
	// Empty if the test run begins from an empty database state.
	initialStateDesc string
	// Disable the block property collector, which may be used by block property
	// filters.
	disableBlockPropertyCollector bool
	// Enable the use of value blocks.
	enableValueBlocks bool
	// Disables value blocks in the sstables written for ingest.
	disableValueBlocksForIngestSSTables bool
	// Use DB.ApplyNoSyncWait for applies that want to sync the WAL.
	asyncApplyToDB bool
	// Enable the use of shared storage.
	sharedStorageEnabled bool
	// Enable the use of shared storage for external file ingestion.
	externalStorageEnabled bool
	// Enables the use of shared replication in TestOptions.
	useSharedReplicate bool
	// Enables the use of external replication in TestOptions.
	useExternalReplicate bool
	// Enable the secondary cache. Only effective if sharedStorageEnabled is
	// also true.
	secondaryCacheEnabled bool
	// If nonzero, enables the use of EventuallyFileOnlySnapshots for
	// newSnapshotOps that are keyspan-bounded. The set of which newSnapshotOps
	// are actually created as EventuallyFileOnlySnapshots is deterministically
	// derived from the seed and the operation index.
	seedEFOS uint64
	// If nonzero, enables the injection of random IO latency. The mechanics of
	// a Pebble operation can be very timing dependent, so artificial latency
	// can ensure a wide variety of mechanics are exercised. Additionally,
	// exercising some mechanics such as WAL failover require IO latency.
	ioLatencyProbability float64
	ioLatencySeed        int64
	ioLatencyMean        time.Duration
	// Enables ingest splits. Saved here for serialization as Options does not
	// serialize this.
	ingestSplit bool
	// Enables operations that do excises. Note that a false value for this does
	// not guarantee the lack of excises, as useSharedReplicate can also cause
	// excises. However !useExcise && !useSharedReplicate can be used to guarantee
	// lack of excises.
	useExcise bool
	// useDeleteOnlyCompactionExcises turns on the ability for delete-only compactions
	// to do excises. Note that this can be true even when useExcise is false.
	useDeleteOnlyCompactionExcises bool
	// disableDownloads, if true, makes downloadOp a no-op.
	disableDownloads bool
}

// CustomOption defines a custom option that configures the behavior of an
// individual test run. Like all test options, custom options are serialized to
// the OPTIONS file even if they're not options ordinarily understood by Pebble.
type CustomOption interface {
	// Name returns the name of the custom option. This is the key under which
	// the option appears in the OPTIONS file, within the [TestOptions] stanza.
	Name() string
	// Value returns the value of the custom option, serialized as it should
	// appear within the OPTIONS file.
	Value() string
	// Close is run after the test database has been closed at the end of the
	// test as well as during restart operations within the test sequence. It's
	// passed a copy of the *pebble.Options. If the custom options hold on to
	// any resources outside, Close should release them.
	Close(*pebble.Options) error
	// Open is run before the test runs and during a restart operation after the
	// test database has been closed and Close has been called. It's passed a
	// copy of the *pebble.Options. If the custom options must acquire any
	// resources before the test continues, it should reacquire them.
	Open(*pebble.Options) error

	// TODO(jackson): provide additional hooks for custom options changing the
	// behavior of a run.
}

func standardOptions(kf KeyFormat) []*TestOptions {
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
[TestOptions]
  use_jemalloc_size_classes=true
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
  wal_dir={store_path}/wal
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
[Options]
  l0_stop_writes_threshold=100
[Level "0"]
  target_file_size=12
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
  use_jemalloc_size_classes=true
`,
		22: `
[TestOptions]
  disable_block_property_collector=true
`,
		23: `
[TestOptions]
  threads=1
`,
		24: `
[TestOptions]
  enable_value_blocks=true
  use_jemalloc_size_classes=true
`,
		25: fmt.Sprintf(`
[Options]
  format_major_version=%s
`, newestFormatMajorVersionToTest),
		26: fmt.Sprintf(`
[Options]
  format_major_version=%s
[TestOptions]
  shared_storage_enabled=true
  secondary_cache_enabled=true
`, pebble.FormatMinForSharedObjects),
		27: fmt.Sprintf(`
[Options]
  format_major_version=%s
[TestOptions]
  external_storage_enabled=true
`, pebble.FormatSyntheticPrefixSuffix),
		28: fmt.Sprintf(`
[Options]
  format_major_version=%s
  max_concurrent_downloads=2
[TestOptions]
  shared_storage_enabled=true
  external_storage_enabled=true
`, pebble.FormatSyntheticPrefixSuffix),
		29: `
[TestOptions]
  disable_value_separation=true
`,
	}

	opts := make([]*TestOptions, len(stdOpts))
	for i := range opts {
		opts[i] = defaultTestOptions(kf)
		// NB: The standard options by definition can never include custom
		// options, so no need to propagate custom option parsers.
		if err := parseOptions(opts[i], stdOpts[i], nil /* custom option parsers */); err != nil {
			panic(err)
		}
	}
	return opts
}

// RandomOptions generates a random set of operations, drawing randomness from
// rng.
func RandomOptions(
	rng *rand.Rand, kf KeyFormat, customOptionParsers map[string]func(string) (CustomOption, bool),
) *TestOptions {
	testOpts := defaultTestOptions(kf)
	opts := testOpts.Opts

	// There are some private options, which we don't want users to fiddle with.
	// There's no way to set it through the public interface. The only method is
	// through Parse.
	{
		var privateOpts bytes.Buffer
		fmt.Fprintln(&privateOpts, `[Options]`)
		if rng.IntN(3) == 0 /* 33% */ {
			fmt.Fprintln(&privateOpts, `  disable_delete_only_compactions=true`)
		}
		if rng.IntN(3) == 0 /* 33% */ {
			fmt.Fprintln(&privateOpts, `  disable_elision_only_compactions=true`)
		}
		if rng.IntN(5) == 0 /* 20% */ {
			fmt.Fprintln(&privateOpts, `  disable_lazy_combined_iteration=true`)
		}
		if privateOptsStr := privateOpts.String(); privateOptsStr != `[Options]\n` {
			if err := parseOptions(testOpts, privateOptsStr, customOptionParsers); err != nil {
				panic(err)
			}
		}
	}

	opts.BytesPerSync = 1 << uint(rng.IntN(28)) // 1B - 256MB
	opts.CacheSize = 1 << uint(rng.IntN(30))    // 1B - 1GB
	opts.DisableWAL = rng.IntN(2) == 0
	opts.FlushDelayDeleteRange = time.Millisecond * time.Duration(5*rng.IntN(245)) // 5-250ms
	opts.FlushDelayRangeKey = time.Millisecond * time.Duration(5*rng.IntN(245))    // 5-250ms
	opts.FlushSplitBytes = 1 << rng.IntN(20)                                       // 1B - 1MB
	opts.FormatMajorVersion = minimumFormatMajorVersion
	n := int(newestFormatMajorVersionToTest - opts.FormatMajorVersion)
	opts.FormatMajorVersion += pebble.FormatMajorVersion(rng.IntN(n + 1))
	opts.Experimental.L0CompactionConcurrency = 1 + rng.IntN(4) // 1-4
	opts.Experimental.LevelMultiplier = 5 << rng.IntN(7)        // 5 - 320
	opts.TargetByteDeletionRate = 1 << uint(20+rng.IntN(10))    // 1MB - 1GB
	opts.Experimental.ValidateOnIngest = rng.IntN(2) != 0
	opts.L0CompactionThreshold = 1 + rng.IntN(100)     // 1 - 100
	opts.L0CompactionFileThreshold = 1 << rng.IntN(11) // 1 - 1024
	opts.L0StopWritesThreshold = 50 + rng.IntN(100)    // 50 - 150
	if opts.L0StopWritesThreshold < 2*opts.L0CompactionThreshold {
		opts.L0StopWritesThreshold = 2 * opts.L0CompactionThreshold
	}
	opts.LBaseMaxBytes = 1 << uint(rng.IntN(30)) // 1B - 1GB
	maxConcurrentCompactions := rng.IntN(3) + 1  // 1-3
	minConcurrentCompactions := 1
	if rng.IntN(4) == 0 {
		minConcurrentCompactions = rng.IntN(maxConcurrentCompactions) + 1
	}
	opts.CompactionConcurrencyRange = func() (int, int) {
		return minConcurrentCompactions, maxConcurrentCompactions
	}
	// [-0.2, 0.4], in steps of 0.2.
	garbageFrac := float64(rng.IntN(5))/5.0 - 0.2
	opts.Experimental.CompactionGarbageFractionForMaxConcurrency = func() float64 {
		return garbageFrac
	}
	maxConcurrentDownloads := rng.IntN(3) + 1 // 1-3
	opts.MaxConcurrentDownloads = func() int {
		return maxConcurrentDownloads
	}
	opts.MaxManifestFileSize = 1 << uint(rng.IntN(30)) // 1B  - 1GB
	opts.MemTableSize = 2 << (10 + uint(rng.IntN(16))) // 2KB - 256MB
	opts.MemTableStopWritesThreshold = 2 + rng.IntN(5) // 2 - 5
	if rng.IntN(2) == 0 {
		opts.WALDir = pebble.MakeStoreRelativePath(opts.FS, "wal")
	}

	// Half the time enable WAL failover.
	if rng.IntN(2) == 0 {
		// Use 10x longer durations when writing directly to FS; we don't want
		// WAL failover to trigger excessively frequently.
		referenceDur := time.Millisecond
		if testOpts.useDisk {
			referenceDur *= 10
		}

		scaleDuration := func(d time.Duration, minFactor, maxFactor float64) time.Duration {
			return time.Duration(float64(d) * (minFactor + rng.Float64()*(maxFactor-minFactor)))
		}
		probeInterval := scaleDuration(referenceDur, 0.10, 0.50) // Between 10-50% of the reference duration.
		unhealthyThreshold := expRandDuration(rng, 3*referenceDur, time.Second)
		healthyThreshold := expRandDuration(rng, 3*referenceDur, time.Second)
		// Use a healthy interval between 1-119x the probe interval. The probe
		// maintains a maximum history 120 entries, so the healthy interval
		// must not exceed 119x the probe interval.
		healthyInterval := scaleDuration(probeInterval, 1.0, 119.0)
		opts.WALFailover = &pebble.WALFailoverOptions{
			Secondary: wal.Dir{FS: vfs.Default, Dirname: pebble.MakeStoreRelativePath(vfs.Default, "wal_secondary")},
			FailoverOptions: wal.FailoverOptions{
				PrimaryDirProbeInterval:      probeInterval,
				HealthyProbeLatencyThreshold: healthyThreshold,
				HealthyInterval:              healthyInterval,
				UnhealthySamplingInterval:    scaleDuration(unhealthyThreshold, 0.10, 0.50), // Between 10-50% of the unhealthy threshold
				UnhealthyOperationLatencyThreshold: func() (time.Duration, bool) {
					return unhealthyThreshold, true
				},
				ElevatedWriteStallThresholdLag: expRandDuration(rng, 5*referenceDur, 2*time.Second),
			},
		}
	}
	if rng.IntN(2) == 0 {
		opts.Experimental.DisableIngestAsFlushable = func() bool { return true }
	}

	// We either use no multilevel compactions, multilevel compactions with the
	// default (zero) additional propensity, or multilevel compactions with an
	// additional propensity to encourage more multilevel compactions than we
	// ohterwise would.
	switch rng.IntN(3) {
	case 0:
		opts.Experimental.MultiLevelCompactionHeuristic = pebble.NoMultiLevel{}
	case 1:
		opts.Experimental.MultiLevelCompactionHeuristic = pebble.WriteAmpHeuristic{}
	default:
		opts.Experimental.MultiLevelCompactionHeuristic = pebble.WriteAmpHeuristic{
			AddPropensity: rng.Float64() * float64(rng.IntN(3)), // [0,3.0)
			AllowL0:       rng.IntN(4) == 1,                     // 25% of the time
		}
	}
	if rng.IntN(2) == 0 {
		opts.AllocatorSizeClasses = pebble.JemallocSizeClasses
	}

	var lopts pebble.LevelOptions
	lopts.BlockRestartInterval = 1 + rng.IntN(64)  // 1 - 64
	lopts.BlockSize = 1 << uint(rng.IntN(24))      // 1 - 16MB
	lopts.BlockSizeThreshold = 50 + rng.IntN(50)   // 50 - 100
	lopts.IndexBlockSize = 1 << uint(rng.IntN(24)) // 1 - 16MB
	lopts.TargetFileSize = 1 << uint(rng.IntN(28)) // 1 - 256MB
	if lopts.TargetFileSize < 1<<12 {
		// We will generate a lot of files, which will slow down compactions.
		// Increase L0StopWritesThreshold to reduce the number of write stalls
		// that could cause operation timeouts.
		opts.L0StopWritesThreshold = 100
	}
	// The EstimatedSize of an empty table writer is 8 bytes. We want something a
	// little bigger than that as the minimum target.
	lopts.TargetFileSize = max(lopts.TargetFileSize, 12)

	// We either use no bloom filter, the default filter, or a filter with
	// randomized bits-per-key setting. We zero out the Filters map. It'll get
	// repopulated on EnsureDefaults accordingly.
	opts.Filters = nil
	switch rng.IntN(3) {
	case 0:
		lopts.FilterPolicy = nil
	case 1:
		lopts.FilterPolicy = bloom.FilterPolicy(10)
	default:
		lopts.FilterPolicy = newTestingFilterPolicy(1 << rng.IntN(5))
	}

	// We use either no compression, snappy compression or zstd compression.
	switch rng.IntN(3) {
	case 0:
		lopts.Compression = func() block.Compression { return pebble.NoCompression }
	case 1:
		lopts.Compression = func() block.Compression { return pebble.ZstdCompression }
	default:
		lopts.Compression = func() block.Compression { return pebble.SnappyCompression }
	}
	opts.Levels = []pebble.LevelOptions{lopts}

	// Explicitly disable disk-backed FS's for the random configurations. The
	// single standard test configuration that uses a disk-backed FS is
	// sufficient.
	testOpts.useDisk = false
	testOpts.strictFS = rng.IntN(2) != 0 // Only relevant for MemFS.
	// 50% of the time, enable IO latency injection.
	if rng.IntN(2) == 0 {
		// Note: we want ioLatencyProbability to be at least 1e-10, otherwise it
		// might print as 0 when we stringify options.
		testOpts.ioLatencyProbability = 1e-10 + 0.01*rng.Float64() // 0-1%
		testOpts.ioLatencyMean = expRandDuration(rng, 3*time.Millisecond, time.Second)
		testOpts.ioLatencySeed = rng.Int64()
	}
	testOpts.Threads = rng.IntN(runtime.GOMAXPROCS(0)) + 1
	if testOpts.strictFS {
		opts.DisableWAL = false
		opts.FS = vfs.NewCrashableMem()
	} else if !testOpts.useDisk {
		opts.FS = vfs.NewMem()
	}
	// Update the WALFailover's secondary to use the same FS. This isn't
	// strictly necessary (the WALFailover could use a separate FS), but it
	// ensures when we save a copy of the test state to disk, we include the
	// secondary's WALs.
	if opts.WALFailover != nil {
		opts.WALFailover.Secondary.FS = opts.FS
	}
	testOpts.ingestUsingApply = rng.IntN(2) != 0
	testOpts.deleteSized = rng.IntN(2) != 0
	testOpts.replaceSingleDelete = rng.IntN(2) != 0
	testOpts.disableBlockPropertyCollector = rng.IntN(2) == 1
	if testOpts.disableBlockPropertyCollector {
		testOpts.Opts.BlockPropertyCollectors = nil
	}
	testOpts.enableValueBlocks = rng.IntN(2) != 0
	if testOpts.enableValueBlocks {
		testOpts.Opts.Experimental.EnableValueBlocks = func() bool { return true }
	}
	testOpts.disableValueBlocksForIngestSSTables = rng.IntN(2) == 0
	testOpts.asyncApplyToDB = rng.IntN(2) != 0
	// 20% of the time, enable shared storage.
	if rng.IntN(5) == 0 {
		testOpts.sharedStorageEnabled = true
		if testOpts.Opts.FormatMajorVersion < pebble.FormatMinForSharedObjects {
			testOpts.Opts.FormatMajorVersion = pebble.FormatMinForSharedObjects
		}
		// If shared storage is enabled, pick between writing all files on shared
		// vs. lower levels only, 50% of the time.
		testOpts.Opts.Experimental.CreateOnShared = remote.CreateOnSharedAll
		if rng.IntN(2) == 0 {
			testOpts.Opts.Experimental.CreateOnShared = remote.CreateOnSharedLower
		}
		// If shared storage is enabled, enable secondary cache 50% of the time.
		if rng.IntN(2) == 0 {
			testOpts.secondaryCacheEnabled = true
			// TODO(josh): Randomize various secondary cache settings.
			testOpts.Opts.Experimental.SecondaryCacheSizeBytes = 1024 * 1024 * 32 // 32 MBs
		}
		// 50% of the time, enable shared replication.
		testOpts.useSharedReplicate = rng.IntN(2) == 0
	}

	// 50% of the time, enable external storage.
	if rng.IntN(2) == 0 {
		testOpts.externalStorageEnabled = true
		if testOpts.Opts.FormatMajorVersion < pebble.FormatSyntheticPrefixSuffix {
			testOpts.Opts.FormatMajorVersion = pebble.FormatSyntheticPrefixSuffix
		}
	}

	// 75% of the time, randomize value separation parameters.
	if rng.IntN(4) > 0 {
		if testOpts.Opts.FormatMajorVersion < pebble.FormatExperimentalValueSeparation {
			testOpts.Opts.FormatMajorVersion = pebble.FormatExperimentalValueSeparation
		}
		minSize := 1 + rng.IntN(maxValueSize)
		maxBlobReferenceDepth := 2 + rng.IntN(9) // 2-10
		opts.Experimental.ValueSeparationPolicy = func() pebble.ValueSeparationPolicy {
			return pebble.ValueSeparationPolicy{
				Enabled:               true,
				MinimumSize:           minSize,
				MaxBlobReferenceDepth: maxBlobReferenceDepth,
			}
		}
	}

	testOpts.seedEFOS = rng.Uint64()
	testOpts.ingestSplit = rng.IntN(2) == 0
	opts.Experimental.IngestSplit = func() bool { return testOpts.ingestSplit }
	testOpts.useExcise = rng.IntN(2) == 0
	if testOpts.useExcise {
		if testOpts.Opts.FormatMajorVersion < pebble.FormatVirtualSSTables {
			testOpts.Opts.FormatMajorVersion = pebble.FormatVirtualSSTables
		}
	}
	testOpts.useDeleteOnlyCompactionExcises = rng.IntN(2) == 0
	opts.Experimental.EnableDeleteOnlyCompactionExcises = func() bool {
		return testOpts.useDeleteOnlyCompactionExcises
	}
	testOpts.disableDownloads = rng.IntN(2) == 0
	testOpts.Opts.EnsureDefaults()
	return testOpts
}

func expRandDuration(rng *rand.Rand, meanDur, maxDur time.Duration) time.Duration {
	return min(maxDur, time.Duration(math.Round(rng.ExpFloat64()*float64(meanDur))))
}

func setupInitialState(dataDir string, testOpts *TestOptions) error {
	// Copy (vfs.Default,<initialStatePath>/data) to (testOpts.opts.FS,<dataDir>).
	ok, err := vfs.Clone(
		vfs.Default,
		testOpts.Opts.FS,
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
	//
	// If the test opts are not configured to use a WAL dir, we add the WAL dir
	// as a 'WAL recovery dir' so that we'll read any WALs in the directory in
	// Open.
	fs := testOpts.Opts.FS
	walRecoveryPath := fs.PathJoin(dataDir, "wal")
	if _, err := fs.Stat(walRecoveryPath); err == nil {
		// Previous test used a WAL dir.
		if testOpts.Opts.WALDir == "" {
			// This test is not using a WAL dir. Add the previous WAL dir as a
			// recovery dir.
			testOpts.Opts.WALRecoveryDirs = append(testOpts.Opts.WALRecoveryDirs, wal.Dir{
				FS:      fs,
				Dirname: pebble.MakeStoreRelativePath(fs, "wal"),
			})
		} else {
			// Both the previous test and the current test are using a WAL dir. We
			// assume that they are the same.
			if testOpts.Opts.WALDir != pebble.MakeStoreRelativePath(fs, "wal") {
				return errors.Errorf("unsupported wal dir value %q", testOpts.Opts.WALDir)
			}
		}
	} else {
		// Previous test did not use a WAL dir.
		if testOpts.Opts.WALDir != "" {
			// The current test is using a WAL dir; we add the data directory itself
			// as a 'WAL recovery dir' so that we'll read any WALs if the previous
			// test was writing them to the data directory.
			testOpts.Opts.WALRecoveryDirs = append(testOpts.Opts.WALRecoveryDirs, wal.Dir{
				FS:      fs,
				Dirname: pebble.MakeStoreRelativePath(fs, ""),
			})
		}
	}

	// If the previous test used WAL failover and this test does not use failover,
	// add the failover directory as a 'WAL recovery dir' in case the previous
	// test was configured to use failover.
	failoverDir := testOpts.Opts.FS.PathJoin(dataDir, "wal_secondary")
	if _, err := testOpts.Opts.FS.Stat(failoverDir); err == nil {
		if testOpts.Opts.WALFailover == nil {
			testOpts.Opts.WALRecoveryDirs = append(testOpts.Opts.WALRecoveryDirs, wal.Dir{
				FS:      testOpts.Opts.FS,
				Dirname: pebble.MakeStoreRelativePath(testOpts.Opts.FS, "wal_secondary"),
			})
		} else if testOpts.Opts.WALFailover.Secondary.Dirname != pebble.MakeStoreRelativePath(testOpts.Opts.FS, "wal_secondary") {
			return errors.Errorf("unsupported wal failover dir value %q", testOpts.Opts.WALFailover.Secondary.Dirname)
		}
	}
	return nil
}

// testingFilterPolicy is used to allow bloom filter policies with non-default
// bits-per-key setting. It is necessary because the name of the production
// filter policy is fixed (see bloom.FilterPolicy.Name()); we need to output a
// custom policy name to the OPTIONS file that the test can then parse.
type testingFilterPolicy struct {
	bloom.FilterPolicy
}

var _ pebble.FilterPolicy = (*testingFilterPolicy)(nil)

func newTestingFilterPolicy(bitsPerKey int) *testingFilterPolicy {
	return &testingFilterPolicy{
		FilterPolicy: bloom.FilterPolicy(bitsPerKey),
	}
}

const testingFilterPolicyFmt = "testing_bloom_filter/bits_per_key=%d"

// Name implements the pebble.FilterPolicy interface.
func (t *testingFilterPolicy) Name() string {
	if t.FilterPolicy == 10 {
		return "rocksdb.BuiltinBloomFilter"
	}
	return fmt.Sprintf(testingFilterPolicyFmt, t.FilterPolicy)
}

func filterPolicyFromName(name string) (pebble.FilterPolicy, error) {
	switch name {
	case "none":
		return nil, nil
	case "rocksdb.BuiltinBloomFilter":
		return bloom.FilterPolicy(10), nil
	}
	var bitsPerKey int
	if _, err := fmt.Sscanf(name, testingFilterPolicyFmt, &bitsPerKey); err != nil {
		return nil, errors.Errorf("Invalid filter policy name '%s'", name)
	}
	return newTestingFilterPolicy(bitsPerKey), nil
}
