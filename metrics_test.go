// Copyright 2019 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package pebble

import (
	"bytes"
	"context"
	"fmt"
	"math/rand/v2"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/cockroachdb/crlib/crstrings"
	"github.com/cockroachdb/crlib/testutils/leaktest"
	"github.com/cockroachdb/datadriven"
	"github.com/cockroachdb/pebble/internal/base"
	"github.com/cockroachdb/pebble/internal/cache"
	"github.com/cockroachdb/pebble/internal/humanize"
	"github.com/cockroachdb/pebble/internal/manifest"
	"github.com/cockroachdb/pebble/internal/manual"
	"github.com/cockroachdb/pebble/internal/testkeys"
	"github.com/cockroachdb/pebble/objstorage/remote"
	"github.com/cockroachdb/pebble/sstable/block"
	"github.com/cockroachdb/pebble/vfs"
	"github.com/cockroachdb/pebble/vfs/errorfs"
	"github.com/cockroachdb/redact"
	"github.com/stretchr/testify/require"
)

func exampleMetrics() Metrics {
	var m Metrics
	m.BlockCache.Size = 1
	m.BlockCache.Count = 2
	m.BlockCache.Hits = 3
	m.BlockCache.Misses = 4
	m.Compact.Count = 5
	m.Compact.DefaultCount = 27
	m.Compact.DeleteOnlyCount = 28
	m.Compact.ElisionOnlyCount = 29
	m.Compact.MoveCount = 30
	m.Compact.ReadCount = 31
	m.Compact.TombstoneDensityCount = 16
	m.Compact.RewriteCount = 32
	m.Compact.CopyCount = 33
	m.Compact.MultiLevelCount = 34
	m.Compact.EstimatedDebt = 6
	m.Compact.InProgressBytes = 7
	m.Compact.NumInProgress = 2
	m.Compact.CounterLevelCount = 10
	m.Compact.CancelledCount = 3
	m.Compact.CancelledBytes = 3 * 1024
	m.Compact.FailedCount = 5
	m.Compact.NumProblemSpans = 2
	m.Flush.Count = 8
	m.Flush.AsIngestBytes = 34
	m.Flush.AsIngestTableCount = 35
	m.Flush.AsIngestCount = 36
	m.Filter.Hits = 9
	m.Filter.Misses = 10
	m.MemTable.Size = 11
	m.MemTable.Count = 12
	m.MemTable.ZombieSize = 13
	m.MemTable.ZombieCount = 14
	m.Keys.RangeKeySetsCount = 123
	m.Keys.TombstoneCount = 456
	m.Keys.MissizedTombstonesCount = 789
	m.Keys.MaxUserKeySize = 1234
	m.Keys.MaxUserKeySizeUnknownTables = 5
	m.Snapshots.Count = 4
	m.Snapshots.EarliestSeqNum = 1024
	m.Table.ZombieSize = 15
	m.Table.BackingTableCount = 1
	m.Table.BackingTableSize = 2 << 20
	m.Table.ZombieCount = 16
	m.FileCache.Size = 17
	m.FileCache.Count = 18
	m.FileCache.Hits = 19
	m.FileCache.Misses = 20
	m.TableIters = 21
	m.WAL.Files = 22
	m.WAL.ObsoleteFiles = 23
	m.WAL.Size = 24
	m.WAL.BytesIn = 25
	m.WAL.BytesWritten = 26
	m.Ingest.Count = 27
	m.Table.Local.LiveSize = 28
	m.Table.Local.ObsoleteSize = 29
	m.Table.Local.ZombieSize = 30
	m.Table.PendingStatsCollectionCount = 31
	m.Table.InitialStatsCollectionComplete = true

	for i := range m.Levels {
		l := &m.Levels[i]
		base := uint64((i + 1) * 100)
		l.Sublevels = int32(i + 1)
		l.NumFiles = int64(base) + 1
		l.NumVirtualFiles = uint64(base) + 1
		l.VirtualSize = base + 3
		l.Size = int64(base) + 2
		if i < numLevels-1 {
			l.Score = 1.0 + float64(i+1)*0.1
		}
		l.UncompensatedScore = 2.0 + float64(i+1)*0.1
		l.CompensatedScore = 3.0 * +float64(i+1) * 0.1
		l.BytesIn = base + 4
		l.BytesIngested = base + 4
		l.BytesMoved = base + 6
		l.BytesRead = base + 7
		l.BytesCompacted = base + 8
		l.BytesFlushed = base + 9
		l.TablesCompacted = base + 10
		l.TablesFlushed = base + 11
		l.TablesIngested = base + 12
		l.TablesMoved = base + 13
		l.MultiLevel.BytesInTop = base + 4
		l.MultiLevel.BytesIn = base + 4
		l.MultiLevel.BytesRead = base + 4
	}
	for i := range m.manualMemory {
		m.manualMemory[i].InUseBytes = uint64((i + 1) * 1024)
	}
	return m
}

func init() {
	// Register some categories for the purposes of the test.
	block.RegisterCategory("a", block.NonLatencySensitiveQoSLevel)
	block.RegisterCategory("b", block.LatencySensitiveQoSLevel)
	block.RegisterCategory("c", block.NonLatencySensitiveQoSLevel)
}

func TestMetrics(t *testing.T) {
	if runtime.GOARCH == "386" {
		t.Skip("skipped on 32-bit due to slightly varied output")
	}
	defer block.DeterministicReadBlockDurationForTesting()()

	var d *DB
	var iters map[string]*Iterator
	var closeFunc func()
	var memFS *vfs.MemFS
	var remoteStorage remote.Storage
	defer func() {
		if closeFunc != nil {
			closeFunc()
		}
	}()
	init := func(t *testing.T, createOnSharedLower bool, reopen bool) {
		if closeFunc != nil {
			closeFunc()
		}
		if !reopen {
			memFS = vfs.NewMem()
			remoteStorage = remote.NewInMem()
		}
		c := cache.New(cacheDefaultSize)
		defer c.Unref()
		opts := &Options{
			Cache:                 c,
			Comparer:              testkeys.Comparer,
			FormatMajorVersion:    FormatNewest,
			FS:                    memFS,
			L0CompactionThreshold: 8,
			// Large value for determinism.
			MaxOpenFiles: 10000,
		}
		opts.Experimental.EnableValueBlocks = func() bool { return true }
		opts.Experimental.EnableColumnarBlocks = func() bool { return true }
		opts.Levels = append(opts.Levels, LevelOptions{TargetFileSize: 50})

		// Prevent foreground flushes and compactions from triggering asynchronous
		// follow-up compactions. This avoids asynchronously-scheduled work from
		// interfering with the expected metrics output and reduces test flakiness.
		opts.DisableAutomaticCompactions = true

		// Increase the threshold for memtable stalls to allow for more flushable
		// ingests.
		opts.MemTableStopWritesThreshold = 4

		opts.Experimental.RemoteStorage = remote.MakeSimpleFactory(map[remote.Locator]remote.Storage{
			"": remoteStorage,
		})
		if createOnSharedLower {
			opts.Experimental.CreateOnShared = remote.CreateOnSharedLower
		} else {
			opts.Experimental.CreateOnShared = remote.CreateOnSharedNone
		}
		var err error
		d, err = Open("", opts)
		require.NoError(t, err)
		if createOnSharedLower {
			require.NoError(t, d.SetCreatorID(1))
		}
		if reopen {
			// Stats population is eventually consistent, and happens in the
			// background when a DB is re-opened. To avoid races, wait synchronously
			// for all tables to have their stats fully populated, which requires
			// opening each SST.
			d.mu.Lock()
			for !d.mu.tableStats.loadedInitial {
				d.mu.tableStats.cond.Wait()
			}
			d.mu.Unlock()
		}
		iters = make(map[string]*Iterator)
		closeFunc = func() {
			for _, i := range iters {
				require.NoError(t, i.Close())
			}
			require.NoError(t, d.Close())
		}
	}
	datadriven.RunTest(t, "testdata/metrics", func(t *testing.T, td *datadriven.TestData) string {
		switch td.Cmd {
		case "init":
			createOnSharedLower := false
			if td.HasArg("shared-lower") {
				createOnSharedLower = true
			}
			reopen := false
			if td.HasArg("reopen") {
				reopen = true
			}
			init(t, createOnSharedLower, reopen)
			return ""

		case "example":
			m := exampleMetrics()
			res := m.String()

			// Nothing in the metrics should be redacted.
			redacted := string(redact.Sprintf("%s", &m).Redact())
			if redacted != res {
				td.Fatalf(t, "redacted metrics don't match\nunredacted:\n%s\nredacted:%s\n", res, redacted)
			}
			return res

		case "batch":
			b := d.NewBatch()
			if err := runBatchDefineCmd(td, b); err != nil {
				return err.Error()
			}
			b.Commit(nil)
			return ""

		case "build":
			if err := runBuildCmd(td, d, d.opts.FS); err != nil {
				return err.Error()
			}
			return ""

		case "compact":
			if err := runCompactCmd(td, d); err != nil {
				return err.Error()
			}

			d.mu.Lock()
			s := d.mu.versions.currentVersion().String()
			d.mu.Unlock()
			return s

		case "delay-flush":
			d.mu.Lock()
			defer d.mu.Unlock()
			switch td.Input {
			case "enable":
				d.mu.compact.flushing = true
			case "disable":
				d.mu.compact.flushing = false
			default:
				return fmt.Sprintf("unknown directive %q (expected 'enable'/'disable')", td.Input)
			}
			return ""

		case "flush":
			if err := d.Flush(); err != nil {
				return err.Error()
			}

			d.mu.Lock()
			s := d.mu.versions.currentVersion().String()
			d.mu.Unlock()
			return s

		case "ingest":
			if err := runIngestCmd(td, d, d.opts.FS); err != nil {
				return err.Error()
			}
			return ""

		case "lsm":
			d.mu.Lock()
			s := d.mu.versions.currentVersion().String()
			d.mu.Unlock()
			return s

		case "ingest-and-excise":
			if err := runIngestAndExciseCmd(td, d); err != nil {
				return err.Error()
			}
			return ""

		case "iter-close":
			if len(td.CmdArgs) != 1 {
				return "iter-close <name>"
			}
			name := td.CmdArgs[0].String()
			if iter := iters[name]; iter != nil {
				if err := iter.Close(); err != nil {
					return err.Error()
				}
				delete(iters, name)
			} else {
				return fmt.Sprintf("%s: not found", name)
			}

			// The deletion of obsolete files happens asynchronously when an iterator
			// is closed. Wait for the obsolete tables to be deleted.
			d.cleanupManager.Wait()
			return ""

		case "iter-new":
			if len(td.CmdArgs) < 1 {
				return "iter-new <name>"
			}
			name := td.CmdArgs[0].String()
			if iter := iters[name]; iter != nil {
				if err := iter.Close(); err != nil {
					return err.Error()
				}
			}
			category := block.CategoryUnknown
			if td.HasArg("category") {
				var s string
				td.ScanArgs(t, "category", &s)
				category = block.StringToCategoryForTesting(s)
			}
			iter, _ := d.NewIter(&IterOptions{Category: category})
			// Some iterators (eg. levelIter) do not instantiate the underlying
			// iterator until the first positioning call. Position the iterator
			// so that levelIters will have loaded an sstable.
			iter.First()
			iters[name] = iter
			return ""

		case "metrics":
			// The asynchronous loading of table stats can change metrics, so
			// wait for all the tables' stats to be loaded.
			d.mu.Lock()
			d.waitTableStatsInitialLoad()
			d.waitTableStats()
			d.mu.Unlock()

			m := d.Metrics()
			// Don't show memory usage as that can depend on architecture, invariants
			// tag, etc.
			m.manualMemory = manual.Metrics{}
			// Some subset of cases show non-determinism in cache hits/misses.
			if td.HasArg("zero-cache-hits-misses") {
				// Avoid non-determinism.
				m.FileCache = cache.Metrics{}
				m.BlockCache = cache.Metrics{}
				// Empirically, the unknown stats are also non-deterministic.
				if len(m.CategoryStats) > 0 && m.CategoryStats[0].Category == block.CategoryUnknown {
					m.CategoryStats[0].CategoryStats = block.CategoryStats{}
				}
			}
			var buf strings.Builder
			fmt.Fprintf(&buf, "%s", m.StringForTests())
			if len(m.CategoryStats) > 0 {
				fmt.Fprintf(&buf, "Iter category stats:\n")
				for _, stats := range m.CategoryStats {
					fmt.Fprintf(&buf, "%20s, %11s: %+v\n", stats.Category,
						redact.StringWithoutMarkers(stats.Category.QoSLevel()), stats.CategoryStats)
				}
			}
			return buf.String()

		case "metrics-value":
			// metrics-value confirms the value of a given metric. Note that there
			// are some metrics which aren't deterministic and behave differently
			// for invariant/non-invariant builds. An example of this is cache
			// hit rates. Under invariant builds, the excising code will try
			// to create iterators and confirm that the virtual sstable bounds
			// are accurate. Reads on these iterators will change the cache hit
			// rates.
			lines := strings.Split(td.Input, "\n")
			for _, line := range lines {
				if strings.HasPrefix(line, "max-user-key-size") {
					// These metrics are derived from the tables' properties, which
					// are loaded asynchronously when the DB is reopened.
					waitTableStatsForTest(d)
					break
				}
			}
			m := d.Metrics()
			// TODO(bananabrick): Use reflection to pull the values associated
			// with the metrics fields.
			var buf bytes.Buffer
			for i := range lines {
				line := lines[i]
				if line == "num-backing" {
					buf.WriteString(fmt.Sprintf("%d\n", m.Table.BackingTableCount))
				} else if line == "backing-size" {
					buf.WriteString(fmt.Sprintf("%s\n", humanize.Bytes.Uint64(m.Table.BackingTableSize)))
				} else if line == "virtual-size" {
					buf.WriteString(fmt.Sprintf("%s\n", humanize.Bytes.Uint64(m.VirtualSize())))
				} else if strings.HasPrefix(line, "num-virtual") {
					splits := strings.Split(line, " ")
					if len(splits) == 1 {
						buf.WriteString(fmt.Sprintf("%d\n", m.NumVirtual()))
						continue
					}
					// Level is specified.
					l, err := strconv.Atoi(splits[1])
					if err != nil {
						panic(err)
					}
					if l >= numLevels {
						panic(fmt.Sprintf("invalid level %d", l))
					}
					buf.WriteString(fmt.Sprintf("%d\n", m.Levels[l].NumVirtualFiles))
				} else if line == "max-user-key-size" {
					buf.WriteString(fmt.Sprintf("%d\n", m.Keys.MaxUserKeySize))
				} else if line == "max-user-key-size-unknown-tables" {
					buf.WriteString(fmt.Sprintf("%d\n", m.Keys.MaxUserKeySizeUnknownTables))
				} else {
					panic(fmt.Sprintf("invalid field: %s", line))
				}
			}
			return buf.String()

		case "disk-usage":
			return humanize.Bytes.Uint64(d.Metrics().DiskSpaceUsage()).String()

		case "additional-metrics":
			// The asynchronous loading of table stats can change metrics, so
			// wait for all the tables' stats to be loaded.
			d.mu.Lock()
			d.waitTableStats()
			d.mu.Unlock()

			m := d.Metrics()
			var b strings.Builder
			fmt.Fprintf(&b, "block bytes written:\n")
			fmt.Fprintf(&b, " __level___data-block__value-block\n")
			for i := range m.Levels {
				fmt.Fprintf(&b, "%7d ", i)
				fmt.Fprintf(&b, "%12s %12s\n",
					humanize.Bytes.Uint64(m.Levels[i].Additional.BytesWrittenDataBlocks),
					humanize.Bytes.Uint64(m.Levels[i].Additional.BytesWrittenValueBlocks))
			}
			return b.String()

		case "problem-spans":
			d.mu.Lock()
			defer d.mu.Unlock()
			d.problemSpans.Init(manifest.NumLevels, d.cmp)
			for _, line := range crstrings.Lines(td.Input) {
				var level int
				var span1, span2 string
				n, err := fmt.Sscanf(line, "L%d %s %s", &level, &span1, &span2)
				if err != nil || n != 3 {
					td.Fatalf(t, "malformed problem span %q", line)
				}
				bounds := base.ParseUserKeyBounds(span1 + " " + span2)
				d.problemSpans.Add(level, bounds, time.Hour*10)
			}
			return ""

		default:
			return fmt.Sprintf("unknown command: %s", td.Cmd)
		}
	})
}

func TestMetricsWAmpDisableWAL(t *testing.T) {
	d, err := Open("", &Options{FS: vfs.NewMem(), DisableWAL: true})
	require.NoError(t, err)
	ks := testkeys.Alpha(2)
	wo := WriteOptions{Sync: false}
	for i := 0; i < 5; i++ {
		v := []byte(strconv.Itoa(i))
		for j := int64(0); j < ks.Count(); j++ {
			require.NoError(t, d.Set(testkeys.Key(ks, j), v, &wo))
		}
		require.NoError(t, d.Flush())
		require.NoError(t, d.Compact([]byte("a"), []byte("z"), false /* parallelize */))
	}
	m := d.Metrics()
	tot := m.Total()
	require.Greater(t, tot.WriteAmp(), 1.0)
	require.NoError(t, d.Close())
}

// TestMetricsWALBytesWrittenMonotonicity tests that the
// Metrics.WAL.BytesWritten metric is always nondecreasing.
// It's a regression test for issue #3505.
func TestMetricsWALBytesWrittenMonotonicity(t *testing.T) {
	fs := errorfs.Wrap(vfs.NewMem(), errorfs.RandomLatency(
		nil, 100*time.Microsecond, time.Now().UnixNano(), 0 /* no limit */))
	d, err := Open("", &Options{
		FS: fs,
		// Use a tiny memtable size so that we get frequent flushes. While a
		// flush is in-progress or completing, the WAL bytes written should
		// remain nondecreasing.
		MemTableSize: 1 << 20, /* 20 KiB */
	})
	require.NoError(t, err)

	stopCh := make(chan struct{})

	ks := testkeys.Alpha(3)
	var wg sync.WaitGroup
	const concurrentWriters = 5
	wg.Add(concurrentWriters)
	for w := 0; w < concurrentWriters; w++ {
		go func() {
			defer wg.Done()
			data := make([]byte, 1<<10) // 1 KiB
			rng := rand.New(rand.NewPCG(0, uint64(time.Now().UnixNano())))
			for i := range data {
				data[i] = byte(rng.Uint32())
			}

			buf := make([]byte, ks.MaxLen())
			for i := 0; ; i++ {
				select {
				case <-stopCh:
					return
				default:
				}
				n := testkeys.WriteKey(buf, ks, int64(i)%ks.Count())
				require.NoError(t, d.Set(buf[:n], data, NoSync))
			}
		}()
	}

	func() {
		defer func() { close(stopCh) }()
		abort := time.After(time.Second)
		var prevWALBytesWritten uint64
		for {
			select {
			case <-abort:
				return
			default:
			}

			m := d.Metrics()
			if m.WAL.BytesWritten < prevWALBytesWritten {
				t.Fatalf("WAL bytes written decreased: %d -> %d", prevWALBytesWritten, m.WAL.BytesWritten)
			}
			prevWALBytesWritten = m.WAL.BytesWritten
		}
	}()
	wg.Wait()
}

// waitTableStatsForTest waits until the stats of all tables are loaded, unless
// table stats are disabled.
func waitTableStatsForTest(d *DB) {
	if d.opts.DisableTableStats {
		return
	}
	d.mu.Lock()
	defer d.mu.Unlock()
	d.waitTableStatsInitialLoad()
	d.waitTableStats()
}

// TestMetricsMaxUserKeySize exercises Metrics.Keys.MaxUserKeySize and
// Metrics.Keys.MaxUserKeySizeUnknownTables, which are aggregated across the
// LSM from the tables' backings (which are populated from the table
// properties).
//
// Commands:
//
//   - init [disable-table-stats] [shared-lower] [format-major-version=<n>]:
//     creates a new DB on a fresh MemFS, using the testkeys comparer. With
//     shared-lower, the lower levels are created on shared storage.
//   - open-fixture <dir> [format-major-version=<n>]: opens a copy of the DB in
//     testdata/<dir>, which was created by an older version of Pebble, using the
//     default comparer. Prints the format major version.
//   - reopen [disable-table-stats]: closes and reopens the DB, on the same FS
//     and with the same options. With disable-table-stats, the tables' stats
//     are never loaded (except for tables whose stats are set at creation).
//   - enable-table-stats: enables table stats collection (after a reopen
//     with disable-table-stats) and starts collecting the stats.
//   - batch, flush, compact, build, ingest, ingest-and-excise, excise,
//     build-remote, ingest-external, lsm: like elsewhere.
//   - download <start> <end> [via-backing-file-download]: downloads the
//     external tables overlapping the span.
//   - max-key-size: waits for table stats (unless disabled), then prints the
//     per-table MaxUserKeySize (from the table backing) followed by the
//     LSM-wide metrics.
func TestMetricsMaxUserKeySize(t *testing.T) {
	defer leaktest.AfterTest(t)()

	var d *DB
	var opts *Options
	var remoteStorage remote.Storage
	defer func() {
		if d != nil {
			require.NoError(t, d.Close())
		}
	}()
	closeDB := func() {
		if d != nil {
			require.NoError(t, d.Close())
			d = nil
		}
	}
	parseFormatMajorVersion := func(td *datadriven.TestData, def FormatMajorVersion) FormatMajorVersion {
		if !td.HasArg("format-major-version") {
			return def
		}
		var v uint64
		td.ScanArgs(t, "format-major-version", &v)
		return FormatMajorVersion(v)
	}

	datadriven.RunTest(t, "testdata/metrics_max_user_key_size", func(t *testing.T, td *datadriven.TestData) string {
		switch td.Cmd {
		case "init":
			closeDB()
			remoteStorage = remote.NewInMem()
			opts = &Options{
				Comparer:                    testkeys.Comparer,
				FS:                          vfs.NewMem(),
				FormatMajorVersion:          parseFormatMajorVersion(td, FormatNewest),
				DisableAutomaticCompactions: true,
				DisableTableStats:           td.HasArg("disable-table-stats"),
				DebugCheck:                  DebugCheckLevels,
				Logger:                      testLogger{t: t},
			}
			opts.Experimental.RemoteStorage = remote.MakeSimpleFactory(map[remote.Locator]remote.Storage{
				"external-locator": remoteStorage,
				// Shared storage.
				"": remote.NewInMem(),
			})
			opts.Experimental.CreateOnShared = remote.CreateOnSharedNone
			if td.HasArg("shared-lower") {
				opts.Experimental.CreateOnShared = remote.CreateOnSharedLower
			}
			var err error
			d, err = Open("", opts)
			require.NoError(t, err)
			if td.HasArg("shared-lower") {
				require.NoError(t, d.SetCreatorID(1))
			}
			return ""

		case "open-fixture":
			closeDB()
			if len(td.CmdArgs) < 1 {
				td.Fatalf(t, "open-fixture <dir>")
			}
			dir := td.CmdArgs[0].String()
			fs := vfs.NewMem()
			_, err := vfs.Clone(vfs.Default, fs, filepath.Join("testdata", dir), dir)
			require.NoError(t, err)
			opts = &Options{
				// The fixtures use the default comparer.
				FS:                          fs,
				FormatMajorVersion:          parseFormatMajorVersion(td, FormatDefault),
				DisableAutomaticCompactions: true,
				DebugCheck:                  DebugCheckLevels,
				Logger:                      testLogger{t: t},
			}
			d, err = Open(dir, opts)
			require.NoError(t, err)
			return fmt.Sprintf("format major version: %d", d.FormatMajorVersion())

		case "reopen":
			dirname := d.dirname
			closeDB()
			opts.DisableTableStats = td.HasArg("disable-table-stats")
			var err error
			d, err = Open(dirname, opts)
			require.NoError(t, err)
			return ""

		case "enable-table-stats":
			d.mu.Lock()
			d.opts.DisableTableStats = false
			d.maybeCollectTableStatsLocked()
			d.mu.Unlock()
			return ""

		case "batch":
			b := d.NewBatch()
			if err := runBatchDefineCmd(td, b); err != nil {
				return err.Error()
			}
			if err := b.Commit(nil); err != nil {
				return err.Error()
			}
			return ""

		case "flush":
			if err := d.Flush(); err != nil {
				return err.Error()
			}
			return runLSMCmd(td, d)

		case "compact":
			if err := runCompactCmd(td, d); err != nil {
				return err.Error()
			}
			return runLSMCmd(td, d)

		case "build":
			if err := runBuildCmd(td, d, d.opts.FS); err != nil {
				return err.Error()
			}
			return ""

		case "ingest":
			if err := runIngestCmd(td, d, d.opts.FS); err != nil {
				return err.Error()
			}
			return runLSMCmd(td, d)

		case "ingest-and-excise":
			if err := runIngestAndExciseCmd(td, d); err != nil {
				return err.Error()
			}
			return runLSMCmd(td, d)

		case "excise":
			if err := runExciseCmd(td, d); err != nil {
				return err.Error()
			}
			return runLSMCmd(td, d)

		case "build-remote":
			if err := runBuildRemoteCmd(td, d, remoteStorage); err != nil {
				return err.Error()
			}
			return ""

		case "ingest-external":
			if err := runIngestExternalCmd(t, td, d, remoteStorage, "external-locator"); err != nil {
				return err.Error()
			}
			return runLSMCmd(td, d)

		case "download":
			if len(td.CmdArgs) < 2 {
				td.Fatalf(t, "download <start> <end> [via-backing-file-download]")
			}
			ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
			defer cancel()
			if err := d.Download(ctx, []DownloadSpan{{
				StartKey:               []byte(td.CmdArgs[0].String()),
				EndKey:                 []byte(td.CmdArgs[1].String()),
				ViaBackingFileDownload: td.HasArg("via-backing-file-download"),
			}}); err != nil {
				return err.Error()
			}
			return runLSMCmd(td, d)

		case "lsm":
			return runLSMCmd(td, d)

		case "max-key-size":
			waitTableStatsForTest(d)
			var buf strings.Builder
			d.mu.Lock()
			v := d.mu.versions.currentVersion()
			for l := range v.Levels {
				if v.Levels[l].Empty() {
					continue
				}
				fmt.Fprintf(&buf, "L%d:\n", l)
				for f := range v.Levels[l].All() {
					fmt.Fprintf(&buf, "  %s", f.FileNum)
					if f.Virtual {
						fmt.Fprintf(&buf, "(%s)", f.FileBacking.DiskFileNum)
					}
					if size := f.FileBacking.MaxUserKeySize(); size != 0 {
						fmt.Fprintf(&buf, ": %d", size)
						if !f.StatsValid() {
							buf.WriteString(" (stats not loaded)")
						}
					} else if !f.StatsValid() {
						buf.WriteString(": stats not loaded")
					} else {
						buf.WriteString(": property absent")
					}
					if p := f.SyntheticPrefixAndSuffix.Prefix(); p != nil {
						fmt.Fprintf(&buf, " synthetic-prefix=%q", p)
					}
					if s := f.SyntheticPrefixAndSuffix.Suffix(); s != nil {
						fmt.Fprintf(&buf, " synthetic-suffix=%q", s)
					}
					buf.WriteString("\n")
				}
			}
			d.mu.Unlock()
			m := d.Metrics()
			fmt.Fprintf(&buf, "max user key size: %d\n", m.Keys.MaxUserKeySize)
			fmt.Fprintf(&buf, "unknown tables: %d\n", m.Keys.MaxUserKeySizeUnknownTables)
			return buf.String()

		default:
			return fmt.Sprintf("unknown command: %s", td.Cmd)
		}
	})
}
