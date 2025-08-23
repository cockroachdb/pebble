// Copyright 2019 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package pebble

import (
	"bytes"
	"context"
	"fmt"
	"math/rand/v2"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/cockroachdb/crlib/crstrings"
	"github.com/cockroachdb/datadriven"
	"github.com/cockroachdb/pebble/internal/base"
	"github.com/cockroachdb/pebble/internal/cache"
	"github.com/cockroachdb/pebble/internal/humanize"
	"github.com/cockroachdb/pebble/internal/manifest"
	"github.com/cockroachdb/pebble/internal/testkeys"
	"github.com/cockroachdb/pebble/internal/testutils"
	"github.com/cockroachdb/pebble/objstorage/remote"
	"github.com/cockroachdb/pebble/sstable/block"
	"github.com/cockroachdb/pebble/vfs"
	"github.com/cockroachdb/pebble/vfs/errorfs"
	"github.com/cockroachdb/redact"
	"github.com/stretchr/testify/require"
)

func exampleMetrics() Metrics {
	const MB = 1 << 20
	const GB = 1 << 30

	var m Metrics
	m.BlockCache.Size = 1 * GB
	m.BlockCache.Count = 100
	m.BlockCache.Hits = 10000
	m.BlockCache.Misses = 5000

	m.Compact.Count = 1000
	m.Compact.DefaultCount = 10
	m.Compact.DeleteOnlyCount = 11
	m.Compact.ElisionOnlyCount = 12
	m.Compact.CopyCount = 13
	m.Compact.MoveCount = 14
	m.Compact.ReadCount = 15
	m.Compact.TombstoneDensityCount = 16
	m.Compact.RewriteCount = 17
	m.Compact.MultiLevelCount = 18
	m.Compact.BlobFileRewriteCount = 19
	m.Compact.EstimatedDebt = 6 * GB
	m.Compact.InProgressBytes = 1 * MB
	m.Compact.NumInProgress = 2
	m.Compact.CancelledCount = 3
	m.Compact.CancelledBytes = 3 * 1024
	m.Compact.FailedCount = 4
	m.Compact.NumProblemSpans = 5
	m.Compact.MarkedFiles = 6
	m.Compact.Duration = 10 * time.Hour

	m.Ingest.Count = 12

	m.Flush.Count = 8
	m.Flush.WriteThroughput.Bytes = 2 * MB
	m.Flush.WriteThroughput.WorkDuration = time.Hour
	m.Flush.WriteThroughput.IdleDuration = 10 * time.Hour
	m.Flush.NumInProgress = 2
	m.Flush.AsIngestCount = 4
	m.Flush.AsIngestTableCount = 5
	m.Flush.AsIngestBytes = 6

	m.Filter.Hits = 9
	m.Filter.Misses = 10

	for i := range m.Levels {
		l := &m.Levels[i]
		base := uint64((i + 1) * 100)
		l.Sublevels = int32(i + 1)
		l.TablesCount = int64(base) + 1
		l.VirtualTablesCount = uint64(base) + 1
		l.VirtualTablesSize = base + 3
		l.TablesSize = int64(base) + 2
		l.EstimatedReferencesSize = base + 14
		if i < numLevels-1 {
			l.Score = 1.0 + float64(i+1)*0.1
		}
		l.FillFactor = 2.0 + float64(i+1)*0.1
		l.CompensatedFillFactor = 3.0 * +float64(i+1) * 0.1
		l.TableBytesIn = base + 4
		l.TableBytesIngested = base + 4
		l.TableBytesMoved = base + 6
		l.TableBytesRead = base + 7
		l.TableBytesCompacted = base + 8
		l.TableBytesFlushed = base + 9
		l.TablesCompacted = base + 10
		l.TablesFlushed = base + 11
		l.TablesIngested = base + 12
		l.TablesMoved = base + 13
		l.Additional.ValueBlocksSize = base + 14
		l.BlobBytesCompacted = base + 15
		l.BlobBytesFlushed = base + 16
		l.BlobBytesRead = base + 17
		l.MultiLevel.TableBytesInTop = base + 4
		l.MultiLevel.TableBytesIn = base + 4
		l.MultiLevel.TableBytesRead = base + 4
	}

	m.MemTable.Size = 2 * GB
	m.MemTable.Count = 12
	m.MemTable.ZombieSize = 13 * MB
	m.MemTable.ZombieCount = 5

	m.Keys.RangeKeySetsCount = 123
	m.Keys.TombstoneCount = 456
	m.Keys.MissizedTombstonesCount = 789

	m.Snapshots.Count = 4
	m.Snapshots.EarliestSeqNum = 1024
	m.Snapshots.PinnedKeys = 1234
	m.Snapshots.PinnedSize = 3 * GB

	m.Table.ObsoleteSize = 15 * MB
	m.Table.ObsoleteCount = 16
	m.Table.ZombieSize = 17 * MB
	m.Table.ZombieCount = 18
	m.Table.BackingTableCount = 1
	m.Table.BackingTableSize = 2 * MB
	m.Table.Compression.NoCompressionBytes = 100 * MB
	m.Table.Compression.CompressedBytesWithoutStats = 500 * MB
	m.Table.Compression.Snappy.CompressedBytes = 1 * GB
	m.Table.Compression.Snappy.UncompressedBytes = 2 * GB
	m.Table.Compression.MinLZ.CompressedBytes = 1 * GB
	m.Table.Compression.MinLZ.UncompressedBytes = 3 * GB
	m.Table.Compression.Zstd.CompressedBytes = 10 * GB
	m.Table.Compression.Zstd.UncompressedBytes = 50 * GB
	m.Table.Local.LiveSize = 28 * GB
	m.Table.Local.LiveCount = 10_000
	m.Table.Local.ObsoleteSize = 29 * MB
	m.Table.Local.ObsoleteCount = 13
	m.Table.Local.ZombieSize = 30 * MB
	m.Table.Local.ZombieCount = 14
	m.Table.Garbage.PointDeletionsBytesEstimate = 1 * MB
	m.Table.Garbage.RangeDeletionsBytesEstimate = 2 * MB
	m.Table.InitialStatsCollectionComplete = true
	m.Table.PendingStatsCollectionCount = 31

	m.BlobFiles.LiveCount = 1234
	m.BlobFiles.LiveSize = 15 * GB
	m.BlobFiles.ValueSize = 14 * GB
	m.BlobFiles.ReferencedValueSize = 11 * GB
	m.BlobFiles.ObsoleteCount = 13
	m.BlobFiles.ObsoleteSize = 29 * MB
	m.BlobFiles.ZombieCount = 14
	m.BlobFiles.ZombieSize = 30 * MB
	m.BlobFiles.Local.LiveSize = 28 * GB
	m.BlobFiles.Local.LiveCount = 10_000
	m.BlobFiles.Local.ObsoleteSize = 29 * MB
	m.BlobFiles.Local.ObsoleteCount = 13
	m.BlobFiles.Local.ZombieSize = 30 * MB
	m.BlobFiles.Local.ZombieCount = 14
	m.BlobFiles.Compression.NoCompressionBytes = 10 * MB
	m.BlobFiles.Compression.CompressedBytesWithoutStats = 50 * MB
	m.BlobFiles.Compression.Snappy.CompressedBytes = 10 * GB
	m.BlobFiles.Compression.Snappy.UncompressedBytes = 20 * GB
	m.BlobFiles.Compression.MinLZ.CompressedBytes = 10 * GB
	m.BlobFiles.Compression.MinLZ.UncompressedBytes = 30 * GB
	m.BlobFiles.Compression.Zstd.CompressedBytes = 100 * GB
	m.BlobFiles.Compression.Zstd.UncompressedBytes = 500 * GB

	m.FileCache.Size = 1 * MB
	m.FileCache.TableCount = 180
	m.FileCache.BlobFileCount = 181
	m.FileCache.Hits = 19
	m.FileCache.Misses = 20

	m.TableIters = 21
	m.Uptime = 72 * time.Hour

	m.WAL.Files = 22
	m.WAL.ObsoleteFiles = 23
	m.WAL.Size = 24
	m.WAL.BytesIn = 25
	m.WAL.BytesWritten = 26

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
	defer base.DeterministicReadDurationForTesting()()

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
			FormatMajorVersion:    internalFormatNewest,
			FS:                    memFS,
			Logger:                testutils.Logger{T: t},
			L0CompactionThreshold: 8,
			// Large value for determinism.
			MaxOpenFiles: 10000,
		}
		opts.Experimental.EnableValueBlocks = func() bool { return true }
		opts.Experimental.ValueSeparationPolicy = func() ValueSeparationPolicy {
			return ValueSeparationPolicy{
				Enabled:               true,
				MinimumSize:           3,
				MaxBlobReferenceDepth: 5,
			}
		}
		opts.TargetFileSizes[0] = 50

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
			d.waitTableStats()

			m := d.Metrics()
			// Some subset of cases show non-determinism in cache hits/misses.
			if td.HasArg("zero-cache-hits-misses") {
				// Avoid non-determinism.
				m.FileCache = FileCacheMetrics{}
				m.BlockCache = cache.Metrics{}
				// Empirically, the unknown stats are also non-deterministic.
				if len(m.CategoryStats) > 0 && m.CategoryStats[0].Category == block.CategoryUnknown {
					m.CategoryStats[0].CategoryStats = block.CategoryStats{}
				}
			}
			var buf strings.Builder
			fmt.Fprintf(&buf, "%s\n", m.StringForTests())
			if len(m.CategoryStats) > 0 {
				fmt.Fprintf(&buf, "\nIter category stats:\n")
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
					buf.WriteString(fmt.Sprintf("%d\n", m.Levels[l].VirtualTablesCount))
				} else if line == "remote-count" {
					count, _ := m.RemoteTablesTotal()
					buf.WriteString(fmt.Sprintf("%d\n", count))
				} else if line == "remote-size" {
					_, size := m.RemoteTablesTotal()
					buf.WriteString(fmt.Sprintf("%s\n", humanize.Bytes.Uint64(size)))
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
			d.waitTableStats()

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
	d, err := Open("", &Options{FS: vfs.NewMem(), DisableWAL: true, Logger: testutils.Logger{T: t}})
	require.NoError(t, err)
	ks := testkeys.Alpha(2)
	wo := WriteOptions{Sync: false}
	for i := 0; i < 5; i++ {
		v := []byte(strconv.Itoa(i))
		for j := int64(0); j < ks.Count(); j++ {
			require.NoError(t, d.Set(testkeys.Key(ks, j), v, &wo))
		}
		require.NoError(t, d.Flush())
		require.NoError(t, d.Compact(
			context.Background(), []byte("a"), []byte("z"), false /* parallelize */))
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
		FS:     fs,
		Logger: testutils.Logger{T: t},
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
