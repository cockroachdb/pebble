// Copyright 2019 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package pebble

import (
	"fmt"
	"iter"
	"math"
	"slices"
	"time"
	"unsafe"

	"github.com/cockroachdb/crlib/crhumanize"
	"github.com/cockroachdb/pebble/internal/ascii"
	"github.com/cockroachdb/pebble/internal/ascii/table"
	"github.com/cockroachdb/pebble/internal/base"
	"github.com/cockroachdb/pebble/internal/cache"
	"github.com/cockroachdb/pebble/internal/compression"
	"github.com/cockroachdb/pebble/internal/manifest"
	"github.com/cockroachdb/pebble/internal/manual"
	"github.com/cockroachdb/pebble/objstorage/objstorageprovider/sharedcache"
	"github.com/cockroachdb/pebble/record"
	"github.com/cockroachdb/pebble/sstable"
	"github.com/cockroachdb/pebble/sstable/blob"
	"github.com/cockroachdb/pebble/sstable/block"
	"github.com/cockroachdb/pebble/wal"
	"github.com/cockroachdb/redact"
	"github.com/prometheus/client_golang/prometheus"
	"golang.org/x/exp/constraints"
)

// CacheMetrics holds metrics for the block and file cache.
type CacheMetrics = cache.Metrics

// FilterMetrics holds metrics for the filter policy
type FilterMetrics = sstable.FilterMetrics

// ThroughputMetric is a cumulative throughput metric. See the detailed
// comment in base.
type ThroughputMetric = base.ThroughputMetric

// SecondaryCacheMetrics holds metrics for the persistent secondary cache
// that caches commonly accessed blocks from blob storage on a local
// file system.
type SecondaryCacheMetrics = sharedcache.Metrics

// LevelMetrics holds per-level metrics such as the number of files and total
// size of the files, and compaction related metrics.
type LevelMetrics struct {
	// The number of sublevels within the level. The sublevel count corresponds
	// to the read amplification for the level. An empty level will have a
	// sublevel count of 0, implying no read amplification. Only L0 will have
	// a sublevel count other than 0 or 1.
	Sublevels int32
	// The total count of sstables in the level.
	TablesCount int64
	// The total size in bytes of the sstables in the level. Note that if tables
	// contain references to blob files, this quantity does not include the the
	// size of the blob files or the referenced values.
	TablesSize int64
	// The total number of virtual sstables in the level.
	VirtualTablesCount uint64
	// The total size of the virtual sstables in the level.
	VirtualTablesSize uint64
	// The estimated total physical size of all blob references across all
	// sstables in the level. The physical size is estimated based on the size
	// of referenced values and the values' blob file's compression ratios.
	EstimatedReferencesSize uint64
	// The level's compaction score, used to rank levels (0 if the level doesn't
	// need compaction). See candidateLevelInfo.
	Score float64
	// The level's fill factor (the ratio between the size of the level and the
	// ideal size). See candidateLevelInfo.
	FillFactor float64
	// The level's compensated fill factor. See candidateLevelInfo.
	CompensatedFillFactor float64
	// The number of incoming bytes from other levels' sstables read during
	// compactions. This excludes bytes moved and bytes ingested. For L0 this is
	// the bytes written to the WAL.
	TableBytesIn uint64
	// The number of sstable bytes ingested. The sibling metric for tables is
	// TablesIngested.
	TableBytesIngested uint64
	// The number of sstable bytes moved into the level by a "move" compaction.
	// The sibling metric for tables is TablesMoved.
	TableBytesMoved uint64
	// The number of bytes read for compactions at the level. This includes bytes
	// read from other levels (BytesIn), as well as bytes read for the level.
	TableBytesRead uint64
	// The number of bytes written to sstables during compactions. The sibling
	// metric for tables is TablesCompacted. This metric may be summed with
	// BytesFlushed to compute the total bytes written for the level.
	TableBytesCompacted uint64
	// The number of bytes written to sstables during flushes. The sibling
	// metrics for tables is TablesFlushed. This metric is always zero for all
	// levels other than L0.
	TableBytesFlushed uint64
	// The number of sstables compacted to this level.
	TablesCompacted uint64
	// The number of sstables flushed to this level.
	TablesFlushed uint64
	// The number of sstables ingested into the level.
	TablesIngested uint64
	// The number of sstables moved to this level by a "move" compaction.
	TablesMoved uint64
	// The number of sstables deleted in a level by a delete-only compaction.
	TablesDeleted uint64
	// The number of sstables excised in a level by a delete-only compaction.
	TablesExcised uint64
	// BlobBytesRead is the volume of physical bytes read from blob files during
	// compactions outputting into this level.
	BlobBytesRead uint64
	// BlobBytesCompacted is the number of bytes written to blob files while
	// compacting sstables in this level.
	BlobBytesCompacted uint64
	// BlobBytesFlushed is the number of bytes written to blob files while
	// flushing sstables. This metric is always zero for all levels other than
	// L0.
	BlobBytesFlushed uint64

	MultiLevel struct {
		// TableBytesInTop are the total bytes in a multilevel compaction coming
		// from the top level.
		TableBytesInTop uint64

		// TableBytesIn, exclusively for multiLevel compactions.
		TableBytesIn uint64

		// TableBytesRead, exclusively for multilevel compactions.
		TableBytesRead uint64
	}

	// Additional contains misc additional metrics that are not always printed.
	Additional struct {
		// The sum of Properties.ValueBlocksSize for all the sstables in this
		// level. Printed by LevelMetrics.format iff there is at least one level
		// with a non-zero value.
		ValueBlocksSize uint64
		// Cumulative metrics about bytes written to data blocks and value blocks,
		// via compactions (except move compactions) or flushes. Not printed by
		// LevelMetrics.format, but are available to sophisticated clients.
		BytesWrittenDataBlocks  uint64
		BytesWrittenValueBlocks uint64
	}
}

// AggregateSize returns an estimated physical size of the level's sstables and
// their referenced values stored in blob files. The size of physical sstables
// is exactly known. Virtual sstables' sizes are estimated, and the size of
// values stored in blob files is estimated based on the volume of referenced
// data and the blob file's compression ratio.
func (m *LevelMetrics) AggregateSize() int64 {
	return m.TablesSize + int64(m.EstimatedReferencesSize)
}

// Add updates the counter metrics for the level.
func (m *LevelMetrics) Add(u *LevelMetrics) {
	m.Sublevels += u.Sublevels
	m.TablesCount += u.TablesCount
	m.TablesSize += u.TablesSize
	m.VirtualTablesCount += u.VirtualTablesCount
	m.VirtualTablesSize += u.VirtualTablesSize
	m.EstimatedReferencesSize += u.EstimatedReferencesSize
	m.TableBytesIn += u.TableBytesIn
	m.TableBytesIngested += u.TableBytesIngested
	m.TableBytesMoved += u.TableBytesMoved
	m.TableBytesRead += u.TableBytesRead
	m.TableBytesCompacted += u.TableBytesCompacted
	m.TableBytesFlushed += u.TableBytesFlushed
	m.TablesCompacted += u.TablesCompacted
	m.TablesFlushed += u.TablesFlushed
	m.TablesIngested += u.TablesIngested
	m.TablesMoved += u.TablesMoved
	m.BlobBytesCompacted += u.BlobBytesCompacted
	m.BlobBytesFlushed += u.BlobBytesFlushed
	m.BlobBytesRead += u.BlobBytesRead
	m.MultiLevel.TableBytesInTop += u.MultiLevel.TableBytesInTop
	m.MultiLevel.TableBytesRead += u.MultiLevel.TableBytesRead
	m.MultiLevel.TableBytesIn += u.MultiLevel.TableBytesIn
	m.Additional.BytesWrittenDataBlocks += u.Additional.BytesWrittenDataBlocks
	m.Additional.BytesWrittenValueBlocks += u.Additional.BytesWrittenValueBlocks
	m.Additional.ValueBlocksSize += u.Additional.ValueBlocksSize
}

// WriteAmp computes the write amplification for compactions at this
// level.
//
// The write amplification is computed as the quantity of physical bytes written
// divided by the quantity of logical bytes written.
//
// Concretely, it's computed as:
//
//	TableBytesFlushed + TableBytesCompacted + BlobBytesFlushed + BlobBytesCompacted
//	-------------------------------------------------------------------------------
//	                              TableBytesIn
func (m *LevelMetrics) WriteAmp() float64 {
	if m.TableBytesIn == 0 {
		return 0
	}
	return float64(m.TableBytesFlushed+m.TableBytesCompacted+m.BlobBytesFlushed+m.BlobBytesCompacted) /
		float64(m.TableBytesIn)
}

var categoryCompaction = block.RegisterCategory("pebble-compaction", block.NonLatencySensitiveQoSLevel)
var categoryIngest = block.RegisterCategory("pebble-ingest", block.LatencySensitiveQoSLevel)
var categoryGet = block.RegisterCategory("pebble-get", block.LatencySensitiveQoSLevel)

// Metrics holds metrics for various subsystems of the DB such as the Cache,
// Compactions, WAL, and per-Level metrics.
//
// TODO(peter): The testing of these metrics is relatively weak. There should
// be testing that performs various operations on a DB and verifies that the
// metrics reflect those operations.
type Metrics struct {
	BlockCache CacheMetrics

	Compact struct {
		// The total number of compactions, and per-compaction type counts.
		Count                 int64
		DefaultCount          int64
		DeleteOnlyCount       int64
		ElisionOnlyCount      int64
		CopyCount             int64
		MoveCount             int64
		ReadCount             int64
		TombstoneDensityCount int64
		RewriteCount          int64
		MultiLevelCount       int64
		BlobFileRewriteCount  int64
		// An estimate of the number of bytes that need to be compacted for the LSM
		// to reach a stable state.
		EstimatedDebt uint64
		// Number of bytes present in sstables being written by in-progress
		// compactions. This value will be zero if there are no in-progress
		// compactions.
		InProgressBytes int64
		// Number of compactions that are in-progress.
		NumInProgress int64
		// Number of compactions that were cancelled.
		CancelledCount int64
		// CancelledBytes the number of bytes written by compactions that were
		// cancelled.
		CancelledBytes int64
		// Total number of compactions that hit an error.
		FailedCount int64
		// NumProblemSpans is the current (instantaneous) count of "problem spans"
		// which temporarily block compactions.
		NumProblemSpans int
		// MarkedFiles is a count of files that are marked for
		// compaction. Such files are compacted in a rewrite compaction
		// when no other compactions are picked.
		MarkedFiles int
		// Duration records the cumulative duration of all compactions since the
		// database was opened.
		Duration time.Duration
	}

	Ingest struct {
		// The total number of ingestions
		Count uint64
		// The number of excise operations during ingestion
		ExciseIngestCount int64
	}

	Flush struct {
		// The total number of flushes.
		Count int64
		// TODO(sumeer): the IdleDuration in this metric is flawed. It only
		// measures idle duration when a flush finishes, representing the idleness
		// before the start of a flush. So computing deltas over this metric over
		// some time interval D may observe the sum of IdleDuration+WorkDuration
		// to be either much smaller or much larger than D.
		WriteThroughput ThroughputMetric
		// Number of flushes that are in-progress. In the current implementation
		// this will always be zero or one.
		NumInProgress int64
		// AsIngestCount is a monotonically increasing counter of flush operations
		// handling ingested tables.
		AsIngestCount uint64
		// AsIngestCount is a monotonically increasing counter of tables ingested as
		// flushables.
		AsIngestTableCount uint64
		// AsIngestBytes is a monotonically increasing counter of the bytes flushed
		// for flushables that originated as ingestion operations.
		AsIngestBytes uint64
	}

	Filter FilterMetrics

	Levels [numLevels]LevelMetrics

	MemTable struct {
		// The number of bytes allocated by memtables and large (flushable)
		// batches.
		Size uint64
		// The count of memtables.
		Count int64
		// The number of bytes present in zombie memtables which are no longer
		// referenced by the current DB state. An unbounded number of memtables
		// may be zombie if they're still in use by an iterator. One additional
		// memtable may be zombie if it's no longer in use and waiting to be
		// recycled.
		ZombieSize uint64
		// The count of zombie memtables.
		ZombieCount int64
	}

	Keys struct {
		// The approximate count of internal range key set keys in the database.
		RangeKeySetsCount uint64
		// The approximate count of internal tombstones (DEL, SINGLEDEL and
		// RANGEDEL key kinds) within the database.
		TombstoneCount uint64
		// A cumulative total number of missized DELSIZED keys encountered by
		// compactions since the database was opened.
		MissizedTombstonesCount uint64
	}

	Snapshots struct {
		// The number of currently open snapshots.
		Count int
		// The sequence number of the earliest, currently open snapshot.
		EarliestSeqNum base.SeqNum
		// A running tally of keys written to sstables during flushes or
		// compactions that would've been elided if it weren't for open
		// snapshots.
		PinnedKeys uint64
		// A running cumulative sum of the size of keys and values written to
		// sstables during flushes or compactions that would've been elided if
		// it weren't for open snapshots.
		PinnedSize uint64
	}

	Table struct {
		// The number of bytes present in obsolete tables which are no longer
		// referenced by the current DB state or any open iterators.
		ObsoleteSize uint64
		// The count of obsolete tables.
		ObsoleteCount int64
		// The number of bytes present in zombie tables which are no longer
		// referenced by the current DB state but are still in use by an iterator.
		ZombieSize uint64
		// The count of zombie tables.
		ZombieCount int64
		// The count of sstables backing virtual tables.
		BackingTableCount uint64
		// The sum of the sizes of the BackingTableCount sstables that are backing virtual tables.
		BackingTableSize uint64
		// Compression statistics for sstable data (does not include blob files).
		Compression CompressionMetrics

		// Local file sizes.
		Local struct {
			// LiveSize is the number of bytes in live tables.
			LiveSize uint64
			// LiveCount is the number of live tables.
			LiveCount uint64
			// ObsoleteSize is the number of bytes in obsolete tables.
			ObsoleteSize uint64
			// ObsoleteCount is the number of obsolete tables.
			ObsoleteCount uint64
			// ZombieSize is the number of bytes in zombie tables.
			ZombieSize uint64
			// ZombieCount is the number of zombie tables.
			ZombieCount uint64
		}

		// Garbage bytes.
		Garbage struct {
			// PointDeletionsBytesEstimate is the estimated file bytes that will be
			// saved by compacting all point deletions. This is dependent on table
			// stats collection, so can be very incomplete until
			// InitialStatsCollectionComplete becomes true.
			PointDeletionsBytesEstimate uint64
			// RangeDeletionsBytesEstimate is the estimated file bytes that will be
			// saved by compacting all range deletions. This is dependent on table
			// stats collection, so can be very incomplete until
			// InitialStatsCollectionComplete becomes true.
			RangeDeletionsBytesEstimate uint64
		}

		// Whether the initial stats collection (for existing tables on Open) is
		// complete.
		InitialStatsCollectionComplete bool
		// The count of recently created sstables that need stats collection. This
		// does not include sstables that existed when the DB was opened, so the
		// value is only useful when InitialStatsCollectionComplete is true.
		PendingStatsCollectionCount int64
	}

	BlobFiles struct {
		// The count of all live blob files.
		LiveCount uint64
		// The physical file size of all live blob files.
		LiveSize uint64
		// ValueSize is the sum of the length of the uncompressed values in all
		// live (referenced by some sstable(s) within the current version) blob
		// files. ValueSize may be greater than LiveSize when compression is
		// effective. ValueSize includes bytes in live blob files that are not
		// actually reachable by any sstable key. If any value within the blob
		// file is reachable by a key in a live sstable, then the entirety of
		// the blob file's values are included within ValueSize.
		ValueSize uint64
		// ReferencedValueSize is the sum of the length of the uncompressed
		// values (in all live blob files) that are still referenced by keys
		// within live tables. Over the lifetime of a blob file, a blob file's
		// references are removed as some compactions choose to write new blob
		// files containing the same values or keys referencing the file's
		// values are deleted. ReferencedValueSize accounts the volume of bytes
		// that are actually reachable by some key in a live table.
		//
		// The difference between ValueSize and ReferencedValueSize is
		// (uncompressed) space amplification that could be reclaimed if all
		// blob files were rewritten, discarding values that are no longer
		// referenced by any keys in any sstables within the current version.
		ReferencedValueSize uint64
		// The count of all obsolete blob files.
		ObsoleteCount uint64
		// The physical size of all obsolete blob files.
		ObsoleteSize uint64
		// The count of all zombie blob files.
		ZombieCount uint64
		// The physical size of all zombie blob files.
		ZombieSize uint64
		// Local file sizes.
		Local struct {
			// LiveSize is the physical size of local live blob files.
			LiveSize uint64
			// LiveCount is the number of local live blob files.
			LiveCount uint64
			// ObsoleteSize is the physical size of local obsolete blob files.
			ObsoleteSize uint64
			// ObsoleteCount is the number of local obsolete blob files.
			ObsoleteCount uint64
			// ZombieSize is the physical size of local zombie blob files.
			ZombieSize uint64
			// ZombieCount is the number of local zombie blob files.
			ZombieCount uint64
		}

		// TODO(radu): add compression stats.
	}

	FileCache FileCacheMetrics

	// Count of the number of open sstable iterators.
	TableIters int64
	// Uptime is the total time since this DB was opened.
	Uptime time.Duration

	WAL struct {
		// Number of live WAL files.
		Files int64
		// Number of obsolete WAL files.
		ObsoleteFiles int64
		// Physical size of the obsolete WAL files.
		ObsoletePhysicalSize uint64
		// Size of the live data in the WAL files. Note that with WAL file
		// recycling this is less than the actual on-disk size of the WAL files.
		Size uint64
		// Physical size of the WAL files on-disk. With WAL file recycling,
		// this is greater than the live data in WAL files.
		//
		// TODO(sumeer): it seems this does not include ObsoletePhysicalSize.
		// Should the comment be updated?
		PhysicalSize uint64
		// Number of logical bytes written to the WAL.
		BytesIn uint64
		// Number of bytes written to the WAL.
		BytesWritten uint64
		// Failover contains failover stats. Empty if failover is not enabled.
		Failover wal.FailoverStats
	}

	LogWriter struct {
		FsyncLatency prometheus.Histogram
		record.LogWriterMetrics
	}

	CategoryStats []block.CategoryStatsAggregate

	SecondaryCacheMetrics SecondaryCacheMetrics

	private struct {
		optionsFileSize  uint64
		manifestFileSize uint64
	}

	manualMemory manual.Metrics
}

// CompressionMetrics contains compression metrics for sstables or blob files.
type CompressionMetrics struct {
	// NoCompressionBytes is the total number of bytes in files that do are not
	// compressed. Data can be uncompressed when 1) compression is disabled; 2)
	// for certain special types of blocks; and 3) for blocks that are not
	// compressible.
	NoCompressionBytes uint64
	// CompressedBytesWithoutStats is the total number of bytes in files that do
	// not encode compression statistics (or for which there are no statistics
	// yet).
	CompressedBytesWithoutStats uint64
	Snappy                      CompressionStatsForSetting
	MinLZ                       CompressionStatsForSetting
	Zstd                        CompressionStatsForSetting
}

type CompressionStatsForSetting = block.CompressionStatsForSetting

func (cm *CompressionMetrics) Add(stats *block.CompressionStats) {
	for s, cs := range stats.All() {
		switch s.Algorithm {
		case compression.NoCompression:
			cm.NoCompressionBytes += cs.UncompressedBytes
		case compression.SnappyAlgorithm:
			cm.Snappy.Add(cs)
		case compression.MinLZ:
			cm.MinLZ.Add(cs)
		case compression.Zstd:
			cm.Zstd.Add(cs)
		}
	}
}

func (cm *CompressionMetrics) MergeWith(o *CompressionMetrics) {
	cm.NoCompressionBytes += o.NoCompressionBytes
	cm.CompressedBytesWithoutStats += o.CompressedBytesWithoutStats
	cm.Snappy.Add(o.Snappy)
	cm.MinLZ.Add(o.MinLZ)
	cm.Zstd.Add(o.Zstd)
}

var (
	// FsyncLatencyBuckets are prometheus histogram buckets suitable for a histogram
	// that records latencies for fsyncs.
	FsyncLatencyBuckets = append(
		prometheus.LinearBuckets(0.0, float64(time.Microsecond*100), 50),
		prometheus.ExponentialBucketsRange(float64(time.Millisecond*5), float64(10*time.Second), 50)...,
	)

	// SecondaryCacheIOBuckets exported to enable exporting from package pebble to
	// enable exporting metrics with below buckets in CRDB.
	SecondaryCacheIOBuckets = sharedcache.IOBuckets
	// SecondaryCacheChannelWriteBuckets exported to enable exporting from package
	// pebble to enable exporting metrics with below buckets in CRDB.
	SecondaryCacheChannelWriteBuckets = sharedcache.ChannelWriteBuckets
)

// DiskSpaceUsage returns the total disk space used by the database in bytes,
// including live and obsolete files. This only includes local files, i.e.,
// remote files (as known to objstorage.Provider) are not included.
func (m *Metrics) DiskSpaceUsage() uint64 {
	var usageBytes uint64
	usageBytes += m.WAL.PhysicalSize
	usageBytes += m.WAL.ObsoletePhysicalSize
	usageBytes += m.Table.Local.LiveSize
	usageBytes += m.Table.Local.ObsoleteSize
	usageBytes += m.Table.Local.ZombieSize
	usageBytes += m.BlobFiles.Local.LiveSize
	usageBytes += m.BlobFiles.Local.ObsoleteSize
	usageBytes += m.BlobFiles.Local.ZombieSize
	usageBytes += m.private.optionsFileSize
	usageBytes += m.private.manifestFileSize
	// TODO(sumeer): InProgressBytes does not distinguish between local and
	// remote files. This causes a small error. Fix.
	usageBytes += uint64(m.Compact.InProgressBytes)
	return usageBytes
}

// NumVirtual is the number of virtual sstables in the latest version
// summed over every level in the lsm.
func (m *Metrics) NumVirtual() uint64 {
	var n uint64
	for _, level := range m.Levels {
		n += level.VirtualTablesCount
	}
	return n
}

// VirtualSize is the sum of the sizes of the virtual sstables in the
// latest version. BackingTableSize - VirtualSize gives an estimate for
// the space amplification caused by not compacting virtual sstables.
func (m *Metrics) VirtualSize() uint64 {
	var size uint64
	for _, level := range m.Levels {
		size += level.VirtualTablesSize
	}
	return size
}

// ReadAmp returns the current read amplification of the database.
// It's computed as the number of sublevels in L0 + the number of non-empty
// levels below L0.
func (m *Metrics) ReadAmp() int {
	var ramp int32
	for _, l := range m.Levels {
		ramp += l.Sublevels
	}
	return int(ramp)
}

// Total returns the sum of the per-level metrics and WAL metrics.
func (m *Metrics) Total() LevelMetrics {
	var total LevelMetrics
	for level := 0; level < numLevels; level++ {
		l := &m.Levels[level]
		total.Add(l)
	}
	// Compute total bytes-in as the bytes written to the WAL + bytes ingested.
	total.TableBytesIn = m.WAL.BytesWritten + total.TableBytesIngested
	// Add the total bytes-in to the total bytes-flushed. This is to account for
	// the bytes written to the log and bytes written externally and then
	// ingested.
	total.TableBytesFlushed += total.TableBytesIn
	return total
}

// RemoteTablesTotal returns the total number of remote tables and their total
// size. Remote tables are computed as the difference between total tables
// (live + obsolete + zombie) and local tables.
func (m *Metrics) RemoteTablesTotal() (count uint64, size uint64) {
	var liveTables, liveTableBytes int64
	for level := 0; level < numLevels; level++ {
		liveTables += m.Levels[level].TablesCount
		liveTableBytes += m.Levels[level].TablesSize
	}
	totalCount := liveTables + m.Table.ObsoleteCount + m.Table.ZombieCount
	localCount := m.Table.Local.LiveCount + m.Table.Local.ObsoleteCount + m.Table.Local.ZombieCount
	remoteCount := uint64(totalCount) - localCount

	totalSize := uint64(liveTableBytes) + m.Table.ObsoleteSize + m.Table.ZombieSize
	localSize := m.Table.Local.LiveSize + m.Table.Local.ObsoleteSize + m.Table.Local.ZombieSize
	remoteSize := totalSize - localSize

	return remoteCount, remoteSize
}

// Assert that Metrics implements redact.SafeFormatter.
var _ redact.SafeFormatter = (*Metrics)(nil)

// SafeFormat implements redact.SafeFormatter.
func (m *Metrics) SafeFormat(w redact.SafePrinter, _ rune) {
	w.SafeString(redact.SafeString(m.String()))
}

var (
	levelMetricsTableTopHeader = `LSM                             |    vtables   |   value sep   |        |   ingested   |    amp`
	levelMetricsTable          = table.Define[*LevelMetrics](
		table.AutoIncrement[*LevelMetrics]("level", 5, table.AlignRight),
		table.Bytes("size", 10, table.AlignRight, func(m *LevelMetrics) uint64 { return uint64(m.TablesSize) + m.EstimatedReferencesSize }),
		table.Div(),
		table.Count("tables", 6, table.AlignRight, func(m *LevelMetrics) int64 { return m.TablesCount }),
		table.Bytes("size", 5, table.AlignRight, func(m *LevelMetrics) int64 { return m.TablesSize }),
		table.Div(),
		table.Count("count", 6, table.AlignRight, func(m *LevelMetrics) uint64 { return m.VirtualTablesCount }),
		table.Count("size", 5, table.AlignRight, func(m *LevelMetrics) uint64 { return m.VirtualTablesSize }),
		table.Div(),
		table.Bytes("refsz", 6, table.AlignRight, func(m *LevelMetrics) uint64 { return m.EstimatedReferencesSize }),
		table.Bytes("valblk", 6, table.AlignRight, func(m *LevelMetrics) uint64 { return m.Additional.ValueBlocksSize }),
		table.Div(),
		table.Bytes("in", 6, table.AlignRight, func(m *LevelMetrics) uint64 { return m.TableBytesIn }),
		table.Div(),
		table.Count("tables", 6, table.AlignRight, func(m *LevelMetrics) uint64 { return m.TablesIngested }),
		table.Bytes("size", 5, table.AlignRight, func(m *LevelMetrics) uint64 { return m.TableBytesIngested }),
		table.Div(),
		table.Int("r", 3, table.AlignRight, func(m *LevelMetrics) int { return int(m.Sublevels) }),
		table.Float("w", 5, table.AlignRight, func(m *LevelMetrics) float64 { return m.WriteAmp() }),
	)
	levelCompactionMetricsTableTopHeader = `COMPACTIONS               |     moved    |     multilevel    |     read     |       written`
	compactionLevelMetricsTable          = table.Define[*LevelMetrics](
		table.AutoIncrement[*LevelMetrics]("level", 5, table.AlignRight),
		table.Div(),
		table.Float("score", 5, table.AlignRight, func(m *LevelMetrics) float64 { return m.Score }),
		table.Float("ff", 5, table.AlignRight, func(m *LevelMetrics) float64 { return m.FillFactor }),
		table.Float("cff", 5, table.AlignRight, func(m *LevelMetrics) float64 { return m.CompensatedFillFactor }),
		table.Div(),
		table.Count("tables", 6, table.AlignRight, func(m *LevelMetrics) uint64 { return m.TablesMoved }),
		table.Bytes("size", 5, table.AlignRight, func(m *LevelMetrics) uint64 { return m.TableBytesMoved }),
		table.Div(),
		table.Bytes("top", 5, table.AlignRight, func(m *LevelMetrics) uint64 { return m.MultiLevel.TableBytesInTop }),
		table.Bytes("in", 5, table.AlignRight, func(m *LevelMetrics) uint64 { return m.MultiLevel.TableBytesIn }),
		table.Bytes("read", 5, table.AlignRight, func(m *LevelMetrics) uint64 { return m.MultiLevel.TableBytesRead }),
		table.Div(),
		table.Bytes("tables", 6, table.AlignRight, func(m *LevelMetrics) uint64 { return m.TableBytesRead }),
		table.Bytes("blob", 5, table.AlignRight, func(m *LevelMetrics) uint64 { return m.BlobBytesRead }),
		table.Div(),
		table.Count("tables", 6, table.AlignRight, func(m *LevelMetrics) uint64 { return m.TablesFlushed + m.TablesCompacted }),
		table.Bytes("sstsz", 6, table.AlignRight, func(m *LevelMetrics) uint64 { return m.TableBytesFlushed + m.TableBytesCompacted }),
		table.Bytes("blobsz", 6, table.AlignRight, func(m *LevelMetrics) uint64 { return m.BlobBytesFlushed + m.BlobBytesCompacted }),
	)
	compactionKindTable = table.Define[pair[string, int64]](
		table.String("kind", 5, table.AlignRight, func(p pair[string, int64]) string { return p.k }),
		table.Count("count", 5, table.AlignRight, func(p pair[string, int64]) int64 { return p.v }),
	)
	commitPipelineInfoTableTopHeader = `COMMIT PIPELINE`
	commitPipelineInfoTableSubHeader = `               wals                |              memtables              |       ingestions`
	commitPipelineInfoTable          = table.Define[commitPipelineInfo](
		table.String("files", 9, table.AlignRight, func(i commitPipelineInfo) string { return i.files }),
		table.Div(),
		table.String("written", 10, table.AlignRight, func(i commitPipelineInfo) string { return i.written }),
		table.Div(),
		table.String("overhead", 9, table.AlignRight, func(i commitPipelineInfo) string { return i.overhead }),
		table.Div(),
		table.String("flushes", 9, table.AlignRight, func(i commitPipelineInfo) string { return i.flushes }),
		table.Div(),
		table.String("live", 10, table.AlignRight, func(i commitPipelineInfo) string { return i.live }),
		table.Div(),
		table.String("zombie", 10, table.AlignRight, func(i commitPipelineInfo) string { return i.zombie }),
		table.Div(),
		table.String("total", 9, table.AlignRight, func(i commitPipelineInfo) string { return i.total }),
		table.Div(),
		table.String("flushable", 11, table.AlignRight, func(i commitPipelineInfo) string { return i.flushable }),
	)
	iteratorInfoTableTopHeader = `ITERATORS`
	iteratorInfoTableSubHeader = `        block cache        |         file cache         |    filter    |  sst iters  |  snapshots`
	iteratorInfoTable          = table.Define[iteratorInfo](
		table.String("entries", 12, table.AlignRight, func(i iteratorInfo) string { return i.bcEntries }),
		table.Div(),
		table.String("hit rate", 11, table.AlignRight, func(i iteratorInfo) string { return i.bcHitRate }),
		table.Div(),
		table.String("entries", 12, table.AlignRight, func(i iteratorInfo) string { return i.fcEntries }),
		table.Div(),
		table.String("hit rate", 11, table.AlignRight, func(i iteratorInfo) string { return i.fcHitRate }),
		table.Div(),
		table.String("util", 12, table.AlignRight, func(i iteratorInfo) string { return i.bloomFilterUtil }),
		table.Div(),
		table.String("open", 11, table.AlignRight, func(i iteratorInfo) string { return i.sstableItersOpen }),
		table.Div(),
		table.String("open", 11, table.AlignRight, func(i iteratorInfo) string { return i.snapshotsOpen }),
	)
	fileInfoTableHeader = `FILES                 tables                       |       blob files        |     blob values`
	fileInfoTable       = table.Define[tableAndBlobInfo](
		table.String("stats prog", 13, table.AlignRight, func(i tableAndBlobInfo) string { return i.tableInfo.stats }),
		table.Div(),
		table.String("backing", 10, table.AlignRight, func(i tableAndBlobInfo) string { return i.tableInfo.backing }),
		table.Div(),
		table.String("zombie", 21, table.AlignRight, func(i tableAndBlobInfo) string { return i.tableInfo.zombie }),
		table.Div(),
		table.String("live", 10, table.AlignRight, func(i tableAndBlobInfo) string { return i.blobInfo.live }),
		table.Div(),
		table.String("zombie", 10, table.AlignRight, func(i tableAndBlobInfo) string { return i.blobInfo.zombie }),
		table.Div(),
		table.String("total", 6, table.AlignRight, func(i tableAndBlobInfo) string { return i.blobInfo.total }),
		table.Div(),
		table.String("refed", 10, table.AlignRight, func(i tableAndBlobInfo) string { return i.blobInfo.referenced }),
	)
	cgoMemInfoTableHeader = `CGO MEMORY    |          block cache           |                     memtables`
	cgoMemInfoTable       = table.Define[cgoMemInfo](
		table.String("tot", 13, table.AlignRight, func(i cgoMemInfo) string { return i.tot }),
		table.Div(),
		table.String("tot", 13, table.AlignRight, func(i cgoMemInfo) string { return i.bcTot }),
		table.Div(),
		table.String("data", 14, table.AlignRight, func(i cgoMemInfo) string { return i.bcData }),
		table.Div(),
		table.String("maps", 15, table.AlignRight, func(i cgoMemInfo) string { return i.bcMaps }),
		table.Div(),
		table.String("ents", 15, table.AlignRight, func(i cgoMemInfo) string { return i.bcEnts }),
		table.Div(),
		table.String("tot", 13, table.AlignRight, func(i cgoMemInfo) string { return i.memtablesTot }),
	)
	compactionInfoTableTopHeader = `COMPACTIONS`
	compactionInfoTable          = table.Define[compactionMetricsInfo](
		table.String("estimated debt", 17, table.AlignRight, func(i compactionMetricsInfo) string { return i.estimatedDebt }),
		table.Div(),
		table.String("in progress", 17, table.AlignRight, func(i compactionMetricsInfo) string { return i.inProgress }),
		table.Div(),
		table.String("cancelled", 17, table.AlignRight, func(i compactionMetricsInfo) string { return i.cancelled }),
		table.Div(),
		table.String("failed", 17, table.AlignRight, func(i compactionMetricsInfo) string { return fmt.Sprint(i.failed) }),
		table.Div(),
		table.String("problem spans", 18, table.AlignRight, func(i compactionMetricsInfo) string { return i.problemSpans }),
	)
	keysInfoTableTopHeader = `KEYS`
	keysInfoTable          = table.Define[keysInfo](
		table.String("range keys", 16, table.AlignRight, func(i keysInfo) string { return i.rangeKeys }),
		table.Div(),
		table.String("tombstones", 16, table.AlignRight, func(i keysInfo) string { return i.tombstones }),
		table.Div(),
		table.String("missized tombstones", 24, table.AlignRight, func(i keysInfo) string { return i.missizedTombstones }),
		table.Div(),
		table.String("point dels", 15, table.AlignRight, func(i keysInfo) string { return i.pointDels }),
		table.Div(),
		table.String("range dels", 15, table.AlignRight, func(i keysInfo) string { return i.rangeDels }),
	)
	compressionTableHeader = `COMPRESSION`
	compressionTable       = table.Define[pair[string, CompressionStatsForSetting]](
		table.String("algorithm", 13, table.AlignRight, func(p pair[string, CompressionStatsForSetting]) string { return p.k }),
		table.Bytes("on disk bytes", 13, table.AlignRight, func(p pair[string, CompressionStatsForSetting]) uint64 { return p.v.CompressedBytes }),
		table.String("CR", 13, table.AlignRight, func(p pair[string, CompressionStatsForSetting]) string {
			if p.v.UncompressedBytes == p.v.CompressedBytes {
				return ""
			}
			if p.v.UncompressedBytes == 0 {
				return "?"
			}
			return crhumanize.Float(p.v.CompressionRatio(), 2 /* precision */).String()
		}),
	)
)

type commitPipelineInfo struct {
	files     string
	written   string
	overhead  string
	flushes   string
	live      string
	zombie    string
	total     string
	flushable string
}

type iteratorInfo struct {
	bcEntries        string
	bcHitRate        string
	fcEntries        string
	fcHitRate        string
	bloomFilterUtil  string
	sstableItersOpen string
	snapshotsOpen    string
}
type tableInfo struct {
	stats   string
	backing string
	zombie  string
}

type blobInfo struct {
	live       string
	zombie     string
	total      string
	referenced string
}

type tableAndBlobInfo struct {
	tableInfo tableInfo
	blobInfo  blobInfo
}

type cgoMemInfo struct {
	tot          string
	bcTot        string
	bcData       string
	bcMaps       string
	bcEnts       string
	memtablesTot string
}

type compactionMetricsInfo struct {
	estimatedDebt string
	inProgress    string
	cancelled     string
	failed        int64
	problemSpans  string
}

type keysInfo struct {
	rangeKeys          string
	tombstones         string
	missizedTombstones string
	pointDels          string
	rangeDels          string
}

type pair[k, v any] struct {
	k k
	v v
}

// String pretty-prints the metrics.
//
// See testdata/metrics for an example.
func (m *Metrics) String() string {
	wb := ascii.Make(128 /* width */, 80 /* height */)
	var total LevelMetrics
	for l := range numLevels {
		total.Add(&m.Levels[l])
	}
	// Compute total bytes-in as the bytes written to the WAL + bytes ingested.
	total.TableBytesIn = m.WAL.BytesWritten + total.TableBytesIngested
	// Add the total bytes-in to the total bytes-flushed. This is to account for
	// the bytes written to the log and bytes written externally and then
	// ingested.
	total.TableBytesFlushed += total.TableBytesIn
	total.Score = math.NaN()
	total.FillFactor = math.NaN()
	total.CompensatedFillFactor = math.NaN()

	// LSM level metrics.
	cur := wb.At(0, 0)
	cur = cur.WriteString(levelMetricsTableTopHeader).NewlineReturn()
	cur = levelMetricsTable.Render(cur, table.RenderOptions{}, m.LevelMetricsIter())
	cur.Offset(-1, 0).WriteString("total")
	cur = cur.NewlineReturn()

	// Compaction level metrics.
	cur = cur.WriteString(levelCompactionMetricsTableTopHeader).NewlineReturn()
	cur = compactionLevelMetricsTable.Render(cur, table.RenderOptions{}, m.LevelMetricsIter())
	cur.Offset(-1, 0).WriteString("total")

	cur = cur.NewlineReturn()
	compactionKindContents := []pair[string, int64]{
		{k: "default", v: m.Compact.DefaultCount},
		{k: "delete", v: m.Compact.DeleteOnlyCount},
		{k: "elision", v: m.Compact.ElisionOnlyCount},
		{k: "move", v: m.Compact.MoveCount},
		{k: "read", v: m.Compact.ReadCount},
		{k: "tomb", v: m.Compact.TombstoneDensityCount},
		{k: "rewrite", v: m.Compact.RewriteCount},
		{k: "copy", v: m.Compact.CopyCount},
		{k: "multi", v: m.Compact.MultiLevelCount},
		{k: "blob", v: m.Compact.BlobFileRewriteCount},
	}
	cur = compactionKindTable.Render(cur, table.RenderOptions{Orientation: table.Horizontally}, slices.Values(compactionKindContents))
	cur = cur.NewlineReturn()

	commitPipelineInfoContents := commitPipelineInfo{
		// wals.
		files:    fmt.Sprintf("%s (%s)", humanizeCount(m.WAL.Files), humanizeBytes(m.WAL.Size)),
		written:  fmt.Sprintf("%s: %s", humanizeBytes(m.WAL.BytesIn), humanizeBytes(m.WAL.BytesWritten)),
		overhead: fmt.Sprintf("%.1f%%", percent(int64(m.WAL.BytesWritten)-int64(m.WAL.BytesIn), int64(m.WAL.BytesIn))),
		// memtables.
		flushes: crhumanize.Count(m.Flush.Count).String(),
		live:    fmt.Sprintf("%s (%s)", humanizeCount(m.MemTable.Count), humanizeBytes(m.MemTable.Size)),
		zombie:  fmt.Sprintf("%s (%s)", humanizeCount(m.MemTable.ZombieCount), humanizeBytes(m.MemTable.ZombieSize)),
		// ingestions.
		total:     crhumanize.Count(m.WAL.BytesIn + m.WAL.BytesWritten).String(),
		flushable: fmt.Sprintf("%s (%s)", humanizeCount(m.Flush.AsIngestCount), humanizeBytes(m.Flush.AsIngestBytes)),
	}
	cur = cur.WriteString(commitPipelineInfoTableTopHeader).NewlineReturn()
	cur = cur.WriteString(commitPipelineInfoTableSubHeader).NewlineReturn()
	cur = commitPipelineInfoTable.Render(cur, table.RenderOptions{}, oneItemIter(commitPipelineInfoContents))
	cur = cur.NewlineReturn()

	iteratorInfoContents := iteratorInfo{
		bcEntries:        fmt.Sprintf("%s (%s)", humanizeCount(m.BlockCache.Count), humanizeBytes(m.BlockCache.Size)),
		bcHitRate:        fmt.Sprintf("%.1f%%", hitRate(m.BlockCache.Hits, m.BlockCache.Misses)),
		fcEntries:        fmt.Sprintf("%s (%s)", humanizeCount(m.FileCache.TableCount), humanizeBytes(m.FileCache.Size)),
		fcHitRate:        fmt.Sprintf("%.1f%%", hitRate(m.FileCache.Hits, m.FileCache.Misses)),
		bloomFilterUtil:  fmt.Sprintf("%.1f%%", hitRate(m.Filter.Hits, m.Filter.Misses)),
		sstableItersOpen: humanizeCount(m.TableIters).String(),
		snapshotsOpen:    humanizeCount(m.Snapshots.Count).String(),
	}
	cur = cur.WriteString(iteratorInfoTableTopHeader).NewlineReturn()
	cur = cur.WriteString(iteratorInfoTableSubHeader).NewlineReturn()
	cur = iteratorInfoTable.Render(cur, table.RenderOptions{}, oneItemIter(iteratorInfoContents))
	cur = cur.NewlineReturn()

	status := fmt.Sprintf("%s pending", humanizeCount(m.Table.PendingStatsCollectionCount))
	if !m.Table.InitialStatsCollectionComplete {
		status = "loading"
	} else if m.Table.PendingStatsCollectionCount == 0 {
		status = "all loaded"
	}
	tableInfoContents := tableInfo{
		stats:   status,
		backing: fmt.Sprintf("%s (%s)", humanizeCount(m.Table.BackingTableCount), humanizeBytes(m.Table.BackingTableSize)),
		zombie:  fmt.Sprintf("%s (%s local:%s)", humanizeCount(m.Table.ZombieCount), humanizeBytes(m.Table.ZombieSize), humanizeBytes(m.Table.Local.ZombieSize)),
	}
	blobInfoContents := blobInfo{
		live:       fmt.Sprintf("%s (%s)", humanizeCount(m.BlobFiles.LiveCount), humanizeBytes(m.BlobFiles.LiveSize)),
		zombie:     fmt.Sprintf("%s (%s)", humanizeCount(m.BlobFiles.ZombieCount), humanizeBytes(m.BlobFiles.ZombieSize)),
		total:      humanizeBytes(m.BlobFiles.ValueSize).String(),
		referenced: fmt.Sprintf("%.0f%% (%s)", percent(m.BlobFiles.ReferencedValueSize, m.BlobFiles.ValueSize), humanizeBytes(m.BlobFiles.ReferencedValueSize)),
	}
	fileInfoContents := tableAndBlobInfo{
		tableInfo: tableInfoContents,
		blobInfo:  blobInfoContents,
	}
	cur = cur.WriteString(fileInfoTableHeader).NewlineReturn()
	cur = fileInfoTable.Render(cur, table.RenderOptions{}, oneItemIter(fileInfoContents))
	cur = cur.NewlineReturn()

	var inUseTotal uint64
	for i := range m.manualMemory {
		inUseTotal += m.manualMemory[i].InUseBytes
	}
	inUse := func(purpose manual.Purpose) uint64 {
		return m.manualMemory[purpose].InUseBytes
	}
	cgoMemInfoContents := cgoMemInfo{
		tot: humanizeBytes(inUseTotal).String(),
		bcTot: humanizeBytes(inUse(manual.BlockCacheData) +
			inUse(manual.BlockCacheMap) + inUse(manual.BlockCacheEntry)).String(),
		bcData:       humanizeBytes(inUse(manual.BlockCacheData)).String(),
		bcMaps:       humanizeBytes(inUse(manual.BlockCacheMap)).String(),
		bcEnts:       humanizeBytes(inUse(manual.BlockCacheEntry)).String(),
		memtablesTot: humanizeBytes(inUse(manual.MemTable)).String(),
	}
	cur = cur.WriteString(cgoMemInfoTableHeader).NewlineReturn()
	cur = cgoMemInfoTable.Render(cur, table.RenderOptions{}, oneItemIter(cgoMemInfoContents))
	cur = cur.NewlineReturn()

	compactionMetricsInfoContents := compactionMetricsInfo{
		estimatedDebt: humanizeBytes(m.Compact.EstimatedDebt).String(),
		inProgress: fmt.Sprintf("%s (%s)", humanizeCount(m.Compact.NumInProgress),
			humanizeBytes(m.Compact.InProgressBytes)),
		cancelled: fmt.Sprintf("%s (%s)", humanizeCount(m.Compact.CancelledCount),
			humanizeBytes(m.Compact.CancelledBytes)),
		failed:       m.Compact.FailedCount,
		problemSpans: fmt.Sprintf("%d%s", m.Compact.NumProblemSpans, ifNonZero(m.Compact.NumProblemSpans, "!!")),
	}
	cur = cur.WriteString(compactionInfoTableTopHeader).NewlineReturn()
	cur = compactionInfoTable.Render(cur, table.RenderOptions{}, oneItemIter(compactionMetricsInfoContents))
	cur = cur.NewlineReturn()

	keysInfoContents := keysInfo{
		rangeKeys:          humanizeCount(m.Keys.RangeKeySetsCount).String(),
		tombstones:         humanizeCount(m.Keys.TombstoneCount).String(),
		missizedTombstones: fmt.Sprintf("%d%s", m.Keys.MissizedTombstonesCount, ifNonZero(m.Keys.MissizedTombstonesCount, "!!")),
		pointDels:          humanizeBytes(m.Table.Garbage.PointDeletionsBytesEstimate).String(),
		rangeDels:          humanizeBytes(m.Table.Garbage.RangeDeletionsBytesEstimate).String(),
	}
	cur = cur.WriteString(keysInfoTableTopHeader).NewlineReturn()
	cur = keysInfoTable.Render(cur, table.RenderOptions{}, oneItemIter(keysInfoContents))
	cur = cur.NewlineReturn()

	cur = cur.WriteString(compressionTableHeader).NewlineReturn()
	compressionContents := []pair[string, CompressionStatsForSetting]{
		{k: "none", v: CompressionStatsForSetting{
			CompressedBytes:   m.Table.Compression.NoCompressionBytes,
			UncompressedBytes: m.Table.Compression.NoCompressionBytes,
		}},
		{k: "snappy", v: m.Table.Compression.Snappy},
		{k: "minlz", v: m.Table.Compression.MinLZ},
		{k: "zstd", v: m.Table.Compression.Zstd},
		{k: "unknown", v: CompressionStatsForSetting{CompressedBytes: m.Table.Compression.CompressedBytesWithoutStats}},
	}
	compressionContents = slices.DeleteFunc(compressionContents, func(p pair[string, CompressionStatsForSetting]) bool {
		return p.v.CompressedBytes == 0
	})
	compressionTable.Render(cur, table.RenderOptions{Orientation: table.Horizontally}, slices.Values(compressionContents))

	return wb.String()
}

func ifNonZero[T constraints.Integer](v T, s string) string {
	if v > 0 {
		return s
	}
	return ""
}

func hitRate(hits, misses int64) float64 {
	return percent(hits, hits+misses)
}

func percent[T constraints.Integer](numerator, denominator T) float64 {
	if denominator == 0 {
		return 0
	}
	return 100 * float64(numerator) / float64(denominator)
}

// StringForTests is identical to m.String() on 64-bit platforms. It is used to
// provide a platform-independent result for tests.
func (m *Metrics) StringForTests() string {
	mCopy := *m

	// We recalculate the file cache size using the 64-bit sizes, and we ignore
	// the genericcache metadata size which is harder to adjust.
	const sstableReaderSize64bit = 280
	const blobFileReaderSize64bit = 96
	mCopy.FileCache.Size = mCopy.FileCache.TableCount*sstableReaderSize64bit + mCopy.FileCache.BlobFileCount*blobFileReaderSize64bit
	if math.MaxInt == math.MaxInt64 {
		// Verify the 64-bit sizes, so they are kept updated.
		if sstableReaderSize64bit != unsafe.Sizeof(sstable.Reader{}) {
			panic(fmt.Sprintf("sstableReaderSize64bit should be updated to %d", unsafe.Sizeof(sstable.Reader{})))
		}
		if blobFileReaderSize64bit != unsafe.Sizeof(blob.FileReader{}) {
			panic(fmt.Sprintf("blobFileReaderSize64bit should be updated to %d", unsafe.Sizeof(blob.FileReader{})))
		}
	}
	// Don't show cgo memory statistics as they can vary based on architecture,
	// invariants tag, etc.
	mCopy.manualMemory = manual.Metrics{}
	return redact.StringWithoutMarkers(&mCopy)
}

// LevelMetricsIter returns an iterator over all level metrics - including the
// total for all levels.
func (m *Metrics) LevelMetricsIter() iter.Seq[*LevelMetrics] {
	return func(yield func(*LevelMetrics) bool) {
		for i := range m.Levels {
			lvlMetric := m.Levels[i]
			if lvlMetric.Score == 0 {
				lvlMetric.Score = math.NaN()
			}
			if !yield(&lvlMetric) {
				break
			}
		}
		t := m.Total()
		t.Score, t.FillFactor, t.CompensatedFillFactor = math.NaN(), math.NaN(), math.NaN()
		yield(&t)
	}
}

// levelMetricsDelta accumulates incremental ("delta") level metric updates
// (e.g. from compactions or flushes).
type levelMetricsDelta [manifest.NumLevels]*LevelMetrics

func (m *levelMetricsDelta) level(level int) *LevelMetrics {
	if m[level] == nil {
		m[level] = &LevelMetrics{}
	}
	return m[level]
}

func (m *Metrics) updateLevelMetrics(updates levelMetricsDelta) {
	for i, u := range updates {
		if u != nil {
			m.Levels[i].Add(u)
		}
	}
}

func humanizeCount[T crhumanize.Integer](value T) crhumanize.SafeString {
	return crhumanize.Count(value, crhumanize.Compact, crhumanize.OmitI)
}

func humanizeBytes[T crhumanize.Integer](value T) crhumanize.SafeString {
	return crhumanize.Bytes(value, crhumanize.Compact, crhumanize.OmitI)
}

func oneItemIter[T any](v T) iter.Seq[T] {
	return func(yield func(T) bool) {
		yield(v)
	}
}
