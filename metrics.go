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
	"github.com/cockroachdb/pebble/internal/deletepacer"
	"github.com/cockroachdb/pebble/internal/manifest"
	"github.com/cockroachdb/pebble/internal/manual"
	"github.com/cockroachdb/pebble/metrics"
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

// AllLevelMetrics contains LevelMetrics for each level.
type AllLevelMetrics [manifest.NumLevels]LevelMetrics

// LevelMetrics holds per-level metrics such as the number of files and total
// size of the files, and compaction related metrics.
type LevelMetrics struct {
	// The number of sublevels within the level. The sublevel count corresponds
	// to the read amplification for the level. An empty level will have a
	// sublevel count of 0, implying no read amplification. Only L0 will have
	// a sublevel count other than 0 or 1.
	Sublevels int32

	// The total count and size of sstables in the level. The size does not
	// include the size of the blob files or the referenced values.
	Tables metrics.CountAndSize

	// The total count and total estimated size of virtual sstables in the level.
	// The size does not include the size of the blob files or the referenced
	// values.
	VirtualTables metrics.CountAndSize

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

	// The count and total size of sstable tables ingested into this level.
	TablesIngested metrics.CountAndSize
	// The count and total size of sstable tables compacted to this level.
	TablesCompacted metrics.CountAndSize
	// The count and total size of sstables flushed to this level.
	TablesFlushed metrics.CountAndSize
	// The count and total size of sstables moved to this level by a "move"
	// compaction.
	TablesMoved metrics.CountAndSize

	// The number of incoming bytes from other levels' sstables read during
	// compactions. This excludes bytes moved and bytes ingested. For L0 this is
	// the bytes written to the WAL.
	TableBytesIn uint64
	// The number of bytes read for compactions at the level. This includes bytes
	// read from other levels (BytesIn), as well as bytes read for the level.
	TableBytesRead uint64
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
		// The sum of FileProperties.ValueBlocksSize for all the sstables in this
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
func (m *LevelMetrics) AggregateSize() uint64 {
	return m.Tables.Bytes + m.EstimatedReferencesSize
}

// Add updates the counter metrics for the level.
func (m *LevelMetrics) Add(u *LevelMetrics) {
	m.Sublevels += u.Sublevels
	m.Tables.Accumulate(u.Tables)
	m.VirtualTables.Accumulate(u.VirtualTables)
	m.EstimatedReferencesSize += u.EstimatedReferencesSize
	m.TablesIngested.Accumulate(u.TablesIngested)
	m.TablesCompacted.Accumulate(u.TablesCompacted)
	m.TablesFlushed.Accumulate(u.TablesFlushed)
	m.TablesMoved.Accumulate(u.TablesMoved)
	m.TableBytesIn += u.TableBytesIn
	m.TableBytesRead += u.TableBytesRead
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
	return float64(m.TablesFlushed.Bytes+m.TablesCompacted.Bytes+m.BlobBytesFlushed+m.BlobBytesCompacted) /
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
	Compact    CompactMetrics
	Ingest     IngestMetrics
	Flush      FlushMetrics
	Filter     FilterMetrics
	Levels     AllLevelMetrics

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

	Keys KeysMetrics

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
		// Physical contains space usage metrics for physical tables and backings
		// for virtual tables. These metrics do not include blob reference sizes.
		Physical struct {
			// The counts and total sizes of live tables.
			Live metrics.CountAndSizeByPlacement
			// The counts and total sizes of obsolete tables; these are tables which are
			// no longer referenced by the current DB state or any open iterators.
			Obsolete metrics.CountAndSizeByPlacement
			// The counts and total sizes of zombie tables; these are tables which are
			// no longer referenced by the current DB state but are still in use by an iterator.
			Zombie metrics.CountAndSizeByPlacement
		}

		// Compression statistics for the live sstables.
		Compression CompressionMetrics

		// Garbage bytes.
		Garbage struct {
			// PointDeletionsBytesEstimate is the estimated file bytes (sstables plus
			// blob file references) that will be saved by compacting all point
			// deletions. This is dependent on table stats collection, so can be very
			// incomplete until InitialStatsCollectionComplete becomes true.
			PointDeletionsBytesEstimate uint64
			// RangeDeletionsBytesEstimate is the estimated file bytes (sstables plus
			// blob file references) that will be saved by compacting all range
			// deletions. This is dependent on table stats collection, so can be very
			// incomplete until InitialStatsCollectionComplete becomes true.
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
		// The counts and total physical sizes of blob files.
		Live metrics.CountAndSizeByPlacement
		// The counts and total physical sizes of obsolete blob files; these are blob
		// files which are no longer referenced by the current DB state or any open
		// iterators.
		Obsolete metrics.CountAndSizeByPlacement
		// The count and total physical size of zombie blob files; these are blob
		// files which are no longer referenced by the current DB state but are
		// still in use by an iterator.
		Zombie metrics.CountAndSizeByPlacement

		// ValueSize is the sum of the length of the uncompressed values in all live
		// (referenced by some sstable(s) within the current version) blob files.
		// ValueSize may be greater than Live.Total().Size when compression is
		// effective. ValueSize includes bytes in live blob files that are not
		// actually reachable by any sstable key. If every value within the blob
		// file is reachable by a key in a live sstable, then the entirety of the
		// blob file's values are included within ValueSize.
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
		// ReferencedBackingValueSize is the sum of the length of the uncompressed
		// values (in all live blob files) that are still referenced by keys
		// within backing tables. Note that this value is an overestimate because
		// each virtual table will contribute their backing table's referenced
		// value sizes.
		ReferencedBackingValueSize uint64

		// Compression statistics for the live blob files.
		Compression CompressionMetrics
	}

	// CompressionCounters are cumulative counters for the number of logical
	// (uncompressed) bytes that went through compression and decompression.
	CompressionCounters struct {
		LogicalBytesCompressed   block.ByLevel[block.ByKind[uint64]]
		LogicalBytesDecompressed block.ByLevel[block.ByKind[uint64]]
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

	// DeletePacer are metrics from the delete pacer, which manages obsolete file
	// deletion. These can be relevant if free disk space is unexplainably low.
	DeletePacer deletepacer.Metrics

	WALMetrics WALMetrics

	CategoryStats []block.CategoryStatsAggregate

	SecondaryCacheMetrics SecondaryCacheMetrics

	private struct {
		optionsFileSize  uint64
		manifestFileSize uint64
	}

	manualMemory manual.Metrics
}

// CompactMetrics contains metric related to compaction activity.
type CompactMetrics struct {
	// The total number of compactions, and per-compaction type counts.
	Count                  int64
	DefaultCount           int64
	DeleteOnlyCount        int64
	ElisionOnlyCount       int64
	CopyCount              int64
	MoveCount              int64
	ReadCount              int64
	TombstoneDensityCount  int64
	RewriteCount           int64
	MultiLevelCount        int64
	BlobFileRewriteCount   int64
	VirtualRewriteCount    int64
	PolicyEnforcementCount int64
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
	// BlobFileRewrite contains metrics for blob file rewrite compactions.
	BlobFileRewrite struct {
		// The total number of bytes read during blob file rewrite compactions.
		// This only counts blob value blocks (data blocks) that are read from
		// the input blob file. The index block is read but tracked as
		// blockkind.Metadata, not blockkind.BlobValue. As a result, BytesRead
		// can be smaller than BytesWritten.
		BytesRead int64
		// The total number of bytes written during blob file rewrite compactions.
		// This counts all bytes written to the output blob file, including blob
		// value blocks, index block, properties block, and footer. As a result,
		// BytesWritten can be larger than BytesRead.
		BytesWritten int64
	}
}

// IngestMetrics contains metrics related to ingestions.
type IngestMetrics struct {
	// The total number of ingestions
	Count uint64
	// The number of excise operations during ingestion
	ExciseIngestCount int64
}

// FlushMetrics contains metrics related to flush activity.
type FlushMetrics struct {
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

type KeysMetrics struct {
	// The approximate count of internal range key set keys in the database.
	RangeKeySetsCount uint64
	// The approximate count of internal tombstones (DEL, SINGLEDEL and
	// RANGEDEL key kinds) within the database.
	TombstoneCount uint64
	// A cumulative total number of missized DELSIZED keys encountered by
	// compactions since the database was opened.
	MissizedTombstonesCount uint64
}

// CompressionMetrics contains compression metrics for sstables or blob files.
type CompressionMetrics struct {
	// NoCompressionBytes is the total number of bytes in files that are not
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
		case compression.NoAlgorithm:
			cm.NoCompressionBytes += cs.UncompressedBytes
		case compression.Snappy:
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

// DirectoryContext identifies which WAL directory an operation targets
type DirectoryContext int

const (
	// DirectoryPrimary indicates operation targets the primary WAL directory
	DirectoryPrimary DirectoryContext = iota
	// DirectorySecondary indicates operation targets the secondary WAL directory
	DirectorySecondary
	// DirectoryUnknown indicates directory context is unknown or not applicable
	DirectoryUnknown
)

// WALMetrics contains directory-specific latency histograms for all WAL operations
type WALMetrics struct {
	// PrimaryFileOpLatency tracks all file operations for the primary directory
	PrimaryFileOpLatency prometheus.Histogram
	// SecondaryFileOpLatency tracks all file operations for the secondary directory
	SecondaryFileOpLatency prometheus.Histogram
	// Updated whenever a wal.Writer is closed
	record.LogWriterMetrics
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
	usageBytes += m.Table.Physical.Live.Local.Bytes
	usageBytes += m.Table.Physical.Obsolete.Local.Bytes
	usageBytes += m.Table.Physical.Zombie.Local.Bytes
	usageBytes += m.BlobFiles.Live.Local.Bytes
	usageBytes += m.BlobFiles.Obsolete.Local.Bytes
	usageBytes += m.BlobFiles.Zombie.Local.Bytes
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
		n += level.VirtualTables.Count
	}
	return n
}

// VirtualSize is the sum of the sizes of the virtual sstables in the
// latest version. BackingTable.Bytes - VirtualSize gives an estimate for
// the space amplification caused by not compacting virtual sstables.
func (m *Metrics) VirtualSize() uint64 {
	var size uint64
	for _, level := range m.Levels {
		size += level.VirtualTables.Bytes
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
	return m.Levels.Total(m.WAL.BytesWritten)
}

// RemoteTablesTotal returns the total number of remote tables and their total
// size. Remote tables are computed as the difference between total tables
// (live + obsolete + zombie) and local tables.
func (m *Metrics) RemoteTablesTotal() metrics.CountAndSize {
	cs := m.Table.Physical.Live
	cs.Accumulate(m.Table.Physical.Obsolete)
	cs.Accumulate(m.Table.Physical.Zombie)
	return cs.Shared.Sum(cs.External)
}

// Assert that Metrics implements redact.SafeFormatter.
var _ redact.SafeFormatter = (*Metrics)(nil)

// SafeFormat implements redact.SafeFormatter.
func (m *Metrics) SafeFormat(w redact.SafePrinter, _ rune) {
	w.SafeString(redact.SafeString(m.String()))
}

var (
	levelMetricsTableTopHeader = `LSM                             |    vtables   |   value sep   |        |   ingested   |    amp`
	levelMetricsTable          = func() table.Layout[*LevelMetrics] {
		def := table.Define[*LevelMetrics](
			table.StringWithTupleIndex("level", 5, table.AlignRight, func(tupleIndex int, m *LevelMetrics) string {
				if tupleIndex == manifest.NumLevels {
					return "total"
				}
				return fmt.Sprintf("L%d", tupleIndex)
			}),
			table.Bytes("size", 10, table.AlignRight, func(m *LevelMetrics) uint64 { return uint64(m.Tables.Bytes) + m.EstimatedReferencesSize }),
			table.Div(),
			table.Count("tables", 6, table.AlignRight, func(m *LevelMetrics) uint64 { return m.Tables.Count }),
			table.Bytes("size", 5, table.AlignRight, func(m *LevelMetrics) uint64 { return m.Tables.Bytes }),
			table.Div(),
			table.Count("count", 6, table.AlignRight, func(m *LevelMetrics) uint64 { return m.VirtualTables.Count }),
			table.Count("size", 5, table.AlignRight, func(m *LevelMetrics) uint64 { return m.VirtualTables.Bytes }),
			table.Div(),
			table.Bytes("refsz", 6, table.AlignRight, func(m *LevelMetrics) uint64 { return m.EstimatedReferencesSize }),
			table.Bytes("valblk", 6, table.AlignRight, func(m *LevelMetrics) uint64 { return m.Additional.ValueBlocksSize }),
			table.Div(),
			table.Bytes("in", 6, table.AlignRight, func(m *LevelMetrics) uint64 { return m.TableBytesIn }),
			table.Div(),
			table.Count("tables", 6, table.AlignRight, func(m *LevelMetrics) uint64 { return m.TablesIngested.Count }),
			table.Bytes("size", 5, table.AlignRight, func(m *LevelMetrics) uint64 { return m.TablesIngested.Bytes }),
			table.Div(),
			table.Int("r", 3, table.AlignRight, func(m *LevelMetrics) int { return int(m.Sublevels) }),
			table.Float("w", 5, table.AlignRight, func(m *LevelMetrics) float64 { return m.WriteAmp() }),
		)
		def.FilterFn = func(tupleIndex int, m *LevelMetrics) (passed bool) {
			return m.Tables.Count != 0 || m.VirtualTables.Count != 0 || m.TableBytesIn != 0 || m.TablesIngested.Count != 0
		}
		return def
	}()
	levelCompactionMetricsTableTopHeader = `COMPACTIONS               |     moved    |     multilevel    |     read     |       written`
	compactionLevelMetricsTable          = func() table.Layout[*LevelMetrics] {
		def := table.Define[*LevelMetrics](
			table.StringWithTupleIndex("level", 5, table.AlignRight, func(tupleIndex int, m *LevelMetrics) string {
				if tupleIndex == manifest.NumLevels {
					return "total"
				}
				return fmt.Sprintf("L%d", tupleIndex)
			}),
			table.Div(),
			table.Float("score", 5, table.AlignRight, func(m *LevelMetrics) float64 { return m.Score }),
			table.Float("ff", 5, table.AlignRight, func(m *LevelMetrics) float64 { return m.FillFactor }),
			table.Float("cff", 5, table.AlignRight, func(m *LevelMetrics) float64 { return m.CompensatedFillFactor }),
			table.Div(),
			table.Count("tables", 6, table.AlignRight, func(m *LevelMetrics) uint64 { return m.TablesMoved.Count }),
			table.Bytes("size", 5, table.AlignRight, func(m *LevelMetrics) uint64 { return m.TablesMoved.Bytes }),
			table.Div(),
			table.Bytes("top", 5, table.AlignRight, func(m *LevelMetrics) uint64 { return m.MultiLevel.TableBytesInTop }),
			table.Bytes("in", 5, table.AlignRight, func(m *LevelMetrics) uint64 { return m.MultiLevel.TableBytesIn }),
			table.Bytes("read", 5, table.AlignRight, func(m *LevelMetrics) uint64 { return m.MultiLevel.TableBytesRead }),
			table.Div(),
			table.Bytes("tables", 6, table.AlignRight, func(m *LevelMetrics) uint64 { return m.TableBytesRead }),
			table.Bytes("blob", 5, table.AlignRight, func(m *LevelMetrics) uint64 { return m.BlobBytesRead }),
			table.Div(),
			table.Count("tables", 6, table.AlignRight, func(m *LevelMetrics) uint64 { return m.TablesFlushed.Count + m.TablesCompacted.Count }),
			table.Bytes("sstsz", 6, table.AlignRight, func(m *LevelMetrics) uint64 { return m.TablesFlushed.Bytes + m.TablesCompacted.Bytes }),
			table.Bytes("blobsz", 6, table.AlignRight, func(m *LevelMetrics) uint64 { return m.BlobBytesFlushed + m.BlobBytesCompacted }),
		)
		def.FilterFn = func(tupleIndex int, m *LevelMetrics) (passed bool) {
			return !math.IsNaN(m.Score) || m.FillFactor != 0 || m.TablesMoved.Count != 0 || m.MultiLevel.TableBytesInTop != 0 ||
				m.MultiLevel.TableBytesIn != 0 || m.MultiLevel.TableBytesRead != 0 || m.BlobBytesRead != 0 ||
				m.TablesFlushed.Count != 0 || m.TablesCompacted.Count != 0 || m.BlobBytesFlushed != 0 || m.BlobBytesCompacted != 0
		}
		return def
	}()
	compactionKindTable = table.Define[*Metrics](
		table.String("kind", 5, table.AlignRight, func(m *Metrics) string { return "count" }),
		table.Div(),
		table.Int64("default", 7, table.AlignRight, func(m *Metrics) int64 { return m.Compact.DefaultCount }),
		table.Int64("delete", 7, table.AlignRight, func(m *Metrics) int64 { return m.Compact.DeleteOnlyCount }),
		table.Int64("elision", 8, table.AlignRight, func(m *Metrics) int64 { return m.Compact.ElisionOnlyCount }),
		table.Int64("move", 5, table.AlignRight, func(m *Metrics) int64 { return m.Compact.MoveCount }),
		table.Int64("read", 5, table.AlignRight, func(m *Metrics) int64 { return m.Compact.ReadCount }),
		table.Int64("tomb", 5, table.AlignRight, func(m *Metrics) int64 { return m.Compact.TombstoneDensityCount }),
		table.Int64("rewrite", 8, table.AlignRight, func(m *Metrics) int64 { return m.Compact.RewriteCount }),
		table.Int64("copy", 5, table.AlignRight, func(m *Metrics) int64 { return m.Compact.CopyCount }),
		table.Int64("multi", 6, table.AlignRight, func(m *Metrics) int64 { return m.Compact.MultiLevelCount }),
		table.Int64("blob", 5, table.AlignRight, func(m *Metrics) int64 { return m.Compact.BlobFileRewriteCount }),
		table.Int64("virtual", 7, table.AlignRight, func(m *Metrics) int64 { return m.Compact.VirtualRewriteCount }),
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
	blockCacheInfoTableTopHeader = `BLOCK CACHE`
	blockCacheInfoTable          = table.Define[blockCacheInfo](
		func() []table.Element {
			e := make([]table.Element, 0, 3+cache.NumCategories)
			e = append(e, table.String("level", 6, table.AlignCenter, func(i blockCacheInfo) string { return i.level }))
			e = append(e, table.String("all", 11, table.AlignCenter, func(i blockCacheInfo) string { return i.missRate }))
			e = append(e, table.Div())
			for c := range cache.Categories {
				e = append(e, table.String(c.String(), 12, table.AlignCenter, func(i blockCacheInfo) string {
					return i.perCategory[c]
				}))
			}
			return e
		}()...,
	)
	iteratorInfoTableTopHeader = `ITERATORS`
	iteratorInfoTableSubHeader = `        file cache        |    filter   |    open     |    open`
	iteratorInfoTable          = table.Define[iteratorInfo](
		table.String("entries", 12, table.AlignRight, func(i iteratorInfo) string { return i.fcEntries }),
		table.Div(),
		table.String("hit rate", 10, table.AlignRight, func(i iteratorInfo) string { return i.fcHitRate }),
		table.Div(),
		table.String("utilization", 11, table.AlignRight, func(i iteratorInfo) string { return i.bloomFilterUtil }),
		table.Div(),
		table.String("sst iters ", 11, table.AlignRight, func(i iteratorInfo) string { return i.sstableItersOpen }),
		table.Div(),
		table.String("snapshots ", 11, table.AlignRight, func(i iteratorInfo) string { return i.snapshotsOpen }),
	)
	diskUsageTableHeader = `FILES                 physical tables                 |                blob files`
	diskUsageTable       = table.Define[diskUsageInfo](
		table.String("", 9, table.AlignRight, func(i diskUsageInfo) string { return i.header }),
		table.Div(),
		table.String("local", 13, table.AlignCenter, func(i diskUsageInfo) string { return i.tables.Local.String() }),
		table.String("shared", 13, table.AlignCenter, func(i diskUsageInfo) string { return i.tables.Shared.String() }),
		table.String("remote", 13, table.AlignCenter, func(i diskUsageInfo) string { return i.tables.External.String() }),
		table.Div(),
		table.String("local", 13, table.AlignCenter, func(i diskUsageInfo) string { return i.blobFiles.Local.String() }),
		table.String("shared", 13, table.AlignCenter, func(i diskUsageInfo) string { return i.blobFiles.Shared.String() }),
		table.String("remote", 13, table.AlignCenter, func(i diskUsageInfo) string { return i.blobFiles.External.String() }),
	)
	miscInfoTable = table.Define[[2]string](
		table.String("MISC", 5, table.AlignLeft, func(i [2]string) string { return i[0] }),
		table.Div(),
		table.String("", 5, table.AlignLeft, func(i [2]string) string { return i[1] }),
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
	compactionInfoTableSubHeader = `                                                                         |      blob rewrites`
	compactionInfoTable          = table.Define[compactionMetricsInfo](
		table.String("est. debt", 13, table.AlignRight, func(i compactionMetricsInfo) string { return i.estimatedDebt }),
		table.Div(),
		table.String("in progress", 13, table.AlignRight, func(i compactionMetricsInfo) string { return i.inProgress }),
		table.Div(),
		table.String("cancelled", 10, table.AlignRight, func(i compactionMetricsInfo) string { return i.cancelled }),
		table.Div(),
		table.String("failed", 8, table.AlignRight, func(i compactionMetricsInfo) string { return fmt.Sprint(i.failed) }),
		table.Div(),
		table.String("problem spans", 16, table.AlignRight, func(i compactionMetricsInfo) string { return i.problemSpans }),
		table.Div(),
		table.String("read", 10, table.AlignRight, func(i compactionMetricsInfo) string { return i.blobFileRewriteBytesRead }),
		table.Div(),
		table.String("written", 10, table.AlignRight, func(i compactionMetricsInfo) string { return i.blobFileRewriteBytesWritten }),
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
	compressionTable       = table.Define[compressionInfo](
		table.String("algorithm", 13, table.AlignRight, func(i compressionInfo) string { return i.algorithm }),
		table.Div(),
		table.String("tables", 13, table.AlignRight, func(i compressionInfo) string { return i.tables }),
		table.Div(),
		table.String("blob files", 13, table.AlignRight, func(i compressionInfo) string { return i.blobFiles }),
	)
	compressionCountersTableHeader = `        Logical bytes compressed / decompressed`

	compressionCountersTable = table.Define[compressionCountersInfo](
		table.String("level", 5, table.AlignRight, func(i compressionCountersInfo) string { return i.level }),
		table.Div(),
		table.String("data blocks", 14, table.AlignCenter, func(i compressionCountersInfo) string { return i.DataBlocks }),
		table.Div(),
		table.String("value blocks", 14, table.AlignCenter, func(i compressionCountersInfo) string { return i.ValueBlocks }),
		table.Div(),
		table.String("other blocks", 14, table.AlignCenter, func(i compressionCountersInfo) string { return i.OtherBlocks }),
	)
	deletePacerTableHeader = `DELETE PACER`
	deletePacerTable       = table.Define[deletePacerInfo](
		table.String("", 14, table.AlignRight, func(i deletePacerInfo) string { return i.label }),
		table.Div(),
		table.String("in queue", 12, table.AlignCenter, func(i deletePacerInfo) string { return i.inQueue }),
		table.Div(),
		table.String("deleted", 12, table.AlignCenter, func(i deletePacerInfo) string { return i.deleted }),
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

type blockCacheInfo struct {
	level       string
	missRate    string
	perCategory [cache.NumCategories]string
}

func makeBlockCacheInfo(hm *cache.HitsAndMisses) []blockCacheInfo {
	// missRateAndPercentage returns a string that shows misses/(hits+misses) and
	// misses/totalMisses as percentages; for example: "12% [4.5%]".
	missRateAndPercentage := func(hits, misses, totalMisses int64) string {
		if hits == 0 && misses == 0 {
			return ""
		}
		return fmt.Sprintf("%s [%s]",
			crhumanize.Percent(misses, hits+misses),
			crhumanize.Percent(misses, totalMisses),
		)
	}

	totalHits, totalMisses := hm.Aggregate()
	res := make([]blockCacheInfo, 0, cache.NumLevels+1)
	for level := range cache.Levels {
		levelHits, levelMisses := hm.AggregateLevel(level)
		if levelHits == 0 && levelMisses == 0 {
			// Skip levels with no activity.
			continue
		}
		bci := blockCacheInfo{
			level:    level.String(),
			missRate: missRateAndPercentage(levelHits, levelMisses, totalMisses),
		}
		for category := range cache.Categories {
			hits, misses := hm.Get(level, category)
			bci.perCategory[category] = missRateAndPercentage(hits, misses, totalMisses)
		}
		res = append(res, bci)
	}
	last := blockCacheInfo{
		level:    "total",
		missRate: fmt.Sprintf("%s%%", crhumanize.Float(percent(totalMisses, totalHits+totalMisses), 1)),
	}
	for category := range cache.Categories {
		catHits, catMisses := hm.AggregateCategory(category)
		last.perCategory[category] = missRateAndPercentage(catHits, catMisses, totalMisses)
	}
	return append(res, last)
}

type iteratorInfo struct {
	fcEntries        string
	fcHitRate        string
	bloomFilterUtil  string
	sstableItersOpen string
	snapshotsOpen    string
}

type diskUsageInfo struct {
	header    string
	tables    metrics.CountAndSizeByPlacement
	blobFiles metrics.CountAndSizeByPlacement
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
	estimatedDebt               string
	inProgress                  string
	cancelled                   string
	failed                      int64
	problemSpans                string
	blobFileRewriteBytesRead    string
	blobFileRewriteBytesWritten string
}

type keysInfo struct {
	rangeKeys          string
	tombstones         string
	missizedTombstones string
	pointDels          string
	rangeDels          string
}

type compressionInfo struct {
	algorithm string
	tables    string
	blobFiles string
}

type deletePacerInfo struct {
	label   string
	inQueue string
	deleted string
}

func makeCompressionInfo(algorithm string, table, blob CompressionStatsForSetting) compressionInfo {
	i := compressionInfo{algorithm: algorithm}
	if table.CompressedBytes > 0 {
		i.tables = fmt.Sprintf("%s (CR=%s)", humanizeBytes(table.CompressedBytes), crhumanize.Float(table.CompressionRatio(), 2 /* precision */))
	}
	if blob.CompressedBytes > 0 {
		i.blobFiles = fmt.Sprintf("%s (CR=%s)", humanizeBytes(blob.CompressedBytes), crhumanize.Float(blob.CompressionRatio(), 2 /* precision */))
	}
	return i
}

type compressionCountersInfo struct {
	level string
	block.ByKind[string]
}

func makeCompressionCountersInfo(m *Metrics) []compressionCountersInfo {
	var result []compressionCountersInfo
	isZero := func(c *block.ByKind[uint64]) bool {
		return c.DataBlocks == 0 && c.ValueBlocks == 0 && c.OtherBlocks == 0
	}
	addLevel := func(level string, compressed, decompressed *block.ByKind[uint64]) {
		if isZero(compressed) && isZero(decompressed) {
			return
		}
		result = append(result, compressionCountersInfo{
			level: level,
			ByKind: block.ByKind[string]{
				DataBlocks:  humanizeBytes(compressed.DataBlocks) + " / " + humanizeBytes(decompressed.DataBlocks),
				ValueBlocks: humanizeBytes(compressed.ValueBlocks) + " / " + humanizeBytes(decompressed.ValueBlocks),
				OtherBlocks: humanizeBytes(compressed.OtherBlocks) + " / " + humanizeBytes(decompressed.OtherBlocks)},
		})
	}
	addLevel("L0-L4", &m.CompressionCounters.LogicalBytesCompressed.OtherLevels, &m.CompressionCounters.LogicalBytesDecompressed.OtherLevels)
	addLevel("L5", &m.CompressionCounters.LogicalBytesCompressed.L5, &m.CompressionCounters.LogicalBytesDecompressed.L5)
	addLevel("L6", &m.CompressionCounters.LogicalBytesCompressed.L6, &m.CompressionCounters.LogicalBytesDecompressed.L6)
	return result
}

// String pretty-prints the metrics.
//
// See testdata/metrics for an example.
func (m *Metrics) String() string {
	wb := ascii.Make(128 /* width */, 80 /* height */)

	// LSM level metrics.
	cur := wb.At(0, 0)
	cur = cur.WriteString(levelMetricsTableTopHeader).NewlineReturn()
	cur = levelMetricsTable.Render(cur, table.RenderOptions{
		HorizontalDividers: table.MakeHorizontalDividers(0, -1),
	}, slices.Collect(m.Levels.Iter(m.WAL.BytesWritten))...)
	cur = cur.NewlineReturn()

	// Compaction level metrics.
	cur = cur.WriteString(levelCompactionMetricsTableTopHeader).NewlineReturn()
	cur = compactionLevelMetricsTable.Render(cur, table.RenderOptions{
		HorizontalDividers: table.MakeHorizontalDividers(0, -1),
	}, slices.Collect(m.Levels.Iter(m.WAL.BytesWritten))...)

	cur = cur.NewlineReturn()
	cur = compactionKindTable.Render(cur, table.RenderOptions{
		HorizontalDividers: table.HorizontalDividers{},
	}, m)
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
		total:     crhumanize.Count(m.Ingest.Count).String(),
		flushable: fmt.Sprintf("%s (%s)", humanizeCount(m.Flush.AsIngestCount), humanizeBytes(m.Flush.AsIngestBytes)),
	}
	cur = cur.WriteString(commitPipelineInfoTableTopHeader).NewlineReturn()
	cur = cur.WriteString(commitPipelineInfoTableSubHeader).NewlineReturn()
	cur = commitPipelineInfoTable.Render(cur, table.RenderOptions{}, commitPipelineInfoContents)
	cur = cur.NewlineReturn()

	cur = cur.WriteString(blockCacheInfoTableTopHeader)
	cur = cur.Printf(": %s entries (%s)", humanizeCount(m.BlockCache.Count), humanizeBytes(m.BlockCache.Size))
	cur = cur.NewlineReturn()

	cur = cur.WriteString("                 miss rate [percentage of total misses] since start\n")
	bci := makeBlockCacheInfo(&m.BlockCache.HitsAndMisses)
	cur = blockCacheInfoTable.Render(cur, table.RenderOptions{
		HorizontalDividers: table.MakeHorizontalDividers(0, len(bci)-1),
	}, bci...)
	cur = cur.NewlineReturn()

	if m.BlockCache.Recent[0].Since != 0 {
		cur = cur.WriteString("                 miss rate [percentage of total misses] over last ~10m\n") // TODO(radu): print exact timeframe
		bci = makeBlockCacheInfo(&m.BlockCache.Recent[0].HitsAndMisses)
		cur = blockCacheInfoTable.Render(cur, table.RenderOptions{
			HorizontalDividers: table.MakeHorizontalDividers(0, len(bci)-1),
		}, bci...)
		cur = cur.NewlineReturn()
	}

	if m.BlockCache.Recent[1].Since != 0 {
		cur = cur.WriteString("                 miss rate [percentage of total misses] over last ~1h\n") // TODO(radu): print exact timeframe
		bci = makeBlockCacheInfo(&m.BlockCache.Recent[1].HitsAndMisses)
		cur = blockCacheInfoTable.Render(cur, table.RenderOptions{
			HorizontalDividers: table.MakeHorizontalDividers(0, len(bci)-1),
		}, bci...)
		cur = cur.NewlineReturn()
	}

	iteratorInfoContents := iteratorInfo{
		fcEntries:        fmt.Sprintf("%s (%s)", humanizeCount(m.FileCache.TableCount), humanizeBytes(m.FileCache.Size)),
		fcHitRate:        fmt.Sprintf("%.1f%%", hitRate(m.FileCache.Hits, m.FileCache.Misses)),
		bloomFilterUtil:  fmt.Sprintf("%.1f%%", hitRate(m.Filter.Hits, m.Filter.Misses)),
		sstableItersOpen: humanizeCount(m.TableIters),
		snapshotsOpen:    humanizeCount(m.Snapshots.Count),
	}
	cur = cur.WriteString(iteratorInfoTableTopHeader).NewlineReturn()
	cur = cur.WriteString(iteratorInfoTableSubHeader).NewlineReturn()
	cur = iteratorInfoTable.Render(cur, table.RenderOptions{}, iteratorInfoContents)
	cur = cur.NewlineReturn()

	diskUsageContents := []diskUsageInfo{
		{header: "live", tables: m.Table.Physical.Live, blobFiles: m.BlobFiles.Live},
		{header: "zombie", tables: m.Table.Physical.Zombie, blobFiles: m.BlobFiles.Zombie},
		{header: "obsolete", tables: m.Table.Physical.Obsolete, blobFiles: m.BlobFiles.Obsolete},
	}
	cur = cur.WriteString(diskUsageTableHeader).NewlineReturn()
	cur = diskUsageTable.Render(cur, table.RenderOptions{}, diskUsageContents...)
	cur = cur.NewlineReturn()

	tableStatsStatus := func() string {
		switch {
		case !m.Table.InitialStatsCollectionComplete:
			return "loading"
		case m.Table.PendingStatsCollectionCount > 0:
			return fmt.Sprintf("%s pending", humanizeCount(m.Table.PendingStatsCollectionCount))
		default:
			return "all loaded"
		}
	}

	bytesAndPercent := func(value, denominator uint64) string {
		if denominator == 0 || value == 0 {
			return humanizeBytes(value)
		}
		return fmt.Sprintf("%s (%s)", humanizeBytes(value), crhumanize.Percent(value, denominator))
	}

	miscContents := [][2]string{
		{"table stats", tableStatsStatus()},
		{"table garbage", ""},
		{"  point del", humanizeBytes(m.Table.Garbage.PointDeletionsBytesEstimate)},
		{"  range del", humanizeBytes(m.Table.Garbage.RangeDeletionsBytesEstimate)},
		{"blob values", ""},
		{"  total", humanizeBytes(m.BlobFiles.ValueSize)},
		{"  refed", bytesAndPercent(m.BlobFiles.ReferencedValueSize, m.BlobFiles.ValueSize)},
		{"  backing-refed", bytesAndPercent(m.BlobFiles.ReferencedBackingValueSize, m.BlobFiles.ValueSize)},
	}
	cur = miscInfoTable.Render(cur, table.RenderOptions{}, miscContents...)
	cur = cur.NewlineReturn()

	var inUseTotal uint64
	for i := range m.manualMemory {
		inUseTotal += m.manualMemory[i].InUseBytes
	}
	inUse := func(purpose manual.Purpose) uint64 {
		return m.manualMemory[purpose].InUseBytes
	}
	cgoMemInfoContents := cgoMemInfo{
		tot: humanizeBytes(inUseTotal),
		bcTot: humanizeBytes(inUse(manual.BlockCacheData) +
			inUse(manual.BlockCacheMap) + inUse(manual.BlockCacheEntry)),
		bcData:       humanizeBytes(inUse(manual.BlockCacheData)),
		bcMaps:       humanizeBytes(inUse(manual.BlockCacheMap)),
		bcEnts:       humanizeBytes(inUse(manual.BlockCacheEntry)),
		memtablesTot: humanizeBytes(inUse(manual.MemTable)),
	}
	cur = cur.WriteString(cgoMemInfoTableHeader).NewlineReturn()
	cur = cgoMemInfoTable.Render(cur, table.RenderOptions{}, cgoMemInfoContents)
	cur = cur.NewlineReturn()

	compactionMetricsInfoContents := compactionMetricsInfo{
		estimatedDebt: humanizeBytes(m.Compact.EstimatedDebt),
		inProgress: fmt.Sprintf("%s (%s)", humanizeCount(m.Compact.NumInProgress),
			humanizeBytes(m.Compact.InProgressBytes)),
		cancelled: fmt.Sprintf("%s (%s)", humanizeCount(m.Compact.CancelledCount),
			humanizeBytes(m.Compact.CancelledBytes)),
		failed:                      m.Compact.FailedCount,
		problemSpans:                fmt.Sprintf("%d%s", m.Compact.NumProblemSpans, ifNonZero(m.Compact.NumProblemSpans, "!!")),
		blobFileRewriteBytesRead:    humanizeBytes(m.Compact.BlobFileRewrite.BytesRead),
		blobFileRewriteBytesWritten: humanizeBytes(m.Compact.BlobFileRewrite.BytesWritten),
	}
	cur = cur.WriteString(compactionInfoTableTopHeader).NewlineReturn()
	cur = cur.WriteString(compactionInfoTableSubHeader).NewlineReturn()
	cur = compactionInfoTable.Render(cur, table.RenderOptions{}, compactionMetricsInfoContents)
	cur = cur.NewlineReturn()

	keysInfoContents := keysInfo{
		rangeKeys:          humanizeCount(m.Keys.RangeKeySetsCount),
		tombstones:         humanizeCount(m.Keys.TombstoneCount),
		missizedTombstones: fmt.Sprintf("%d%s", m.Keys.MissizedTombstonesCount, ifNonZero(m.Keys.MissizedTombstonesCount, "!!")),
		pointDels:          humanizeBytes(m.Table.Garbage.PointDeletionsBytesEstimate),
		rangeDels:          humanizeBytes(m.Table.Garbage.RangeDeletionsBytesEstimate),
	}
	cur = cur.WriteString(keysInfoTableTopHeader).NewlineReturn()
	cur = keysInfoTable.Render(cur, table.RenderOptions{}, keysInfoContents)
	cur = cur.NewlineReturn()

	cur = cur.WriteString(compressionTableHeader).NewlineReturn()

	compressionContents := []compressionInfo{
		{
			algorithm: "none",
			tables:    humanizeBytesOrEmpty(m.Table.Compression.NoCompressionBytes),
			blobFiles: humanizeBytesOrEmpty(m.BlobFiles.Compression.NoCompressionBytes),
		},
		makeCompressionInfo("snappy", m.Table.Compression.Snappy, m.BlobFiles.Compression.Snappy),
		makeCompressionInfo("minlz", m.Table.Compression.MinLZ, m.BlobFiles.Compression.MinLZ),
		makeCompressionInfo("zstd", m.Table.Compression.Zstd, m.BlobFiles.Compression.Zstd),
		{
			algorithm: "unknown",
			tables:    humanizeBytesOrEmpty(m.Table.Compression.CompressedBytesWithoutStats),
			blobFiles: humanizeBytesOrEmpty(m.BlobFiles.Compression.CompressedBytesWithoutStats),
		},
	}
	compressionContents = slices.DeleteFunc(compressionContents, func(i compressionInfo) bool {
		return i.tables == "" && i.blobFiles == ""
	})
	cur = compressionTable.Render(cur, table.RenderOptions{}, compressionContents...)

	cur = cur.NewlineReturn()
	cur = cur.WriteString(compressionCountersTableHeader).NewlineReturn()
	cur = compressionCountersTable.Render(cur, table.RenderOptions{}, makeCompressionCountersInfo(m)...)

	cur = cur.NewlineReturn()
	cur.WriteString(deletePacerTableHeader)
	deletePacerContents := []deletePacerInfo{
		{
			label:   "tables",
			inQueue: m.DeletePacer.InQueue.Tables.String(),
			deleted: m.DeletePacer.Deleted.Tables.String(),
		},
		{
			label:   "blob files",
			inQueue: m.DeletePacer.InQueue.BlobFiles.String(),
			deleted: m.DeletePacer.Deleted.BlobFiles.String(),
		},
		{
			label:   "other files",
			inQueue: m.DeletePacer.InQueue.Other.String(),
			deleted: m.DeletePacer.Deleted.Other.String(),
		},
	}
	cur = deletePacerTable.Render(cur, table.RenderOptions{}, deletePacerContents...)

	_ = cur
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
	const sstableReaderSize64bit = 304
	const blobFileReaderSize64bit = 120
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

	// Clear the recent block cache stats as they can vary based on timing.
	for i := range mCopy.BlockCache.Recent {
		mCopy.BlockCache.Recent[i].HitsAndMisses = cache.HitsAndMisses{}
		mCopy.BlockCache.Recent[i].Since = 0
	}
	// Clear the obsolete stats and delete pacer stats as they can vary based on
	// timing.
	mCopy.Table.Physical.Obsolete = metrics.CountAndSizeByPlacement{}
	mCopy.BlobFiles.Obsolete = metrics.CountAndSizeByPlacement{}
	mCopy.DeletePacer = deletepacer.Metrics{}
	return redact.StringWithoutMarkers(&mCopy)
}

// Total returns the sum of the per-level metrics and WAL metrics.
func (lm *AllLevelMetrics) Total(walBytesWritten uint64) LevelMetrics {
	var total LevelMetrics
	for level := range lm {
		total.Add(&lm[level])
	}
	// Compute total bytes-in as the bytes written to the WAL + bytes ingested.
	total.TableBytesIn = walBytesWritten + total.TablesIngested.Bytes
	// Add the total bytes-in to the total bytes-flushed. This is to account for
	// the bytes written to the log and bytes written externally and then
	// ingested.
	total.TablesFlushed.Bytes += total.TableBytesIn
	return total
}

// Iter returns an iterator over all level metrics - including the total for all
// levels.
func (lm *AllLevelMetrics) Iter(walBytesWritten uint64) iter.Seq[*LevelMetrics] {
	return func(yield func(*LevelMetrics) bool) {
		for i := range lm {
			lvlMetric := lm[i]
			if lvlMetric.Score == 0 {
				lvlMetric.Score = math.NaN()
			}
			if !yield(&lvlMetric) {
				break
			}
		}
		t := lm.Total(walBytesWritten)
		t.Score, t.FillFactor, t.CompensatedFillFactor = math.NaN(), math.NaN(), math.NaN()
		yield(&t)
	}
}

func (lm *AllLevelMetrics) String() string {
	wb := ascii.Make(128 /* width */, 80 /* height */)

	// LSM level metrics.
	cur := wb.At(0, 0)
	cur = cur.WriteString(levelMetricsTableTopHeader).NewlineReturn()
	levelMetricsTable.Render(cur, table.RenderOptions{
		HorizontalDividers: table.MakeHorizontalDividers(0, -1),
	}, slices.Collect(lm.Iter(0))...)
	return wb.String()
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

func (lm *AllLevelMetrics) update(updates levelMetricsDelta) {
	for i, u := range updates {
		if u != nil {
			lm[i].Add(u)
		}
	}
}

func humanizeCount[T crhumanize.Integer](value T) string {
	return crhumanize.Count(value, crhumanize.Compact, crhumanize.OmitI).String()
}

func humanizeBytes[T crhumanize.Integer](value T) string {
	return crhumanize.Bytes(value, crhumanize.Compact, crhumanize.OmitI).String()
}

func humanizeBytesOrEmpty[T crhumanize.Integer](value T) string {
	if value == 0 {
		return ""
	}
	return crhumanize.Bytes(value, crhumanize.Compact, crhumanize.OmitI).String()
}
