// Copyright 2019 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package pebble

// HistogramType represents a type of histogram.
type HistogramType int

// HistogramTypes.
const (
	DbGet HistogramType = iota
	DbWrite
	CompactionTime
	CompactionCPUTime
	SubcompactionSetupTime
	TableSyncMicros
	CompactionOutfileSyncMicros
	WalFileSyncMicros
	ManifestFileSyncMicros
	// TIME SPENT IN IO DURING TABLE OPEN
	TableOpenIoMicros
	DbMultiget
	ReadBlockCompactionMicros
	ReadBlockGetMicros
	WriteRawBlockMicros
	StallL0SlowdownCount
	StallMemtableCompactionCount
	StallL0NumFilesCount
	HardRateLimitDelayCount
	SoftRateLimitDelayCount
	NumFilesInSingleCompaction
	DbSeek
	WriteStall
	SstReadMicros
	// The number of subcompactions actually scheduled during a compaction
	NumSubcompactionsScheduled
	// Value size distribution in each operation
	BytesPerRead
	BytesPerWrite
	BytesPerMultiget

	// number of bytes compressed/decompressed
	// number of bytes is when uncompressed; i.e. before/after respectively
	BytesCompressed
	BytesDecompressed
	CompressionTimesNanos
	DecompressionTimesNanos
	// Number of merge operands passed to the merge operator in user read
	// requests.
	ReadNumMergeOperands

	// BlobDB specific stats
	// Size of keys written to BlobDB.
	BlobDbKeySize
	// Size of values written to BlobDB.
	BlobDbValueSize
	// BlobDB Put/PutWithTTL/PutUntil/Write latency.
	BlobDbWriteMicros
	// BlobDB Get lagency.
	BlobDbGetMicros
	// BlobDB MultiGet latency.
	BlobDbMultigetMicros
	// BlobDB Seek/SeekToFirst/SeekToLast/SeekForPrev latency.
	BlobDbSeekMicros
	// BlobDB Next latency.
	BlobDbNextMicros
	// BlobDB Prev latency.
	BlobDbPrevMicros
	// Blob file write latency.
	BlobDbBlobFileWriteMicros
	// Blob file read latency.
	BlobDbBlobFileReadMicros
	// Blob file sync latency.
	BlobDbBlobFileSyncMicros
	// BlobDB garbage collection time.
	BlobDbGcMicros
	// BlobDB compression time.
	BlobDbCompressionMicros
	// BlobDB decompression time.
	BlobDbDecompressionMicros
	// Time spent flushing memtable to disk
	FlushTime
	SstBatchSize

	HistogramTypeMax
)

// HistogramHook is a hook for measuring histogram from externally.
type HistogramHook func(gistogramType HistogramType) (stop func())
