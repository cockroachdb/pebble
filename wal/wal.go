// Copyright 2024 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package wal

import (
	"fmt"
	"io"
	"sync"
	"time"

	"github.com/cockroachdb/pebble/internal/base"
	"github.com/cockroachdb/pebble/record"
	"github.com/cockroachdb/pebble/vfs"
	"github.com/prometheus/client_golang/prometheus"
)

// TODO(sumeer): write a high-level comment describing the approach.

// Dir is used for storing log files.
type Dir struct {
	FS      vfs.FS
	Dirname string
}

// NumWAL is the number of the virtual WAL. It can map to one or more physical
// log files. In standalone mode, it will map to exactly one log file. In
// failover mode, it can map to many log files, which are totally ordered
// (using a dense logNameIndex).
//
// In general, WAL refers to the virtual WAL, and file refers to a log file.
// The Pebble MANIFEST only knows about virtual WALs and assigns numbers to
// them. Additional mapping to one or more files happens in this package. If a
// WAL maps to multiple files, the source of truth regarding that mapping is
// the contents of the directories.
type NumWAL base.DiskFileNum

// logNameIndex numbers log files within a WAL.
type logNameIndex uint32

// TODO(sumeer): parsing func. And remove attempts to parse log files outside
// the wal package (including tools).

// makeLogFilename makes a log filename.
func makeLogFilename(wn NumWAL, index logNameIndex) string {
	if index == 0 {
		// Use a backward compatible name, for simplicity.
		return base.MakeFilename(base.FileTypeLog, base.DiskFileNum(wn))
	}
	return fmt.Sprintf("%s-%03d.log", base.DiskFileNum(wn).String(), index)
}

// Options provides configuration for the Manager.
type Options struct {
	// Primary dir for storing WAL files. It must already be created and synced
	// up to the root.
	Primary Dir
	// Secondary is used for failover. Optional. It must already be created and
	// synced up to the root.
	Secondary Dir

	// MinUnflushedLogNum is the smallest WAL number corresponding to
	// mutations that have not been flushed to a sstable.
	MinUnflushedWALNum NumWAL

	// Recycling configuration. Only files in the primary dir are recycled.

	// MaxNumRecyclableLogs is the maximum number of log files to maintain for
	// recycling.
	MaxNumRecyclableLogs int

	// Configuration for calling vfs.NewSyncingFile.

	// NoSyncOnClose is documented in SyncingFileOptions.
	NoSyncOnClose bool
	// BytesPerSync is documented in SyncingFileOptions.
	BytesPerSync int
	// PreallocateSize is documented in SyncingFileOptions.
	PreallocateSize func() int

	// MinSyncInterval is documented in Options.WALMinSyncInterval.
	MinSyncInterval func() time.Duration
	// FsyncLatency records fsync latency. This doesn't differentiate between
	// fsyncs on the primary and secondary dir.
	//
	// TODO(sumeer): consider separating out into two histograms.
	FsyncLatency prometheus.Histogram
	// QueueSemChan is the channel to pop from when popping from queued records
	// that have requested a sync. It's original purpose was to function as a
	// semaphore that prevents the record.LogWriter.flusher.syncQueue from
	// overflowing (which will cause a panic). It is still useful in that role
	// when the WALManager is configured in standalone mode. In failover mode
	// there is no syncQueue, so the pushback into the commit pipeline is
	// unnecessary, but possibly harmless.
	QueueSemChan chan struct{}

	// ElevatedWriteStallThresholdLag is the duration for which an elevated
	// threshold should continue after a switch back to the primary dir.
	ElevatedWriteStallThresholdLag time.Duration

	// Logger for logging.
	Logger base.Logger

	// EventListener is called on events, like log file creation.
	EventListener EventListener
}

// EventListener is called on events, like log file creation.
type EventListener interface {
	// LogCreated informs the listener of a log file creation.
	LogCreated(CreateInfo)
}

// CreateInfo contains info about a log file creation event.
type CreateInfo struct {
	// JobID is the ID of the job the caused the WAL to be created.
	//
	// TODO(sumeer): for a file created later due to the need to failover, we
	// need to provide a JobID generator func in Options.
	JobID int
	// Path to the file. This includes the NumWAL, and implicitly or explicitly
	// includes the logNameIndex.
	Path string
	// IsSecondary is true if the file was created on the secondary.
	IsSecondary bool
	// Num is the WAL number.
	Num NumWAL
	// RecycledFileNum is the file number of a previous log file which was
	// recycled to create this one. Zero if recycling did not take place.
	RecycledFileNum base.DiskFileNum
	// Err contains any error.
	Err error
}

// Stats exposes stats used in Pebble metrics.
//
// NB: Metrics.WAL.{Size,BytesIn,BytesWritten} are not maintained by the wal
// package.
//
// TODO(sumeer): with failover, Metrics.WAL.BytesWritten needs to be
// maintained here.
type Stats struct {
	// ObsoleteFileCount is the number of obsolete log files.
	ObsoleteFileCount int
	// ObsoleteFileSize is the total size of obsolete log files.
	ObsoleteFileSize uint64
	// LiveFileCount is the number of live log files.
	LiveFileCount int
	// LiveFileSize is the total size of live log files. This can be higher than
	// LiveSize due to log recycling (a live log file may be larger than the
	// size used in its latest incarnation), or failover (resulting in multiple
	// log files containing the same records).
	//
	// This is updated only when log files are closed, to minimize
	// synchronization.
	LiveFileSize uint64
}

// Manager handles all WAL work.
//
//   - Init, List, OpenForRead will be called during DB initialization.
//   - Obsolete can be called concurrently with WAL writing.
//   - WAL writing: Is done via Create, and the various Writer methods. These
//     are required to be serialized via external synchronization (specifically,
//     the caller does it via commitPipeline.mu).
type Manager interface {
	// Init initializes the Manager.
	Init(o Options) error
	// List returns the virtual WALs in ascending order.
	List() ([]NumWAL, error)
	// Obsolete informs the manager that all virtual WALs less than
	// minUnflushedNum are obsolete. The callee can choose to recycle some
	// underlying log files, if !noRecycle. The log files that are not recycled,
	// and therefore can be deleted, are returned. The deletable files are no
	// longer tracked by the manager.
	Obsolete(minUnflushedNum NumWAL, noRecycle bool) (toDelete []DeletableLog, err error)
	// OpenForRead opens a virtual WAL for read.
	OpenForRead(wn NumWAL, strictWALTail bool) (Reader, error)
	// Create creates a new virtual WAL.
	//
	// NumWALs passed to successive Create calls must be monotonically
	// increasing, and be greater than any NumWAL seen earlier. The caller must
	// close the previous Writer before calling Create.
	//
	// jobID is used for the WALEventListener.
	Create(wn NumWAL, jobID int) (Writer, error)
	// ListFiles lists the log files backing the given unflushed WAL.
	ListFiles(wn NumWAL) (files []CopyableLog, err error)
	// ElevateWriteStallThresholdForFailover returns true if the caller should
	// use a high write stall threshold because the WALs are being written to
	// the secondary dir.
	ElevateWriteStallThresholdForFailover() bool
	// Stats returns the latest Stats.
	Stats() Stats
	// Close the manager.
	// REQUIRES: Writers and Readers have already been closed.
	Close() error

	// RecyclerForTesting exposes the internal LogRecycler.
	RecyclerForTesting() *LogRecycler
}

// DeletableLog contains information about a log file that can be deleted.
type DeletableLog struct {
	vfs.FS
	// Path to the file.
	Path string
	NumWAL
	FileSize uint64
}

// CopyableLog contains information about a log file that can be copied (e.g.
// for checkpointing).
type CopyableLog struct {
	vfs.FS
	Path string
}

// SyncOptions has non-nil Done and Err when fsync is requested, else both are
// nil.
type SyncOptions struct {
	Done *sync.WaitGroup
	Err  *error
}

// Writer writes to a virtual WAL. A Writer in standalone mode maps to a
// single record.LogWriter. In failover mode, it can failover across multiple
// physical log files.
type Writer interface {
	// WriteRecord writes a complete record. The record is asynchronously
	// persisted to the underlying writer. If SyncOptions.Done != nil, the wait
	// group will be notified when durability is guaranteed or an error has
	// occurred (set in SyncOptions.Err). External synchronisation provided by
	// commitPipeline.mu guarantees that WriteRecord calls are serialized.
	//
	// The logicalOffset is the logical size of the WAL after this record is
	// written. If the WAL corresponds to a single log file, this is the offset
	// in that log file.
	WriteRecord(p []byte, opts SyncOptions) (logicalOffset int64, err error)
	// Close the writer.
	Close() (logicalOffset int64, err error)
	// Metrics must be called after Close. The callee will no longer modify the
	// returned LogWriterMetrics.
	Metrics() *record.LogWriterMetrics
}

// Reader reads a virtual WAL.
type Reader interface {
	// NextRecord returns the next record, or error.
	NextRecord() (io.Reader, error)
	// LogicalOffset is the monotonically increasing offset in the WAL. When the
	// WAL corresponds to a single log file, this is the offset in that log
	// file.
	LogicalOffset() int64
	// Close the reader.
	Close() error
}

// Make lint happy.
var _ logNameIndex = 0
var _ = makeLogFilename
