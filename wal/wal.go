// Copyright 2024 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package wal

import (
	"fmt"
	"io"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/cockroachdb/pebble/internal/base"
	"github.com/cockroachdb/pebble/record"
	"github.com/cockroachdb/pebble/vfs"
	"github.com/cockroachdb/redact"
	"github.com/prometheus/client_golang/prometheus"
)

// TODO(sumeer): write a high-level comment describing the approach.

// Dir is used for storing log files.
type Dir struct {
	Lock    *base.DirLock
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

// String implements fmt.Stringer.
func (s NumWAL) String() string { return base.DiskFileNum(s).String() }

// LogNameIndex numbers log files within a WAL.
type LogNameIndex uint32

// String implements fmt.Stringer.
func (li LogNameIndex) String() string {
	return fmt.Sprintf("%03d", li)
}

// makeLogFilename makes a log filename.
func makeLogFilename(wn NumWAL, index LogNameIndex) string {
	if index == 0 {
		// Use a backward compatible name, for simplicity.
		return fmt.Sprintf("%s.log", base.DiskFileNum(wn).String())
	}
	return fmt.Sprintf("%s-%s.log", base.DiskFileNum(wn).String(), index)
}

// ParseLogFilename takes a base filename and parses it into its constituent
// NumWAL and LogNameIndex. If the filename is not a log file, it returns false
// for the final return value.
func ParseLogFilename(name string) (NumWAL, LogNameIndex, bool) {
	i := strings.IndexByte(name, '.')
	if i < 0 || name[i:] != ".log" {
		return 0, 0, false
	}
	j := strings.IndexByte(name[:i], '-')
	if j < 0 {
		dfn, ok := base.ParseDiskFileNum(name[:i])
		if !ok {
			// We've considered returning an error for filenames that end in
			// '.log' but fail to parse correctly. We decided against it because
			// the '.log' suffix is used by Cockroach's daignostics log files.
			// It's conceivable that some of these found their way into a data
			// directory, and erroring would cause an issue for an existing
			// Cockroach deployment.
			return 0, 0, false
		}
		return NumWAL(dfn), 0, true
	}
	dfn, ok := base.ParseDiskFileNum(name[:j])
	if !ok {
		return 0, 0, false
	}
	li, err := strconv.ParseUint(name[j+1:i], 10, 64)
	if err != nil {
		return 0, 0, false
	}
	return NumWAL(dfn), LogNameIndex(li), true
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

	// WAL file operation latency histograms
	PrimaryFileOpHistogram   record.WALFileOpHistogram
	SecondaryFileOpHistogram record.WALFileOpHistogram
	// QueueSemChan is the channel to pop from when popping from queued records
	// that have requested a sync. It's original purpose was to function as a
	// semaphore that prevents the record.LogWriter.flusher.syncQueue from
	// overflowing (which will cause a panic). It is still useful in that role
	// when the WALManager is configured in standalone mode. In failover mode
	// there is no syncQueue, so the pushback into the commit pipeline is
	// unnecessary, but possibly harmless.
	QueueSemChan chan struct{}

	// Logger for logging.
	Logger base.Logger

	// EventListener is called on events, like log file creation.
	EventListener EventListener

	FailoverOptions
	// FailoverWriteAndSyncLatency is only populated when WAL failover is
	// configured.
	FailoverWriteAndSyncLatency prometheus.Histogram

	// WriteWALSyncOffsets determines whether to write WAL sync chunk offsets.
	// The format major version can change (ratchet) at runtime, so this must be
	// a function rather than a static bool to ensure we use the latest format version.
	// It is plumbed down from wal.Options to record.newLogWriter.
	WriteWALSyncOffsets func() bool
}

// Init constructs and initializes a WAL manager from the provided options and
// the set of initial logs. The initial parameter must contain all WAL files
// that have not been deleted. It is only used to queue these up for deletion
// at the appropriate time (i.e. when their numbers are less than minUnflushedNum)
// and to ensure that they won't be recycled.
func Init(o Options, initial Logs) (Manager, error) {
	var m Manager
	if o.Secondary == (Dir{}) {
		m = new(StandaloneManager)
	} else {
		m = new(failoverManager)
	}
	if err := m.init(o, initial); err != nil {
		return nil, err
	}
	return m, nil
}

// Dirs returns the primary Dir and the secondary if provided.
func (o *Options) Dirs() []Dir {
	if o.Secondary == (Dir{}) {
		return []Dir{o.Primary}
	}
	return []Dir{o.Primary, o.Secondary}
}

// FailoverOptions are options that are specific to failover mode.
type FailoverOptions struct {
	// PrimaryDirProbeInterval is the interval for probing the primary dir, when
	// the WAL is being written to the secondary, to decide when to fail back.
	PrimaryDirProbeInterval time.Duration
	// HealthyProbeLatencyThreshold is the latency threshold to declare that the
	// primary is healthy again.
	HealthyProbeLatencyThreshold time.Duration
	// HealthyInterval is the time interval over which the probes have to be
	// healthy. That is, we look at probe history of length
	// HealthyInterval/PrimaryDirProbeInterval.
	HealthyInterval time.Duration

	// UnhealthySamplingInterval is the interval for sampling ongoing calls and
	// errors in the latest LogWriter.
	UnhealthySamplingInterval time.Duration
	// UnhealthyOperationLatencyThreshold is the latency threshold that is
	// considered unhealthy, for operations done by a LogWriter. The second return
	// value indicates whether we should consider failover at all. If the second
	// return value is false, failover is disabled.
	UnhealthyOperationLatencyThreshold func() (time.Duration, bool)

	// ElevatedWriteStallThresholdLag is the duration for which an elevated
	// threshold should continue after a switch back to the primary dir. This is
	// because we may have accumulated many unflushed memtables and flushing
	// them can take some time. Maybe set to 60s.
	ElevatedWriteStallThresholdLag time.Duration

	// timeSource is only non-nil for tests.
	timeSource

	monitorIterationForTesting chan<- struct{}
	proberIterationForTesting  chan<- struct{}
	monitorStateForTesting     func(numSwitches int, ongoingLatencyAtSwitch time.Duration)
	logWriterCreatedForTesting chan<- struct{}
}

// EnsureDefaults ensures that the default values for all options are set if a
// valid value was not already specified.
func (o *FailoverOptions) EnsureDefaults() {
	if o.PrimaryDirProbeInterval == 0 {
		o.PrimaryDirProbeInterval = time.Second
	}
	if o.HealthyProbeLatencyThreshold == 0 {
		o.HealthyProbeLatencyThreshold = 25 * time.Millisecond
	}
	if o.HealthyInterval == 0 {
		o.HealthyInterval = 15 * time.Second
	}
	if o.UnhealthySamplingInterval == 0 {
		o.UnhealthySamplingInterval = 100 * time.Millisecond
	}
	if o.UnhealthyOperationLatencyThreshold == nil {
		o.UnhealthyOperationLatencyThreshold = func() (time.Duration, bool) {
			return 100 * time.Millisecond, true
		}
	}
	if o.ElevatedWriteStallThresholdLag == 0 {
		o.ElevatedWriteStallThresholdLag = 60 * time.Second
	}
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
	// Failover contains failover stats.
	Failover FailoverStats
}

// FailoverStats contains stats about WAL failover. These are empty if
// failover is not configured.
type FailoverStats struct {
	// DirSwitchCount is the number of times WAL writing has switched to a
	// different directory, either due to failover, when the current dir is
	// unhealthy, or to failback to the primary, when the primary is healthy
	// again.
	DirSwitchCount int64
	// The following durations do not account for continued background writes to
	// a directory that has been switched away from. These background writes can
	// happen because of queued records.

	// PrimaryWriteDuration is the cumulative duration for which WAL writes are
	// using the primary directory.
	PrimaryWriteDuration time.Duration
	// SecondaryWriteDuration is the cumulative duration for which WAL writes
	// are using the secondary directory.
	SecondaryWriteDuration time.Duration

	// FailoverWriteAndSyncLatency measures the latency of writing and syncing a
	// set of writes that were synced together. Each sample represents the
	// highest latency observed across the writes in the set of writes. It gives
	// us a sense of the user-observed latency, which can be much lower than the
	// underlying fsync latency, when WAL failover is working effectively.
	FailoverWriteAndSyncLatency prometheus.Histogram
}

// Manager handles all WAL work.
//
//   - Obsolete can be called concurrently with WAL writing.
//   - WAL writing: Is done via Create, and the various Writer methods. These
//     are required to be serialized via external synchronization (specifically,
//     the caller does it via commitPipeline.mu).
type Manager interface {
	// init initializes the Manager. init is called during DB initialization.
	init(o Options, initial Logs) error

	// List returns the virtual WALs in ascending order. List must not perform
	// I/O.
	List() Logs
	// Obsolete informs the manager that all virtual WALs less than
	// minUnflushedNum are obsolete. The callee can choose to recycle some
	// underlying log files, if !noRecycle. The log files that are not recycled,
	// and therefore can be deleted, are returned. The deletable files are no
	// longer tracked by the manager.
	Obsolete(minUnflushedNum NumWAL, noRecycle bool) (toDelete []DeletableLog, err error)
	// Create creates a new virtual WAL.
	//
	// NumWALs passed to successive Create calls must be monotonically
	// increasing, and be greater than any NumWAL seen earlier. The caller must
	// close the previous Writer before calling Create.
	//
	// jobID is used for the WALEventListener.
	Create(wn NumWAL, jobID int) (Writer, error)
	// ElevateWriteStallThresholdForFailover returns true if the caller should
	// use a high write stall threshold because the WALs are being written to
	// the secondary dir.
	//
	// In practice, if this value is true, we give an unlimited memory budget
	// for memtables. This is simpler than trying to configure an explicit
	// value, given that memory resources can vary. When using WAL failover in
	// CockroachDB, an OOM risk is worth tolerating for workloads that have a
	// strict latency SLO. Also, an unlimited budget here does not mean that the
	// disk stall in the primary will go unnoticed until the OOM -- CockroachDB
	// is monitoring disk stalls, and we expect it to fail the node after ~60s
	// if the primary is stalled.
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
	ApproxFileSize uint64
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
	//
	// Some Writer implementations may continue to read p after WriteRecord
	// returns. This is an obstacle to reusing p's memory. If the caller would
	// like to reuse p's memory, the caller may pass a non-nil [RefCount].  If
	// the Writer will retain p, it will invoke the [RefCount] before returning.
	// When it's finished, it will invoke [RefCount.Unref] to release its
	// reference.
	WriteRecord(p []byte, opts SyncOptions, ref RefCount) (logicalOffset int64, err error)
	// Close the writer.
	Close() (logicalOffset int64, err error)
	// Metrics must be called after Close. The callee will no longer modify the
	// returned LogWriterMetrics.
	Metrics() record.LogWriterMetrics
}

// RefCount is a reference count associated with a record passed to
// [Writer.WriteRecord]. See the comment on WriteRecord.
type RefCount interface {
	// Ref increments the reference count.
	Ref()
	// Unref increments the reference count.
	Unref()
}

// Reader reads a virtual WAL.
type Reader interface {
	// NextRecord returns a reader for the next record. It returns io.EOF if there
	// are no more records. The reader returned becomes stale after the next NextRecord
	// call, and should no longer be used.
	NextRecord() (io.Reader, Offset, error)
	// Close the reader.
	Close() error
}

// Offset indicates the offset or position of a record within a WAL.
type Offset struct {
	// PhysicalFile is the path to the physical file containing a particular
	// record.
	PhysicalFile string
	// Physical indicates the file offset at which a record begins within
	// the physical file named by PhysicalFile.
	Physical int64
	// PreviousFilesBytes is the bytes read from all the previous physical
	// segment files that have been read up to the current log segment. If WAL
	// failover is not in use, PreviousFileBytes will always be zero. Otherwise,
	// it may be non-zero when replaying records from multiple segment files
	// that make up a single logical WAL.
	PreviousFilesBytes int64
}

// String implements fmt.Stringer, returning a string representation of the
// offset.
func (o Offset) String() string {
	return redact.StringWithoutMarkers(o)
}

// SafeFormat implements redact.SafeFormatter.
func (o Offset) SafeFormat(w redact.SafePrinter, _ rune) {
	if o.PreviousFilesBytes > 0 {
		w.Printf("(%s: %d), %d from previous files", o.PhysicalFile, o.Physical, o.PreviousFilesBytes)
		return
	}
	w.Printf("(%s: %d)", o.PhysicalFile, o.Physical)

}
