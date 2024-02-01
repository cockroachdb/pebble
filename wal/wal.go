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
// (using a dense logIndex).
//
// In general, WAL refers to the virtual WAL, and file refers to a log file.
// The Pebble MANIFEST only knows about virtual WALs and assigns numbers to
// them. Additional mapping to one or more files happens in this package. If a
// WAL maps to multiple files, the source of truth regarding that mapping is
// the contents of the directories.
type NumWAL base.DiskFileNum

// String implements fmt.Stringer.
func (s NumWAL) String() string { return base.DiskFileNum(s).String() }

// logIndex numbers log files within a WAL.
type logIndex uint32

func (li logIndex) String() string {
	return fmt.Sprintf("%03d", li)
}

// TODO(sumeer): Remove attempts to parse log files outside
// the wal package (including tools).

// makeLogFilename makes a log filename.
func makeLogFilename(wn NumWAL, index logIndex) string {
	if index == 0 {
		// Use a backward compatible name, for simplicity.
		return base.MakeFilename(base.FileTypeLog, base.DiskFileNum(wn))
	}
	return fmt.Sprintf("%s-%03d.log", base.DiskFileNum(wn).String(), index)
}

// parseLogFilename takes a base filename and parses it into its constituent
// NumWAL and logIndex. If the filename is not a log file, it returns false for
// the final return value.
func parseLogFilename(name string) (NumWAL, logIndex, bool) {
	i := strings.IndexByte(name, '.')
	if i < 0 || name[i:] != ".log" {
		return 0, 0, false
	}
	j := strings.IndexByte(name[:i], '-')
	if j < 0 {
		dfn, ok := base.ParseDiskFileNum(name[:i])
		if !ok {
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
	return NumWAL(dfn), logIndex(li), true
}

// Options provides configuration for the Manager.
type Options struct {
	// Primary dir for storing WAL files.
	Primary Dir
	// Secondary is used for failover. Optional.
	Secondary Dir

	// Recyling configuration. Only files in the primary dir are recycled.

	// MinRecycleLogNum is the minimum log file number that is allowed to be
	// recycled. Log file numbers smaller than this will be deleted. This is
	// used to prevent recycling a log written by a previous instance of the DB
	// which may not have had log recycling enabled.
	MinRecycleLogNum base.DiskFileNum
	// MaxNumRecyclableLogs is the maximum number of log files to maintain for
	// recycling.
	MaxNumRecyclableLogs int

	// SyncingFileOptions is the configuration when calling vfs.NewSyncingFile.
	SyncingFileOpts vfs.SyncingFileOptions

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
}

// Stats exposes stats used in Pebble metrics.
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
	LiveFileSize uint64
	// LiveSize is the total size of the live data in log files.
	LiveSize uint64
}

// Manager handles all WAL work.
//
// It is an interface for now, but if we end up with a single implementation
// for both standalone mode and failover mode, we will get rid of the
// interface.
type Manager interface {
	// Init initializes the Manager.
	//
	// Implementation notes:
	// - lists and stats the directories, so that Stats are up to date (assuming
	//   no obsolete files yet), and the list of WALs and their constituent log
	//   files is initialized.
	// - ensures dirs are created and synced.
	Init(o Options) error
	// List returns the virtual WALs in ascending order.
	List() ([]NumWAL, error)
	// Delete deletes all virtual WALs up to highestObsoleteNum. The
	// underlying physical WAL files may be recycled.
	Delete(highestObsoleteNum NumWAL) error
	// OpenForRead opens a virtual WAL for read.
	OpenForRead(wn NumWAL, strictWALTail bool) (Reader, error)
	// Create creates a new virtual WAL.
	//
	// NumWALs passed to successive Create calls must be monotonically
	// increasing, and be greater than any NumWAL seen earlier. The caller must
	// close the previous Writer before calling Create.
	Create(wn NumWAL) (Writer, error)
	// Stats returns the latest Stats.
	Stats() Stats
	// Close the manager.
	Close() error
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
	// Size based on writes.
	Size() uint64
	// FileSize is the size of the file(s) underlying this WAL. FileSize
	// >= Size because of recycling and failover. This is an estimate.
	FileSize() uint64
	// WriteRecord writes a complete record. The record is asynchronously
	// persisted to the underlying writer. If SyncOptions.Done != nil, the wait
	// group will be notified when durability is guaranteed or an error has
	// occurred (set in SyncOptions.Err). External synchronisation provided by
	// commitPipeline.mu guarantees that WriteRecord calls are serialized.
	WriteRecord(p []byte, opts SyncOptions) error
	// Close the writer.
	Close() error
	// Metrics must be called after Close. The callee will no longer modify the
	// returned LogWriterMetrics.
	Metrics() *record.LogWriterMetrics
}

// Reader reads a virtual WAL.
type Reader interface {
	// NextRecord returns a reader for the next record. It returns io.EOF if there
	// are no more records. The reader returned becomes stale after the next Next
	// call, and should no longer be used.
	NextRecord() (io.Reader, Offset, error)
	// Close the reader.
	Close() error
}

// Offset indicates the offset or position of a record within a WAL.
type Offset struct {
	// Logical is a monotonically increasing offset within a virtual WAL
	// indicating the position of a record. Logical is deterministic with
	// respect to the contents of the records.
	Logical int
	// PhysicalFile is the path to the physical file containing a particular
	// record.
	PhysicalFile string
	// Physical indicates the file offset at which a record begins within
	// the physical file named by PhysicalFile.
	Physical int64
}

// String implements fmt.Stringer, returning a string representation of the
// offset.
func (o Offset) String() string {
	return fmt.Sprintf("%d (%s: %d)", o.Logical, o.PhysicalFile, o.Physical)
}

// Make lint happy.
var _ logIndex = 0
var _ = makeLogFilename
