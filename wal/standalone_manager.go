// Copyright 2024 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package wal

import (
	"os"
	"slices"
	"sync"

	"github.com/cockroachdb/pebble/internal/base"
	"github.com/cockroachdb/pebble/record"
	"github.com/cockroachdb/pebble/vfs"
)

// StandaloneManager implements Manager with a single log file per WAL (no
// failover capability).
type StandaloneManager struct {
	o        Options
	recycler LogRecycler
	walDir   vfs.File
	// initialObsolete holds the set of DeletableLogs that formed the logs
	// passed into Init. The initialObsolete logs are all obsolete. Once
	// returned via Manager.Obsolete, initialObsolete is cleared. The
	// initialObsolete logs are stored separately from mu.queue because they may
	// include logs that were NOT created by the standalone manager, and
	// multiple physical log files may form one logical WAL.
	initialObsolete []DeletableLog

	// External synchronization is relied on when accessing w in Manager.Create,
	// Writer.{WriteRecord,Close}.
	w *standaloneWriter

	mu struct {
		sync.Mutex
		// The queue of WALs, containing both flushed and unflushed WALs. The
		// FileInfo.FileNum is also the NumWAL, since there is one log file for
		// each WAL. The flushed logs are a prefix, the unflushed logs a suffix.
		// If w != nil, the last entry here is that active WAL. For the active
		// log, FileInfo.FileSize is the size when it was opened and can be
		// greater than zero because of log recycling.
		queue []base.FileInfo
	}
}

var _ Manager = &StandaloneManager{}

// init implements Manager.
func (m *StandaloneManager) init(o Options, initial Logs) error {
	if o.Secondary.FS != nil {
		return base.AssertionFailedf("cannot create StandaloneManager with a secondary")
	}
	var err error
	var walDir vfs.File
	if walDir, err = o.Primary.FS.OpenDir(o.Primary.Dirname); err != nil {
		return err
	}
	*m = StandaloneManager{
		o:      o,
		walDir: walDir,
	}
	m.recycler.Init(o.MaxNumRecyclableLogs)

	closeAndReturnErr := func(err error) error {
		err = firstError(err, walDir.Close())
		return err
	}
	for _, ll := range initial {
		if m.recycler.MinRecycleLogNum() <= ll.Num {
			m.recycler.SetMinRecycleLogNum(ll.Num + 1)
		}
		m.initialObsolete, err = appendDeletableLogs(m.initialObsolete, ll)
		if err != nil {
			return closeAndReturnErr(err)
		}
	}
	return nil
}

// List implements Manager.
func (m *StandaloneManager) List() Logs {
	m.mu.Lock()
	defer m.mu.Unlock()
	wals := make(Logs, len(m.mu.queue))
	for i := range m.mu.queue {
		wals[i] = LogicalLog{
			Num:      NumWAL(m.mu.queue[i].FileNum),
			segments: []segment{{dir: m.o.Primary}},
		}
	}
	return wals
}

// Obsolete implements Manager.
func (m *StandaloneManager) Obsolete(
	minUnflushedNum NumWAL, noRecycle bool,
) (toDelete []DeletableLog, err error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	// If the DB was recently opened, we may have deletable logs outside the
	// queue.
	m.initialObsolete = slices.DeleteFunc(m.initialObsolete, func(dl DeletableLog) bool {
		if dl.NumWAL >= minUnflushedNum {
			return false
		}
		toDelete = append(toDelete, dl)
		return true
	})

	i := 0
	for ; i < len(m.mu.queue); i++ {
		fi := m.mu.queue[i]
		if fi.FileNum >= base.DiskFileNum(minUnflushedNum) {
			break
		}
		if noRecycle || !m.recycler.Add(fi) {
			toDelete = append(toDelete, DeletableLog{
				FS:             m.o.Primary.FS,
				Path:           m.o.Primary.FS.PathJoin(m.o.Primary.Dirname, makeLogFilename(NumWAL(fi.FileNum), 000)),
				NumWAL:         NumWAL(fi.FileNum),
				ApproxFileSize: fi.FileSize,
			})
		}
	}
	m.mu.queue = m.mu.queue[i:]
	return toDelete, nil
}

// Create implements Manager.
func (m *StandaloneManager) Create(wn NumWAL, jobID int) (Writer, error) {
	// TODO(sumeer): check monotonicity of wn.
	newLogNum := base.DiskFileNum(wn)
	newLogName := m.o.Primary.FS.PathJoin(m.o.Primary.Dirname, makeLogFilename(wn, 0))

	// Try to use a recycled log file. Recycling log files is an important
	// performance optimization as it is faster to sync a file that has
	// already been written, than one which is being written for the first
	// time. This is due to the need to sync file metadata when a file is
	// being written for the first time. Note this is true even if file
	// preallocation is performed (e.g. fallocate).
	var recycleLog base.FileInfo
	var recycleOK bool
	var newLogFile vfs.File
	var err error
	recycleLog, recycleOK = m.recycler.Peek()
	if recycleOK {
		recycleLogName := m.o.Primary.FS.PathJoin(m.o.Primary.Dirname, makeLogFilename(NumWAL(recycleLog.FileNum), 0))
		newLogFile, err = m.o.Primary.FS.ReuseForWrite(recycleLogName, newLogName, "pebble-wal")
		base.MustExist(m.o.Primary.FS, newLogName, m.o.Logger, err)
	} else {
		newLogFile, err = m.o.Primary.FS.Create(newLogName, "pebble-wal")
		base.MustExist(m.o.Primary.FS, newLogName, m.o.Logger, err)
	}
	createInfo := CreateInfo{
		JobID:           jobID,
		Path:            newLogName,
		IsSecondary:     false,
		Num:             wn,
		RecycledFileNum: recycleLog.FileNum,
		Err:             nil,
	}
	defer func() {
		createInfo.Err = err
		m.o.EventListener.LogCreated(createInfo)
	}()

	if err != nil {
		return nil, err
	}
	var newLogSize uint64
	if recycleOK {
		// Figure out the recycled WAL size. This Stat is necessary
		// because ReuseForWrite's contract allows for removing the
		// old file and creating a new one. We don't know whether the
		// WAL was actually recycled.
		// TODO(jackson): Adding a boolean to the ReuseForWrite return
		// value indicating whether or not the file was actually
		// reused would allow us to skip the stat and use
		// recycleLog.FileSize.
		var finfo os.FileInfo
		finfo, err = newLogFile.Stat()
		if err == nil {
			newLogSize = uint64(finfo.Size())
		}
		err = firstError(err, m.recycler.Pop(recycleLog.FileNum))
		if err != nil {
			return nil, firstError(err, newLogFile.Close())
		}
	}
	// TODO(peter): RocksDB delays sync of the parent directory until the
	// first time the log is synced. Is that worthwhile?
	if err = m.walDir.Sync(); err != nil {
		err = firstError(err, newLogFile.Close())
		return nil, err
	}
	newLogFile = vfs.NewSyncingFile(newLogFile, vfs.SyncingFileOptions{
		NoSyncOnClose:   m.o.NoSyncOnClose,
		BytesPerSync:    m.o.BytesPerSync,
		PreallocateSize: m.o.PreallocateSize(),
	})
	w := record.NewLogWriter(newLogFile, newLogNum, record.LogWriterConfig{
		WALFsyncLatency:     m.o.FsyncLatency,
		WALMinSyncInterval:  m.o.MinSyncInterval,
		QueueSemChan:        m.o.QueueSemChan,
		WriteWALSyncOffsets: m.o.WriteWALSyncOffsets,
	})
	m.w = &standaloneWriter{
		m: m,
		w: w,
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	m.mu.queue = append(m.mu.queue, base.FileInfo{FileNum: newLogNum, FileSize: newLogSize})
	return m.w, nil
}

// ElevateWriteStallThresholdForFailover implements Manager.
func (m *StandaloneManager) ElevateWriteStallThresholdForFailover() bool {
	return false
}

// Stats implements Manager.
func (m *StandaloneManager) Stats() Stats {
	obsoleteLogsCount, obsoleteLogSize := m.recycler.Stats()
	m.mu.Lock()
	defer m.mu.Unlock()
	var fileSize uint64
	for i := range m.mu.queue {
		fileSize += m.mu.queue[i].FileSize
	}
	for i := range m.initialObsolete {
		if i == 0 || m.initialObsolete[i].NumWAL != m.initialObsolete[i-1].NumWAL {
			obsoleteLogsCount++
		}
		obsoleteLogSize += m.initialObsolete[i].ApproxFileSize
	}
	return Stats{
		ObsoleteFileCount: obsoleteLogsCount,
		ObsoleteFileSize:  obsoleteLogSize,
		LiveFileCount:     len(m.mu.queue),
		LiveFileSize:      fileSize,
	}
}

// Close implements Manager.
func (m *StandaloneManager) Close() error {
	var err error
	if m.w != nil {
		_, err = m.w.Close()
	}
	err = firstError(err, m.walDir.Close())
	if m.o.Primary.Lock != nil {
		err = firstError(err, m.o.Primary.Lock.Close())
	}
	return err
}

// Opts implements Manager.
func (m *StandaloneManager) Opts() Options {
	return m.o
}

// RecyclerForTesting implements Manager.
func (m *StandaloneManager) RecyclerForTesting() *LogRecycler {
	return &m.recycler
}

// firstError returns the first non-nil error of err0 and err1, or nil if both
// are nil.
func firstError(err0, err1 error) error {
	if err0 != nil {
		return err0
	}
	return err1
}

type standaloneWriter struct {
	m *StandaloneManager
	w *record.LogWriter
}

var _ Writer = &standaloneWriter{}

// WriteRecord implements Writer.
func (w *standaloneWriter) WriteRecord(
	p []byte, opts SyncOptions, _ RefCount,
) (logicalOffset int64, err error) {
	return w.w.SyncRecord(p, opts.Done, opts.Err)
}

// Close implements Writer.
func (w *standaloneWriter) Close() (logicalOffset int64, err error) {
	logicalOffset = w.w.Size()
	// Close the log. This writes an EOF trailer signifying the end of the file
	// and syncs it to disk. The caller must close the previous log before
	// creating the new log file, otherwise a crash could leave both logs with
	// unclean tails, and DB.Open will treat the previous log as corrupt.
	err = w.w.Close()
	w.m.mu.Lock()
	defer w.m.mu.Unlock()
	i := len(w.m.mu.queue) - 1
	// The log may have grown past its original physical size. Update its file
	// size in the queue so we have a proper accounting of its file size.
	if w.m.mu.queue[i].FileSize < uint64(logicalOffset) {
		w.m.mu.queue[i].FileSize = uint64(logicalOffset)
	}
	w.m.w = nil
	return logicalOffset, err
}

// Metrics implements Writer.
func (w *standaloneWriter) Metrics() record.LogWriterMetrics {
	return w.w.Metrics()
}
