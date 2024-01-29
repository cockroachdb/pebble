// Copyright 2024 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package wal

import (
	"cmp"
	"io"
	"os"
	"slices"
	"sync"

	"github.com/cockroachdb/errors"
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

// Init implements Manager.
func (m *StandaloneManager) Init(o Options) error {
	if o.Secondary.FS != nil {
		return errors.AssertionFailedf("cannot create StandaloneManager with a secondary")
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

	ls, err := o.Primary.FS.List(o.Primary.Dirname)
	if err != nil {
		return err
	}
	closeAndReturnErr := func(err error) error {
		err = firstError(err, walDir.Close())
		return err
	}
	var files []base.FileInfo
	for _, filename := range ls {
		ft, fn, ok := base.ParseFilename(o.Primary.FS, filename)
		if !ok || ft != base.FileTypeLog {
			continue
		}
		stat, err := o.Primary.FS.Stat(o.Primary.FS.PathJoin(o.Primary.Dirname, filename))
		if err != nil {
			return closeAndReturnErr(err)
		}
		files = append(files, base.FileInfo{FileNum: fn, FileSize: uint64(stat.Size())})
		if m.recycler.MinRecycleLogNum() <= fn {
			m.recycler.SetMinRecycleLogNum(fn + 1)
		}
	}
	slices.SortFunc(files, func(a, b base.FileInfo) int { return cmp.Compare(a.FileNum, b.FileNum) })
	m.mu.queue = files
	return nil
}

// List implements Manager.
func (m *StandaloneManager) List() ([]NumWAL, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	wals := make([]NumWAL, len(m.mu.queue))
	for i := range m.mu.queue {
		wals[i] = NumWAL(m.mu.queue[i].FileNum)
	}
	return wals, nil
}

// Obsolete implements Manager.
func (m *StandaloneManager) Obsolete(
	minUnflushedNum NumWAL, noRecycle bool,
) (toDelete []DeletableLog, err error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	i := 0
	for ; i < len(m.mu.queue); i++ {
		fi := m.mu.queue[i]
		if fi.FileNum >= base.DiskFileNum(minUnflushedNum) {
			break
		}
		if noRecycle || !m.recycler.Add(fi) {
			toDelete = append(toDelete, DeletableLog{
				FS:       m.o.Primary.FS,
				Path:     m.o.Primary.FS.PathJoin(m.o.Primary.Dirname, base.MakeFilename(base.FileTypeLog, fi.FileNum)),
				NumWAL:   NumWAL(fi.FileNum),
				FileSize: fi.FileSize,
			})
		}
	}
	m.mu.queue = m.mu.queue[i:]
	return toDelete, nil
}

// OpenForRead implements Manager.
func (m *StandaloneManager) OpenForRead(wn NumWAL) (Reader, error) {
	if wn < m.o.MinUnflushedWALNum {
		return nil, errors.AssertionFailedf(
			"attempting to open WAL %d which is earlier than min unflushed %d", wn, m.o.MinUnflushedWALNum)
	}
	var filename string
	m.mu.Lock()
	for i := range m.mu.queue {
		if NumWAL(m.mu.queue[i].FileNum) == wn {
			filename = m.o.Primary.FS.PathJoin(
				m.o.Primary.Dirname, base.MakeFilename(base.FileTypeLog, m.mu.queue[i].FileNum))
			break
		}
	}
	m.mu.Unlock()
	if len(filename) == 0 {
		return nil, errors.AssertionFailedf("attempting to open WAL %d which is unknown", wn)
	}
	file, err := m.o.Primary.FS.Open(filename)
	if err != nil {
		return nil, err
	}
	return &standaloneReader{
		filename: filename,
		rr:       record.NewReader(file, base.DiskFileNum(wn)),
		f:        file,
	}, nil
}

// Create implements Manager.
func (m *StandaloneManager) Create(wn NumWAL, jobID int) (Writer, error) {
	// TODO(sumeer): check monotonicity of wn.
	newLogNum := base.DiskFileNum(wn)
	newLogName :=
		base.MakeFilepath(m.o.Primary.FS, m.o.Primary.Dirname, base.FileTypeLog, base.DiskFileNum(wn))

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
		recycleLogName := base.MakeFilepath(
			m.o.Primary.FS, m.o.Primary.Dirname, base.FileTypeLog, recycleLog.FileNum)
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
		WALFsyncLatency:    m.o.FsyncLatency,
		WALMinSyncInterval: m.o.MinSyncInterval,
		QueueSemChan:       m.o.QueueSemChan,
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

// ListFiles implements Manager.
func (m *StandaloneManager) ListFiles(wn NumWAL) (files []CopyableLog, err error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	for _, fi := range m.mu.queue {
		if NumWAL(fi.FileNum) == wn {
			return []CopyableLog{{
				FS:   m.o.Primary.FS,
				Path: m.o.Primary.FS.PathJoin(m.o.Primary.Dirname, base.MakeFilename(base.FileTypeLog, fi.FileNum)),
			}}, nil
		}
	}
	return nil, errors.Errorf("WAL %d not found", wn)
}

// ElevateWriteStallThresholdForFailover implements Manager.
func (m *StandaloneManager) ElevateWriteStallThresholdForFailover() bool {
	return false
}

// Stats implements Manager.
func (m *StandaloneManager) Stats() Stats {
	recycledLogsCount, recycledLogSize := m.recycler.Stats()
	m.mu.Lock()
	defer m.mu.Unlock()
	var fileSize uint64
	for i := range m.mu.queue {
		fileSize += m.mu.queue[i].FileSize
	}
	return Stats{
		ObsoleteFileCount: recycledLogsCount,
		ObsoleteFileSize:  recycledLogSize,
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
	return firstError(err, m.walDir.Close())
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

type standaloneReader struct {
	filename string
	rr       *record.Reader
	f        vfs.File
}

var _ Reader = &standaloneReader{}

// NextRecord implements Reader.
func (r *standaloneReader) NextRecord() (io.Reader, Offset, error) {
	record, err := r.rr.Next()
	off := Offset{
		PhysicalFile: r.filename,
		Physical:     r.rr.Offset(),
	}
	return record, off, err
}

// Close implements Reader.
func (r *standaloneReader) Close() error {
	return r.f.Close()
}

type standaloneWriter struct {
	m *StandaloneManager
	w *record.LogWriter
}

var _ Writer = &standaloneWriter{}

// WriteRecord implements Writer.
func (w *standaloneWriter) WriteRecord(
	p []byte, opts SyncOptions,
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
func (w *standaloneWriter) Metrics() *record.LogWriterMetrics {
	return w.w.Metrics()
}
