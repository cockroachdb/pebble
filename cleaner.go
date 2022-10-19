// Copyright 2019 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package pebble

import (
	"sync"

	"github.com/cockroachdb/pebble/internal/base"
	"github.com/cockroachdb/pebble/vfs"
)

// Cleaner exports the base.Cleaner type.
type Cleaner = base.Cleaner

// DeleteCleaner exports the base.DeleteCleaner type.
type DeleteCleaner = base.DeleteCleaner

// ArchiveCleaner exports the base.ArchiveCleaner type.
type ArchiveCleaner = base.ArchiveCleaner

// WorkloadCaptureCleaner is a cleaner that is designed to capture a workload by
// handling flushed and ingested SSTs. The cleaner only deletes obselete files
// after they have been processed by the fileHandler.
type WorkloadCaptureCleaner struct {
	*sync.RWMutex
	// The fileHandler performs the actual work of capturing the workload. The
	// fileHandler is run in the OnFlushEnd and OnTableIngested which are supposed
	// to be hooked up to the respective EventListener events for TableIngested
	// and FlushEnded.
	fileHandler   WorkloadCaptureFileHandler
	fileProcessed map[string]bool
	fs            vfs.FS
	storageDir    string
}

// WorkloadCaptureFileHandler is an interface that allows anyone to write their
// own file handler to perform the workload capturing.
type WorkloadCaptureFileHandler interface {
	HandleFile(fs vfs.FS, path string) error
}

// DefaultWorkloadCaptureFileHandler is a default workload capture tool that
// copies files over to the archive directory.
type DefaultWorkloadCaptureFileHandler struct{}

// HandleFile is similar to the ArchiveCleaner's Clean except that it copies
// files over to the archive directory instead of moving them.
func (wcc DefaultWorkloadCaptureFileHandler) HandleFile(fs vfs.FS, path string) error {
	destDir := fs.PathJoin(fs.PathDir(path), "archive")
	if err := fs.MkdirAll(destDir, 0755); err != nil {
		return err
	}
	destPath := fs.PathJoin(destDir, fs.PathBase(path))
	return vfs.Copy(fs, path, destPath)
}

// NewWorkloadCaptureCleaner is used externally to create a New WorkloadCaptureCleaner.
func NewWorkloadCaptureCleaner(
	fs vfs.FS, dirname string, fileHandler WorkloadCaptureFileHandler,
) WorkloadCaptureCleaner {
	return WorkloadCaptureCleaner{
		RWMutex:       &sync.RWMutex{},
		fileHandler:   fileHandler,
		fileProcessed: make(map[string]bool),
		fs:            fs,
		storageDir:    dirname,
	}
}

// Clean deletes files only after they have been processed.
func (w WorkloadCaptureCleaner) Clean(fs vfs.FS, _ base.FileType, path string) error {
	w.RLock()
	defer w.RUnlock()
	if w.fileProcessed[path] {
		delete(w.fileProcessed, path)
		return fs.Remove(path)
	}
	return nil
}

// OnTableIngest is a handler that is to be setup on a EventListener and triggered
// by EventListener.TableIngested calls. It runs through the tables and processes
// them by calling processFileByNum.
func (w WorkloadCaptureCleaner) OnTableIngest(info TableIngestInfo) {
	w.Lock()
	defer w.Unlock()
	for _, table := range info.Tables {
		w.processFileByNum(table.FileNum)
	}
}

// OnFlushEnd is a handler that is to be setup on a EventListener and triggered
// by EventListener.FlushEnd calls. It runs through the tables and processes
// them by calling processFileByNum.
func (w WorkloadCaptureCleaner) OnFlushEnd(info FlushInfo) {
	w.Lock()
	defer w.Unlock()
	for _, table := range info.Output {
		w.processFileByNum(table.FileNum)
	}
}

// processFileByNum calls the handler for the file and marks it as processed.
// Must be called while holding a write lock.
func (w WorkloadCaptureCleaner) processFileByNum(fileNum base.FileNum) {
	filepath := base.MakeFilepath(w.fs, w.storageDir, fileTypeTable, fileNum)
	err := w.fileHandler.HandleFile(w.fs, filepath)
	if err != nil {
		// TODO(leon): How should this error be handled?
		return
	}
	w.fileProcessed[filepath] = true
}
