package replay

import (
	"sync"

	"github.com/cockroachdb/pebble"
	"github.com/cockroachdb/pebble/internal/base"
	"github.com/cockroachdb/pebble/vfs"
)

type workloadCaptureState uint8

const (
	obsolete = workloadCaptureState(1) << iota
	readyForProcessing
	capturedSuccessfully
)

func (wcs workloadCaptureState) is(flag workloadCaptureState) bool { return wcs&flag != 0 }

// WorkloadCollectorFileHandler is an interface that allows anyone to write their
// own file handler to perform the workload capturing.
type WorkloadCollectorFileHandler interface {
	HandleFile(fs vfs.FS, path string) error
}

// DefaultWorkloadCollectorFileHandler is a default workload capture tool that
// copies files over to the archive directory.
type DefaultWorkloadCollectorFileHandler struct {
	DestinationDirectory string
}

// HandleFile is similar to the ArchiveCleaner's Clean except that it copies
// files over to the archive directory instead of moving them.
func (wcc DefaultWorkloadCollectorFileHandler) HandleFile(fs vfs.FS, path string) error {
	if err := fs.MkdirAll(wcc.DestinationDirectory, 0755); err != nil {
		return err
	}
	destPath := fs.PathJoin(wcc.DestinationDirectory, fs.PathBase(path))
	return vfs.Copy(fs, path, destPath)
}

// WorkloadCollector is a cleaner that is designed to capture a workload by
// handling flushed and ingested SSTs. The cleaner only deletes obsolete files
// after they have been processed by the fileHandler.
type WorkloadCollector struct {
	// The fileHandler performs the actual work of capturing the workload. The
	// fileHandler is run in the OnFlushEnd and OnTableIngested which are supposed
	// to be hooked up to the respective EventListener events for TableIngested
	// and FlushEnded.
	sync.Mutex

	fileState      map[string]workloadCaptureState
	filesToProcess []string

	configuration struct {
		fileHandler WorkloadCollectorFileHandler
		fs          vfs.FS
		storageDir  string
	}

	fileListener struct {
		sync.Cond
		stopFileListener bool
	}
}

// NewWorkloadCaptureCleaner is used externally to create a New WorkloadCollector.
func NewWorkloadCaptureCleaner(
	fs vfs.FS, dirname string, fileHandler WorkloadCollectorFileHandler,
) *WorkloadCollector {
	wc := &WorkloadCollector{}
	wc.configuration.fileHandler = fileHandler
	wc.configuration.fs = fs
	wc.configuration.storageDir = dirname
	wc.fileState = make(map[string]workloadCaptureState)
	wc.fileListener.Cond.L = &wc.Mutex
	return wc
}

// setFileAsReadyForProcessing calls the handler for the file and marks it as processed.
// Must be called while holding a write lock.
func (w *WorkloadCollector) setFileAsReadyForProcessing(fileNum base.FileNum) {
	filepath := base.MakeFilepath(w.configuration.fs, w.configuration.storageDir, base.FileTypeTable, fileNum)
	w.fileState[filepath] |= readyForProcessing
	w.filesToProcess = append(w.filesToProcess, filepath)
}

// deleteFile deletes the specified path and removes the path from the fileState map
func (w *WorkloadCollector) deleteFile(path string) error {
	err := w.configuration.fs.Remove(path)
	if err == nil {
		delete(w.fileState, path)
	}
	return err
}

// Clean deletes files only after they have been processed.
func (w *WorkloadCollector) Clean(_ vfs.FS, _ base.FileType, path string) error {
	w.Lock()
	fileState := w.fileState[path]
	if fileState.is(capturedSuccessfully) {
		w.Unlock()
		err := w.deleteFile(path)
		return err
	}
	w.fileState[path] |= obsolete
	w.Unlock()
	return nil
}

// OnTableIngest is a handler that is to be setup on a EventListener and triggered
// by EventListener.TableIngested calls. It runs through the tables and processes
// them by calling setFileAsReadyForProcessing.
func (w *WorkloadCollector) OnTableIngest(info pebble.TableIngestInfo) {
	w.Lock()
	defer w.Unlock()
	for _, table := range info.Tables {
		w.setFileAsReadyForProcessing(table.FileNum)
	}
	w.fileListener.Signal()
}

// OnFlushEnd is a handler that is to be setup on a EventListener and triggered
// by EventListener.FlushEnd calls. It runs through the tables and processes
// them by calling setFileAsReadyForProcessing.
func (w *WorkloadCollector) OnFlushEnd(info pebble.FlushInfo) {
	w.Lock()
	defer w.Unlock()
	for _, table := range info.Output {
		w.setFileAsReadyForProcessing(table.FileNum)
	}
	w.fileListener.Signal()
}

// waitAndProcessFile is a function that performs the workload capture. It
// waits on a condition variable (notifier) to let it know when new files are
// available to be collected at which point it runs the fileHandler on each file
// that is marked as ready for processing (readyForProcessing). After processing, if
// the file has already been marked as obsolete, the file will be deleted.
func (w *WorkloadCollector) waitAndProcessFile() bool {
	w.Lock()
	if len(w.filesToProcess) == 0 {
		w.fileListener.Wait()
	}
	if w.fileListener.stopFileListener {
		w.Unlock()
		return false
	}
	for _, filepath := range w.filesToProcess {
		err := w.configuration.fileHandler.HandleFile(w.configuration.fs, filepath)
		if err != nil {
			// TODO(leon): How should this error be handled?
			return false
		}
		if w.fileState[filepath].is(obsolete) {
			err := w.deleteFile(filepath)
			if err != nil {
				// TODO(leon): How should this error be handled?
				return false
			}
		} else {
			w.fileState[filepath] |= capturedSuccessfully
		}
	}
	w.filesToProcess = w.filesToProcess[:0]
	w.Unlock()
	return true
}

// filesToProcessWatcher is wrapper over waitAndProcessFile that runs indefinitely
func (w *WorkloadCollector) filesToProcessWatcher() {
	for w.waitAndProcessFile() {
	}
}

// StartCollectorFileListener starts a go routine that listens for new files that
// need to be collected.
func (w *WorkloadCollector) StartCollectorFileListener() {
	go w.filesToProcessWatcher()
}

// StopCollectorFileListener stops the go routine that listens for new files
// that need to be collected
func (w *WorkloadCollector) StopCollectorFileListener() {
	w.Lock()
	defer w.Unlock()
	w.fileListener.stopFileListener = true
	w.fileListener.Signal()
}
