package replay

import (
	"sync"

	"github.com/cockroachdb/pebble"
	"github.com/cockroachdb/pebble/internal/base"
	"github.com/cockroachdb/pebble/vfs"
)

// WorkloadCollector is a cleaner that is designed to capture a workload by
// handling flushed and ingested SSTs. The cleaner only deletes obsolete files
// after they have been processed by the fileHandler.
type WorkloadCollector struct {
	// The fileHandler performs the actual work of capturing the workload. The
	// fileHandler is run in the OnFlushEnd and OnTableIngested which are supposed
	// to be hooked up to the respective EventListener events for TableIngested
	// and FlushEnded.
	*sync.Mutex
	fileHandler        WorkloadCollectorFileHandler
	fileState          map[string]workloadCaptureState
	filesToProcess     []string
	fs                 vfs.FS
	storageDir         string
	notifier           *sync.Cond
	shouldStopNotifier bool // TODO(leon): stop.Stopper? in CRDB?
}

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

// NewWorkloadCaptureCleaner is used externally to create a New WorkloadCollector.
func NewWorkloadCaptureCleaner(
	fs vfs.FS, dirname string, fileHandler WorkloadCollectorFileHandler,
) WorkloadCollector {
	wc := WorkloadCollector{
		Mutex:       &sync.Mutex{},
		fileHandler: fileHandler,
		fileState:   make(map[string]workloadCaptureState),
		fs:          fs,
		storageDir:  dirname,
	}
	wc.notifier = sync.NewCond(wc.Mutex)
	return wc
}

// Clean deletes files only after they have been processed.
func (w *WorkloadCollector) Clean(_ vfs.FS, _ base.FileType, path string) error {
	w.Lock()
	defer w.Unlock()

	fileState := w.fileState[path]
	if fileState.has(capturedSuccessfully) {
		fileState |= obsolete
		err := w.deleteFile(path)
		return err
	}
	return nil
}

func (w *WorkloadCollector) deleteFile(path string) error {
	err := w.fs.Remove(path)
	if err == nil {
		w.fileState[path] |= deletedSuccessfully
		delete(w.fileState, path)
	}
	return err
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
	w.notifier.Signal()
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
	w.notifier.Signal()
}

// setFileAsReadyForProcessing calls the handler for the file and marks it as processed.
// Must be called while holding a write lock.
func (w *WorkloadCollector) setFileAsReadyForProcessing(fileNum base.FileNum) {
	filepath := base.MakeFilepath(w.fs, w.storageDir, base.FileTypeTable, fileNum)
	w.fileState[filepath] |= readyForProcessing
	w.filesToProcess = append(w.filesToProcess, filepath)
}

// StartCollectorFileListener starts a go routine that listens for new files that
// need to be collected.
func (w *WorkloadCollector) StartCollectorFileListener() {
	go w.filesToProcessWatcher()
}

// filesToProcessWatcher is wrapper over waitAndProcessFile that runs indefinitely
func (w *WorkloadCollector) filesToProcessWatcher() {
	for w.waitAndProcessFile() {
	}
}

// StopCollectorFileListener stops the go routine that listens for new files
// that need to be collected
func (w *WorkloadCollector) StopCollectorFileListener() {
	w.Lock()
	defer w.Unlock()
	w.shouldStopNotifier = true
	w.notifier.Signal()
}

// waitAndProcessFile is a function that performs the workload capture. It
// waits on a condition variable (notifier) to let it know when new files are
// available to be collected at which point it runs the fileHandler on each file
// that is marked as ready for processing (readyForProcessing). After processing, if
// the file has already been marked as obsolete, the file will be deleted.
func (w *WorkloadCollector) waitAndProcessFile() bool {
	w.Lock()
	if len(w.filesToProcess) == 0 {
		w.notifier.Wait()
	}
	if w.shouldStopNotifier {
		return false
	}
	for _, filepath := range w.filesToProcess {
		err := w.fileHandler.HandleFile(w.fs, filepath)
		if err != nil {
			// TODO(leon): How should this error be handled?
			return false
		}
		if w.fileState[filepath].has(obsolete) {
			err := w.deleteFile(filepath)
			if err != nil {
				// TODO(leon): How should this error be handled?
				return false
			}
		}
		w.fileState[filepath] |= capturedSuccessfully
	}
	w.filesToProcess = w.filesToProcess[:0]
	w.Unlock()
	return true
}

const (
	obsolete = workloadCaptureState(1) << iota
	readyForProcessing
	capturedSuccessfully
	deletedSuccessfully
)

type workloadCaptureState uint16

func (wcs workloadCaptureState) has(flag workloadCaptureState) bool { return wcs&flag != 0 }
