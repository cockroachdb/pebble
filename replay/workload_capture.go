package replay

import (
	"io"
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
	HandleTableFile(fs vfs.FS, path string) error
	HandleManifestFileMutated(fs vfs.FS, details workloadCollectorManifestDetails) (int64, error)
	OutputDirectory() string
}

// DefaultWorkloadCollectorFileHandler is a default workload capture tool that
// copies files over to the archive directory.
type DefaultWorkloadCollectorFileHandler struct {
	destinationDirectory string
	buffer               []byte
}

// NewDefaultWorkloadCollectorFileHandler creates a DefaultWorkloadCollectorFileHandler
func NewDefaultWorkloadCollectorFileHandler(
	captureDestinationDirectory string,
) DefaultWorkloadCollectorFileHandler {
	return DefaultWorkloadCollectorFileHandler{
		destinationDirectory: captureDestinationDirectory,
		buffer:               make([]byte, 1024 /* 1KB */),
	}
}

// HandleTableFile is similar to the ArchiveCleaner's Clean except that it copies
// files over to the archive directory instead of moving them.
func (wcc DefaultWorkloadCollectorFileHandler) HandleTableFile(fs vfs.FS, path string) error {
	if err := fs.MkdirAll(wcc.destinationDirectory, 0755); err != nil {
		return err
	}
	destPath := fs.PathJoin(wcc.destinationDirectory, fs.PathBase(path))
	return vfs.Copy(fs, path, destPath)
}

// OutputDirectory returns the output directory that is to be used for collecting the necessary files
func (wcc DefaultWorkloadCollectorFileHandler) OutputDirectory() string {
	return wcc.destinationDirectory
}

// HandleManifestFileMutated processes the manifest file after it has been
// mutated. It reads from the source manifest file at a byte offset and copies
// the newly added data to the source manifest file to the destination manifest
// file.
func (wcc DefaultWorkloadCollectorFileHandler) HandleManifestFileMutated(
	_ vfs.FS, manifestDetails workloadCollectorManifestDetails,
) (numBytes int64, err error) {
	return io.CopyBuffer(manifestDetails.destFile, manifestDetails.sourceFile, wcc.buffer)
}

type workloadCollectorManifestDetails struct {
	sourceFilepath string
	sourceFile     vfs.File

	destFilepath string
	destFile     vfs.File
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

	manifest      []workloadCollectorManifestDetails
	manifestIndex int

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
	fs vfs.FS, captureFromDirectory string, fileHandler WorkloadCollectorFileHandler,
) *WorkloadCollector {
	wc := &WorkloadCollector{}
	wc.configuration.fileHandler = fileHandler
	wc.configuration.fs = fs
	wc.configuration.storageDir = captureFromDirectory
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
		w.Lock()
		delete(w.fileState, path)
		w.Unlock()
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

// OnManifestCreated is a handler that is to be setup on a EventListener and
// triggered by EventListener.ManifestCreated calls. It opens the newly created
// manifest file and appends it to a list of manifest files to process.
func (w *WorkloadCollector) OnManifestCreated(info pebble.ManifestCreateInfo) {
	w.Lock()
	defer w.Unlock()
	manifest := workloadCollectorManifestDetails{
		sourceFilepath: info.Path,
	}
	sourceFile, err := w.configuration.fs.Open(info.Path)
	if err != nil {
		panic(err)
	}
	manifest.sourceFile = sourceFile
	w.manifest = append(w.manifest, manifest)
}

// filesToProcessWatcher is wrapper over processFiles that runs indefinitely
func (w *WorkloadCollector) filesToProcessWatcher() {
	w.Lock()
	for !w.fileListener.stopFileListener {
		// The following performs the workload capture. It waits on a condition
		// variable (fileListener) to let it know when new files are available to be
		// collected at which point it runs the fileHandler on each file that is
		// marked as ready for processing (readyForProcessing). After processing, if
		// the file has already been marked as obsolete, the file will
		// be deleted. The manifests are also processed in this section by calling
		// the manifest fileHandler
		if len(w.filesToProcess) == 0 {
			w.fileListener.Wait()
		}
		filesToProcess := w.filesToProcess[:]
		w.filesToProcess = w.filesToProcess[:0]
		w.Unlock()

		// Handle Tables
		for _, filepath := range filesToProcess {
			err := w.configuration.fileHandler.HandleTableFile(w.configuration.fs, filepath)
			if err != nil {
				panic(err)
			}
			w.Lock()
			if w.fileState[filepath].is(obsolete) {
				w.Unlock()
				err := w.deleteFile(filepath)
				if err != nil {
					panic(err)
				}
			} else {
				w.fileState[filepath] |= capturedSuccessfully
				w.Unlock()
			}
		}

		// Handle the manifest file
		w.Lock()
		totalManifests := len(w.manifest)
		index := w.manifestIndex
		w.Unlock()
		for ; index < totalManifests; index++ {
			w.Lock()
			manifest := w.manifest[index]
			w.Unlock()

			if manifest.destFile == nil {
				manifest.destFilepath = w.configuration.fs.PathJoin(
					w.configuration.fileHandler.OutputDirectory(),
					w.configuration.fs.PathBase(manifest.sourceFilepath))

				// DestFile does not exist so create it
				var err error
				manifest.destFile, err = w.configuration.fs.Create(manifest.destFilepath)
				if err != nil {
					panic(err)
				}

				w.Lock()
				w.manifest[index].destFilepath = manifest.destFilepath
				w.manifest[index].destFile = manifest.destFile
				w.Unlock()
			}

			numBytesRead, err := w.configuration.fileHandler.HandleManifestFileMutated(w.configuration.fs, manifest)
			if err != nil {
				panic(err)
			}

			w.Lock()
			if numBytesRead == 0 && index != totalManifests-1 {
				// Rotating the manifests so we can close the files
				se := w.manifest[w.manifestIndex].sourceFile.Close()
				if se != nil {
					panic(se)
				}
				de := w.manifest[w.manifestIndex].destFile.Close()
				if de != nil {
					panic(de)
				}
				w.manifestIndex++
			}
			w.Unlock()
		}

		// reset lock for loop
		w.Lock()
	}
	w.Unlock() // unlock for end of loop
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
