package replay

import (
	"io"
	"sync"
	"sync/atomic"

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

// WorkloadStorage is an interface that allows anyone to write their
// own file handler to perform the workload capturing.
type WorkloadStorage interface {
	CopySSTable(fs vfs.FS, path string) error
	CreateManifestFile(name string) (vfs.File, error)
	Checkpoint(db *pebble.DB) error
}

// filesystemWorkloadStorage is a default workload capture tool that
// copies files over to the archive directory.
type filesystemWorkloadStorage struct {
	destDir string
	buffer  []byte
	fs      vfs.FS
}

func (wcc filesystemWorkloadStorage) Checkpoint(db *pebble.DB) error {
	return db.Checkpoint(wcc.destDir)
}

func (wcc filesystemWorkloadStorage) CreateManifestFile(name string) (vfs.File, error) {
	return wcc.fs.Create(wcc.fs.PathJoin(wcc.destDir, name))
}

// FilesystemWorkloadStorage creates a filesystemWorkloadStorage
func FilesystemWorkloadStorage(fs vfs.FS, destDir string) WorkloadStorage {
	fws := filesystemWorkloadStorage{
		destDir: destDir,
		buffer:  make([]byte, 1024 /* 1KB */),
		fs:      fs,
	}
	if err := fws.fs.MkdirAll(fws.destDir, 0755); err != nil {
		panic(err)
	}
	return fws
}

// CopySSTable is similar to the ArchiveCleaner's Clean except that it copies
// files over to the archive directory instead of moving them.
func (wcc filesystemWorkloadStorage) CopySSTable(fs vfs.FS, path string) error {
	destPath := fs.PathJoin(wcc.destDir, fs.PathBase(path))
	return vfs.Copy(fs, path, destPath)
}

type manifestDetails struct {
	sourceFilepath string
	sourceFile     vfs.File

	destFile vfs.File
}

// WorkloadCollector is a cleaner that is designed to capture a workload by
// handling flushed and ingested SSTs. The cleaner only deletes obsolete files
// after they have been processed by the fileHandler.
type WorkloadCollector struct {
	// The fileHandler performs the actual work of capturing the workload. The
	// fileHandler is run in the OnFlushEnd and OnTableIngested which are supposed
	// to be hooked up to the respective EventListener events for TableIngested
	// and FlushEnded.

	mu struct {
		sync.Mutex

		fileState         map[string]workloadCaptureState
		sstablesToProcess []string
		enabled           uint32

		manifests     []manifestDetails
		manifestIndex int
	}
	buffer []byte

	configuration struct {
		fileHandler WorkloadStorage
		fs          vfs.FS
		storageDir  string
		cleaner     base.Cleaner
	}

	fileListener struct {
		sync.Cond
		stopFileListener bool
	}
}

// NewWorkloadCollector is used externally to create a New WorkloadCollector.
func NewWorkloadCollector(srcDir string, fileHandler WorkloadStorage) *WorkloadCollector {
	wc := &WorkloadCollector{}
	wc.buffer = make([]byte, 1<<10 /* 1KB */)

	wc.configuration.fileHandler = fileHandler
	wc.configuration.storageDir = srcDir

	wc.mu.fileState = make(map[string]workloadCaptureState)
	wc.fileListener.Cond.L = &wc.mu.Mutex
	return wc
}

// Attach is used to setup the WorkloadCollector by attaching itself to
// pebble.Options EventListener and Cleaner
func (w *WorkloadCollector) Attach(opts *pebble.Options) {
	opts.EnsureDefaults()
	// Replace the original Cleaner with the workload collector's implementation,
	// which will invoke the original Cleaner, but only once the collector's copied
	// what it needs.
	w.configuration.cleaner, opts.Cleaner = opts.Cleaner, w

	l := pebble.EventListener{
		FlushEnd:        w.OnFlushEnd,
		ManifestCreated: w.OnManifestCreated,
		TableIngested:   w.OnTableIngest,
	}

	opts.EventListener = pebble.TeeEventListener(opts.EventListener, l)

	w.configuration.fs = opts.FS
}

// setFileAsReadyForProcessing calls the handler for the file and marks it as processed.
// Must be called while holding a write lock.
func (w *WorkloadCollector) setFileAsReadyForProcessing(fileNum base.FileNum) {
	filepath := base.MakeFilepath(w.configuration.fs, w.configuration.storageDir, base.FileTypeTable, fileNum)
	w.mu.fileState[filepath] |= readyForProcessing
	w.mu.sstablesToProcess = append(w.mu.sstablesToProcess, filepath)
}

// cleanFile calls the cleaner on the specified path and removes the path from
// the fileState map
func (w *WorkloadCollector) cleanFile(fileType base.FileType, path string) error {
	err := w.configuration.cleaner.Clean(w.configuration.fs, fileType, path)
	if err == nil {
		w.mu.Lock()
		delete(w.mu.fileState, path)
		w.mu.Unlock()
	}
	return err
}

// Clean deletes files only after they have been processed.
func (w *WorkloadCollector) Clean(_ vfs.FS, fileType base.FileType, path string) error {
	w.mu.Lock()
	if fileState, ok := w.mu.fileState[path]; !ok || fileState.is(capturedSuccessfully) {
		// Delete the file if it has been captured or the file is not important to
		// capture which means it can be deleted
		w.mu.Unlock()
		return w.cleanFile(fileType, path)
	}
	w.mu.fileState[path] |= obsolete
	w.mu.Unlock()
	return nil
}

// OnTableIngest is a handler that is to be setup on a EventListener and triggered
// by EventListener.TableIngested calls. It runs through the tables and processes
// them by calling setFileAsReadyForProcessing.
func (w *WorkloadCollector) OnTableIngest(info pebble.TableIngestInfo) {
	if atomic.LoadUint32(&w.mu.enabled) == 0 {
		return
	}
	w.mu.Lock()
	defer w.mu.Unlock()
	for _, table := range info.Tables {
		w.setFileAsReadyForProcessing(table.FileNum)
	}
	w.fileListener.Signal()
}

// OnFlushEnd is a handler that is to be setup on a EventListener and triggered
// by EventListener.FlushEnd calls. It runs through the tables and processes
// them by calling setFileAsReadyForProcessing.
func (w *WorkloadCollector) OnFlushEnd(info pebble.FlushInfo) {
	if atomic.LoadUint32(&w.mu.enabled) == 0 {
		return
	}
	w.mu.Lock()
	defer w.mu.Unlock()
	for _, table := range info.Output {
		w.setFileAsReadyForProcessing(table.FileNum)
	}
	w.fileListener.Signal()
}

// OnManifestCreated is a handler that is to be setup on a EventListener and
// triggered by EventListener.ManifestCreated calls. It sets the state of the
// newly created manifests file and appends it to a list of manifests files to
// process.
func (w *WorkloadCollector) OnManifestCreated(info pebble.ManifestCreateInfo) {
	if atomic.LoadUint32(&w.mu.enabled) == 0 {
		return
	}
	w.mu.Lock()
	defer w.mu.Unlock()
	w.mu.fileState[info.Path] |= readyForProcessing
	w.mu.manifests = append(w.mu.manifests, manifestDetails{
		sourceFilepath: info.Path,
	})
}

// filesToProcessWatcher is wrapper over processFiles that runs indefinitely
func (w *WorkloadCollector) filesToProcessWatcher() {
	w.mu.Lock()
	for !w.fileListener.stopFileListener {
		// The following performs the workload capture. It waits on a condition
		// variable (fileListener) to let it know when new files are available to be
		// collected at which point it runs the fileHandler on each file that is
		// marked as ready for processing (readyForProcessing). After processing, if
		// the file has already been marked as obsolete, the file will
		// be deleted. The manifests are also processed in this section by calling
		// the manifests fileHandler
		if len(w.mu.sstablesToProcess) == 0 {
			w.fileListener.Wait()
		}
		filesToProcess := w.mu.sstablesToProcess[:]
		w.mu.sstablesToProcess = w.mu.sstablesToProcess[:0]
		w.mu.Unlock()

		// Handle Tables
		for _, filepath := range filesToProcess {
			err := w.configuration.fileHandler.CopySSTable(w.configuration.fs, filepath)
			if err != nil {
				panic(err)
			}
			w.mu.Lock()
			if w.mu.fileState[filepath].is(obsolete) {
				w.mu.Unlock()
				err := w.cleanFile(base.FileTypeTable, filepath)
				if err != nil {
					panic(err)
				}
			} else {
				w.mu.fileState[filepath] |= capturedSuccessfully
				w.mu.Unlock()
			}
		}

		// Handle the manifests file
		w.mu.Lock()
		totalManifests := len(w.mu.manifests)
		index := w.mu.manifestIndex
		w.mu.Unlock()
		for ; index < totalManifests; index++ {
			w.mu.Lock()
			manifest := w.mu.manifests[index]
			w.mu.Unlock()

			if manifest.destFile == nil && manifest.sourceFile == nil {
				var err error
				manifest.destFile, err = w.configuration.fileHandler.CreateManifestFile(w.configuration.fs.PathBase(manifest.sourceFilepath))
				if err != nil {
					panic(err)
				}
				manifest.sourceFile, err = w.configuration.fs.Open(manifest.sourceFilepath)
				if err != nil {
					panic(err)
				}

				w.mu.Lock()
				w.mu.manifests[index].sourceFile = manifest.sourceFile
				w.mu.manifests[index].destFile = manifest.destFile
				w.mu.Unlock()
			}

			numBytesRead, err := io.CopyBuffer(manifest.destFile, manifest.sourceFile, w.buffer)
			if err != nil {
				panic(err)
			}

			w.mu.Lock()
			if numBytesRead == 0 && index != totalManifests-1 {
				// Rotating the manifests so we can close the files
				err := w.mu.manifests[w.mu.manifestIndex].sourceFile.Close()
				if err != nil {
					panic(err)
				}
				err = w.mu.manifests[w.mu.manifestIndex].destFile.Close()
				if err != nil {
					panic(err)
				}
				w.mu.manifestIndex++
			}
			w.mu.Unlock()
		}

		// reset lock for loop
		w.mu.Lock()
	}
	w.mu.Unlock() // unlock for end of loop
}

// StartCollectorFileListener starts a go routine that listens for new files that
// need to be collected.
func (w *WorkloadCollector) StartCollectorFileListener() {
	// If the collector not is running hence w.mu.enabled == 0 swap it to 1 and
	// continue else it is not running return
	if !atomic.CompareAndSwapUint32(&w.mu.enabled, 0, 1) {
		return
	}
	go w.filesToProcessWatcher()
}

// StopCollectorFileListener stops the go routine that listens for new files
// that need to be collected
func (w *WorkloadCollector) StopCollectorFileListener() {
	// If the collector is running hence w.mu.enabled == 1 swap it to 0 and
	// continue else it is not running return
	if !atomic.CompareAndSwapUint32(&w.mu.enabled, 1, 0) {
		return
	}
	w.mu.Lock()
	defer w.mu.Unlock()
	w.fileListener.stopFileListener = true
	w.fileListener.Signal()
}
