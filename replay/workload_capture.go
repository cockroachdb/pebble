// Copyright 2022 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

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

type manifestDetails struct {
	sourceFilepath string
	sourceFile     vfs.File

	destFile vfs.File
}

// WorkloadCollector is a cleaner that is designed to capture a workload by
// handling flushed and ingested SSTs. The cleaner only deletes obsolete files
// if they are relevant to workload capture and have been processed.
type WorkloadCollector struct {
	mu struct {
		sync.Mutex

		fileState         map[string]workloadCaptureState
		sstablesToProcess []string

		manifestIndex int

		// appending to manifests requires holding mu however reading data does not
		manifests []*manifestDetails
	}

	// Stores the current manifest that is being used by the database. Updated
	// atomically
	curManifest uint64

	// A boolean represented as an atomic uint32 that stores whether the workload
	// collector is enabled
	enabled uint32

	buffer []byte

	// configuration contains information that is only set on the creation of the
	// WorkloadCollector
	configuration struct {
		// srcFS and srcDir represent the location from which the workload collector
		// collects the files from
		srcFS  vfs.FS
		srcDir string

		// destFS and destDir represent the location to which the workload collector
		// sends the files to
		destFS  vfs.FS
		destDir string

		// cleaner stores the cleaner to use when files become obsolete and need to
		// be cleaned
		cleaner base.Cleaner
	}

	fileListener struct {
		sync.Cond
		stopFileListener bool
	}
}

// NewWorkloadCollector is used externally to create a New WorkloadCollector.
func NewWorkloadCollector(srcDir string) *WorkloadCollector {
	wc := &WorkloadCollector{}
	wc.buffer = make([]byte, 1<<10 /* 1KB */)

	wc.configuration.srcDir = srcDir

	wc.mu.fileState = make(map[string]workloadCaptureState)
	wc.fileListener.Cond.L = &wc.mu.Mutex
	return wc
}

// Attach is used to setup the WorkloadCollector by attaching itself to
// pebble.Options EventListener and Cleaner
func (w *WorkloadCollector) Attach(opts *pebble.Options) {
	l := pebble.EventListener{
		FlushEnd:        w.OnFlushEnd,
		ManifestCreated: w.OnManifestCreated,
		TableIngested:   w.OnTableIngest,
	}

	if !opts.EventListener.IsConfigured() {
		opts.EventListener = l
	} else {
		opts.EventListener = pebble.TeeEventListener(opts.EventListener, l)
	}

	opts.EnsureDefaults()
	// Replace the original Cleaner with the workload collector's implementation,
	// which will invoke the original Cleaner, but only once the collector's copied
	// what it needs.
	w.configuration.cleaner, opts.Cleaner = opts.Cleaner, w
	w.configuration.srcFS = opts.FS
}

// setFileAsReadyForProcessing calls the handler for the file and marks it as processed.
// Must be called while holding a write lock.
func (w *WorkloadCollector) setFileAsReadyForProcessing(fileNum base.FileNum) {
	filepath := base.MakeFilepath(w.configuration.srcFS, w.configuration.srcDir, base.FileTypeTable, fileNum)
	w.mu.fileState[filepath] |= readyForProcessing
	w.mu.sstablesToProcess = append(w.mu.sstablesToProcess, filepath)
}

// cleanFile calls the cleaner on the specified path and removes the path from
// the fileState map
func (w *WorkloadCollector) cleanFile(fileType base.FileType, path string) error {
	err := w.configuration.cleaner.Clean(w.configuration.srcFS, fileType, path)
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
	if atomic.LoadUint32(&w.enabled) == 0 {
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
	if atomic.LoadUint32(&w.enabled) == 0 {
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
	atomic.StoreUint64(&w.curManifest, uint64(info.FileNum))
	if atomic.LoadUint32(&w.enabled) == 0 {
		return
	}
	w.mu.Lock()
	defer w.mu.Unlock()

	// mark the manifest file as ready for processing to prevent it from being
	// cleaned before we process it
	w.mu.fileState[info.Path] |= readyForProcessing
	w.mu.manifests = append(w.mu.manifests, &manifestDetails{
		sourceFilepath: info.Path,
	})
}

// copyBetweenFS copies the contents of srcPath to a file on the dstFS. If the srcPath
// file exists on dstFS, it will be overwritten.
func copyBetweenFS(srcFS vfs.FS, srcPath string, destFS vfs.FS, destDir string) error {
	src, err := srcFS.Open(srcPath)
	if err != nil {
		return err
	}
	defer src.Close()

	dst, err := destFS.Create(destFS.PathJoin(destDir, srcFS.PathBase(srcPath)))
	if err != nil {
		return err
	}
	defer dst.Close()

	if _, err := io.Copy(dst, src); err != nil {
		return err
	}
	return dst.Sync()
}

// filesToProcessWatcher is wrapper over processFiles that runs indefinitely
func (w *WorkloadCollector) filesToProcessWatcher() {
	w.mu.Lock() // lock [1]
	for !w.fileListener.stopFileListener {
		// The following performs the workload capture. It waits on a condition
		// variable (fileListener) to let it know when new files are available to be
		// collected.
		if len(w.mu.sstablesToProcess) == 0 {
			w.fileListener.Wait()
		}
		// Copied details for SSTs
		sstablesToProcess := w.mu.sstablesToProcess
		w.mu.sstablesToProcess = nil

		// Copied details for manifests
		totalManifests := len(w.mu.manifests)
		index := w.mu.manifestIndex
		w.mu.Unlock() // lock [1]

		// Process the SSTables provided in sstablesToProcess. sstablesToProcess
		// will be rewritten and cannot be used following this call.
		w.processSSTables(sstablesToProcess)

		// Process the manifests files.
		w.processManifests(index, totalManifests)

		w.mu.Lock() // reset lock for loop [1]
	}
	w.mu.Unlock() // unlock for end of loop [1]
}

// processManifests iterates over the manifests and copies data that has been added to the source
func (w *WorkloadCollector) processManifests(index int, totalManifests int) {
	for ; index < totalManifests; index++ {
		manifest := w.mu.manifests[index]

		destFS := w.configuration.destFS
		if manifest.destFile == nil && manifest.sourceFile == nil {
			// srcFile and destFile are not opened / created as this is the first time
			// this manifest is being processed
			var err error
			manifest.destFile, err = destFS.Create(
				destFS.PathJoin(w.configuration.destDir,
					destFS.PathBase(manifest.sourceFilepath)))

			if err != nil {
				panic(err)
			}
			manifest.sourceFile, err = w.configuration.srcFS.Open(manifest.sourceFilepath)
			if err != nil {
				panic(err)
			}

			// Only this method will be updating the source and destination files as a
			// result it is safe to do so without holding a lock
			w.mu.manifests[index].sourceFile = manifest.sourceFile
			w.mu.manifests[index].destFile = manifest.destFile
		}

		// Copy the data from the source to destination using w.buffer as the buffer
		numBytesRead, err := io.CopyBuffer(manifest.destFile, manifest.sourceFile, w.buffer)
		if err != nil {
			panic(err)
		}

		// Read 0 bytes from the current manifest and this is not the latest/newest
		// manifest which means we have read all the data from that manifest and no
		// new data will be written to it since it's not the latest one. Close
		// the current source and destination files and move the manifest to start
		// at index up by 1.
		if numBytesRead == 0 && index != totalManifests-1 {
			// Rotating the manifests so we can close the files
			err := w.mu.manifests[index].sourceFile.Close()
			if err != nil {
				panic(err)
			}
			err = w.mu.manifests[index].destFile.Close()
			if err != nil {
				panic(err)
			}
			w.mu.Lock()
			w.mu.manifestIndex = index + 1
			w.mu.Unlock()
		}
	}
}

// processSSTables goes through the sstablesToProcess and copies them between
// srcFS and the destFS. Additionally, each table has its file state updated. If
// a file has already been marked as obsolete, then file will be cleaned by the
// w.configuration.cleaner. The sstablesToProcess will be rewritten and should
// not be used following the call to this function.
func (w *WorkloadCollector) processSSTables(sstablesToProcess []string) {
	for _, filepath := range sstablesToProcess {
		err := copyBetweenFS(w.configuration.srcFS, filepath, w.configuration.destFS, w.configuration.destDir)
		if err != nil {
			panic(err)
		}
	}

	// reuse the slice
	filesToDelete := sstablesToProcess[:0]
	w.mu.Lock()
	for _, filepath := range sstablesToProcess {
		if w.mu.fileState[filepath].is(obsolete) {
			filesToDelete = append(filesToDelete, filepath)

		} else {
			w.mu.fileState[filepath] |= capturedSuccessfully
		}
	}
	w.mu.Unlock()

	for _, filepath := range filesToDelete {
		err := w.cleanFile(base.FileTypeTable, filepath)
		if err != nil {
			panic(err)
		}
	}
}

// StartCollectorFileListener starts a go routine that listens for new files that
// need to be collected.
func (w *WorkloadCollector) StartCollectorFileListener(destFS vfs.FS, destPath string) {
	// If the collector not is running hence w.enabled == 0 swap it to 1 and
	// continue else it is not running return
	if !atomic.CompareAndSwapUint32(&w.enabled, 0, 1) {
		return
	}
	w.configuration.destFS = destFS
	w.configuration.destDir = destPath

	fileNum := atomic.LoadUint64(&w.curManifest)
	filePath := base.MakeFilepath(w.configuration.srcFS,
		w.configuration.srcDir,
		base.FileTypeManifest,
		base.FileNum(fileNum))
	w.mu.manifests = append(w.mu.manifests,
		&manifestDetails{
			sourceFilepath: filePath,
		})
	w.mu.fileState[filePath] |= readyForProcessing

	go w.filesToProcessWatcher()
}

// StopCollectorFileListener stops the go routine that listens for new files
// that need to be collected
func (w *WorkloadCollector) StopCollectorFileListener() {
	// If the collector is running hence w.enabled == 1 swap it to 0 and
	// continue else it is not running return
	if !atomic.CompareAndSwapUint32(&w.enabled, 1, 0) {
		return
	}
	w.mu.Lock()
	defer w.mu.Unlock()
	w.fileListener.stopFileListener = true
	w.fileListener.Signal()
}

// IsRunning returns whether the WorkloadCollector is currently running
func (w *WorkloadCollector) IsRunning() bool {
	return atomic.LoadUint32(&w.enabled) == 1
}
