// Copyright 2019 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package pebble

import (
	"cmp"
	"slices"

	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/errors/oserror"
	"github.com/cockroachdb/pebble/internal/base"
	"github.com/cockroachdb/pebble/internal/deletepacer"
	"github.com/cockroachdb/pebble/internal/invariants"
	"github.com/cockroachdb/pebble/metrics"
	"github.com/cockroachdb/pebble/objstorage"
	"github.com/cockroachdb/pebble/vfs"
	"github.com/cockroachdb/pebble/wal"
)

// Cleaner exports the base.Cleaner type.
type Cleaner = base.Cleaner

// DeleteCleaner exports the base.DeleteCleaner type.
type DeleteCleaner = base.DeleteCleaner

// ArchiveCleaner exports the base.ArchiveCleaner type.
type ArchiveCleaner = base.ArchiveCleaner

func openDeletePacer(
	opts *Options, objProvider objstorage.Provider, diskFreeSpaceFn deletepacer.DiskFreeSpaceFn,
) *deletepacer.DeletePacer {
	return deletepacer.Open(
		opts.DeletionPacing,
		opts.Logger,
		diskFreeSpaceFn,
		func(of deletepacer.ObsoleteFile, jobID int) {
			deleteObsoleteFile(opts.Cleaner, objProvider, opts.EventListener, of, jobID)
		},
	)
}

// deleteObsoleteFile deletes a file or object, once the delete pacer decided it is time.
func deleteObsoleteFile(
	cleaner Cleaner,
	objProvider objstorage.Provider,
	eventListener *EventListener,
	of deletepacer.ObsoleteFile,
	jobID int,
) {
	path := of.Path
	var err error
	if of.FileType == base.FileTypeTable || of.FileType == base.FileTypeBlob {
		var meta objstorage.ObjectMetadata
		meta, err = objProvider.Lookup(of.FileType, of.FileNum)
		if err != nil {
			path = "<nil>"
		} else {
			path = objProvider.Path(meta)
			err = objProvider.Remove(of.FileType, of.FileNum)
		}
		if objProvider.IsNotExistError(err) {
			return
		}
	} else {
		// TODO(peter): need to handle this error, probably by re-adding the
		// file that couldn't be deleted to one of the obsolete slices map.
		err := cleaner.Clean(of.FS, of.FileType, path)
		if oserror.IsNotExist(err) {
			return
		}
	}

	switch of.FileType {
	case base.FileTypeTable:
		eventListener.TableDeleted(TableDeleteInfo{
			JobID:   jobID,
			Path:    path,
			FileNum: of.FileNum,
			Err:     err,
		})
	case base.FileTypeBlob:
		eventListener.BlobFileDeleted(BlobFileDeleteInfo{
			JobID:   jobID,
			Path:    path,
			FileNum: of.FileNum,
			Err:     err,
		})
	case base.FileTypeLog:
		eventListener.WALDeleted(WALDeleteInfo{
			JobID:   jobID,
			Path:    path,
			FileNum: of.FileNum,
			Err:     err,
		})
	case base.FileTypeManifest:
		eventListener.ManifestDeleted(ManifestDeleteInfo{
			JobID:   jobID,
			Path:    path,
			FileNum: of.FileNum,
			Err:     err,
		})
	}
}

// scanObsoleteFiles compares the provided directory listing to the set of
// known, in-use files to find files no longer needed and adds those to the
// internal lists of obsolete files. Note that the files are not actually
// deleted by this method. A subsequent call to deleteObsoleteFiles must be
// performed. Must be not be called concurrently with compactions and flushes
// and will panic if any are in-progress. db.mu must be held when calling this
// function.
func (d *DB) scanObsoleteFiles(list []string, flushableIngests []*ingestedFlushable) {
	if d.mu.compact.compactingCount > 0 || d.mu.compact.downloadingCount > 0 || d.mu.compact.flushing {
		panic(errors.AssertionFailedf("compaction or flush in progress"))
	}

	liveFileNums := make(map[base.DiskFileNum]struct{})
	d.mu.versions.addLiveFileNums(liveFileNums)
	// Protect against files which are only referred to by the ingestedFlushable
	// from being deleted. These are added to the flushable queue on WAL replay
	// and handle their own obsoletion/deletion. We exclude them from this obsolete
	// file scan to avoid double-deleting these files.
	for _, f := range flushableIngests {
		for _, file := range f.files {
			liveFileNums[file.TableBacking.DiskFileNum] = struct{}{}
		}
	}

	manifestFileNum := d.mu.versions.manifestFileNum

	var obsoleteTables []deletepacer.ObsoleteFile
	var obsoleteBlobs []deletepacer.ObsoleteFile
	var obsoleteOptions []deletepacer.ObsoleteFile
	var obsoleteManifests []deletepacer.ObsoleteFile

	for _, filename := range list {
		fileType, diskFileNum, ok := base.ParseFilename(d.opts.FS, filename)
		if !ok {
			continue
		}
		makeObsoleteFile := func() deletepacer.ObsoleteFile {
			of := deletepacer.ObsoleteFile{
				FileType: fileType,
				FS:       d.opts.FS,
				Path:     d.opts.FS.PathJoin(d.dirname, filename),
				FileNum:  diskFileNum,
				IsLocal:  true,
			}
			if stat, err := d.opts.FS.Stat(filename); err == nil {
				of.FileSize = uint64(stat.Size())
			}
			return of
		}
		switch fileType {
		case base.FileTypeManifest:
			if diskFileNum >= manifestFileNum {
				continue
			}
			obsoleteManifests = append(obsoleteManifests, makeObsoleteFile())
		case base.FileTypeOptions:
			if diskFileNum >= d.optionsFileNum {
				continue
			}
			obsoleteOptions = append(obsoleteOptions, makeObsoleteFile())
		case base.FileTypeTable, base.FileTypeBlob:
			// Objects are handled through the objstorage provider below.
		default:
			// Don't delete files we don't know about.
		}
	}

	objects := d.objProvider.List()
	for _, obj := range objects {
		if _, ok := liveFileNums[obj.DiskFileNum]; ok {
			continue
		}
		if obj.FileType != base.FileTypeTable && obj.FileType != base.FileTypeBlob {
			// Ignore object types we don't know about.
			continue
		}
		of := deletepacer.ObsoleteFile{
			FileType: obj.FileType,
			FS:       d.opts.FS,
			Path:     base.MakeFilepath(d.opts.FS, d.dirname, obj.FileType, obj.DiskFileNum),
			FileNum:  obj.DiskFileNum,
			IsLocal:  true,
		}
		if size, err := d.objProvider.Size(obj); err == nil {
			of.FileSize = uint64(size)
		}
		if obj.FileType == base.FileTypeTable {
			obsoleteTables = append(obsoleteTables, of)
		} else {
			obsoleteBlobs = append(obsoleteBlobs, of)
		}
	}

	d.mu.versions.obsoleteTables = mergeObsoleteFiles(d.mu.versions.obsoleteTables, obsoleteTables)
	d.mu.versions.obsoleteBlobs = mergeObsoleteFiles(d.mu.versions.obsoleteBlobs, obsoleteBlobs)
	d.mu.versions.obsoleteManifests = mergeObsoleteFiles(d.mu.versions.obsoleteManifests, obsoleteManifests)
	d.mu.versions.obsoleteOptions = mergeObsoleteFiles(d.mu.versions.obsoleteOptions, obsoleteOptions)
	d.mu.versions.updateObsoleteObjectMetricsLocked()
}

// disableFileDeletions disables file deletions and then waits for any
// in-progress deletion to finish. The caller is required to call
// enableFileDeletions in order to enable file deletions again. It is ok for
// multiple callers to disable file deletions simultaneously, though they must
// all invoke enableFileDeletions in order for file deletions to be re-enabled
// (there is an internal reference count on file deletion disablement).
//
// d.mu must be held when calling this method.
func (d *DB) disableFileDeletions() {
	d.mu.fileDeletions.disableCount++
	d.mu.Unlock()
	defer d.mu.Lock()
	d.deletePacer.WaitForTesting()
}

// enableFileDeletions enables previously disabled file deletions. A cleanup job
// is queued if necessary.
//
// d.mu must be held when calling this method.
func (d *DB) enableFileDeletions() {
	if d.mu.fileDeletions.disableCount <= 0 {
		panic("pebble: file deletion disablement invariant violated")
	}
	d.mu.fileDeletions.disableCount--
	if d.mu.fileDeletions.disableCount > 0 {
		return
	}
	d.deleteObsoleteFiles(d.newJobIDLocked())
}

type fileInfo = base.FileInfo

// deleteObsoleteFiles enqueues a cleanup job to the cleanup manager, if necessary.
//
// d.mu must be held when calling this. The function will release and re-aquire the mutex.
//
// Does nothing if file deletions are disabled (see disableFileDeletions). A
// cleanup job will be scheduled when file deletions are re-enabled.
func (d *DB) deleteObsoleteFiles(jobID JobID) {
	if d.mu.fileDeletions.disableCount > 0 {
		return
	}
	_, noRecycle := d.opts.Cleaner.(base.NeedsFileContents)

	// NB: d.mu.versions.minUnflushedLogNum is the log number of the earliest
	// log that has not had its contents flushed to an sstable.
	obsoleteLogs, err := d.mu.log.manager.Obsolete(wal.NumWAL(d.mu.versions.minUnflushedLogNum), noRecycle)
	if err != nil {
		panic(err)
	}

	obsoleteTables := slices.Clone(d.mu.versions.obsoleteTables)
	d.mu.versions.obsoleteTables = d.mu.versions.obsoleteTables[:0]
	obsoleteBlobs := slices.Clone(d.mu.versions.obsoleteBlobs)
	d.mu.versions.obsoleteBlobs = d.mu.versions.obsoleteBlobs[:0]

	// Ensure everything is already sorted. We want determinism for testing, and
	// we need the manifests to be sorted because we want to delete some
	// contiguous prefix of the older manifests.
	if invariants.Enabled {
		switch {
		case !slices.IsSortedFunc(d.mu.versions.obsoleteManifests, cmpObsoleteFileNumbers):
			d.opts.Logger.Fatalf("obsoleteManifests is not sorted")
		case !slices.IsSortedFunc(d.mu.versions.obsoleteOptions, cmpObsoleteFileNumbers):
			d.opts.Logger.Fatalf("obsoleteOptions is not sorted")
		case !slices.IsSortedFunc(obsoleteTables, cmpObsoleteFileNumbers):
			d.opts.Logger.Fatalf("obsoleteTables is not sorted")
		case !slices.IsSortedFunc(obsoleteBlobs, cmpObsoleteFileNumbers):
			d.opts.Logger.Fatalf("obsoleteBlobs is not sorted")
		}
	}

	var obsoleteManifests []deletepacer.ObsoleteFile
	manifestsToDelete := len(d.mu.versions.obsoleteManifests) - d.opts.NumPrevManifest
	if manifestsToDelete > 0 {
		obsoleteManifests = d.mu.versions.obsoleteManifests[:manifestsToDelete]
		d.mu.versions.obsoleteManifests = d.mu.versions.obsoleteManifests[manifestsToDelete:]
		if len(d.mu.versions.obsoleteManifests) == 0 {
			d.mu.versions.obsoleteManifests = nil
		}
	}

	obsoleteOptions := d.mu.versions.obsoleteOptions
	d.mu.versions.obsoleteOptions = nil

	// Update the obsolete object metrics in d.mu.versions.metrics. These metrics
	// will be combined with the metrics from the delete pacer in DB.Metrics().
	d.mu.versions.updateObsoleteObjectMetricsLocked()

	// Release d.mu while preparing the cleanup job and possibly waiting.
	// Note the unusual order: Unlock and then Lock.
	d.mu.Unlock()
	defer d.mu.Lock()

	n := len(obsoleteLogs) + len(obsoleteTables) + len(obsoleteBlobs) + len(obsoleteManifests) + len(obsoleteOptions)
	filesToDelete := make([]deletepacer.ObsoleteFile, 0, n)
	filesToDelete = append(filesToDelete, obsoleteManifests...)
	filesToDelete = append(filesToDelete, obsoleteOptions...)
	filesToDelete = append(filesToDelete, obsoleteTables...)
	filesToDelete = append(filesToDelete, obsoleteBlobs...)
	for _, f := range obsoleteLogs {
		filesToDelete = append(filesToDelete, deletepacer.ObsoleteFile{
			FileType: base.FileTypeLog,
			FS:       f.FS,
			Path:     f.Path,
			FileNum:  base.DiskFileNum(f.NumWAL),
			FileSize: f.ApproxFileSize,
			IsLocal:  true,
		})
	}
	for _, f := range obsoleteTables {
		d.fileCache.Evict(f.FileNum, base.FileTypeTable)
	}
	for _, f := range obsoleteBlobs {
		d.fileCache.Evict(f.FileNum, base.FileTypeBlob)
	}
	if len(filesToDelete) > 0 {
		d.deletePacer.Enqueue(int(jobID), filesToDelete...)
	}
	if d.opts.private.testingAlwaysWaitForCleanup {
		d.deletePacer.WaitForTesting()
	}
}

func (d *DB) maybeScheduleObsoleteObjectDeletion() {
	d.mu.Lock()
	defer d.mu.Unlock()
	if len(d.mu.versions.obsoleteTables) > 0 || len(d.mu.versions.obsoleteBlobs) > 0 {
		d.deleteObsoleteFiles(d.newJobIDLocked())
	}
}

func mergeObsoleteFiles(a, b []deletepacer.ObsoleteFile) []deletepacer.ObsoleteFile {
	if len(b) == 0 {
		return a
	}

	a = append(a, b...)
	slices.SortFunc(a, cmpObsoleteFileNumbers)
	return slices.CompactFunc(a, func(a, b deletepacer.ObsoleteFile) bool {
		return a.FileNum == b.FileNum
	})
}

func cmpObsoleteFileNumbers(a, b deletepacer.ObsoleteFile) int {
	return cmp.Compare(a.FileNum, b.FileNum)
}

// objectInfo describes an object in object storage (either a sstable or a blob
// file).
type objectInfo struct {
	fileInfo
	isLocal bool
}

func (o objectInfo) asObsoleteFile(
	fs vfs.FS, fileType base.FileType, dirname string,
) deletepacer.ObsoleteFile {
	return deletepacer.ObsoleteFile{
		FileType: fileType,
		FS:       fs,
		Path:     base.MakeFilepath(fs, dirname, fileType, o.FileNum),
		FileNum:  o.FileNum,
		FileSize: o.FileSize,
		IsLocal:  o.isLocal,
	}
}

func makeZombieObjects() zombieObjects {
	return zombieObjects{
		objs: make(map[base.DiskFileNum]objectInfo),
	}
}

// zombieObjects tracks a set of objects that are no longer required by the most
// recent version of the LSM, but may still need to be accessed by an open
// iterator. Such objects are 'dead,' but cannot be deleted until iterators that
// may access them are closed.
type zombieObjects struct {
	objs map[base.DiskFileNum]objectInfo

	metrics.TableCountsAndSizes
	totalSize  uint64
	localSize  uint64
	localCount uint64
}

// Add adds an object to the set of zombie objects.
func (z *zombieObjects) Add(obj objectInfo) {
	if _, ok := z.objs[obj.FileNum]; ok {
		panic(errors.AssertionFailedf("zombie object %s already exists", obj.FileNum))
	}
	z.objs[obj.FileNum] = obj
	z.totalSize += obj.FileSize
	if obj.isLocal {
		z.localSize += obj.FileSize
		z.localCount++
	}
}

// AddMetadata is like Add, but takes an ObjectMetadata and the object's size.
func (z *zombieObjects) AddMetadata(meta *objstorage.ObjectMetadata, size uint64) {
	z.Add(objectInfo{
		fileInfo: fileInfo{
			FileNum:  meta.DiskFileNum,
			FileSize: size,
		},
		isLocal: !meta.IsRemote(),
	})
}

// Count returns the number of zombie objects.
func (z *zombieObjects) Count() int {
	return len(z.objs)
}

// Extract removes an object from the set of zombie objects, returning the
// object that was removed.
func (z *zombieObjects) Extract(fileNum base.DiskFileNum) objectInfo {
	obj, ok := z.objs[fileNum]
	if !ok {
		panic(errors.AssertionFailedf("zombie object %s not found", fileNum))
	}
	delete(z.objs, fileNum)

	// Detect underflow in case we have a bug that causes an object's size to be
	// mutated.
	if z.totalSize < obj.FileSize {
		panic(errors.AssertionFailedf("zombie object %s size %d is greater than total size %d", fileNum, obj.FileSize, z.totalSize))
	}
	if obj.isLocal && z.localSize < obj.FileSize {
		panic(errors.AssertionFailedf("zombie object %s size %d is greater than local size %d", fileNum, obj.FileSize, z.localSize))
	}

	z.totalSize -= obj.FileSize
	if obj.isLocal {
		z.localSize -= obj.FileSize
		z.localCount--
	}
	return obj
}

// TotalSize returns the size of all objects in the set.
func (z *zombieObjects) TotalSize() uint64 {
	return z.totalSize
}

// LocalStats returns the count and size of all local objects in the set.
func (z *zombieObjects) LocalStats() (count uint64, size uint64) {
	return z.localCount, z.localSize
}
