// Copyright 2012 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package pebble

import (
	"fmt"
	"io"
	"os"
	"sync"
	"sync/atomic"

	"github.com/cockroachdb/pebble/internal/base"
	"github.com/cockroachdb/pebble/internal/manifest"
	"github.com/cockroachdb/pebble/internal/record"
	"github.com/cockroachdb/pebble/vfs"
)

const numLevels = manifest.NumLevels

// Provide type aliases for the various manifest structs.
type bulkVersionEdit = manifest.BulkVersionEdit
type deletedFileEntry = manifest.DeletedFileEntry
type fileMetadata = manifest.FileMetadata
type newFileEntry = manifest.NewFileEntry
type version = manifest.Version
type versionEdit = manifest.VersionEdit
type versionList = manifest.VersionList

// versionSet manages a collection of immutable versions, and manages the
// creation of a new version from the most recent version. A new versions is
// created from an existing version by applying a version edit which is just
// like it sounds: a delta from the previous version. Version edits are logged
// to the manifest file, which is replayed at startup.
type versionSet struct {
	// Immutable fields.
	dirname string
	mu      *sync.Mutex
	opts    *Options
	fs      vfs.FS
	cmp     Compare
	cmpName string
	// Dynamic base level allows the dynamic base level computation to be
	// disabled. Used by tests which want to create specific LSM structures.
	dynamicBaseLevel bool

	// Mutable fields.
	versions versionList
	picker   *compactionPicker

	metrics VersionMetrics

	// A pointer to versionSet.addObsoleteLocked. Avoids allocating a new closure
	// on the creation of every version.
	obsoleteFn        func(obsolete []uint64)
	obsoleteTables    []uint64
	obsoleteManifests []uint64
	obsoleteOptions   []uint64

	logNum          uint64
	prevLogNum      uint64
	nextFileNum     uint64
	logSeqNum       uint64 // next seqNum to use for WAL writes
	visibleSeqNum   uint64 // visible seqNum (<= logSeqNum)
	manifestFileNum uint64

	manifestFile vfs.File
	manifest     *record.Writer

	writing    bool
	writerCond sync.Cond
}

// load loads the version set from the manifest file.
func (vs *versionSet) load(dirname string, opts *Options, mu *sync.Mutex) error {
	vs.dirname = dirname
	vs.mu = mu
	vs.writerCond.L = mu
	vs.opts = opts
	vs.fs = opts.FS
	vs.cmp = opts.Comparer.Compare
	vs.cmpName = opts.Comparer.Name
	vs.dynamicBaseLevel = true
	vs.versions.Init(mu)
	vs.obsoleteFn = vs.addObsoleteLocked
	// For historical reasons, the next file number is initialized to 2.
	vs.nextFileNum = 2

	// Read the CURRENT file to find the current manifest file.
	current, err := vs.fs.Open(base.MakeFilename(dirname, fileTypeCurrent, 0))
	if err != nil {
		return fmt.Errorf("pebble: could not open CURRENT file for DB %q: %v", dirname, err)
	}
	defer current.Close()
	stat, err := current.Stat()
	if err != nil {
		return err
	}
	n := stat.Size()
	if n == 0 {
		return fmt.Errorf("pebble: CURRENT file for DB %q is empty", dirname)
	}
	if n > 4096 {
		return fmt.Errorf("pebble: CURRENT file for DB %q is too large", dirname)
	}
	b := make([]byte, n)
	_, err = current.ReadAt(b, 0)
	if err != nil {
		return err
	}
	if b[n-1] != '\n' {
		return fmt.Errorf("pebble: CURRENT file for DB %q is malformed", dirname)
	}
	b = b[:n-1]

	// Read the versionEdits in the manifest file.
	var bve bulkVersionEdit
	manifest, err := vs.fs.Open(dirname + string(os.PathSeparator) + string(b))
	if err != nil {
		return fmt.Errorf("pebble: could not open manifest file %q for DB %q: %v", b, dirname, err)
	}
	defer manifest.Close()
	rr := record.NewReader(manifest, 0 /* logNum */)
	for {
		r, err := rr.Next()
		if err == io.EOF {
			break
		}
		if err != nil {
			return err
		}
		var ve versionEdit
		err = ve.Decode(r)
		if err != nil {
			return err
		}
		if ve.ComparerName != "" {
			if ve.ComparerName != vs.cmpName {
				return fmt.Errorf("pebble: manifest file %q for DB %q: "+
					"comparer name from file %q != comparer name from Options %q",
					b, dirname, ve.ComparerName, vs.cmpName)
			}
		}
		bve.Accumulate(&ve)
		if ve.LogNum != 0 {
			vs.logNum = ve.LogNum
		}
		if ve.PrevLogNum != 0 {
			vs.prevLogNum = ve.PrevLogNum
		}
		if ve.NextFileNum != 0 {
			vs.nextFileNum = ve.NextFileNum
		}
		if ve.LastSeqNum != 0 {
			vs.logSeqNum = ve.LastSeqNum
		}
	}
	if vs.logNum == 0 || vs.nextFileNum == 0 {
		if vs.nextFileNum == 2 {
			// We have a freshly created DB.
		} else {
			return fmt.Errorf("pebble: incomplete manifest file %q for DB %q", b, dirname)
		}
	}
	vs.markFileNumUsed(vs.logNum)
	vs.markFileNumUsed(vs.prevLogNum)

	newVersion, err := bve.Apply(nil, vs.cmp, opts.Comparer.Format)
	if err != nil {
		return err
	}
	vs.append(newVersion)

	for i := range vs.metrics.Levels {
		l := &vs.metrics.Levels[i]
		l.NumFiles = int64(len(newVersion.Files[i]))
		l.Size = uint64(totalSize(newVersion.Files[i]))
	}
	return nil
}

// logAndApply logs the version edit to the manifest, applies the version edit
// to the current version, and installs the new version. DB.mu must be held
// when calling this method and will be released temporarily while performing
// file I/O.
func (vs *versionSet) logAndApply(
	jobID int,
	ve *versionEdit,
	metrics map[int]*LevelMetrics,
	dir vfs.File,
) error {
	// Wait for any existing writing to the manifest to complete, then mark the
	// manifest as busy.
	for vs.writing {
		vs.writerCond.Wait()
	}
	vs.writing = true
	defer func() {
		vs.writing = false
		vs.writerCond.Signal()
	}()

	if ve.LogNum != 0 {
		if ve.LogNum < vs.logNum || vs.nextFileNum <= ve.LogNum {
			panic(fmt.Sprintf("pebble: inconsistent versionEdit logNumber %d", ve.LogNum))
		}
	}
	ve.NextFileNum = vs.nextFileNum
	ve.LastSeqNum = atomic.LoadUint64(&vs.logSeqNum)
	currentVersion := vs.currentVersion()
	var newVersion *version

	// Generate a new manifest if we don't currently have one, or the current one
	// is too large.
	var newManifestFileNum uint64
	if vs.manifest == nil || vs.manifest.Size() >= vs.opts.MaxManifestFileSize {
		newManifestFileNum = vs.getNextFileNum()
	}

	var picker *compactionPicker
	if err := func() error {
		vs.mu.Unlock()
		defer vs.mu.Lock()

		var bve bulkVersionEdit
		bve.Accumulate(ve)

		var err error
		newVersion, err = bve.Apply(currentVersion, vs.cmp, vs.opts.Comparer.Format)
		if err != nil {
			return err
		}

		if newManifestFileNum != 0 {
			if err := vs.createManifest(vs.dirname, newManifestFileNum); err != nil {
				vs.opts.EventListener.ManifestCreated(ManifestCreateInfo{
					JobID:   jobID,
					Path:    base.MakeFilename(vs.dirname, fileTypeManifest, newManifestFileNum),
					FileNum: newManifestFileNum,
					Err:     err,
				})
				return err
			}
		}

		w, err := vs.manifest.Next()
		if err != nil {
			return err
		}
		// NB: Any error from this point on is considered fatal as we don't now if
		// the MANIFEST write occurred or not. Trying to determine that is
		// fraught. Instead we rely on the standard recovery mechanism run when a
		// database is open. In particular, that mechanism generates a new MANIFEST
		// and ensures it is synced.
		if err := ve.Encode(w); err != nil {
			vs.opts.Logger.Fatalf("MANIFEST write failed: %v", err)
			return err
		}
		if err := vs.manifest.Flush(); err != nil {
			vs.opts.Logger.Fatalf("MANIFEST flush failed: %v", err)
			return err
		}
		if err := vs.manifestFile.Sync(); err != nil {
			vs.opts.Logger.Fatalf("MANIFEST sync failed: %v", err)
			return err
		}
		if newManifestFileNum != 0 {
			if err := setCurrentFile(vs.dirname, vs.fs, newManifestFileNum); err != nil {
				vs.opts.Logger.Fatalf("MANIFEST set current failed: %v", err)
				return err
			}
			if err := dir.Sync(); err != nil {
				vs.opts.Logger.Fatalf("MANIFEST dirsync failed: %v", err)
				return err
			}
			vs.opts.EventListener.ManifestCreated(ManifestCreateInfo{
				JobID:   jobID,
				Path:    base.MakeFilename(vs.dirname, fileTypeManifest, newManifestFileNum),
				FileNum: newManifestFileNum,
			})
		}
		picker = newCompactionPicker(newVersion, vs.opts)
		if !vs.dynamicBaseLevel {
			picker.baseLevel = 1
		}
		return nil
	}(); err != nil {
		return err
	}

	// Install the new version.
	vs.append(newVersion)
	if ve.LogNum != 0 {
		vs.logNum = ve.LogNum
	}
	if ve.PrevLogNum != 0 {
		vs.prevLogNum = ve.PrevLogNum
	}
	if newManifestFileNum != 0 {
		if vs.manifestFileNum != 0 {
			vs.obsoleteManifests = append(vs.obsoleteManifests, vs.manifestFileNum)
		}
		vs.manifestFileNum = newManifestFileNum
	}
	vs.picker = picker

	if metrics != nil {
		for level, update := range metrics {
			vs.metrics.Levels[level].Add(update)
		}
	}
	for i := range vs.metrics.Levels {
		l := &vs.metrics.Levels[i]
		l.NumFiles = int64(len(newVersion.Files[i]))
		l.Size = uint64(totalSize(newVersion.Files[i]))
	}
	return nil
}

// createManifest creates a manifest file that contains a snapshot of vs.
func (vs *versionSet) createManifest(dirname string, fileNum uint64) (err error) {
	var (
		filename     = base.MakeFilename(dirname, fileTypeManifest, fileNum)
		manifestFile vfs.File
		manifest     *record.Writer
	)
	defer func() {
		if manifest != nil {
			manifest.Close()
		}
		if manifestFile != nil {
			manifestFile.Close()
		}
		if err != nil {
			vs.fs.Remove(filename)
		}
	}()
	manifestFile, err = vs.fs.Create(filename)
	if err != nil {
		return err
	}
	manifest = record.NewWriter(manifestFile)

	snapshot := versionEdit{
		ComparerName: vs.cmpName,
	}
	for level, fileMetadata := range vs.currentVersion().Files {
		for _, meta := range fileMetadata {
			snapshot.NewFiles = append(snapshot.NewFiles, newFileEntry{
				Level: level,
				Meta:  meta,
			})
		}
	}

	w, err1 := manifest.Next()
	if err1 != nil {
		return err1
	}
	if err := snapshot.Encode(w); err != nil {
		return err
	}

	vs.manifest, manifest = manifest, nil
	vs.manifestFile, manifestFile = manifestFile, nil
	return nil
}

func (vs *versionSet) markFileNumUsed(fileNum uint64) {
	if vs.nextFileNum <= fileNum {
		vs.nextFileNum = fileNum + 1
	}
}

func (vs *versionSet) getNextFileNum() uint64 {
	x := vs.nextFileNum
	vs.nextFileNum++
	return x
}

func (vs *versionSet) append(v *version) {
	if v.Refs() != 0 {
		panic("pebble: version should be unreferenced")
	}
	if !vs.versions.Empty() {
		vs.versions.Back().UnrefLocked()
	}
	v.Deleted = vs.obsoleteFn
	v.Ref()
	vs.versions.PushBack(v)
}

func (vs *versionSet) currentVersion() *version {
	return vs.versions.Back()
}

func (vs *versionSet) addLiveFileNums(m map[uint64]struct{}) {
	current := vs.currentVersion()
	for v := vs.versions.Front(); true; v = v.Next() {
		for _, ff := range v.Files {
			for _, f := range ff {
				m[f.FileNum] = struct{}{}
			}
		}
		if v == current {
			break
		}
	}
}

func (vs *versionSet) addObsoleteLocked(obsolete []uint64) {
	vs.obsoleteTables = append(vs.obsoleteTables, obsolete...)
}
