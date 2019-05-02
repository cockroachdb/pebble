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

	"github.com/petermattis/pebble/db"
	"github.com/petermattis/pebble/internal/record"
	"github.com/petermattis/pebble/vfs"
)

// versionSet manages a collection of immutable versions, and manages the
// creation of a new version from the most recent version. A new versions is
// created from an existing version by applying a version edit which is just
// like it sounds: a delta from the previous version. Version edits are logged
// to the manifest file, which is replayed at startup.
type versionSet struct {
	// Immutable fields.
	dirname string
	mu      *sync.Mutex
	opts    *db.Options
	fs      vfs.FS
	cmp     db.Compare
	cmpName string
	// Dynamic base level allows the dynamic base level computation to be
	// disabled. Used by tests which want to create specific LSM structures.
	dynamicBaseLevel bool

	// Mutable fields.
	versions versionList
	picker   *compactionPicker

	obsoleteTables    []uint64
	obsoleteManifests []uint64
	obsoleteOptions   []uint64

	logNumber          uint64
	prevLogNumber      uint64
	nextFileNumber     uint64
	logSeqNum          uint64 // next seqNum to use for WAL writes
	visibleSeqNum      uint64 // visible seqNum (<= logSeqNum)
	manifestFileNumber uint64

	manifestFile vfs.File
	manifest     *record.Writer

	writing    bool
	writerCond sync.Cond
}

// load loads the version set from the manifest file.
func (vs *versionSet) load(dirname string, opts *db.Options, mu *sync.Mutex) error {
	vs.dirname = dirname
	vs.mu = mu
	vs.versions.mu = mu
	vs.writerCond.L = mu
	vs.opts = opts
	vs.fs = opts.VFS
	vs.cmp = opts.Comparer.Compare
	vs.cmpName = opts.Comparer.Name
	vs.dynamicBaseLevel = true
	vs.versions.init()
	// For historical reasons, the next file number is initialized to 2.
	vs.nextFileNumber = 2

	// Read the CURRENT file to find the current manifest file.
	current, err := vs.fs.Open(dbFilename(dirname, fileTypeCurrent, 0))
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
		err = ve.decode(r)
		if err != nil {
			return err
		}
		if ve.comparatorName != "" {
			if ve.comparatorName != vs.cmpName {
				return fmt.Errorf("pebble: manifest file %q for DB %q: "+
					"comparer name from file %q != comparer name from db.Options %q",
					b, dirname, ve.comparatorName, vs.cmpName)
			}
		}
		bve.accumulate(&ve)
		if ve.logNumber != 0 {
			vs.logNumber = ve.logNumber
		}
		if ve.prevLogNumber != 0 {
			vs.prevLogNumber = ve.prevLogNumber
		}
		if ve.nextFileNumber != 0 {
			vs.nextFileNumber = ve.nextFileNumber
		}
		if ve.lastSequence != 0 {
			vs.logSeqNum = ve.lastSequence
		}
	}
	if vs.logNumber == 0 || vs.nextFileNumber == 0 {
		if vs.nextFileNumber == 2 {
			// We have a freshly created DB.
		} else {
			return fmt.Errorf("pebble: incomplete manifest file %q for DB %q", b, dirname)
		}
	}
	vs.markFileNumUsed(vs.logNumber)
	vs.markFileNumUsed(vs.prevLogNumber)

	newVersion, err := bve.apply(opts, nil, vs.cmp)
	if err != nil {
		return err
	}
	vs.append(newVersion)
	return nil
}

// logAndApply logs the version edit to the manifest, applies the version edit
// to the current version, and installs the new version. DB.mu must be held
// when calling this method and will be released temporarily while performing
// file I/O.
func (vs *versionSet) logAndApply(ve *versionEdit) error {
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

	if ve.logNumber != 0 {
		if ve.logNumber < vs.logNumber || vs.nextFileNumber <= ve.logNumber {
			panic(fmt.Sprintf("pebble: inconsistent versionEdit logNumber %d", ve.logNumber))
		}
	}
	ve.nextFileNumber = vs.nextFileNumber
	ve.lastSequence = atomic.LoadUint64(&vs.logSeqNum)
	currentVersion := vs.currentVersion()
	var newVersion *version

	// Generate a new manifest if we don't currently have one, or the current one
	// is too large.
	var newManifestFileNumber uint64
	if vs.manifest == nil || vs.manifest.Size() >= vs.opts.MaxManifestFileSize {
		newManifestFileNumber = vs.nextFileNum()
	}

	var picker *compactionPicker
	if err := func() error {
		vs.mu.Unlock()
		defer vs.mu.Lock()

		var bve bulkVersionEdit
		bve.accumulate(ve)

		var err error
		newVersion, err = bve.apply(vs.opts, currentVersion, vs.cmp)
		if err != nil {
			return err
		}

		if newManifestFileNumber != 0 {
			if err := vs.createManifest(vs.dirname, newManifestFileNumber); err != nil {
				return err
			}
		}

		w, err := vs.manifest.Next()
		if err != nil {
			return err
		}
		if err := ve.encode(w); err != nil {
			return err
		}
		if err := vs.manifest.Flush(); err != nil {
			return err
		}
		if err := vs.manifestFile.Sync(); err != nil {
			return err
		}
		if newManifestFileNumber != 0 {
			if err := setCurrentFile(vs.dirname, vs.fs, newManifestFileNumber); err != nil {
				return err
			}
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
	if ve.logNumber != 0 {
		vs.logNumber = ve.logNumber
	}
	if ve.prevLogNumber != 0 {
		vs.prevLogNumber = ve.prevLogNumber
	}
	if newManifestFileNumber != 0 {
		if vs.manifestFileNumber != 0 {
			vs.obsoleteManifests = append(vs.obsoleteManifests, vs.manifestFileNumber)
		}
		vs.manifestFileNumber = newManifestFileNumber
	}
	vs.picker = picker
	return nil
}

// createManifest creates a manifest file that contains a snapshot of vs.
func (vs *versionSet) createManifest(dirname string, fileNum uint64) (err error) {
	var (
		filename     = dbFilename(dirname, fileTypeManifest, fileNum)
		manifestFile vfs.File
		manifest     *record.Writer
	)
	defer func() {
		if manifest != nil {
			manifest.Close()
		}
		if manifestFile != nil {
			manifestFile.Sync()
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
		comparatorName: vs.cmpName,
	}
	for level, fileMetadata := range vs.currentVersion().files {
		for _, meta := range fileMetadata {
			snapshot.newFiles = append(snapshot.newFiles, newFileEntry{
				level: level,
				meta:  meta,
			})
		}
	}

	w, err1 := manifest.Next()
	if err1 != nil {
		return err1
	}
	if err := snapshot.encode(w); err != nil {
		return err
	}

	vs.manifest, manifest = manifest, nil
	vs.manifestFile, manifestFile = manifestFile, nil
	return nil
}

func (vs *versionSet) markFileNumUsed(fileNum uint64) {
	if vs.nextFileNumber <= fileNum {
		vs.nextFileNumber = fileNum + 1
	}
}

func (vs *versionSet) nextFileNum() uint64 {
	x := vs.nextFileNumber
	vs.nextFileNumber++
	return x
}

func (vs *versionSet) append(v *version) {
	if v.refs != 0 {
		panic("pebble: version should be unreferenced")
	}
	if !vs.versions.empty() {
		vs.versions.back().unrefLocked()
	}
	v.vs = vs
	v.ref()
	vs.versions.pushBack(v)
}

func (vs *versionSet) currentVersion() *version {
	return vs.versions.back()
}

func (vs *versionSet) addLiveFileNums(m map[uint64]struct{}) {
	for v := vs.versions.root.next; v != &vs.versions.root; v = v.next {
		for _, ff := range v.files {
			for _, f := range ff {
				m[f.fileNum] = struct{}{}
			}
		}
	}
}

func (vs *versionSet) addObsoleteLocked(obsolete []uint64) {
	vs.obsoleteTables = append(vs.obsoleteTables, obsolete...)
}
