// Copyright 2012 The LevelDB-Go Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package leveldb

import (
	"fmt"
	"io"
	"os"

	"code.google.com/p/leveldb-go/leveldb/db"
	"code.google.com/p/leveldb-go/leveldb/record"
)

// TODO: describe what a versionSet is.
type versionSet struct {
	dirname    string
	opts       *db.Options
	fs         db.FileSystem
	ucmp, icmp db.Comparer

	// dummyVersion is the head of a circular doubly-linked list of versions.
	// dummyVersion.prev is the current version.
	dummyVersion version

	logNumber          uint64
	prevLogNumber      uint64
	nextFileNumber     uint64
	lastSequence       uint64
	manifestFileNumber uint64

	manifestFile db.File
	manifest     *record.Writer
}

// load loads the version set from the manifest file.
func (vs *versionSet) load(dirname string, opts *db.Options) error {
	vs.dirname = dirname
	vs.opts = opts
	vs.fs = opts.GetFileSystem()
	vs.ucmp = opts.GetComparer()
	vs.icmp = internalKeyComparer{vs.ucmp}
	vs.dummyVersion.prev = &vs.dummyVersion
	vs.dummyVersion.next = &vs.dummyVersion
	// For historical reasons, the next file number is initialized to 2.
	vs.nextFileNumber = 2

	// Read the CURRENT file to find the current manifest file.
	current, err := vs.fs.Open(dbFilename(dirname, fileTypeCurrent, 0))
	if err != nil {
		return fmt.Errorf("leveldb: could not open CURRENT file for DB %q: %v", dirname, err)
	}
	defer current.Close()
	stat, err := current.Stat()
	if err != nil {
		return err
	}
	n := stat.Size()
	if n == 0 {
		return fmt.Errorf("leveldb: CURRENT file for DB %q is empty", dirname)
	}
	if n > 4096 {
		return fmt.Errorf("leveldb: CURRENT file for DB %q is too large", dirname)
	}
	b := make([]byte, n)
	_, err = current.ReadAt(b, 0)
	if err != nil {
		return err
	}
	if b[n-1] != '\n' {
		return fmt.Errorf("leveldb: CURRENT file for DB %q is malformed", dirname)
	}
	b = b[:n-1]

	// Read the versionEdits in the manifest file.
	var bve bulkVersionEdit
	manifest, err := vs.fs.Open(dirname + string(os.PathSeparator) + string(b))
	if err != nil {
		return fmt.Errorf("leveldb: could not open manifest file %q for DB %q: %v", b, dirname, err)
	}
	defer manifest.Close()
	rr := record.NewReader(manifest)
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
			if ve.comparatorName != vs.ucmp.Name() {
				return fmt.Errorf("leveldb: manifest file %q for DB %q: "+
					"comparer name from file %q != comparer name from db.Options %q",
					b, dirname, ve.comparatorName, vs.ucmp.Name())
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
			vs.lastSequence = ve.lastSequence
		}
	}
	if vs.logNumber == 0 || vs.nextFileNumber == 0 {
		if vs.nextFileNumber == 2 {
			// We have a freshly created DB.
		} else {
			return fmt.Errorf("leveldb: incomplete manifest file %q for DB %q", b, dirname)
		}
	}
	vs.markFileNumUsed(vs.logNumber)
	vs.markFileNumUsed(vs.prevLogNumber)
	vs.manifestFileNumber = vs.nextFileNum()

	newVersion, err := bve.apply(nil, vs.icmp)
	if err != nil {
		return err
	}
	vs.append(newVersion)
	return nil
}

// TODO: describe what this function does and how it interacts concurrently
// with a running leveldb.
//
// d.mu must be held when calling this, for the enclosing *DB d.
// TODO: actually pass d.mu, and drop and re-acquire it around the I/O.
func (vs *versionSet) logAndApply(dirname string, ve *versionEdit) error {
	if ve.logNumber != 0 {
		if ve.logNumber < vs.logNumber || vs.nextFileNumber <= ve.logNumber {
			panic(fmt.Sprintf("leveldb: inconsistent versionEdit logNumber %d", ve.logNumber))
		}
	}
	ve.nextFileNumber = vs.nextFileNumber
	ve.lastSequence = vs.lastSequence

	var bve bulkVersionEdit
	bve.accumulate(ve)
	newVersion, err := bve.apply(vs.currentVersion(), vs.icmp)
	if err != nil {
		return err
	}

	if vs.manifest == nil {
		if err := vs.createManifest(dirname); err != nil {
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
	if err := setCurrentFile(dirname, vs.opts.GetFileSystem(), vs.manifestFileNumber); err != nil {
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
	return nil
}

// createManifest creates a manifest file that contains a snapshot of vs.
func (vs *versionSet) createManifest(dirname string) (err error) {
	var (
		filename     = dbFilename(dirname, fileTypeManifest, vs.manifestFileNumber)
		manifestFile db.File
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
		comparatorName: vs.ucmp.Name(),
	}
	// TODO: save compaction pointers.
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
	err1 = snapshot.encode(w)
	if err1 != nil {
		return err1
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
	if v.prev != nil || v.next != nil {
		panic("leveldb: version linked list is inconsistent")
	}
	v.prev = vs.dummyVersion.prev
	v.prev.next = v
	v.next = &vs.dummyVersion
	v.next.prev = v
}

func (vs *versionSet) currentVersion() *version {
	return vs.dummyVersion.prev
}

func (vs *versionSet) addLiveFileNums(m map[uint64]struct{}) {
	for v := vs.dummyVersion.next; v != &vs.dummyVersion; v = v.next {
		for _, ff := range v.files {
			for _, f := range ff {
				m[f.fileNum] = struct{}{}
			}
		}
	}
}
