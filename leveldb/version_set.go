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
	dirname string
	opts    *db.Options
	icmp    db.Comparer

	dummyVersion version

	logNumber          uint64
	prevLogNumber      uint64
	nextFileNumber     uint64
	lastSequence       uint64
	manifestFileNumber uint64
}

// load loads the version set from the manifest file.
func (vs *versionSet) load(dirname string, opts *db.Options) error {
	vs.dirname = dirname
	vs.opts = opts
	vs.icmp = internalKeyComparer{opts.GetComparer()}
	vs.dummyVersion.prev = &vs.dummyVersion
	vs.dummyVersion.next = &vs.dummyVersion
	// For historical reasons, the next file number is initialized to 2.
	vs.nextFileNumber = 2

	fs := opts.GetFileSystem()
	cmpName := opts.GetComparer().Name()

	// Read the CURRENT file to find the current manifest file.
	current, err := fs.Open(dbFilename(dirname, fileTypeCurrent, 0))
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
	manifest, err := fs.Open(dirname + string(os.PathSeparator) + string(b))
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
			if ve.comparatorName != cmpName {
				return fmt.Errorf("leveldb: manifest file %q for DB %q: "+
					"comparer name from file %q != comparer name from db.Options %q",
					b, dirname, ve.comparatorName, cmpName)
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
		return fmt.Errorf("leveldb: incomplete manifest file %q for DB %q", b, dirname)
	}
	vs.markFileNumUsed(vs.logNumber)
	vs.markFileNumUsed(vs.prevLogNumber)
	vs.manifestFileNumber = vs.nextFileNum()

	newVersion, err := bve.apply(nil, vs.icmp)
	if err != nil {
		return err
	}
	vs.append(newVersion)

	// TODO: compute the compaction score. The C++ code calls this VersionSet::Finalize.

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
