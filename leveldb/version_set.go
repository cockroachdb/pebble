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
}

// load loads the version set from the manifest file.
func (s *versionSet) load(dirname string, opts *db.Options) error {
	fs := opts.GetFileSystem()

	// Read the CURRENT file to find the current manifest file.
	current, err := fs.Open(filename(dirname, fileTypeCurrent, 0))
	if err != nil {
		return fmt.Errorf("leveldb: could not open CURRENT file for %q: %v", dirname, err)
	}
	defer current.Close()
	stat, err := current.Stat()
	if err != nil {
		return err
	}
	n := stat.Size()
	if n == 0 {
		return fmt.Errorf("leveldb: CURRENT file for %q is empty", dirname)
	}
	if n > 4096 {
		return fmt.Errorf("leveldb: CURRENT file for %q is too large", dirname)
	}
	b := make([]byte, n)
	_, err = current.ReadAt(b, 0)
	if err != nil {
		return err
	}
	if b[n-1] != '\n' {
		return fmt.Errorf("leveldb: CURRENT file for %q is malformed", dirname)
	}
	b = b[:n-1]

	// Read the versionEdits in the manifest file.
	manifest, err := fs.Open(dirname + string(os.PathSeparator) + string(b))
	if err != nil {
		return fmt.Errorf("leveldb: could not open manifest file %q for %q: %v", b, dirname, err)
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
			if s, t := ve.comparatorName, opts.GetComparer().Name(); s != t {
				return fmt.Errorf("leveldb: comparer name from file %q != comparer name from db.Options %q", s, t)
			}
		}
		// TODO: apply the versionEdit to this versionSet.
	}
	return nil
}
