// Copyright 2012 The LevelDB-Go Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

// Package leveldb provides an ordered key/value store.
//
// BUG: This package is incomplete.
package leveldb

import (
	"io"

	"code.google.com/p/leveldb-go/leveldb/db"
)

// TODO: document DB.
type DB struct {
	dirname string
	opts    *db.Options

	fileLock io.Closer

	versions versionSet
}

var _ db.DB = (*DB)(nil)

func (d *DB) Get(key []byte, opts *db.ReadOptions) ([]byte, error) {
	panic("unimplemented")
}

func (d *DB) Set(key, value []byte, opts *db.WriteOptions) error {
	panic("unimplemented")
}

func (d *DB) Delete(key []byte, opts *db.WriteOptions) error {
	panic("unimplemented")
}

func (d *DB) Find(key []byte, opts *db.ReadOptions) db.Iterator {
	panic("unimplemented")
}

func (d *DB) Close() error {
	if d.fileLock == nil {
		return nil
	}
	err := d.fileLock.Close()
	d.fileLock = nil
	return err
}

// Open opens a LevelDB whose files live in the given directory.
func Open(dirname string, opts *db.Options) (*DB, error) {
	d := &DB{
		dirname: dirname,
		opts:    opts,
	}
	fs := opts.GetFileSystem()

	// Lock the database directory.
	err := fs.MkdirAll(dirname, 0755)
	if err != nil {
		return nil, err
	}
	fileLock, err := fs.Lock(filename(dirname, fileTypeLock, 0))
	if err != nil {
		return nil, err
	}
	defer func() {
		if fileLock != nil {
			fileLock.Close()
		}
	}()

	// Load the version set.
	err = d.versions.load(dirname, opts)
	if err != nil {
		return nil, err
	}

	d.fileLock, fileLock = fileLock, nil
	return d, nil
}
