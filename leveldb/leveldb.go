// Copyright 2012 The LevelDB-Go Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

// Package leveldb provides an ordered key/value store.
//
// BUG: This package is incomplete.
package leveldb

import (
	"bytes"
	"fmt"
	"io"
	"path/filepath"
	"sort"

	"code.google.com/p/leveldb-go/leveldb/db"
	"code.google.com/p/leveldb-go/leveldb/memdb"
	"code.google.com/p/leveldb-go/leveldb/record"
)

// TODO: document DB.
type DB struct {
	dirname string
	opts    *db.Options
	icmp    internalKeyComparer

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

func (d *DB) Apply(batch Batch, opts *db.WriteOptions) error {
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

type fileNumAndName struct {
	num  uint64
	name string
}

type fileNumAndNameSlice []fileNumAndName

func (p fileNumAndNameSlice) Len() int           { return len(p) }
func (p fileNumAndNameSlice) Less(i, j int) bool { return p[i].num < p[j].num }
func (p fileNumAndNameSlice) Swap(i, j int)      { p[i], p[j] = p[j], p[i] }

// Open opens a LevelDB whose files live in the given directory.
func Open(dirname string, opts *db.Options) (*DB, error) {
	d := &DB{
		dirname: dirname,
		opts:    opts,
		icmp:    internalKeyComparer{opts.GetComparer()},
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

	// TODO: add options for CreateIfMissing and ErrorIfExists, and check them here.

	// Load the version set.
	err = d.versions.load(dirname, opts)
	if err != nil {
		return nil, err
	}

	// Replay any newer log files than the ones named in the manifest.
	ls, err := fs.List(dirname)
	if err != nil {
		return nil, err
	}
	var logFiles fileNumAndNameSlice
	for _, filename := range ls {
		n := logFileNum(filename)
		if n != 0 && (n >= d.versions.logNumber || n == d.versions.prevLogNumber) {
			logFiles = append(logFiles, fileNumAndName{n, filename})
		}
	}
	sort.Sort(logFiles)
	for _, lf := range logFiles {
		maxSeqNum, err := d.replayLogFile(fs, filepath.Join(dirname, lf.name))
		if err != nil {
			return nil, err
		}
		d.versions.markFileNumUsed(lf.num)
		if d.versions.lastSequence < maxSeqNum {
			d.versions.lastSequence = maxSeqNum
		}
	}

	// TODO: write a new manifest to disk.

	d.fileLock, fileLock = fileLock, nil
	return d, nil
}

func (d *DB) replayLogFile(fs db.FileSystem, filename string) (maxSeqNum uint64, err error) {
	file, err := fs.Open(filename)
	if err != nil {
		return 0, err
	}
	defer file.Close()

	var (
		mem      *memdb.MemDB
		batchBuf = new(bytes.Buffer)
		ikeyBuf  = make(internalKey, 512)
		rr       = record.NewReader(file)
	)
	for {
		r, err := rr.Next()
		if err == io.EOF {
			break
		}
		if err != nil {
			return 0, err
		}
		_, err = io.Copy(batchBuf, r)
		if err != nil {
			return 0, err
		}

		if batchBuf.Len() < batchHeaderLen {
			return 0, fmt.Errorf("leveldb: corrupt log file %q", filename)
		}
		b := Batch{batchBuf.Bytes()}
		seqNum := b.seqNum()
		seqNum1 := seqNum + uint64(b.count())
		if maxSeqNum < seqNum1 {
			maxSeqNum = seqNum1
		}

		if mem == nil {
			mem = memdb.New(&db.Options{
				Comparer: d.icmp,
			})
		}

		t := b.iter()
		for ; seqNum != seqNum1; seqNum++ {
			kind, key, value, ok := t.next()
			if !ok {
				return 0, fmt.Errorf("leveldb: corrupt log file %q", filename)
			}
			// Convert seqNum, kind and key into an internalKey, and add that ikey/value
			// pair to mem.
			//
			// TODO: instead of copying to an intermediate buffer (ikeyBuf), is it worth
			// adding a SetTwoPartKey(db.TwoPartKey{key0, key1}, value, opts) method to
			// memdb.MemDB? What effect does that have on the db.Comparer interface?
			//
			// The C++ LevelDB code does not need an intermediate copy because its memdb
			// implementation is a private implementation detail, and copies each internal
			// key component from the Batch format straight to the skiplist buffer.
			//
			// Go's LevelDB considers the memdb functionality to be useful in its own
			// right, and so leveldb/memdb is a separate package that is usable without
			// having to import the top-level leveldb package. That extra abstraction
			// means that we need to copy to an intermediate buffer here, to reconstruct
			// the complete internal key to pass to the memdb.
			if n := len(ikeyBuf); n < len(key)+8 {
				for {
					n *= 2
					if n >= len(key)+8 {
						break
					}
				}
				ikeyBuf = make(internalKey, n)
			}
			ikey := ikeyBuf[:len(key)+8]
			copy(ikey, key)
			ikey.encodeTrailer(kind, seqNum)
			mem.Set(ikey, value, nil)
		}
		if len(t) != 0 {
			return 0, fmt.Errorf("leveldb: corrupt log file %q", filename)
		}

		// TODO: if mem is large enough, write it to a level-0 table and set mem = nil.

		batchBuf.Reset()
	}

	// TODO: write mem to a level-0 table.

	const debug = false
	if debug && mem != nil {
		t := mem.Find(nil, nil)
		for t.Next() {
			fmt.Printf("key=%q, value=%q\n", t.Key(), t.Value())
		}
		if err := t.Close(); err != nil {
			return 0, err
		}
		fmt.Println()
	}

	return maxSeqNum, nil
}
