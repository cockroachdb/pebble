// Copyright 2018 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

// +build boltdb

package main

import (
	"bytes"
	"log"

	"github.com/cockroachdb/pebble"
	"github.com/ngaut/bolt"
)

// Adapters for BoltDB
type boltDB struct {
	db     *bolt.DB
	bucket []byte
}

func newBoltDB(dir string) DB {
	db, err := bolt.Open(dir, 0755, &bolt.Options{})
	if err != nil {
		log.Fatal(err)
	}

	bucket := []byte("test")
	db.Update(func(tx *bolt.Tx) error {
		if _, err := tx.CreateBucketIfNotExists(bucket); err != nil {
			log.Fatalf("create bucket: %s", err)
		}
		return nil
	})
	return &boltDB{db: db, bucket: bucket}
}

func (b boltDB) NewIter(opts *pebble.IterOptions) iterator {
	tx, err := b.db.Begin(false)
	if err != nil {
		log.Fatal(err)
	}
	cursor := tx.Bucket(b.bucket).Cursor()
	return &boltDBIterator{
		tx:     tx,
		cursor: cursor,
		lower:  opts.GetLowerBound(),
		upper:  opts.GetUpperBound(),
	}
}

func (b boltDB) NewBatch() batch {
	tx, err := b.db.Begin(true)
	if err != nil {
		log.Fatal(err)
	}
	bucket := tx.Bucket(b.bucket)
	return boltDBBatch{tx: tx, bucket: bucket}
}

func (b boltDB) Scan(key []byte, count int64, reverse bool) error {
	panic("boltDB.Scan: unimplemented")
}

func (b boltDB) Metrics() *pebble.VersionMetrics {
	return &pebble.VersionMetrics{}
}

func (b boltDB) Flush() error {
	// NB: Flushing is a no-op with BoltDB.
	return nil
}

type boltDBIterator struct {
	tx     *bolt.Tx
	cursor *bolt.Cursor
	key    []byte
	value  []byte
	lower  []byte
	upper  []byte
}

func (i *boltDBIterator) SeekGE(key []byte) bool {
	i.key, i.value = i.cursor.Seek(key)
	if i.key != nil && i.upper != nil && bytes.Compare(i.key, i.upper) >= 0 {
		i.key = nil
	}
	return i.key != nil
}

func (i *boltDBIterator) Valid() bool {
	return i.key != nil
}

func (i *boltDBIterator) Key() []byte {
	return i.key
}

func (i *boltDBIterator) Value() []byte {
	return i.value
}

func (i *boltDBIterator) First() bool {
	if i.lower != nil {
		return i.SeekGE(i.lower)
	}
	i.key, i.value = i.cursor.First()
	if i.key != nil && i.upper != nil && bytes.Compare(i.key, i.upper) >= 0 {
		i.key = nil
	}
	return i.key != nil
}

func (i *boltDBIterator) Next() bool {
	i.key, i.value = i.cursor.Next()
	if i.key != nil && i.upper != nil && bytes.Compare(i.key, i.upper) >= 0 {
		i.key = nil
	}
	return i.key != nil
}

func (i *boltDBIterator) Last() bool {
	if i.upper != nil {
		return i.SeekGE(i.upper)
	}
	i.key, i.value = i.cursor.Last()
	if i.key != nil && i.lower != nil && bytes.Compare(i.key, i.lower) < 0 {
		i.key = nil
	}
	return i.key != nil
}

func (i *boltDBIterator) Prev() bool {
	i.key, i.value = i.cursor.Prev()
	if i.key != nil && i.lower != nil && bytes.Compare(i.key, i.lower) < 0 {
		i.key = nil
	}
	return i.key != nil
}

func (i *boltDBIterator) Close() error {
	i.tx.Rollback()
	return nil
}

type boltDBBatch struct {
	tx     *bolt.Tx
	bucket *bolt.Bucket
}

func (b boltDBBatch) Commit(opts *pebble.WriteOptions) error {
	return b.tx.Commit()
}

func (b boltDBBatch) Set(key, value []byte, _ *pebble.WriteOptions) error {
	return b.bucket.Put(key, value)
}

func (b boltDBBatch) LogData(data []byte, _ *pebble.WriteOptions) error {
	panic("boltDBBatch.logData: unimplemented")
}

func (b boltDBBatch) Repr() []byte {
	return nil
}
