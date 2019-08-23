// Copyright 2019 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package main

import (
	"log"

	"github.com/petermattis/pebble"
	"github.com/petermattis/pebble/cache"
	"github.com/petermattis/pebble/internal/bytealloc"
)

// DB specifies the minimal interfaces that need to be implemented to support
// the pebble command.
type DB interface {
	NewIter(*pebble.IterOptions) iterator
	NewBatch() batch
	Scan(key []byte, count int64, reverse bool) error
	Metrics() *pebble.VersionMetrics
	Flush() error
}

type iterator interface {
	SeekGE(key []byte) bool
	Valid() bool
	Key() []byte
	Value() []byte
	First() bool
	Next() bool
	Last() bool
	Prev() bool
	Close() error
}

type batch interface {
	Commit(opts *pebble.WriteOptions) error
	Set(key, value []byte, opts *pebble.WriteOptions) error
	LogData(data []byte, opts *pebble.WriteOptions) error
	Repr() []byte
}

// Adapters for Pebble. Since the interfaces above are based on Pebble's
// interfaces, it can simply forward calls for everything.
type pebbleDB struct {
	d *pebble.DB
	// These channels are used to convey when write stall mode begins/ends.
	// Write stalls are slowdown triggers for `--find-peak-ops-per-sec`. If
	// that option is unset then these will both be nil.
	stallBegan chan struct{}
	stallEnded chan struct{}
}

func newPebbleDB(dir string) DB {
	opts := &pebble.Options{
		Cache:                       cache.New(cacheSize),
		Comparer:                    mvccComparer,
		DisableWAL:                  disableWAL,
		MemTableSize:                64 << 20,
		MemTableStopWritesThreshold: 4,
		MinCompactionRate:           4 << 20, // 4 MB/s
		MinFlushRate:                1 << 20, // 1 MB/s
		L0CompactionThreshold:       2,
		L0StopWritesThreshold:       32,
		LBaseMaxBytes:               64 << 20, // 64 MB
		Levels: []pebble.LevelOptions{{
			BlockSize: 32 << 10,
		}},
		Merger: &pebble.Merger{
			Name: "cockroach_merge_operator",
		},
	}
	opts.EnsureDefaults()

	if verbose {
		opts.EventListener = pebble.MakeLoggingEventListener(nil)
		opts.EventListener.TableDeleted = nil
		opts.EventListener.TableIngested = nil
		opts.EventListener.WALCreated = nil
		opts.EventListener.WALDeleted = nil
	}
	var stallBegan, stallEnded chan struct{}
	if findPeakOpsPerSec {
		stallBegan = make(chan struct{}, 1)
		stallEnded = make(chan struct{}, 1)
		origWriteStallBegin := opts.EventListener.WriteStallBegin
		opts.EventListener.WriteStallBegin = func(info pebble.WriteStallBeginInfo) {
			stallBegan <- struct{}{}
			if origWriteStallBegin != nil {
				origWriteStallBegin(info)
			}
		}
		origWriteStallEnd := opts.EventListener.WriteStallEnd
		opts.EventListener.WriteStallEnd = func() {
			stallEnded <- struct{}{}
			if origWriteStallEnd != nil {
				origWriteStallEnd()
			}
		}
	}

	p, err := pebble.Open(dir, opts)
	if err != nil {
		log.Fatal(err)
	}
	return pebbleDB{p, stallBegan, stallEnded}
}

func (p pebbleDB) Flush() error {
	return p.d.Flush()
}

func (p pebbleDB) NewIter(opts *pebble.IterOptions) iterator {
	return p.d.NewIter(opts)
}

func (p pebbleDB) NewBatch() batch {
	return p.d.NewBatch()
}

func (p pebbleDB) Scan(key []byte, count int64, reverse bool) error {
	var data bytealloc.A
	iter := p.d.NewIter(nil)
	if reverse {
		for i, valid := 0, iter.SeekLT(key); valid; valid = iter.Prev() {
			data, _ = data.Copy(iter.Key())
			data, _ = data.Copy(iter.Value())
			i++
			if i >= int(count) {
				break
			}
		}
	} else {
		for i, valid := 0, iter.SeekGE(key); valid; valid = iter.Next() {
			data, _ = data.Copy(iter.Key())
			data, _ = data.Copy(iter.Value())
			i++
			if i >= int(count) {
				break
			}
		}
	}
	return iter.Close()
}

func (p pebbleDB) Metrics() *pebble.VersionMetrics {
	return p.d.Metrics()
}
