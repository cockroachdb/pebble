// Copyright 2019 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

// +build rocksdb

package main

import (
	"log"
	"strconv"
	"strings"

	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/storage/engine"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/pebble"
)

// Adapters for rocksDB
type rocksDB struct {
	d       *engine.RocksDB
	ballast []byte
}

func newRocksDB(dir string) DB {
	// TODO: match Pebble / Rocks options
	r, err := engine.NewRocksDB(
		engine.RocksDBConfig{
			Dir: dir,
		},
		engine.NewRocksDBCache(cacheSize),
	)
	if err != nil {
		log.Fatal(err)
	}
	return rocksDB{
		d:       r,
		ballast: make([]byte, 1<<30),
	}
}

type rocksDBIterator struct {
	iter       engine.Iterator
	lowerBound []byte
	upperBound []byte
}

type rocksDBBatch struct {
	batch engine.Batch
}

func (i rocksDBIterator) SeekGE(key []byte) bool {
	// TODO: unnecessary overhead here. Change the interface.
	userKey, _, ok := mvccSplitKey(key)
	if !ok {
		panic("mvccSplitKey failed")
	}
	i.iter.Seek(engine.MVCCKey{
		Key: userKey,
	})
	return i.Valid()
}

func (i rocksDBIterator) Valid() bool {
	valid, _ := i.iter.Valid()
	return valid
}

func (i rocksDBIterator) Key() []byte {
	key := i.iter.Key()
	return []byte(key.Key)
}

func (i rocksDBIterator) Value() []byte {
	return i.iter.Value()
}

func (i rocksDBIterator) First() bool {
	return i.SeekGE(i.lowerBound)
}

func (i rocksDBIterator) Next() bool {
	i.iter.Next()
	valid, _ := i.iter.Valid()
	return valid
}

func (i rocksDBIterator) Last() bool {
	// TODO: unnecessary overhead here. Change the interface.
	userKey, _, ok := mvccSplitKey(i.upperBound)
	if !ok {
		panic("mvccSplitKey failed")
	}
	i.iter.SeekReverse(engine.MVCCKey{
		Key: userKey,
	})
	return i.Valid()
}

func (i rocksDBIterator) Prev() bool {
	i.iter.Prev()
	valid, _ := i.iter.Valid()
	return valid
}

func (i rocksDBIterator) Close() error {
	i.iter.Close()
	return nil
}

func (b rocksDBBatch) Commit(opts *pebble.WriteOptions) error {
	return b.batch.Commit(opts.Sync)
}

func (b rocksDBBatch) Set(key, value []byte, _ *pebble.WriteOptions) error {
	// TODO: unnecessary overhead here. Change the interface.
	userKey, _, ok := mvccSplitKey(key)
	if !ok {
		panic("mvccSplitKey failed")
	}
	ts := hlc.Timestamp{WallTime: 1}
	return b.batch.Put(engine.MVCCKey{Key: userKey, Timestamp: ts}, value)
}

func (b rocksDBBatch) LogData(data []byte, _ *pebble.WriteOptions) error {
	return b.batch.LogData(data)
}

func (b rocksDBBatch) Repr() []byte {
	return b.batch.Repr()
}

func (r rocksDB) Flush() error {
	return r.d.Flush()
}

func (r rocksDB) NewIter(opts *pebble.IterOptions) iterator {
	ropts := engine.IterOptions{}
	if opts != nil {
		ropts.LowerBound = opts.LowerBound
		ropts.UpperBound = opts.UpperBound
	} else {
		ropts.UpperBound = roachpb.KeyMax
	}
	iter := r.d.NewIterator(ropts)
	return rocksDBIterator{
		iter:       iter,
		lowerBound: ropts.LowerBound,
		upperBound: ropts.UpperBound,
	}
}

func (r rocksDB) NewBatch() batch {
	return rocksDBBatch{r.d.NewBatch()}
}

func (r rocksDB) Scan(key []byte, count int64, reverse bool) error {
	// TODO: unnecessary overhead here. Change the interface.
	beginKey, _, ok := mvccSplitKey(key)
	if !ok {
		panic("mvccSplitKey failed")
	}
	endKey := roachpb.KeyMax
	ropts := engine.IterOptions{
		LowerBound: key,
	}
	if reverse {
		endKey = beginKey
		beginKey = roachpb.KeyMin
		ropts.UpperBound = key
		ropts.LowerBound = nil
	}

	iter := r.d.NewIterator(ropts)
	defer iter.Close()
	// We hard code a timestamp with walltime=1 in the data, so we just have to
	// use a larger timestamp here (walltime=10).
	ts := hlc.Timestamp{WallTime: 10}
	_, numKVs, _, intents, err := iter.MVCCScan(
		beginKey, endKey, count, ts, engine.MVCCScanOptions{Reverse: reverse},
	)
	if numKVs > count {
		panic("MVCCScan returned too many keys")
	}
	if len(intents) > 0 {
		panic("MVCCScan found intents")
	}
	return err
}

func (r rocksDB) Metrics() *pebble.VersionMetrics {
	stats := r.d.GetCompactionStats()
	var inLevelsSection bool
	var vMetrics pebble.VersionMetrics
	for _, line := range strings.Split(stats, "\n") {
		if strings.HasPrefix(line, "-----") {
			continue
		}
		if !inLevelsSection && strings.HasPrefix(line, "Level") {
			inLevelsSection = true
			continue
		}
		if strings.HasPrefix(line, "Flush(GB):") {
			// line looks like:
			// "Flush(GB): cumulative 0.302, interval 0.302"
			// pretend cumulative flush is WAL size and L0 input since we don't have
			// access to WAL stats in rocks.
			// TODO: this is slightly different than Pebble which uses the real physical
			// WAL size. This way prevents compression ratio from affecting write-amp,
			// but it also prevents apples-to-apples w-amp comparison.
			fields := strings.Fields(line)
			field := fields[2]
			walWrittenGB, _ := strconv.ParseFloat(field[0:len(field)-1], 64)
			vMetrics.Levels[0].BytesIn = uint64(1024.0 * 1024.0 * 1024.0 * walWrittenGB)
			vMetrics.WAL.BytesWritten = vMetrics.Levels[0].BytesIn
		}
		if inLevelsSection && strings.HasPrefix(line, " Sum") {
			inLevelsSection = false
			continue
		}
		if inLevelsSection {
			fields := strings.Fields(line)
			level, _ := strconv.Atoi(fields[0][1:])
			if level < 0 || level > 6 {
				panic("expected at most 7 levels")
			}
			vMetrics.Levels[level].NumFiles, _ = strconv.ParseInt(strings.Split(fields[1], "/")[0], 10, 64)
			size, _ := strconv.ParseFloat(fields[2], 64)
			if fields[3] == "KB" {
				size *= 1024.0
			} else if fields[3] == "MB" {
				size *= 1024.0 * 1024.0
			} else if fields[3] == "GB" {
				size *= 1024.0 * 1024.0 * 1024.0
			} else {
				panic("unknown unit")
			}
			vMetrics.Levels[level].Size = uint64(size)
			vMetrics.Levels[level].Score, _ = strconv.ParseFloat(fields[4], 64)
			if level > 0 {
				bytesInGB, _ := strconv.ParseFloat(fields[6], 64)
				vMetrics.Levels[level].BytesIn = uint64(1024.0 * 1024.0 * 1024.0 * bytesInGB)
			}
			bytesMovedGB, _ := strconv.ParseFloat(fields[10], 64)
			vMetrics.Levels[level].BytesMoved = uint64(1024.0 * 1024.0 * 1024.0 * bytesMovedGB)
			bytesReadGB, _ := strconv.ParseFloat(fields[5], 64)
			vMetrics.Levels[level].BytesRead = uint64(1024.0 * 1024.0 * 1024.0 * bytesReadGB)
			bytesWrittenGB, _ := strconv.ParseFloat(fields[8], 64)
			vMetrics.Levels[level].BytesWritten = uint64(1024.0 * 1024.0 * 1024.0 * bytesWrittenGB)
		}
	}
	return &vMetrics
}
