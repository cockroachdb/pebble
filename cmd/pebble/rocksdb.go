// Copyright 2019 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

// +build rocksdb

package main

import (
	"context"
	"log"
	"strconv"
	"strings"

	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/storage"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/pebble"
)

// Adapters for rocksDB
type rocksDB struct {
	d       *storage.RocksDB
	ballast []byte
}

func newRocksDB(dir string) DB {
	// TODO: match Pebble / Rocks options
	cfg := storage.RocksDBConfig{}
	cfg.Dir = dir
	r, err := storage.NewRocksDB(cfg, storage.NewRocksDBCache(cacheSize))
	if err != nil {
		log.Fatal(err)
	}
	return rocksDB{
		d:       r,
		ballast: make([]byte, 1<<30),
	}
}

type rocksDBIterator struct {
	iter       storage.Iterator
	lowerBound []byte
	upperBound []byte
}

type rocksDBBatch struct {
	batch storage.Batch
}

func (i rocksDBIterator) SeekGE(key []byte) bool {
	// TODO: unnecessary overhead here. Change the interface.
	userKey, _, ok := mvccSplitKey(key)
	if !ok {
		panic("mvccSplitKey failed")
	}
	i.iter.SeekGE(storage.MVCCKey{
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
	i.iter.SeekLT(storage.MVCCKey{
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

func (b rocksDBBatch) Close() error {
	b.batch.Close()
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
	return b.batch.Put(storage.MVCCKey{Key: userKey, Timestamp: ts}, value)
}

func (b rocksDBBatch) LogData(data []byte, _ *pebble.WriteOptions) error {
	return b.batch.LogData(data)
}

func (r rocksDB) Flush() error {
	return r.d.Flush()
}

func (r rocksDB) NewIter(opts *pebble.IterOptions) iterator {
	ropts := storage.IterOptions{}
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

func (r rocksDB) Scan(iter iterator, key []byte, count int64, reverse bool) error {
	// TODO: unnecessary overhead here. Change the interface.
	beginKey, _, ok := mvccSplitKey(key)
	if !ok {
		panic("mvccSplitKey failed")
	}
	endKey := roachpb.KeyMax
	if reverse {
		endKey = beginKey
		beginKey = roachpb.KeyMin
	}

	// We hard code a timestamp with walltime=1 in the data, so we just have to
	// use a larger timestamp here (walltime=10).
	ts := hlc.Timestamp{WallTime: 10}
	res, err := storage.MVCCScanToBytes(
		context.Background(), r.d, beginKey, endKey, ts,
		storage.MVCCScanOptions{Reverse: reverse, MaxKeys: count},
	)
	if res.NumKeys > count {
		panic("MVCCScan returned too many keys")
	}
	if len(res.Intents) > 0 {
		panic("MVCCScan found intents")
	}
	return err
}

func (r rocksDB) Metrics() *pebble.Metrics {
	stats := r.d.GetCompactionStats()
	var inLevelsSection bool
	var vMetrics pebble.Metrics
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
			vMetrics.Levels[level].BytesCompacted = uint64(1024.0 * 1024.0 * 1024.0 * bytesWrittenGB)
		}
	}
	return &vMetrics
}

type crdbPebbleDB struct {
	d       *storage.Pebble
	ballast []byte
}

func newCRDBPebbleDB(dir string) DB {
	cfg := storage.PebbleConfig{}
	cfg.Dir = dir
	// TODO(peter): We can't create a pebble.Cache here because cfg.Opts.Cache is
	// the vendored type and we can't import that vendored directory here. The
	// following diff can be applied to github.com/cockroachdb/cockroach/pkg/storage/pebble.go:
	//
	// --- a/pkg/storage/pebble.go
	// +++ b/pkg/storage/pebble.go
	// @@ -377,6 +377,7 @@ func (l pebbleLogger) Fatalf(format string, args ...interface{}) {
	//  type PebbleConfig struct {
	//         // StorageConfig contains storage configs for all storage engines.
	//         base.StorageConfig
	// +       CacheSize int64
	//         // Pebble specific options.
	//         Opts *pebble.Options
	//  }
	// @@ -461,6 +462,11 @@ func ResolveEncryptedEnvOptions(

	//  // NewPebble creates a new Pebble instance, at the specified path.
	//  func NewPebble(ctx context.Context, cfg PebbleConfig) (*Pebble, error) {
	// +       if cfg.Opts.Cache == nil {
	// +               cfg.Opts.Cache = pebble.NewCache(cfg.CacheSize)
	// +               defer cfg.Opts.Cache.Unref()
	// +       }
	// +
	//
	// With the above diff applied, we can uncomment the line below:
	//
	// cfg.CacheSize = cacheSize
	cfg.Opts = storage.DefaultPebbleOptions()
	r, err := storage.NewPebble(context.Background(), cfg)
	if err != nil {
		log.Fatal(err)
	}
	return crdbPebbleDB{
		d:       r,
		ballast: make([]byte, 1<<30),
	}
}

type crdbPebbleDBIterator struct {
	iter       storage.Iterator
	lowerBound []byte
	upperBound []byte
}

type crdbPebbleDBBatch struct {
	batch storage.Batch
}

func (i crdbPebbleDBIterator) SeekGE(key []byte) bool {
	// TODO: unnecessary overhead here. Change the interface.
	userKey, _, ok := mvccSplitKey(key)
	if !ok {
		panic("mvccSplitKey failed")
	}
	i.iter.SeekGE(storage.MVCCKey{
		Key: userKey,
	})
	return i.Valid()
}

func (i crdbPebbleDBIterator) Valid() bool {
	valid, _ := i.iter.Valid()
	return valid
}

func (i crdbPebbleDBIterator) Key() []byte {
	key := i.iter.Key()
	return []byte(key.Key)
}

func (i crdbPebbleDBIterator) Value() []byte {
	return i.iter.Value()
}

func (i crdbPebbleDBIterator) First() bool {
	return i.SeekGE(i.lowerBound)
}

func (i crdbPebbleDBIterator) Next() bool {
	i.iter.Next()
	valid, _ := i.iter.Valid()
	return valid
}

func (i crdbPebbleDBIterator) Last() bool {
	// TODO: unnecessary overhead here. Change the interface.
	userKey, _, ok := mvccSplitKey(i.upperBound)
	if !ok {
		panic("mvccSplitKey failed")
	}
	i.iter.SeekLT(storage.MVCCKey{
		Key: userKey,
	})
	return i.Valid()
}

func (i crdbPebbleDBIterator) Prev() bool {
	i.iter.Prev()
	valid, _ := i.iter.Valid()
	return valid
}

func (i crdbPebbleDBIterator) Close() error {
	i.iter.Close()
	return nil
}

func (b crdbPebbleDBBatch) Close() error {
	b.batch.Close()
	return nil
}

func (b crdbPebbleDBBatch) Commit(opts *pebble.WriteOptions) error {
	return b.batch.Commit(opts.Sync)
}

func (b crdbPebbleDBBatch) Set(key, value []byte, _ *pebble.WriteOptions) error {
	// TODO: unnecessary overhead here. Change the interface.
	userKey, _, ok := mvccSplitKey(key)
	if !ok {
		panic("mvccSplitKey failed")
	}
	ts := hlc.Timestamp{WallTime: 1}
	return b.batch.Put(storage.MVCCKey{Key: userKey, Timestamp: ts}, value)
}

func (b crdbPebbleDBBatch) LogData(data []byte, _ *pebble.WriteOptions) error {
	return b.batch.LogData(data)
}

func (r crdbPebbleDB) Flush() error {
	return r.d.Flush()
}

func (r crdbPebbleDB) NewIter(opts *pebble.IterOptions) iterator {
	ropts := storage.IterOptions{}
	if opts != nil {
		ropts.LowerBound = opts.LowerBound
		ropts.UpperBound = opts.UpperBound
	} else {
		ropts.UpperBound = roachpb.KeyMax
	}
	iter := r.d.NewIterator(ropts)
	return crdbPebbleDBIterator{
		iter:       iter,
		lowerBound: ropts.LowerBound,
		upperBound: ropts.UpperBound,
	}
}

func (r crdbPebbleDB) NewBatch() batch {
	return crdbPebbleDBBatch{r.d.NewBatch()}
}

func (r crdbPebbleDB) Scan(key []byte, count int64, reverse bool) error {
	// TODO: unnecessary overhead here. Change the interface.
	beginKey, _, ok := mvccSplitKey(key)
	if !ok {
		panic("mvccSplitKey failed")
	}
	endKey := roachpb.KeyMax
	if reverse {
		endKey = beginKey
		beginKey = roachpb.KeyMin
	}

	// We hard code a timestamp with walltime=1 in the data, so we just have to
	// use a larger timestamp here (walltime=10).
	ts := hlc.Timestamp{WallTime: 10}
	res, err := storage.MVCCScanToBytes(
		context.Background(), r.d, beginKey, endKey, ts,
		storage.MVCCScanOptions{Reverse: reverse, MaxKeys: count},
	)
	if res.NumKeys > count {
		panic("MVCCScan returned too many keys")
	}
	if len(res.Intents) > 0 {
		panic("MVCCScan found intents")
	}
	return err
}

func (r crdbPebbleDB) Metrics() *pebble.Metrics {
	return &pebble.Metrics{}
}
