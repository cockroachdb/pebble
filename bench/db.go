// Copyright 2019 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package bench

import (
	"log"
	"time"

	"github.com/cockroachdb/pebble"
	"github.com/cockroachdb/pebble/cockroachkvs"
	"github.com/cockroachdb/pebble/internal/bytealloc"
	"github.com/cockroachdb/pebble/objstorage/remote"
	"github.com/cockroachdb/pebble/sstable"
	"github.com/cockroachdb/pebble/vfs"
)

// DB specifies the minimal interfaces that need to be implemented by the
// storage layer used by the benchmarks.
type DB interface {
	NewIter(*pebble.IterOptions) Iterator
	NewBatch() Batch
	Scan(iter Iterator, key []byte, count int64, reverse bool) error
	Metrics() *pebble.Metrics
	Flush() error
}

// Iterator is the iterator interface used by the benchmarks.
type Iterator interface {
	SeekLT(key []byte) bool
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

// Batch is the batch interface used by the benchmarks.
type Batch interface {
	Close() error
	Commit(opts *pebble.WriteOptions) error
	Set(key, value []byte, opts *pebble.WriteOptions) error
	Delete(key []byte, opts *pebble.WriteOptions) error
	LogData(data []byte, opts *pebble.WriteOptions) error
}

// pebbleDB is the Pebble adapter for the DB interface.
type pebbleDB struct {
	d       *pebble.DB
	ballast []byte
}

// NewPebbleDB opens a Pebble DB at the given directory using settings derived
// from cfg.
func NewPebbleDB(dir string, cfg *CommonConfig) DB {
	opts := &pebble.Options{
		CacheSize:                   cfg.CacheSize,
		Comparer:                    &cockroachkvs.Comparer,
		DisableWAL:                  cfg.DisableWAL,
		FormatMajorVersion:          pebble.FormatNewest,
		KeySchema:                   cockroachkvs.KeySchema.Name,
		KeySchemas:                  sstable.MakeKeySchemas(&cockroachkvs.KeySchema),
		L0CompactionThreshold:       2,
		L0StopWritesThreshold:       1000,
		LBaseMaxBytes:               64 << 20, // 64 MB
		MaxOpenFiles:                16384,
		MemTableSize:                64 << 20,
		MemTableStopWritesThreshold: 4,
		Merger: &pebble.Merger{
			Name: "cockroach_merge_operator",
		},
		CompactionConcurrencyRange: func() (int, int) {
			return 1, 3
		},
		DisableAutomaticCompactions: cfg.DisableAutoCompactions,
	}
	// Enable value separation. Note the minimum size of 512 means that only the
	// variant of the ycsb benchmarks that uses 1024 values will result in any
	// value separation.
	opts.ValueSeparationPolicy = func() pebble.ValueSeparationPolicy {
		return pebble.ValueSeparationPolicy{
			Enabled:                  true,
			MinimumSize:              512,
			MinimumMVCCGarbageSize:   32,
			MaxBlobReferenceDepth:    10,
			RewriteMinimumAge:        5 * time.Minute,
			GarbageRatioLowPriority:  0.10, // 10% garbage
			GarbageRatioHighPriority: 0.30, // 30% garbage
		}
	}

	// Running the tool should not start compactions due to garbage.
	opts.CompactionGarbageFractionForMaxConcurrency = func() float64 {
		return -1.0
	}
	for i := 0; i < len(opts.Levels); i++ {
		l := &opts.Levels[i]
		l.BlockSize = 32 << 10       // 32 KB
		l.IndexBlockSize = 256 << 10 // 256 KB
	}
	opts.ApplyTableFilterPolicy(func() pebble.DBTableFilterPolicy { return pebble.DBTableFilterPolicyProgressive })
	opts.FlushSplitBytes = opts.TargetFileSizes[0]

	opts.EnsureDefaults()

	if cfg.Verbose {
		lel := pebble.MakeLoggingEventListener(nil)
		opts.EventListener = &lel
		opts.EventListener.TableDeleted = nil
		opts.EventListener.TableIngested = nil
		opts.EventListener.WALCreated = nil
		opts.EventListener.WALDeleted = nil
	}

	if cfg.PathToLocalSharedStorage != "" {
		opts.RemoteStorage = remote.MakeSimpleFactory(map[remote.Locator]remote.Storage{
			// Store all shared objects on local disk, for convenience.
			remote.MakeLocator(""): remote.NewLocalFS(cfg.PathToLocalSharedStorage, vfs.Default),
		})
		opts.CreateOnShared = remote.CreateOnSharedAll
		if cfg.SecondaryCacheSize != 0 {
			opts.SecondaryCacheSizeBytes = cfg.SecondaryCacheSize
		}
	}

	p, err := pebble.Open(dir, opts)
	if err != nil {
		log.Fatal(err)
	}
	if cfg.PathToLocalSharedStorage != "" {
		if err := p.SetCreatorID(1); err != nil {
			log.Fatal(err)
		}
	}
	db := pebbleDB{d: p}
	if cfg.Ballast > 0 {
		db.ballast = make([]byte, cfg.Ballast)
	}
	return db
}

func (p pebbleDB) Flush() error {
	return p.d.Flush()
}

func (p pebbleDB) NewIter(opts *pebble.IterOptions) Iterator {
	iter, _ := p.d.NewIter(opts)
	return iter
}

func (p pebbleDB) NewBatch() Batch {
	return p.d.NewBatch()
}

func (p pebbleDB) Scan(iter Iterator, key []byte, count int64, reverse bool) error {
	var data bytealloc.A
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
	return nil
}

func (p pebbleDB) Metrics() *pebble.Metrics {
	return p.d.Metrics()
}
