// Copyright 2024 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package crdbtest

import (
	"bytes"
	"context"
	"fmt"
	"math/rand/v2"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/cockroachdb/datadriven"
	"github.com/cockroachdb/pebble/internal/base"
	"github.com/cockroachdb/pebble/internal/cache"
	"github.com/cockroachdb/pebble/internal/sstableinternal"
	"github.com/cockroachdb/pebble/internal/testutils"
	"github.com/cockroachdb/pebble/objstorage"
	"github.com/cockroachdb/pebble/sstable"
	"github.com/stretchr/testify/require"
)

func TestComparer(t *testing.T) {
	prefixes := [][]byte{
		EncodeMVCCKey(nil, []byte("abc"), 0, 0),
		EncodeMVCCKey(nil, []byte("d"), 0, 0),
		EncodeMVCCKey(nil, []byte("ef"), 0, 0),
	}

	suffixes := [][]byte{{}}
	for walltime := 3; walltime > 0; walltime-- {
		for logical := 2; logical >= 0; logical-- {
			key := EncodeMVCCKey(nil, []byte("foo"), uint64(walltime), uint32(logical))
			suffix := key[Comparer.Split(key):]
			suffixes = append(suffixes, suffix)

			if len(suffix) == suffixLenWithWall {
				// Append a suffix that encodes a zero logical value that should be
				// ignored in key comparisons, but not suffix comparisons.
				newSuffix := slices.Concat(suffix[:suffixLenWithWall-1], zeroLogical[:], []byte{suffixLenWithLogical})
				if Comparer.CompareRangeSuffixes(suffix, newSuffix) != 1 {
					t.Fatalf("expected suffixes %x < %x", suffix, newSuffix)
				}
				if Comparer.Compare(slices.Concat(prefixes[0], suffix), slices.Concat(prefixes[0], newSuffix)) != 0 {
					t.Fatalf("expected keys with suffixes %x and %x to be equal", suffix, newSuffix)
				}
				suffixes = append(suffixes, newSuffix)
				suffix = newSuffix
			}
			if len(suffix) != suffixLenWithLogical {
				t.Fatalf("unexpected suffix %x", suffix)
			}
			// Append a synthetic bit that should be ignored in key comparisons, but
			// not suffix comparisons.
			newSuffix := slices.Concat(suffix[:suffixLenWithLogical-1], []byte{1}, []byte{suffixLenWithSynthetic})
			if Comparer.CompareRangeSuffixes(suffix, newSuffix) != 1 {
				t.Fatalf("expected suffixes %x < %x", suffix, newSuffix)
			}
			if Comparer.Compare(slices.Concat(prefixes[0], suffix), slices.Concat(prefixes[0], newSuffix)) != 0 {
				t.Fatalf("expected keys with suffixes %x and %x to be equal", suffix, newSuffix)
			}
			suffixes = append(suffixes, newSuffix)
		}
	}
	// Add some lock table suffixes.
	suffixes = append(suffixes, append(bytes.Repeat([]byte{1}, engineKeyVersionLockTableLen), suffixLenWithLockTable))
	suffixes = append(suffixes, append(bytes.Repeat([]byte{2}, engineKeyVersionLockTableLen), suffixLenWithLockTable))
	if err := base.CheckComparer(&Comparer, prefixes, suffixes); err != nil {
		t.Error(err)
	}
}

func TestDataDriven(t *testing.T) {
	var keys [][]byte
	datadriven.RunTest(t, "testdata", func(t *testing.T, d *datadriven.TestData) string {
		seed := uint64(1234)
		count := 10
		valueLen := 4
		cfg := KeyConfig{
			PrefixAlphabetLen: 8,
			PrefixLenShared:   4,
			PrefixLen:         8,
			AvgKeysPerPrefix:  2,
			PercentLogical:    10,
		}
		const layout = "2006-01-02T15:04:05"
		baseWallTime := "2020-01-01T00:00:00"
		d.MaybeScanArgs(t, "seed", &seed)
		d.MaybeScanArgs(t, "count", &count)
		d.MaybeScanArgs(t, "alpha-len", &cfg.PrefixAlphabetLen)
		d.MaybeScanArgs(t, "prefix-len-shared", &cfg.PrefixLenShared)
		d.MaybeScanArgs(t, "prefix-len", &cfg.PrefixLen)
		d.MaybeScanArgs(t, "avg-keys-pre-prefix", &cfg.AvgKeysPerPrefix)
		d.MaybeScanArgs(t, "percent-logical", &cfg.PercentLogical)
		d.MaybeScanArgs(t, "base-wall-time", &baseWallTime)
		d.MaybeScanArgs(t, "value-len", &valueLen)
		cfg.BaseWallTime = uint64(testutils.CheckErr(time.Parse(layout, baseWallTime)).UnixNano())

		rng := rand.New(rand.NewPCG(0, seed))
		var buf strings.Builder
		switch d.Cmd {
		case "rand-kvs":
			var vals [][]byte
			keys, vals = RandomKVs(rng, count, cfg, valueLen)
			for i, key := range keys {
				n := Split(key)
				prefix := key[:n-1]
				suffix := key[n : len(key)-1]
				fmt.Fprintf(&buf, "%s @ %X = %X\n", prefix, suffix, vals[i])
			}

		case "rand-query-keys":
			queryKeys := RandomQueryKeys(rng, count, keys, cfg.BaseWallTime)
			for _, key := range queryKeys {
				n := Split(key)
				prefix := key[:n-1]
				suffix := key[n : len(key)-1]
				fmt.Fprintf(&buf, "%s @ %X\n", prefix, suffix)
			}

		default:
			d.Fatalf(t, "unknown command %q", d.Cmd)
		}
		return buf.String()
	})
}

func BenchmarkRandSeekInSST(b *testing.B) {
	configs := []struct {
		name     string
		numKeys  int
		valueLen int
		version  sstable.TableFormat
	}{
		{
			name:     "v4/single-level",
			numKeys:  200 * 100, // ~100 data blocks.
			valueLen: 128,       // ~200 KVs per data block
			version:  sstable.TableFormatPebblev4,
		},
		{
			name:     "v4/two-level",
			numKeys:  200 * 5000, // ~5000 data blocks
			valueLen: 128,        // ~200 KVs per data block
			version:  sstable.TableFormatPebblev4,
		},
		{
			name:     "v5/single-level",
			numKeys:  200 * 100, // ~100 data blocks.
			valueLen: 128,       // ~200 KVs per data block
			version:  sstable.TableFormatPebblev5,
		},
		{
			name:     "v5/two-level",
			numKeys:  200 * 5000, // ~5000 data blocks
			valueLen: 128,        // ~200 KVs per data block
			version:  sstable.TableFormatPebblev5,
		},
	}
	keyCfg := KeyConfig{
		PrefixAlphabetLen: 26,
		PrefixLen:         12,
		PrefixLenShared:   4,
		AvgKeysPerPrefix:  1,
		BaseWallTime:      uint64(time.Now().UnixNano()),
	}
	rng := rand.New(rand.NewPCG(0, rand.Uint64()))
	for _, cfg := range configs {
		o := sstable.WriterOptions{
			BlockSize:            32 * 1024,
			IndexBlockSize:       128 * 1024,
			AllocatorSizeClasses: sstable.JemallocSizeClasses,
			TableFormat:          cfg.version,
			Comparer:             &Comparer,
			KeySchema:            KeySchema,
		}
		b.Run(cfg.name, func(b *testing.B) {
			benchmarkRandSeekInSST(b, rng, cfg.numKeys, keyCfg, cfg.valueLen, o)
		})

	}
}

func benchmarkRandSeekInSST(
	b *testing.B,
	rng *rand.Rand,
	numKeys int,
	keyCfg KeyConfig,
	valueLen int,
	writerOpts sstable.WriterOptions,
) {
	keys, values := RandomKVs(rng, numKeys, keyCfg, valueLen)
	obj := &objstorage.MemObj{}
	w := sstable.NewWriter(obj, writerOpts)
	for i := range keys {
		require.NoError(b, w.Set(keys[i], values[i]))
	}
	require.NoError(b, w.Close())
	// Make the cache twice the size of the object, to allow for decompression and
	// overhead.
	c := cache.New(obj.Size() * 2)
	defer c.Unref()
	ctx := context.Background()
	readerOpts := sstable.ReaderOptions{
		Comparer:   writerOpts.Comparer,
		KeySchemas: sstable.MakeKeySchemas(KeySchema),
	}
	readerOpts.SetInternalCacheOpts(sstableinternal.CacheOptions{
		Cache:   c,
		CacheID: c.NewID(),
		FileNum: 1,
	})
	reader, err := sstable.NewReader(ctx, obj, readerOpts)
	require.NoError(b, err)
	defer reader.Close()

	// Iterate through the entire table to warm up the cache.
	var stats base.InternalIteratorStats
	rp := sstable.MakeTrivialReaderProvider(reader)
	iter, err := reader.NewPointIter(
		ctx, sstable.NoTransforms, nil, nil, nil, sstable.NeverUseFilterBlock,
		&stats, sstable.CategoryAndQoS{}, nil, rp)
	require.NoError(b, err)
	n := 0
	for kv := iter.First(); kv != nil; kv = iter.Next() {
		n++
	}
	require.Equal(b, len(keys), n)
	require.NoError(b, iter.Close())

	const numQueryKeys = 65536
	queryKeys := RandomQueryKeys(rng, numQueryKeys, keys, keyCfg.BaseWallTime)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		key := queryKeys[i%numQueryKeys]
		iter, err := reader.NewPointIter(
			ctx, sstable.NoTransforms, nil, nil, nil, sstable.NeverUseFilterBlock,
			&stats, sstable.CategoryAndQoS{}, nil, rp)
		if err != nil {
			b.Fatal(err)
		}
		iter.SeekGE(key, base.SeekGEFlagsNone)
		if err := iter.Close(); err != nil {
			b.Fatal(err)
		}
	}
	// Stop the timer before any deferred cleanup.
	b.StopTimer()
}
