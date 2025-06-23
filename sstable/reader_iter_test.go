// Copyright 2024 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package sstable

import (
	"context"
	"math/rand/v2"
	"testing"

	"github.com/cockroachdb/crlib/testutils/leaktest"
	"github.com/cockroachdb/pebble/internal/base"
	"github.com/cockroachdb/pebble/internal/testutils"
	"github.com/cockroachdb/pebble/objstorage/objstorageprovider"
	"github.com/cockroachdb/pebble/sstable/block"
	"github.com/cockroachdb/pebble/vfs"
	"github.com/cockroachdb/pebble/vfs/errorfs"
	"github.com/stretchr/testify/require"
)

// TestIteratorErrorOnInit tests the path where creation of an iterator fails
// when reading the index block.
func TestIteratorErrorOnInit(t *testing.T) {
	defer leaktest.AfterTest(t)()
	mem := vfs.NewMem()

	f0, err := mem.Create("test.sst", vfs.WriteCategoryUnspecified)
	require.NoError(t, err)
	writerOpts := WriterOptions{
		Comparer:   base.DefaultComparer,
		MergerName: base.DefaultMerger.Name,
	}
	w := NewWriter(objstorageprovider.NewFileWritable(f0), writerOpts)
	require.NoError(t, w.Set([]byte("test"), nil))
	require.NoError(t, w.Close())

	toggle := errorfs.Toggle{Injector: errorfs.ErrInjected}
	f1 := errorfs.WrapFile(testutils.CheckErr(mem.Open("test.sst")), &toggle)

	r, err := newReader(f1, ReaderOptions{
		Comparer: base.DefaultComparer,
		Merger:   base.DefaultMerger,
	})
	require.NoError(t, err)
	defer r.Close()

	var pool block.BufferPool
	pool.Init(5)
	defer pool.Release()

	toggle.On()
	defer toggle.Off()

	var stats base.InternalIteratorStats
	for range 20 {
		if rand.IntN(2) == 0 {
			iter, err := newRowBlockSingleLevelIterator(context.Background(), r, IterOptions{
				Transforms:           NoTransforms,
				FilterBlockSizeLimit: NeverUseFilterBlock,
				Env:                  ReadEnv{Block: block.ReadEnv{Stats: &stats, BufferPool: &pool}},
				ReaderProvider:       MakeTrivialReaderProvider(r),
			})
			// Single-level iterators use lazy loading - creation succeeds but first access fails
			require.NoError(t, err)
			// Error should surface when trying to use the iterator
			_ = iter.First()
			require.Error(t, iter.Error())
			iter.Close()
		} else {
			_, err := newRowBlockTwoLevelIterator(context.Background(), r, IterOptions{
				Transforms:           NoTransforms,
				FilterBlockSizeLimit: NeverUseFilterBlock,
				Env:                  ReadEnv{Block: block.ReadEnv{Stats: &stats, BufferPool: &pool}},
				ReaderProvider:       MakeTrivialReaderProvider(r),
			})
			require.Error(t, err)
		}
	}
}
