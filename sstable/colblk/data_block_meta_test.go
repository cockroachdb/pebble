// Copyright 2025 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package colblk

import (
	"testing"

	"github.com/cockroachdb/pebble/internal/base"
	"github.com/cockroachdb/pebble/internal/testkeys"
	"github.com/cockroachdb/pebble/sstable/block"
	"github.com/cockroachdb/pebble/sstable/blockiter"
	"github.com/stretchr/testify/require"
)

// TestDataBlockIterWithMeta tests that the *WithMeta methods correctly
// extract and return KVMeta for keys with tiering metadata.
func TestDataBlockIterWithMeta(t *testing.T) {
	schema := DefaultKeySchema(testkeys.Comparer, 16)
	comparer := testkeys.Comparer.EnsureDefaults()
	tieringColConfig := WithTieringColumns()

	var w DataBlockEncoder
	w.Init(&schema, tieringColConfig)
	testKVs := []struct {
		key  string
		meta base.KVMeta
	}{
		{"a#1,SET", base.KVMeta{TieringSpanID: 42, TieringAttribute: 100}},
		{"b#2,SET", base.KVMeta{TieringSpanID: 43, TieringAttribute: 200, SecondaryBlobHandle: []byte("cold-handle-b")}},
		{"c#3,SET", base.KVMeta{TieringSpanID: 0, TieringAttribute: 0}},
	}
	for _, kv := range testKVs {
		ikey := base.ParseInternalKey(kv.key)
		kcmp := w.KeyWriter.ComparePrev(ikey.UserKey)
		vp := block.InPlaceValuePrefix(kcmp.PrefixEqual())
		if kv.meta.SecondaryBlobHandle != nil {
			w.AddWithSecondaryBlobHandle(ikey, []byte("value"), vp, kcmp, false, kv.meta, kv.meta.SecondaryBlobHandle)
		} else {
			w.Add(ikey, []byte("value"), vp, kcmp, false, kv.meta)
		}
	}
	block, _ := w.Finish(w.Rows(), w.Size())

	var d DataBlockDecoder
	bd := d.Init(&schema, block)
	var it DataBlockIter
	it.InitOnce(&schema, comparer, nil, tieringColConfig)
	require.NoError(t, it.Init(&d, bd, blockiter.Transforms{}, tieringColConfig))

	// Test FirstWithMeta and NextWithMeta.
	for i, expected := range testKVs {
		var kv *base.InternalKV
		var meta base.KVMeta
		if i == 0 {
			kv, meta = it.FirstWithMeta()
		} else {
			kv, meta = it.NextWithMeta()
		}
		require.NotNil(t, kv)
		require.Equal(t, expected.meta, meta)
	}

	// Test exhaustion.
	kv, meta := it.NextWithMeta()
	require.Nil(t, kv)
	require.Equal(t, base.KVMeta{}, meta)

	// Test SeekGEWithMeta.
	kv, meta = it.SeekGEWithMeta([]byte("b"), base.SeekGEFlagsNone)
	require.NotNil(t, kv)
	require.Equal(t, testKVs[1].meta, meta)

	kv, meta = it.SeekGEWithMeta([]byte("z"), base.SeekGEFlagsNone)
	require.Nil(t, kv)
	require.Equal(t, base.KVMeta{}, meta)
}
