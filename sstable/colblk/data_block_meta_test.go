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
	colFmt := ColumnFormatv2
	schema := DefaultKeySchema(testkeys.Comparer, 16)
	comparer := testkeys.Comparer.EnsureDefaults()

	var w DataBlockEncoder
	w.Init(colFmt, &schema)
	kvs := []struct {
		key  string
		meta base.KVMeta
	}{
		{
			key: "a#1,SET",
			meta: base.KVMeta{
				TieringSpanID:    42,
				TieringAttribute: 100,
			},
		},
		{
			key: "b#2,SET",
			meta: base.KVMeta{
				TieringSpanID:    43,
				TieringAttribute: 200,
			},
		},
		{
			key: "c#3,SET",
			meta: base.KVMeta{
				TieringSpanID:    0,
				TieringAttribute: 0,
			},
		},
		{
			key: "d#3,SET",
			meta: base.KVMeta{
				TieringSpanID:    44,
				TieringAttribute: 300,
			},
		},
	}
	for _, kv := range kvs {
		ikey := base.ParseInternalKey(kv.key)
		kcmp := w.KeyWriter.ComparePrev(ikey.UserKey)
		vp := block.InPlaceValuePrefix(kcmp.PrefixEqual())
		w.Add(ikey, []byte("value"), vp, kcmp, false, kv.meta)
	}
	block, _ := w.Finish(w.Rows(), w.Size())

	var d DataBlockDecoder
	bd := d.Init(colFmt, &schema, block)

	var it DataBlockIter
	it.InitOnce(colFmt, &schema, comparer, nil)
	require.NoError(t, it.Init(&d, bd, blockiter.Transforms{}))

	kv, meta := it.FirstWithMeta()
	require.NotNil(t, kv)
	require.Equal(t, kvs[0].meta.TieringSpanID, meta.TieringSpanID)
	require.Equal(t, kvs[0].meta.TieringAttribute, meta.TieringAttribute)

	for i := 1; i < len(kvs); i++ {
		kv, meta = it.NextWithMeta()
		require.NotNil(t, kv, "expected key at index %d", i)
		require.Equal(t, kvs[i].meta.TieringSpanID, meta.TieringSpanID, "TieringSpanID mismatch at index %d", i)
		require.Equal(t, kvs[i].meta.TieringAttribute, meta.TieringAttribute, "TieringAttribute mismatch at index %d", i)
	}

	kv, meta = it.NextWithMeta()
	require.Nil(t, kv)
	require.Equal(t, base.KVMeta{}, meta)
}
