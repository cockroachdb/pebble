// Copyright 2026 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package invalidating

import (
	"testing"

	"github.com/cockroachdb/pebble/internal/base"
	"github.com/stretchr/testify/require"
)

type fakeMetaIter struct {
	*base.FakeIter
	metas map[string]base.KVMeta
}

var _ base.InternalIteratorWithKVMeta = (*fakeMetaIter)(nil)

func (f *fakeMetaIter) metaFor(kv *base.InternalKV) base.KVMeta {
	if kv == nil {
		return base.KVMeta{}
	}
	return f.metas[string(kv.K.UserKey)]
}

func (f *fakeMetaIter) FirstWithMeta() (*base.InternalKV, base.KVMeta) {
	kv := f.First()
	return kv, f.metaFor(kv)
}

func (f *fakeMetaIter) NextWithMeta() (*base.InternalKV, base.KVMeta) {
	kv := f.Next()
	return kv, f.metaFor(kv)
}

func (f *fakeMetaIter) SeekGEWithMeta(
	key []byte, flags base.SeekGEFlags,
) (*base.InternalKV, base.KVMeta) {
	kv := f.SeekGE(key, flags)
	return kv, f.metaFor(kv)
}

func TestIterWithKVMeta(t *testing.T) {
	kvs := []base.InternalKV{
		base.MakeInternalKV(base.ParseInternalKey("a#1,SET"), []byte("va")),
		base.MakeInternalKV(base.ParseInternalKey("b#2,SET"), []byte("vb")),
		base.MakeInternalKV(base.ParseInternalKey("c#3,SET"), []byte("vc")),
	}
	metas := map[string]base.KVMeta{
		"a": {TieringSpanID: 10, TieringAttribute: 100},
		"b": {TieringSpanID: 20, TieringAttribute: 200},
		"c": {TieringSpanID: 30, TieringAttribute: 300},
	}

	t.Run("with-meta", func(t *testing.T) {
		underlying := &fakeMetaIter{
			FakeIter: base.NewFakeIter(kvs),
			metas:    metas,
		}
		it := NewIter(underlying).(base.InternalIteratorWithKVMeta)
		defer func() { require.NoError(t, it.Close()) }()

		kv1, meta := it.FirstWithMeta()
		require.NotNil(t, kv1)
		require.Equal(t, "a", string(kv1.K.UserKey))
		require.Equal(t, "va", string(kv1.InPlaceValue()))
		require.Equal(t, metas["a"], meta)

		kv2, meta := it.NextWithMeta()
		require.NotNil(t, kv2)
		require.Equal(t, "b", string(kv2.K.UserKey))
		require.Equal(t, "vb", string(kv2.InPlaceValue()))
		require.Equal(t, metas["b"], meta)
		// Previous key/value buffers must have been trashed.
		require.Equal(t, []byte{0xff}, kv1.K.UserKey)
		require.Equal(t, []byte{0xff, 0xff}, kv1.InPlaceValue())

		kv3, meta := it.SeekGEWithMeta([]byte("c"), base.SeekGEFlagsNone)
		require.NotNil(t, kv3)
		require.Equal(t, "c", string(kv3.K.UserKey))
		require.Equal(t, "vc", string(kv3.InPlaceValue()))
		require.Equal(t, metas["c"], meta)
		require.Equal(t, []byte{0xff}, kv2.K.UserKey)

		kvEnd, meta := it.NextWithMeta()
		require.Nil(t, kvEnd)
		require.Equal(t, base.KVMeta{}, meta)
	})

	t.Run("without-meta-fallback", func(t *testing.T) {
		underlying := base.NewFakeIter(kvs)
		it := NewIter(underlying).(base.InternalIteratorWithKVMeta)
		defer func() { require.NoError(t, it.Close()) }()

		kv1, meta := it.FirstWithMeta()
		require.NotNil(t, kv1)
		require.Equal(t, "a", string(kv1.K.UserKey))
		require.Equal(t, base.KVMeta{}, meta)

		kv2, meta := it.NextWithMeta()
		require.NotNil(t, kv2)
		require.Equal(t, "b", string(kv2.K.UserKey))
		require.Equal(t, base.KVMeta{}, meta)
		require.Equal(t, []byte{0xff}, kv1.K.UserKey)

		kv3, meta := it.SeekGEWithMeta([]byte("c"), base.SeekGEFlagsNone)
		require.NotNil(t, kv3)
		require.Equal(t, "c", string(kv3.K.UserKey))
		require.Equal(t, base.KVMeta{}, meta)
		require.Equal(t, []byte{0xff}, kv2.K.UserKey)
	})
}
