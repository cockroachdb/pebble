// Copyright 2023 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package sstable

import (
	"context"
	"encoding/binary"
	"fmt"
	"testing"

	"github.com/cockroachdb/pebble/internal/base"
	"github.com/stretchr/testify/require"
)

func TestPrefixReplacingIterator(t *testing.T) {
	for _, tc := range []struct{ from, to []byte }{
		{from: []byte(""), to: []byte("bb")},
		{from: []byte("aa"), to: []byte("aa")},
		{from: []byte("aa"), to: []byte("bb")},
		{from: []byte("bb"), to: []byte("aa")},
		{from: []byte("aa"), to: []byte("zzz")},
		{from: []byte("zzz"), to: []byte("aa")},
	} {
		t.Run(fmt.Sprintf("%s_%s", tc.from, tc.to), func(t *testing.T) {
			r := buildTestTable(t, 20, 256, 256, DefaultCompression, tc.from)
			defer r.Close()
			rawIter, err := r.NewIter(nil, nil)
			require.NoError(t, err)
			defer rawIter.Close()

			raw := rawIter.(*singleLevelIterator)

			it := newPrefixReplacingIterator(raw, tc.from, tc.to, tc.to, DefaultComparer.Compare)

			kMin := []byte{0}
			kMax := []byte("~")
			k := func(i uint64) []byte {
				return binary.BigEndian.AppendUint64(tc.to[:len(tc.to):len(tc.to)], i)
			}

			var got *base.InternalKey

			t.Run("FirstNextLast", func(t *testing.T) {
				got, _ = it.First()
				require.Equal(t, k(0), got.UserKey)
				got, _ = it.Next()
				require.Equal(t, k(1), got.UserKey)
				got, _ = it.Next()
				require.Equal(t, k(2), got.UserKey)
				got, _ = it.Prev()
				require.Equal(t, k(1), got.UserKey)
				got, _ = it.Last()
				require.Equal(t, k(19), got.UserKey)
			})

			t.Run("SetBounds", func(t *testing.T) {
				it.SetBounds(k(5), k(15))
				defer it.SetBounds(nil, nil)

				got, _ = it.SeekGE(k(16), base.SeekGEFlagsNone)
				require.Nil(t, got)

				got, _ = it.SeekGE(k(14), base.SeekGEFlagsNone)
				require.Equal(t, k(14), got.UserKey)

				got, _ = it.SeekLT(k(100), base.SeekLTFlagsNone)
				require.Equal(t, k(19), got.UserKey)
			})

			t.Run("SetHookAndCtx", func(t *testing.T) {
				it.SetCloseHook(func(i Iterator) error { return nil })
				require.NotNil(t, raw.closeHook)
				it.SetCloseHook(nil)
				require.Nil(t, raw.closeHook)

				require.Equal(t, context.Background(), raw.ctx)
				ctx := context.WithValue(context.Background(), struct{}{}, "")
				it.SetContext(ctx)
				require.Equal(t, ctx, raw.ctx)
				it.SetContext(context.Background())
				require.Equal(t, context.Background(), raw.ctx)
			})

			t.Run("SeekGE", func(t *testing.T) {
				got, _ = it.SeekGE(kMin, base.SeekGEFlagsNone)
				require.Equal(t, k(0), got.UserKey)

				got, _ = it.SeekGE(k(0), base.SeekGEFlagsNone)
				require.Equal(t, k(0), got.UserKey)

				got, _ = it.SeekGE(k(10), base.SeekGEFlagsNone)
				require.Equal(t, k(10), got.UserKey)

				got, _ = it.SeekGE(k(100), base.SeekGEFlagsNone)
				require.Nil(t, got)

				got, _ = it.SeekGE(kMax, base.SeekGEFlagsNone)
				require.Nil(t, got)
			})

			t.Run("SeekPrefixGE", func(t *testing.T) {
				got, _ = it.SeekPrefixGE(tc.to, k(0), base.SeekGEFlagsNone)
				require.Equal(t, k(0), got.UserKey)

				got, _ = it.SeekPrefixGE(tc.to, k(10), base.SeekGEFlagsNone)
				require.Equal(t, k(10), got.UserKey)

				got, _ = it.Next()
				require.Equal(t, k(11), got.UserKey)

				got, _ = it.SeekPrefixGE(tc.to, k(100), base.SeekGEFlagsNone)
				require.Nil(t, got)
			})

			t.Run("SeekLT", func(t *testing.T) {
				got, _ = it.SeekLT(kMin, base.SeekLTFlagsNone)
				require.Nil(t, got)

				got, _ = it.SeekLT(k(0), base.SeekLTFlagsNone)
				require.Nil(t, got)

				got, _ = it.SeekLT(k(10), base.SeekLTFlagsNone)
				require.Equal(t, k(9), got.UserKey)

				got, _ = it.Next()
				require.Equal(t, k(10), got.UserKey)

				got, _ = it.SeekLT(k(100), base.SeekLTFlagsNone)
				require.Equal(t, k(19), got.UserKey)

				got, _ = it.SeekLT(kMax, base.SeekLTFlagsNone)
				require.Equal(t, k(19), got.UserKey)
			})
		})
	}
}
