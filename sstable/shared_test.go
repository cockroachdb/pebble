// Copyright 2022 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package sstable

import (
	"testing"

	"github.com/cockroachdb/pebble/internal/base"
	"github.com/cockroachdb/pebble/internal/cache"
	"github.com/cockroachdb/pebble/internal/manifest"
	"github.com/cockroachdb/pebble/vfs"
	"github.com/stretchr/testify/require"
)

func TestSharedSST(t *testing.T) {
	t.Logf("Start TestSharedSST")
	mem := vfs.NewMem()
	f0, err := mem.Create("test")
	require.NoError(t, err)

	w := NewWriter(f0, WriterOptions{})

	// Insert the following kv:
	//   a#1,DEL
	//   a#0,SET - foo
	//   b#0,SET - foo
	//   c#1,SET - foo
	//   c#0,SET - bar
	//   d#0,SET - foo
	//   e#0,SET - foo
	//   e-f#2,RANGEDEL
	//   If the reader treats this table as purely foreign, it can only see keys
	//   b, c and d with value "foo" (only latest version, and also considering rangedels)
	//   For the creator, it can see all the keys

	kvPairs := []struct {
		k      []byte
		v      []byte
		seqNum uint64
		kind   InternalKeyKind
	}{
		{[]byte("a"), []byte{}, 1, InternalKeyKindDelete},
		{[]byte("a"), []byte("foo"), 0, InternalKeyKindSet},
		{[]byte("b"), []byte("foo"), 0, InternalKeyKindSet},
		{[]byte("c"), []byte("foo"), 1, InternalKeyKindSet},
		{[]byte("c"), []byte("bar"), 0, InternalKeyKindSet},
		{[]byte("d"), []byte("foo"), 0, InternalKeyKindSet},
		{[]byte("e"), []byte("foo"), 0, InternalKeyKindSet},
		{[]byte("e"), []byte("f"), 2, InternalKeyKindRangeDelete},
	}

	for i := range kvPairs {
		kv := kvPairs[i]
		if kv.kind != InternalKeyKindRangeDelete {
			w.addPoint(base.MakeInternalKey(kv.k, kv.seqNum, kv.kind), kv.v)
		} else {
			w.addTombstone(base.MakeInternalKey(kv.k, kv.seqNum, kv.kind), kv.v)
		}
	}

	require.NoError(t, w.Close())
	t.Logf("Table writing finished")

	// Reopen the file for further reading tests

	f1, err := mem.Open("test")
	require.NoError(t, err)

	c := cache.New(128 << 10)
	defer c.Unref()
	r, err := NewReader(f1, ReaderOptions{
		Cache: c,
	}, FileReopenOpt{
		FS:       mem,
		Filename: "test",
	})
	require.NoError(t, err)

	// local table
	t.Logf("Read as locally created shared table")

	meta := &manifest.FileMetadata{
		IsShared:        true,
		CreatorUniqueID: 0,
		Smallest:        InternalKey{UserKey: []byte("a"), Trailer: 0},
		Largest:         InternalKey{UserKey: []byte("e"), Trailer: 0},
	}
	r.meta = meta
	require.Equal(t, uint32(0), r.dbUniqueID)

	iter, err := r.NewIter(nil, nil)
	require.NoError(t, err)
	require.NotEqual(t, iter, nil)
	iter.SetLevel(5)

	i := 0
	for k, v := iter.First(); k != nil; k, v = iter.Next() {
		t.Logf("  - %s %s", k, v)
		require.Equal(t, base.MakeInternalKey(kvPairs[i].k, kvPairs[i].seqNum, kvPairs[i].kind), *k)
		require.Equal(t, kvPairs[i].v, v)
		i++
	}
	require.NoError(t, iter.Close())

	fragmentIter, err := r.NewRawRangeDelIter()
	require.NoError(t, err)
	require.NotEqual(t, fragmentIter, nil)
	rDelIter, ok := fragmentIter.(*rangeDelIter)
	require.Equal(t, true, ok)
	rDelIter.SetLevel(5)

	s := rDelIter.First()
	require.Equal(t, []byte("e"), s.Start)
	require.Equal(t, []byte("f"), s.End)
	for i := range s.Keys {
		// here we should be able to see 2 as SeqNum
		require.Equal(t, uint64(2), s.Keys[i].SeqNum())
	}
	require.NoError(t, rDelIter.Close())

	// foreign table
	t.Logf("Read as remotely created shared table")
	r.meta.CreatorUniqueID = 1
	iter, err = r.NewIter(nil, nil)
	require.NoError(t, err)
	require.NotEqual(t, iter, nil)
	require.NotEqual(t, iter.(*tableIterator).rangeDelIter, nil)
	iter.SetLevel(5)

	// Visible keys: b#2,SET, c#2,SET, d#2,SET
	visible := []int{2, 3, 5}
	i = 0
	for k, v := iter.First(); k != nil; k, v = iter.Next() {
		require.Less(t, i, len(visible))
		t.Logf("  - %s %s", k, v)
		require.Equal(t, base.MakeInternalKey(kvPairs[visible[i]].k, seqNumL5PointKey, kvPairs[visible[i]].kind), *k)
		require.Equal(t, kvPairs[visible[i]].v, v)
		i++
	}

	require.NoError(t, iter.Close())

	fragmentIter, err = r.NewRawRangeDelIter()
	require.NoError(t, err)
	require.NotEqual(t, fragmentIter, nil)
	rDelIter, ok = fragmentIter.(*rangeDelIter)
	require.Equal(t, true, ok)
	rDelIter.SetLevel(6)

	s = rDelIter.First()
	require.Equal(t, []byte("e"), s.Start)
	require.Equal(t, []byte("f"), s.End)
	for i := range s.Keys {
		require.Equal(t, uint64(seqNumL6All), s.Keys[i].SeqNum())
	}
	require.NoError(t, rDelIter.Close())

	require.NoError(t, r.Close())
}
