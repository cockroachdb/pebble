/*
 * Copyright 2017 Dgraph Labs, Inc. and Contributors
 * Modifications copyright (C) 2017 Andy Kimball and Contributors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package batchskl

import (
	"encoding/binary"
	"fmt"
	"math/rand/v2"
	"reflect"
	"testing"
	"time"

	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/pebble/internal/base"
	"github.com/cockroachdb/pebble/internal/testutils"
	"github.com/stretchr/testify/require"
)

// length iterates over skiplist to give exact size.
func length(s *Skiplist) int {
	count := 0
	it := s.NewIter(nil, nil)
	for k := it.First(); k != nil; k = it.Next() {
		count++
	}
	return count
}

// length iterates over skiplist in reverse order to give exact size.
func lengthRev(s *Skiplist) int {
	count := 0
	it := s.NewIter(nil, nil)
	for k := it.Last(); k != nil; k = it.Prev() {
		count++
	}
	return count
}

func makeKey(s string) []byte {
	return []byte(s)
}

type testStorage struct {
	data []byte
}

func (d *testStorage) add(key string) uint32 {
	offset := uint32(len(d.data))
	d.data = append(d.data, uint8(base.InternalKeyKindSet))
	var buf [binary.MaxVarintLen64]byte
	n := binary.PutUvarint(buf[:], uint64(len(key)))
	d.data = append(d.data, buf[:n]...)
	d.data = append(d.data, key...)
	return offset
}

func (d *testStorage) addBytes(key []byte) uint32 {
	offset := uint32(len(d.data))
	d.data = append(d.data, uint8(base.InternalKeyKindSet))
	var buf [binary.MaxVarintLen64]byte
	n := binary.PutUvarint(buf[:], uint64(len(key)))
	d.data = append(d.data, buf[:n]...)
	d.data = append(d.data, key...)
	return offset
}

func newTestSkiplist(storage *testStorage) *Skiplist {
	return NewSkiplist(&storage.data, base.DefaultComparer.Compare,
		base.DefaultComparer.AbbreviatedKey)
}

// TestNoPointers tests that the node struct does not contain any pointers. No
// struct that is backed by the arena may contain pointers (at least without
// zeroing the backing memory) *before* type casting the pointer. Otherwise, the
// Go GC may observe the pointer (while a GC write barrier is in effect) and
// complain that the uninitialized value is a bad pointer. See 273e2665.
func TestNoPointers(t *testing.T) {
	require.False(t, testutils.AnyPointers(reflect.TypeOf(node{})))
}

func TestEmpty(t *testing.T) {
	key := makeKey("aaa")
	l := newTestSkiplist(&testStorage{})
	it := l.NewIter(nil, nil)
	require.Nil(t, it.First())
	require.Nil(t, it.Last())
	require.Nil(t, it.SeekGE(key, base.SeekGEFlagsNone))
	require.Nil(t, it.SeekLT(key))
}

func assertKey(t *testing.T, key string, ik *base.InternalKey) {
	require.NotNil(t, ik)
	require.EqualValues(t, key, string(ik.UserKey))
}

// TestBasic tests seeks and adds.
func TestBasic(t *testing.T) {
	d := &testStorage{}
	l := newTestSkiplist(d)
	it := l.NewIter(nil, nil)

	// Try adding values.
	require.Nil(t, l.Add(d.add("key1")))
	require.Nil(t, l.Add(d.add("key2")))
	require.Nil(t, l.Add(d.add("key3")))

	assertKey(t, "key1", it.SeekGE(makeKey("key"), base.SeekGEFlagsNone))
	assertKey(t, "key1", it.SeekGE(makeKey("key1"), base.SeekGEFlagsNone))
	assertKey(t, "key2", it.SeekGE(makeKey("key2"), base.SeekGEFlagsNone))
	assertKey(t, "key3", it.SeekGE(makeKey("key3"), base.SeekGEFlagsNone))
	assertKey(t, "key2", it.SeekGE(makeKey("key2"), base.SeekGEFlagsNone))
	assertKey(t, "key3", it.SeekGE(makeKey("key3"), base.SeekGEFlagsNone))
}

func TestSkiplistAdd(t *testing.T) {
	d := &testStorage{}
	l := newTestSkiplist(d)
	it := l.NewIter(nil, nil)

	// Add empty key.
	require.Nil(t, l.Add(d.add("")))
	assertKey(t, "", it.First())

	// Add to empty list.
	require.Nil(t, l.Add(d.add("00002")))
	assertKey(t, "00002", it.SeekGE(makeKey("00002"), base.SeekGEFlagsNone))

	// Add first element in non-empty list.
	require.Nil(t, l.Add(d.add("00001")))
	assertKey(t, "00001", it.SeekGE(makeKey("00001"), base.SeekGEFlagsNone))

	// Add last element in non-empty list.
	require.Nil(t, l.Add(d.add("00004")))
	assertKey(t, "00004", it.SeekGE(makeKey("00004"), base.SeekGEFlagsNone))

	// Add element in middle of list.
	require.Nil(t, l.Add(d.add("00003")))
	assertKey(t, "00003", it.SeekGE(makeKey("00003"), base.SeekGEFlagsNone))

	// Try to add element that already exists.
	require.Nil(t, l.Add(d.add("00002")))
	require.Equal(t, 6, length(l))
	require.Equal(t, 6, lengthRev(l))
}

func TestSkiplistAdd_Overflow(t *testing.T) {
	// Regression test for cockroachdb/pebble#1258. The length of the nodes buffer
	// cannot exceed the maximum allowable size.
	d := &testStorage{}
	l := newTestSkiplist(d)

	// Simulate a full nodes slice. This speeds up the test significantly, as
	// opposed to adding data to the list.
	l.nodes = make([]byte, maxNodesSize)

	// Adding a new node to the list would overflow the nodes slice. Note that it
	// is the size of a new node struct that is relevant here, rather than the
	// size of the data being added to the list.
	err := l.Add(d.add("too much!"))
	require.Error(t, err)
	require.True(t, errors.Is(err, ErrTooManyRecords))
}

// TestIteratorNext tests a basic iteration over all nodes from the beginning.
func TestIteratorNext(t *testing.T) {
	const n = 100
	d := &testStorage{}
	l := newTestSkiplist(d)
	it := l.NewIter(nil, nil)
	require.Nil(t, it.First())

	for i := n - 1; i >= 0; i-- {
		require.Nil(t, l.Add(d.add(fmt.Sprintf("%05d", i))))
	}

	k := it.First()
	for i := 0; i < n; i++ {
		require.NotNil(t, k)
		require.EqualValues(t, fmt.Sprintf("%05d", i), string(k.UserKey))
		k = it.Next()
	}
	require.Nil(t, k)
}

// // TestIteratorPrev tests a basic iteration over all nodes from the end.
func TestIteratorPrev(t *testing.T) {
	const n = 100
	d := &testStorage{}
	l := newTestSkiplist(d)
	it := l.NewIter(nil, nil)
	require.Nil(t, it.Last())

	for i := 0; i < n; i++ {
		l.Add(d.add(fmt.Sprintf("%05d", i)))
	}

	k := it.Last()
	for i := n - 1; i >= 0; i-- {
		require.NotNil(t, k)
		require.EqualValues(t, fmt.Sprintf("%05d", i), string(k.UserKey))
		k = it.Prev()
	}
	require.Nil(t, k)
}

func TestIteratorSeekGE(t *testing.T) {
	const n = 1000
	d := &testStorage{}
	l := newTestSkiplist(d)
	it := l.NewIter(nil, nil)
	require.Nil(t, it.First())

	// 1000, 1010, 1020, ..., 1990.
	for i := n - 1; i >= 0; i-- {
		require.Nil(t, l.Add(d.add(fmt.Sprintf("%05d", i*10+1000))))
	}

	assertKey(t, "01000", it.SeekGE(makeKey("01000"), base.SeekGEFlagsNone))
	assertKey(t, "01000", it.SeekGE(makeKey("01000"), base.SeekGEFlagsNone))
	assertKey(t, "01010", it.SeekGE(makeKey("01005"), base.SeekGEFlagsNone))
	assertKey(t, "01010", it.SeekGE(makeKey("01010"), base.SeekGEFlagsNone))
	require.Nil(t, it.SeekGE(makeKey("99999"), base.SeekGEFlagsNone))

	// Test seek for empty key.
	require.Nil(t, l.Add(d.add("")))
	assertKey(t, "", it.SeekGE([]byte{}, base.SeekGEFlagsNone))
	assertKey(t, "", it.SeekGE(makeKey(""), base.SeekGEFlagsNone))
}

func TestIteratorSeekLT(t *testing.T) {
	const n = 100
	d := &testStorage{}
	l := newTestSkiplist(d)
	it := l.NewIter(nil, nil)
	require.Nil(t, it.First())

	// 1000, 1010, 1020, ..., 1990.
	for i := n - 1; i >= 0; i-- {
		require.Nil(t, l.Add(d.add(fmt.Sprintf("%05d", i*10+1000))))
	}

	require.Nil(t, it.SeekLT(makeKey("")))
	require.Nil(t, it.SeekLT(makeKey("01000")))
	assertKey(t, "01000", it.SeekLT(makeKey("01001")))
	assertKey(t, "01000", it.SeekLT(makeKey("01005")))
	assertKey(t, "01990", it.SeekLT(makeKey("01991")))
	assertKey(t, "01990", it.SeekLT(makeKey("99999")))

	// Test seek for empty key.
	require.Nil(t, l.Add(d.add("")))
	require.Nil(t, it.SeekLT([]byte{}))
	assertKey(t, "", it.SeekLT(makeKey("\x01")))
}

// TODO(peter): test First and Last.
func TestIteratorBounds(t *testing.T) {
	d := &testStorage{}
	l := newTestSkiplist(d)
	for i := 1; i < 10; i++ {
		require.NoError(t, l.Add(d.add(fmt.Sprintf("%05d", i))))
	}

	it := l.NewIter(makeKey("00003"), makeKey("00007"))

	// SeekGE within the lower and upper bound succeeds.
	for i := 3; i <= 6; i++ {
		key := makeKey(fmt.Sprintf("%05d", i))
		assertKey(t, string(key), it.SeekGE(key, base.SeekGEFlagsNone))
	}

	// SeekGE before the lower bound still succeeds (only the upper bound is
	// checked).
	for i := 1; i < 3; i++ {
		key := makeKey(fmt.Sprintf("%05d", i))
		assertKey(t, string(key), it.SeekGE(key, base.SeekGEFlagsNone))
	}

	// SeekGE beyond the upper bound fails.
	for i := 7; i < 10; i++ {
		key := makeKey(fmt.Sprintf("%05d", i))
		require.Nil(t, it.SeekGE(key, base.SeekGEFlagsNone))
	}

	assertKey(t, "00006", it.SeekGE(makeKey("00006"), base.SeekGEFlagsNone))
	// Next into the upper bound fails.
	require.Nil(t, it.Next())

	// SeekLT within the lower and upper bound succeeds.
	for i := 4; i <= 7; i++ {
		key := makeKey(fmt.Sprintf("%05d", i))
		assertKey(t, fmt.Sprintf("%05d", i-1), it.SeekLT(key))
	}

	// SeekLT beyond the upper bound still succeeds (only the lower bound is
	// checked).
	for i := 8; i < 9; i++ {
		key := makeKey(fmt.Sprintf("%05d", i))
		assertKey(t, fmt.Sprintf("%05d", i-1), it.SeekLT(key))
	}

	// SeekLT before the lower bound fails.
	for i := 1; i < 4; i++ {
		key := makeKey(fmt.Sprintf("%05d", i))
		require.Nil(t, it.SeekLT(key))
	}

	assertKey(t, "00003", it.SeekLT(makeKey("00004")))
	// Prev into the lower bound fails.
	require.Nil(t, it.Prev())
}

func randomKey(rng *rand.Rand, b []byte) []byte {
	key := rng.Uint32()
	key2 := rng.Uint32()
	binary.LittleEndian.PutUint32(b, key)
	binary.LittleEndian.PutUint32(b[4:], key2)
	return b
}

// Standard test. Some fraction is read. Some fraction is write. Writes have
// to go through mutex lock.
func BenchmarkReadWrite(b *testing.B) {
	for i := 0; i <= 10; i++ {
		readFrac := float32(i) / 10.0
		b.Run(fmt.Sprintf("frac_%d", i*10), func(b *testing.B) {
			var buf [8]byte
			d := &testStorage{
				data: make([]byte, 0, b.N*10),
			}
			l := newTestSkiplist(d)
			it := l.NewIter(nil, nil)
			rng := rand.New(rand.NewPCG(0, uint64(time.Now().UnixNano())))

			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				key := randomKey(rng, buf[:])
				if rng.Float32() < readFrac {
					_ = it.SeekGE(key, base.SeekGEFlagsNone)
				} else {
					offset := d.addBytes(buf[:])
					_ = l.Add(offset)
				}
			}
			b.StopTimer()
		})
	}
}

func BenchmarkOrderedWrite(b *testing.B) {
	var buf [8]byte
	d := &testStorage{
		data: make([]byte, 0, b.N*10),
	}
	l := newTestSkiplist(d)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		binary.BigEndian.PutUint64(buf[:], uint64(i))
		offset := d.addBytes(buf[:])
		_ = l.Add(offset)
	}
}

func BenchmarkIterNext(b *testing.B) {
	var buf [8]byte
	d := &testStorage{
		data: make([]byte, 0, 64<<10),
	}
	l := newTestSkiplist(d)

	rng := rand.New(rand.NewPCG(0, uint64(time.Now().UnixNano())))
	for len(d.data)+20 < cap(d.data) {
		key := randomKey(rng, buf[:])
		offset := d.addBytes(key)
		err := l.Add(offset)
		require.NoError(b, err)
	}

	it := l.NewIter(nil, nil)
	b.ResetTimer()
	var k *base.InternalKey
	for i := 0; i < b.N; i++ {
		if k == nil {
			_ = it.First()
		} else {
			k = it.Next()
		}
	}
}

func BenchmarkIterPrev(b *testing.B) {
	var buf [8]byte
	d := &testStorage{
		data: make([]byte, 0, 64<<10),
	}
	l := newTestSkiplist(d)

	rng := rand.New(rand.NewPCG(0, uint64(time.Now().UnixNano())))
	for len(d.data)+20 < cap(d.data) {
		key := randomKey(rng, buf[:])
		offset := d.addBytes(key)
		err := l.Add(offset)
		require.NoError(b, err)
	}

	it := l.NewIter(nil, nil)
	b.ResetTimer()
	var k *base.InternalKey
	for i := 0; i < b.N; i++ {
		if k == nil {
			_ = it.Last()
		}
		k = it.Prev()
	}
}
