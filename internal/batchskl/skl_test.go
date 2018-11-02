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
	"bytes"
	"encoding/binary"
	"fmt"
	"math/rand"
	"testing"
	"time"

	"github.com/petermattis/pebble/db"
	"github.com/stretchr/testify/require"
)

// length iterates over skiplist to give exact size.
func length(s *Skiplist) int {
	count := 0

	it := s.NewIter()
	for it.First(); it.Valid(); it.Next() {
		count++
	}

	return count
}

// length iterates over skiplist in reverse order to give exact size.
func lengthRev(s *Skiplist) int {
	count := 0

	it := s.NewIter()
	for it.Last(); it.Valid(); it.Prev() {
		count++
	}

	return count
}

func makeKey(s string) []byte {
	return []byte(s)
}

type testStorage struct {
	keys [][]byte
}

func (d *testStorage) add(key string) uint32 {
	offset := uint32(len(d.keys))
	d.keys = append(d.keys, []byte(key))
	return offset
}

func (d *testStorage) Get(offset uint32) db.InternalKey {
	return db.InternalKey{UserKey: d.keys[offset]}
}

func (d *testStorage) InlineKey(key []byte) uint64 {
	return db.DefaultComparer.InlineKey(key)
}

func (d *testStorage) Compare(a []byte, b uint32) int {
	return bytes.Compare(a, d.keys[b])
}

func TestEmpty(t *testing.T) {
	key := makeKey("aaa")
	l := NewSkiplist(&testStorage{}, 0)
	it := l.NewIter()

	require.False(t, it.Valid())

	it.First()
	require.False(t, it.Valid())

	it.Last()
	require.False(t, it.Valid())

	require.False(t, it.SeekGE(key))
	require.False(t, it.Valid())
}

// TestBasic tests seeks and adds.
func TestBasic(t *testing.T) {
	d := &testStorage{}
	l := NewSkiplist(d, 0)
	it := l.NewIter()

	// Try adding values.
	require.Nil(t, l.Add(d.add("key1")))
	require.Nil(t, l.Add(d.add("key2")))
	require.Nil(t, l.Add(d.add("key3")))

	require.False(t, it.SeekGE(makeKey("key")))

	require.True(t, it.SeekGE(makeKey("key1")))
	require.EqualValues(t, "key1", it.Key().UserKey)

	require.True(t, it.SeekGE(makeKey("key2")))
	require.EqualValues(t, "key2", it.Key().UserKey)

	require.True(t, it.SeekGE(makeKey("key3")))
	require.EqualValues(t, "key3", it.Key().UserKey)

	require.True(t, it.SeekGE(makeKey("key2")))
	require.True(t, it.SeekGE(makeKey("key3")))
}

func TestSkiplistAdd(t *testing.T) {
	d := &testStorage{}
	l := NewSkiplist(d, 0)
	it := l.NewIter()

	// Add empty key.
	require.Nil(t, l.Add(d.add("")))
	require.EqualValues(t, []byte{}, it.Key().UserKey)

	// Add to empty list.
	require.Nil(t, l.Add(d.add("00002")))
	require.True(t, it.SeekGE(makeKey("00002")))
	require.EqualValues(t, "00002", it.Key().UserKey)

	// Add first element in non-empty list.
	require.Nil(t, l.Add(d.add("00001")))
	require.True(t, it.SeekGE(makeKey("00001")))
	require.EqualValues(t, "00001", it.Key().UserKey)

	// Add last element in non-empty list.
	require.Nil(t, l.Add(d.add("00004")))
	require.True(t, it.SeekGE(makeKey("00004")))
	require.EqualValues(t, "00004", it.Key().UserKey)

	// Add element in middle of list.
	require.Nil(t, l.Add(d.add("00003")))
	require.True(t, it.SeekGE(makeKey("00003")))
	require.EqualValues(t, "00003", it.Key().UserKey)

	// Try to add element that already exists.
	require.Equal(t, ErrExists, l.Add(d.add("00002")))
	require.Equal(t, 5, length(l))
	require.Equal(t, 5, lengthRev(l))
}

// TestIteratorNext tests a basic iteration over all nodes from the beginning.
func TestIteratorNext(t *testing.T) {
	const n = 100
	d := &testStorage{}
	l := NewSkiplist(d, 0)
	it := l.NewIter()

	require.False(t, it.Valid())

	it.First()
	require.False(t, it.Valid())

	for i := n - 1; i >= 0; i-- {
		require.Nil(t, l.Add(d.add(fmt.Sprintf("%05d", i))))
	}

	it.First()
	for i := 0; i < n; i++ {
		require.True(t, it.Valid())
		require.EqualValues(t, fmt.Sprintf("%05d", i), it.Key().UserKey)
		it.Next()
	}
	require.False(t, it.Valid())
}

// // TestIteratorPrev tests a basic iteration over all nodes from the end.
func TestIteratorPrev(t *testing.T) {
	const n = 100
	d := &testStorage{}
	l := NewSkiplist(d, 0)
	it := l.NewIter()

	require.False(t, it.Valid())

	it.Last()
	require.False(t, it.Valid())

	for i := 0; i < n; i++ {
		l.Add(d.add(fmt.Sprintf("%05d", i)))
	}

	it.Last()
	for i := n - 1; i >= 0; i-- {
		require.True(t, it.Valid())
		require.EqualValues(t, fmt.Sprintf("%05d", i), string(it.Key().UserKey))
		it.Prev()
	}
	require.False(t, it.Valid())
}

func TestIteratorSeekGE(t *testing.T) {
	const n = 1000
	d := &testStorage{}
	l := NewSkiplist(d, 0)
	it := l.NewIter()

	require.False(t, it.Valid())
	it.First()
	require.False(t, it.Valid())
	// 1000, 1010, 1020, ..., 1990.
	for i := n - 1; i >= 0; i-- {
		require.Nil(t, l.Add(d.add(fmt.Sprintf("%05d", i*10+1000))))
	}

	require.False(t, it.SeekGE(makeKey("")))
	require.True(t, it.Valid())
	require.EqualValues(t, "01000", it.Key().UserKey)

	require.True(t, it.SeekGE(makeKey("01000")))
	require.True(t, it.Valid())
	require.EqualValues(t, "01000", it.Key().UserKey)

	require.False(t, it.SeekGE(makeKey("01005")))
	require.True(t, it.Valid())
	require.EqualValues(t, "01010", it.Key().UserKey)

	require.True(t, it.SeekGE(makeKey("01010")))
	require.True(t, it.Valid())
	require.EqualValues(t, "01010", it.Key().UserKey)

	require.False(t, it.SeekGE(makeKey("99999")))
	require.False(t, it.Valid())

	// Test seek for empty key.
	require.Nil(t, l.Add(d.add("")))
	require.True(t, it.SeekGE([]byte{}))
	require.True(t, it.Valid())

	require.True(t, it.SeekGE(makeKey("")))
	require.True(t, it.Valid())
}

func TestIteratorSeekLT(t *testing.T) {
	const n = 100
	d := &testStorage{}
	l := NewSkiplist(d, 0)
	it := l.NewIter()

	require.False(t, it.Valid())
	it.First()
	require.False(t, it.Valid())
	// 1000, 1010, 1020, ..., 1990.
	for i := n - 1; i >= 0; i-- {
		require.Nil(t, l.Add(d.add(fmt.Sprintf("%05d", i*10+1000))))
	}

	it.SeekLT(makeKey(""))
	require.False(t, it.Valid())

	it.SeekLT(makeKey("01000"))
	require.False(t, it.Valid())

	it.SeekLT(makeKey("01001"))
	require.EqualValues(t, "01000", it.Key().UserKey)
	require.True(t, it.Valid())

	it.SeekLT(makeKey("01005"))
	require.EqualValues(t, "01000", it.Key().UserKey)
	require.True(t, it.Valid())

	it.SeekLT(makeKey("01991"))
	require.EqualValues(t, "01990", it.Key().UserKey)
	require.True(t, it.Valid())

	it.SeekLT(makeKey("99999"))
	require.True(t, it.Valid())
	require.EqualValues(t, "01990", it.Key().UserKey)

	// Test seek for empty key.
	require.Nil(t, l.Add(d.add("")))
	it.SeekLT([]byte{})
	require.False(t, it.Valid())
	it.SeekLT(makeKey("\x01"))
	require.True(t, it.Valid())
	require.EqualValues(t, "", it.Key().UserKey)
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
			buf := make([]byte, b.N*8)
			d := &testStorage{
				keys: make([][]byte, 0, b.N),
			}
			l := NewSkiplist(d, 0)
			it := l.NewIter()
			rng := rand.New(rand.NewSource(time.Now().UnixNano()))

			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				key := randomKey(rng, buf[i*8:(i+1)*8])
				if rng.Float32() < readFrac {
					_ = it.SeekGE(key)
				} else {
					offset := uint32(len(d.keys))
					d.keys = append(d.keys, key)
					_ = l.Add(offset)
				}
			}
			b.StopTimer()
		})
	}
}

func BenchmarkIterNext(b *testing.B) {
	buf := make([]byte, 64<<10)
	d := &testStorage{
		keys: make([][]byte, 0, (64<<10)/8),
	}
	l := NewSkiplist(d, 0)

	rng := rand.New(rand.NewSource(time.Now().UnixNano()))
	for i := 0; i < len(buf); i += 8 {
		key := randomKey(rng, buf[i:i+8])
		offset := uint32(len(d.keys))
		d.keys = append(d.keys, key)
		_ = l.Add(offset)
	}

	it := l.NewIter()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if !it.Valid() {
			it.First()
		}
		it.Next()
	}
}

func BenchmarkIterPrev(b *testing.B) {
	buf := make([]byte, 64<<10)
	d := &testStorage{
		keys: make([][]byte, 0, (64<<10)/8),
	}
	l := NewSkiplist(d, 0)

	rng := rand.New(rand.NewSource(time.Now().UnixNano()))
	for i := 0; i < len(buf); i += 8 {
		key := randomKey(rng, buf[i:i+8])
		offset := uint32(len(d.keys))
		d.keys = append(d.keys, key)
		_ = l.Add(offset)
	}

	it := l.NewIter()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if !it.Valid() {
			it.Last()
		}
		it.Prev()
	}
}
