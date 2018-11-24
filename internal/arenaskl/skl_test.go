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

package arenaskl

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"math/rand"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/petermattis/pebble/db"
	"github.com/stretchr/testify/require"
)

const arenaSize = 1 << 20

func makeIntKey(i int) db.InternalKey {
	return db.InternalKey{UserKey: []byte(fmt.Sprintf("%05d", i))}
}

func makeKey(s string) []byte {
	return []byte(s)
}

func makeIkey(s string) db.InternalKey {
	return db.InternalKey{UserKey: []byte(s)}
}

func makeValue(i int) []byte {
	return []byte(fmt.Sprintf("v%05d", i))
}

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

func TestEmpty(t *testing.T) {
	key := makeKey("aaa")
	l := NewSkiplist(NewArena(arenaSize, 0), bytes.Compare)
	it := l.NewIter()

	require.False(t, it.Valid())

	it.First()
	require.False(t, it.Valid())

	it.Last()
	require.False(t, it.Valid())

	it.SeekGE(key)
	require.False(t, it.Valid())
}

func TestFull(t *testing.T) {
	l := NewSkiplist(NewArena(1000, 0), bytes.Compare)

	foundArenaFull := false
	for i := 0; i < 100; i++ {
		err := l.Add(makeIntKey(i), makeValue(i))
		if err == ErrArenaFull {
			foundArenaFull = true
			break
		}
	}

	require.True(t, foundArenaFull)

	err := l.Add(makeIkey("someval"), nil)
	require.Equal(t, ErrArenaFull, err)
}

// TestBasic tests single-threaded seeks and adds.
func TestBasic(t *testing.T) {
	l := NewSkiplist(NewArena(arenaSize, 0), bytes.Compare)
	it := l.NewIter()

	// Try adding values.
	l.Add(makeIkey("key1"), makeValue(1))
	l.Add(makeIkey("key3"), makeValue(3))
	l.Add(makeIkey("key2"), makeValue(2))

	it.SeekGE(makeKey("key"))
	require.True(t, it.Valid())
	require.NotEqual(t, "key", it.Key().UserKey)

	it.SeekGE(makeKey("key1"))
	require.EqualValues(t, "key1", it.Key().UserKey)
	require.EqualValues(t, makeValue(1), it.Value())

	it.SeekGE(makeKey("key2"))
	require.EqualValues(t, "key2", it.Key().UserKey)
	require.EqualValues(t, makeValue(2), it.Value())

	it.SeekGE(makeKey("key3"))
	require.EqualValues(t, "key3", it.Key().UserKey)
	require.EqualValues(t, makeValue(3), it.Value())
}

// TestConcurrentBasic tests concurrent writes followed by concurrent reads.
func TestConcurrentBasic(t *testing.T) {
	const n = 1000

	// Set testing flag to make it easier to trigger unusual race conditions.
	l := NewSkiplist(NewArena(arenaSize, 0), bytes.Compare)
	l.testing = true

	var wg sync.WaitGroup
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()

			l.Add(makeIntKey(i), makeValue(i))
		}(i)
	}
	wg.Wait()

	// Check values. Concurrent reads.
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()

			it := l.NewIter()
			it.SeekGE(makeKey(fmt.Sprintf("%05d", i)))
			require.EqualValues(t, fmt.Sprintf("%05d", i), it.Key().UserKey)
		}(i)
	}
	wg.Wait()
	require.Equal(t, n, length(l))
	require.Equal(t, n, lengthRev(l))
}

// TestConcurrentOneKey will read while writing to one single key.
func TestConcurrentOneKey(t *testing.T) {
	const n = 100
	key := makeKey("thekey")
	ikey := makeIkey("thekey")

	// Set testing flag to make it easier to trigger unusual race conditions.
	l := NewSkiplist(NewArena(arenaSize, 0), bytes.Compare)
	l.testing = true

	var wg sync.WaitGroup
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()

			l.Add(ikey, makeValue(i))
		}(i)
	}
	// We expect that at least some write made it such that some read returns a value.
	var sawValue int32
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()

			it := l.NewIter()
			it.SeekGE(key)
			if !it.Valid() || !bytes.Equal(key, it.Key().UserKey) {
				return
			}

			atomic.StoreInt32(&sawValue, 1)
			v, err := strconv.Atoi(string(it.Value()[1:]))
			require.NoError(t, err)
			require.True(t, 0 <= v && v < n)
		}()
	}
	wg.Wait()
	require.True(t, sawValue > 0)
	require.Equal(t, 1, length(l))
	require.Equal(t, 1, lengthRev(l))
}

func TestSkiplistAdd(t *testing.T) {
	l := NewSkiplist(NewArena(arenaSize, 0), bytes.Compare)
	it := l.NewIter()

	// Add nil key and value (treated same as empty).
	err := l.Add(db.InternalKey{}, nil)
	require.Nil(t, err)
	it.SeekGE([]byte{})
	require.EqualValues(t, []byte{}, it.Key().UserKey)
	require.EqualValues(t, []byte{}, it.Value())

	l = NewSkiplist(NewArena(arenaSize, 0), bytes.Compare)
	it = l.NewIter()

	// Add empty key and value (treated same as nil).
	err = l.Add(makeIkey(""), []byte{})
	require.Nil(t, err)
	it.SeekGE([]byte{})
	require.EqualValues(t, []byte{}, it.Key().UserKey)
	require.EqualValues(t, []byte{}, it.Value())

	// Add to empty list.
	err = l.Add(makeIntKey(2), makeValue(2))
	require.Nil(t, err)
	it.SeekGE(makeKey("00002"))
	require.EqualValues(t, "00002", it.Key().UserKey)
	require.EqualValues(t, makeValue(2), it.Value())

	// Add first element in non-empty list.
	err = l.Add(makeIntKey(1), makeValue(1))
	require.Nil(t, err)
	it.SeekGE(makeKey("00001"))
	require.EqualValues(t, "00001", it.Key().UserKey)
	require.EqualValues(t, makeValue(1), it.Value())

	// Add last element in non-empty list.
	err = l.Add(makeIntKey(4), makeValue(4))
	require.Nil(t, err)
	it.SeekGE(makeKey("00004"))
	require.EqualValues(t, "00004", it.Key().UserKey)
	require.EqualValues(t, makeValue(4), it.Value())

	// Add element in middle of list.
	err = l.Add(makeIntKey(3), makeValue(3))
	require.Nil(t, err)
	it.SeekGE(makeKey("00003"))
	require.EqualValues(t, "00003", it.Key().UserKey)
	require.EqualValues(t, makeValue(3), it.Value())

	// Try to add element that already exists.
	err = l.Add(makeIntKey(2), nil)
	require.Equal(t, ErrRecordExists, err)
	require.EqualValues(t, "00003", it.Key().UserKey)
	require.EqualValues(t, makeValue(3), it.Value())

	require.Equal(t, 5, length(l))
	require.Equal(t, 5, lengthRev(l))
}

// TestConcurrentAdd races between adding same nodes.
func TestConcurrentAdd(t *testing.T) {
	const n = 100

	// Set testing flag to make it easier to trigger unusual race conditions.
	l := NewSkiplist(NewArena(arenaSize, 0), bytes.Compare)
	l.testing = true

	start := make([]sync.WaitGroup, n)
	end := make([]sync.WaitGroup, n)

	for i := 0; i < n; i++ {
		start[i].Add(1)
		end[i].Add(2)
	}

	for f := 0; f < 2; f++ {
		go func() {
			it := l.NewIter()

			for i := 0; i < n; i++ {
				start[i].Wait()

				key := makeIntKey(i)
				if l.Add(key, nil) == nil {
					it.SeekGE(key.UserKey)
					require.EqualValues(t, key, it.Key())
				}

				end[i].Done()
			}
		}()
	}

	for i := 0; i < n; i++ {
		start[i].Done()
		end[i].Wait()
	}

	require.Equal(t, n, length(l))
	require.Equal(t, n, lengthRev(l))
}

// TestIteratorNext tests a basic iteration over all nodes from the beginning.
func TestIteratorNext(t *testing.T) {
	const n = 100
	l := NewSkiplist(NewArena(arenaSize, 0), bytes.Compare)
	it := l.NewIter()

	require.False(t, it.Valid())

	it.First()
	require.False(t, it.Valid())

	for i := n - 1; i >= 0; i-- {
		l.Add(makeIntKey(i), makeValue(i))
	}

	it.First()
	for i := 0; i < n; i++ {
		require.True(t, it.Valid())
		require.EqualValues(t, makeIntKey(i), it.Key())
		require.EqualValues(t, makeValue(i), it.Value())
		it.Next()
	}
	require.False(t, it.Valid())
}

// TestIteratorPrev tests a basic iteration over all nodes from the end.
func TestIteratorPrev(t *testing.T) {
	const n = 100
	l := NewSkiplist(NewArena(arenaSize, 0), bytes.Compare)
	it := l.NewIter()

	require.False(t, it.Valid())

	it.Last()
	require.False(t, it.Valid())

	for i := 0; i < n; i++ {
		l.Add(makeIntKey(i), makeValue(i))
	}

	it.Last()
	for i := n - 1; i >= 0; i-- {
		require.True(t, it.Valid())
		require.EqualValues(t, makeIntKey(i), it.Key())
		require.EqualValues(t, makeValue(i), it.Value())
		it.Prev()
	}
	require.False(t, it.Valid())
}

func TestIteratorSeekGE(t *testing.T) {
	const n = 100
	l := NewSkiplist(NewArena(arenaSize, 0), bytes.Compare)
	it := l.NewIter()

	require.False(t, it.Valid())
	it.First()
	require.False(t, it.Valid())
	// 1000, 1010, 1020, ..., 1990.
	for i := n - 1; i >= 0; i-- {
		v := i*10 + 1000
		l.Add(makeIntKey(v), makeValue(v))
	}

	it.SeekGE(makeKey(""))
	require.True(t, it.Valid())
	require.EqualValues(t, "01000", it.Key().UserKey)
	require.EqualValues(t, "v01000", it.Value())

	it.SeekGE(makeKey("01000"))
	require.True(t, it.Valid())
	require.EqualValues(t, "01000", it.Key().UserKey)
	require.EqualValues(t, "v01000", it.Value())

	it.SeekGE(makeKey("01005"))
	require.True(t, it.Valid())
	require.EqualValues(t, "01010", it.Key().UserKey)
	require.EqualValues(t, "v01010", it.Value())

	it.SeekGE(makeKey("01010"))
	require.True(t, it.Valid())
	require.EqualValues(t, "01010", it.Key().UserKey)
	require.EqualValues(t, "v01010", it.Value())

	it.SeekGE(makeKey("99999"))
	require.False(t, it.Valid())

	// Test seek for empty key.
	l.Add(db.InternalKey{}, nil)
	it.SeekGE([]byte{})
	require.True(t, it.Valid())
	require.EqualValues(t, "", it.Key().UserKey)

	it.SeekGE(makeKey(""))
	require.True(t, it.Valid())
	require.EqualValues(t, "", it.Key().UserKey)
}

func TestIteratorSeekLT(t *testing.T) {
	const n = 100
	l := NewSkiplist(NewArena(arenaSize, 0), bytes.Compare)
	it := l.NewIter()

	require.False(t, it.Valid())
	it.First()
	require.False(t, it.Valid())
	// 1000, 1010, 1020, ..., 1990.
	for i := n - 1; i >= 0; i-- {
		v := i*10 + 1000
		l.Add(makeIntKey(v), makeValue(v))
	}

	it.SeekLT(makeKey(""))
	require.False(t, it.Valid())

	it.SeekLT(makeKey("01000"))
	require.False(t, it.Valid())

	it.SeekLT(makeKey("01001"))
	require.True(t, it.Valid())
	require.EqualValues(t, "01000", it.Key().UserKey)
	require.EqualValues(t, "v01000", it.Value())

	it.SeekLT(makeKey("01005"))
	require.True(t, it.Valid())
	require.EqualValues(t, "01000", it.Key().UserKey)
	require.EqualValues(t, "v01000", it.Value())

	it.SeekLT(makeKey("01991"))
	require.True(t, it.Valid())
	require.EqualValues(t, "01990", it.Key().UserKey)
	require.EqualValues(t, "v01990", it.Value())

	it.SeekLT(makeKey("99999"))
	require.True(t, it.Valid())
	require.EqualValues(t, "01990", it.Key().UserKey)
	require.EqualValues(t, "v01990", it.Value())

	// Test seek for empty key.
	l.Add(db.InternalKey{}, nil)
	it.SeekLT([]byte{})
	require.False(t, it.Valid())

	it.SeekLT(makeKey("\x01"))
	require.True(t, it.Valid())
	require.EqualValues(t, "", it.Key().UserKey)
}

func TestExtValue(t *testing.T) {
	l := NewSkiplist(NewArena(1000, 5), bytes.Compare)
	l.Add(makeIkey("a"), []byte("aaaaa"))
	l.Add(makeIkey("b"), []byte("bbbbb"))
	l.Add(makeIkey("c"), []byte("cccc"))

	it := l.NewIter()
	it.First()
	require.True(t, it.Valid())
	require.True(t, it.nd.valueSize < 0)
	require.EqualValues(t, "aaaaa", it.Value())

	it.Next()
	require.True(t, it.Valid())
	require.True(t, it.nd.valueSize < 0)
	require.EqualValues(t, "bbbbb", it.Value())

	it.Next()
	require.True(t, it.Valid())
	require.True(t, it.nd.valueSize > 0)
	require.EqualValues(t, "cccc", it.Value())
}

func randomKey(rng *rand.Rand, b []byte) db.InternalKey {
	key := rng.Uint32()
	key2 := rng.Uint32()
	binary.LittleEndian.PutUint32(b, key)
	binary.LittleEndian.PutUint32(b[4:], key2)
	return db.InternalKey{UserKey: b}
}

// Standard test. Some fraction is read. Some fraction is write. Writes have
// to go through mutex lock.
func BenchmarkReadWrite(b *testing.B) {
	for i := 0; i <= 10; i++ {
		readFrac := float32(i) / 10.0
		b.Run(fmt.Sprintf("frac_%d", i*10), func(b *testing.B) {
			l := NewSkiplist(NewArena(uint32((b.N+2)*maxNodeSize), 0), bytes.Compare)
			b.ResetTimer()
			var count int
			b.RunParallel(func(pb *testing.PB) {
				it := l.NewIter()
				rng := rand.New(rand.NewSource(time.Now().UnixNano()))
				buf := make([]byte, 8)

				for pb.Next() {
					if rng.Float32() < readFrac {
						it.SeekGE(randomKey(rng, buf).UserKey)
						if it.Valid() {
							_ = it.Key()
							count++
						}
					} else {
						l.Add(randomKey(rng, buf), nil)
					}
				}
			})
		})
	}
}

func BenchmarkOrderedWrite(b *testing.B) {
	l := NewSkiplist(NewArena(8<<20, 0), bytes.Compare)
	buf := make([]byte, 8)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		binary.BigEndian.PutUint64(buf, uint64(i))
		if err := l.Add(db.InternalKey{UserKey: buf}, nil); err == ErrArenaFull {
			b.StopTimer()
			l = NewSkiplist(NewArena(uint32((b.N+2)*maxNodeSize), 0), bytes.Compare)
			b.StartTimer()
		}
	}
}

func BenchmarkIterNext(b *testing.B) {
	l := NewSkiplist(NewArena(64<<10, 0), bytes.Compare)
	rng := rand.New(rand.NewSource(time.Now().UnixNano()))
	buf := make([]byte, 8)
	for {
		if err := l.Add(randomKey(rng, buf), nil); err == ErrArenaFull {
			break
		}
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
	l := NewSkiplist(NewArena(64<<10, 0), bytes.Compare)
	rng := rand.New(rand.NewSource(time.Now().UnixNano()))
	buf := make([]byte, 8)
	for {
		if err := l.Add(randomKey(rng, buf), nil); err == ErrArenaFull {
			break
		}
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

// Standard test. Some fraction is read. Some fraction is write. Writes have
// to go through mutex lock.
// func BenchmarkReadWriteMap(b *testing.B) {
// 	for i := 0; i <= 10; i++ {
// 		readFrac := float32(i) / 10.0
// 		b.Run(fmt.Sprintf("frac_%d", i*10), func(b *testing.B) {
// 			m := make(map[string]struct{})
// 			var mutex sync.RWMutex
// 			b.ResetTimer()
// 			var count int
// 			b.RunParallel(func(pb *testing.PB) {
// 				rng := rand.New(rand.NewSource(time.Now().UnixNano()))
// 				for pb.Next() {
// 					if rng.Float32() < readFrac {
// 						mutex.RLock()
// 						_, ok := m[string(randomKey(rng))]
// 						mutex.RUnlock()
// 						if ok {
// 							count++
// 						}
// 					} else {
// 						mutex.Lock()
// 						m[string(randomKey(rng))] = struct{}{}
// 						mutex.Unlock()
// 					}
// 				}
// 			})
// 		})
// 	}
// }
