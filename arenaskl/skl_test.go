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

	"github.com/stretchr/testify/require"
)

const arenaSize = 1 << 20

type testKey []byte

func (k testKey) Compare(cmp func(a, b []byte) int, other []byte) int {
	return cmp(k, other)
}

func (k testKey) Encode(buf []byte) {
	copy(buf, k)
}

func (k testKey) Size() int {
	return len(k)
}

func makeKey(i int) testKey {
	return []byte(fmt.Sprintf("%05d", i))
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
	key := testKey("aaa")
	l := NewSkiplist(NewArena(arenaSize), bytes.Compare)
	it := l.NewIter()

	require.False(t, it.Valid())

	it.First()
	require.False(t, it.Valid())

	it.Last()
	require.False(t, it.Valid())

	found := it.SeekGE(key)
	require.False(t, found)
	require.False(t, it.Valid())
}

func TestFull(t *testing.T) {
	l := NewSkiplist(NewArena(1000), bytes.Compare)

	foundArenaFull := false
	for i := 0; i < 100; i++ {
		err := l.Add(makeKey(i), makeValue(i))
		if err == ErrArenaFull {
			foundArenaFull = true
		}
	}

	require.True(t, foundArenaFull)

	err := l.Add(testKey("someval"), nil)
	require.Equal(t, ErrArenaFull, err)
}

// TestBasic tests single-threaded seeks and adds.
func TestBasic(t *testing.T) {
	l := NewSkiplist(NewArena(arenaSize), bytes.Compare)
	it := l.NewIter()

	// Try adding values.
	l.Add(testKey("key1"), makeValue(1))
	l.Add(testKey("key3"), makeValue(3))
	l.Add(testKey("key2"), makeValue(2))

	require.False(t, it.SeekGE(testKey("key")))

	require.True(t, it.SeekGE(testKey("key1")))
	require.EqualValues(t, "key1", it.Key())
	require.EqualValues(t, makeValue(1), it.Value())

	require.True(t, it.SeekGE(testKey("key2")))
	require.EqualValues(t, "key2", it.Key())
	require.EqualValues(t, makeValue(2), it.Value())

	require.True(t, it.SeekGE(testKey("key3")))
	require.EqualValues(t, "key3", it.Key())
	require.EqualValues(t, makeValue(3), it.Value())
}

// TestConcurrentBasic tests concurrent writes followed by concurrent reads.
func TestConcurrentBasic(t *testing.T) {
	const n = 1000

	// Set testing flag to make it easier to trigger unusual race conditions.
	l := NewSkiplist(NewArena(arenaSize), bytes.Compare)
	l.testing = true

	var wg sync.WaitGroup
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()

			l.Add(makeKey(i), makeValue(i))
		}(i)
	}
	wg.Wait()

	// Check values. Concurrent reads.
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()

			it := l.NewIter()
			found := it.SeekGE(testKey(fmt.Sprintf("%05d", i)))
			require.True(t, found)
			require.EqualValues(t, fmt.Sprintf("%05d", i), it.Key())
		}(i)
	}
	wg.Wait()
	require.Equal(t, n, length(l))
	require.Equal(t, n, lengthRev(l))
}

// TestConcurrentOneKey will read while writing to one single key.
func TestConcurrentOneKey(t *testing.T) {
	const n = 100
	key := testKey("thekey")

	// Set testing flag to make it easier to trigger unusual race conditions.
	l := NewSkiplist(NewArena(arenaSize), bytes.Compare)
	l.testing = true

	var wg sync.WaitGroup
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()

			l.Add(key, makeValue(i))
		}(i)
	}
	// We expect that at least some write made it such that some read returns a value.
	var sawValue int32
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()

			it := l.NewIter()
			if !it.SeekGE(key) {
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
	l := NewSkiplist(NewArena(arenaSize), bytes.Compare)
	it := l.NewIter()

	// Add nil key and value (treated same as empty).
	err := l.Add(testKey(nil), nil)
	require.Nil(t, err)
	require.True(t, it.SeekGE(testKey(nil)))
	require.EqualValues(t, testKey{}, it.Key())
	require.EqualValues(t, testKey{}, it.Value())

	l = NewSkiplist(NewArena(arenaSize), bytes.Compare)
	it = l.NewIter()

	// Add empty key and value (treated same as nil).
	err = l.Add(testKey{}, testKey{})
	require.Nil(t, err)
	require.True(t, it.SeekGE(testKey(nil)))
	require.EqualValues(t, testKey{}, it.Key())
	require.EqualValues(t, testKey{}, it.Value())

	// Add to empty list.
	err = l.Add(makeKey(2), makeValue(2))
	require.Nil(t, err)
	require.True(t, it.SeekGE(testKey("00002")))
	require.EqualValues(t, makeKey(2), it.Key())
	require.EqualValues(t, makeValue(2), it.Value())

	// Add first element in non-empty list.
	err = l.Add(makeKey(1), makeValue(1))
	require.Nil(t, err)
	require.True(t, it.SeekGE(testKey("00001")))
	require.EqualValues(t, makeKey(1), it.Key())
	require.EqualValues(t, makeValue(1), it.Value())

	// Add last element in non-empty list.
	err = l.Add(makeKey(4), makeValue(4))
	require.Nil(t, err)
	require.True(t, it.SeekGE(testKey("00004")))
	require.EqualValues(t, makeKey(4), it.Key())
	require.EqualValues(t, makeValue(4), it.Value())

	// Add element in middle of list.
	err = l.Add(makeKey(3), makeValue(3))
	require.Nil(t, err)
	require.True(t, it.SeekGE(testKey("00003")))
	require.EqualValues(t, makeKey(3), it.Key())
	require.EqualValues(t, makeValue(3), it.Value())

	// Try to add element that already exists.
	err = l.Add(makeKey(2), nil)
	require.Equal(t, ErrRecordExists, err)
	require.EqualValues(t, makeKey(3), it.Key())
	require.EqualValues(t, makeValue(3), it.Value())

	require.Equal(t, 5, length(l))
	require.Equal(t, 5, lengthRev(l))
}

// TestConcurrentAdd races between adding same nodes.
func TestConcurrentAdd(t *testing.T) {
	const n = 100

	// Set testing flag to make it easier to trigger unusual race conditions.
	l := NewSkiplist(NewArena(arenaSize), bytes.Compare)
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

				key := makeKey(i)
				if l.Add(key, nil) == nil {
					it.SeekGE(key)
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
	l := NewSkiplist(NewArena(arenaSize), bytes.Compare)
	it := l.NewIter()

	require.False(t, it.Valid())

	it.First()
	require.False(t, it.Valid())

	for i := n - 1; i >= 0; i-- {
		l.Add(makeKey(i), makeValue(i))
	}

	it.First()
	for i := 0; i < n; i++ {
		require.True(t, it.Valid())
		require.EqualValues(t, makeKey(i), it.Key())
		require.EqualValues(t, makeValue(i), it.Value())
		it.Next()
	}
	require.False(t, it.Valid())
}

// TestIteratorPrev tests a basic iteration over all nodes from the end.
func TestIteratorPrev(t *testing.T) {
	const n = 100
	l := NewSkiplist(NewArena(arenaSize), bytes.Compare)
	it := l.NewIter()

	require.False(t, it.Valid())

	it.Last()
	require.False(t, it.Valid())

	for i := 0; i < n; i++ {
		l.Add(makeKey(i), makeValue(i))
	}

	it.Last()
	for i := n - 1; i >= 0; i-- {
		require.True(t, it.Valid())
		require.EqualValues(t, makeKey(i), it.Key())
		require.EqualValues(t, makeValue(i), it.Value())
		it.Prev()
	}
	require.False(t, it.Valid())
}

func TestIteratorSeekGE(t *testing.T) {
	const n = 100
	l := NewSkiplist(NewArena(arenaSize), bytes.Compare)
	it := l.NewIter()

	require.False(t, it.Valid())
	it.First()
	require.False(t, it.Valid())
	// 1000, 1010, 1020, ..., 1990.
	for i := n - 1; i >= 0; i-- {
		v := i*10 + 1000
		l.Add(makeKey(v), makeValue(v))
	}

	found := it.SeekGE(testKey(""))
	require.False(t, found)
	require.True(t, it.Valid())
	require.EqualValues(t, "01000", it.Key())
	require.EqualValues(t, "v01000", it.Value())

	found = it.SeekGE(testKey("01000"))
	require.True(t, found)
	require.True(t, it.Valid())
	require.EqualValues(t, "01000", it.Key())
	require.EqualValues(t, "v01000", it.Value())

	found = it.SeekGE(testKey("01005"))
	require.False(t, found)
	require.True(t, it.Valid())
	require.EqualValues(t, "01010", it.Key())
	require.EqualValues(t, "v01010", it.Value())

	found = it.SeekGE(testKey("01010"))
	require.True(t, found)
	require.True(t, it.Valid())
	require.EqualValues(t, "01010", it.Key())
	require.EqualValues(t, "v01010", it.Value())

	found = it.SeekGE(testKey("99999"))
	require.False(t, found)
	require.False(t, it.Valid())

	// Test seek for empty key.
	l.Add(testKey(nil), nil)
	found = it.SeekGE(testKey(nil))
	require.True(t, found)
	require.True(t, it.Valid())
	require.EqualValues(t, "", it.Key())

	found = it.SeekGE(testKey{})
	require.True(t, found)
	require.True(t, it.Valid())
	require.EqualValues(t, "", it.Key())
}

func TestIteratorSeekLE(t *testing.T) {
	const n = 100
	l := NewSkiplist(NewArena(arenaSize), bytes.Compare)
	it := l.NewIter()

	require.False(t, it.Valid())
	it.First()
	require.False(t, it.Valid())
	// 1000, 1010, 1020, ..., 1990.
	for i := n - 1; i >= 0; i-- {
		v := i*10 + 1000
		l.Add(makeKey(v), makeValue(v))
	}

	found := it.SeekLE(testKey(""))
	require.False(t, found)
	require.False(t, it.Valid())

	found = it.SeekLE(testKey("00990"))
	require.False(t, found)
	require.False(t, it.Valid())

	found = it.SeekLE(testKey("01000"))
	require.True(t, found)
	require.True(t, it.Valid())
	require.EqualValues(t, "01000", it.Key())
	require.EqualValues(t, "v01000", it.Value())

	found = it.SeekLE(testKey("01005"))
	require.False(t, found)
	require.True(t, it.Valid())
	require.EqualValues(t, "01000", it.Key())
	require.EqualValues(t, "v01000", it.Value())

	found = it.SeekLE(testKey("01990"))
	require.True(t, found)
	require.True(t, it.Valid())
	require.EqualValues(t, "01990", it.Key())
	require.EqualValues(t, "v01990", it.Value())

	found = it.SeekLE(testKey("99999"))
	require.False(t, found)
	require.True(t, it.Valid())
	require.EqualValues(t, "01990", it.Key())
	require.EqualValues(t, "v01990", it.Value())

	// Test seek for empty key.
	l.Add(testKey(nil), nil)
	found = it.SeekLE(testKey(nil))
	require.True(t, found)
	require.True(t, it.Valid())
	require.EqualValues(t, "", it.Key())

	found = it.SeekLE(testKey{})
	require.True(t, found)
	require.True(t, it.Valid())
	require.EqualValues(t, "", it.Key())
}

func randomKey(rng *rand.Rand) testKey {
	b := make(testKey, 8)
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
			l := NewSkiplist(NewArena(uint32((b.N+2)*maxNodeSize)), bytes.Compare)
			b.ResetTimer()
			var count int
			b.RunParallel(func(pb *testing.PB) {
				it := l.NewIter()
				rng := rand.New(rand.NewSource(time.Now().UnixNano()))

				for pb.Next() {
					if rng.Float32() < readFrac {
						if it.SeekGE(randomKey(rng)) {
							_ = it.Key()
							count++
						}
					} else {
						l.Add(randomKey(rng), nil)
					}
				}
			})
		})
	}
}

func BenchmarkIterNext(b *testing.B) {
	l := NewSkiplist(NewArena(64<<10), bytes.Compare)
	rng := rand.New(rand.NewSource(time.Now().UnixNano()))
	for {
		if err := l.Add(randomKey(rng), nil); err == ErrArenaFull {
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
	l := NewSkiplist(NewArena(64<<10), bytes.Compare)
	rng := rand.New(rand.NewSource(time.Now().UnixNano()))
	for {
		if err := l.Add(randomKey(rng), nil); err == ErrArenaFull {
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
