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

func newValue(v int) []byte {
	return []byte(fmt.Sprintf("%05d", v))
}

// length iterates over skiplist to give exact size.
func length(s *Skiplist) int {
	count := 0

	var it Iterator
	it.Init(s)

	for it.SeekToFirst(); it.Valid(); it.Next() {
		count++
	}

	return count
}

// length iterates over skiplist in reverse order to give exact size.
func lengthRev(s *Skiplist) int {
	count := 0

	var it Iterator
	it.Init(s)

	for it.SeekToLast(); it.Valid(); it.Prev() {
		count++
	}

	return count
}

func TestEmpty(t *testing.T) {
	key := []byte("aaa")
	l := NewSkiplist(NewArena(arenaSize))

	var it Iterator
	it.Init(l)

	require.False(t, it.Valid())

	it.SeekToFirst()
	require.False(t, it.Valid())

	it.SeekToLast()
	require.False(t, it.Valid())

	found := it.Seek(key)
	require.False(t, found)
	require.False(t, it.Valid())
}

func TestFull(t *testing.T) {
	l := NewSkiplist(NewArena(1000))

	var it Iterator
	it.Init(l)

	foundArenaFull := false
	for i := 0; i < 100; i++ {
		err := it.Add([]byte(fmt.Sprintf("%05d", i)), newValue(i), 0)
		if err == ErrArenaFull {
			foundArenaFull = true
		}
	}

	require.True(t, foundArenaFull)

	err := it.Set([]byte("someval"), 0)
	require.Equal(t, ErrArenaFull, err)

	// Delete does not perform any allocation.
	err = it.Delete()
	require.Nil(t, err)
}

// TestBasic tests single-threaded seeks and sets, adds, and deletes.
func TestBasic(t *testing.T) {
	l := NewSkiplist(NewArena(arenaSize))

	var it Iterator
	it.Init(l)

	val1 := newValue(42)
	val2 := newValue(52)
	val3 := newValue(62)
	val4 := newValue(72)

	// Try adding values.
	it.Add([]byte("key1"), val1, 0)
	it.Add([]byte("key3"), val3, 0xffff)
	it.Add([]byte("key2"), val2, 100)

	require.False(t, it.Seek([]byte("key")))

	require.True(t, it.Seek([]byte("key1")))
	require.EqualValues(t, "00042", it.Value())
	require.EqualValues(t, 0, it.Meta())

	require.True(t, it.Seek([]byte("key2")))
	require.EqualValues(t, "00052", it.Value())
	require.EqualValues(t, 100, it.Meta())

	require.True(t, it.Seek([]byte("key3")))
	require.EqualValues(t, "00062", it.Value())
	require.EqualValues(t, 0xffff, it.Meta())

	require.True(t, it.Seek([]byte("key2")))
	require.Nil(t, it.Set(val4, 101))
	require.EqualValues(t, "00072", it.Value())
	require.EqualValues(t, 101, it.Meta())

	require.True(t, it.Seek([]byte("key3")))
	require.Nil(t, it.Delete())
	require.True(t, !it.Valid())
}

// TestConcurrentBasic tests concurrent writes followed by concurrent reads.
func TestConcurrentBasic(t *testing.T) {
	const n = 1000

	// Set testing flag to make it easier to trigger unusual race conditions.
	l := NewSkiplist(NewArena(arenaSize))
	l.testing = true

	var wg sync.WaitGroup
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()

			var it Iterator
			it.Init(l)

			it.Add([]byte(fmt.Sprintf("%05d", i)), newValue(i), 0)
		}(i)
	}
	wg.Wait()

	// Check values. Concurrent reads.
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()

			var it Iterator
			it.Init(l)

			found := it.Seek([]byte(fmt.Sprintf("%05d", i)))
			require.True(t, found)
			require.EqualValues(t, newValue(i), it.Value())
		}(i)
	}
	wg.Wait()
	require.Equal(t, n, length(l))
	require.Equal(t, n, lengthRev(l))
}

// TestConcurrentOneKey will read while writing to one single key.
func TestConcurrentOneKey(t *testing.T) {
	const n = 100
	key := []byte("thekey")

	// Set testing flag to make it easier to trigger unusual race conditions.
	l := NewSkiplist(NewArena(arenaSize))
	l.testing = true

	var wg sync.WaitGroup
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()

			var it Iterator
			it.Init(l)
			it.Add(key, newValue(i), 0)
		}(i)
	}
	// We expect that at least some write made it such that some read returns a value.
	var sawValue int32
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()

			var it Iterator
			it.Init(l)
			if !it.Seek(key) {
				return
			}

			atomic.StoreInt32(&sawValue, 1)
			v, err := strconv.Atoi(string(it.Value()))
			require.NoError(t, err)
			require.True(t, 0 <= v && v < n)
		}()
	}
	wg.Wait()
	require.True(t, sawValue > 0)
	require.Equal(t, 1, length(l))
	require.Equal(t, 1, lengthRev(l))
}

func TestIteratorAdd(t *testing.T) {
	l := NewSkiplist(NewArena(arenaSize))

	var it Iterator
	it.Init(l)

	// Add nil key and value (treated same as empty).
	err := it.Add(nil, nil, 0)
	require.Nil(t, err)
	require.EqualValues(t, []byte{}, it.Key())
	require.EqualValues(t, []byte{}, it.Value())
	require.EqualValues(t, 0, it.Meta())
	it.Delete()

	// Add empty key and value (treated same as nil).
	err = it.Add([]byte{}, []byte{}, 0)
	require.Nil(t, err)
	require.EqualValues(t, []byte{}, it.Key())
	require.EqualValues(t, []byte{}, it.Value())
	require.EqualValues(t, 0, it.Meta())

	// Add to empty list.
	err = it.Add([]byte("00002"), []byte("00002"), 50)
	require.Nil(t, err)
	require.EqualValues(t, "00002", it.Value())
	require.EqualValues(t, 50, it.Meta())

	// Add first element in non-empty list.
	err = it.Add([]byte("00001"), []byte("00001"), 100)
	require.Nil(t, err)
	require.EqualValues(t, "00001", it.Value())
	require.EqualValues(t, 100, it.Meta())

	// Add last element in non-empty list.
	err = it.Add([]byte("00004"), []byte("00004"), 150)
	require.Nil(t, err)
	require.EqualValues(t, "00004", it.Value())
	require.EqualValues(t, 150, it.Meta())

	// Add element in middle of list.
	err = it.Add([]byte("00003"), []byte("00003"), 200)
	require.Nil(t, err)
	require.EqualValues(t, "00003", it.Value())
	require.EqualValues(t, 200, it.Meta())

	// Try to add element that already exists.
	err = it.Add([]byte("00002"), []byte("00002*"), 250)
	require.Equal(t, ErrRecordExists, err)
	require.EqualValues(t, []byte("00002"), it.Value())
	require.EqualValues(t, 50, it.Meta())

	// Try to add element that was previously deleted.
	it.Seek([]byte("00004"))
	it.Delete()
	err = it.Add([]byte("00004"), []byte("00004*"), 300)
	require.Nil(t, err)
	require.EqualValues(t, []byte("00004*"), it.Value())
	require.EqualValues(t, 300, it.Meta())

	require.Equal(t, 5, length(l))
	require.Equal(t, 5, lengthRev(l))
}

func TestIteratorSet(t *testing.T) {
	l := NewSkiplist(NewArena(arenaSize))

	var it Iterator
	it.Init(l)

	var it2 Iterator
	it2.Init(l)

	// Set when iterator position is invalid.
	require.Panics(t, func() { it.Set([]byte("00001a"), 0) })

	// Set new value.
	it.Add([]byte("00001"), []byte("00001a"), 100)
	err := it.Set([]byte("00001b"), 200)
	require.Nil(t, err)
	require.EqualValues(t, "00001b", it.Value())
	require.EqualValues(t, 200, it.Meta())

	// Try to set value that's been updated by a different iterator.
	it2.Seek([]byte("00001"))
	err = it.Set([]byte("00001c"), 300)
	require.Nil(t, err)
	err = it2.Set([]byte("00001d"), 400)
	require.Equal(t, ErrRecordUpdated, err)
	require.EqualValues(t, []byte("00001c"), it2.Value())
	require.EqualValues(t, 300, it2.Meta())
	err = it2.Set([]byte("00001d"), 400)
	require.Nil(t, err)
	require.EqualValues(t, "00001d", it2.Value())
	require.EqualValues(t, 400, it2.Meta())

	// Try to set value that's been deleted by a different iterator.
	it.Seek([]byte("00001"))
	it2.Seek([]byte("00001"))
	err = it.Delete()
	require.Nil(t, err)
	err = it.Add([]byte("00002"), []byte("00002"), 500)
	require.Nil(t, err)
	err = it2.Set([]byte("00001e"), 600)
	require.Equal(t, ErrRecordDeleted, err)
	require.EqualValues(t, "00001d", it2.Value())
	require.EqualValues(t, 400, it2.Meta())

	require.Equal(t, 1, length(l))
	require.Equal(t, 1, lengthRev(l))
}

func TestIteratorSetMeta(t *testing.T) {
	l := NewSkiplist(NewArena(arenaSize))

	var it Iterator
	it.Init(l)

	var it2 Iterator
	it2.Init(l)

	// SetMeta when iterator position is invalid.
	require.Panics(t, func() { it.SetMeta(0) })

	// SetMeta new value.
	it.Add([]byte("00001"), []byte("00001a"), 100)
	val := it.Value()
	err := it.SetMeta(200)
	require.Nil(t, err)
	require.EqualValues(t, "00001a", it.Value())
	require.True(t, &val[0] == &it.Value()[0], "Value bytes should not be reused")
	require.EqualValues(t, 200, it.Meta())

	// SetMeta new lower value.
	err = it.SetMeta(50)
	require.Nil(t, err)
	require.EqualValues(t, "00001a", it.Value())
	require.False(t, &val[0] == &it.Value()[0], "Value bytes should not be reused")
	require.EqualValues(t, 50, it.Meta())

	// Try to set meta that's been updated by a different iterator.
	it2.Seek([]byte("00001"))
	err = it.SetMeta(300)
	require.Nil(t, err)
	err = it2.SetMeta(400)
	require.Equal(t, ErrRecordUpdated, err)
	require.EqualValues(t, 300, it2.Meta())
	err = it2.SetMeta(400)
	require.Nil(t, err)
	require.EqualValues(t, 400, it2.Meta())

	// Try to set value that's been deleted by a different iterator.
	it.Seek([]byte("00001"))
	it2.Seek([]byte("00001"))
	err = it.Delete()
	require.Nil(t, err)
	err = it.Add([]byte("00002"), []byte("00002"), 500)
	require.Nil(t, err)
	err = it2.SetMeta(600)
	require.Equal(t, ErrRecordDeleted, err)
	require.EqualValues(t, 400, it2.Meta())

	require.Equal(t, 1, length(l))
	require.Equal(t, 1, lengthRev(l))
}

func TestIteratorDelete(t *testing.T) {
	l := NewSkiplist(NewArena(arenaSize))

	var it Iterator
	it.Init(l)

	var it2 Iterator
	it2.Init(l)

	// Delete when iterator position is invalid.
	require.Panics(t, func() { it.Delete() })

	it.Add([]byte("00001"), []byte("00001"), 100)
	it.Add([]byte("00002"), []byte("00002"), 200)
	it.Add([]byte("00003"), []byte("00003"), 300)
	it.Add([]byte("00004"), []byte("00004"), 400)

	// Delete first node.
	it.SeekToFirst()
	err := it.Delete()
	require.Nil(t, err)
	require.EqualValues(t, "00002", it.Value())
	require.EqualValues(t, 200, it.Meta())

	// Delete last node.
	it.Seek([]byte("00004"))
	err = it.Delete()
	require.Nil(t, err)
	require.False(t, it.Valid())
	require.Equal(t, 2, length(l))

	// Try to delete node that's been updated by another iterator.
	it.SeekToFirst()
	require.EqualValues(t, "00002", it.Value())
	it2.SeekToFirst()
	require.EqualValues(t, "00002", it2.Value())
	it2.Set([]byte("00002a"), 500)
	err = it.Delete()
	require.Equal(t, ErrRecordUpdated, err)
	require.EqualValues(t, "00002a", it.Value())
	require.EqualValues(t, 500, it.Meta())

	// Delete node that's been deleted by another iterator.
	err = it2.Delete()
	require.Nil(t, err)
	err = it.Delete()
	require.Nil(t, err)
	require.EqualValues(t, "00003", it.Value())
	require.EqualValues(t, 300, it.Meta())

	// Delete final node so that list is empty.
	err = it.Delete()
	require.Nil(t, err)
	require.False(t, it.Valid())
	require.Equal(t, 0, length(l))
	require.Equal(t, 0, lengthRev(l))
}

// TestConcurrentAdd races between adding same nodes.
func TestConcurrentAdd(t *testing.T) {
	const n = 100

	// Set testing flag to make it easier to trigger unusual race conditions.
	l := NewSkiplist(NewArena(arenaSize))
	l.testing = true

	start := make([]sync.WaitGroup, n)
	end := make([]sync.WaitGroup, n)

	for i := 0; i < n; i++ {
		start[i].Add(1)
		end[i].Add(2)
	}

	for f := 0; f < 2; f++ {
		go func() {
			var it Iterator
			it.Init(l)

			for i := 0; i < n; i++ {
				start[i].Wait()

				key := newValue(i)
				val := []byte(fmt.Sprintf("%d: %05d", f, i))
				if it.Add(key, val, 0) == nil {
					it.Seek(key)
					require.EqualValues(t, val, it.Value())
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

// TestConcurrentAddDelete races between adding and deleting the same node.
func TestConcurrentAddDelete(t *testing.T) {
	const n = 100

	// Set testing flag to make it easier to trigger unusual race conditions.
	l := NewSkiplist(NewArena(arenaSize))
	l.testing = true

	var wg sync.WaitGroup
	for i := 0; i < n; i++ {
		key := []byte("key")
		val := newValue(i)

		wg.Add(1)
		go func(i int) {
			defer wg.Done()

			var it Iterator
			it.Init(l)

			for {
				if it.Add(key, val, 0) == nil {
					require.EqualValues(t, val, it.Value())
					break
				}

				require.NotEqual(t, val, it.Value())
				if it.Delete() == nil {
					require.False(t, it.Valid())
				}
			}
		}(i)
	}
	wg.Wait()

	require.Equal(t, 1, length(l))
	require.Equal(t, 1, lengthRev(l))
}

// TestIteratorNext tests a basic iteration over all nodes from the beginning.
func TestIteratorNext(t *testing.T) {
	const n = 100
	l := NewSkiplist(NewArena(arenaSize))

	var it Iterator
	it.Init(l)

	require.False(t, it.Valid())

	it.SeekToFirst()
	require.False(t, it.Valid())

	for i := n - 1; i >= 0; i-- {
		it.Add([]byte(fmt.Sprintf("%05d", i)), newValue(i), 0)
	}

	it.SeekToFirst()
	for i := 0; i < n; i++ {
		require.True(t, it.Valid())
		require.EqualValues(t, newValue(i), it.Value())
		it.Next()
	}
	require.False(t, it.Valid())
}

// TestIteratorPrev tests a basic iteration over all nodes from the end.
func TestIteratorPrev(t *testing.T) {
	const n = 100
	l := NewSkiplist(NewArena(arenaSize))

	var it Iterator
	it.Init(l)

	require.False(t, it.Valid())

	it.SeekToLast()
	require.False(t, it.Valid())

	for i := 0; i < n; i++ {
		it.Add([]byte(fmt.Sprintf("%05d", i)), newValue(i), 0)
	}

	it.SeekToLast()
	for i := n - 1; i >= 0; i-- {
		require.True(t, it.Valid())
		require.EqualValues(t, newValue(i), it.Value())
		it.Prev()
	}
	require.False(t, it.Valid())
}

func TestIteratorSeek(t *testing.T) {
	const n = 100
	l := NewSkiplist(NewArena(arenaSize))

	var it Iterator
	it.Init(l)

	require.False(t, it.Valid())
	it.SeekToFirst()
	require.False(t, it.Valid())
	// 1000, 1010, 1020, ..., 1990.
	for i := n - 1; i >= 0; i-- {
		v := i*10 + 1000
		it.Add([]byte(fmt.Sprintf("%05d", i*10+1000)), newValue(v), uint16(v))
	}

	found := it.Seek([]byte(""))
	require.False(t, found)
	require.True(t, it.Valid())
	require.EqualValues(t, "01000", it.Value())
	require.EqualValues(t, 1000, it.Meta())

	found = it.Seek([]byte("01000"))
	require.True(t, found)
	require.True(t, it.Valid())
	require.EqualValues(t, "01000", it.Value())
	require.EqualValues(t, 1000, it.Meta())

	found = it.Seek([]byte("01005"))
	require.False(t, found)
	require.True(t, it.Valid())
	require.EqualValues(t, "01010", it.Value())
	require.EqualValues(t, 1010, it.Meta())

	found = it.Seek([]byte("01010"))
	require.True(t, found)
	require.True(t, it.Valid())
	require.EqualValues(t, "01010", it.Value())
	require.EqualValues(t, 1010, it.Meta())

	found = it.Seek([]byte("99999"))
	require.False(t, found)
	require.False(t, it.Valid())

	// Test seek for deleted key.
	it.Seek([]byte("01020"))
	it.Delete()
	found = it.Seek([]byte("01020"))
	require.False(t, found)
	require.True(t, it.Valid())
	require.EqualValues(t, "01030", it.Value())
	require.EqualValues(t, 1030, it.Meta())

	// Test seek for empty key.
	it.Add(nil, nil, 0)
	found = it.Seek(nil)
	require.True(t, found)
	require.True(t, it.Valid())
	require.EqualValues(t, "", it.Value())
	require.EqualValues(t, 0, it.Meta())

	found = it.Seek([]byte{})
	require.True(t, found)
	require.True(t, it.Valid())
	require.EqualValues(t, "", it.Value())
	require.EqualValues(t, 0, it.Meta())
}

func TestIteratorSeekForPrev(t *testing.T) {
	const n = 100
	l := NewSkiplist(NewArena(arenaSize))

	var it Iterator
	it.Init(l)

	require.False(t, it.Valid())
	it.SeekToFirst()
	require.False(t, it.Valid())
	// 1000, 1010, 1020, ..., 1990.
	for i := n - 1; i >= 0; i-- {
		v := i*10 + 1000
		it.Add([]byte(fmt.Sprintf("%05d", i*10+1000)), newValue(v), uint16(v))
	}

	found := it.SeekForPrev([]byte(""))
	require.False(t, found)
	require.False(t, it.Valid())

	found = it.SeekForPrev([]byte("00990"))
	require.False(t, found)
	require.False(t, it.Valid())

	found = it.SeekForPrev([]byte("01000"))
	require.True(t, found)
	require.True(t, it.Valid())
	require.EqualValues(t, "01000", it.Value())
	require.EqualValues(t, 1000, it.Meta())

	found = it.SeekForPrev([]byte("01005"))
	require.False(t, found)
	require.True(t, it.Valid())
	require.EqualValues(t, "01000", it.Value())
	require.EqualValues(t, 1000, it.Meta())

	found = it.SeekForPrev([]byte("01990"))
	require.True(t, found)
	require.True(t, it.Valid())
	require.EqualValues(t, "01990", it.Value())
	require.EqualValues(t, 1990, it.Meta())

	found = it.SeekForPrev([]byte("99999"))
	require.False(t, found)
	require.True(t, it.Valid())
	require.EqualValues(t, "01990", it.Value())
	require.EqualValues(t, 1990, it.Meta())

	// Test seek for deleted key.
	it.Seek([]byte("01020"))
	it.Delete()
	found = it.SeekForPrev([]byte("01020"))
	require.False(t, found)
	require.True(t, it.Valid())
	require.EqualValues(t, "01010", it.Value())
	require.EqualValues(t, 1010, it.Meta())

	// Test seek for empty key.
	it.Add(nil, nil, 0)
	found = it.SeekForPrev(nil)
	require.True(t, found)
	require.True(t, it.Valid())
	require.EqualValues(t, "", it.Value())
	require.EqualValues(t, 0, it.Meta())

	found = it.SeekForPrev([]byte{})
	require.True(t, found)
	require.True(t, it.Valid())
	require.EqualValues(t, "", it.Value())
	require.EqualValues(t, 0, it.Meta())
}

func randomKey(rng *rand.Rand) []byte {
	b := make([]byte, 8)
	key := rng.Uint32()
	key2 := rng.Uint32()
	binary.LittleEndian.PutUint32(b, key)
	binary.LittleEndian.PutUint32(b[4:], key2)
	return b
}

// Standard test. Some fraction is read. Some fraction is write. Writes have
// to go through mutex lock.
func BenchmarkReadWrite(b *testing.B) {
	value := newValue(123)
	for i := 0; i <= 10; i++ {
		readFrac := float32(i) / 10.0
		b.Run(fmt.Sprintf("frac_%d", i*10), func(b *testing.B) {
			l := NewSkiplist(NewArena(uint32((b.N + 2) * MaxNodeSize)))
			b.ResetTimer()
			var count int
			b.RunParallel(func(pb *testing.PB) {
				var iter Iterator
				iter.Init(l)

				rng := rand.New(rand.NewSource(time.Now().UnixNano()))

				for pb.Next() {
					if rng.Float32() < readFrac {
						if iter.Seek(randomKey(rng)) {
							_ = iter.Value()
							count++
						}
					} else {
						iter.Add(randomKey(rng), value, 0)
					}
				}
			})
		})
	}
}

// Standard test. Some fraction is read. Some fraction is write. Writes have
// to go through mutex lock.
func BenchmarkReadWriteMap(b *testing.B) {
	value := newValue(123)
	for i := 0; i <= 10; i++ {
		readFrac := float32(i) / 10.0
		b.Run(fmt.Sprintf("frac_%d", i), func(b *testing.B) {
			m := make(map[string][]byte)
			var mutex sync.RWMutex
			b.ResetTimer()
			var count int
			b.RunParallel(func(pb *testing.PB) {
				rng := rand.New(rand.NewSource(time.Now().UnixNano()))
				for pb.Next() {
					if rng.Float32() < readFrac {
						mutex.RLock()
						_, ok := m[string(randomKey(rng))]
						mutex.RUnlock()
						if ok {
							count++
						}
					} else {
						mutex.Lock()
						m[string(randomKey(rng))] = value
						mutex.Unlock()
					}
				}
			})
		})
	}
}
