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
	"math"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/petermattis/pebble/internal/base"
	"github.com/stretchr/testify/require"
	"golang.org/x/exp/rand"
)

const arenaSize = 1 << 20

// iterAdapter adapts the new Iterator API which returns the key and value from
// positioning methods (Seek*, First, Last, Next, Prev) to the old API which
// returned a boolean corresponding to Valid. Only used by test code.
type iterAdapter struct {
	*Iterator
}

func (i *iterAdapter) verify(key *base.InternalKey, val []byte) bool {
	valid := key != nil
	if valid != i.Valid() {
		panic(fmt.Sprintf("inconsistent valid: %t != %t", valid, i.Valid()))
	}
	if valid {
		if base.InternalCompare(bytes.Compare, *key, i.Key()) != 0 {
			panic(fmt.Sprintf("inconsistent key: %s != %s", *key, i.Key()))
		}
		if !bytes.Equal(val, i.Value()) {
			panic(fmt.Sprintf("inconsistent value: [% x] != [% x]", val, i.Value()))
		}
	}
	return valid
}

func (i *iterAdapter) SeekGE(key []byte) bool {
	return i.verify(i.Iterator.SeekGE(key))
}

func (i *iterAdapter) SeekPrefixGE(prefix, key []byte) bool {
	return i.verify(i.Iterator.SeekPrefixGE(prefix, key))
}

func (i *iterAdapter) SeekLT(key []byte) bool {
	return i.verify(i.Iterator.SeekLT(key))
}

func (i *iterAdapter) First() bool {
	return i.verify(i.Iterator.First())
}

func (i *iterAdapter) Last() bool {
	return i.verify(i.Iterator.Last())
}

func (i *iterAdapter) Next() bool {
	return i.verify(i.Iterator.Next())
}

func (i *iterAdapter) Prev() bool {
	return i.verify(i.Iterator.Prev())
}

func (i *iterAdapter) Key() base.InternalKey {
	return *i.Iterator.Key()
}

func makeIntKey(i int) base.InternalKey {
	return base.InternalKey{UserKey: []byte(fmt.Sprintf("%05d", i))}
}

func makeKey(s string) []byte {
	return []byte(s)
}

func makeIkey(s string) base.InternalKey {
	return base.InternalKey{UserKey: []byte(s)}
}

func makeValue(i int) []byte {
	return []byte(fmt.Sprintf("v%05d", i))
}

func makeInserterAdd(s *Skiplist) func(key base.InternalKey, value []byte) error {
	ins := &Inserter{}
	return func(key base.InternalKey, value []byte) error {
		return ins.Add(s, key, value)
	}
}

// length iterates over skiplist to give exact size.
func length(s *Skiplist) int {
	count := 0

	it := iterAdapter{s.NewIter(nil, nil)}
	for valid := it.First(); valid; valid = it.Next() {
		count++
	}

	return count
}

// length iterates over skiplist in reverse order to give exact size.
func lengthRev(s *Skiplist) int {
	count := 0

	it := iterAdapter{s.NewIter(nil, nil)}
	for valid := it.Last(); valid; valid = it.Prev() {
		count++
	}

	return count
}

func TestEmpty(t *testing.T) {
	key := makeKey("aaa")
	l := NewSkiplist(NewArena(arenaSize, 0), bytes.Compare)
	it := iterAdapter{l.NewIter(nil, nil)}

	require.False(t, it.Valid())

	it.First()
	require.False(t, it.Valid())

	it.Last()
	require.False(t, it.Valid())

	require.False(t, it.SeekGE(key))
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
	for _, inserter := range []bool{false, true} {
		t.Run(fmt.Sprintf("inserter=%t", inserter), func(t *testing.T) {
			l := NewSkiplist(NewArena(arenaSize, 0), bytes.Compare)
			it := iterAdapter{l.NewIter(nil, nil)}

			add := l.Add
			if inserter {
				add = makeInserterAdd(l)
			}

			// Try adding values.
			add(makeIkey("key1"), makeValue(1))
			add(makeIkey("key3"), makeValue(3))
			add(makeIkey("key2"), makeValue(2))

			require.True(t, it.SeekGE(makeKey("key")))
			require.True(t, it.Valid())
			require.NotEqual(t, "key", it.Key().UserKey)

			require.True(t, it.SeekGE(makeKey("key1")))
			require.EqualValues(t, "key1", it.Key().UserKey)
			require.EqualValues(t, makeValue(1), it.Value())

			require.True(t, it.SeekGE(makeKey("key2")))
			require.EqualValues(t, "key2", it.Key().UserKey)
			require.EqualValues(t, makeValue(2), it.Value())

			require.True(t, it.SeekGE(makeKey("key3")))
			require.EqualValues(t, "key3", it.Key().UserKey)
			require.EqualValues(t, makeValue(3), it.Value())

			key := makeIkey("a")
			key.SetSeqNum(1)
			add(key, nil)
			key.SetSeqNum(2)
			add(key, nil)

			require.True(t, it.SeekGE(makeKey("a")))
			require.True(t, it.Valid())
			require.EqualValues(t, "a", it.Key().UserKey)
			require.EqualValues(t, 2, it.Key().SeqNum())

			require.True(t, it.Next())
			require.True(t, it.Valid())
			require.EqualValues(t, "a", it.Key().UserKey)
			require.EqualValues(t, 1, it.Key().SeqNum())

			key = makeIkey("b")
			key.SetSeqNum(2)
			add(key, nil)
			key.SetSeqNum(1)
			add(key, nil)

			require.True(t, it.SeekGE(makeKey("b")))
			require.True(t, it.Valid())
			require.EqualValues(t, "b", it.Key().UserKey)
			require.EqualValues(t, 2, it.Key().SeqNum())

			require.True(t, it.Next())
			require.True(t, it.Valid())
			require.EqualValues(t, "b", it.Key().UserKey)
			require.EqualValues(t, 1, it.Key().SeqNum())
		})
	}
}

// TestConcurrentBasic tests concurrent writes followed by concurrent reads.
func TestConcurrentBasic(t *testing.T) {
	const n = 1000

	for _, inserter := range []bool{false, true} {
		t.Run(fmt.Sprintf("inserter=%t", inserter), func(t *testing.T) {
			// Set testing flag to make it easier to trigger unusual race conditions.
			l := NewSkiplist(NewArena(arenaSize, 0), bytes.Compare)
			l.testing = true

			var wg sync.WaitGroup
			for i := 0; i < n; i++ {
				wg.Add(1)
				go func(i int) {
					defer wg.Done()

					if inserter {
						var ins Inserter
						ins.Add(l, makeIntKey(i), makeValue(i))
					} else {
						l.Add(makeIntKey(i), makeValue(i))
					}
				}(i)
			}
			wg.Wait()

			// Check values. Concurrent reads.
			for i := 0; i < n; i++ {
				wg.Add(1)
				go func(i int) {
					defer wg.Done()

					it := iterAdapter{l.NewIter(nil, nil)}
					require.True(t, it.SeekGE(makeKey(fmt.Sprintf("%05d", i))))
					require.EqualValues(t, fmt.Sprintf("%05d", i), it.Key().UserKey)
				}(i)
			}
			wg.Wait()
			require.Equal(t, n, length(l))
			require.Equal(t, n, lengthRev(l))
		})
	}
}

// TestConcurrentOneKey will read while writing to one single key.
func TestConcurrentOneKey(t *testing.T) {
	const n = 100
	key := makeKey("thekey")
	ikey := makeIkey("thekey")

	for _, inserter := range []bool{false, true} {
		t.Run(fmt.Sprintf("inserter=%t", inserter), func(t *testing.T) {
			// Set testing flag to make it easier to trigger unusual race conditions.
			l := NewSkiplist(NewArena(arenaSize, 0), bytes.Compare)
			l.testing = true

			var wg sync.WaitGroup
			writeDone := make(chan struct{}, 1)
			for i := 0; i < n; i++ {
				wg.Add(1)
				go func(i int) {
					defer func() {
						wg.Done()
						select {
						case writeDone <- struct{}{}:
						default:
						}
					}()

					if inserter {
						var ins Inserter
						ins.Add(l, ikey, makeValue(i))
					} else {
						l.Add(ikey, makeValue(i))
					}
				}(i)
			}
			// Wait until at least some write made it such that reads return a value.
			<-writeDone
			var sawValue int32
			for i := 0; i < n; i++ {
				wg.Add(1)
				go func() {
					defer wg.Done()

					it := iterAdapter{l.NewIter(nil, nil)}
					it.SeekGE(key)
					require.True(t, it.Valid())
					require.True(t, bytes.Equal(key, it.Key().UserKey))

					atomic.AddInt32(&sawValue, 1)
					v, err := strconv.Atoi(string(it.Value()[1:]))
					require.NoError(t, err)
					require.True(t, 0 <= v && v < n)
				}()
			}
			wg.Wait()
			require.Equal(t, int32(n), sawValue)
			require.Equal(t, 1, length(l))
			require.Equal(t, 1, lengthRev(l))
		})
	}
}

func TestSkiplistAdd(t *testing.T) {
	for _, inserter := range []bool{false, true} {
		t.Run(fmt.Sprintf("inserter=%t", inserter), func(t *testing.T) {
			l := NewSkiplist(NewArena(arenaSize, 0), bytes.Compare)
			it := iterAdapter{l.NewIter(nil, nil)}

			add := l.Add
			if inserter {
				add = makeInserterAdd(l)
			}

			// Add nil key and value (treated same as empty).
			err := add(base.InternalKey{}, nil)
			require.Nil(t, err)
			require.True(t, it.SeekGE([]byte{}))
			require.EqualValues(t, []byte{}, it.Key().UserKey)
			require.EqualValues(t, []byte{}, it.Value())

			l = NewSkiplist(NewArena(arenaSize, 0), bytes.Compare)
			it = iterAdapter{l.NewIter(nil, nil)}

			add = l.Add
			if inserter {
				add = makeInserterAdd(l)
			}

			// Add empty key and value (treated same as nil).
			err = add(makeIkey(""), []byte{})
			require.Nil(t, err)
			require.True(t, it.SeekGE([]byte{}))
			require.EqualValues(t, []byte{}, it.Key().UserKey)
			require.EqualValues(t, []byte{}, it.Value())

			// Add to empty list.
			err = add(makeIntKey(2), makeValue(2))
			require.Nil(t, err)
			require.True(t, it.SeekGE(makeKey("00002")))
			require.EqualValues(t, "00002", it.Key().UserKey)
			require.EqualValues(t, makeValue(2), it.Value())

			// Add first element in non-empty list.
			err = add(makeIntKey(1), makeValue(1))
			require.Nil(t, err)
			require.True(t, it.SeekGE(makeKey("00001")))
			require.EqualValues(t, "00001", it.Key().UserKey)
			require.EqualValues(t, makeValue(1), it.Value())

			// Add last element in non-empty list.
			err = add(makeIntKey(4), makeValue(4))
			require.Nil(t, err)
			require.True(t, it.SeekGE(makeKey("00004")))
			require.EqualValues(t, "00004", it.Key().UserKey)
			require.EqualValues(t, makeValue(4), it.Value())

			// Add element in middle of list.
			err = add(makeIntKey(3), makeValue(3))
			require.Nil(t, err)
			require.True(t, it.SeekGE(makeKey("00003")))
			require.EqualValues(t, "00003", it.Key().UserKey)
			require.EqualValues(t, makeValue(3), it.Value())

			// Try to add element that already exists.
			err = add(makeIntKey(2), nil)
			require.Equal(t, ErrRecordExists, err)
			require.EqualValues(t, "00003", it.Key().UserKey)
			require.EqualValues(t, makeValue(3), it.Value())

			require.Equal(t, 5, length(l))
			require.Equal(t, 5, lengthRev(l))
		})
	}
}

// TestConcurrentAdd races between adding same nodes.
func TestConcurrentAdd(t *testing.T) {
	for _, inserter := range []bool{false, true} {
		t.Run(fmt.Sprintf("inserter=%t", inserter), func(t *testing.T) {
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
					it := iterAdapter{l.NewIter(nil, nil)}
					add := l.Add
					if inserter {
						add = makeInserterAdd(l)
					}

					for i := 0; i < n; i++ {
						start[i].Wait()

						key := makeIntKey(i)
						if add(key, nil) == nil {
							require.True(t, it.SeekGE(key.UserKey))
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
		})
	}
}

// TestIteratorNext tests a basic iteration over all nodes from the beginning.
func TestIteratorNext(t *testing.T) {
	const n = 100
	l := NewSkiplist(NewArena(arenaSize, 0), bytes.Compare)
	it := iterAdapter{l.NewIter(nil, nil)}

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
	it := iterAdapter{l.NewIter(nil, nil)}

	require.False(t, it.Valid())

	it.Last()
	require.False(t, it.Valid())

	var ins Inserter
	for i := 0; i < n; i++ {
		ins.Add(l, makeIntKey(i), makeValue(i))
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
	it := iterAdapter{l.NewIter(nil, nil)}

	require.False(t, it.Valid())
	it.First()
	require.False(t, it.Valid())
	// 1000, 1010, 1020, ..., 1990.

	var ins Inserter
	for i := n - 1; i >= 0; i-- {
		v := i*10 + 1000
		ins.Add(l, makeIntKey(v), makeValue(v))
	}

	require.True(t, it.SeekGE(makeKey("")))
	require.True(t, it.Valid())
	require.EqualValues(t, "01000", it.Key().UserKey)
	require.EqualValues(t, "v01000", it.Value())

	require.True(t, it.SeekGE(makeKey("01000")))
	require.True(t, it.Valid())
	require.EqualValues(t, "01000", it.Key().UserKey)
	require.EqualValues(t, "v01000", it.Value())

	require.True(t, it.SeekGE(makeKey("01005")))
	require.True(t, it.Valid())
	require.EqualValues(t, "01010", it.Key().UserKey)
	require.EqualValues(t, "v01010", it.Value())

	require.True(t, it.SeekGE(makeKey("01010")))
	require.True(t, it.Valid())
	require.EqualValues(t, "01010", it.Key().UserKey)
	require.EqualValues(t, "v01010", it.Value())

	require.False(t, it.SeekGE(makeKey("99999")))
	require.False(t, it.Valid())

	// Test seek for empty key.
	ins.Add(l, base.InternalKey{}, nil)
	require.True(t, it.SeekGE([]byte{}))
	require.True(t, it.Valid())
	require.EqualValues(t, "", it.Key().UserKey)

	require.True(t, it.SeekGE(makeKey("")))
	require.True(t, it.Valid())
	require.EqualValues(t, "", it.Key().UserKey)
}

func TestIteratorSeekLT(t *testing.T) {
	const n = 100
	l := NewSkiplist(NewArena(arenaSize, 0), bytes.Compare)
	it := iterAdapter{l.NewIter(nil, nil)}

	require.False(t, it.Valid())
	it.First()
	require.False(t, it.Valid())
	// 1000, 1010, 1020, ..., 1990.
	var ins Inserter
	for i := n - 1; i >= 0; i-- {
		v := i*10 + 1000
		ins.Add(l, makeIntKey(v), makeValue(v))
	}

	require.False(t, it.SeekLT(makeKey("")))
	require.False(t, it.Valid())

	require.False(t, it.SeekLT(makeKey("01000")))
	require.False(t, it.Valid())

	require.True(t, it.SeekLT(makeKey("01001")))
	require.True(t, it.Valid())
	require.EqualValues(t, "01000", it.Key().UserKey)
	require.EqualValues(t, "v01000", it.Value())

	require.True(t, it.SeekLT(makeKey("01005")))
	require.True(t, it.Valid())
	require.EqualValues(t, "01000", it.Key().UserKey)
	require.EqualValues(t, "v01000", it.Value())

	require.True(t, it.SeekLT(makeKey("01991")))
	require.True(t, it.Valid())
	require.EqualValues(t, "01990", it.Key().UserKey)
	require.EqualValues(t, "v01990", it.Value())

	require.True(t, it.SeekLT(makeKey("99999")))
	require.True(t, it.Valid())
	require.EqualValues(t, "01990", it.Key().UserKey)
	require.EqualValues(t, "v01990", it.Value())

	// Test seek for empty key.
	ins.Add(l, base.InternalKey{}, nil)
	require.False(t, it.SeekLT([]byte{}))
	require.False(t, it.Valid())

	require.True(t, it.SeekLT(makeKey("\x01")))
	require.True(t, it.Valid())
	require.EqualValues(t, "", it.Key().UserKey)
}

// TODO(peter): test First and Last.
func TestIteratorBounds(t *testing.T) {
	l := NewSkiplist(NewArena(arenaSize, 0), bytes.Compare)
	for i := 1; i < 10; i++ {
		err := l.Add(makeIntKey(i), makeValue(i))
		if err != nil {
			t.Fatal(err)
		}
	}

	key := func(i int) []byte {
		return makeIntKey(i).UserKey
	}

	it := iterAdapter{l.NewIter(key(3), key(7))}

	// SeekGE within the lower and upper bound succeeds.
	for i := 3; i <= 6; i++ {
		k := key(i)
		require.True(t, it.SeekGE(k))
		require.EqualValues(t, string(k), string(it.Key().UserKey))
	}

	// SeekGE before the lower bound still succeeds (only the upper bound is
	// checked).
	for i := 1; i < 3; i++ {
		k := key(i)
		require.True(t, it.SeekGE(k))
		require.EqualValues(t, string(k), string(it.Key().UserKey))
	}

	// SeekGE beyond the upper bound fails.
	for i := 7; i < 10; i++ {
		require.False(t, it.SeekGE(key(i)))
	}

	require.True(t, it.SeekGE(key(5)))
	require.EqualValues(t, "00005", it.Key().UserKey)
	require.EqualValues(t, "v00005", it.Value())

	require.True(t, it.Next())
	// Next into the upper bound fails.
	require.False(t, it.Next())

	// SeekLT within the lower and upper bound succeeds.
	for i := 4; i <= 7; i++ {
		require.True(t, it.SeekLT(key(i)))
		require.EqualValues(t, string(key(i-1)), string(it.Key().UserKey))
	}

	// SeekLT beyond the upper bound still succeeds (only the lower bound is
	// checked).
	for i := 8; i < 9; i++ {
		require.True(t, it.SeekLT(key(8)))
		require.EqualValues(t, string(key(i-1)), string(it.Key().UserKey))
	}

	// SeekLT before the lower bound fails.
	for i := 1; i < 4; i++ {
		require.False(t, it.SeekLT(key(i)))
	}

	require.True(t, it.SeekLT(key(5)))
	require.EqualValues(t, "00004", it.Key().UserKey)
	require.EqualValues(t, "v00004", it.Value())

	require.True(t, it.Prev())
	// Prev into the lower bound fails.
	require.False(t, it.Prev())
}

func randomKey(rng *rand.Rand, b []byte) base.InternalKey {
	key := rng.Uint32()
	key2 := rng.Uint32()
	binary.LittleEndian.PutUint32(b, key)
	binary.LittleEndian.PutUint32(b[4:], key2)
	return base.InternalKey{UserKey: b}
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
				it := l.NewIter(nil, nil)
				rng := rand.New(rand.NewSource(uint64(time.Now().UnixNano())))
				buf := make([]byte, 8)

				for pb.Next() {
					if rng.Float32() < readFrac {
						key, _ := it.SeekGE(randomKey(rng, buf).UserKey)
						if key != nil {
							_ = key
							count++
						}
					} else {
						_ = l.Add(randomKey(rng, buf), nil)
					}
				}
			})
		})
	}
}

func BenchmarkOrderedWrite(b *testing.B) {
	l := NewSkiplist(NewArena(8<<20, 0), bytes.Compare)
	var ins Inserter
	buf := make([]byte, 8)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		binary.BigEndian.PutUint64(buf, uint64(i))
		if err := ins.Add(l, base.InternalKey{UserKey: buf}, nil); err == ErrArenaFull {
			b.StopTimer()
			l = NewSkiplist(NewArena(uint32((b.N+2)*maxNodeSize), 0), bytes.Compare)
			ins = Inserter{}
			b.StartTimer()
		}
	}
}

func BenchmarkIterNext(b *testing.B) {
	l := NewSkiplist(NewArena(64<<10, 0), bytes.Compare)
	rng := rand.New(rand.NewSource(uint64(time.Now().UnixNano())))
	buf := make([]byte, 8)
	for {
		if err := l.Add(randomKey(rng, buf), nil); err == ErrArenaFull {
			break
		}
	}

	it := l.NewIter(nil, nil)
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
	rng := rand.New(rand.NewSource(uint64(time.Now().UnixNano())))
	buf := make([]byte, 8)
	for {
		if err := l.Add(randomKey(rng, buf), nil); err == ErrArenaFull {
			break
		}
	}

	it := l.NewIter(nil, nil)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if !it.Valid() {
			it.Last()
		}
		it.Prev()
	}
}

func benchmarkIterBoundNext(keyCount int, b *testing.B) {
	l := NewSkiplist(NewArena(64<<10, 0), bytes.Compare)

	upper := make([]byte, 8)
	binary.LittleEndian.PutUint32(upper, math.MaxUint32)
	binary.LittleEndian.PutUint32(upper[4:], math.MaxUint32)
	l.Add(base.InternalKey{UserKey: upper}, nil)

	rng := rand.New(rand.NewSource(uint64(time.Now().UnixNano())))
	buf := make([]byte, 8)
	for i := 0; i < keyCount; i++ {
		if err := l.Add(randomKey(rng, buf), nil); err == ErrArenaFull {
			panic("Arena not large enough to hold all the keys")
		}
	}

	it := l.NewIter(nil, upper)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if !it.Valid() {
			it.SetBounds(nil, upper)
			it.First()
		}
		it.Next()
	}
}

func BenchmarkIterBoundNext1(b *testing.B) {
	benchmarkIterBoundNext(1, b)
}

func BenchmarkIterBoundNext10(b *testing.B) {
	benchmarkIterBoundNext(10, b)
}

func BenchmarkIterBoundNext15(b *testing.B) {
	benchmarkIterBoundNext(15, b)
}

func BenchmarkIterBoundNext30(b *testing.B) {
	benchmarkIterBoundNext(30, b)
}

func BenchmarkIterBoundNext100(b *testing.B) {
	benchmarkIterBoundNext(100, b)
}

func BenchmarkIterBoundNext1000(b *testing.B) {
	benchmarkIterBoundNext(1000, b)
}

func benchmarkIterBoundPrev(keyCount int, b *testing.B) {
	l := NewSkiplist(NewArena(64<<10, 0), bytes.Compare)

	// This is needed because there must be a key less than lower, otherwise
	// lower is set to nil since it is deemed unneeded.
	lessThanLower := make([]byte, 8)
	binary.LittleEndian.PutUint32(lessThanLower, 0)
	binary.LittleEndian.PutUint32(lessThanLower[4:], 0)
	l.Add(base.InternalKey{UserKey: lessThanLower}, nil)

	rng := rand.New(rand.NewSource(uint64(time.Now().UnixNano())))
	buf := make([]byte, 8)
	for i := 0; i < keyCount; i++ {
		if err := l.Add(randomKey(rng, buf), nil); err == ErrArenaFull {
			panic("Arena not large enough to hold all the keys")
		}
	}

	lower := make([]byte, 8)
	binary.LittleEndian.PutUint32(lower, 1)
	binary.LittleEndian.PutUint32(lower[4:], 1)

	it := l.NewIter(lower, nil)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if !it.Valid() {
			it.SetBounds(lower, nil)
			it.Last()
		}
		it.Prev()
	}
}

func BenchmarkIterBoundPrev1(b *testing.B) {
	benchmarkIterBoundPrev(1, b)
}

func BenchmarkIterBoundPrev10(b *testing.B) {
	benchmarkIterBoundPrev(10, b)
}

func BenchmarkIterBoundPrev15(b *testing.B) {
	benchmarkIterBoundPrev(15, b)
}

func BenchmarkIterBoundPrev30(b *testing.B) {
	benchmarkIterBoundPrev(30, b)
}

func BenchmarkIterBoundPrev100(b *testing.B) {
	benchmarkIterBoundPrev(100, b)
}

func BenchmarkIterBoundPrev1000(b *testing.B) {
	benchmarkIterBoundPrev(1000, b)
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
