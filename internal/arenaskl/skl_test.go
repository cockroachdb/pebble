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
	"math/rand/v2"
	"reflect"
	"slices"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/cockroachdb/pebble/internal/base"
	"github.com/cockroachdb/pebble/internal/testkeys"
	"github.com/cockroachdb/pebble/internal/testutils"
	"github.com/stretchr/testify/require"
)

const arenaSize = 1 << 20

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
	it := s.NewIter(nil, nil)
	for kv := it.First(); kv != nil; kv = it.Next() {
		count++
	}
	return count
}

// length iterates over skiplist in reverse order to give exact size.
func lengthRev(s *Skiplist) int {
	count := 0
	it := s.NewIter(nil, nil)
	for kv := it.Last(); kv != nil; kv = it.Prev() {
		count++
	}
	return count
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
	l := NewSkiplist(newArena(arenaSize), bytes.Compare)
	it := l.NewIter(nil, nil)
	require.Nil(t, it.First())
	require.Nil(t, it.Last())
	require.Nil(t, it.SeekGE(key, base.SeekGEFlagsNone))
}

func TestFull(t *testing.T) {
	l := NewSkiplist(newArena(1000), bytes.Compare)

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

func mustGetValue(t *testing.T, lv base.InternalValue) []byte {
	v, _, err := lv.Value(nil)
	require.NoError(t, err)
	return v
}

// TestBasic tests single-threaded seeks and adds.
func TestBasic(t *testing.T) {
	for _, inserter := range []bool{false, true} {
		t.Run(fmt.Sprintf("inserter=%t", inserter), func(t *testing.T) {
			l := NewSkiplist(newArena(arenaSize), bytes.Compare)
			it := l.NewIter(nil, nil)

			add := l.Add
			if inserter {
				add = makeInserterAdd(l)
			}

			// Try adding values.
			add(makeIkey("key1"), makeValue(1))
			add(makeIkey("key3"), makeValue(3))
			add(makeIkey("key2"), makeValue(2))

			kv := it.SeekGE(makeKey("key"), base.SeekGEFlagsNone)
			require.NotNil(t, kv)
			require.NotEqual(t, "key", kv.K.UserKey)

			kv = it.SeekGE(makeKey("key1"), base.SeekGEFlagsNone)
			require.NotNil(t, kv)
			require.EqualValues(t, "key1", kv.K.UserKey)
			require.EqualValues(t, makeValue(1), mustGetValue(t, kv.V))

			kv = it.SeekGE(makeKey("key2"), base.SeekGEFlagsNone)
			require.NotNil(t, kv)
			require.EqualValues(t, "key2", kv.K.UserKey)
			require.EqualValues(t, makeValue(2), mustGetValue(t, kv.V))

			kv = it.SeekGE(makeKey("key3"), base.SeekGEFlagsNone)
			require.NotNil(t, kv)
			require.EqualValues(t, "key3", kv.K.UserKey)
			require.EqualValues(t, makeValue(3), mustGetValue(t, kv.V))

			key := makeIkey("a")
			key.SetSeqNum(1)
			add(key, nil)
			key.SetSeqNum(2)
			add(key, nil)

			kv = it.SeekGE(makeKey("a"), base.SeekGEFlagsNone)
			require.NotNil(t, kv)
			require.EqualValues(t, "a", kv.K.UserKey)
			require.EqualValues(t, 2, kv.K.SeqNum())

			kv = it.Next()
			require.NotNil(t, kv)
			require.EqualValues(t, "a", kv.K.UserKey)
			require.EqualValues(t, 1, kv.K.SeqNum())

			key = makeIkey("b")
			key.SetSeqNum(2)
			add(key, nil)
			key.SetSeqNum(1)
			add(key, nil)

			kv = it.SeekGE(makeKey("b"), base.SeekGEFlagsNone)
			require.NotNil(t, kv)
			require.EqualValues(t, "b", kv.K.UserKey)
			require.EqualValues(t, 2, kv.K.SeqNum())

			kv = it.Next()
			require.NotNil(t, kv)
			require.EqualValues(t, "b", kv.K.UserKey)
			require.EqualValues(t, 1, kv.K.SeqNum())
		})
	}
}

// TestConcurrentBasic tests concurrent writes followed by concurrent reads.
func TestConcurrentBasic(t *testing.T) {
	const n = 1000

	for _, inserter := range []bool{false, true} {
		t.Run(fmt.Sprintf("inserter=%t", inserter), func(t *testing.T) {
			// Set testing flag to make it easier to trigger unusual race conditions.
			l := NewSkiplist(newArena(arenaSize), bytes.Compare)
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

					it := l.NewIter(nil, nil)
					kv := it.SeekGE(makeKey(fmt.Sprintf("%05d", i)), base.SeekGEFlagsNone)
					require.NotNil(t, kv)
					require.EqualValues(t, fmt.Sprintf("%05d", i), kv.K.UserKey)
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
			l := NewSkiplist(newArena(arenaSize), bytes.Compare)
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
			var sawValue atomic.Int32
			for i := 0; i < n; i++ {
				wg.Add(1)
				go func() {
					defer wg.Done()

					it := l.NewIter(nil, nil)
					kv := it.SeekGE(key, base.SeekGEFlagsNone)
					require.NotNil(t, kv)
					require.True(t, bytes.Equal(key, kv.K.UserKey))

					sawValue.Add(1)
					v, err := strconv.Atoi(string(mustGetValue(t, kv.V)[1:]))
					require.NoError(t, err)
					require.True(t, 0 <= v && v < n)
				}()
			}
			wg.Wait()
			require.Equal(t, int32(n), sawValue.Load())
			require.Equal(t, 1, length(l))
			require.Equal(t, 1, lengthRev(l))
		})
	}
}

func TestSkiplistAdd(t *testing.T) {
	for _, inserter := range []bool{false, true} {
		t.Run(fmt.Sprintf("inserter=%t", inserter), func(t *testing.T) {
			l := NewSkiplist(newArena(arenaSize), bytes.Compare)
			it := l.NewIter(nil, nil)

			add := l.Add
			if inserter {
				add = makeInserterAdd(l)
			}

			// Add nil key and value (treated same as empty).
			err := add(base.InternalKey{}, nil)
			require.Nil(t, err)
			kv := it.SeekGE([]byte{}, base.SeekGEFlagsNone)
			require.NotNil(t, kv)
			require.EqualValues(t, []byte{}, kv.K.UserKey)
			require.EqualValues(t, []byte{}, mustGetValue(t, kv.V))

			l = NewSkiplist(newArena(arenaSize), bytes.Compare)
			it = l.NewIter(nil, nil)

			add = l.Add
			if inserter {
				add = makeInserterAdd(l)
			}

			// Add empty key and value (treated same as nil).
			err = add(makeIkey(""), []byte{})
			require.Nil(t, err)
			kv = it.SeekGE([]byte{}, base.SeekGEFlagsNone)
			require.NotNil(t, kv)
			require.EqualValues(t, []byte{}, kv.K.UserKey)
			require.EqualValues(t, []byte{}, mustGetValue(t, kv.V))

			// Add to empty list.
			err = add(makeIntKey(2), makeValue(2))
			require.Nil(t, err)
			kv = it.SeekGE(makeKey("00002"), base.SeekGEFlagsNone)
			require.NotNil(t, kv)
			require.EqualValues(t, "00002", kv.K.UserKey)
			require.EqualValues(t, makeValue(2), mustGetValue(t, kv.V))

			// Add first element in non-empty list.
			err = add(makeIntKey(1), makeValue(1))
			require.Nil(t, err)
			kv = it.SeekGE(makeKey("00001"), base.SeekGEFlagsNone)
			require.NotNil(t, kv)
			require.EqualValues(t, "00001", kv.K.UserKey)
			require.EqualValues(t, makeValue(1), mustGetValue(t, kv.V))

			// Add last element in non-empty list.
			err = add(makeIntKey(4), makeValue(4))
			require.Nil(t, err)
			kv = it.SeekGE(makeKey("00004"), base.SeekGEFlagsNone)
			require.NotNil(t, kv)
			require.EqualValues(t, "00004", kv.K.UserKey)
			require.EqualValues(t, makeValue(4), mustGetValue(t, kv.V))

			// Add element in middle of list.
			err = add(makeIntKey(3), makeValue(3))
			require.Nil(t, err)
			kv = it.SeekGE(makeKey("00003"), base.SeekGEFlagsNone)
			require.NotNil(t, kv)
			require.EqualValues(t, "00003", kv.K.UserKey)
			require.EqualValues(t, makeValue(3), mustGetValue(t, kv.V))

			// Try to add element that already exists.
			err = add(makeIntKey(2), nil)
			require.Equal(t, ErrRecordExists, err)

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
			l := NewSkiplist(newArena(arenaSize), bytes.Compare)
			l.testing = true

			start := make([]sync.WaitGroup, n)
			end := make([]sync.WaitGroup, n)

			for i := 0; i < n; i++ {
				start[i].Add(1)
				end[i].Add(2)
			}

			for f := 0; f < 2; f++ {
				go func(f int) {
					it := l.NewIter(nil, nil)
					add := l.Add
					if inserter {
						add = makeInserterAdd(l)
					}

					for i := 0; i < n; i++ {
						start[i].Wait()

						key := makeIntKey(i)
						if add(key, nil) == nil {
							kv := it.SeekGE(key.UserKey, base.SeekGEFlagsNone)
							require.NotNil(t, kv)
							require.EqualValues(t, key, kv.K)
						}

						end[i].Done()
					}
				}(f)
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
	l := NewSkiplist(newArena(arenaSize), bytes.Compare)
	it := l.NewIter(nil, nil)

	require.Nil(t, it.First())
	for i := n - 1; i >= 0; i-- {
		l.Add(makeIntKey(i), makeValue(i))
	}

	kv := it.First()
	for i := 0; i < n; i++ {
		require.NotNil(t, kv)
		require.EqualValues(t, makeIntKey(i), kv.K)
		require.EqualValues(t, makeValue(i), mustGetValue(t, kv.V))
		kv = it.Next()
	}
	require.Nil(t, kv)
}

// TestIteratorPrev tests a basic iteration over all nodes from the end.
func TestIteratorPrev(t *testing.T) {
	const n = 100
	l := NewSkiplist(newArena(arenaSize), bytes.Compare)
	it := l.NewIter(nil, nil)

	require.Nil(t, it.Last())
	var ins Inserter
	for i := 0; i < n; i++ {
		ins.Add(l, makeIntKey(i), makeValue(i))
	}

	kv := it.Last()
	for i := n - 1; i >= 0; i-- {
		require.NotNil(t, kv)
		require.EqualValues(t, makeIntKey(i), kv.K)
		require.EqualValues(t, makeValue(i), mustGetValue(t, kv.V))
		kv = it.Prev()
	}
	require.Nil(t, kv)
}

func mustSeekGEKV(t *testing.T, it *Iterator, seekKey []byte, flags base.SeekGEFlags, k, v string) {
	kv := it.SeekGE(seekKey, flags)
	require.NotNil(t, kv)
	require.EqualValues(t, k, string(kv.K.UserKey))
	gotV := mustGetValue(t, kv.V)
	require.EqualValues(t, v, string(gotV))
}

func mustSeekPrefixGEKV(
	t *testing.T, it *Iterator, seekKey []byte, flags base.SeekGEFlags, k, v string,
) {
	kv := it.SeekPrefixGE(seekKey, seekKey, flags)
	require.NotNil(t, kv)
	require.EqualValues(t, k, string(kv.K.UserKey))
	gotV := mustGetValue(t, kv.V)
	require.EqualValues(t, v, string(gotV))
}

func TestIteratorSeekGEAndSeekPrefixGE(t *testing.T) {
	const n = 100
	l := NewSkiplist(newArena(arenaSize), bytes.Compare)
	it := l.NewIter(nil, nil)

	require.Nil(t, it.First())
	// 1000, 1010, 1020, ..., 1990.

	var ins Inserter
	for i := n - 1; i >= 0; i-- {
		v := i*10 + 1000
		ins.Add(l, makeIntKey(v), makeValue(v))
	}

	mustSeekGEKV(t, it, makeKey(""), base.SeekGEFlagsNone, "01000", "v01000")
	mustSeekGEKV(t, it, makeKey("01000"), base.SeekGEFlagsNone, "01000", "v01000")
	mustSeekGEKV(t, it, makeKey("01005"), base.SeekGEFlagsNone, "01010", "v01010")
	mustSeekGEKV(t, it, makeKey("01010"), base.SeekGEFlagsNone, "01010", "v01010")
	require.Nil(t, it.SeekGE(makeKey("99999"), base.SeekGEFlagsNone))

	// Test SeekGE with trySeekUsingNext optimization.
	{
		mustSeekGEKV(t, it, makeKey("01000"), base.SeekGEFlagsNone, "01000", "v01000")

		// Seeking to the same key.
		mustSeekGEKV(t, it, makeKey("01000"), base.SeekGEFlagsNone.EnableTrySeekUsingNext(), "01000", "v01000")

		// Seeking to a nearby key that can be reached using Next.
		mustSeekGEKV(t, it, makeKey("01020"), base.SeekGEFlagsNone.EnableTrySeekUsingNext(), "01020", "v01020")

		// Seeking to a key that cannot be reached using Next.
		mustSeekGEKV(t, it, makeKey("01200"), base.SeekGEFlagsNone.EnableTrySeekUsingNext(), "01200", "v01200")

		// Seeking to an earlier key, but the caller lies. Incorrect result.
		mustSeekGEKV(t, it, makeKey("01100"), base.SeekGEFlagsNone.EnableTrySeekUsingNext(), "01200", "v01200")

		// Telling the truth works.
		mustSeekGEKV(t, it, makeKey("01100"), base.SeekGEFlagsNone, "01100", "v01100")
	}

	// Test SeekPrefixGE with trySeekUsingNext optimization.
	{
		mustSeekPrefixGEKV(t, it, makeKey("01000"), base.SeekGEFlagsNone, "01000", "v01000")

		// Seeking to the same key.
		mustSeekPrefixGEKV(t, it, makeKey("01000"), base.SeekGEFlagsNone.EnableTrySeekUsingNext(), "01000", "v01000")

		// Seeking to a nearby key that can be reached using Next.
		mustSeekPrefixGEKV(t, it, makeKey("01020"), base.SeekGEFlagsNone.EnableTrySeekUsingNext(), "01020", "v01020")

		// Seeking to a key that cannot be reached using Next.
		mustSeekPrefixGEKV(t, it, makeKey("01200"), base.SeekGEFlagsNone.EnableTrySeekUsingNext(), "01200", "v01200")

		// Seeking to an earlier key, but the caller lies. Incorrect result.
		mustSeekPrefixGEKV(t, it, makeKey("01100"), base.SeekGEFlagsNone.EnableTrySeekUsingNext(), "01200", "v01200")

		// Telling the truth works.
		mustSeekPrefixGEKV(t, it, makeKey("01100"), base.SeekGEFlagsNone, "01100", "v01100")
	}

	// Test seek for empty key.
	ins.Add(l, base.InternalKey{}, nil)
	mustSeekGEKV(t, it, makeKey(""), base.SeekGEFlagsNone, "", "")
}

func TestIteratorSeekLT(t *testing.T) {
	const n = 100
	l := NewSkiplist(newArena(arenaSize), bytes.Compare)
	it := l.NewIter(nil, nil)

	require.Nil(t, it.First())
	// 1000, 1010, 1020, ..., 1990.
	var ins Inserter
	for i := n - 1; i >= 0; i-- {
		v := i*10 + 1000
		ins.Add(l, makeIntKey(v), makeValue(v))
	}

	require.Nil(t, it.SeekLT(makeKey(""), base.SeekLTFlagsNone))
	require.Nil(t, it.SeekLT(makeKey("01000"), base.SeekLTFlagsNone))

	kv := it.SeekLT(makeKey("01001"), base.SeekLTFlagsNone)
	require.NotNil(t, kv)
	require.EqualValues(t, "01000", kv.K.UserKey)
	require.EqualValues(t, "v01000", mustGetValue(t, kv.V))

	kv = it.SeekLT(makeKey("01005"), base.SeekLTFlagsNone)
	require.NotNil(t, kv)
	require.EqualValues(t, "01000", kv.K.UserKey)
	require.EqualValues(t, "v01000", mustGetValue(t, kv.V))

	kv = it.SeekLT(makeKey("01991"), base.SeekLTFlagsNone)
	require.NotNil(t, kv)
	require.EqualValues(t, "01990", kv.K.UserKey)
	require.EqualValues(t, "v01990", mustGetValue(t, kv.V))

	kv = it.SeekLT(makeKey("99999"), base.SeekLTFlagsNone)
	require.NotNil(t, kv)
	require.EqualValues(t, "01990", kv.K.UserKey)
	require.EqualValues(t, "v01990", mustGetValue(t, kv.V))

	// Test seek for empty key.
	ins.Add(l, base.InternalKey{}, nil)
	require.Nil(t, it.SeekLT([]byte{}, base.SeekLTFlagsNone))

	kv = it.SeekLT(makeKey("\x01"), base.SeekLTFlagsNone)
	require.NotNil(t, kv)
	require.EqualValues(t, "", kv.K.UserKey)
}

// TODO(peter): test First and Last.
func TestIteratorBounds(t *testing.T) {
	l := NewSkiplist(newArena(arenaSize), bytes.Compare)
	for i := 1; i < 10; i++ {
		require.NoError(t, l.Add(makeIntKey(i), makeValue(i)))
	}

	key := func(i int) []byte {
		return makeIntKey(i).UserKey
	}

	it := l.NewIter(key(3), key(7))

	// SeekGE within the lower and upper bound succeeds.
	for i := 3; i <= 6; i++ {
		k := key(i)
		kv := it.SeekGE(k, base.SeekGEFlagsNone)
		require.NotNil(t, kv)
		require.EqualValues(t, string(k), string(kv.K.UserKey))
	}

	// SeekGE before the lower bound still succeeds (only the upper bound is
	// checked).
	for i := 1; i < 3; i++ {
		k := key(i)
		kv := it.SeekGE(k, base.SeekGEFlagsNone)
		require.NotNil(t, kv)
		require.EqualValues(t, string(k), string(kv.K.UserKey))
	}

	// SeekGE beyond the upper bound fails.
	for i := 7; i < 10; i++ {
		require.Nil(t, it.SeekGE(key(i), base.SeekGEFlagsNone))
	}

	mustSeekGEKV(t, it, key(6), base.SeekGEFlagsNone, "00006", "v00006")

	// Next into the upper bound fails.
	require.Nil(t, it.Next())

	// SeekLT within the lower and upper bound succeeds.
	for i := 4; i <= 7; i++ {
		kv := it.SeekLT(key(i), base.SeekLTFlagsNone)
		require.NotNil(t, kv)
		require.EqualValues(t, string(key(i-1)), string(kv.K.UserKey))
	}

	// SeekLT beyond the upper bound still succeeds (only the lower bound is
	// checked).
	for i := 8; i < 9; i++ {
		kv := it.SeekLT(key(8), base.SeekLTFlagsNone)
		require.NotNil(t, kv)
		require.EqualValues(t, string(key(i-1)), string(kv.K.UserKey))
	}

	// SeekLT before the lower bound fails.
	for i := 1; i < 4; i++ {
		require.Nil(t, it.SeekLT(key(i), base.SeekLTFlagsNone))
	}

	kv := it.SeekLT(key(4), base.SeekLTFlagsNone)
	require.NotNil(t, kv)
	require.EqualValues(t, "00003", kv.K.UserKey)
	require.EqualValues(t, "v00003", mustGetValue(t, kv.V))

	// Prev into the lower bound fails.
	require.Nil(t, it.Prev())
}

func randomKey(rng *rand.Rand, b []byte) base.InternalKey {
	key := rng.Uint32()
	key2 := rng.Uint32()
	binary.LittleEndian.PutUint32(b, key)
	binary.LittleEndian.PutUint32(b[4:], key2)
	return base.InternalKey{UserKey: b}
}

// randomTestkeysSkiplist creates an arena skiplist of with an arena of the
// given size. It fills the arena with random keys and values. It returns the
// skiplist with n keys and a slice of 2n+1 sorted keys, alternating between
// keys not in the skiplist and keys in the skiplist. The even-indexed keys can
// be used as seek keys to test searches that do not find exact key matches.
func randomTestkeysSkiplist(rng *rand.Rand, size int) (*Skiplist, [][]byte) {
	ks := testkeys.Alpha(5)
	keys := make([][]byte, 2*size+1)
	for i := range keys {
		keys[i] = testkeys.Key(ks, int64(i))
	}
	slices.SortFunc(keys, testkeys.Comparer.Compare)

	l := NewSkiplist(newArena(uint32(size)), testkeys.Comparer.Compare)
	var n int
	for n = 1; n < len(keys); n += 2 {
		value := testutils.RandBytes(rng, rng.IntN(90)+10)
		k := base.MakeInternalKey(keys[n], base.SeqNum(n), base.InternalKeyKindSet)
		err := l.Add(k, value)
		if err == ErrArenaFull {
			break
		} else if err != nil {
			panic(err)
		}
	}
	return l, keys[:n]
}

func TestSkiplistFindSplice(t *testing.T) {
	seed := int64(time.Now().UnixNano())
	t.Logf("seed: %d", seed)
	rng := rand.New(rand.NewPCG(0, uint64(seed)))
	skl, keys := randomTestkeysSkiplist(rng, 4<<20)

	// Build a slice of all the the nodes.
	var nodes []*node
	for n := skl.head; n != skl.tail; n = skl.getNext(n, 0) {
		nodes = append(nodes, n)
	}
	nodes = append(nodes, skl.tail)

	// Check that the nodes match the odd-indexed keys.
	// NB: We skip the head and tail nodes.
	for i, n := range nodes[1 : len(nodes)-1] {
		require.Equal(t, keys[2*i+1], n.getKeyBytes(skl.arena))
	}

	var ins Inserter
	for i, key := range keys {
		// Only the odd-indexed keys are in the skiplist, so the index of the
		// node we expect to find is i / 2.
		ni := i / 2
		skl.findSplice(base.MakeSearchKey(key), &ins)

		// Check that the prev and next nodes match the expected adjacent nodes.
		if nodes[ni] != ins.spl[0].prev {
			t.Errorf("prev: %s != %s", nodes[ni].getKeyBytes(skl.arena),
				ins.spl[0].prev.getKeyBytes(skl.arena))
		}
		if nodes[ni+1] != ins.spl[0].next {
			t.Errorf("next: %s != %s", nodes[ni+1].getKeyBytes(skl.arena),
				ins.spl[0].next.getKeyBytes(skl.arena))
		}

		// At every height, ensure that the splice brackets the key and is
		// tight.
		for j := 0; j < int(ins.height); j++ {
			spl := &ins.spl[j]
			if prevNext := skl.getNext(spl.prev, j); prevNext != spl.next {
				t.Errorf("level %d; spl = (%q,%q); prevNext = %q", j,
					spl.prev.getKeyBytes(skl.arena), spl.next.getKeyBytes(skl.arena), prevNext.getKeyBytes(skl.arena))
			}
			if nextPrev := skl.getPrev(spl.next, j); nextPrev != spl.prev {
				t.Errorf("level %d; spl = (%q,%q); nextPrev = %q", j,
					spl.prev.getKeyBytes(skl.arena), spl.next.getKeyBytes(skl.arena), nextPrev.getKeyBytes(skl.arena))
			}
			if spl.prev != skl.head && testkeys.Comparer.Compare(spl.prev.getKeyBytes(skl.arena), key) >= 0 {
				t.Errorf("level %d; spl = (%q,%q); prev is not before key %q", j,
					spl.prev.getKeyBytes(skl.arena), spl.next.getKeyBytes(skl.arena), key)
			}
			if spl.next != skl.tail && testkeys.Comparer.Compare(spl.next.getKeyBytes(skl.arena), key) < 0 {
				t.Errorf("level %d; spl = (%q,%q); next is not after key %q", j,
					spl.prev.getKeyBytes(skl.arena), spl.next.getKeyBytes(skl.arena), key)
			}
		}

		// Sometimes reset the Inserter to test a non-cached splice.
		if rng.IntN(2) == 1 {
			ins = Inserter{}
		}
	}
}

// Standard test. Some fraction is read. Some fraction is write. Writes have
// to go through mutex lock.
func BenchmarkReadWrite(b *testing.B) {
	for i := 0; i <= 10; i++ {
		readFrac := float32(i) / 10.0
		b.Run(fmt.Sprintf("frac_%d", i*10), func(b *testing.B) {
			l := NewSkiplist(newArena(uint32((b.N+2)*maxNodeSize)), bytes.Compare)
			b.ResetTimer()
			var count int
			b.RunParallel(func(pb *testing.PB) {
				it := l.NewIter(nil, nil)
				rng := rand.New(rand.NewPCG(0, uint64(time.Now().UnixNano())))
				buf := make([]byte, 8)

				for pb.Next() {
					if rng.Float32() < readFrac {
						kv := it.SeekGE(randomKey(rng, buf).UserKey, base.SeekGEFlagsNone)
						if kv != nil {
							_ = kv
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
	l := NewSkiplist(newArena(8<<20), bytes.Compare)
	var ins Inserter
	buf := make([]byte, 8)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		binary.BigEndian.PutUint64(buf, uint64(i))
		if err := ins.Add(l, base.InternalKey{UserKey: buf}, nil); err == ErrArenaFull {
			b.StopTimer()
			l = NewSkiplist(newArena(uint32((b.N+2)*maxNodeSize)), bytes.Compare)
			ins = Inserter{}
			b.StartTimer()
		}
	}
}

func BenchmarkIterNext(b *testing.B) {
	l := NewSkiplist(newArena(64<<10), bytes.Compare)
	rng := rand.New(rand.NewPCG(0, uint64(time.Now().UnixNano())))
	buf := make([]byte, 8)
	for {
		if err := l.Add(randomKey(rng, buf), nil); err == ErrArenaFull {
			break
		}
	}

	it := l.NewIter(nil, nil)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		kv := it.Next()
		if kv == nil {
			kv = it.First()
		}
		_ = kv
	}
}

func BenchmarkIterPrev(b *testing.B) {
	l := NewSkiplist(newArena(64<<10), bytes.Compare)
	rng := rand.New(rand.NewPCG(0, uint64(time.Now().UnixNano())))
	buf := make([]byte, 8)
	for {
		if err := l.Add(randomKey(rng, buf), nil); err == ErrArenaFull {
			break
		}
	}

	it := l.NewIter(nil, nil)
	_ = it.Last()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		kv := it.Prev()
		if kv == nil {
			kv = it.Last()
		}
		_ = kv
	}
}

// BenchmarkSeekPrefixGE looks at the performance of repeated calls to
// SeekPrefixGE, with different skip distances and different settings of
// trySeekUsingNext.
func BenchmarkSeekPrefixGE(b *testing.B) {
	l := NewSkiplist(newArena(64<<10), bytes.Compare)
	var count int
	// count was measured to be 1279.
	for count = 0; ; count++ {
		if err := l.Add(makeIntKey(count), makeValue(count)); err == ErrArenaFull {
			break
		}
	}
	for _, skip := range []int{1, 2, 4, 8, 16} {
		for _, useNext := range []bool{false, true} {
			b.Run(fmt.Sprintf("skip=%d/use-next=%t", skip, useNext), func(b *testing.B) {
				it := l.NewIter(nil, nil)
				j := 0
				var k []byte
				makeKey := func() {
					k = []byte(fmt.Sprintf("%05d", j))
				}
				makeKey()
				it.SeekPrefixGE(k, k, base.SeekGEFlagsNone)
				b.ResetTimer()
				for i := 0; i < b.N; i++ {
					j += skip
					var flags base.SeekGEFlags
					if useNext {
						flags = flags.EnableTrySeekUsingNext()
					}
					if j >= count {
						j = 0
						flags = flags.DisableTrySeekUsingNext()
					}
					makeKey()
					it.SeekPrefixGE(k, k, flags)
				}
			})
		}
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
// 				rng := rand.New(rand.NewPCG(0, time.Now().UnixNano()))
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
