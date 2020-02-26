// Copyright 2018. All rights reserved. Use of this source code is governed by
// an MIT-style license that can be found in the LICENSE file.

package cache

import (
	"bufio"
	"bytes"
	"fmt"
	"os"
	"strconv"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"golang.org/x/exp/rand"
)

func TestCache(t *testing.T) {
	// Test data was generated from the python code
	f, err := os.Open("testdata/cache")
	if err != nil {
		t.Fatal(err)
	}

	cache := newShards(200, 1)
	defer cache.Unref()

	scanner := bufio.NewScanner(f)
	line := 1

	for scanner.Scan() {
		fields := bytes.Fields(scanner.Bytes())

		key, err := strconv.Atoi(string(fields[0]))
		if err != nil {
			t.Fatal(err)
		}
		wantHit := fields[1][0] == 'h'

		var hit bool
		h := cache.Get(1, uint64(key), 0)
		if v := h.Get(); v == nil {
			value := cache.Alloc(1, false /* weak */)
			value.Buf()[0] = fields[0][0]
			cache.Set(1, uint64(key), 0, value).Release()
		} else {
			hit = true
			if !bytes.Equal(v, fields[0][:1]) {
				t.Errorf("%d: cache returned bad data: got %s , want %s\n", line, v, fields[0][:1])
			}
		}
		h.Release()
		if hit != wantHit {
			t.Errorf("%d: cache hit mismatch: got %v, want %v\n", line, hit, wantHit)
		}
		line++
	}
}

func testValue(cache *Cache, s string, repeat int) *Value {
	b := bytes.Repeat([]byte(s), repeat)
	v := cache.Alloc(len(b), false /* weak */)
	copy(v.Buf(), b)
	return v
}

func testWeakValue(cache *Cache, s string, repeat int) *Value {
	b := bytes.Repeat([]byte(s), repeat)
	v := cache.Alloc(len(b), true /* weak */)
	copy(v.Buf(), b)
	return v
}

func TestWeakHandle(t *testing.T) {
	cache := newShards(5, 1)
	defer cache.Unref()

	cache.Set(1, 1, 0, testWeakValue(cache, "a", 5)).Release()
	h := cache.Set(1, 0, 0, testWeakValue(cache, "b", 5))
	if v := h.Get(); string(v) != "bbbbb" {
		t.Fatalf("expected bbbbb, but found %v", v)
	}
	w := h.Weak()
	h.Release()
	h = w.Strong()
	if v := h.Get(); string(v) != "bbbbb" {
		t.Fatalf("expected bbbbb, but found %v", v)
	}
	if h.Weak() != nil {
		t.Fatalf("unexpectedly convert strong handle back to weak handle")
	}
	h.Release()
	cache.Set(1, 2, 0, testValue(cache, "a", 5)).Release()
	h = w.Strong()
	if v := h.Get(); v != nil {
		t.Fatalf("expected nil, but found %s", v)
	}
	h.Release()
	w.Release()
}

func TestCacheDelete(t *testing.T) {
	cache := newShards(100, 1)
	defer cache.Unref()

	cache.Set(1, 0, 0, testValue(cache, "a", 5)).Release()
	cache.Set(1, 1, 0, testValue(cache, "a", 5)).Release()
	cache.Set(1, 2, 0, testValue(cache, "a", 5)).Release()
	if expected, size := int64(15), cache.Size(); expected != size {
		t.Fatalf("expected cache size %d, but found %d", expected, size)
	}
	cache.Delete(1, 1, 0)
	if expected, size := int64(10), cache.Size(); expected != size {
		t.Fatalf("expected cache size %d, but found %d", expected, size)
	}
	if h := cache.Get(1, 0, 0); h.Get() == nil {
		t.Fatalf("expected to find block 0/0")
	} else {
		h.Release()
	}
	if h := cache.Get(1, 1, 0); h.Get() != nil {
		t.Fatalf("expected to not find block 1/0")
	} else {
		h.Release()
	}
	// Deleting a non-existing block does nothing.
	cache.Delete(1, 1, 0)
	if expected, size := int64(10), cache.Size(); expected != size {
		t.Fatalf("expected cache size %d, but found %d", expected, size)
	}
}

func TestEvictFile(t *testing.T) {
	cache := newShards(100, 1)
	defer cache.Unref()

	cache.Set(1, 0, 0, testValue(cache, "a", 5)).Release()
	cache.Set(1, 1, 0, testValue(cache, "a", 5)).Release()
	cache.Set(1, 2, 0, testValue(cache, "a", 5)).Release()
	cache.Set(1, 2, 1, testValue(cache, "a", 5)).Release()
	cache.Set(1, 2, 2, testValue(cache, "a", 5)).Release()
	if expected, size := int64(25), cache.Size(); expected != size {
		t.Fatalf("expected cache size %d, but found %d", expected, size)
	}
	cache.EvictFile(1, 0)
	if expected, size := int64(20), cache.Size(); expected != size {
		t.Fatalf("expected cache size %d, but found %d", expected, size)
	}
	cache.EvictFile(1, 1)
	if expected, size := int64(15), cache.Size(); expected != size {
		t.Fatalf("expected cache size %d, but found %d", expected, size)
	}
	cache.EvictFile(1, 2)
	if expected, size := int64(0), cache.Size(); expected != size {
		t.Fatalf("expected cache size %d, but found %d", expected, size)
	}
}

func TestEvictAll(t *testing.T) {
	// Verify that it is okay to evict all of the data from a cache. Previously
	// this would trigger a nil-pointer dereference.
	cache := newShards(100, 1)
	defer cache.Unref()

	cache.Set(1, 0, 0, testValue(cache, "a", 101)).Release()
	cache.Set(1, 1, 0, testValue(cache, "a", 101)).Release()
}

func TestMultipleDBs(t *testing.T) {
	cache := newShards(100, 1)
	defer cache.Unref()

	cache.Set(1, 0, 0, testValue(cache, "a", 5)).Release()
	cache.Set(2, 0, 0, testValue(cache, "b", 5)).Release()
	if expected, size := int64(10), cache.Size(); expected != size {
		t.Fatalf("expected cache size %d, but found %d", expected, size)
	}
	cache.EvictFile(1, 0)
	if expected, size := int64(5), cache.Size(); expected != size {
		t.Fatalf("expected cache size %d, but found %d", expected, size)
	}
	h := cache.Get(1, 0, 0)
	if v := h.Get(); v != nil {
		t.Fatalf("expected not present, but found %s", v)
	}
	h = cache.Get(2, 0, 0)
	if v := h.Get(); string(v) != "bbbbb" {
		t.Fatalf("expected bbbbb, but found %s", v)
	} else {
		h.Release()
	}
}

func TestZeroSize(t *testing.T) {
	cache := newShards(0, 1)
	defer cache.Unref()

	cache.Set(1, 0, 0, testValue(cache, "a", 5)).Release()
}

func TestReserve(t *testing.T) {
	cache := newShards(4, 2)
	defer cache.Unref()

	cache.Set(1, 0, 0, testValue(cache, "a", 1)).Release()
	cache.Set(2, 0, 0, testValue(cache, "a", 1)).Release()
	require.EqualValues(t, 2, cache.Size())
	r := cache.Reserve(1)
	require.EqualValues(t, 0, cache.Size())
	cache.Set(1, 0, 0, testValue(cache, "a", 1)).Release()
	cache.Set(2, 0, 0, testValue(cache, "a", 1)).Release()
	cache.Set(3, 0, 0, testValue(cache, "a", 1)).Release()
	cache.Set(4, 0, 0, testValue(cache, "a", 1)).Release()
	require.EqualValues(t, 2, cache.Size())
	r()
	require.EqualValues(t, 2, cache.Size())
	cache.Set(1, 0, 0, testValue(cache, "a", 1)).Release()
	cache.Set(2, 0, 0, testValue(cache, "a", 1)).Release()
	require.EqualValues(t, 4, cache.Size())
}

func TestReserveDoubleRelease(t *testing.T) {
	cache := newShards(100, 1)
	defer cache.Unref()

	r := cache.Reserve(10)
	r()

	result := func() (result string) {
		defer func() {
			if v := recover(); v != nil {
				result = fmt.Sprint(v)
			}
		}()
		r()
		return ""
	}()
	const expected = "pebble: cache reservation already released"
	if expected != result {
		t.Fatalf("expected %q, but found %q", expected, result)
	}
}

func BenchmarkCacheGet(b *testing.B) {
	const size = 100000

	for _, kind := range []string{"strong", "weak"} {
		b.Run(kind, func(b *testing.B) {
			cache := newShards(size, 1)
			defer cache.Unref()

			for i := 0; i < size; i++ {
				var v *Value
				if kind == "strong" {
					v = testValue(cache, "a", 1)
				} else {
					v = testWeakValue(cache, "a", 1)
				}
				cache.Set(1, 0, uint64(i), v).Release()
			}

			b.ResetTimer()
			b.RunParallel(func(pb *testing.PB) {
				rng := rand.New(rand.NewSource(uint64(time.Now().UnixNano())))

				for pb.Next() {
					h := cache.Get(1, 0, uint64(rng.Intn(size)))
					if h.Get() == nil {
						b.Fatal("failed to lookup value")
					}
					h.Release()
				}
			})
		})
	}
}
