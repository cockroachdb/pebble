// Copyright 2018. All rights reserved. Use of this source code is governed by
// an MIT-style license that can be found in the LICENSE file.

package cache

import (
	"bufio"
	"bytes"
	"fmt"
	"math/rand/v2"
	"os"
	"runtime"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/cockroachdb/pebble/internal/base"
	"github.com/stretchr/testify/require"
)

func TestCache(t *testing.T) {
	// Test data was generated from the python code
	f, err := os.Open("testdata/cache")
	require.NoError(t, err)

	cache := newShards(200, 1)
	defer cache.Unref()

	scanner := bufio.NewScanner(f)
	line := 1

	for scanner.Scan() {
		fields := bytes.Fields(scanner.Bytes())

		key, err := strconv.Atoi(string(fields[0]))
		require.NoError(t, err)

		wantHit := fields[1][0] == 'h'

		var hit bool
		cv := cache.Get(1, base.DiskFileNum(key), 0)
		if cv == nil {
			cv = Alloc(1)
			cv.RawBuffer()[0] = fields[0][0]
			cache.Set(1, base.DiskFileNum(key), 0, cv)
		} else {
			hit = true
			if v := cv.RawBuffer(); !bytes.Equal(v, fields[0][:1]) {
				t.Errorf("%d: cache returned bad data: got %s , want %s\n", line, v, fields[0][:1])
			}
		}
		cv.Release()
		if hit != wantHit {
			t.Errorf("%d: cache hit mismatch: got %v, want %v\n", line, hit, wantHit)
		}
		line++
	}
}

func setTestValue(
	cache *Cache, id ID, fileNum base.DiskFileNum, offset uint64, s string, repeat int,
) {
	b := bytes.Repeat([]byte(s), repeat)
	v := Alloc(len(b))
	copy(v.RawBuffer(), b)
	cache.Set(id, fileNum, offset, v)
	v.Release()
}

func TestCacheDelete(t *testing.T) {
	cache := newShards(100, 1)
	defer cache.Unref()

	setTestValue(cache, 1, 0, 0, "a", 5)
	setTestValue(cache, 1, 1, 0, "a", 5)
	setTestValue(cache, 1, 2, 0, "a", 5)
	if expected, size := int64(15), cache.Size(); expected != size {
		t.Fatalf("expected cache size %d, but found %d", expected, size)
	}
	cache.Delete(1, base.DiskFileNum(1), 0)
	if expected, size := int64(10), cache.Size(); expected != size {
		t.Fatalf("expected cache size %d, but found %d", expected, size)
	}
	if v := cache.Get(1, base.DiskFileNum(0), 0); v == nil {
		t.Fatalf("expected to find block 0/0")
	} else {
		v.Release()
	}
	if v := cache.Get(1, base.DiskFileNum(1), 0); v != nil {
		t.Fatalf("expected to not find block 1/0")
	}
	// Deleting a non-existing block does nothing.
	cache.Delete(1, base.DiskFileNum(1), 0)
	if expected, size := int64(10), cache.Size(); expected != size {
		t.Fatalf("expected cache size %d, but found %d", expected, size)
	}
}

func TestEvictFile(t *testing.T) {
	cache := newShards(100, 1)
	defer cache.Unref()

	setTestValue(cache, 1, 0, 0, "a", 5)
	setTestValue(cache, 1, 1, 0, "a", 5)
	setTestValue(cache, 1, 2, 0, "a", 5)
	setTestValue(cache, 1, 2, 1, "a", 5)
	setTestValue(cache, 1, 2, 2, "a", 5)
	if expected, size := int64(25), cache.Size(); expected != size {
		t.Fatalf("expected cache size %d, but found %d", expected, size)
	}
	cache.EvictFile(1, base.DiskFileNum(0))
	if expected, size := int64(20), cache.Size(); expected != size {
		t.Fatalf("expected cache size %d, but found %d", expected, size)
	}
	cache.EvictFile(1, base.DiskFileNum(1))
	if expected, size := int64(15), cache.Size(); expected != size {
		t.Fatalf("expected cache size %d, but found %d", expected, size)
	}
	cache.EvictFile(1, base.DiskFileNum(2))
	if expected, size := int64(0), cache.Size(); expected != size {
		t.Fatalf("expected cache size %d, but found %d", expected, size)
	}
}

func TestEvictAll(t *testing.T) {
	// Verify that it is okay to evict all of the data from a cache. Previously
	// this would trigger a nil-pointer dereference.
	cache := newShards(100, 1)
	defer cache.Unref()

	setTestValue(cache, 1, 0, 0, "a", 101)
	setTestValue(cache, 1, 1, 0, "a", 101)
}

func TestMultipleDBs(t *testing.T) {
	cache := newShards(100, 1)
	defer cache.Unref()

	setTestValue(cache, 1, 0, 0, "a", 5)
	setTestValue(cache, 2, 0, 0, "b", 5)
	if expected, size := int64(10), cache.Size(); expected != size {
		t.Fatalf("expected cache size %d, but found %d", expected, size)
	}
	cache.EvictFile(1, base.DiskFileNum(0))
	if expected, size := int64(5), cache.Size(); expected != size {
		t.Fatalf("expected cache size %d, but found %d", expected, size)
	}
	v := cache.Get(1, base.DiskFileNum(0), 0)
	if v != nil {
		t.Fatalf("expected not present, but found %#v", v)
	}
	v = cache.Get(2, base.DiskFileNum(0), 0)
	if v := v.RawBuffer(); string(v) != "bbbbb" {
		t.Fatalf("expected bbbbb, but found %s", v)
	}
	v.Release()
}

func TestZeroSize(t *testing.T) {
	cache := newShards(0, 1)
	defer cache.Unref()

	setTestValue(cache, 1, 0, 0, "a", 5)
}

func TestReserve(t *testing.T) {
	cache := newShards(4, 2)
	defer cache.Unref()

	setTestValue(cache, 1, 0, 0, "a", 1)
	setTestValue(cache, 2, 0, 0, "a", 1)
	require.EqualValues(t, 2, cache.Size())
	r := cache.Reserve(1)
	require.EqualValues(t, 0, cache.Size())

	setTestValue(cache, 1, 0, 0, "a", 1)
	setTestValue(cache, 2, 0, 0, "a", 1)
	setTestValue(cache, 3, 0, 0, "a", 1)
	setTestValue(cache, 4, 0, 0, "a", 1)
	require.EqualValues(t, 2, cache.Size())
	r()
	require.EqualValues(t, 2, cache.Size())
	setTestValue(cache, 1, 0, 0, "a", 1)
	setTestValue(cache, 2, 0, 0, "a", 1)
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

func TestCacheStressSetExisting(t *testing.T) {
	cache := newShards(1, 1)
	defer cache.Unref()

	var wg sync.WaitGroup
	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			for j := 0; j < 10000; j++ {
				setTestValue(cache, 1, 0, uint64(i), "a", 1)
				runtime.Gosched()
			}
		}(i)
	}
	wg.Wait()
}

func BenchmarkCacheGet(b *testing.B) {
	const size = 100000

	cache := newShards(size, 1)
	defer cache.Unref()

	for i := 0; i < size; i++ {
		setTestValue(cache, 1, 0, 0, "a", 1)
	}

	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		rng := rand.New(rand.NewPCG(0, uint64(time.Now().UnixNano())))

		for pb.Next() {
			v := cache.Get(1, base.DiskFileNum(0), uint64(rng.IntN(size)))
			if v == nil {
				b.Fatal("failed to lookup value")
			}
			v.Release()
		}
	})
}

func TestReserveColdTarget(t *testing.T) {
	// If coldTarget isn't updated when we call shard.Reserve,
	// then we unnecessarily remove nodes from the
	// cache.

	cache := newShards(100, 1)
	defer cache.Unref()

	for i := 0; i < 50; i++ {
		setTestValue(cache, ID(i+1), 0, 0, "a", 1)
	}

	if cache.Size() != 50 {
		require.Equal(t, 50, cache.Size(), "nodes were unnecessarily evicted from the cache")
	}

	// There won't be enough space left for 50 nodes in the cache after
	// we call shard.Reserve. This should trigger a call to evict.
	cache.Reserve(51)

	// If we don't update coldTarget in Reserve then the cache gets emptied to
	// size 0. In shard.Evict, we loop until shard.Size() < shard.targetSize().
	// Therefore, 100 - 51 = 49, but we evict one more node.
	if cache.Size() != 48 {
		t.Fatalf("expected positive cache size %d, but found %d", 48, cache.Size())
	}
}
