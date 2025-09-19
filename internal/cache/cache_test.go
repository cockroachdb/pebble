// Copyright 2025 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

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

	cache := NewWithShards(200, 1)
	defer cache.Unref()
	h := cache.NewHandle()
	defer h.Close()

	scanner := bufio.NewScanner(f)
	line := 1

	for scanner.Scan() {
		fields := bytes.Fields(scanner.Bytes())

		key, err := strconv.Atoi(string(fields[0]))
		require.NoError(t, err)

		wantHit := fields[1][0] == 'h'

		var hit bool
		cv := h.Get(base.DiskFileNum(key), 0)
		if cv == nil {
			cv = Alloc(1)
			cv.RawBuffer()[0] = fields[0][0]
			h.Set(base.DiskFileNum(key), 0, cv)
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

func setTestValue(c *Handle, fileNum base.DiskFileNum, offset uint64, s string, repeat int) {
	b := bytes.Repeat([]byte(s), repeat)
	v := Alloc(len(b))
	copy(v.RawBuffer(), b)
	c.Set(fileNum, offset, v)
	v.Release()
}

// TestCachePeek verifies that Peek does not update the recently accessed status
// of an entry.
func TestCachePeek(t *testing.T) {
	const size = 10
	cache := NewWithShards(size, 1)
	defer cache.Unref()
	h := cache.NewHandle()
	defer h.Close()

	for i := range size {
		setTestValue(h, 0, uint64(i), "a", 1)
	}
	for i := range size / 2 {
		v := h.Get(base.DiskFileNum(0), uint64(i))
		if v == nil {
			t.Fatalf("expected to find block %d", i)
		}
		v.Release()
	}
	for i := size / 2; i < size; i++ {
		v := h.Peek(base.DiskFileNum(0), uint64(i))
		if v == nil {
			t.Fatalf("expected to find block %d", i)
		}
		v.Release()
	}
	// Now add more entries to cause eviction.
	for i := range size / 4 {
		setTestValue(h, 0, uint64(size+i), "a", 1)
	}
	// Verify that the Gets still find their values, despite the Peeks.
	for i := range size / 2 {
		v := h.Get(base.DiskFileNum(0), uint64(i))
		if v == nil {
			t.Fatalf("expected to find block %d", i)
		}
		v.Release()
	}
}

func TestCacheDelete(t *testing.T) {
	cache := NewWithShards(100, 1)
	defer cache.Unref()
	h := cache.NewHandle()
	defer h.Close()

	setTestValue(h, 0, 0, "a", 5)
	setTestValue(h, 1, 0, "a", 5)
	setTestValue(h, 2, 0, "a", 5)
	if expected, size := int64(15), cache.Size(); expected != size {
		t.Fatalf("expected cache size %d, but found %d", expected, size)
	}
	h.Delete(base.DiskFileNum(1), 0)
	if expected, size := int64(10), cache.Size(); expected != size {
		t.Fatalf("expected cache size %d, but found %d", expected, size)
	}
	if v := h.Get(base.DiskFileNum(0), 0); v == nil {
		t.Fatalf("expected to find block 0/0")
	} else {
		v.Release()
	}
	if v := h.Get(base.DiskFileNum(1), 0); v != nil {
		t.Fatalf("expected to not find block 1/0")
	}
	// Deleting a non-existing block does nothing.
	h.Delete(base.DiskFileNum(1), 0)
	if expected, size := int64(10), cache.Size(); expected != size {
		t.Fatalf("expected cache size %d, but found %d", expected, size)
	}
}

func TestEvictFile(t *testing.T) {
	cache := NewWithShards(100, 1)
	defer cache.Unref()
	h := cache.NewHandle()
	defer h.Close()

	setTestValue(h, 0, 0, "a", 5)
	setTestValue(h, 1, 0, "a", 5)
	setTestValue(h, 2, 0, "a", 5)
	setTestValue(h, 2, 1, "a", 5)
	setTestValue(h, 2, 2, "a", 5)
	if expected, size := int64(25), cache.Size(); expected != size {
		t.Fatalf("expected cache size %d, but found %d", expected, size)
	}
	h.EvictFile(base.DiskFileNum(0))
	if expected, size := int64(20), cache.Size(); expected != size {
		t.Fatalf("expected cache size %d, but found %d", expected, size)
	}
	h.EvictFile(base.DiskFileNum(1))
	if expected, size := int64(15), cache.Size(); expected != size {
		t.Fatalf("expected cache size %d, but found %d", expected, size)
	}
	h.EvictFile(base.DiskFileNum(2))
	if expected, size := int64(0), cache.Size(); expected != size {
		t.Fatalf("expected cache size %d, but found %d", expected, size)
	}
}

func TestEvictAll(t *testing.T) {
	// Verify that it is okay to evict all of the data from a cache. Previously
	// this would trigger a nil-pointer dereference.
	cache := NewWithShards(100, 1)
	defer cache.Unref()
	h := cache.NewHandle()
	defer h.Close()

	setTestValue(h, 0, 0, "a", 101)
	setTestValue(h, 1, 0, "a", 101)
}

func TestMultipleDBs(t *testing.T) {
	cache := NewWithShards(100, 1)
	defer cache.Unref()
	h1 := cache.NewHandle()
	defer h1.Close()
	h2 := cache.NewHandle()
	defer h2.Close()

	setTestValue(h1, 0, 0, "a", 5)
	setTestValue(h2, 0, 0, "b", 5)
	if expected, size := int64(10), cache.Size(); expected != size {
		t.Fatalf("expected cache size %d, but found %d", expected, size)
	}
	h1.EvictFile(base.DiskFileNum(0))
	if expected, size := int64(5), cache.Size(); expected != size {
		t.Fatalf("expected cache size %d, but found %d", expected, size)
	}
	v := h1.Get(base.DiskFileNum(0), 0)
	if v != nil {
		t.Fatalf("expected not present, but found %#v", v)
	}
	v = h2.Get(base.DiskFileNum(0), 0)
	if v := v.RawBuffer(); string(v) != "bbbbb" {
		t.Fatalf("expected bbbbb, but found %s", v)
	}
	v.Release()
}

func TestZeroSize(t *testing.T) {
	c := New(0)
	defer c.Unref()
	h := c.NewHandle()
	defer h.Close()
	setTestValue(h, 0, 0, "a", 5)
}

func TestReserve(t *testing.T) {
	cache := NewWithShards(4, 2)
	defer cache.Unref()
	h1 := cache.NewHandle()
	defer h1.Close()
	h2 := cache.NewHandle()
	defer h2.Close()
	h3 := cache.NewHandle()
	defer h3.Close()
	h4 := cache.NewHandle()
	defer h4.Close()

	setTestValue(h1, 0, 0, "a", 1)
	setTestValue(h2, 0, 0, "a", 1)
	require.EqualValues(t, 2, cache.Size())
	r := cache.Reserve(1)
	require.EqualValues(t, 0, cache.Size())

	setTestValue(h1, 0, 0, "a", 1)
	setTestValue(h2, 0, 0, "a", 1)
	setTestValue(h3, 0, 0, "a", 1)
	setTestValue(h4, 0, 0, "a", 1)
	require.EqualValues(t, 2, cache.Size())
	r()
	require.EqualValues(t, 2, cache.Size())
	setTestValue(h1, 0, 0, "a", 1)
	setTestValue(h2, 0, 0, "a", 1)
	require.EqualValues(t, 4, cache.Size())
}

func TestReserveDoubleRelease(t *testing.T) {
	cache := NewWithShards(100, 1)
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
	cache := NewWithShards(1, 1)
	defer cache.Unref()
	h := cache.NewHandle()
	defer h.Close()

	var wg sync.WaitGroup
	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			for j := 0; j < 10000; j++ {
				setTestValue(h, 0, uint64(i), "a", 1)
				runtime.Gosched()
			}
		}(i)
	}
	wg.Wait()
}

func BenchmarkCacheGet(b *testing.B) {
	const size = 100000

	n := runtime.GOMAXPROCS(0)
	cache := NewWithShards(size*int64(n), n)
	defer cache.Unref()
	h := cache.NewHandle()
	defer h.Close()

	for i := 0; i < size; i++ {
		setTestValue(h, 0, uint64(i), "a", 1)
	}

	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		rng := rand.New(rand.NewPCG(0, uint64(time.Now().UnixNano())))

		for pb.Next() {
			v := h.Get(base.DiskFileNum(0), uint64(rng.IntN(size)))
			if v == nil {
				b.Fatal("failed to lookup value")
			}
			v.Release()
		}
	})
}
