// Copyright 2018. All rights reserved. Use of this source code is governed by
// an MIT-style license that can be found in the LICENSE file.

package cache

import (
	"bufio"
	"bytes"
	"os"
	"strconv"
	"testing"
)

func TestCache(t *testing.T) {
	// Test data was generated from the python code
	f, err := os.Open("testdata/cache")
	if err != nil {
		t.Fatal(err)
	}

	cache := newShards(200, 1)
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
		h := cache.Get(0, uint64(key), 0)
		if v := h.Get(); v == nil {
			cache.Set(0, uint64(key), 0, append([]byte(nil), fields[0][0]))
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

func TestWeakHandle(t *testing.T) {
	cache := newShards(5, 1)
	cache.Set(0, 1, 0, bytes.Repeat([]byte("a"), 5))
	h := cache.Set(0, 0, 0, bytes.Repeat([]byte("b"), 5))
	if v := h.Get(); string(v) != "bbbbb" {
		t.Fatalf("expected bbbbb, but found %v", v)
	}
	w := h.Weak()
	h.Release()
	if v := w.Get(); string(v) != "bbbbb" {
		t.Fatalf("expected bbbbb, but found %v", v)
	}
	cache.Set(0, 2, 0, bytes.Repeat([]byte("a"), 5))
	if v := w.Get(); v != nil {
		t.Fatalf("expected nil, but found %s", v)
	}
}

func TestEvictFile(t *testing.T) {
	cache := newShards(100, 1)
	cache.Set(0, 0, 0, bytes.Repeat([]byte("a"), 5))
	cache.Set(0, 1, 0, bytes.Repeat([]byte("a"), 5))
	cache.Set(0, 2, 0, bytes.Repeat([]byte("a"), 5))
	cache.Set(0, 2, 1, bytes.Repeat([]byte("a"), 5))
	cache.Set(0, 2, 2, bytes.Repeat([]byte("a"), 5))
	if expected, size := int64(25), cache.Size(); expected != size {
		t.Fatalf("expected cache size %d, but found %d", expected, size)
	}
	cache.EvictFile(0, 0)
	if expected, size := int64(20), cache.Size(); expected != size {
		t.Fatalf("expected cache size %d, but found %d", expected, size)
	}
	cache.EvictFile(0, 1)
	if expected, size := int64(15), cache.Size(); expected != size {
		t.Fatalf("expected cache size %d, but found %d", expected, size)
	}
	cache.EvictFile(0, 2)
	if expected, size := int64(0), cache.Size(); expected != size {
		t.Fatalf("expected cache size %d, but found %d", expected, size)
	}
}

func TestEvictAll(t *testing.T) {
	// Verify that it is okay to evict all of the data from a cache. Previously
	// this would trigger a nil-pointer dereference.
	cache := newShards(100, 1)
	cache.Set(0, 0, 0, bytes.Repeat([]byte("a"), 101))
	cache.Set(0, 1, 0, bytes.Repeat([]byte("a"), 101))
}

func TestMultipleDBs(t *testing.T) {
	cache := newShards(100, 1)
	cache.Set(0, 0, 0, bytes.Repeat([]byte("a"), 5))
	cache.Set(1, 0, 0, bytes.Repeat([]byte("b"), 5))
	if expected, size := int64(10), cache.Size(); expected != size {
		t.Fatalf("expected cache size %d, but found %d", expected, size)
	}
	cache.EvictFile(0, 0)
	if expected, size := int64(5), cache.Size(); expected != size {
		t.Fatalf("expected cache size %d, but found %d", expected, size)
	}
	h := cache.Get(0, 0, 0)
	if v := h.Get(); v != nil {
		t.Fatalf("expected not present, but found %s", v)
	}
	h = cache.Get(1, 0, 0)
	if v := h.Get(); string(v) != "bbbbb" {
		t.Fatalf("expected bbbbb, but found %v", v)
	}
}

func TestZeroSize(t *testing.T) {
	cache := newShards(0, 1)
	cache.Set(0, 0, 0, bytes.Repeat([]byte("a"), 5))
}
