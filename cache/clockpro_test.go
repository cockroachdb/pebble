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

	cache := New(200)
	scanner := bufio.NewScanner(f)

	for scanner.Scan() {
		fields := bytes.Fields(scanner.Bytes())

		key, err := strconv.Atoi(string(fields[0]))
		if err != nil {
			t.Fatal(err)
		}
		wantHit := fields[1][0] == 'h'

		var hit bool
		v := cache.Get(uint64(key), 0)
		if v == nil {
			cache.Set(uint64(key), 0, append([]byte(nil), fields[0][0]))
		} else {
			hit = true
			if !bytes.Equal(v, fields[0][:1]) {
				t.Errorf("cache returned bad data: got %s , want %s\n", v, fields[0][:1])
			}
		}
		if hit != wantHit {
			t.Errorf("cache hit mismatch: got %v, want %v\n", hit, wantHit)
		}
	}
}

func TestWeakHandle(t *testing.T) {
	cache := New(5)
	cache.Set(1, 0, bytes.Repeat([]byte("a"), 5))
	h := cache.Set(0, 0, bytes.Repeat([]byte("b"), 5))
	if v := h.Get(); string(v) != "bbbbb" {
		t.Fatalf("expected bbbbb, but found %v", v)
	}
	cache.Set(2, 0, bytes.Repeat([]byte("a"), 5))
	if v := h.Get(); v != nil {
		t.Fatalf("expected nil, but found %s", v)
	}
}

func TestEvictFiles(t *testing.T) {
	cache := New(100)
	cache.Set(0, 0, bytes.Repeat([]byte("a"), 5))
	cache.Set(1, 0, bytes.Repeat([]byte("a"), 5))
	cache.Set(2, 0, bytes.Repeat([]byte("a"), 5))
	cache.Set(2, 1, bytes.Repeat([]byte("a"), 5))
	cache.Set(2, 2, bytes.Repeat([]byte("a"), 5))
	if expected, size := int64(25), cache.Size(); expected != size {
		t.Fatalf("expected cache size of %d, but found %d", expected, size)
	}
	files := func(files ...uint64) map[uint64]struct{} {
		m := make(map[uint64]struct{})
		for _, f := range files {
			m[f] = struct{}{}
		}
		return m
	}
	cache.EvictFiles(files(2))
	if expected, size := int64(10), cache.Size(); expected != size {
		t.Fatalf("expected cache size of %d, but found %d", expected, size)
	}
	cache.EvictFiles(files(1, 0))
	if expected, size := int64(0), cache.Size(); expected != size {
		t.Fatalf("expected cache size of %d, but found %d", expected, size)
	}
}
