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
