package cache

import (
	"bufio"
	"bytes"
	"os"
	"testing"
)

func TestCache(t *testing.T) {
	// Test data was generated from the python code
	f, err := os.Open("testdata/domains.txt")
	if err != nil {
		t.Fatal(err)
	}

	cache := New(200)
	scanner := bufio.NewScanner(f)

	for scanner.Scan() {
		fields := bytes.Fields(scanner.Bytes())

		key := string(fields[0])
		wantHit := fields[1][0] == 'h'

		var hit bool
		v := cache.Get(key)
		if v == nil {
			cache.Set(key, key)
		} else {
			hit = true
			if v.(string) != key {
				t.Errorf("cache returned bad data: got %+v , want %+v\n", v, key)
			}
		}
		if hit != wantHit {
			t.Errorf("cache hit mismatch: got %v, want %v\n", hit, wantHit)
		}
	}
}
