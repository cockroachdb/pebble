// Copyright 2011 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package base

import (
	"fmt"
	"math/rand/v2"
	"slices"
	"testing"
	"time"
)

func TestDefaultComparer_Separator(t *testing.T) {
	testCases := []struct {
		a, b, want string
	}{
		{"black", "blue", "blb"},
		{"1", "2", "1"},
		{"1", "29", "2"},
		{"13", "19", "14"},
		{"13", "99", "2"},
		{"135", "19", "14"},
		{"1357", "19", "14"},
		{"1357", "2", "14"},
		{"13\xff", "14", "13\xff"},
		{"13\xff", "19", "14"},
		{"1\xff\xff", "19", "1\xff\xff"},
		{"1\xff\xff", "2", "1\xff\xff"},
		{"1\xff\xff", "9", "2"},
	}
	for _, tc := range testCases {
		t.Run("", func(t *testing.T) {
			got := string(DefaultComparer.Separator(nil, []byte(tc.a), []byte(tc.b)))
			if got != tc.want {
				t.Errorf("DefaultComparer.Separator(nil, %q, %q) = %q, want %q", tc.a, tc.b, got, tc.want)
			}
		})
	}
}

func TestDefaultComparer_Successor(t *testing.T) {
	testCases := []struct {
		a, want string
	}{
		{"green", "h"},
		{"", ""},
		{"1", "2"},
		{"11", "2"},
		{"11\xff", "2"},
		{"1\xff", "2"},
		{"1\xff\xff", "2"},
		{"\xff", "\xff"},
		{"\xff\xff", "\xff\xff"},
		{"\xff\xff\xff", "\xff\xff\xff"},
	}
	for _, tc := range testCases {
		t.Run("", func(t *testing.T) {
			got := string(DefaultComparer.Successor(nil, []byte(tc.a)))
			if got != tc.want {
				t.Errorf("DefaultComparer.Successor(nil, %q) = %q, want %q", tc.a, got, tc.want)
			}
		})
	}
}

func TestDefaultComparer(t *testing.T) {
	if err := CheckComparer(DefaultComparer, [][]byte{{}, []byte("abc"), []byte("d"), []byte("ef")}, [][]byte{{}}); err != nil {
		t.Error(err)
	}
}

func TestAbbreviatedKey(t *testing.T) {
	rng := rand.New(rand.NewPCG(0, uint64(time.Now().UnixNano())))
	randBytes := func(size int) []byte {
		data := make([]byte, size)
		for i := range data {
			data[i] = byte(rng.Int() & 0xff)
		}
		return data
	}

	keys := make([][]byte, 10000)
	for i := range keys {
		keys[i] = randBytes(rng.IntN(16))
	}
	slices.SortFunc(keys, DefaultComparer.Compare)

	for i := 1; i < len(keys); i++ {
		last := DefaultComparer.AbbreviatedKey(keys[i-1])
		cur := DefaultComparer.AbbreviatedKey(keys[i])
		cmp := DefaultComparer.Compare(keys[i-1], keys[i])
		if cmp == 0 {
			if last != cur {
				t.Fatalf("expected equal abbreviated keys: %x[%x] != %x[%x]",
					last, keys[i-1], cur, keys[i])
			}
		} else {
			if last > cur {
				t.Fatalf("unexpected abbreviated key ordering: %x[%x] > %x[%x]",
					last, keys[i-1], cur, keys[i])
			}
		}
	}
}

func BenchmarkAbbreviatedKey(b *testing.B) {
	rng := rand.New(rand.NewPCG(0, 1449168817))
	randBytes := func(size int) []byte {
		data := make([]byte, size)
		for i := range data {
			data[i] = byte(rng.Int() & 0xff)
		}
		return data
	}
	keys := make([][]byte, 10000)
	for i := range keys {
		keys[i] = randBytes(8)
	}

	b.ResetTimer()
	var sum uint64
	for i := 0; i < b.N; i++ {
		j := i % len(keys)
		sum += DefaultComparer.AbbreviatedKey(keys[j])
	}

	if testing.Verbose() {
		// Ensure the compiler doesn't optimize away our benchmark.
		fmt.Println(sum)
	}
}
