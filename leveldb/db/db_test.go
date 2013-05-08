// Copyright 2013 The LevelDB-Go Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package db

import (
	"bytes"
	"errors"
	"fmt"
	"math/rand"
	"strings"
	"testing"
)

var testKeyValuePairs = []string{
	"10:ten",
	"11:eleven",
	"12:twelve",
	"13:thirteen",
	"14:fourteen",
	"15:fifteen",
	"16:sixteen",
	"17:seventeen",
	"18:eighteen",
	"19:nineteen",
}

type fakeIter struct {
	kvPairs  []string
	index    int
	closeErr error
}

func newFakeIterator(closeErr error, kvPairs ...string) *fakeIter {
	for _, kv := range kvPairs {
		if !strings.Contains(kv, ":") {
			panic(fmt.Sprintf(`key-value pair %q does not contain ":"`, kv))
		}
	}
	return &fakeIter{
		kvPairs:  kvPairs,
		index:    -1,
		closeErr: closeErr,
	}
}

func (f *fakeIter) Next() bool {
	f.index++
	return f.index < len(f.kvPairs)
}

func (f *fakeIter) Key() []byte {
	kv := f.kvPairs[f.index]
	i := strings.Index(kv, ":")
	return []byte(kv[:i])
}

func (f *fakeIter) Value() []byte {
	kv := f.kvPairs[f.index]
	i := strings.Index(kv, ":")
	return []byte(kv[i+1:])
}

func (f *fakeIter) Close() error {
	return f.closeErr
}

// testIterator tests creating a combined iterator from a number of sub-
// iterators. newFunc is a constructor function. splitFunc returns a random
// split of the testKeyValuePairs slice such that walking a combined iterator
// over those splits should recover the original key/value pairs in order.
func testIterator(t *testing.T, newFunc func(...Iterator) Iterator, splitFunc func(r *rand.Rand) [][]string) {
	// Test pre-determined sub-iterators. The sub-iterators are designed
	// so that the combined key/value pair order is the same whether the
	// combined iterator is concatenating or merging.
	testCases := []struct {
		desc  string
		iters []Iterator
		want  string
	}{
		{
			"one sub-iterator",
			[]Iterator{
				newFakeIterator(nil, "e:east", "w:west"),
			},
			"<e:east><w:west>.",
		},
		{
			"two sub-iterators",
			[]Iterator{
				newFakeIterator(nil, "a0:0"),
				newFakeIterator(nil, "b1:1", "b2:2"),
			},
			"<a0:0><b1:1><b2:2>.",
		},
		{
			"empty sub-iterators",
			[]Iterator{
				newFakeIterator(nil),
				newFakeIterator(nil),
				newFakeIterator(nil),
			},
			".",
		},
		{
			"sub-iterator errors",
			[]Iterator{
				newFakeIterator(nil, "a0:0", "a1:1"),
				newFakeIterator(errors.New("the sky is falling!"), "b2:2", "b3:3", "b4:4"),
				newFakeIterator(errors.New("run for your lives!"), "c5:5", "c6:6"),
			},
			"<a0:0><a1:1><b2:2><b3:3><b4:4>err=the sky is falling!",
		},
	}
	for _, tc := range testCases {
		var b bytes.Buffer
		iter := newFunc(tc.iters...)
		for iter.Next() {
			fmt.Fprintf(&b, "<%s:%s>", iter.Key(), iter.Value())
		}
		if err := iter.Close(); err != nil {
			fmt.Fprintf(&b, "err=%v", err)
		} else {
			b.WriteByte('.')
		}
		if got := b.String(); got != tc.want {
			t.Errorf("%s:\ngot  %q\nwant %q", tc.desc, got, tc.want)
		}
	}

	// Test randomly generated sub-iterators.
	r := rand.New(rand.NewSource(0))
	for i, nBad := 0, 0; i < 1000; i++ {
		bad := false

		splits := splitFunc(r)
		iters := make([]Iterator, len(splits))
		for i, split := range splits {
			iters[i] = newFakeIterator(nil, split...)
		}
		iter := newFunc(iters...)

		j := 0
		for ; iter.Next() && j < len(testKeyValuePairs); j++ {
			got := string(iter.Key()) + ":" + string(iter.Value())
			want := testKeyValuePairs[j]
			if got != want {
				bad = true
				t.Errorf("random splits: i=%d, j=%d: got %q, want %q", i, j, got, want)
			}
		}
		if iter.Next() {
			bad = true
			t.Errorf("random splits: i=%d, j=%d: iter was not exhausted", i, j)
		}
		if j != len(testKeyValuePairs) {
			bad = true
			t.Errorf("random splits: i=%d, j=%d: want j=%d", i, j, len(testKeyValuePairs))
		}
		if err := iter.Close(); err != nil {
			bad = true
			t.Errorf("random splits: i=%d, j=%d: %v", i, j, err)
		}

		if bad {
			nBad++
			if nBad == 10 {
				t.Fatal("random splits: too many errors; stopping")
			}
		}
	}
}

func TestConcatenatingIterator(t *testing.T) {
	testIterator(t, NewConcatenatingIterator, func(r *rand.Rand) [][]string {
		// Partition testKeyValuePairs into one or more splits. Each individual
		// split is in increasing order, and different splits may not overlap
		// in range. Some of the splits may be empty.
		splits, remainder := [][]string{}, testKeyValuePairs
		for r.Intn(4) != 0 {
			i := r.Intn(1 + len(remainder))
			splits = append(splits, remainder[:i])
			remainder = remainder[i:]
		}
		if len(remainder) > 0 {
			splits = append(splits, remainder)
		}
		return splits
	})
}

func TestMergingIterator(t *testing.T) {
	newFunc := func(iters ...Iterator) Iterator {
		return NewMergingIterator(DefaultComparer, iters...)
	}
	testIterator(t, newFunc, func(r *rand.Rand) [][]string {
		// Shuffle testKeyValuePairs into one or more splits. Each individual
		// split is in increasing order, but different splits may overlap in
		// range. Some of the splits may be empty.
		splits := make([][]string, 1+r.Intn(2+len(testKeyValuePairs)))
		for _, kv := range testKeyValuePairs {
			j := r.Intn(len(splits))
			splits[j] = append(splits[j], kv)
		}
		return splits
	})
}
