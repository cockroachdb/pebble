// Copyright 2013 The LevelDB-Go Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package pebble

import (
	"bytes"
	"errors"
	"fmt"
	"math/rand"
	"strconv"
	"strings"
	"testing"

	"github.com/petermattis/pebble/db"
)

var testKeyValuePairs = []string{
	"10:10",
	"11:11",
	"12:12",
	"13:13",
	"14:14",
	"15:15",
	"16:16",
	"17:17",
	"18:18",
	"19:19",
}

type fakeIter struct {
	keys     []db.InternalKey
	index    int
	closeErr error
}

func newFakeIterator(closeErr error, keys ...string) *fakeIter {
	ikeys := make([]db.InternalKey, len(keys))
	for i, k := range keys {
		j := strings.Index(k, ":")
		seqnum, err := strconv.Atoi(k[j+1:])
		if err != nil {
			panic(err)
		}
		ikeys[i] = db.MakeInternalKey([]byte(k[:j]), uint64(seqnum), db.InternalKeyKindSet)
	}
	return &fakeIter{
		keys:     ikeys,
		index:    0,
		closeErr: closeErr,
	}
}

func (f *fakeIter) SeekGE(key *db.InternalKey) {
	for f.index = 0; f.index < len(f.keys); f.index++ {
		if db.InternalCompare(db.DefaultComparer.Compare, *key, *f.Key()) <= 0 {
			break
		}
	}
}

func (f *fakeIter) SeekLE(key *db.InternalKey) {
	for f.index = len(f.keys) - 1; f.index >= 0; f.index-- {
		if db.InternalCompare(db.DefaultComparer.Compare, *key, *f.Key()) >= 0 {
			break
		}
	}
}

func (f *fakeIter) First() {
	f.index = 0
}

func (f *fakeIter) Last() {
	f.index = len(f.keys) - 1
}

func (f *fakeIter) Next() bool {
	f.index++
	return f.index < len(f.keys)
}

func (f *fakeIter) Prev() bool {
	f.index--
	return f.index >= 0
}

func (f *fakeIter) Key() *db.InternalKey {
	return &f.keys[f.index]
}

func (f *fakeIter) Value() []byte {
	return nil
}

func (f *fakeIter) Valid() bool {
	return f.index >= 0 && f.index < len(f.keys)
}

func (f *fakeIter) Error() error {
	return f.closeErr
}

func (f *fakeIter) Close() error {
	return f.closeErr
}

// testIterator tests creating a combined iterator from a number of sub-
// iterators. newFunc is a constructor function. splitFunc returns a random
// split of the testKeyValuePairs slice such that walking a combined iterator
// over those splits should recover the original key/value pairs in order.
func testIterator(
	t *testing.T,
	newFunc func(...db.InternalIterator) db.InternalIterator,
	splitFunc func(r *rand.Rand) [][]string,
) {
	// Test pre-determined sub-iterators. The sub-iterators are designed
	// so that the combined key/value pair order is the same whether the
	// combined iterator is concatenating or merging.
	testCases := []struct {
		desc  string
		iters []db.InternalIterator
		want  string
	}{
		{
			"one sub-iterator",
			[]db.InternalIterator{
				newFakeIterator(nil, "e:1", "w:2"),
			},
			"<e:1><w:2>.",
		},
		{
			"two sub-iterators",
			[]db.InternalIterator{
				newFakeIterator(nil, "a0:0"),
				newFakeIterator(nil, "b1:1", "b2:2"),
			},
			"<a0:0><b1:1><b2:2>.",
		},
		{
			"empty sub-iterators",
			[]db.InternalIterator{
				newFakeIterator(nil),
				newFakeIterator(nil),
				newFakeIterator(nil),
			},
			".",
		},
		{
			"sub-iterator errors",
			[]db.InternalIterator{
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
		for ; iter.Valid(); iter.Next() {
			fmt.Fprintf(&b, "<%s:%d>", iter.Key().UserKey, iter.Key().Seqnum())
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
		iters := make([]db.InternalIterator, len(splits))
		for i, split := range splits {
			iters[i] = newFakeIterator(nil, split...)
		}
		iter := newFunc(iters...)

		j := 0
		for ; iter.Valid() && j < len(testKeyValuePairs); j++ {
			got := fmt.Sprintf("%s:%d", iter.Key().UserKey, iter.Key().Seqnum())
			want := testKeyValuePairs[j]
			if got != want {
				bad = true
				t.Errorf("random splits: i=%d, j=%d: got %q, want %q", i, j, got, want)
			}
			iter.Next()
		}
		if iter.Valid() {
			bad = true
			t.Errorf("random splits: i=%d, j=%d: iter was not exhausted", i, j)
		}
		if j != len(testKeyValuePairs) {
			bad = true
			t.Errorf("random splits: i=%d, j=%d: want j=%d", i, j, len(testKeyValuePairs))
			fmt.Printf("splits: %v\n", splits)
			return
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
	testIterator(t, newConcatenatingIterator, func(r *rand.Rand) [][]string {
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
	newFunc := func(iters ...db.InternalIterator) db.InternalIterator {
		return newMergingIterator(db.DefaultComparer.Compare, iters...)
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

func TestMergingIteratorSeek(t *testing.T) {
	testCases := []struct {
		key          string
		iters        string
		expectedNext string
		expectedPrev string
	}{
		{
			"a0.SET.3",
			"a0:0;a1:1;a2:2",
			"<a0:0><a1:1><a2:2>.",
			".",
		},
		{
			"a1.SET.3",
			"a0:0;a1:1;a2:2",
			"<a1:1><a2:2>.",
			"<a0:0>.",
		},
		{
			"a2.SET.3",
			"a0:0;a1:1;a2:2",
			"<a2:2>.",
			"<a1:1><a0:0>.",
		},
		{
			"a3.SET.3",
			"a0:0;a1:1;a2:2",
			".",
			"<a2:2><a1:1><a0:0>.",
		},
		{
			"a2.SET.3",
			"a0:0,b3:3;a1:1;a2:2",
			"<a2:2><b3:3>.",
			"<a1:1><a0:0>.",
		},
		{
			"a.SET.2",
			"a:0;a:1;a:2",
			"<a:2><a:1><a:0>.",
			"<a:2>.",
		},
		{
			"a.SET.1",
			"a:0;a:1;a:2",
			"<a:1><a:0>.",
			"<a:1><a:2>.",
		},
		{
			"a.SET.0",
			"a:0;a:1;a:2",
			"<a:0>.",
			"<a:0><a:1><a:2>.",
		},
	}
	for _, tc := range testCases {
		t.Run("", func(t *testing.T) {
			var iters []db.InternalIterator
			for _, s := range strings.Split(tc.iters, ";") {
				iters = append(iters, newFakeIterator(nil, strings.Split(s, ",")...))
			}

			var b bytes.Buffer
			iter := newMergingIterator(db.DefaultComparer.Compare, iters...)
			ikey := makeIkey(tc.key)
			iter.SeekGE(&ikey)
			for ; iter.Valid(); iter.Next() {
				fmt.Fprintf(&b, "<%s:%d>", iter.Key().UserKey, iter.Key().Seqnum())
			}
			if err := iter.Error(); err != nil {
				fmt.Fprintf(&b, "err=%v", err)
			} else {
				b.WriteByte('.')
			}
			if got := b.String(); got != tc.expectedNext {
				t.Errorf("got  %q\nwant %q", got, tc.expectedNext)
			}

			b.Reset()
			iter.SeekLE(&ikey)
			for ; iter.Valid(); iter.Prev() {
				fmt.Fprintf(&b, "<%s:%d>", iter.Key().UserKey, iter.Key().Seqnum())
			}
			if err := iter.Close(); err != nil {
				fmt.Fprintf(&b, "err=%v", err)
			} else {
				b.WriteByte('.')
			}
			if got := b.String(); got != tc.expectedPrev {
				t.Errorf("got  %q\nwant %q", got, tc.expectedPrev)
			}
		})
	}
}

func TestMergingIteratorNextPrev(t *testing.T) {
	// The data is the same in each of these cases, but divided up amongst the
	// iterators differently.
	iterCases := [][]db.InternalIterator{
		[]db.InternalIterator{
			newFakeIterator(nil, "a:2", "a:1", "b:2", "b:1", "c:2", "c:1"),
		},
		[]db.InternalIterator{
			newFakeIterator(nil, "a:2", "b:2", "c:2"),
			newFakeIterator(nil, "a:1", "b:1", "c:1"),
		},
		[]db.InternalIterator{
			newFakeIterator(nil, "a:2", "b:2"),
			newFakeIterator(nil, "a:1", "b:1"),
			newFakeIterator(nil, "c:2", "c:1"),
		},
		[]db.InternalIterator{
			newFakeIterator(nil, "a:2"),
			newFakeIterator(nil, "a:1"),
			newFakeIterator(nil, "b:2"),
			newFakeIterator(nil, "b:1"),
			newFakeIterator(nil, "c:2"),
			newFakeIterator(nil, "c:1"),
		},
	}
	for _, iters := range iterCases {
		t.Run("", func(t *testing.T) {
			m := newMergingIterator(db.DefaultComparer.Compare, iters...)
			m.First()

			testCases := []struct {
				dir      string
				expected string
			}{
				{"+", "<a:1>"},
				{"+", "<b:2>"},
				{"-", "<a:1>"},
				{"-", "<a:2>"},
				{"-", "."},
				{"+", "<a:2>"},
				{"+", "<a:1>"},
				{"+", "<b:2>"},
				{"+", "<b:1>"},
				{"+", "<c:2>"},
				{"+", "<c:1>"},
				{"-", "<c:2>"},
				{"-", "<b:1>"},
				{"-", "<b:2>"},
				{"+", "<b:1>"},
				{"+", "<c:2>"},
				{"-", "<b:1>"},
				{"+", "<c:2>"},
				{"+", "<c:1>"},
				{"+", "."},
				{"-", "<c:1>"},
			}
			for i, c := range testCases {
				switch c.dir {
				case "+":
					m.Next()
				case "-":
					m.Prev()
				default:
					t.Fatalf("unexpected direction: %q", c.dir)
				}
				var got string
				if !m.Valid() {
					got = "."
				} else {
					got = fmt.Sprintf("<%s:%d>", m.Key().UserKey, m.Key().Seqnum())
				}
				if got != c.expected {
					t.Fatalf("%d: got  %q\nwant %q", i, got, c.expected)
				}
			}
		})
	}
}
