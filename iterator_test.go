// Copyright 2013 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package pebble

import (
	"bytes"
	"errors"
	"fmt"
	"math/rand"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/petermattis/pebble/db"
	"github.com/petermattis/pebble/internal/datadriven"
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
	vals     [][]byte
	index    int
	closeErr error
}

func fakeIkey(s string) db.InternalKey {
	j := strings.Index(s, ":")
	seqNum, err := strconv.Atoi(s[j+1:])
	if err != nil {
		panic(err)
	}
	return db.MakeInternalKey([]byte(s[:j]), uint64(seqNum), db.InternalKeyKindSet)
}

func newFakeIterator(closeErr error, keys ...string) *fakeIter {
	ikeys := make([]db.InternalKey, len(keys))
	for i, k := range keys {
		ikeys[i] = fakeIkey(k)
	}
	return &fakeIter{
		keys:     ikeys,
		index:    0,
		closeErr: closeErr,
	}
}

func (f *fakeIter) SeekGE(key []byte) bool {
	for f.index = 0; f.index < len(f.keys); f.index++ {
		if db.DefaultComparer.Compare(key, f.Key().UserKey) <= 0 {
			return true
		}
	}
	return false
}

func (f *fakeIter) SeekLT(key []byte) bool {
	for f.index = len(f.keys) - 1; f.index >= 0; f.index-- {
		if db.DefaultComparer.Compare(key, f.Key().UserKey) > 0 {
			return true
		}
	}
	return false
}

func (f *fakeIter) First() bool {
	f.index = -1
	return f.Next()
}

func (f *fakeIter) Last() bool {
	f.index = len(f.keys)
	return f.Prev()
}

func (f *fakeIter) Next() bool {
	if f.index == len(f.keys) {
		return false
	}
	f.index++
	return f.index < len(f.keys)
}

func (f *fakeIter) Prev() bool {
	if f.index < 0 {
		return false
	}
	f.index--
	return f.index >= 0
}

func (f *fakeIter) Key() db.InternalKey {
	return f.keys[f.index]
}

func (f *fakeIter) Value() []byte {
	if f.index >= 0 && f.index < len(f.vals) {
		return f.vals[f.index]
	}
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
	newFunc func(...internalIterator) internalIterator,
	splitFunc func(r *rand.Rand) [][]string,
) {
	// Test pre-determined sub-iterators. The sub-iterators are designed
	// so that the combined key/value pair order is the same whether the
	// combined iterator is concatenating or merging.
	testCases := []struct {
		desc  string
		iters []internalIterator
		want  string
	}{
		{
			"one sub-iterator",
			[]internalIterator{
				newFakeIterator(nil, "e:1", "w:2"),
			},
			"<e:1><w:2>.",
		},
		{
			"two sub-iterators",
			[]internalIterator{
				newFakeIterator(nil, "a0:0"),
				newFakeIterator(nil, "b1:1", "b2:2"),
			},
			"<a0:0><b1:1><b2:2>.",
		},
		{
			"empty sub-iterators",
			[]internalIterator{
				newFakeIterator(nil),
				newFakeIterator(nil),
				newFakeIterator(nil),
			},
			".",
		},
		{
			"sub-iterator errors",
			[]internalIterator{
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
		for valid := iter.First(); valid; valid = iter.Next() {
			fmt.Fprintf(&b, "<%s:%d>", iter.Key().UserKey, iter.Key().SeqNum())
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
		iters := make([]internalIterator, len(splits))
		for i, split := range splits {
			iters[i] = newFakeIterator(nil, split...)
		}
		iter := newFunc(iters...)

		j := 0
		for ; iter.Valid() && j < len(testKeyValuePairs); j++ {
			got := fmt.Sprintf("%s:%d", iter.Key().UserKey, iter.Key().SeqNum())
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

func TestIterator(t *testing.T) {
	var keys []db.InternalKey
	var vals [][]byte

	newIter := func(seqNum uint64, opts *db.IterOptions) *Iterator {
		cmp := db.DefaultComparer
		equal := db.DefaultComparer.Equal
		// NB: Use a mergingIter to filter entries newer than seqNum.
		iter := newMergingIter(cmp, &fakeIter{keys: keys, vals: vals})
		iter.snapshot = seqNum
		return &Iterator{
			opts:  opts,
			cmp:   cmp.Compare,
			equal: equal,
			merge: db.DefaultMerger.Merge,
			iter:  iter,
		}
	}

	datadriven.RunTest(t, "testdata/iterator", func(d *datadriven.TestData) string {
		switch d.Cmd {
		case "define":
			keys = keys[:0]
			vals = vals[:0]
			for _, key := range strings.Split(d.Input, "\n") {
				j := strings.Index(key, ":")
				keys = append(keys, db.ParseInternalKey(key[:j]))
				vals = append(vals, []byte(key[j+1:]))
			}
			return ""

		case "iter":
			var seqNum int
			var opts db.IterOptions

			for _, arg := range d.CmdArgs {
				if len(arg.Vals) != 1 {
					return fmt.Sprintf("%s: %s=<value>", d.Cmd, arg.Key)
				}
				switch arg.Key {
				case "seq":
					var err error
					seqNum, err = strconv.Atoi(arg.Vals[0])
					if err != nil {
						return err.Error()
					}
				case "lower":
					opts.LowerBound = []byte(arg.Vals[0])
				case "upper":
					opts.UpperBound = []byte(arg.Vals[0])
				default:
					return fmt.Sprintf("%s: unknown arg: %s", d.Cmd, arg.Key)
				}
			}

			iter := newIter(uint64(seqNum), &opts)
			defer iter.Close()
			return runIterCmd(d, iter)

		default:
			return fmt.Sprintf("unknown command: %s", d.Cmd)
		}
	})
}

func BenchmarkIteratorSeekGE(b *testing.B) {
	m, keys := buildMemTable(b)
	iter := &Iterator{
		cmp:   db.DefaultComparer.Compare,
		equal: db.DefaultComparer.Equal,
		iter:  m.newIter(nil),
	}
	rng := rand.New(rand.NewSource(time.Now().UnixNano()))

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		key := keys[rng.Intn(len(keys))]
		iter.SeekGE(key)
	}
}

func BenchmarkIteratorNext(b *testing.B) {
	m, _ := buildMemTable(b)
	iter := &Iterator{
		cmp:   db.DefaultComparer.Compare,
		equal: db.DefaultComparer.Equal,
		iter:  m.newIter(nil),
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if !iter.Valid() {
			iter.First()
		}
		iter.Next()
	}
}

func BenchmarkIteratorPrev(b *testing.B) {
	m, _ := buildMemTable(b)
	iter := &Iterator{
		cmp:   db.DefaultComparer.Compare,
		equal: db.DefaultComparer.Equal,
		iter:  m.newIter(nil),
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if !iter.Valid() {
			iter.Last()
		}
		iter.Prev()
	}
}
