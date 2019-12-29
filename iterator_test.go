// Copyright 2013 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package pebble

import (
	"bytes"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/cockroachdb/pebble/internal/base"
	"github.com/cockroachdb/pebble/internal/datadriven"
	"github.com/cockroachdb/pebble/vfs"
	"golang.org/x/exp/rand"
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
	lower    []byte
	upper    []byte
	keys     []InternalKey
	vals     [][]byte
	index    int
	valid    bool
	closeErr error
}

func fakeIkey(s string) InternalKey {
	j := strings.Index(s, ":")
	seqNum, err := strconv.Atoi(s[j+1:])
	if err != nil {
		panic(err)
	}
	return base.MakeInternalKey([]byte(s[:j]), uint64(seqNum), InternalKeyKindSet)
}

func newFakeIterator(closeErr error, keys ...string) *fakeIter {
	ikeys := make([]InternalKey, len(keys))
	for i, k := range keys {
		ikeys[i] = fakeIkey(k)
	}
	return &fakeIter{
		keys:     ikeys,
		index:    0,
		valid:    len(ikeys) > 0,
		closeErr: closeErr,
	}
}

func (f *fakeIter) SeekGE(key []byte) (*InternalKey, []byte) {
	f.valid = false
	for f.index = 0; f.index < len(f.keys); f.index++ {
		if DefaultComparer.Compare(key, f.key().UserKey) <= 0 {
			if f.upper != nil && DefaultComparer.Compare(f.upper, f.key().UserKey) <= 0 {
				return nil, nil
			}
			f.valid = true
			return f.Key(), f.Value()
		}
	}
	return nil, nil
}

func (f *fakeIter) SeekPrefixGE(prefix, key []byte) (*InternalKey, []byte) {
	return f.SeekGE(key)
}

func (f *fakeIter) SeekLT(key []byte) (*InternalKey, []byte) {
	f.valid = false
	for f.index = len(f.keys) - 1; f.index >= 0; f.index-- {
		if DefaultComparer.Compare(key, f.key().UserKey) > 0 {
			if f.lower != nil && DefaultComparer.Compare(f.lower, f.key().UserKey) > 0 {
				return nil, nil
			}
			f.valid = true
			return f.Key(), f.Value()
		}
	}
	return nil, nil
}

func (f *fakeIter) First() (*InternalKey, []byte) {
	f.valid = false
	f.index = -1
	if key, _ := f.Next(); key == nil {
		return nil, nil
	}
	if f.upper != nil && DefaultComparer.Compare(f.upper, f.key().UserKey) <= 0 {
		return nil, nil
	}
	f.valid = true
	return f.Key(), f.Value()
}

func (f *fakeIter) Last() (*InternalKey, []byte) {
	f.valid = false
	f.index = len(f.keys)
	if key, _ := f.Prev(); key == nil {
		return nil, nil
	}
	if f.lower != nil && DefaultComparer.Compare(f.lower, f.key().UserKey) > 0 {
		return nil, nil
	}
	f.valid = true
	return f.Key(), f.Value()
}

func (f *fakeIter) Next() (*InternalKey, []byte) {
	f.valid = false
	if f.index == len(f.keys) {
		return nil, nil
	}
	f.index++
	if f.index == len(f.keys) {
		return nil, nil
	}
	if f.upper != nil && DefaultComparer.Compare(f.upper, f.key().UserKey) <= 0 {
		return nil, nil
	}
	f.valid = true
	return f.Key(), f.Value()
}

func (f *fakeIter) Prev() (*InternalKey, []byte) {
	f.valid = false
	if f.index < 0 {
		return nil, nil
	}
	f.index--
	if f.index < 0 {
		return nil, nil
	}
	if f.lower != nil && DefaultComparer.Compare(f.lower, f.key().UserKey) > 0 {
		return nil, nil
	}
	f.valid = true
	return f.Key(), f.Value()
}

// key returns the current Key the iterator is positioned at regardless of the
// value of f.valid.
func (f *fakeIter) key() *InternalKey {
	return &f.keys[f.index]
}

func (f *fakeIter) Key() *InternalKey {
	if f.valid {
		return &f.keys[f.index]
	}
	// It is invalid to call Key() when Valid() returns false. Rather than
	// returning nil here which would technically be more correct, return a
	// non-nil key which is the behavior of some InternalIterator
	// implementations. This provides better testing of users of
	// InternalIterators.
	if f.index < 0 {
		return &f.keys[0]
	}
	return &f.keys[len(f.keys)-1]
}

func (f *fakeIter) Value() []byte {
	if f.index >= 0 && f.index < len(f.vals) {
		return f.vals[f.index]
	}
	return nil
}

func (f *fakeIter) Valid() bool {
	return f.index >= 0 && f.index < len(f.keys) && f.valid
}

func (f *fakeIter) Error() error {
	return f.closeErr
}

func (f *fakeIter) Close() error {
	return f.closeErr
}

func (f *fakeIter) SetBounds(lower, upper []byte) {
	f.lower = lower
	f.upper = upper
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
				newFakeIterator(errors.New("the sky is falling"), "b2:2", "b3:3", "b4:4"),
				newFakeIterator(errors.New("run for your lives"), "c5:5", "c6:6"),
			},
			"<a0:0><a1:1><b2:2><b3:3><b4:4>err=the sky is falling",
		},
	}
	for _, tc := range testCases {
		var b bytes.Buffer
		iter := newFunc(tc.iters...)
		for key, _ := iter.First(); key != nil; key, _ = iter.Next() {
			fmt.Fprintf(&b, "<%s:%d>", key.UserKey, key.SeqNum())
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
		iter := newInternalIterAdapter(newFunc(iters...))
		iter.First()

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
	var keys []InternalKey
	var vals [][]byte

	newIter := func(seqNum uint64, opts IterOptions) *Iterator {
		cmp := DefaultComparer.Compare
		equal := DefaultComparer.Equal
		split := func(a []byte) int { return len(a) }
		// NB: Use a mergingIter to filter entries newer than seqNum.
		iter := newMergingIter(nil /* logger */, cmp, &fakeIter{
			lower: opts.GetLowerBound(),
			upper: opts.GetUpperBound(),
			keys:  keys,
			vals:  vals,
		})
		iter.snapshot = seqNum
		return &Iterator{
			opts:  opts,
			cmp:   cmp,
			equal: equal,
			split: split,
			merge: DefaultMerger.Merge,
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
				keys = append(keys, base.ParseInternalKey(key[:j]))
				vals = append(vals, []byte(key[j+1:]))
			}
			return ""

		case "iter":
			var seqNum int
			var opts IterOptions

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

			iter := newIter(uint64(seqNum), opts)
			defer iter.Close()
			return runIterCmd(d, iter)

		default:
			return fmt.Sprintf("unknown command: %s", d.Cmd)
		}
	})
}

type minSeqNumPropertyCollector struct {
	minSeqNum uint64
}

func (c *minSeqNumPropertyCollector) Add(key InternalKey, value []byte) error {
	if c.minSeqNum == 0 || c.minSeqNum > key.SeqNum() {
		c.minSeqNum = key.SeqNum()
	}
	return nil
}

func (c *minSeqNumPropertyCollector) Finish(userProps map[string]string) error {
	userProps["test.min-seq-num"] = fmt.Sprint(c.minSeqNum)
	return nil
}

func (c *minSeqNumPropertyCollector) Name() string {
	return "minSeqNumPropertyCollector"
}

func TestIteratorTableFilter(t *testing.T) {
	var d *DB

	datadriven.RunTest(t, "testdata/iterator_table_filter", func(td *datadriven.TestData) string {
		switch td.Cmd {
		case "define":
			opts := &Options{}
			opts.TablePropertyCollectors = append(opts.TablePropertyCollectors,
				func() TablePropertyCollector {
					return &minSeqNumPropertyCollector{}
				})

			var err error
			if d, err = runDBDefineCmd(td, opts); err != nil {
				return err.Error()
			}

			d.mu.Lock()
			// Disable the "dynamic base level" code for this test.
			d.mu.versions.picker.baseLevel = 1
			s := d.mu.versions.currentVersion().DebugString(base.DefaultFormatter)
			d.mu.Unlock()
			return s

		case "iter":
			// We're using an iterator table filter to approximate what is done by
			// snapshots.
			iterOpts := &IterOptions{}
			for _, arg := range td.CmdArgs {
				if len(arg.Vals) != 1 {
					return fmt.Sprintf("%s: %s=<value>", td.Cmd, arg.Key)
				}
				switch arg.Key {
				case "filter":
					seqNum, err := strconv.ParseUint(arg.Vals[0], 10, 64)
					if err != nil {
						return err.Error()
					}
					iterOpts.TableFilter = func(userProps map[string]string) bool {
						minSeqNum, err := strconv.ParseUint(userProps["test.min-seq-num"], 10, 64)
						if err != nil {
							return true
						}
						return minSeqNum < seqNum
					}
				default:
					return fmt.Sprintf("%s: unknown arg: %s", td.Cmd, arg.Key)
				}
			}

			// TODO(peter): runDBDefineCmd doesn't properly update the visible
			// sequence number. So we have to use a snapshot with a very large
			// sequence number, otherwise the DB appears empty.
			snap := Snapshot{
				db:     d,
				seqNum: InternalKeySeqNumMax,
			}
			iter := snap.NewIter(iterOpts)
			defer iter.Close()
			return runIterCmd(td, iter)

		default:
			return fmt.Sprintf("unknown command: %s", td.Cmd)
		}
	})
}

func TestIteratorNextPrev(t *testing.T) {
	var mem vfs.FS
	var d *DB

	reset := func() {
		mem = vfs.NewMem()
		err := mem.MkdirAll("ext", 0755)
		if err != nil {
			t.Fatal(err)
		}
		d, err = Open("", &Options{
			FS: mem,
		})
		if err != nil {
			t.Fatal(err)
		}
	}
	reset()

	datadriven.RunTest(t, "testdata/iterator_next_prev", func(td *datadriven.TestData) string {
		switch td.Cmd {
		case "reset":
			reset()
			return ""

		case "build":
			if err := runBuildCmd(td, d, mem); err != nil {
				return err.Error()
			}
			return ""

		case "ingest":
			if err := runIngestCmd(td, d, mem); err != nil {
				return err.Error()
			}
			return runLSMCmd(td, d)

		case "iter":
			seqNum := InternalKeySeqNumMax
			for _, arg := range td.CmdArgs {
				if len(arg.Vals) != 1 {
					return fmt.Sprintf("%s: %s=<value>", td.Cmd, arg.Key)
				}
				switch arg.Key {
				case "seq":
					var err error
					seqNum, err = strconv.ParseUint(arg.Vals[0], 10, 64)
					if err != nil {
						return err.Error()
					}
				default:
					return fmt.Sprintf("%s: unknown arg: %s", td.Cmd, arg.Key)
				}
			}

			snap := Snapshot{
				db:     d,
				seqNum: seqNum,
			}
			iter := snap.NewIter(nil)
			defer iter.Close()
			return runIterCmd(td, iter)

		default:
			return fmt.Sprintf("unknown command: %s", td.Cmd)
		}
	})
}

func BenchmarkIteratorSeekGE(b *testing.B) {
	m, keys := buildMemTable(b)
	iter := &Iterator{
		cmp:   DefaultComparer.Compare,
		equal: DefaultComparer.Equal,
		iter:  m.newIter(nil),
	}
	rng := rand.New(rand.NewSource(uint64(time.Now().UnixNano())))

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		key := keys[rng.Intn(len(keys))]
		iter.SeekGE(key)
	}
}

func BenchmarkIteratorNext(b *testing.B) {
	m, _ := buildMemTable(b)
	iter := &Iterator{
		cmp:   DefaultComparer.Compare,
		equal: DefaultComparer.Equal,
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
		cmp:   DefaultComparer.Compare,
		equal: DefaultComparer.Equal,
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
