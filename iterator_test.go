// Copyright 2013 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package pebble

import (
	"bytes"
	"flag"
	"fmt"
	"io"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/pebble/internal/keyspan"
	"github.com/stretchr/testify/require"
	"golang.org/x/exp/rand"

	"github.com/cockroachdb/pebble/internal/base"
	"github.com/cockroachdb/pebble/internal/datadriven"
	"github.com/cockroachdb/pebble/internal/manifest"
	"github.com/cockroachdb/pebble/sstable"
	"github.com/cockroachdb/pebble/vfs"
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

// fakeIter implements the base.InternalIterator interface.
var _ base.InternalIterator = (*fakeIter)(nil)

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

func (f *fakeIter) String() string {
	return "fake"
}

func (f *fakeIter) SeekGE(key []byte, trySeekUsingNext bool) (*InternalKey, []byte) {
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

func (f *fakeIter) SeekPrefixGE(
	prefix, key []byte, trySeekUsingNext bool,
) (*base.InternalKey, []byte) {
	return f.SeekGE(key, trySeekUsingNext)
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

// invalidatingIter tests unsafe key/value slice reuse by modifying the last
// returned key/value to all 1s.
type invalidatingIter struct {
	iter        internalIterator
	lastKey     *InternalKey
	lastValue   []byte
	ignoreKinds [base.InternalKeyKindMax + 1]bool
}

func newInvalidatingIter(iter internalIterator) *invalidatingIter {
	return &invalidatingIter{iter: iter}
}

func (i *invalidatingIter) ignoreKind(kind base.InternalKeyKind) {
	i.ignoreKinds[kind] = true
}

func (i *invalidatingIter) update(key *InternalKey, value []byte) (*InternalKey, []byte) {
	i.zeroLast()

	if key == nil {
		i.lastKey = nil
		i.lastValue = nil
		return nil, nil
	}

	i.lastKey = &InternalKey{}
	*i.lastKey = key.Clone()
	i.lastValue = make([]byte, len(value))
	copy(i.lastValue, value)
	return i.lastKey, i.lastValue
}

func (i *invalidatingIter) zeroLast() {
	if i.lastKey == nil {
		return
	}
	if i.ignoreKinds[i.lastKey.Kind()] {
		return
	}

	if i.lastKey != nil {
		for j := range i.lastKey.UserKey {
			i.lastKey.UserKey[j] = 0xff
		}
		i.lastKey.Trailer = 0
	}
	for j := range i.lastValue {
		i.lastValue[j] = 0xff
	}
}

func (i *invalidatingIter) SeekGE(key []byte, trySeekUsingNext bool) (*InternalKey, []byte) {
	return i.update(i.iter.SeekGE(key, trySeekUsingNext))
}

func (i *invalidatingIter) SeekPrefixGE(
	prefix, key []byte, trySeekUsingNext bool,
) (*base.InternalKey, []byte) {
	return i.update(i.iter.SeekPrefixGE(prefix, key, trySeekUsingNext))
}

func (i *invalidatingIter) SeekLT(key []byte) (*InternalKey, []byte) {
	return i.update(i.iter.SeekLT(key))
}

func (i *invalidatingIter) First() (*InternalKey, []byte) {
	return i.update(i.iter.First())
}

func (i *invalidatingIter) Last() (*InternalKey, []byte) {
	return i.update(i.iter.Last())
}

func (i *invalidatingIter) Next() (*InternalKey, []byte) {
	return i.update(i.iter.Next())
}

func (i *invalidatingIter) Prev() (*InternalKey, []byte) {
	return i.update(i.iter.Prev())
}

func (i *invalidatingIter) Error() error {
	return i.iter.Error()
}

func (i *invalidatingIter) Close() error {
	return i.iter.Close()
}

func (i *invalidatingIter) SetBounds(lower, upper []byte) {
	i.iter.SetBounds(lower, upper)
}

func (i *invalidatingIter) String() string {
	return i.iter.String()
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
		iter := newInvalidatingIter(newFunc(tc.iters...))
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
		iter := newInternalIterAdapter(newInvalidatingIter(newFunc(iters...)))
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

// deletableSumValueMerger computes the sum of its arguments,
// but transforms a zero sum into a non-existent entry.
type deletableSumValueMerger struct {
	sum int64
}

func newDeletableSumValueMerger(key, value []byte) (ValueMerger, error) {
	m := &deletableSumValueMerger{}
	return m, m.MergeNewer(value)
}

func (m *deletableSumValueMerger) parseAndCalculate(value []byte) error {
	v, err := strconv.ParseInt(string(value), 10, 64)
	if err == nil {
		m.sum += v
	}
	return err
}

func (m *deletableSumValueMerger) MergeNewer(value []byte) error {
	return m.parseAndCalculate(value)
}

func (m *deletableSumValueMerger) MergeOlder(value []byte) error {
	return m.parseAndCalculate(value)
}

func (m *deletableSumValueMerger) Finish(includesBase bool) ([]byte, io.Closer, error) {
	if m.sum == 0 {
		return nil, nil, nil
	}
	return []byte(strconv.FormatInt(m.sum, 10)), nil, nil
}

func (m *deletableSumValueMerger) DeletableFinish(includesBase bool) ([]byte, bool, io.Closer, error) {
	value, closer, err := m.Finish(includesBase)
	return value, len(value) == 0, closer, err
}

func TestIterator(t *testing.T) {
	var merge Merge
	var keys []InternalKey
	var vals [][]byte

	newIter := func(seqNum uint64, opts IterOptions) *Iterator {
		cmp := DefaultComparer.Compare
		equal := DefaultComparer.Equal
		split := func(a []byte) int { return len(a) }
		// NB: Use a mergingIter to filter entries newer than seqNum.
		iter := newMergingIter(nil /* logger */, cmp, split, &fakeIter{
			lower: opts.GetLowerBound(),
			upper: opts.GetUpperBound(),
			keys:  keys,
			vals:  vals,
		})
		iter.snapshot = seqNum
		iter.elideRangeTombstones = true
		// NB: This Iterator cannot be cloned since it is not constructed
		// with a readState. It suffices for this test.
		if merge == nil {
			merge = DefaultMerger.Merge
		}
		wrappedMerge := func(key, value []byte) (ValueMerger, error) {
			if len(key) == 0 {
				t.Fatalf("an empty key is passed into Merge")
			}
			return merge(key, value)
		}
		return &Iterator{
			opts:  opts,
			cmp:   cmp,
			equal: equal,
			split: split,
			merge: wrappedMerge,
			iter:  newInvalidatingIter(iter),
		}
	}

	datadriven.RunTest(t, "testdata/iterator", func(d *datadriven.TestData) string {
		switch d.Cmd {
		case "define":
			merge = nil
			if len(d.CmdArgs) > 0 && d.CmdArgs[0].Key == "merger" &&
				len(d.CmdArgs[0].Vals) > 0 && d.CmdArgs[0].Vals[0] == "deletable" {
				merge = newDeletableSumValueMerger
			}
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
			iterOutput := runIterCmd(d, iter, true)
			stats := iter.Stats()
			return fmt.Sprintf("%sstats: %s\n", iterOutput, stats.String())

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

func TestReadSampling(t *testing.T) {
	var d *DB
	defer func() {
		if d != nil {
			require.NoError(t, d.Close())
		}
	}()

	var iter *Iterator
	defer func() {
		if iter != nil {
			require.NoError(t, iter.Close())
		}
	}()

	datadriven.RunTest(t, "testdata/iterator_read_sampling", func(td *datadriven.TestData) string {
		switch td.Cmd {
		case "define":
			if iter != nil {
				if err := iter.Close(); err != nil {
					return err.Error()
				}
			}
			if d != nil {
				if err := d.Close(); err != nil {
					return err.Error()
				}
			}

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
			// d.mu.versions.picker.forceBaseLevel1()
			s := d.mu.versions.currentVersion().DebugString(base.DefaultFormatter)
			d.mu.Unlock()
			return s

		case "set":
			if d == nil {
				return fmt.Sprintf("%s: db is not defined", td.Cmd)
			}

			var allowedSeeks int64

			for _, arg := range td.CmdArgs {
				if len(arg.Vals) != 1 {
					return fmt.Sprintf("%s: %s=<value>", td.Cmd, arg.Key)
				}
				switch arg.Key {
				case "allowed-seeks":
					var err error
					allowedSeeks, err = strconv.ParseInt(arg.Vals[0], 10, 64)
					if err != nil {
						return err.Error()
					}
				}
			}

			d.mu.Lock()
			for _, l := range d.mu.versions.currentVersion().Levels {
				l.Slice().Each(func(f *fileMetadata) {
					atomic.StoreInt64(&f.Atomic.AllowedSeeks, allowedSeeks)
				})
			}
			d.mu.Unlock()
			return ""

		case "show":
			if d == nil {
				return fmt.Sprintf("%s: db is not defined", td.Cmd)
			}

			var fileNum int64
			for _, arg := range td.CmdArgs {
				if len(arg.Vals) != 2 {
					return fmt.Sprintf("%s: %s=<value>", td.Cmd, arg.Key)
				}
				switch arg.Key {
				case "allowed-seeks":
					var err error
					fileNum, err = strconv.ParseInt(arg.Vals[0], 10, 64)
					if err != nil {
						return err.Error()
					}
				}
			}

			var foundAllowedSeeks int64 = -1
			d.mu.Lock()
			for _, l := range d.mu.versions.currentVersion().Levels {
				l.Slice().Each(func(f *fileMetadata) {
					if f.FileNum == base.FileNum(fileNum) {
						actualAllowedSeeks := atomic.LoadInt64(&f.Atomic.AllowedSeeks)
						foundAllowedSeeks = actualAllowedSeeks
					}
				})
			}
			d.mu.Unlock()

			if foundAllowedSeeks == -1 {
				return fmt.Sprintf("invalid file num: %d", fileNum)
			}
			return fmt.Sprintf("%d", foundAllowedSeeks)

		case "iter":
			if iter == nil || iter.iter == nil {
				// TODO(peter): runDBDefineCmd doesn't properly update the visible
				// sequence number. So we have to use a snapshot with a very large
				// sequence number, otherwise the DB appears empty.
				snap := Snapshot{
					db:     d,
					seqNum: InternalKeySeqNumMax,
				}
				iter = snap.NewIter(nil)
				iter.readSampling.forceReadSampling = true
			}
			return runIterCmd(td, iter, false)

		case "read-compactions":
			if d == nil {
				return fmt.Sprintf("%s: db is not defined", td.Cmd)
			}

			d.mu.Lock()
			var sb strings.Builder
			if d.mu.compact.readCompactions.size == 0 {
				sb.WriteString("(none)")
			}
			for i := 0; i < d.mu.compact.readCompactions.size; i++ {
				rc := d.mu.compact.readCompactions.at(i)
				sb.WriteString(fmt.Sprintf("(level: %d, start: %s, end: %s)\n", rc.level, string(rc.start), string(rc.end)))
			}
			d.mu.Unlock()
			return sb.String()

		case "iter-read-compactions":
			if iter == nil {
				return fmt.Sprintf("%s: iter is not defined", td.Cmd)
			}

			var sb strings.Builder
			if iter.readSampling.pendingCompactions.size == 0 {
				sb.WriteString("(none)")
			}
			for i := 0; i < iter.readSampling.pendingCompactions.size; i++ {
				rc := iter.readSampling.pendingCompactions.at(i)
				sb.WriteString(fmt.Sprintf("(level: %d, start: %s, end: %s)\n", rc.level, string(rc.start), string(rc.end)))
			}
			return sb.String()

		case "close-iter":
			if iter != nil {
				if err := iter.Close(); err != nil {
					return err.Error()
				}
			}
			return ""

		default:
			return fmt.Sprintf("unknown command: %s", td.Cmd)
		}
	})
}

func TestIteratorTableFilter(t *testing.T) {
	var d *DB
	defer func() {
		if d != nil {
			require.NoError(t, d.Close())
		}
	}()

	datadriven.RunTest(t, "testdata/iterator_table_filter", func(td *datadriven.TestData) string {
		switch td.Cmd {
		case "define":
			if d != nil {
				if err := d.Close(); err != nil {
					return err.Error()
				}
			}

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
			d.mu.versions.picker.forceBaseLevel1()
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
			return runIterCmd(td, iter, true)

		default:
			return fmt.Sprintf("unknown command: %s", td.Cmd)
		}
	})
}

func TestIteratorNextPrev(t *testing.T) {
	var mem vfs.FS
	var d *DB
	defer func() {
		require.NoError(t, d.Close())
	}()

	reset := func() {
		if d != nil {
			require.NoError(t, d.Close())
		}

		mem = vfs.NewMem()
		require.NoError(t, mem.MkdirAll("ext", 0755))
		opts := &Options{FS: mem}
		// Automatic compactions may compact away tombstones from L6, making
		// some testcases non-deterministic.
		opts.DisableAutomaticCompactions = true
		var err error
		d, err = Open("", opts)
		require.NoError(t, err)
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
			return runIterCmd(td, iter, true)

		default:
			return fmt.Sprintf("unknown command: %s", td.Cmd)
		}
	})
}

type iterSeekOptWrapper struct {
	internalIterator

	seekGEUsingNext, seekPrefixGEUsingNext *int
}

func (i *iterSeekOptWrapper) SeekGE(key []byte, trySeekUsingNext bool) (*InternalKey, []byte) {
	if trySeekUsingNext {
		*i.seekGEUsingNext++
	}
	return i.internalIterator.SeekGE(key, trySeekUsingNext)
}

func (i *iterSeekOptWrapper) SeekPrefixGE(prefix, key []byte, trySeekUsingNext bool) (*InternalKey, []byte) {
	if trySeekUsingNext {
		*i.seekPrefixGEUsingNext++
	}
	return i.internalIterator.SeekPrefixGE(prefix, key, trySeekUsingNext)
}

func TestIteratorSeekOpt(t *testing.T) {
	var d *DB
	defer func() {
		require.NoError(t, d.Close())
	}()
	var iter *Iterator
	defer func() {
		if iter != nil {
			require.NoError(t, iter.Close())
		}
	}()
	var seekGEUsingNext, seekPrefixGEUsingNext int

	datadriven.RunTest(t, "testdata/iterator_seek_opt", func(td *datadriven.TestData) string {
		switch td.Cmd {
		case "define":
			if iter != nil {
				if err := iter.Close(); err != nil {
					return err.Error()
				}
			}
			if d != nil {
				if err := d.Close(); err != nil {
					return err.Error()
				}
			}
			seekGEUsingNext = 0
			seekPrefixGEUsingNext = 0

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
			s := d.mu.versions.currentVersion().DebugString(base.DefaultFormatter)
			d.mu.Unlock()
			oldNewIters := d.newIters
			d.newIters = func(file *manifest.FileMetadata, opts *IterOptions, bytesIterated *uint64) (internalIterator, keyspan.FragmentIterator, error) {
				iter, rangeIter, err := oldNewIters(file, opts, bytesIterated)
				iterWrapped := &iterSeekOptWrapper{
					internalIterator:      iter,
					seekGEUsingNext:       &seekGEUsingNext,
					seekPrefixGEUsingNext: &seekPrefixGEUsingNext,
				}
				return iterWrapped, rangeIter, err
			}
			return s

		case "iter":
			if iter == nil || iter.iter == nil {
				// TODO(peter): runDBDefineCmd doesn't properly update the visible
				// sequence number. So we have to use a snapshot with a very large
				// sequence number, otherwise the DB appears empty.
				snap := Snapshot{
					db:     d,
					seqNum: InternalKeySeqNumMax,
				}
				iter = snap.NewIter(nil)
				iter.readSampling.forceReadSampling = true
				iter.split = func(a []byte) int { return len(a) }
				iter.forceEnableSeekOpt = true
			}
			iterOutput := runIterCmd(td, iter, false)
			stats := iter.Stats()
			var builder strings.Builder
			fmt.Fprintf(&builder, "%sstats: %s\n", iterOutput, stats.String())
			fmt.Fprintf(&builder, "SeekGEs with trySeekUsingNext: %d\n", seekGEUsingNext)
			fmt.Fprintf(&builder, "SeekPrefixGEs with trySeekUsingNext: %d\n", seekPrefixGEUsingNext)
			return builder.String()

		default:
			return fmt.Sprintf("unknown command: %s", td.Cmd)
		}
	})
}

type errorSeekIter struct {
	internalIterator
	// Fields controlling error injection for seeks.
	injectSeekErrorCounts []int
	seekCount             int
	err                   error
}

func (i *errorSeekIter) SeekGE(key []byte, trySeekUsingNext bool) (*InternalKey, []byte) {
	if i.tryInjectError() {
		return nil, nil
	}
	i.err = nil
	i.seekCount++
	return i.internalIterator.SeekGE(key, trySeekUsingNext)
}

func (i *errorSeekIter) SeekPrefixGE(
	prefix, key []byte, trySeekUsingNext bool,
) (*InternalKey, []byte) {
	if i.tryInjectError() {
		return nil, nil
	}
	i.err = nil
	i.seekCount++
	return i.internalIterator.SeekPrefixGE(prefix, key, trySeekUsingNext)
}

func (i *errorSeekIter) SeekLT(key []byte) (*InternalKey, []byte) {
	if i.tryInjectError() {
		return nil, nil
	}
	i.err = nil
	i.seekCount++
	return i.internalIterator.SeekLT(key)
}

func (i *errorSeekIter) tryInjectError() bool {
	if len(i.injectSeekErrorCounts) > 0 && i.injectSeekErrorCounts[0] == i.seekCount {
		i.seekCount++
		i.err = errors.Errorf("injecting error")
		i.injectSeekErrorCounts = i.injectSeekErrorCounts[1:]
		return true
	}
	return false
}

func (i *errorSeekIter) First() (*InternalKey, []byte) {
	i.err = nil
	return i.internalIterator.First()
}

func (i *errorSeekIter) Last() (*InternalKey, []byte) {
	i.err = nil
	return i.internalIterator.Last()
}

func (i *errorSeekIter) Next() (*InternalKey, []byte) {
	if i.err != nil {
		return nil, nil
	}
	return i.internalIterator.Next()
}

func (i *errorSeekIter) Prev() (*InternalKey, []byte) {
	if i.err != nil {
		return nil, nil
	}
	return i.internalIterator.Prev()
}

func (i *errorSeekIter) Error() error {
	if i.err != nil {
		return i.err
	}
	return i.internalIterator.Error()
}

func TestIteratorSeekOptErrors(t *testing.T) {
	var keys []InternalKey
	var vals [][]byte

	var errorIter errorSeekIter
	newIter := func(opts IterOptions) *Iterator {
		cmp := DefaultComparer.Compare
		equal := DefaultComparer.Equal
		split := func(a []byte) int { return len(a) }
		iter := &fakeIter{
			lower: opts.GetLowerBound(),
			upper: opts.GetUpperBound(),
			keys:  keys,
			vals:  vals,
		}
		errorIter = errorSeekIter{internalIterator: newInvalidatingIter(iter)}
		// NB: This Iterator cannot be cloned since it is not constructed
		// with a readState. It suffices for this test.
		return &Iterator{
			opts:  opts,
			cmp:   cmp,
			equal: equal,
			split: split,
			merge: DefaultMerger.Merge,
			iter:  &errorIter,
		}
	}

	datadriven.RunTest(t, "testdata/iterator_seek_opt_errors", func(d *datadriven.TestData) string {
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
			var opts IterOptions
			var injectSeekGEErrorCounts []int
			for _, arg := range d.CmdArgs {
				if len(arg.Vals) < 1 {
					return fmt.Sprintf("%s: %s=<value>", d.Cmd, arg.Key)
				}
				switch arg.Key {
				case "lower":
					opts.LowerBound = []byte(arg.Vals[0])
				case "upper":
					opts.UpperBound = []byte(arg.Vals[0])
				case "seek-error":
					for i := 0; i < len(arg.Vals); i++ {
						n, err := strconv.Atoi(arg.Vals[i])
						if err != nil {
							return err.Error()
						}
						injectSeekGEErrorCounts = append(injectSeekGEErrorCounts, n)
					}
				default:
					return fmt.Sprintf("%s: unknown arg: %s", d.Cmd, arg.Key)
				}
			}

			iter := newIter(opts)
			errorIter.injectSeekErrorCounts = injectSeekGEErrorCounts
			return runIterCmd(d, iter, true)

		default:
			return fmt.Sprintf("unknown command: %s", d.Cmd)
		}
	})
}

type testBlockIntervalCollector struct {
	numLength     int
	offsetFromEnd int
	initialized   bool
	lower, upper  uint64
}

func (bi *testBlockIntervalCollector) Add(key InternalKey, value []byte) error {
	k := key.UserKey
	if len(k) < bi.numLength+bi.offsetFromEnd {
		return nil
	}
	n := len(k) - bi.offsetFromEnd - bi.numLength
	val, err := strconv.Atoi(string(k[n : n+bi.numLength]))
	if err != nil {
		return err
	}
	if val < 0 {
		panic("testBlockIntervalCollector expects values >= 0")
	}
	uval := uint64(val)
	if !bi.initialized {
		bi.lower, bi.upper = uval, uval+1
		bi.initialized = true
		return nil
	}
	if bi.lower > uval {
		bi.lower = uval
	}
	if uval >= bi.upper {
		bi.upper = uval + 1
	}
	return nil
}

func (bi *testBlockIntervalCollector) FinishDataBlock() (lower uint64, upper uint64, err error) {
	bi.initialized = false
	l, u := bi.lower, bi.upper
	bi.lower, bi.upper = 0, 0
	return l, u, nil
}

func TestIteratorBlockIntervalFilter(t *testing.T) {
	var mem vfs.FS
	var d *DB
	defer func() {
		require.NoError(t, d.Close())
	}()

	type collector struct {
		id     uint16
		offset int
	}
	createDB := func(collectors []collector) {
		if d != nil {
			require.NoError(t, d.Close())
		}

		mem = vfs.NewMem()
		require.NoError(t, mem.MkdirAll("ext", 0755))

		var bpCollectors []func() BlockPropertyCollector
		for _, c := range collectors {
			coll := c
			bpCollectors = append(bpCollectors, func() BlockPropertyCollector {
				return sstable.NewBlockIntervalCollector(
					fmt.Sprintf("%d", coll.id),
					&testBlockIntervalCollector{numLength: 2, offsetFromEnd: coll.offset},
					nil, /* range key collector */
				)
			})
		}
		opts := &Options{
			FS:                      mem,
			FormatMajorVersion:      FormatNewest,
			BlockPropertyCollectors: bpCollectors,
		}
		lo := LevelOptions{BlockSize: 1, IndexBlockSize: 1}
		opts.Levels = append(opts.Levels, lo)

		// Automatic compactions may compact away tombstones from L6, making
		// some testcases non-deterministic.
		opts.DisableAutomaticCompactions = true
		var err error
		d, err = Open("", opts)
		require.NoError(t, err)
	}

	datadriven.RunTest(
		t, "testdata/iterator_block_interval_filter", func(td *datadriven.TestData) string {
			switch td.Cmd {
			case "build":
				var collectors []collector
				for _, arg := range td.CmdArgs {
					switch arg.Key {
					case "id_offset":
						if len(arg.Vals) != 2 {
							return "id and offset not provided"
						}
						var id, offset int
						var err error
						if id, err = strconv.Atoi(arg.Vals[0]); err != nil {
							return err.Error()
						}
						if offset, err = strconv.Atoi(arg.Vals[1]); err != nil {
							return err.Error()
						}
						collectors = append(collectors, collector{id: uint16(id), offset: offset})
					default:
						return fmt.Sprintf("unknown key: %s", arg.Key)
					}
				}
				createDB(collectors)
				b := d.NewBatch()
				if err := runBatchDefineCmd(td, b); err != nil {
					return err.Error()
				}
				if err := b.Commit(nil); err != nil {
					return err.Error()
				}
				if err := d.Flush(); err != nil {
					return err.Error()
				}
				return runLSMCmd(td, d)

			case "iter":
				var opts IterOptions
				for _, arg := range td.CmdArgs {
					switch arg.Key {
					case "id_lower_upper":
						if len(arg.Vals) != 3 {
							return "id, lower, upper not provided"
						}
						var id, lower, upper int
						var err error
						if id, err = strconv.Atoi(arg.Vals[0]); err != nil {
							return err.Error()
						}
						if lower, err = strconv.Atoi(arg.Vals[1]); err != nil {
							return err.Error()
						}
						if upper, err = strconv.Atoi(arg.Vals[2]); err != nil {
							return err.Error()
						}
						opts.BlockPropertyFilters = append(opts.BlockPropertyFilters,
							sstable.NewBlockIntervalFilter(fmt.Sprintf("%d", id),
								uint64(lower), uint64(upper)))
					default:
						return fmt.Sprintf("unknown key: %s", arg.Key)
					}
				}
				rand.Shuffle(len(opts.BlockPropertyFilters), func(i, j int) {
					opts.BlockPropertyFilters[i], opts.BlockPropertyFilters[j] =
						opts.BlockPropertyFilters[j], opts.BlockPropertyFilters[i]
				})
				iter := d.NewIter(&opts)
				return runIterCmd(td, iter, true)

			default:
				return fmt.Sprintf("unknown command: %s", td.Cmd)
			}
		})
}

var seed = flag.Uint64("seed", 0, "a pseudorandom number generator seed")

func randValue(n int, rng *rand.Rand) []byte {
	const letters = "abcdefghijklmnopqrstuvwxyz"
	const lettersLen = len(letters)
	buf := make([]byte, n)
	for i := 0; i < len(buf); i++ {
		buf[i] = letters[rng.Intn(lettersLen)]
	}
	return buf
}

func randKey(n int, rng *rand.Rand) ([]byte, int) {
	keyPrefix := randValue(n, rng)
	suffix := rng.Intn(100)
	return append(keyPrefix, []byte(fmt.Sprintf("%02d", suffix))...), suffix
}

func TestIteratorRandomizedBlockIntervalFilter(t *testing.T) {
	mem := vfs.NewMem()
	opts := &Options{
		FS:                 mem,
		FormatMajorVersion: FormatNewest,
		BlockPropertyCollectors: []func() BlockPropertyCollector{
			func() BlockPropertyCollector {
				return sstable.NewBlockIntervalCollector(
					"0", &testBlockIntervalCollector{numLength: 2}, nil, /* range key collector */
				)
			},
		},
	}
	seed := *seed
	if seed == 0 {
		seed = uint64(time.Now().UnixNano())
		fmt.Printf("seed: %d\n", seed)
	}
	rng := rand.New(rand.NewSource(seed))
	opts.FlushSplitBytes = 1 << rng.Intn(8)       // 1B - 256B
	opts.L0CompactionThreshold = 1 << rng.Intn(2) // 1-2
	opts.LBaseMaxBytes = 1 << rng.Intn(10)        // 1B - 1KB
	opts.MemTableSize = 1 << 10                   // 1KB
	var lopts LevelOptions
	lopts.BlockSize = 1 << rng.Intn(8)      // 1B - 256B
	lopts.IndexBlockSize = 1 << rng.Intn(8) // 1B - 256B
	opts.Levels = []LevelOptions{lopts}

	d, err := Open("", opts)
	require.NoError(t, err)
	defer func() {
		require.NoError(t, d.Close())
	}()
	matchingKeyValues := make(map[string]string)
	lower := rng.Intn(100)
	upper := rng.Intn(100)
	if lower > upper {
		lower, upper = upper, lower
	}
	n := 2000
	for i := 0; i < n; i++ {
		key, suffix := randKey(20+rng.Intn(5), rng)
		value := randValue(50, rng)
		if lower <= suffix && suffix < upper {
			matchingKeyValues[string(key)] = string(value)
		}
		d.Set(key, value, nil)
	}

	var iterOpts IterOptions
	iterOpts.BlockPropertyFilters = []BlockPropertyFilter{
		sstable.NewBlockIntervalFilter("0",
			uint64(lower), uint64(upper)),
	}
	iter := d.NewIter(&iterOpts)
	defer func() {
		require.NoError(t, iter.Close())
	}()
	iter.First()
	found := 0
	matchingCount := len(matchingKeyValues)
	for ; iter.Valid(); iter.Next() {
		found++
		key := string(iter.Key())
		value, ok := matchingKeyValues[key]
		if ok {
			require.Equal(t, value, string(iter.Value()))
			delete(matchingKeyValues, key)
		}
	}
	fmt.Printf("generated %d keys: %d matching, %d found\n", n, matchingCount, found)
	require.Equal(t, 0, len(matchingKeyValues))
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

// BenchmarkIteratorSeqSeekPrefixGENotFound exercises the case of SeekPrefixGE
// specifying monotonic keys all of which precede actual keys present in L6 of
// the DB. Moreover, with-tombstone=true exercises the sub-case where those
// actual keys are deleted using a range tombstone that has not physically
// deleted those keys due to the presence of a snapshot that needs to see
// those keys. This sub-case needs to be efficient in (a) avoiding iteration
// over all those deleted keys, including repeated iteration, (b) using the
// next optimization, since the seeks are monotonic.
func BenchmarkIteratorSeqSeekPrefixGENotFound(b *testing.B) {
	const blockSize = 32 << 10
	const restartInterval = 16
	const levelCount = 5
	const keyOffset = 100000

	var readers [4][][]*sstable.Reader
	var levelSlices [4][]manifest.LevelSlice
	indexFunc := func(bloom bool, withTombstone bool) int {
		index := 0
		if bloom {
			index = 2
		}
		if withTombstone {
			index++
		}
		return index
	}
	for _, bloom := range []bool{false, true} {
		for _, withTombstone := range []bool{false, true} {
			index := indexFunc(bloom, withTombstone)
			levels := levelCount
			if withTombstone {
				levels = 1
			}
			readers[index], levelSlices[index], _ = buildLevelsForMergingIterSeqSeek(
				b, blockSize, restartInterval, levels, keyOffset, withTombstone, bloom)

		}
	}
	// We will not be seeking to the keys that were written but instead to
	// keys before the written keys. This is to validate that the optimization
	// to use Next still functions when mergingIter checks for the prefix
	// match, and that mergingIter can avoid iterating over all the keys
	// deleted by a range tombstone when there is no possibility of matching
	// the prefix.
	var keys [][]byte
	for i := 0; i < keyOffset; i++ {
		keys = append(keys, []byte(fmt.Sprintf("%08d", i)))
	}
	for _, skip := range []int{1, 2, 4} {
		for _, bloom := range []bool{false, true} {
			for _, withTombstone := range []bool{false, true} {
				b.Run(fmt.Sprintf("skip=%d/bloom=%t/with-tombstone=%t", skip, bloom, withTombstone),
					func(b *testing.B) {
						index := indexFunc(bloom, withTombstone)
						readers := readers[index]
						levelSlices := levelSlices[index]
						m := buildMergingIter(readers, levelSlices)
						iter := Iterator{
							cmp:   DefaultComparer.Compare,
							equal: DefaultComparer.Equal,
							split: func(a []byte) int { return len(a) },
							merge: DefaultMerger.Merge,
							iter:  m,
						}
						pos := 0
						b.ResetTimer()
						for i := 0; i < b.N; i++ {
							// When withTombstone=true, and prior to the
							// optimization to stop early due to a range
							// tombstone, the iteration would continue into the
							// next file, and not be able to use Next at the lower
							// level in the next SeekPrefixGE call. So we would
							// incur the cost of iterating over all the deleted
							// keys for every seek. Note that it is not possible
							// to do a noop optimization in Iterator for the
							// prefix case, unlike SeekGE/SeekLT, since we don't
							// know if the iterators inside mergingIter are all
							// appropriately positioned -- some may not be due to
							// bloom filters not matching.
							valid := iter.SeekPrefixGE(keys[pos])
							if valid {
								b.Fatalf("key should not be found")
							}
							pos += skip
							if pos >= keyOffset {
								pos = 0
							}
						}
						b.StopTimer()
						iter.Close()
					})
			}
		}
	}
	for _, r := range readers {
		for i := range r {
			for j := range r[i] {
				r[i][j].Close()
			}
		}
	}
}

// BenchmarkIteratorSeqSeekGEWithBounds is analogous to
// BenchmarkMergingIterSeqSeekGEWithBounds, except for using an Iterator,
// which causes it to exercise the end-to-end code path.
func BenchmarkIteratorSeqSeekGEWithBounds(b *testing.B) {
	const blockSize = 32 << 10
	const restartInterval = 16
	const levelCount = 5
	readers, levelSlices, keys := buildLevelsForMergingIterSeqSeek(
		b, blockSize, restartInterval, levelCount, 0 /* keyOffset */, false, false)
	m := buildMergingIter(readers, levelSlices)
	iter := Iterator{
		cmp:   DefaultComparer.Compare,
		equal: DefaultComparer.Equal,
		split: DefaultComparer.Split,
		merge: DefaultMerger.Merge,
		iter:  m,
	}
	keyCount := len(keys)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		pos := i % (keyCount - 1)
		iter.SetBounds(keys[pos], keys[pos+1])
		// SeekGE will return keys[pos].
		valid := iter.SeekGE(keys[pos])
		for valid {
			valid = iter.Next()
		}
		if iter.Error() != nil {
			b.Fatalf(iter.Error().Error())
		}
	}
	iter.Close()
	for i := range readers {
		for j := range readers[i] {
			readers[i][j].Close()
		}
	}
}

func BenchmarkIteratorSeekGENoop(b *testing.B) {
	const blockSize = 32 << 10
	const restartInterval = 16
	const levelCount = 5
	const keyOffset = 10000
	readers, levelSlices, _ := buildLevelsForMergingIterSeqSeek(
		b, blockSize, restartInterval, levelCount, keyOffset, false, false)
	var keys [][]byte
	for i := 0; i < keyOffset; i++ {
		keys = append(keys, []byte(fmt.Sprintf("%08d", i)))
	}
	for _, withLimit := range []bool{false, true} {
		b.Run(fmt.Sprintf("withLimit=%t", withLimit), func(b *testing.B) {
			m := buildMergingIter(readers, levelSlices)
			iter := Iterator{
				cmp:   DefaultComparer.Compare,
				equal: DefaultComparer.Equal,
				split: DefaultComparer.Split,
				merge: DefaultMerger.Merge,
				iter:  m,
			}
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				pos := i % (len(keys) - 1)
				if withLimit {
					if iter.SeekGEWithLimit(keys[pos], keys[pos+1]) != IterAtLimit {
						b.Fatal("should be at limit")
					}
				} else {
					if !iter.SeekGE(keys[pos]) {
						b.Fatal("should be valid")
					}
				}
			}
			iter.Close()
		})
	}
	for i := range readers {
		for j := range readers[i] {
			readers[i][j].Close()
		}
	}
}

func BenchmarkBlockPropertyFilter(b *testing.B) {
	rng := rand.New(rand.NewSource(uint64(time.Now().UnixNano())))
	for _, matchInterval := range []int{1, 10, 100, 1000} {
		b.Run(fmt.Sprintf("match-interval=%d", matchInterval), func(b *testing.B) {
			mem := vfs.NewMem()
			opts := &Options{
				FS:                 mem,
				FormatMajorVersion: FormatNewest,
				BlockPropertyCollectors: []func() BlockPropertyCollector{
					func() BlockPropertyCollector {
						return sstable.NewBlockIntervalCollector(
							"0", &testBlockIntervalCollector{numLength: 3}, nil, /* range key collector */
						)
					},
				},
			}
			d, err := Open("", opts)
			require.NoError(b, err)
			defer func() {
				require.NoError(b, d.Close())
			}()
			batch := d.NewBatch()
			const numKeys = 20 * 1000
			const valueSize = 1000
			for i := 0; i < numKeys; i++ {
				key := fmt.Sprintf("%06d%03d", i, i%matchInterval)
				value := randValue(valueSize, rng)
				require.NoError(b, batch.Set([]byte(key), value, nil))
			}
			require.NoError(b, batch.Commit(nil))
			require.NoError(b, d.Flush())
			require.NoError(b, d.Compact(nil, []byte{0xFF}, false))

			for _, filter := range []bool{false, true} {
				b.Run(fmt.Sprintf("filter=%t", filter), func(b *testing.B) {
					var iterOpts IterOptions
					if filter {
						iterOpts.BlockPropertyFilters = []BlockPropertyFilter{
							sstable.NewBlockIntervalFilter("0",
								uint64(0), uint64(1)),
						}
					}
					iter := d.NewIter(&iterOpts)
					b.ResetTimer()
					for i := 0; i < b.N; i++ {
						valid := iter.First()
						for valid {
							valid = iter.Next()
						}
					}
					b.StopTimer()
					require.NoError(b, iter.Close())
				})
			}
		})
	}
}
