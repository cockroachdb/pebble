// Copyright 2013 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package pebble

import (
	"bytes"
	"fmt"
	"io"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/pebble/internal/base"
	"github.com/cockroachdb/pebble/internal/datadriven"
	"github.com/cockroachdb/pebble/vfs"
	"github.com/stretchr/testify/require"
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

func (f *fakeIter) SeekPrefixGE(
	prefix, key []byte, trySeekUsingNext bool,
) (*base.InternalKey, []byte) {
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

// invalidatingIter tests unsafe key/value slice reuse by modifying the last
// returned key/value to all 1s.
type invalidatingIter struct {
	iter      internalIterator
	lastKey   *InternalKey
	lastValue []byte
}

func newInvalidatingIter(iter internalIterator) *invalidatingIter {
	return &invalidatingIter{iter: iter}
}

func (i *invalidatingIter) update(key *InternalKey, value []byte) (*InternalKey, []byte) {
	if i.lastKey != nil {
		for j := range i.lastKey.UserKey {
			i.lastKey.UserKey[j] = 0xff
		}
		i.lastKey.Trailer = 0
	}
	for j := range i.lastValue {
		i.lastValue[j] = 0xff
	}

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

func (i *invalidatingIter) SeekGE(key []byte) (*InternalKey, []byte) {
	return i.update(i.iter.SeekGE(key))
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
		iter.elideRangeTombstones = true
		// NB: This Iterator cannot be cloned since it is not constructed
		// with a readState. It suffices for this test.
		return &Iterator{
			opts:  opts,
			cmp:   cmp,
			equal: equal,
			split: split,
			merge: DefaultMerger.Merge,
			iter:  newInvalidatingIter(iter),
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
			return runIterCmd(d, iter, true)

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
			//d.mu.versions.picker.forceBaseLevel1()
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
			if len(d.mu.compact.readCompactions) == 0 {
				sb.WriteString("(none)")
			}
			for _, rc := range d.mu.compact.readCompactions {
				sb.WriteString(fmt.Sprintf("(level: %d, start: %s, end: %s)\n", rc.level, string(rc.start), string(rc.end)))
			}
			d.mu.Unlock()
			return sb.String()

		case "iter-read-compactions":
			if iter == nil {
				return fmt.Sprintf("%s: iter is not defined", td.Cmd)
			}

			var sb strings.Builder
			if len(iter.readSampling.pendingCompactions) == 0 {
				sb.WriteString("(none)")
			}
			for _, rc := range iter.readSampling.pendingCompactions {
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
		opts.private.disableAutomaticCompactions = true
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

// duplicatedKeyDetectMerger check if there are multiple merge operation with the same key
// This implement copy all the key and values byte to ensure at any time, the bytes values is the expected value.
type duplicatedKeyDetectMerger struct {
	key    []byte
	values [][]byte
	// callback expose the duplicated key and values
	callback func([]byte, [][]byte)
}

func (merger *duplicatedKeyDetectMerger) MergeNewer(value []byte) error {
	merger.values = append(merger.values, append([]byte{}, value...))
	return nil
}

func (merger *duplicatedKeyDetectMerger) MergeOlder(value []byte) error {
	buf := make([][]byte, 0, len(merger.values)+1)
	buf = append(buf, append([]byte{}, value...))
	buf = append(buf, merger.values...)
	merger.values = buf
	return nil
}

func (merger *duplicatedKeyDetectMerger) Finish(includesBase bool) ([]byte, io.Closer, error) {
	if len(merger.values) > 1 {
		merger.callback(merger.key, merger.values)
	}
	return merger.values[len(merger.values)-1], nil, nil
}

func TestMerger(t *testing.T) {
	type dupRecord struct {
		key    []byte
		values [][]byte
	}
	duplicatedRecords := make([]dupRecord, 0)
	addRecord := func(key []byte, values [][]byte) {
		duplicatedRecords = append(duplicatedRecords, dupRecord{
			key:    key,
			values: values,
		})
	}
	db, err := Open("", &Options{
		FS: vfs.NewMem(),
		L0CompactionThreshold: 10,
		Merger: &Merger{
			Merge: func(key, value []byte) (ValueMerger, error) {
				// Copy the key and value bytes to make sure they are not change
				return &duplicatedKeyDetectMerger{
					key:      append([]byte{}, key...),
					values:   [][]byte{append([]byte{}, value...)},
					callback: addRecord,
				}, nil
			},
			Name: "TestMerge",
		},
	})
	require.NoError(t, err)
	defer db.Close()

	merge := func(kvs [][]byte) {
		for i := 0; i < len(kvs); i += 2 {
			err = db.Merge(kvs[i], kvs[i+1], NoSync)
			require.NoError(t, err)
		}
		err = db.Flush()
		require.NoError(t, err)
	}
	// Batch merge kvs pair (1,1),(2,2),(1,2) this will trigger a merge during flush
	merge([][]byte{{1}, {1}, {2}, {2}, {1}, {2}})
	require.Len(t, duplicatedRecords, 1)
	require.Equal(t, duplicatedRecords[0], dupRecord{key: []byte{1}, values: [][]byte{{1}, {2}}})

	// Merge another batch (2,3) this won't trigger a merge
	merge([][]byte{{2}, {3}})
	require.Len(t, duplicatedRecords, 1)

	// After this iterator, will detect another duplicate record
	iter := db.NewIter(nil)
	defer iter.Clone()
	require.True(t, iter.First())
	require.EqualValues(t, iter.Key(), []byte{1})
	require.EqualValues(t, iter.Value(), []byte{2})

	require.True(t, iter.Next())
	require.EqualValues(t, iter.Key(), []byte{2})
	require.EqualValues(t, iter.Value(), []byte{3})

	require.False(t, iter.Next())

	require.Len(t, duplicatedRecords, 2)
	require.Equal(t, duplicatedRecords[1], dupRecord{key: []byte{2}, values: [][]byte{{2}, {3}}})
}

type errorSeekIter struct {
	internalIterator
	// Fields controlling error injection for seeks.
	injectSeekErrorCounts []int
	seekCount             int
	err                   error
}

func (i *errorSeekIter) SeekGE(key []byte) (*InternalKey, []byte) {
	if i.tryInjectError() {
		return nil, nil
	}
	i.err = nil
	i.seekCount++
	return i.internalIterator.SeekGE(key)
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
