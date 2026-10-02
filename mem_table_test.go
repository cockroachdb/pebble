// Copyright 2011 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package pebble

import (
	"bytes"
	"context"
	"fmt"
	"math/rand/v2"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"
	"unicode"

	"github.com/cockroachdb/crlib/crstrings"
	"github.com/cockroachdb/crlib/testutils/leaktest"
	"github.com/cockroachdb/datadriven"
	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/pebble/internal/arenaskl"
	"github.com/cockroachdb/pebble/internal/base"
	"github.com/cockroachdb/pebble/internal/itertest"
	"github.com/cockroachdb/pebble/internal/keyspan"
	"github.com/cockroachdb/pebble/internal/rangedel"
	"github.com/cockroachdb/pebble/internal/rangekey"
	"github.com/prometheus/client_golang/prometheus"
	prometheusgo "github.com/prometheus/client_model/go"
	"github.com/stretchr/testify/require"
	"golang.org/x/sync/errgroup"
)

// get gets the value for the given key. It returns ErrNotFound if the DB does
// not contain the key.
func (m *memTable) get(key []byte) (value []byte, err error) {
	it := m.skl.NewIter(m.split, nil, nil)
	defer it.Close()
	kv := it.SeekGE(key, base.SeekGEFlagsNone)
	if kv == nil {
		return nil, ErrNotFound
	}
	if !m.equal(key, kv.K.UserKey) {
		return nil, ErrNotFound
	}
	switch kv.Kind() {
	case InternalKeyKindDelete, InternalKeyKindSingleDelete, InternalKeyKindDeleteSized:
		return nil, ErrNotFound
	default:
		return kv.InPlaceValue(), nil
	}
}

// Set sets the value for the given key. It overwrites any previous value for
// that key; a DB is not a multi-map. NB: this might have unexpected
// interaction with prepare/apply. Caveat emptor!
func (m *memTable) set(key InternalKey, value []byte) error {
	if key.Kind() == InternalKeyKindRangeDelete {
		if err := m.rangeDelSkl.Add(key, value); err != nil {
			return err
		}
		m.tombstones.invalidate(1)
		return nil
	}
	if rangekey.IsRangeKey(key.Kind()) {
		if err := m.rangeKeySkl.Add(key, value); err != nil {
			return err
		}
		m.rangeKeys.invalidate(1)
		return nil
	}
	return m.skl.Add(key, value)
}

// count returns the number of entries in a DB.
func (m *memTable) count() (n int) {
	x := m.newIter(nil)
	for kv := x.First(); kv != nil; kv = x.Next() {
		n++
	}
	if x.Close() != nil {
		return -1
	}
	return n
}

func ikey(s string) InternalKey {
	return base.MakeInternalKey([]byte(s), 0, InternalKeyKindSet)
}

func TestMemTableBasic(t *testing.T) {
	defer leaktest.AfterTest(t)()
	// Check the empty DB.
	m := newMemTable(memTableOptions{})
	if got, want := m.count(), 0; got != want {
		t.Fatalf("0.count: got %v, want %v", got, want)
	}
	v, err := m.get([]byte("cherry"))
	if string(v) != "" || err != ErrNotFound {
		t.Fatalf("1.get: got (%q, %v), want (%q, %v)", v, err, "", ErrNotFound)
	}
	// Add some key/value pairs.
	m.set(ikey("cherry"), []byte("red"))
	m.set(ikey("peach"), []byte("yellow"))
	m.set(ikey("grape"), []byte("red"))
	m.set(ikey("grape"), []byte("green"))
	m.set(ikey("plum"), []byte("purple"))
	if got, want := m.count(), 4; got != want {
		t.Fatalf("2.count: got %v, want %v", got, want)
	}
	// Get keys that are and aren't in the DB.
	v, err = m.get([]byte("plum"))
	if string(v) != "purple" || err != nil {
		t.Fatalf("6.get: got (%q, %v), want (%q, %v)", v, err, "purple", error(nil))
	}
	v, err = m.get([]byte("lychee"))
	if string(v) != "" || err != ErrNotFound {
		t.Fatalf("7.get: got (%q, %v), want (%q, %v)", v, err, "", ErrNotFound)
	}
	// Check an iterator.
	s, x := "", m.newIter(nil)
	for kv := x.SeekGE([]byte("mango"), base.SeekGEFlagsNone); kv != nil; kv = x.Next() {
		v, _, err := kv.Value(nil)
		require.NoError(t, err)
		s += fmt.Sprintf("%s/%s.", kv.K.UserKey, v)
	}
	if want := "peach/yellow.plum/purple."; s != want {
		t.Fatalf("8.iter: got %q, want %q", s, want)
	}
	if err = x.Close(); err != nil {
		t.Fatalf("9.close: %v", err)
	}
	// Check some more sets and deletes.
	if err := m.set(ikey("apricot"), []byte("orange")); err != nil {
		t.Fatalf("12.set: %v", err)
	}
	if got, want := m.count(), 5; got != want {
		t.Fatalf("13.count: got %v, want %v", got, want)
	}
}

func TestMemTableCount(t *testing.T) {
	defer leaktest.AfterTest(t)()
	m := newMemTable(memTableOptions{})
	for i := 0; i < 200; i++ {
		if j := m.count(); j != i {
			t.Fatalf("count: got %d, want %d", j, i)
		}
		m.set(InternalKey{UserKey: []byte{byte(i)}}, nil)
	}
}

func TestMemTableEmpty(t *testing.T) {
	defer leaktest.AfterTest(t)()
	m := newMemTable(memTableOptions{})
	if !m.empty() {
		t.Errorf("got !empty, want empty")
	}
	// Add one key/value pair with an empty key and empty value.
	m.set(InternalKey{}, nil)
	if m.empty() {
		t.Errorf("got empty, want !empty")
	}
}

func TestMemTable1000Entries(t *testing.T) {
	defer leaktest.AfterTest(t)()
	// Initialize the DB.
	const N = 1000
	m0 := newMemTable(memTableOptions{})
	for i := 0; i < N; i++ {
		k := ikey(strconv.Itoa(i))
		v := []byte(strings.Repeat("x", i))
		m0.set(k, v)
	}
	// Check the DB count.
	if got, want := m0.count(), 1000; got != want {
		t.Fatalf("count: got %v, want %v", got, want)
	}
	// Check random-access lookup.
	r := rand.New(rand.NewPCG(0, 0))
	for i := 0; i < 3*N; i++ {
		j := r.IntN(N)
		k := []byte(strconv.Itoa(j))
		v, err := m0.get(k)
		require.NoError(t, err)
		if len(v) != cap(v) {
			t.Fatalf("get: j=%d, got len(v)=%d, cap(v)=%d", j, len(v), cap(v))
		}
		var c uint8
		if len(v) != 0 {
			c = v[0]
		} else {
			c = 'x'
		}
		if len(v) != j || c != 'x' {
			t.Fatalf("get: j=%d, got len(v)=%d,c=%c, want %d,%c", j, len(v), c, j, 'x')
		}
	}
	// Check that iterating through the middle of the DB looks OK.
	// Keys are in lexicographic order, not numerical order.
	// Multiples of 3 are not present.
	wants := []string{
		"499",
		"5",
		"50",
		"500",
		"501",
		"502",
		"503",
		"504",
		"505",
		"506",
		"507",
	}
	x := m0.newIter(nil)
	kv := x.SeekGE([]byte(wants[0]), base.SeekGEFlagsNone)
	for _, want := range wants {
		if kv == nil {
			t.Fatalf("iter: next failed, want=%q", want)
		}
		if got := string(kv.K.UserKey); got != want {
			t.Fatalf("iter: got %q, want %q", got, want)
		}
		if k := kv.K.UserKey; len(k) != cap(k) {
			t.Fatalf("iter: len(k)=%d, cap(k)=%d", len(k), cap(k))
		}
		v, _, err := kv.Value(nil)
		require.NoError(t, err)
		if len(v) != cap(v) {
			t.Fatalf("iter: len(v)=%d, cap(v)=%d", len(v), cap(v))
		}
		x.Next()
	}
	if err := x.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}
}

func TestMemTableIter(t *testing.T) {
	defer leaktest.AfterTest(t)()
	var mem *memTable
	for _, testdata := range []string{
		"testdata/internal_iter_next", "testdata/internal_iter_bounds"} {
		datadriven.RunTest(t, testdata, func(t *testing.T, d *datadriven.TestData) string {
			switch d.Cmd {
			case "define":
				mem = newMemTable(memTableOptions{})
				for key := range crstrings.LinesSeq(d.Input) {
					j := strings.Index(key, ":")
					if err := mem.set(base.ParseInternalKey(key[:j]), []byte(key[j+1:])); err != nil {
						return err.Error()
					}
				}
				return ""

			case "iter":
				var options IterOptions
				for _, arg := range d.CmdArgs {
					switch arg.Key {
					case "lower":
						if len(arg.Vals) != 1 {
							return fmt.Sprintf(
								"%s expects at most 1 value for lower", d.Cmd)
						}
						options.LowerBound = []byte(arg.Vals[0])
					case "upper":
						if len(arg.Vals) != 1 {
							return fmt.Sprintf(
								"%s expects at most 1 value for upper", d.Cmd)
						}
						options.UpperBound = []byte(arg.Vals[0])
					default:
						return fmt.Sprintf("unknown arg: %s", arg.Key)
					}
				}
				iter := mem.newIter(&options)
				defer iter.Close()
				return itertest.RunInternalIterCmd(t, d, iter)

			default:
				return fmt.Sprintf("unknown command: %s", d.Cmd)
			}
		})
	}
}

func TestMemTableDeleteRange(t *testing.T) {
	defer leaktest.AfterTest(t)()
	var mem *memTable
	var seqNum base.SeqNum

	datadriven.RunTest(t, "testdata/delete_range", func(t *testing.T, td *datadriven.TestData) string {
		switch td.Cmd {
		case "clear":
			mem = nil
			seqNum = 0
			return ""

		case "define":
			b := newBatch(nil)
			if err := runBatchDefineCmd(td, b); err != nil {
				return err.Error()
			}
			if mem == nil {
				mem = newMemTable(memTableOptions{})
			}
			if err := mem.apply(b, seqNum); err != nil {
				return err.Error()
			}
			seqNum += base.SeqNum(b.Count())
			return ""

		case "scan":
			var buf bytes.Buffer
			if td.HasArg("range-del") {
				iter := mem.newRangeDelIter(nil)
				defer iter.Close()
				scanKeyspanIterator(&buf, iter)
			} else {
				iter := mem.newIter(nil)
				defer iter.Close()
				scanInternalIter(&buf, iter)
			}
			return buf.String()

		default:
			return fmt.Sprintf("unknown command: %s", td.Cmd)
		}
	})
}

func TestMemTableConcurrentDeleteRange(t *testing.T) {
	defer leaktest.AfterTest(t)()
	// Concurrently write and read range tombstones. Workers add range
	// tombstones, and then immediately retrieve them verifying that the
	// tombstones they've added are all present.

	m := newMemTable(memTableOptions{Options: &Options{MemTableSize: 64 << 20}})

	const workers = 10
	eg, _ := errgroup.WithContext(context.Background())
	var seqNum base.AtomicSeqNum
	seqNum.Store(1)
	for i := 0; i < workers; i++ {
		i := i
		eg.Go(func() error {
			start := ([]byte)(fmt.Sprintf("%03d", i))
			end := ([]byte)(fmt.Sprintf("%03d", i+1))
			for j := 0; j < 100; j++ {
				b := newBatch(nil)
				b.DeleteRange(start, end, nil)
				n := seqNum.Add(1) - 1
				require.NoError(t, m.apply(b, n))
				b.Close()

				var count int
				it := m.newRangeDelIter(nil)
				s, err := it.SeekGE(start)
				for ; s != nil; s, err = it.Next() {
					if m.cmp(s.Start, end) >= 0 {
						break
					}
					count += len(s.Keys)
				}
				if err != nil {
					return err
				}
				if j+1 != count {
					return errors.Errorf("%d: expected %d tombstones, but found %d", i, j+1, count)
				}
			}
			return nil
		})
	}
	err := eg.Wait()
	if err != nil {
		t.Error(err)
	}
}

// histogramSamples returns the number of samples recorded by h and their sum.
func histogramSamples(t testing.TB, h prometheus.Histogram) (count uint64, sum float64) {
	t.Helper()
	require.NotNil(t, h)
	var m prometheusgo.Metric
	require.NoError(t, h.Write(&m))
	return m.GetHistogram().GetSampleCount(), m.GetHistogram().GetSampleSum()
}

// rangeDelCacheSamples summarizes the sample counts and sums of the
// histograms in MemTableRangeDelCacheMetrics.
type rangeDelCacheSamples struct {
	invalidations uint64
	// rebuilds is the sample count of RebuildDuration. The count of every
	// histogram other than ReaderWait must equal it.
	rebuilds       uint64
	readerWaits    uint64
	tombstonesSum  float64
	fragmentsSum   float64
	concurrencySum float64
}

func readRangeDelCacheSamples(t testing.TB, m MemTableRangeDelCacheMetrics) rangeDelCacheSamples {
	t.Helper()
	s := rangeDelCacheSamples{invalidations: m.Invalidations}
	s.rebuilds, _ = histogramSamples(t, m.RebuildDuration)
	s.readerWaits, _ = histogramSamples(t, m.ReaderWait)
	var n uint64
	n, s.tombstonesSum = histogramSamples(t, m.RebuildTombstones)
	require.Equal(t, s.rebuilds, n)
	n, s.fragmentsSum = histogramSamples(t, m.RebuildFragments)
	require.Equal(t, s.rebuilds, n)
	n, s.concurrencySum = histogramSamples(t, m.ConcurrentRebuilds)
	require.Equal(t, s.rebuilds, n)
	return s
}

func TestMemTableRangeDelCacheStats(t *testing.T) {
	defer leaktest.AfterTest(t)()
	stats := newKeySpanCacheStats()
	m := newMemTable(memTableOptions{rangeDelCacheStats: stats})

	seqNum := base.SeqNum(1)
	apply := func(fn func(b *Batch)) {
		t.Helper()
		b := newBatch(nil)
		defer b.Close()
		fn(b)
		require.NoError(t, m.apply(b, seqNum))
		seqNum += base.SeqNum(b.Count())
	}
	// read creates a range deletion iterator and returns the number of
	// fragments it contains.
	read := func() (fragments int) {
		t.Helper()
		it := m.newRangeDelIter(nil)
		require.NotNil(t, it)
		defer it.Close()
		for s, err := it.First(); s != nil; s, err = it.Next() {
			require.NoError(t, err)
			fragments++
		}
		return fragments
	}
	samples := func() rangeDelCacheSamples {
		t.Helper()
		return readRangeDelCacheSamples(t, stats.metrics())
	}

	// Nothing has been recorded before any range deletion is applied, and
	// there is no cache to build.
	require.Equal(t, rangeDelCacheSamples{}, samples())
	require.Nil(t, m.newRangeDelIter(nil))
	require.Equal(t, rangeDelCacheSamples{}, samples())

	// Applying a range deletion invalidates the cache but does not build it.
	apply(func(b *Batch) { require.NoError(t, b.DeleteRange([]byte("a"), []byte("c"), nil)) })
	require.Equal(t, rangeDelCacheSamples{invalidations: 1}, samples())

	// The first read builds the cache.
	require.Equal(t, 1, read())
	require.Equal(t, rangeDelCacheSamples{
		invalidations: 1, rebuilds: 1, tombstonesSum: 1, fragmentsSum: 1, concurrencySum: 1,
	}, samples())

	// Reads of the built cache record nothing.
	for i := 0; i < 3; i++ {
		require.Equal(t, 1, read())
	}
	require.Equal(t, rangeDelCacheSamples{
		invalidations: 1, rebuilds: 1, tombstonesSum: 1, fragmentsSum: 1, concurrencySum: 1,
	}, samples())

	// Batches without range deletions leave the cache and the stats alone.
	apply(func(b *Batch) { require.NoError(t, b.Set([]byte("a"), []byte("v"), nil)) })
	apply(func(b *Batch) {
		require.NoError(t, b.RangeKeySet([]byte("a"), []byte("z"), nil, []byte("v"), nil))
	})
	require.NotNil(t, m.newRangeKeyIter(nil))
	require.Equal(t, 1, read())
	require.Equal(t, rangeDelCacheSamples{
		invalidations: 1, rebuilds: 1, tombstonesSum: 1, fragmentsSum: 1, concurrencySum: 1,
	}, samples())

	// A batch with two range deletions invalidates once, and the rebuild sees
	// all three tombstones: [a,c), [b,d), [c,e) fragment into [a,b), [b,c),
	// [c,d), [d,e).
	apply(func(b *Batch) {
		require.NoError(t, b.DeleteRange([]byte("b"), []byte("d"), nil))
		require.NoError(t, b.DeleteRange([]byte("c"), []byte("e"), nil))
	})
	require.Equal(t, uint64(2), samples().invalidations)
	require.Equal(t, 4, read())
	require.Equal(t, rangeDelCacheSamples{
		invalidations: 2, rebuilds: 2, tombstonesSum: 1 + 3, fragmentsSum: 1 + 4, concurrencySum: 2,
	}, samples())

	// Several invalidations between reads are absorbed by a single rebuild.
	apply(func(b *Batch) { require.NoError(t, b.DeleteRange([]byte("e"), []byte("f"), nil)) })
	apply(func(b *Batch) { require.NoError(t, b.DeleteRange([]byte("f"), []byte("g"), nil)) })
	require.Equal(t, uint64(4), samples().invalidations)
	require.Equal(t, 6, read())
	require.Equal(t, 6, read())
	require.Equal(t, rangeDelCacheSamples{
		invalidations: 4, rebuilds: 3, tombstonesSum: 1 + 3 + 5, fragmentsSum: 1 + 4 + 6,
		concurrencySum: 3,
	}, samples())

	// Nothing above waited on another goroutine's rebuild.
	require.Equal(t, uint64(0), samples().readerWaits)
}

// TestMemTableRangeDelCacheStatsNil checks that a memtable without stats
// invalidates and rebuilds its cache.
func TestMemTableRangeDelCacheStatsNil(t *testing.T) {
	defer leaktest.AfterTest(t)()
	m := newMemTable(memTableOptions{})
	b := newBatch(nil)
	defer b.Close()
	require.NoError(t, b.DeleteRange([]byte("a"), []byte("c"), nil))
	require.NoError(t, m.apply(b, 1))
	it := m.newRangeDelIter(nil)
	require.NotNil(t, it)
	s, err := it.First()
	require.NoError(t, err)
	require.NotNil(t, s)
	it.Close()
	require.Equal(t, MemTableRangeDelCacheMetrics{}, (*keySpanCacheStats)(nil).metrics())
}

// TestMemTableRangeDelCacheConcurrentRebuilds blocks rebuilds of two
// keySpanFrags in the middle of their build to observe rebuilds in flight, and
// races readers against a rebuild.
func TestMemTableRangeDelCacheConcurrentRebuilds(t *testing.T) {
	defer leaktest.AfterTest(t)()
	stats := newKeySpanCacheStats()
	m := newMemTable(memTableOptions{rangeDelCacheStats: stats})
	b := newBatch(nil)
	defer b.Close()
	require.NoError(t, b.DeleteRange([]byte("a"), []byte("c"), nil))
	require.NoError(t, m.apply(b, 1))

	started := make(chan struct{})
	release := make(chan struct{})
	blockingConstructSpan := func(
		ik base.InternalKey, v []byte, keysDst []keyspan.Key,
	) (keyspan.Span, error) {
		started <- struct{}{}
		<-release
		return rangeDelConstructSpan(ik, v, keysDst)
	}
	get := func(f *keySpanFrags) []keyspan.Span {
		return f.get(&m.rangeDelSkl, m.cmp, m.formatKey, blockingConstructSpan, false /* onlyFragmentOverlappingSpans */, stats)
	}

	// Start a rebuild of first and wait for it to block inside the build. Then
	// start a rebuild of a second keySpanFrags: it observes the first one in
	// flight. Readers of first race with its rebuild.
	first, second := &keySpanFrags{count: 1}, &keySpanFrags{count: 1}
	const readers = 8
	var wg sync.WaitGroup
	spans := make([][]keyspan.Span, readers+2)
	wg.Add(1)
	go func() { defer wg.Done(); spans[0] = get(first) }()
	<-started
	wg.Add(1)
	go func() { defer wg.Done(); spans[1] = get(second) }()
	<-started
	for i := 0; i < readers; i++ {
		wg.Add(1)
		go func() { defer wg.Done(); spans[2+i] = get(first) }()
	}
	close(release)
	wg.Wait()

	for i := range spans {
		require.Len(t, spans[i], 1)
	}
	s := readRangeDelCacheSamples(t, stats.metrics())
	require.Equal(t, uint64(2), s.rebuilds)
	// The first rebuild saw itself in flight; the second saw both.
	require.Equal(t, float64(1+2), s.concurrencySum)
	require.Equal(t, float64(2), s.tombstonesSum)
	require.Equal(t, float64(2), s.fragmentsSum)
	// Each reader either waited for the rebuild or found it already built.
	require.LessOrEqual(t, s.readerWaits, uint64(readers))
	require.Equal(t, int64(0), stats.rebuildsInFlight.Load())
	require.True(t, first.built.Load())
	require.True(t, second.built.Load())

	// Reads of a built keySpanFrags record nothing further.
	require.Len(t, get(first), 1)
	require.Equal(t, s, readRangeDelCacheSamples(t, stats.metrics()))
}

// referenceRangeDelFragments is a copy of how keySpanFrags.get rebuilt range
// deletions before disjoint tombstones bypassed the Fragmenter: every
// tombstone in skl goes through a single keyspan.Fragmenter.
func referenceRangeDelFragments(
	skl *arenaskl.Skiplist, cmp Compare, formatKey base.FormatKey,
) []keyspan.Span {
	var spans []keyspan.Span
	frag := &keyspan.Fragmenter{
		Cmp:    cmp,
		Format: formatKey,
		Emit: func(fragmented keyspan.Span) {
			spans = append(spans, fragmented)
		},
	}
	it := skl.NewIter(base.DefaultSplit, nil, nil)
	var keysDst []keyspan.Key
	for kv := it.First(); kv != nil; kv = it.Next() {
		s := rangedel.Decode(kv.K, kv.InPlaceValue(), keysDst)
		frag.Add(s)
		keysDst = s.Keys[len(s.Keys):]
	}
	frag.Finish()
	return spans
}

// TestMemTableRangeDelFragmentsMatchReference checks that the memtable's
// fragmented range deletions match referenceRangeDelFragments over random
// tombstone sets.
func TestMemTableRangeDelFragmentsMatchReference(t *testing.T) {
	defer leaktest.AfterTest(t)()
	seed := uint64(time.Now().UnixNano())
	t.Logf("seed: %d", seed)
	rng := rand.New(rand.NewPCG(seed, seed))

	// Keys are single letters so that shared and touching bounds are common.
	const maxPos = 14
	key := func(pos int) []byte { return []byte{byte('a' + pos)} }
	// between returns a random position in [lo, hi], or lo if hi < lo.
	between := func(lo, hi int) int {
		if hi <= lo {
			return lo
		}
		return lo + rng.IntN(hi-lo+1)
	}
	type tombstone struct{ start, end int }
	randTombstone := func(prev tombstone) tombstone {
		var start, end int
		switch rng.IntN(8) {
		case 0: // Arbitrary, possibly inverted or empty.
			start, end = between(0, maxPos), between(0, maxPos)
		case 1: // Inverted.
			start = between(1, maxPos)
			end = between(0, start-1)
		case 2: // Empty.
			start = between(0, maxPos)
			end = start
		case 3: // Same start as prev, possibly a different end.
			start = prev.start
			end = between(start+1, maxPos)
		case 4: // Nested within prev.
			start = between(prev.start, prev.end)
			end = between(start, prev.end)
		case 5: // Touching: starts where prev ends.
			start = prev.end
			end = between(start+1, maxPos)
		case 6: // Disjoint from prev.
			start = between(prev.end+1, maxPos)
			end = between(start+1, maxPos)
		case 7: // Straddling prev's end.
			start = between(prev.start, prev.end-1)
			end = between(prev.end, maxPos)
		}
		return tombstone{start: start, end: end}
	}

	sentinel := keyspan.Key{Trailer: base.MakeTrailer(base.SeqNumMax, base.InternalKeyKindRangeDelete)}
	for iter := 0; iter < 2000; iter++ {
		m := newMemTable(memTableOptions{size: 256 << 10, releaseAccountingReservation: func() {}})
		n := rng.IntN(48)
		tombstones := make([]tombstone, n)
		for i := range tombstones {
			var prev tombstone
			if i > 0 {
				prev = tombstones[rng.IntN(i)]
			}
			tombstones[i] = randTombstone(prev)
		}
		// Assign sequence numbers in a random order so that they are unrelated
		// to the key order.
		seqNums := rng.Perm(n)
		var desc strings.Builder
		for i, ts := range tombstones {
			seqNum := base.SeqNum(seqNums[i] + 1)
			fmt.Fprintf(&desc, "%s-%s#%d ", key(ts.start), key(ts.end), seqNum)
			ik := base.MakeInternalKey(key(ts.start), seqNum, InternalKeyKindRangeDelete)
			require.NoError(t, m.set(ik, key(ts.end)))
		}

		want := referenceRangeDelFragments(&m.rangeDelSkl, m.cmp, m.formatKey)
		check := func(name string, got []keyspan.Span) {
			t.Helper()
			msg := fmt.Sprintf("seed %d, iteration %d, %s\ntombstones: %s\nwant: %s\ngot:  %s",
				seed, iter, name, desc.String(), want, got)
			require.Equal(t, want == nil, got == nil, msg)
			require.Equal(t, len(want), len(got), msg)
			for i := range want {
				require.Zero(t, m.cmp(want[i].Start, got[i].Start), msg)
				require.Zero(t, m.cmp(want[i].End, got[i].End), msg)
				require.Equal(t, want[i].KeysOrder, got[i].KeysOrder, msg)
				require.Equal(t, want[i].Keys, got[i].Keys, msg)
			}
		}

		got := m.tombstones.get()
		check("memtable", got)

		// Appending to one span's keys must not change any other span.
		for i := range got {
			extended := append(got[i].Keys, sentinel)
			require.Equal(t, sentinel, extended[len(extended)-1])
		}
		check("memtable after appends", got)

		// The count is only a capacity hint: the skiplist may hold more
		// tombstones than it says.
		hint := rng.IntN(2*n + 1)
		f := &keySpanFrags{count: uint32(hint)}
		check(fmt.Sprintf("count hint %d", hint),
			f.get(&m.rangeDelSkl, m.cmp, m.formatKey, rangeDelConstructSpan, true /* onlyFragmentOverlappingSpans */, nil /* stats */))

		m.free()
	}
}

func TestMemTableReserved(t *testing.T) {
	defer leaktest.AfterTest(t)()
	m := newMemTable(memTableOptions{size: 5000})
	// Increase to 2 references.
	m.writerRef()
	// The initial reservation accounts for the already allocated bytes from the
	// arena.
	require.Equal(t, m.reserved, m.skl.Arena().Size())
	b := newBatch(nil)
	b.Set([]byte("blueberry"), []byte("pie"), nil)
	require.NotEqual(t, 0, int(b.memTableSize))
	prevReserved := m.reserved
	m.prepare(b)
	require.Equal(t, int(m.reserved), int(b.memTableSize)+int(prevReserved))
}

func TestMemTable(t *testing.T) {
	defer leaktest.AfterTest(t)()
	var m *memTable
	var buf bytes.Buffer
	batches := map[string]*Batch{}

	summary := func() string {
		return fmt.Sprintf("%d of %d bytes available",
			m.availBytes(), m.totalBytes())
	}

	datadriven.RunTest(t, "testdata/mem_table", func(t *testing.T, td *datadriven.TestData) string {
		buf.Reset()
		switch td.Cmd {
		case "new":
			var o memTableOptions
			td.MaybeScanArgs(t, "size", &o.size)
			m = newMemTable(o)
			return ""
		case "prepare":
			var name string
			td.ScanArgs(t, "name", &name)
			b := newBatch(nil)
			if err := runBatchDefineCmd(td, b); err != nil {
				return err.Error()
			}
			batches[name] = b
			if err := m.prepare(b); err != nil {
				return err.Error()
			}
			return summary()
		case "apply":
			var name string
			var seqNum uint64
			td.ScanArgs(t, "name", &name)
			td.ScanArgs(t, "seq", &seqNum)
			if err := m.apply(batches[name], base.SeqNum(seqNum)); err != nil {
				return err.Error()
			}
			delete(batches, name)
			return summary()
		case "computePossibleOverlaps":
			stopAfterFirst := td.HasArg("stop-after-first")

			var keyRanges []bounded
			for l := range crstrings.LinesSeq(td.Input) {
				s := strings.FieldsFunc(l, func(r rune) bool { return unicode.IsSpace(r) || r == '-' })
				keyRanges = append(keyRanges, KeyRange{Start: []byte(s[0]), End: []byte(s[1])})
			}

			m.computePossibleOverlaps(func(b bounded) shouldContinue {
				fmt.Fprintf(&buf, "%s\n", b)
				if stopAfterFirst {
					return stopIteration
				}
				return continueIteration
			}, keyRanges...)

			return buf.String()
		default:
			return fmt.Sprintf("unrecognized command %q", td.Cmd)
		}
	})
}

func buildMemTable(b *testing.B) (*memTable, [][]byte) {
	m := newMemTable(memTableOptions{})
	var keys [][]byte
	var ikey InternalKey
	for i := 0; ; i++ {
		key := []byte(fmt.Sprintf("%08d", i))
		keys = append(keys, key)
		ikey = base.MakeInternalKey(key, 0, InternalKeyKindSet)
		if m.set(ikey, nil) == arenaskl.ErrArenaFull {
			break
		}
	}
	return m, keys
}

func BenchmarkMemTableIterSeekGE(b *testing.B) {
	m, keys := buildMemTable(b)
	iter := m.newIter(nil)
	rng := rand.New(rand.NewPCG(0, uint64(time.Now().UnixNano())))

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		iter.SeekGE(keys[rng.IntN(len(keys))], base.SeekGEFlagsNone)
	}
}

func BenchmarkMemTableIterSeqSeekGEWithBounds(b *testing.B) {
	m, keys := buildMemTable(b)
	rng := rand.New(rand.NewPCG(0, uint64(17136275210000)))
	// Set bounds to restrict iteration to the middle 50% of keys.
	iter := m.newIter(&IterOptions{
		LowerBound: keys[len(keys)/4],
		UpperBound: keys[3*len(keys)/4],
	})
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		iter.SeekGE(keys[rng.IntN(len(keys))], base.SeekGEFlagsNone)
	}
}

// BenchmarkMemTableIterSeekGESuccessiveWithBounds benchmarks a particular case
// where an upper bound excludes the majority of the memtable keys and the user
// seeks the iterator with successively increasing keys. This pattern is
// expected to be common in CockroachDB: eg, intent resolution with an upper
// bound at the end of the lock table span, or a MVCC iterator with an upper
// bound restricting constraining iteration to a single CockroachDB Range.
func BenchmarkMemTableIterSeekGESuccessiveWithBounds(b *testing.B) {
	m, keys := buildMemTable(b)
	iter := m.newIter(&IterOptions{
		UpperBound: keys[1],
	})
	flags := base.SeekGEFlagsNone.EnableTrySeekUsingNext()

	seekKeys := make([][]byte, 256)
	for i := 1; i < len(seekKeys); i++ {
		seekKeys[i] = append(append([]byte(nil), keys[0]...), byte(i-1))
	}

	b.ResetTimer()
	iter.SeekGE(keys[0], base.SeekGEFlagsNone)
	for i := 0; i < b.N-1; i++ {
		iter.SeekGE(seekKeys[i%len(seekKeys)], flags)
	}
}

func BenchmarkMemTableIterNext(b *testing.B) {
	m, _ := buildMemTable(b)
	iter := m.newIter(nil)
	_ = iter.First()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		kv := iter.Next()
		if kv == nil {
			kv = iter.First()
		}
		_ = kv
	}
}

func BenchmarkMemTableIterNextWithBounds(b *testing.B) {
	m, keys := buildMemTable(b)
	// Set bounds to restrict iteration to the middle 50% of keys.
	opts := &IterOptions{
		LowerBound: keys[len(keys)/4],
		UpperBound: keys[3*len(keys)/4],
	}
	iter := m.newIter(opts)
	_ = iter.SeekGE(opts.LowerBound, base.SeekGEFlagsNone)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		kv := iter.Next()
		if kv == nil {
			kv = iter.SeekGE(opts.LowerBound, base.SeekGEFlagsNone)
		}
		_ = kv
	}
}

func BenchmarkMemTableIterPrev(b *testing.B) {
	m, _ := buildMemTable(b)
	iter := m.newIter(nil)
	_ = iter.Last()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		kv := iter.Prev()
		if kv == nil {
			kv = iter.Last()
		}
		_ = kv
	}
}

func BenchmarkMemTableIterPrevWithBounds(b *testing.B) {
	m, keys := buildMemTable(b)
	// Set bounds to restrict iteration to the middle 50% of keys.
	opts := &IterOptions{
		LowerBound: keys[len(keys)/4],
		UpperBound: keys[3*len(keys)/4],
	}
	iter := m.newIter(opts)
	_ = iter.SeekLT(opts.UpperBound, base.SeekLTFlagsNone)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		kv := iter.Prev()
		if kv == nil {
			kv = iter.SeekLT(opts.UpperBound, base.SeekLTFlagsNone)
		}
		_ = kv
	}
}

// BenchmarkMemTableRangeDelRebuild measures rebuilding a memtable's fragmented
// range deletions from scratch, as the first read after a range deletion is
// applied must do.
func BenchmarkMemTableRangeDelRebuild(b *testing.B) {
	for _, n := range []int{1000, 10000} {
		for _, layout := range []string{"disjoint", "chained", "mostly-disjoint"} {
			b.Run(fmt.Sprintf("n=%d/layout=%s", n, layout), func(b *testing.B) {
				m := newMemTable(memTableOptions{
					size:                         8 << 20,
					releaseAccountingReservation: func() {},
				})
				defer m.free()
				key := func(i int) []byte { return fmt.Appendf(nil, "%08d", i) }
				for i := 0; i < n; i++ {
					// Tombstone i covers [2i, 2i+1). An overlapping tombstone
					// instead covers [2i, 2i+3), which overlaps tombstone i+1.
					end := 2*i + 1
					if layout == "chained" || (layout == "mostly-disjoint" && i%10 == 0) {
						end = 2*i + 3
					}
					ik := base.MakeInternalKey(key(2*i), base.SeqNum(i+1), InternalKeyKindRangeDelete)
					if err := m.set(ik, key(end)); err != nil {
						b.Fatal(err)
					}
				}
				b.ResetTimer()
				for i := 0; i < b.N; i++ {
					m.tombstones.frags.Store(&keySpanFrags{count: uint32(n)})
					if len(m.tombstones.get()) == 0 {
						b.Fatal("no range deletions")
					}
				}
			})
		}
	}
}
