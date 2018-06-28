package pebble

import (
	"bytes"
	"fmt"
	"math/rand"
	"strings"
	"testing"
	"time"

	"github.com/petermattis/pebble/cache"
	"github.com/petermattis/pebble/db"
	"github.com/petermattis/pebble/storage"
	"github.com/petermattis/pebble/table"
)

func TestMergingIter(t *testing.T) {
	newFunc := func(iters ...db.InternalIterator) db.InternalIterator {
		return newMergingIter(db.DefaultComparer.Compare, iters...)
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

func TestMergingIterSeek(t *testing.T) {
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
			".",
		},
		{
			"a.SET.1",
			"a:0;a:1;a:2",
			"<a:1><a:0>.",
			"<a:2>.",
		},
		{
			"a.SET.0",
			"a:0;a:1;a:2",
			"<a:0>.",
			"<a:2><a:1>.",
		},
	}
	for _, tc := range testCases {
		t.Run("", func(t *testing.T) {
			var iters []db.InternalIterator
			for _, s := range strings.Split(tc.iters, ";") {
				iters = append(iters, newFakeIterator(nil, strings.Split(s, ",")...))
			}

			var b bytes.Buffer
			iter := newMergingIter(db.DefaultComparer.Compare, iters...)
			ikey := makeIkey(tc.key)
			for iter.SeekGE(ikey); iter.Valid(); iter.Next() {
				fmt.Fprintf(&b, "<%s:%d>", iter.Key().UserKey, iter.Key().SeqNum())
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
			for iter.SeekLT(ikey); iter.Valid(); iter.Prev() {
				fmt.Fprintf(&b, "<%s:%d>", iter.Key().UserKey, iter.Key().SeqNum())
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

func TestMergingIterNextPrev(t *testing.T) {
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
			m := newMergingIter(db.DefaultComparer.Compare, iters...)
			m.First()

			// These test cases are shared with TestMemTableIterNextPrev.
			testCases := []struct {
				dir      string
				expected string
			}{
				{"+", "<a:1>"}, // 0
				{"+", "<b:2>"}, // 1
				{"-", "<b:1>"}, // 2
				{"-", "<a:2>"}, // 3
				{"-", "<a:1>"}, // 4
				{"-", "."},     // 5
				{"+", "<a:2>"}, // 6
				{"+", "<a:1>"}, // 7
				{"+", "<b:2>"}, // 8
				{"+", "<b:1>"}, // 9
				{"+", "<c:2>"}, // 10
				{"+", "<c:1>"}, // 11
				{"-", "<b:2>"}, // 12
				{"-", "<b:1>"}, // 13
				{"+", "<c:2>"}, // 14
				{"-", "<c:1>"}, // 15
				{"-", "<b:2>"}, // 16
				{"+", "<b:1>"}, // 17
				{"+", "<c:2>"}, // 18
				{"+", "<c:1>"}, // 19
				{"+", "."},     // 20
				{"-", "<c:2>"}, // 21
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
					got = fmt.Sprintf("<%s:%d>", m.Key().UserKey, m.Key().SeqNum())
				}
				if got != c.expected {
					t.Fatalf("%d: got  %q\nwant %q", i, got, c.expected)
				}
			}
		})
	}
}

func TestMergingIterNextPrevUserKey(t *testing.T) {
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
			m := newMergingIter(db.DefaultComparer.Compare, iters...)
			m.First()

			// These test cases are shared with TestMemTableIterNextPrevUserKey.
			testCases := []struct {
				dir      string
				expected string
			}{
				{"+", "<b:2>"}, // 0
				{"-", "<a:2>"}, // 1
				{"-", "."},     // 2
				{"+", "<a:2>"}, // 3
				{"+", "<b:2>"}, // 4
				{"+", "<c:2>"}, // 5
				{"+", "."},     // 6
				{"-", "<c:2>"}, // 7
				{"-", "<b:2>"}, // 8
				{"-", "<a:2>"}, // 9
				{"+", "<b:2>"}, // 10
				{"+", "<c:2>"}, // 11
				{"-", "<b:2>"}, // 12
				{"+", "<c:2>"}, // 13
				{"+", "."},     // 14
				{"-", "<c:2>"}, // 14
			}
			for i, c := range testCases {
				switch c.dir {
				case "+":
					m.NextUserKey()
				case "-":
					m.PrevUserKey()
				default:
					t.Fatalf("unexpected direction: %q", c.dir)
				}
				var got string
				if !m.Valid() {
					got = "."
				} else {
					got = fmt.Sprintf("<%s:%d>", m.Key().UserKey, m.Key().SeqNum())
				}
				if got != c.expected {
					t.Fatalf("%d: got  %q\nwant %q", i, got, c.expected)
				}
			}
		})
	}
}

func buildMergingIterTables(
	b *testing.B, blockSize, restartInterval, count int,
) ([]*table.Reader, [][]byte) {
	mem := storage.NewMem()
	files := make([]storage.File, count)
	for i := range files {
		f, err := mem.Create(fmt.Sprintf("bench%d", i))
		if err != nil {
			b.Fatal(err)
		}
		defer f.Close()
		files[i] = f
	}

	writers := make([]*table.Writer, len(files))
	for i := range files {
		writers[i] = table.NewWriter(files[i], &db.Options{
			BlockRestartInterval: restartInterval,
			BlockSize:            blockSize,
			Compression:          db.NoCompression,
		})
	}

	estimatedSize := func() uint64 {
		var sum uint64
		for _, w := range writers {
			sum += w.EstimatedSize()
		}
		return sum
	}

	var keys [][]byte
	var ikey db.InternalKey
	targetSize := uint64(count * (2 << 20))
	for i := 0; estimatedSize() < targetSize; i++ {
		key := []byte(fmt.Sprintf("%08d", i))
		keys = append(keys, key)
		ikey.UserKey = key
		j := rand.Intn(len(writers))
		w := writers[j]
		w.Add(ikey, nil)
	}

	for _, w := range writers {
		if err := w.Close(); err != nil {
			b.Fatal(err)
		}
	}

	cache := cache.NewBlockCache(128 << 20)
	readers := make([]*table.Reader, len(files))
	for i := range files {
		f, err := mem.Open(fmt.Sprintf("bench%d", i))
		if err != nil {
			b.Fatal(err)
		}
		readers[i] = table.NewReader(f, uint64(i), &db.Options{
			Cache: cache,
		})
	}
	return readers, keys
}

func BenchmarkMergingIterSeekGE(b *testing.B) {
	const blockSize = 32 << 10

	for _, restartInterval := range []int{16} {
		b.Run(fmt.Sprintf("restart=%d", restartInterval),
			func(b *testing.B) {
				for _, count := range []int{1, 2, 3, 4, 5} {
					b.Run(fmt.Sprintf("count=%d", count),
						func(b *testing.B) {
							readers, keys := buildMergingIterTables(b, blockSize, restartInterval, count)
							iters := make([]db.InternalIterator, len(readers))
							for i := range readers {
								iters[i] = readers[i].NewIter(nil)
							}
							m := newMergingIter(db.DefaultComparer.Compare, iters...)
							rng := rand.New(rand.NewSource(time.Now().UnixNano()))

							b.ResetTimer()
							var ikey db.InternalKey
							for i := 0; i < b.N; i++ {
								ikey.UserKey = keys[rng.Intn(len(keys))]
								m.SeekGE(ikey)
							}
						})
				}
			})
	}
}

func BenchmarkMergingIterNext(b *testing.B) {
	const blockSize = 32 << 10

	for _, restartInterval := range []int{16} {
		b.Run(fmt.Sprintf("restart=%d", restartInterval),
			func(b *testing.B) {
				for _, count := range []int{1, 2, 3, 4, 5} {
					b.Run(fmt.Sprintf("count=%d", count),
						func(b *testing.B) {
							readers, _ := buildMergingIterTables(b, blockSize, restartInterval, count)
							iters := make([]db.InternalIterator, len(readers))
							for i := range readers {
								iters[i] = readers[i].NewIter(nil)
							}
							m := newMergingIter(db.DefaultComparer.Compare, iters...)

							b.ResetTimer()
							for i := 0; i < b.N; i++ {
								if !m.Valid() {
									m.First()
								}
								m.Next()
							}
						})
				}
			})
	}
}

func BenchmarkMergingIterPrev(b *testing.B) {
	const blockSize = 32 << 10

	for _, restartInterval := range []int{16} {
		b.Run(fmt.Sprintf("restart=%d", restartInterval),
			func(b *testing.B) {
				for _, count := range []int{1, 2, 3, 4, 5} {
					b.Run(fmt.Sprintf("count=%d", count),
						func(b *testing.B) {
							readers, _ := buildMergingIterTables(b, blockSize, restartInterval, count)
							iters := make([]db.InternalIterator, len(readers))
							for i := range readers {
								iters[i] = readers[i].NewIter(nil)
							}
							m := newMergingIter(db.DefaultComparer.Compare, iters...)

							b.ResetTimer()
							for i := 0; i < b.N; i++ {
								if !m.Valid() {
									m.Last()
								}
								m.Prev()
							}
						})
				}
			})
	}
}
