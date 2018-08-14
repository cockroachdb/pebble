// Copyright 2018 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package pebble

import (
	"bytes"
	"fmt"
	"math/rand"
	"strings"
	"testing"
	"time"

	"github.com/petermattis/pebble/cache"
	"github.com/petermattis/pebble/datadriven"
	"github.com/petermattis/pebble/db"
	"github.com/petermattis/pebble/sstable"
	"github.com/petermattis/pebble/storage"
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
			"a0",
			"a0:0;a1:1;a2:2",
			"<a0:0><a1:1><a2:2>.",
			".",
		},
		{
			"a1",
			"a0:0;a1:1;a2:2",
			"<a1:1><a2:2>.",
			"<a0:0>.",
		},
		{
			"a2",
			"a0:0;a1:1;a2:2",
			"<a2:2>.",
			"<a1:1><a0:0>.",
		},
		{
			"a3",
			"a0:0;a1:1;a2:2",
			".",
			"<a2:2><a1:1><a0:0>.",
		},
		{
			"a2",
			"a0:0,b3:3;a1:1;a2:2",
			"<a2:2><b3:3>.",
			"<a1:1><a0:0>.",
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
			key := []byte(tc.key)
			for iter.SeekGE(key); iter.Valid(); iter.Next() {
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
			for iter.SeekLT(key); iter.Valid(); iter.Prev() {
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

func TestMergingIter2(t *testing.T) {
	// The data is the same in each of these cases, but divided up amongst the
	// iterators differently. This data must match the definition in
	// testdata/internal_iter.
	iterCases := [][]string{
		[]string{
			"a.SET.2:2 a.SET.1:1 b.SET.2:2 b.SET.1:1 c.SET.2:2 c.SET.1:1",
		},
		[]string{
			"a.SET.2:2 b.SET.2:2 c.SET.2:2",
			"a.SET.1:1 b.SET.1:1 c.SET.1:1",
		},
		[]string{
			"a.SET.2:2 b.SET.2:2",
			"a.SET.1:1 b.SET.1:1",
			"c.SET.2:2 c.SET.1:1",
		},
		[]string{
			"a.SET.2:2",
			"a.SET.1:1",
			"b.SET.2:2",
			"b.SET.1:1",
			"c.SET.2:2",
			"c.SET.1:1",
		},
	}

	for _, c := range iterCases {
		t.Run("", func(t *testing.T) {
			datadriven.RunTest(t, "testdata/internal_iter_next", func(d *datadriven.TestData) string {
				switch d.Cmd {
				case "define":
					// Ignore. We've defined the iterator data above.
					return ""

				case "iter":
					iters := make([]db.InternalIterator, len(c))
					for i := range c {
						f := &fakeIter{}
						iters[i] = f
						for _, key := range strings.Fields(c[i]) {
							j := strings.Index(key, ":")
							f.keys = append(f.keys, db.ParseInternalKey(key[:j]))
							f.vals = append(f.vals, []byte(key[j+1:]))
						}
					}

					iter := newMergingIter(db.DefaultComparer.Compare, iters...)
					defer iter.Close()
					return runInternalIterCmd(d, iter)

				default:
					t.Fatalf("unknown command: %s", d.Cmd)
				}

				return ""
			})
		})
	}
}

func buildMergingIterTables(
	b *testing.B, blockSize, restartInterval, count int,
) ([]*sstable.Reader, [][]byte) {
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

	writers := make([]*sstable.Writer, len(files))
	for i := range files {
		writers[i] = sstable.NewWriter(files[i], nil, db.LevelOptions{
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

	cache := cache.New(128 << 20)
	readers := make([]*sstable.Reader, len(files))
	for i := range files {
		f, err := mem.Open(fmt.Sprintf("bench%d", i))
		if err != nil {
			b.Fatal(err)
		}
		readers[i] = sstable.NewReader(f, uint64(i), &db.Options{
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
							for i := 0; i < b.N; i++ {
								m.SeekGE(keys[rng.Intn(len(keys))])
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
