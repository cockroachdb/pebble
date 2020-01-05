// Copyright 2018 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package pebble

import (
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/cockroachdb/pebble/internal/base"
	"github.com/cockroachdb/pebble/internal/datadriven"
	"github.com/cockroachdb/pebble/internal/rangedel"
	"github.com/cockroachdb/pebble/sstable"
	"github.com/cockroachdb/pebble/vfs"
	"golang.org/x/exp/rand"
)

func TestMergingIter(t *testing.T) {
	newFunc := func(iters ...internalIterator) internalIterator {
		return newMergingIter(nil /* logger */, DefaultComparer.Compare, iters...)
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
	var def string
	datadriven.RunTest(t, "testdata/merging_iter_seek", func(d *datadriven.TestData) string {
		switch d.Cmd {
		case "define":
			def = d.Input
			return ""

		case "iter":
			var iters []internalIterator
			for _, line := range strings.Split(def, "\n") {
				f := &fakeIter{}
				for _, key := range strings.Fields(line) {
					j := strings.Index(key, ":")
					f.keys = append(f.keys, base.ParseInternalKey(key[:j]))
					f.vals = append(f.vals, []byte(key[j+1:]))
				}
				iters = append(iters, f)
			}

			iter := newMergingIter(nil /* logger */, DefaultComparer.Compare, iters...)
			defer iter.Close()
			return runInternalIterCmd(d, iter)

		default:
			return fmt.Sprintf("unknown command: %s", d.Cmd)
		}
	})
}

func TestMergingIterNextPrev(t *testing.T) {
	// The data is the same in each of these cases, but divided up amongst the
	// iterators differently. This data must match the definition in
	// testdata/internal_iter_next.
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
					iters := make([]internalIterator, len(c))
					for i := range c {
						f := &fakeIter{}
						iters[i] = f
						for _, key := range strings.Fields(c[i]) {
							j := strings.Index(key, ":")
							f.keys = append(f.keys, base.ParseInternalKey(key[:j]))
							f.vals = append(f.vals, []byte(key[j+1:]))
						}
					}

					iter := newMergingIter(nil /* logger */, DefaultComparer.Compare, iters...)
					defer iter.Close()
					return runInternalIterCmd(d, iter)

				default:
					return fmt.Sprintf("unknown command: %s", d.Cmd)
				}
			})
		})
	}
}

func TestMergingIterCornerCases(t *testing.T) {
	memFS := vfs.NewMem()
	cmp := DefaultComparer.Compare
	var v version
	// Indexed by fileNum.
	var readers []*sstable.Reader
	var fileNum uint64
	newIters :=
		func(meta *fileMetadata, opts *IterOptions, bytesIterated *uint64) (internalIterator, internalIterator, error) {
			r := readers[meta.FileNum]
			return r.NewIter(opts.GetLowerBound(), opts.GetUpperBound()), r.NewRangeDelIter(), nil
		}
	datadriven.RunTest(t, "testdata/merging_iter", func(d *datadriven.TestData) string {
		switch d.Cmd {
		case "define":
			lines := strings.Split(d.Input, "\n")
			v = version{}
			var level int
			for i := 0; i < len(lines); i++ {
				line := lines[i]
				line = strings.TrimSpace(line)
				if line == "L" || line == "L0" {
					// start next level
					level++
					continue
				}
				files := &v.Files[level]
				keys := strings.Fields(line)
				smallestKey := base.ParseInternalKey(keys[0])
				largestKey := base.ParseInternalKey(keys[1])
				*files = append(*files, fileMetadata{
					FileNum:  fileNum,
					Smallest: smallestKey,
					Largest:  largestKey,
				})

				i++
				line = lines[i]
				line = strings.TrimSpace(line)
				name := fmt.Sprint(fileNum)
				fileNum++
				f, err := memFS.Create(name)
				if err != nil {
					return err.Error()
				}
				w := sstable.NewWriter(f, sstable.WriterOptions{})
				var tombstones []rangedel.Tombstone
				frag := rangedel.Fragmenter{
					Cmp: cmp,
					Emit: func(fragmented []rangedel.Tombstone) {
						tombstones = append(tombstones, fragmented...)
					},
				}
				keyvalues := strings.Fields(line)
				for _, kv := range keyvalues {
					j := strings.Index(kv, ":")
					ikey := base.ParseInternalKey(kv[:j])
					value := []byte(kv[j+1:])
					switch ikey.Kind() {
					case InternalKeyKindRangeDelete:
						frag.Add(ikey, value)
					default:
						if err := w.Add(ikey, value); err != nil {
							return err.Error()
						}
					}
				}
				frag.Finish()
				for _, v := range tombstones {
					if err := w.Add(v.Start, v.End); err != nil {
						return err.Error()
					}
				}
				if err := w.Close(); err != nil {
					return err.Error()
				}
				f, err = memFS.Open(name)
				if err != nil {
					return err.Error()
				}
				r, err := sstable.NewReader(f, sstable.ReaderOptions{})
				if err != nil {
					return err.Error()
				}
				readers = append(readers, r)
			}

			return v.DebugString(DefaultComparer.Format)
		case "iter":
			levelIters := make([]mergingIterLevel, 0, len(v.Files))
			for _, l := range v.Files {
				if len(l) == 0 {
					continue
				}
				li := &levelIter{}
				li.init(IterOptions{}, cmp, newIters, l, nil)
				i := len(levelIters)
				levelIters = append(levelIters, mergingIterLevel{iter: li})
				li.initRangeDel(&levelIters[i].rangeDelIter)
				li.initSmallestLargestUserKey(
					&levelIters[i].smallestUserKey, &levelIters[i].largestUserKey, &levelIters[i].isLargestUserKeyRangeDelSentinel)
			}
			miter := &mergingIter{}
			miter.init(nil /* opts */, cmp, levelIters...)
			defer miter.Close()
			return runInternalIterCmd(d, miter, iterCmdVerboseKey)
		default:
			return fmt.Sprintf("unknown command: %s", d.Cmd)
		}
	})
}

func buildMergingIterTables(
	b *testing.B, blockSize, restartInterval, count int,
) ([]*sstable.Reader, [][]byte) {
	mem := vfs.NewMem()
	files := make([]vfs.File, count)
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
		writers[i] = sstable.NewWriter(files[i], sstable.WriterOptions{
			BlockRestartInterval: restartInterval,
			BlockSize:            blockSize,
			Compression:          NoCompression,
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
	var ikey InternalKey
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

	opts := sstable.ReaderOptions{Cache: NewCache(128 << 20)}
	readers := make([]*sstable.Reader, len(files))
	for i := range files {
		f, err := mem.Open(fmt.Sprintf("bench%d", i))
		if err != nil {
			b.Fatal(err)
		}
		readers[i], err = sstable.NewReader(f, opts)
		if err != nil {
			b.Fatal(err)
		}
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
							iters := make([]internalIterator, len(readers))
							for i := range readers {
								iters[i] = readers[i].NewIter(nil /* lower */, nil /* upper */)
							}
							m := newMergingIter(nil /* logger */, DefaultComparer.Compare, iters...)
							rng := rand.New(rand.NewSource(uint64(time.Now().UnixNano())))

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
							iters := make([]internalIterator, len(readers))
							for i := range readers {
								iters[i] = readers[i].NewIter(nil /* lower */, nil /* upper */)
							}
							m := newMergingIter(nil /* logger */, DefaultComparer.Compare, iters...)

							b.ResetTimer()
							for i := 0; i < b.N; i++ {
								key, _ := m.Next()
								if key == nil {
									key, _ = m.First()
								}
								_ = key
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
							iters := make([]internalIterator, len(readers))
							for i := range readers {
								iters[i] = readers[i].NewIter(nil /* lower */, nil /* upper */)
							}
							m := newMergingIter(nil /* logger */, DefaultComparer.Compare, iters...)

							b.ResetTimer()
							for i := 0; i < b.N; i++ {
								key, _ := m.Prev()
								if key == nil {
									key, _ = m.Last()
								}
								_ = key
							}
						})
				}
			})
	}
}
