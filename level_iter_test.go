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
	"github.com/petermattis/pebble/db"
	"github.com/petermattis/pebble/internal/datadriven"
	"github.com/petermattis/pebble/internal/rangedel"
	"github.com/petermattis/pebble/sstable"
	"github.com/petermattis/pebble/storage"
)

func TestLevelIter(t *testing.T) {
	var iters []*fakeIter
	var files []fileMetadata

	newIters := func(
		meta *fileMetadata, _ *db.IterOptions,
	) (internalIterator, internalIterator, error) {
		f := *iters[meta.fileNum]
		return &f, nil, nil
	}

	datadriven.RunTest(t, "testdata/level_iter", func(d *datadriven.TestData) string {
		switch d.Cmd {
		case "define":
			iters = nil
			files = nil

			for _, line := range strings.Split(d.Input, "\n") {
				f := &fakeIter{}
				for _, key := range strings.Fields(line) {
					j := strings.Index(key, ":")
					f.keys = append(f.keys, db.ParseInternalKey(key[:j]))
					f.vals = append(f.vals, []byte(key[j+1:]))
				}
				iters = append(iters, f)

				meta := fileMetadata{
					fileNum: uint64(len(files)),
				}
				meta.smallest = f.keys[0]
				meta.largest = f.keys[len(f.keys)-1]
				files = append(files, meta)
			}

			return ""

		case "iter":
			var opts db.IterOptions
			for _, arg := range d.CmdArgs {
				if len(arg.Vals) != 1 {
					return fmt.Sprintf("%s: %s=<value>", d.Cmd, arg.Key)
				}
				switch arg.Key {
				case "lower":
					opts.LowerBound = []byte(arg.Vals[0])
				case "upper":
					opts.UpperBound = []byte(arg.Vals[0])
				default:
					return fmt.Sprintf("%s: unknown arg: %s", d.Cmd, arg.Key)
				}
			}

			iter := newLevelIter(&opts, db.DefaultComparer.Compare, newIters, files)
			defer iter.Close()
			return runInternalIterCmd(d, iter)

		default:
			return fmt.Sprintf("unknown command: %s", d.Cmd)
		}
	})
}

func TestLevelIterBoundaries(t *testing.T) {
	cmp := db.DefaultComparer.Compare
	fs := storage.NewMem()
	var readers []*sstable.Reader
	var files []fileMetadata

	newIters := func(
		meta *fileMetadata, _ *db.IterOptions,
	) (internalIterator, internalIterator, error) {
		return readers[meta.fileNum].NewIter(nil), nil, nil
	}

	datadriven.RunTest(t, "testdata/level_iter_boundaries", func(d *datadriven.TestData) string {
		switch d.Cmd {
		case "clear":
			fs = storage.NewMem()
			readers = nil
			files = nil
			return ""

		case "build":
			fileNum := uint64(len(readers))
			name := fmt.Sprint(fileNum)
			f0, err := fs.Create(name)
			if err != nil {
				return err.Error()
			}

			w := sstable.NewWriter(f0, nil, db.LevelOptions{})
			var tombstones []rangedel.Tombstone
			f := rangedel.Fragmenter{
				Cmp: cmp,
				Emit: func(fragmented []rangedel.Tombstone) {
					tombstones = append(tombstones, fragmented...)
				},
			}
			for _, key := range strings.Split(d.Input, "\n") {
				j := strings.Index(key, ":")
				ikey := db.ParseInternalKey(key[:j])
				value := []byte(key[j+1:])
				switch ikey.Kind() {
				case db.InternalKeyKindRangeDelete:
					f.Add(ikey, value)
				default:
					if err := w.Add(ikey, value); err != nil {
						return err.Error()
					}
				}
			}
			f.Finish()
			for _, v := range tombstones {
				if err := w.Add(v.Start, v.End); err != nil {
					return err.Error()
				}
			}
			if err := w.Close(); err != nil {
				return err.Error()
			}
			meta, err := w.Metadata()
			if err != nil {
				return err.Error()
			}

			f1, err := fs.Open(name)
			if err != nil {
				return err.Error()
			}
			readers = append(readers, sstable.NewReader(f1, 0, nil))
			files = append(files, fileMetadata{
				fileNum:  fileNum,
				smallest: meta.Smallest(cmp),
				largest:  meta.Largest(cmp),
			})

			var buf bytes.Buffer
			for _, f := range files {
				fmt.Fprintf(&buf, "%d: %s-%s\n", f.fileNum, f.smallest, f.largest)
			}
			return buf.String()

		case "iter":
			iter := newLevelIter(nil, db.DefaultComparer.Compare, newIters, files)
			defer iter.Close()
			// Fake up the range deletion initialization.
			iter.initRangeDel(new(internalIterator))
			return runInternalIterCmd(d, iter, iterCmdVerboseKey)

		default:
			return fmt.Sprintf("unknown command: %s", d.Cmd)
		}
	})
}

func buildLevelIterTables(
	b *testing.B, blockSize, restartInterval, count int,
) ([]*sstable.Reader, []fileMetadata, [][]byte) {
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

	var keys [][]byte
	var i int
	const targetSize = 2 << 20
	for _, w := range writers {
		for ; w.EstimatedSize() < targetSize; i++ {
			key := []byte(fmt.Sprintf("%08d", i))
			keys = append(keys, key)
			ikey := db.MakeInternalKey(key, 0, db.InternalKeyKindSet)
			w.Add(ikey, nil)
		}
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

	meta := make([]fileMetadata, len(readers))
	for i := range readers {
		iter := readers[i].NewIter(nil)
		iter.First()
		meta[i].fileNum = uint64(i)
		meta[i].smallest = iter.Key()
		iter.Last()
		meta[i].largest = iter.Key()
	}
	return readers, meta, keys
}

func BenchmarkLevelIterSeekGE(b *testing.B) {
	const blockSize = 32 << 10

	for _, restartInterval := range []int{16} {
		b.Run(fmt.Sprintf("restart=%d", restartInterval),
			func(b *testing.B) {
				for _, count := range []int{5} {
					b.Run(fmt.Sprintf("count=%d", count),
						func(b *testing.B) {
							readers, files, keys := buildLevelIterTables(b, blockSize, restartInterval, count)
							newIters := func(
								meta *fileMetadata, _ *db.IterOptions,
							) (internalIterator, internalIterator, error) {
								return readers[meta.fileNum].NewIter(nil), nil, nil
							}
							l := newLevelIter(nil, db.DefaultComparer.Compare, newIters, files)
							rng := rand.New(rand.NewSource(time.Now().UnixNano()))

							b.ResetTimer()
							for i := 0; i < b.N; i++ {
								l.SeekGE(keys[rng.Intn(len(keys))])
							}
						})
				}
			})
	}
}

func BenchmarkLevelIterNext(b *testing.B) {
	const blockSize = 32 << 10

	for _, restartInterval := range []int{16} {
		b.Run(fmt.Sprintf("restart=%d", restartInterval),
			func(b *testing.B) {
				for _, count := range []int{5} {
					b.Run(fmt.Sprintf("count=%d", count),
						func(b *testing.B) {
							readers, files, _ := buildLevelIterTables(b, blockSize, restartInterval, count)
							newIters := func(
								meta *fileMetadata, _ *db.IterOptions,
							) (internalIterator, internalIterator, error) {
								return readers[meta.fileNum].NewIter(nil), nil, nil
							}
							l := newLevelIter(nil, db.DefaultComparer.Compare, newIters, files)

							b.ResetTimer()
							for i := 0; i < b.N; i++ {
								if !l.Valid() {
									l.First()
								}
								l.Next()
							}
						})
				}
			})
	}
}

func BenchmarkLevelIterPrev(b *testing.B) {
	const blockSize = 32 << 10

	for _, restartInterval := range []int{16} {
		b.Run(fmt.Sprintf("restart=%d", restartInterval),
			func(b *testing.B) {
				for _, count := range []int{5} {
					b.Run(fmt.Sprintf("count=%d", count),
						func(b *testing.B) {
							readers, files, _ := buildLevelIterTables(b, blockSize, restartInterval, count)
							newIters := func(
								meta *fileMetadata, _ *db.IterOptions,
							) (internalIterator, internalIterator, error) {
								return readers[meta.fileNum].NewIter(nil), nil, nil
							}
							l := newLevelIter(nil, db.DefaultComparer.Compare, newIters, files)

							b.ResetTimer()
							for i := 0; i < b.N; i++ {
								if !l.Valid() {
									l.Last()
								}
								l.Prev()
							}
						})
				}
			})
	}
}
