// Copyright 2018 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package pebble

import (
	"bytes"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/cockroachdb/pebble/bloom"
	"github.com/cockroachdb/pebble/internal/base"
	"github.com/cockroachdb/pebble/internal/datadriven"
	"github.com/cockroachdb/pebble/internal/rangedel"
	"github.com/cockroachdb/pebble/sstable"
	"github.com/cockroachdb/pebble/vfs"
	"golang.org/x/exp/rand"
)

func TestLevelIter(t *testing.T) {
	var iters []*fakeIter
	var files []fileMetadata

	newIters := func(
		meta *fileMetadata, opts *IterOptions, bytesIterated *uint64,
	) (internalIterator, internalIterator, error) {
		f := *iters[meta.FileNum]
		f.lower = opts.GetLowerBound()
		f.upper = opts.GetUpperBound()
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
					f.keys = append(f.keys, base.ParseInternalKey(key[:j]))
					f.vals = append(f.vals, []byte(key[j+1:]))
				}
				iters = append(iters, f)

				meta := fileMetadata{
					FileNum: uint64(len(files)),
				}
				meta.Smallest = f.keys[0]
				meta.Largest = f.keys[len(f.keys)-1]
				files = append(files, meta)
			}

			return ""

		case "iter":
			var opts IterOptions
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

			iter := newLevelIter(opts, DefaultComparer.Compare, newIters, files, nil)
			defer iter.Close()
			// Fake up the range deletion initialization.
			iter.initRangeDel(new(internalIterator))
			return runInternalIterCmd(d, iter, iterCmdVerboseKey)

		case "load":
			// The "load" command allows testing the iterator options passed to load
			// sstables.
			//
			// load <key> [lower=<key>] [upper=<key>]
			var opts IterOptions
			var key string
			for _, arg := range d.CmdArgs {
				if len(arg.Vals) == 0 {
					key = arg.Key
					continue
				}
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

			var tableOpts *IterOptions
			newIters2 := func(
				meta *fileMetadata, opts *IterOptions, bytesIterated *uint64,
			) (internalIterator, internalIterator, error) {
				tableOpts = opts
				return newIters(meta, opts, nil)
			}

			iter := newLevelIter(opts, DefaultComparer.Compare, newIters2, files, nil)
			iter.SeekGE([]byte(key))
			lower, upper := tableOpts.GetLowerBound(), tableOpts.GetUpperBound()
			return fmt.Sprintf("[%s,%s]\n", lower, upper)

		default:
			return fmt.Sprintf("unknown command: %s", d.Cmd)
		}
	})
}

type levelIterTest struct {
	cmp     base.Comparer
	mem     vfs.FS
	readers []*sstable.Reader
	files   []fileMetadata
}

func newLevelIterTest() *levelIterTest {
	lt := &levelIterTest{
		cmp: *DefaultComparer,
		mem: vfs.NewMem(),
	}
	lt.cmp.Split = func(a []byte) int { return len(a) }
	return lt
}

func (lt *levelIterTest) newIters(
	meta *fileMetadata, opts *IterOptions, _ *uint64,
) (internalIterator, internalIterator, error) {
	iter := lt.readers[meta.FileNum].NewIter(opts.LowerBound, opts.UpperBound)
	rangeDelIter := lt.readers[meta.FileNum].NewRangeDelIter()
	return iter, rangeDelIter, nil
}

func (lt *levelIterTest) runClear(d *datadriven.TestData) string {
	lt.mem = vfs.NewMem()
	lt.readers = nil
	lt.files = nil
	return ""
}

func (lt *levelIterTest) runBuild(d *datadriven.TestData) string {
	fileNum := uint64(len(lt.readers))
	name := fmt.Sprint(fileNum)
	f0, err := lt.mem.Create(name)
	if err != nil {
		return err.Error()
	}

	fp := bloom.FilterPolicy(10)
	w := sstable.NewWriter(f0, sstable.WriterOptions{
		Comparer:     &lt.cmp,
		FilterPolicy: fp,
	})
	var tombstones []rangedel.Tombstone
	f := rangedel.Fragmenter{
		Cmp: lt.cmp.Compare,
		Emit: func(fragmented []rangedel.Tombstone) {
			tombstones = append(tombstones, fragmented...)
		},
	}
	for _, key := range strings.Split(d.Input, "\n") {
		j := strings.Index(key, ":")
		ikey := base.ParseInternalKey(key[:j])
		value := []byte(key[j+1:])
		switch ikey.Kind() {
		case InternalKeyKindRangeDelete:
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

	f1, err := lt.mem.Open(name)
	if err != nil {
		return err.Error()
	}
	r, err := sstable.NewReader(f1, sstable.ReaderOptions{
		Filters: map[string]FilterPolicy{
			fp.Name(): fp,
		},
	})
	if err != nil {
		return err.Error()
	}
	lt.readers = append(lt.readers, r)
	lt.files = append(lt.files, fileMetadata{
		FileNum:  fileNum,
		Smallest: meta.Smallest(lt.cmp.Compare),
		Largest:  meta.Largest(lt.cmp.Compare),
	})

	var buf bytes.Buffer
	for _, f := range lt.files {
		fmt.Fprintf(&buf, "%d: %s-%s\n", f.FileNum, f.Smallest, f.Largest)
	}
	return buf.String()
}

func TestLevelIterBoundaries(t *testing.T) {
	lt := newLevelIterTest()
	datadriven.RunTest(t, "testdata/level_iter_boundaries", func(d *datadriven.TestData) string {
		switch d.Cmd {
		case "clear":
			return lt.runClear(d)

		case "build":
			return lt.runBuild(d)

		case "iter":
			iter := newLevelIter(IterOptions{}, DefaultComparer.Compare, lt.newIters, lt.files, nil)
			defer iter.Close()
			// Fake up the range deletion initialization.
			iter.initRangeDel(new(internalIterator))
			return runInternalIterCmd(d, iter, iterCmdVerboseKey)

		default:
			return fmt.Sprintf("unknown command: %s", d.Cmd)
		}
	})
}

// levelIterTestIter allows a datadriven test to use runInternalIterCmd and
// perform parallel operations on both both a levelIter and rangeDelIter.
type levelIterTestIter struct {
	*levelIter
	rangeDelIter internalIterator
}

func (i *levelIterTestIter) rangeDelSeek(
	key []byte, ikey *InternalKey, val []byte, dir int,
) (*InternalKey, []byte) {
	var tombstone rangedel.Tombstone
	if i.rangeDelIter != nil {
		if dir < 0 {
			tombstone = rangedel.SeekLE(i.levelIter.cmp, i.rangeDelIter, key, 1000)
		} else {
			tombstone = rangedel.SeekGE(i.levelIter.cmp, i.rangeDelIter, key, 1000)
		}
	}
	if ikey == nil {
		return &InternalKey{
			UserKey: []byte(fmt.Sprintf("./%s", tombstone)),
		}, nil
	}
	return &InternalKey{
		UserKey: []byte(fmt.Sprintf("%s/%s", ikey.UserKey, tombstone)),
		Trailer: ikey.Trailer,
	}, val
}

func (i *levelIterTestIter) SeekGE(key []byte) (*InternalKey, []byte) {
	ikey, val := i.levelIter.SeekGE(key)
	return i.rangeDelSeek(key, ikey, val, 1)
}

func (i *levelIterTestIter) SeekPrefixGE(prefix, key []byte) (*InternalKey, []byte) {
	ikey, val := i.levelIter.SeekPrefixGE(prefix, key)
	return i.rangeDelSeek(key, ikey, val, 1)
}

func (i *levelIterTestIter) SeekLT(key []byte) (*InternalKey, []byte) {
	ikey, val := i.levelIter.SeekLT(key)
	return i.rangeDelSeek(key, ikey, val, -1)
}

func TestLevelIterSeek(t *testing.T) {
	lt := newLevelIterTest()
	datadriven.RunTest(t, "testdata/level_iter_seek", func(d *datadriven.TestData) string {
		switch d.Cmd {
		case "clear":
			return lt.runClear(d)

		case "build":
			return lt.runBuild(d)

		case "iter":
			iter := &levelIterTestIter{
				levelIter: newLevelIter(IterOptions{}, DefaultComparer.Compare, lt.newIters, lt.files, nil),
			}
			defer iter.Close()
			iter.initRangeDel(&iter.rangeDelIter)
			return runInternalIterCmd(d, iter, iterCmdVerboseKey)

		default:
			return fmt.Sprintf("unknown command: %s", d.Cmd)
		}
	})
}

func buildLevelIterTables(
	b *testing.B, blockSize, restartInterval, count int,
) ([]*sstable.Reader, []fileMetadata, [][]byte) {
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

	var keys [][]byte
	var i int
	const targetSize = 2 << 20
	for _, w := range writers {
		for ; w.EstimatedSize() < targetSize; i++ {
			key := []byte(fmt.Sprintf("%08d", i))
			keys = append(keys, key)
			ikey := base.MakeInternalKey(key, 0, InternalKeyKindSet)
			w.Add(ikey, nil)
		}
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

	meta := make([]fileMetadata, len(readers))
	for i := range readers {
		iter := readers[i].NewIter(nil /* lower */, nil /* upper */)
		key, _ := iter.First()
		meta[i].FileNum = uint64(i)
		meta[i].Smallest = *key
		key, _ = iter.Last()
		meta[i].Largest = *key
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
								meta *fileMetadata, _ *IterOptions, _ *uint64,
							) (internalIterator, internalIterator, error) {
								return readers[meta.FileNum].NewIter(nil /* lower */, nil /* upper */), nil, nil
							}
							l := newLevelIter(IterOptions{}, DefaultComparer.Compare, newIters, files, nil)
							rng := rand.New(rand.NewSource(uint64(time.Now().UnixNano())))

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
								meta *fileMetadata, _ *IterOptions, _ *uint64,
							) (internalIterator, internalIterator, error) {
								return readers[meta.FileNum].NewIter(nil /* lower */, nil /* upper */), nil, nil
							}
							l := newLevelIter(IterOptions{}, DefaultComparer.Compare, newIters, files, nil)

							b.ResetTimer()
							for i := 0; i < b.N; i++ {
								key, _ := l.Next()
								if key == nil {
									key, _ = l.First()
								}
								_ = key
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
								meta *fileMetadata, _ *IterOptions, _ *uint64,
							) (internalIterator, internalIterator, error) {
								return readers[meta.FileNum].NewIter(nil /* lower */, nil /* upper */), nil, nil
							}
							l := newLevelIter(IterOptions{}, DefaultComparer.Compare, newIters, files, nil)

							b.ResetTimer()
							for i := 0; i < b.N; i++ {
								key, _ := l.Prev()
								if key == nil {
									key, _ = l.Last()
								}
								_ = key
							}
						})
				}
			})
	}
}
