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
	"github.com/cockroachdb/pebble/internal/manifest"
	"github.com/cockroachdb/pebble/internal/rangedel"
	"github.com/cockroachdb/pebble/sstable"
	"github.com/cockroachdb/pebble/vfs"
	"github.com/stretchr/testify/require"
	"golang.org/x/exp/rand"
)

const (
	level = 1
)

func TestLevelIter(t *testing.T) {
	var iters []*fakeIter
	var files manifest.LevelSlice

	newIters := func(
		file *manifest.FileMetadata, opts *IterOptions, bytesIterated *uint64,
	) (internalIterator, internalIterator, error) {
		f := *iters[file.FileNum]
		f.lower = opts.GetLowerBound()
		f.upper = opts.GetUpperBound()
		return &f, nil, nil
	}

	datadriven.RunTest(t, "testdata/level_iter", func(d *datadriven.TestData) string {
		switch d.Cmd {
		case "define":
			iters = nil
			var metas []*fileMetadata
			for _, line := range strings.Split(d.Input, "\n") {
				f := &fakeIter{}
				for _, key := range strings.Fields(line) {
					j := strings.Index(key, ":")
					f.keys = append(f.keys, base.ParseInternalKey(key[:j]))
					f.vals = append(f.vals, []byte(key[j+1:]))
				}
				iters = append(iters, f)

				meta := &fileMetadata{
					FileNum: FileNum(len(metas)),
				}
				meta.Smallest = f.keys[0]
				meta.Largest = f.keys[len(f.keys)-1]
				metas = append(metas, meta)
			}
			files = manifest.NewLevelSliceKeySorted(base.DefaultComparer.Compare, metas)

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

			iter := newLevelIter(opts, DefaultComparer.Compare,
				func(a []byte) int { return len(a) }, newIters, files.Iter(), manifest.Level(level),
				nil)
			defer iter.Close()
			// Fake up the range deletion initialization.
			iter.initRangeDel(new(internalIterator))
			iter.disableInvariants = true
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
				file *manifest.FileMetadata, opts *IterOptions, bytesIterated *uint64,
			) (internalIterator, internalIterator, error) {
				tableOpts = opts
				return newIters(file, opts, nil)
			}

			iter := newLevelIter(opts, DefaultComparer.Compare,
				func(a []byte) int { return len(a) }, newIters2, files.Iter(),
				manifest.Level(level), nil)
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
	metas   []*fileMetadata
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
	file *manifest.FileMetadata, opts *IterOptions, _ *uint64,
) (internalIterator, internalIterator, error) {
	iter, err := lt.readers[file.FileNum].NewIter(opts.LowerBound, opts.UpperBound)
	if err != nil {
		return nil, nil, err
	}
	rangeDelIter, err := lt.readers[file.FileNum].NewRawRangeDelIter()
	if err != nil {
		return nil, nil, err
	}
	return iter, rangeDelIter, nil
}

func (lt *levelIterTest) runClear(d *datadriven.TestData) string {
	lt.mem = vfs.NewMem()
	for _, r := range lt.readers {
		r.Close()
	}
	lt.readers = nil
	lt.metas = nil
	return ""
}

func (lt *levelIterTest) runBuild(d *datadriven.TestData) string {
	fileNum := FileNum(len(lt.readers))
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
		Cmp:    lt.cmp.Compare,
		Format: lt.cmp.FormatKey,
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
	lt.metas = append(lt.metas, &fileMetadata{
		FileNum:  fileNum,
		Smallest: meta.Smallest(lt.cmp.Compare),
		Largest:  meta.Largest(lt.cmp.Compare),
	})

	var buf bytes.Buffer
	for _, f := range lt.metas {
		fmt.Fprintf(&buf, "%d: %s-%s\n", f.FileNum, f.Smallest, f.Largest)
	}
	return buf.String()
}

func TestLevelIterBoundaries(t *testing.T) {
	lt := newLevelIterTest()
	defer lt.runClear(nil)

	var iter *levelIter
	datadriven.RunTest(t, "testdata/level_iter_boundaries", func(d *datadriven.TestData) string {
		switch d.Cmd {
		case "clear":
			return lt.runClear(d)

		case "build":
			return lt.runBuild(d)

		case "iter":
			// The save and continue parameters allow us to save the iterator
			// for later continued use.
			save := false
			cont := false
			for _, arg := range d.CmdArgs {
				switch arg.Key {
				case "save":
					save = true
				case "continue":
					cont = true
				default:
					return fmt.Sprintf("%s: unknown arg: %s", d.Cmd, arg.Key)
				}
			}
			if !cont && iter != nil {
				return fmt.Sprintf("preceding iter was not closed")
			}
			if cont && iter == nil {
				return fmt.Sprintf("no existing iter")
			}
			if iter == nil {
				slice := manifest.NewLevelSliceKeySorted(lt.cmp.Compare, lt.metas)
				iter = newLevelIter(IterOptions{}, DefaultComparer.Compare,
					func(a []byte) int { return len(a) }, lt.newIters, slice.Iter(),
					manifest.Level(level), nil)
				// Fake up the range deletion initialization.
				iter.initRangeDel(new(internalIterator))
			}
			if !save {
				defer func() {
					iter.Close()
					iter = nil
				}()
			}
			return runInternalIterCmd(d, iter, iterCmdVerboseKey)

		case "file-pos":
			// Returns the FileNum at which the iterator is positioned.
			if iter == nil {
				return "nil levelIter"
			}
			if iter.iterFile == nil {
				return "nil iterFile"
			}
			return fmt.Sprintf("file %d", iter.iterFile.FileNum)

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

func (i *levelIterTestIter) String() string {
	return "level-iter-test"
}

func (i *levelIterTestIter) SeekGE(key []byte) (*InternalKey, []byte) {
	ikey, val := i.levelIter.SeekGE(key)
	return i.rangeDelSeek(key, ikey, val, 1)
}

func (i *levelIterTestIter) SeekPrefixGE(
	prefix, key []byte, trySeekUsingNext bool,
) (*base.InternalKey, []byte) {
	ikey, val := i.levelIter.SeekPrefixGE(prefix, key, trySeekUsingNext)
	return i.rangeDelSeek(key, ikey, val, 1)
}

func (i *levelIterTestIter) SeekLT(key []byte) (*InternalKey, []byte) {
	ikey, val := i.levelIter.SeekLT(key)
	return i.rangeDelSeek(key, ikey, val, -1)
}

func TestLevelIterSeek(t *testing.T) {
	lt := newLevelIterTest()
	defer lt.runClear(nil)

	datadriven.RunTest(t, "testdata/level_iter_seek", func(d *datadriven.TestData) string {
		switch d.Cmd {
		case "clear":
			return lt.runClear(d)

		case "build":
			return lt.runBuild(d)

		case "iter":
			slice := manifest.NewLevelSliceKeySorted(lt.cmp.Compare, lt.metas)
			iter := &levelIterTestIter{
				levelIter: newLevelIter(IterOptions{}, DefaultComparer.Compare,
					func(a []byte) int { return len(a) }, lt.newIters, slice.Iter(),
					manifest.Level(level), nil),
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
) ([]*sstable.Reader, manifest.LevelSlice, [][]byte, func()) {
	mem := vfs.NewMem()
	files := make([]vfs.File, count)
	for i := range files {
		f, err := mem.Create(fmt.Sprintf("bench%d", i))
		if err != nil {
			b.Fatal(err)
		}
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
	defer opts.Cache.Unref()
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

	cleanup := func() {
		for _, r := range readers {
			require.NoError(b, r.Close())
		}
	}

	meta := make([]*fileMetadata, len(readers))
	for i := range readers {
		iter, err := readers[i].NewIter(nil /* lower */, nil /* upper */)
		require.NoError(b, err)
		key, _ := iter.First()
		meta[i] = &fileMetadata{}
		meta[i].FileNum = FileNum(i)
		meta[i].Smallest = *key
		key, _ = iter.Last()
		meta[i].Largest = *key
	}
	slice := manifest.NewLevelSliceKeySorted(base.DefaultComparer.Compare, meta)
	return readers, slice, keys, cleanup
}

func BenchmarkLevelIterSeekGE(b *testing.B) {
	const blockSize = 32 << 10

	for _, restartInterval := range []int{16} {
		b.Run(fmt.Sprintf("restart=%d", restartInterval),
			func(b *testing.B) {
				for _, count := range []int{5} {
					b.Run(fmt.Sprintf("count=%d", count),
						func(b *testing.B) {
							readers, metas, keys, cleanup := buildLevelIterTables(b, blockSize, restartInterval, count)
							defer cleanup()
							newIters := func(
								file *manifest.FileMetadata, _ *IterOptions, _ *uint64,
							) (internalIterator, internalIterator, error) {
								iter, err := readers[file.FileNum].NewIter(nil /* lower */, nil /* upper */)
								return iter, nil, err
							}
							l := newLevelIter(IterOptions{}, DefaultComparer.Compare, nil, newIters, metas.Iter(), manifest.Level(level), nil)
							rng := rand.New(rand.NewSource(uint64(time.Now().UnixNano())))

							b.ResetTimer()
							for i := 0; i < b.N; i++ {
								l.SeekGE(keys[rng.Intn(len(keys))])
							}
							l.Close()
						})
				}
			})
	}
}

// A benchmark that simulates the behavior of a levelIter being used as part
// of a mergingIter where narrow bounds are repeatedly set and used to Seek
// and then iterate over the keys within the bounds. This resembles MVCC
// scanning by CockroachDB when doing a lookup/index join with a large number
// of left rows, that are batched and reuse the same iterator, and which can
// have good locality of access. This results in the successive bounds being
// in the same file.
func BenchmarkLevelIterSeqSeekGEWithBounds(b *testing.B) {
	const blockSize = 32 << 10

	for _, restartInterval := range []int{16} {
		b.Run(fmt.Sprintf("restart=%d", restartInterval),
			func(b *testing.B) {
				for _, count := range []int{5} {
					b.Run(fmt.Sprintf("count=%d", count),
						func(b *testing.B) {
							readers, metas, keys, cleanup :=
								buildLevelIterTables(b, blockSize, restartInterval, count)
							defer cleanup()
							// This newIters is cheaper than in practice since it does not do
							// tableCacheShard.findNode.
							newIters := func(
								file *manifest.FileMetadata, opts *IterOptions, _ *uint64,
							) (internalIterator, internalIterator, error) {
								iter, err := readers[file.FileNum].NewIter(
									opts.LowerBound, opts.UpperBound)
								return iter, nil, err
							}
							l := newLevelIter(IterOptions{}, DefaultComparer.Compare, nil, newIters, metas.Iter(), manifest.Level(level), nil)
							// Fake up the range deletion initialization, to resemble the usage
							// in a mergingIter.
							l.initRangeDel(new(internalIterator))
							keyCount := len(keys)
							b.ResetTimer()
							for i := 0; i < b.N; i++ {
								pos := i % (keyCount - 1)
								l.SetBounds(keys[pos], keys[pos+1])
								// SeekGE will return keys[pos].
								k, _ := l.SeekGE(keys[pos])
								// Next() will get called once and return nil.
								for k != nil {
									k, _ = l.Next()
								}
							}
							l.Close()
						})
				}
			})
	}
}

// BenchmarkLevelIterSeqSeekPrefixGE simulates the behavior of a levelIter
// being used as part of a mergingIter where SeekPrefixGE is used to seek in a
// monotonically increasing manner. This resembles key-value lookups done by
// CockroachDB when evaluating Put operations.
func BenchmarkLevelIterSeqSeekPrefixGE(b *testing.B) {
	const blockSize = 32 << 10
	const restartInterval = 16
	readers, metas, keys, cleanup :=
		buildLevelIterTables(b, blockSize, restartInterval, 5)
	defer cleanup()
	// This newIters is cheaper than in practice since it does not do
	// tableCacheShard.findNode.
	newIters := func(
		file *manifest.FileMetadata, opts *IterOptions, _ *uint64,
	) (internalIterator, internalIterator, error) {
		iter, err := readers[file.FileNum].NewIter(
			opts.LowerBound, opts.UpperBound)
		return iter, nil, err
	}

	for _, skip := range []int{1, 2, 4, 8, 16} {
		for _, useNext := range []bool{false, true} {
			b.Run(fmt.Sprintf("skip=%d/use-next=%t", skip, useNext),
				func(b *testing.B) {
					l := newLevelIter(IterOptions{}, DefaultComparer.Compare,
						func(a []byte) int { return len(a) }, newIters, metas.Iter(),
						manifest.Level(level), nil)
					// Fake up the range deletion initialization, to resemble the usage
					// in a mergingIter.
					l.initRangeDel(new(internalIterator))
					keyCount := len(keys)
					pos := 0
					l.SeekPrefixGE(keys[pos], keys[pos], false)
					b.ResetTimer()
					for i := 0; i < b.N; i++ {
						pos += skip
						trySeekUsingNext := useNext
						if pos >= keyCount {
							pos = 0
							trySeekUsingNext = false
						}
						// SeekPrefixGE will return keys[pos].
						l.SeekPrefixGE(keys[pos], keys[pos], trySeekUsingNext)
					}
					b.StopTimer()
					l.Close()
				})
		}
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
							readers, metas, _, cleanup := buildLevelIterTables(b, blockSize, restartInterval, count)
							defer cleanup()
							newIters := func(
								file *manifest.FileMetadata, _ *IterOptions, _ *uint64,
							) (internalIterator, internalIterator, error) {
								iter, err := readers[file.FileNum].NewIter(nil /* lower */, nil /* upper */)
								return iter, nil, err
							}
							l := newLevelIter(IterOptions{}, DefaultComparer.Compare, nil, newIters, metas.Iter(), manifest.Level(level), nil)

							b.ResetTimer()
							for i := 0; i < b.N; i++ {
								key, _ := l.Next()
								if key == nil {
									key, _ = l.First()
								}
								_ = key
							}
							l.Close()
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
							readers, metas, _, cleanup := buildLevelIterTables(b, blockSize, restartInterval, count)
							defer cleanup()
							newIters := func(
								file *manifest.FileMetadata, _ *IterOptions, _ *uint64,
							) (internalIterator, internalIterator, error) {
								iter, err := readers[file.FileNum].NewIter(nil /* lower */, nil /* upper */)
								return iter, nil, err
							}
							l := newLevelIter(IterOptions{}, DefaultComparer.Compare, nil, newIters, metas.Iter(), manifest.Level(level), nil)

							b.ResetTimer()
							for i := 0; i < b.N; i++ {
								key, _ := l.Prev()
								if key == nil {
									key, _ = l.Last()
								}
								_ = key
							}
							l.Close()
						})
				}
			})
	}
}
