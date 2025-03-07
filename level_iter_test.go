// Copyright 2018 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package pebble

import (
	"bytes"
	"context"
	"fmt"
	"math/rand/v2"
	"strings"
	"testing"
	"time"

	"github.com/cockroachdb/datadriven"
	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/pebble/bloom"
	"github.com/cockroachdb/pebble/internal/base"
	"github.com/cockroachdb/pebble/internal/itertest"
	"github.com/cockroachdb/pebble/internal/keyspan"
	"github.com/cockroachdb/pebble/internal/manifest"
	"github.com/cockroachdb/pebble/internal/rangedel"
	"github.com/cockroachdb/pebble/internal/rangekey"
	"github.com/cockroachdb/pebble/internal/sstableinternal"
	"github.com/cockroachdb/pebble/internal/testkeys"
	"github.com/cockroachdb/pebble/objstorage/objstorageprovider"
	"github.com/cockroachdb/pebble/sstable"
	"github.com/cockroachdb/pebble/sstable/block"
	"github.com/cockroachdb/pebble/vfs"
	"github.com/stretchr/testify/require"
)

const (
	level = 1
)

func TestLevelIter(t *testing.T) {
	var iterKVs [][]base.InternalKV
	var files manifest.LevelSlice

	newIters := func(
		_ context.Context, file *manifest.TableMetadata, opts *IterOptions, _ internalIterOpts, _ iterKinds,
	) (iterSet, error) {
		f := base.NewFakeIter(iterKVs[file.FileNum])
		f.SetBounds(opts.GetLowerBound(), opts.GetUpperBound())
		return iterSet{point: f}, nil
	}

	datadriven.RunTest(t, "testdata/level_iter", func(t *testing.T, d *datadriven.TestData) string {
		switch d.Cmd {
		case "define":
			iterKVs = nil
			var metas []*tableMetadata
			for _, line := range strings.Split(d.Input, "\n") {
				var kvs []base.InternalKV
				for _, key := range strings.Fields(line) {
					j := strings.Index(key, ":")
					kvs = append(kvs, base.MakeInternalKV(base.ParseInternalKey(key[:j]), []byte(key[j+1:])))
				}
				iterKVs = append(iterKVs, kvs)

				meta := (&tableMetadata{
					FileNum: base.FileNum(len(metas)),
				}).ExtendPointKeyBounds(
					DefaultComparer.Compare,
					kvs[0].K,
					kvs[len(kvs)-1].K,
				)
				meta.InitPhysicalBacking()
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

			iter := newLevelIter(context.Background(), opts, testkeys.Comparer, newIters, files.Iter(), manifest.Level(level), internalIterOpts{})
			defer iter.Close()
			// Fake up the range deletion initialization.
			iter.initRangeDel(rangeDelIterSetterFunc(func(rangeDelIter keyspan.FragmentIterator) {
				if rangeDelIter != nil {
					rangeDelIter.Close()
				}
			}))
			iter.disableInvariants = true
			return itertest.RunInternalIterCmd(t, d, iter, itertest.Verbose)

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
				ctx context.Context, file *manifest.TableMetadata, opts *IterOptions,
				internalOpts internalIterOpts, kinds iterKinds,
			) (iterSet, error) {
				tableOpts = opts
				return newIters(ctx, file, opts, internalOpts, kinds)
			}

			iter := newLevelIter(context.Background(), opts, testkeys.Comparer, newIters2, files.Iter(), manifest.Level(level), internalIterOpts{})
			iter.SeekGE([]byte(key), base.SeekGEFlagsNone)
			lower, upper := tableOpts.GetLowerBound(), tableOpts.GetUpperBound()
			return fmt.Sprintf("[%s,%s]\n", lower, upper)

		default:
			return fmt.Sprintf("unknown command: %s", d.Cmd)
		}
	})
}

type rangeDelIterSetterFunc func(rangeDelIter keyspan.FragmentIterator)

// Assert that rangeDelIterSetterFunc implements the rangeDelIterSetter interface.
var _ rangeDelIterSetter = rangeDelIterSetterFunc(nil)

func (fn rangeDelIterSetterFunc) setRangeDelIter(rangeDelIter keyspan.FragmentIterator) {
	fn(rangeDelIter)
}

type levelIterTest struct {
	cmp          base.Comparer
	mem          vfs.FS
	readers      []*sstable.Reader
	metas        []*tableMetadata
	itersCreated int
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
	ctx context.Context,
	file *manifest.TableMetadata,
	opts *IterOptions,
	iio internalIterOpts,
	kinds iterKinds,
) (iterSet, error) {
	lt.itersCreated++
	transforms := file.IterTransforms()
	var set iterSet
	if kinds.Point() {
		iter, err := lt.readers[file.FileNum].NewPointIter(ctx, sstable.IterOptions{
			Lower:                opts.GetLowerBound(),
			Upper:                opts.GetUpperBound(),
			Transforms:           transforms,
			FilterBlockSizeLimit: sstable.AlwaysUseFilterBlock,
			Env:                  iio.readEnv,
			ReaderProvider:       sstable.MakeTrivialReaderProvider(lt.readers[file.FileNum]),
			BlobContext: sstable.TableBlobContext{
				ValueFetcher: iio.blobValueFetcher,
				References:   file.BlobReferences,
			},
		})
		if err != nil {
			return iterSet{}, errors.CombineErrors(err, set.CloseAll())
		}
		set.point = iter
	}
	if kinds.RangeDeletion() {
		rangeDelIter, err := lt.readers[file.FileNum].NewRawRangeDelIter(ctx, file.FragmentIterTransforms(), sstable.NoReadEnv)
		if err != nil {
			return iterSet{}, errors.CombineErrors(err, set.CloseAll())
		}
		set.rangeDeletion = rangeDelIter
	}
	return set, nil
}

func (lt *levelIterTest) runClear() string {
	lt.mem = vfs.NewMem()
	for _, r := range lt.readers {
		r.Close()
	}
	lt.readers = nil
	lt.metas = nil
	lt.itersCreated = 0
	return ""
}

func (lt *levelIterTest) runBuild(d *datadriven.TestData) string {
	fileNum := base.FileNum(len(lt.readers))
	name := fmt.Sprint(fileNum)
	f0, err := lt.mem.Create(name, vfs.WriteCategoryUnspecified)
	if err != nil {
		return err.Error()
	}

	fp := bloom.FilterPolicy(10)
	writerOpts := sstable.WriterOptions{
		Comparer:     &lt.cmp,
		FilterPolicy: fp,
	}
	if err := sstable.ParseWriterOptions(&writerOpts, d.CmdArgs...); err != nil {
		return err.Error()
	}
	if writerOpts.TableFormat == 0 {
		writerOpts.TableFormat = sstable.TableFormatMinSupported
	}

	w := sstable.NewRawWriter(objstorageprovider.NewFileWritable(f0), writerOpts)
	var tombstones []keyspan.Span
	f := keyspan.Fragmenter{
		Cmp:    lt.cmp.Compare,
		Format: lt.cmp.FormatKey,
		Emit: func(fragmented keyspan.Span) {
			tombstones = append(tombstones, fragmented)
		},
	}
	for _, key := range strings.Split(d.Input, "\n") {
		j := strings.Index(key, ":")
		ikey := base.ParseInternalKey(key[:j])
		value := []byte(key[j+1:])
		switch ikey.Kind() {
		case InternalKeyKindRangeDelete:
			f.Add(rangedel.Decode(ikey, value, nil))
		case InternalKeyKindRangeKeySet, InternalKeyKindRangeKeyUnset, InternalKeyKindRangeKeyDelete:
			span, err := rangekey.Decode(ikey, value, nil)
			if err != nil {
				return err.Error()
			}
			if err := w.EncodeSpan(span); err != nil {
				return err.Error()
			}
		default:
			if err := w.Add(ikey, value, false /* forceObsolete */); err != nil {
				return err.Error()
			}
		}
	}
	f.Finish()
	for _, v := range tombstones {
		if err := w.EncodeSpan(v); err != nil {
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
	readable, err := sstable.NewSimpleReadable(f1)
	if err != nil {
		return err.Error()
	}
	r, err := sstable.NewReader(context.Background(), readable, sstable.ReaderOptions{
		Filters: map[string]FilterPolicy{
			fp.Name(): fp,
		},
	})
	if err != nil {
		return err.Error()
	}
	lt.readers = append(lt.readers, r)
	m := &tableMetadata{FileNum: fileNum}
	if meta.HasPointKeys {
		m.ExtendPointKeyBounds(lt.cmp.Compare, meta.SmallestPoint, meta.LargestPoint)
	}
	if meta.HasRangeDelKeys {
		m.ExtendPointKeyBounds(lt.cmp.Compare, meta.SmallestRangeDel, meta.LargestRangeDel)
	}
	if meta.HasRangeKeys {
		m.ExtendRangeKeyBounds(lt.cmp.Compare, meta.SmallestRangeKey, meta.LargestRangeKey)
	}
	m.InitPhysicalBacking()
	lt.metas = append(lt.metas, m)

	var buf bytes.Buffer
	for _, f := range lt.metas {
		fmt.Fprintf(&buf, "%d: %s-%s\n", f.FileNum, f.Smallest, f.Largest)
	}
	return buf.String()
}

func TestLevelIterBoundaries(t *testing.T) {
	lt := newLevelIterTest()
	defer lt.runClear()

	var iter *levelIter
	datadriven.RunTest(t, "testdata/level_iter_boundaries", func(t *testing.T, d *datadriven.TestData) string {
		switch d.Cmd {
		case "clear":
			return lt.runClear()

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
				return "preceding iter was not closed"
			}
			if cont && iter == nil {
				return "no existing iter"
			}
			if iter == nil {
				slice := manifest.NewLevelSliceKeySorted(lt.cmp.Compare, lt.metas)
				iter = newLevelIter(context.Background(), IterOptions{}, testkeys.Comparer, lt.newIters, slice.Iter(), manifest.Level(level), internalIterOpts{})
				// Fake up the range deletion initialization.
				iter.initRangeDel(rangeDelIterSetterFunc(func(rangeDelIter keyspan.FragmentIterator) {
					if rangeDelIter != nil {
						rangeDelIter.Close()
					}
				}))
			}
			if !save {
				defer func() {
					iter.Close()
					iter = nil
				}()
			}
			return itertest.RunInternalIterCmd(t, d, iter, itertest.Verbose)

		case "file-pos":
			// Returns the FileNum at which the iterator is positioned.
			if iter == nil {
				return "nil levelIter"
			}
			if iter.iterFile == nil {
				return "nil iterFile"
			}
			if iter.iter != nil {
				return fmt.Sprintf("file %s [loaded]", iter.iterFile.FileNum)
			}
			return fmt.Sprintf("file %s [not loaded]", iter.iterFile.FileNum)

		default:
			return fmt.Sprintf("unknown command: %s", d.Cmd)
		}
	})
}

// levelIterTestIter allows a datadriven test to use runInternalIterCmd and
// perform parallel operations on both both a levelIter and rangeDelIter.
type levelIterTestIter struct {
	*levelIter
	rangeDelIter keyspan.FragmentIterator
	// rangeDel is only set on seeks: SeekGE, SeekLT, SeekPrefixGE.
	// TODO(jackson): Clean this up when #2863 is resolved.
	rangeDel *keyspan.Span
}

func must(err error) {
	if err != nil {
		panic(err)
	}
}

func (i *levelIterTestIter) getRangeDel() *keyspan.Span {
	return i.rangeDel
}

func (i *levelIterTestIter) rangeDelSeek(
	key []byte, kv *base.InternalKV, dir int,
) *base.InternalKV {
	i.rangeDel = nil
	if i.rangeDelIter != nil {
		var t *keyspan.Span
		var err error
		if dir < 0 {
			t, err = keyspan.SeekLE(i.levelIter.cmp, i.rangeDelIter, key)
		} else {
			t, err = i.rangeDelIter.SeekGE(key)
		}
		// TODO(jackson): Clean this up when the InternalIterator interface
		// is refactored to return an error return value from all
		// positioning methods.
		must(err)
		if t != nil {
			i.rangeDel = new(keyspan.Span)
			*i.rangeDel = t.Visible(1000)
		}
	}
	return kv
}

func (i *levelIterTestIter) Close() error {
	if i.rangeDelIter != nil {
		i.rangeDelIter.Close()
		i.rangeDelIter = nil
	}
	return i.levelIter.Close()
}

func (i *levelIterTestIter) String() string {
	return "level-iter-test"
}

func (i *levelIterTestIter) SeekGE(key []byte, flags base.SeekGEFlags) *base.InternalKV {
	kv := i.levelIter.SeekGE(key, flags)
	return i.rangeDelSeek(key, kv, 1)
}

func (i *levelIterTestIter) SeekPrefixGE(
	prefix, key []byte, flags base.SeekGEFlags,
) *base.InternalKV {
	kv := i.levelIter.SeekPrefixGE(prefix, key, flags)
	return i.rangeDelSeek(key, kv, 1)
}

func (i *levelIterTestIter) SeekLT(key []byte, flags base.SeekLTFlags) *base.InternalKV {
	kv := i.levelIter.SeekLT(key, flags)
	return i.rangeDelSeek(key, kv, -1)
}

func (i *levelIterTestIter) Next() *base.InternalKV {
	i.rangeDel = nil
	return i.levelIter.Next()
}

func (i *levelIterTestIter) Prev() *base.InternalKV {
	i.rangeDel = nil
	return i.levelIter.Prev()
}

func TestLevelIterSeek(t *testing.T) {
	lt := newLevelIterTest()
	defer lt.runClear()

	datadriven.RunTest(t, "testdata/level_iter_seek", func(t *testing.T, d *datadriven.TestData) string {
		switch d.Cmd {
		case "clear":
			return lt.runClear()

		case "build":
			return lt.runBuild(d)

		case "iter":
			var stats base.InternalIteratorStats
			slice := manifest.NewLevelSliceKeySorted(lt.cmp.Compare, lt.metas)
			iter := &levelIterTestIter{levelIter: &levelIter{}}
			iter.init(context.Background(), IterOptions{}, testkeys.Comparer, lt.newIters, slice.Iter(),
				manifest.Level(level), internalIterOpts{readEnv: sstable.ReadEnv{Block: block.ReadEnv{Stats: &stats}}})
			defer iter.Close()
			iter.initRangeDel(rangeDelIterSetterFunc(func(rangeDelIter keyspan.FragmentIterator) {
				if iter.rangeDelIter != nil {
					iter.rangeDelIter.Close()
				}
				iter.rangeDelIter = rangeDelIter
			}))
			return itertest.RunInternalIterCmd(t, d, iter, itertest.Verbose, itertest.WithSpan(iter.getRangeDel), itertest.WithStats(&stats))

		case "iters-created":
			return fmt.Sprintf("%d", lt.itersCreated)
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
		f, err := mem.Create(fmt.Sprintf("bench%d", i), vfs.WriteCategoryUnspecified)
		if err != nil {
			b.Fatal(err)
		}
		files[i] = f
	}

	writers := make([]sstable.RawWriter, len(files))
	for i := range files {
		writers[i] = sstable.NewRawWriter(objstorageprovider.NewFileWritable(files[i]), sstable.WriterOptions{
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
			require.NoError(b, w.Add(ikey, nil, false /* forceObsolete */))
		}
		if err := w.Close(); err != nil {
			b.Fatal(err)
		}
	}
	c := NewCache(128 << 20 /* 128MB */)
	defer c.Unref()
	ch := c.NewHandle()
	defer ch.Close()
	opts := sstable.ReaderOptions{Comparer: DefaultComparer}
	opts.CacheOpts = sstableinternal.CacheOptions{CacheHandle: ch}
	readers := make([]*sstable.Reader, len(files))
	for i := range files {
		f, err := mem.Open(fmt.Sprintf("bench%d", i))
		if err != nil {
			b.Fatal(err)
		}
		readable, err := sstable.NewSimpleReadable(f)
		if err != nil {
			b.Fatal(err)
		}
		readers[i], err = sstable.NewReader(context.Background(), readable, opts)
		if err != nil {
			b.Fatal(err)
		}
	}

	cleanup := func() {
		for _, r := range readers {
			require.NoError(b, r.Close())
		}
	}

	meta := make([]*tableMetadata, len(readers))
	for i := range readers {
		iter, err := readers[i].NewIter(sstable.NoTransforms, nil /* lower */, nil /* upper */, sstable.AssertNoBlobHandles)
		require.NoError(b, err)
		smallest := iter.First()
		meta[i] = &tableMetadata{}
		meta[i].FileNum = base.FileNum(i)
		largest := iter.Last()
		meta[i].ExtendPointKeyBounds(opts.Comparer.Compare, smallest.K.Clone(), largest.K.Clone())
		meta[i].InitPhysicalBacking()
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
								_ context.Context, file *manifest.TableMetadata, _ *IterOptions, _ internalIterOpts, _ iterKinds,
							) (iterSet, error) {
								iter, err := readers[file.FileNum].NewIter(sstable.NoTransforms, nil /* lower */, nil /* upper */, sstable.AssertNoBlobHandles)
								return iterSet{point: iter}, err
							}
							l := newLevelIter(context.Background(), IterOptions{}, DefaultComparer, newIters, metas.Iter(), manifest.Level(level), internalIterOpts{})
							rng := rand.New(rand.NewPCG(0, uint64(time.Now().UnixNano())))

							b.ResetTimer()
							for i := 0; i < b.N; i++ {
								l.SeekGE(keys[rng.IntN(len(keys))], base.SeekGEFlagsNone)
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
							// fileCacheShard.findNode.
							newIters := func(
								_ context.Context, file *manifest.TableMetadata, opts *IterOptions, _ internalIterOpts, _ iterKinds,
							) (iterSet, error) {
								iter, err := readers[file.FileNum].NewIter(
									sstable.NoTransforms, opts.LowerBound, opts.UpperBound, sstable.AssertNoBlobHandles)
								return iterSet{point: iter}, err
							}
							l := newLevelIter(context.Background(), IterOptions{}, DefaultComparer, newIters, metas.Iter(), manifest.Level(level), internalIterOpts{})
							// Fake up the range deletion initialization, to resemble the usage
							// in a mergingIter.
							l.initRangeDel(rangeDelIterSetterFunc(func(rangeDelIter keyspan.FragmentIterator) {
								if rangeDelIter != nil {
									rangeDelIter.Close()
								}
							}))
							keyCount := len(keys)
							b.ResetTimer()
							for i := 0; i < b.N; i++ {
								pos := i % (keyCount - 1)
								l.SetBounds(keys[pos], keys[pos+1])
								// SeekGE will return keys[pos].
								kv := l.SeekGE(keys[pos], base.SeekGEFlagsNone)
								// Next() will get called once and return nil.
								for kv != nil {
									kv = l.Next()
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
	// fileCacheShard.findNode.
	newIters := func(
		_ context.Context, file *manifest.TableMetadata, opts *IterOptions, _ internalIterOpts, _ iterKinds,
	) (iterSet, error) {
		iter, err := readers[file.FileNum].NewIter(
			sstable.NoTransforms, opts.LowerBound, opts.UpperBound, sstable.AssertNoBlobHandles)
		return iterSet{point: iter}, err
	}

	for _, skip := range []int{1, 2, 4, 8, 16} {
		for _, useNext := range []bool{false, true} {
			b.Run(fmt.Sprintf("skip=%d/use-next=%t", skip, useNext),
				func(b *testing.B) {
					l := newLevelIter(context.Background(), IterOptions{}, testkeys.Comparer, newIters, metas.Iter(), manifest.Level(level), internalIterOpts{})
					// Fake up the range deletion initialization, to resemble the usage
					// in a mergingIter.
					l.initRangeDel(rangeDelIterSetterFunc(func(rangeDelIter keyspan.FragmentIterator) {
						if rangeDelIter != nil {
							rangeDelIter.Close()
						}
					}))
					keyCount := len(keys)
					pos := 0
					l.SeekPrefixGE(keys[pos], keys[pos], base.SeekGEFlagsNone)
					b.ResetTimer()
					for i := 0; i < b.N; i++ {
						pos += skip
						var flags base.SeekGEFlags
						if useNext {
							flags = flags.EnableTrySeekUsingNext()
						}
						if pos >= keyCount {
							pos = 0
							flags = flags.DisableTrySeekUsingNext()
						}
						// SeekPrefixGE will return keys[pos].
						l.SeekPrefixGE(keys[pos], keys[pos], flags)
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
								_ context.Context, file *manifest.TableMetadata, _ *IterOptions, _ internalIterOpts, _ iterKinds,
							) (iterSet, error) {
								iter, err := readers[file.FileNum].NewIter(sstable.NoTransforms,
									nil /* lower */, nil /* upper */, sstable.AssertNoBlobHandles)
								return iterSet{point: iter}, err
							}
							l := newLevelIter(context.Background(), IterOptions{}, testkeys.Comparer, newIters, metas.Iter(), manifest.Level(level), internalIterOpts{})

							b.ResetTimer()
							for i := 0; i < b.N; i++ {
								kv := l.Next()
								if kv == nil {
									kv = l.First()
								}
								_ = kv
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
								_ context.Context, file *manifest.TableMetadata, _ *IterOptions, _ internalIterOpts, _ iterKinds,
							) (iterSet, error) {
								iter, err := readers[file.FileNum].NewIter(
									sstable.NoTransforms, nil /* lower */, nil /* upper */, sstable.AssertNoBlobHandles)
								return iterSet{point: iter}, err
							}
							l := newLevelIter(context.Background(), IterOptions{}, DefaultComparer, newIters, metas.Iter(), manifest.Level(level), internalIterOpts{})

							b.ResetTimer()
							for i := 0; i < b.N; i++ {
								kv := l.Prev()
								if kv == nil {
									kv = l.Last()
								}
								_ = kv
							}
							l.Close()
						})
				}
			})
	}
}
