// Copyright 2018 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package pebble

import (
	"bytes"
	"context"
	"fmt"
	"math/rand/v2"
	"slices"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/cockroachdb/datadriven"
	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/pebble/bloom"
	"github.com/cockroachdb/pebble/internal/base"
	"github.com/cockroachdb/pebble/internal/cache"
	"github.com/cockroachdb/pebble/internal/itertest"
	"github.com/cockroachdb/pebble/internal/keyspan"
	"github.com/cockroachdb/pebble/internal/manifest"
	"github.com/cockroachdb/pebble/internal/sstableinternal"
	"github.com/cockroachdb/pebble/internal/testkeys"
	"github.com/cockroachdb/pebble/internal/testutils/indenttree"
	"github.com/cockroachdb/pebble/objstorage/objstorageprovider"
	"github.com/cockroachdb/pebble/sstable"
	"github.com/cockroachdb/pebble/sstable/block"
	"github.com/cockroachdb/pebble/vfs"
	"github.com/stretchr/testify/require"
)

func TestMergingIter(t *testing.T) {
	var stats base.InternalIteratorStats
	newFunc := func(iters ...internalIterator) internalIterator {
		return newMergingIter(nil /* logger */, &stats, DefaultComparer.Compare,
			func(a []byte) int { return len(a) }, iters...)
	}
	testIterator(t, newFunc, func(r *rand.Rand) [][]string {
		// Shuffle testKeyValuePairs into one or more splits. Each individual
		// split is in increasing order, but different splits may overlap in
		// range. Some of the splits may be empty.
		splits := make([][]string, 1+r.IntN(2+len(testKeyValuePairs)))
		for _, kv := range testKeyValuePairs {
			j := r.IntN(len(splits))
			splits[j] = append(splits[j], kv)
		}
		return splits
	})
}

func TestMergingIterSeek(t *testing.T) {
	var def string
	datadriven.RunTest(t, "testdata/merging_iter_seek", func(t *testing.T, d *datadriven.TestData) string {
		switch d.Cmd {
		case "define":
			def = d.Input
			return ""

		case "iter":
			var iters []internalIterator
			for _, line := range strings.Split(def, "\n") {
				var kvs []base.InternalKV
				for _, key := range strings.Fields(line) {
					j := strings.Index(key, ":")
					kvs = append(kvs, base.MakeInternalKV(base.ParseInternalKey(key[:j]), []byte(key[j+1:])))
				}
				iters = append(iters, base.NewFakeIter(kvs))
			}

			var stats base.InternalIteratorStats
			iter := newMergingIter(nil /* logger */, &stats, DefaultComparer.Compare,
				func(a []byte) int { return len(a) }, iters...)
			defer iter.Close()
			return itertest.RunInternalIterCmd(t, d, iter)

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
		{
			"a.SET.2:2 a.SET.1:1 b.SET.2:2 b.SET.1:1 c.SET.2:2 c.SET.1:1",
		},
		{
			"a.SET.2:2 b.SET.2:2 c.SET.2:2",
			"a.SET.1:1 b.SET.1:1 c.SET.1:1",
		},
		{
			"a.SET.2:2 b.SET.2:2",
			"a.SET.1:1 b.SET.1:1",
			"c.SET.2:2 c.SET.1:1",
		},
		{
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
			datadriven.RunTest(t, "testdata/internal_iter_next", func(t *testing.T, d *datadriven.TestData) string {
				switch d.Cmd {
				case "define":
					// Ignore. We've defined the iterator data above.
					return ""

				case "iter":
					iters := make([]internalIterator, len(c))
					for i := range c {
						var kvs []base.InternalKV
						for _, key := range strings.Fields(c[i]) {
							j := strings.Index(key, ":")
							kvs = append(kvs, base.MakeInternalKV(base.ParseInternalKey(key[:j]), []byte(key[j+1:])))
						}
						iters[i] = base.NewFakeIter(kvs)
					}

					var stats base.InternalIteratorStats
					iter := newMergingIter(nil /* logger */, &stats, DefaultComparer.Compare,
						func(a []byte) int { return len(a) }, iters...)
					defer iter.Close()
					return itertest.RunInternalIterCmd(t, d, iter)

				default:
					return fmt.Sprintf("unknown command: %s", d.Cmd)
				}
			})
		})
	}
}

func TestMergingIterDataDriven(t *testing.T) {
	memFS := vfs.NewMem()
	cmp := DefaultComparer.Compare
	fmtKey := DefaultComparer.FormatKey
	opts := DefaultOptions()
	var v *version
	var buf bytes.Buffer

	// Indexed by FileNum.
	readers := make(map[base.FileNum]*sstable.Reader)
	defer func() {
		for _, r := range readers {
			r.Close()
		}
	}()
	parser := itertest.NewParser()

	var (
		pointProbes    map[base.FileNum][]itertest.Probe
		rangeDelProbes map[base.FileNum][]keyspanProbe
		fileNum        base.FileNum
	)
	newIters :=
		func(ctx context.Context, file *manifest.TableMetadata, opts *IterOptions, iio internalIterOpts, kinds iterKinds,
		) (iterSet, error) {
			var set iterSet
			var err error
			r := readers[file.FileNum]
			if kinds.RangeDeletion() {
				set.rangeDeletion, err = r.NewRawRangeDelIter(ctx, sstable.NoFragmentTransforms, iio.readEnv)
				if err != nil {
					return iterSet{}, errors.CombineErrors(err, set.CloseAll())
				}
			}
			if kinds.Point() {
				set.point, err = r.NewPointIter(ctx, sstable.IterOptions{
					Lower:                opts.GetLowerBound(),
					Upper:                opts.GetUpperBound(),
					Transforms:           sstable.NoTransforms,
					FilterBlockSizeLimit: sstable.AlwaysUseFilterBlock,
					Env:                  iio.readEnv,
					ReaderProvider:       sstable.MakeTrivialReaderProvider(r),
					BlobContext: sstable.TableBlobContext{
						ValueFetcher: iio.blobValueFetcher,
						References:   file.BlobReferences,
					},
				})
				if err != nil {
					return iterSet{}, errors.CombineErrors(err, set.CloseAll())
				}
			}
			set.point = itertest.Attach(set.point, itertest.ProbeState{Log: &buf}, pointProbes[file.FileNum]...)
			set.rangeDeletion = attachKeyspanProbes(set.rangeDeletion, keyspanProbeContext{log: &buf}, rangeDelProbes[file.FileNum]...)
			return set, nil
		}

	datadriven.RunTest(t, "testdata/merging_iter", func(t *testing.T, d *datadriven.TestData) string {
		switch d.Cmd {
		case "define":
			levels, err := indenttree.Parse(d.Input)
			if err != nil {
				d.Fatalf(t, "%v", err)
			}
			var files [numLevels][]*tableMetadata
			for l := range levels {
				if levels[l].Value() != "L" {
					d.Fatalf(t, "top-level strings should be L")
				}
				for _, file := range levels[l].Children() {
					m, err := manifest.ParseTableMetadataDebug(file.Value())
					if err != nil {
						d.Fatalf(t, "table metadata: %s", err)
					}
					files[l+1] = append(files[l+1], m)

					name := fmt.Sprint(fileNum)
					fileNum++
					f, err := memFS.Create(name, vfs.WriteCategoryUnspecified)
					if err != nil {
						return err.Error()
					}
					w := sstable.NewRawWriter(objstorageprovider.NewFileWritable(f), sstable.WriterOptions{})
					var tombstones []keyspan.Span
					frag := keyspan.Fragmenter{
						Cmp:    cmp,
						Format: fmtKey,
						Emit: func(fragmented keyspan.Span) {
							tombstones = append(tombstones, fragmented)
						},
					}

					for _, kvRow := range file.Children() {
						for _, kv := range strings.Fields(kvRow.Value()) {
							j := strings.Index(kv, ":")
							ikey := base.ParseInternalKey(kv[:j])
							value := []byte(kv[j+1:])
							switch ikey.Kind() {
							case InternalKeyKindRangeDelete:
								frag.Add(keyspan.Span{Start: ikey.UserKey, End: value, Keys: []keyspan.Key{{Trailer: ikey.Trailer}}})
							default:
								if err := w.Add(ikey, value, false /* forceObsolete */); err != nil {
									return err.Error()
								}
							}
						}
					}
					frag.Finish()
					for _, v := range tombstones {
						if err := w.EncodeSpan(v); err != nil {
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
					readable, err := sstable.NewSimpleReadable(f)
					if err != nil {
						return err.Error()
					}
					r, err := sstable.NewReader(context.Background(), readable, opts.MakeReaderOptions())
					if err != nil {
						return err.Error()
					}
					readers[m.FileNum] = r
				}
			}
			v = newVersion(opts, files)
			return v.String()
		case "iter":
			buf.Reset()
			pointProbes = make(map[base.FileNum][]itertest.Probe, len(v.Levels))
			rangeDelProbes = make(map[base.FileNum][]keyspanProbe, len(v.Levels))
			for _, cmdArg := range d.CmdArgs {
				switch key := cmdArg.Key; key {
				case "probe-points":
					i, err := strconv.Atoi(cmdArg.Vals[0][1:])
					if err != nil {
						require.NoError(t, err)
					}
					pointProbes[base.FileNum(i)] = itertest.MustParseProbes(parser, cmdArg.Vals[1:]...)
				case "probe-rangedels":
					i, err := strconv.Atoi(cmdArg.Vals[0][1:])
					if err != nil {
						require.NoError(t, err)
					}
					rangeDelProbes[base.FileNum(i)] = parseKeyspanProbes(cmdArg.Vals[1:]...)
				default:
					// Might be a command understood by the RunInternalIterCmd
					// command, so don't error.
				}
			}

			levelIters := make([]mergingIterLevel, 0, len(v.Levels))
			var stats base.InternalIteratorStats
			iio := internalIterOpts{readEnv: sstable.ReadEnv{Block: block.ReadEnv{Stats: &stats}}}
			for i, l := range v.Levels {
				slice := l.Slice()
				if slice.Empty() {
					continue
				}
				li := &levelIter{}
				li.init(context.Background(), IterOptions{}, testkeys.Comparer,
					newIters, slice.Iter(), manifest.Level(i), iio)

				i := len(levelIters)
				levelIters = append(levelIters, mergingIterLevel{iter: li})
				li.initRangeDel(&levelIters[i])
			}
			miter := &mergingIter{}
			miter.init(nil /* opts */, &stats, cmp, func(a []byte) int { return len(a) }, levelIters...)
			defer miter.Close()
			miter.forceEnableSeekOpt = true
			// Exercise SetContext for fun
			// (https://github.com/cockroachdb/pebble/pull/3037 caused a SIGSEGV due
			// to a nil pointer dereference).
			miter.SetContext(context.Background())
			itertest.RunInternalIterCmdWriter(t, &buf, d, miter,
				itertest.Verbose, itertest.WithStats(&stats))
			return buf.String()
		default:
			return fmt.Sprintf("unknown command: %s", d.Cmd)
		}
	})
}

func buildMergingIterTables(
	b *testing.B, ch *cache.Handle, blockSize, restartInterval, count int,
) ([]*sstable.Reader, [][]byte, func()) {
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
		j := rand.IntN(len(writers))
		w := writers[j]
		w.Add(ikey, nil, false /* forceObsolete */)
	}

	for _, w := range writers {
		if err := w.Close(); err != nil {
			b.Fatal(err)
		}
	}

	var opts sstable.ReaderOptions
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
		opts.CacheOpts.FileNum = base.DiskFileNum(i)
		readers[i], err = sstable.NewReader(context.Background(), readable, opts)
		if err != nil {
			b.Fatal(err)
		}
	}
	return readers, keys, func() {
		for _, r := range readers {
			require.NoError(b, r.Close())
		}
	}
}

func BenchmarkMergingIterSeekGE(b *testing.B) {
	const blockSize = 32 << 10

	for _, restartInterval := range []int{16} {
		b.Run(fmt.Sprintf("restart=%d", restartInterval),
			func(b *testing.B) {
				for _, count := range []int{1, 2, 3, 4, 5} {
					b.Run(fmt.Sprintf("count=%d", count),
						func(b *testing.B) {
							c := NewCache(128 << 20 /* 128MB */)
							defer c.Unref()
							ch := c.NewHandle()
							defer ch.Close()
							readers, keys, cleanup := buildMergingIterTables(b, ch, blockSize, restartInterval, count)
							defer cleanup()
							iters := make([]internalIterator, len(readers))
							for i := range readers {
								var err error
								iters[i], err = readers[i].NewIter(sstable.NoTransforms, nil /* lower */, nil /* upper */, sstable.AssertNoBlobHandles)
								require.NoError(b, err)
							}
							var stats base.InternalIteratorStats
							m := newMergingIter(nil /* logger */, &stats, DefaultComparer.Compare,
								func(a []byte) int { return len(a) }, iters...)
							rng := rand.New(rand.NewPCG(0, uint64(time.Now().UnixNano())))

							b.ResetTimer()
							for i := 0; i < b.N; i++ {
								m.SeekGE(keys[rng.IntN(len(keys))], base.SeekGEFlagsNone)
							}
							m.Close()
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
							c := NewCache(128 << 20 /* 128MB */)
							defer c.Unref()
							ch := c.NewHandle()
							defer ch.Close()
							readers, _, cleanup := buildMergingIterTables(b, ch, blockSize, restartInterval, count)
							defer cleanup()
							iters := make([]internalIterator, len(readers))
							for i := range readers {
								var err error
								iters[i], err = readers[i].NewIter(sstable.NoTransforms, nil /* lower */, nil /* upper */, sstable.AssertNoBlobHandles)
								require.NoError(b, err)
							}
							var stats base.InternalIteratorStats
							m := newMergingIter(nil /* logger */, &stats, DefaultComparer.Compare,
								func(a []byte) int { return len(a) }, iters...)

							b.ResetTimer()
							for i := 0; i < b.N; i++ {
								kv := m.Next()
								if kv == nil {
									kv = m.First()
								}
								_ = kv
							}
							m.Close()
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
							c := NewCache(128 << 20 /* 128MB */)
							defer c.Unref()
							ch := c.NewHandle()
							defer ch.Close()
							readers, _, cleanup := buildMergingIterTables(b, ch, blockSize, restartInterval, count)
							defer cleanup()
							iters := make([]internalIterator, len(readers))
							for i := range readers {
								var err error
								iters[i], err = readers[i].NewIter(sstable.NoTransforms, nil /* lower */, nil /* upper */, sstable.AssertNoBlobHandles)
								require.NoError(b, err)
							}
							var stats base.InternalIteratorStats
							m := newMergingIter(nil /* logger */, &stats, DefaultComparer.Compare,
								func(a []byte) int { return len(a) }, iters...)

							b.ResetTimer()
							for i := 0; i < b.N; i++ {
								kv := m.Prev()
								if kv == nil {
									kv = m.Last()
								}
								_ = kv
							}
							m.Close()
						})
				}
			})
	}
}

// Builds levels for BenchmarkMergingIterSeqSeekGEWithBounds. The lowest level,
// index 0 here, contains most of the data. Each level has 2 files, to allow for
// stepping into the second file if needed. The lowest level has all the keys in
// the file 0, and a single "lastIKey" in file 1. File 0 in all other levels have
// only the first and last key of file 0 of the aforementioned level -- this
// simulates sparseness of data, but not necessarily of file width, in higher
// levels. File 1 in other levels is similar to File 1 in the aforementioned level
// since it is only for stepping into. If writeRangeTombstoneToLowestLevel is
// true, a range tombstone is written to the first lowest level file that
// deletes all the keys in it, and no other levels should be written.
func buildLevelsForMergingIterSeqSeek(
	b *testing.B,
	ch *cache.Handle,
	blockSize, restartInterval, levelCount int,
	keyPrefix []byte,
	keyOffset int,
	writeRangeTombstoneToLowestLevel bool,
	writeBloomFilters bool,
	forceTwoLevelIndex bool,
) (readers [][]*sstable.Reader, levelSlices []manifest.LevelSlice, keys [][]byte) {
	mem := vfs.NewMem()
	if writeRangeTombstoneToLowestLevel && levelCount != 1 {
		panic("expect to write only 1 level")
	}
	files := make([][]vfs.File, levelCount)
	for i := range files {
		for j := 0; j < 2; j++ {
			f, err := mem.Create(fmt.Sprintf("bench%d_%d", i, j), vfs.WriteCategoryUnspecified)
			if err != nil {
				b.Fatal(err)
			}
			files[i] = append(files[i], f)
		}
	}

	const targetL6FirstFileSize = 2 << 20
	writers := make([][]sstable.RawWriter, levelCount)
	// A policy unlikely to have false positives.
	filterPolicy := bloom.FilterPolicy(100)
	for i := range files {
		for j := range files[i] {
			writerOptions := sstable.WriterOptions{
				BlockRestartInterval: restartInterval,
				BlockSize:            blockSize,
				Compression:          NoCompression,
			}
			if writeBloomFilters {
				writerOptions.FilterPolicy = filterPolicy
				writerOptions.FilterType = base.TableFilter
			}
			if forceTwoLevelIndex {
				if i == 0 && j == 0 {
					// Ignoring compression, approximate number of blocks
					numDataBlocks := targetL6FirstFileSize / blockSize
					if numDataBlocks < 4 {
						b.Fatalf("cannot produce two level index")
					}
					// Produce ~2 lower-level index blocks.
					writerOptions.IndexBlockSize = (numDataBlocks / 2) * 8
				} else if j == 0 {
					// Only 2 keys in these files, so to produce two level indexes we
					// set the block sizes to 1.
					writerOptions.BlockSize = 1
					writerOptions.IndexBlockSize = 1
				}
			}
			writers[i] = append(writers[i], sstable.NewRawWriter(objstorageprovider.NewFileWritable(files[i][j]), writerOptions))
		}
	}

	i := keyOffset
	w := writers[0][0]
	makeKey := func(i int) []byte {
		return append(slices.Clone(keyPrefix), []byte(fmt.Sprintf("%08d", i))...)
	}
	for ; w.EstimatedSize() < targetL6FirstFileSize; i++ {
		key := makeKey(i)
		keys = append(keys, key)
		ikey := base.MakeInternalKey(key, 0, InternalKeyKindSet)
		require.NoError(b, w.Add(ikey, nil, false /* forceObsolete */))
	}
	if writeRangeTombstoneToLowestLevel {
		require.NoError(b, w.EncodeSpan(keyspan.Span{
			Start: keys[0],
			End:   makeKey(i),
			Keys: []keyspan.Key{{
				Trailer: base.MakeTrailer(1, InternalKeyKindRangeDelete),
			}},
		}))
	}
	for j := 1; j < len(files); j++ {
		for _, k := range []int{0, len(keys) - 1} {
			ikey := base.MakeInternalKey(keys[k], base.SeqNum(j), InternalKeyKindSet)
			require.NoError(b, writers[j][0].Add(ikey, nil, false /* forceObsolete */))
		}
	}
	lastKey := makeKey(i)
	keys = append(keys, lastKey)
	for j := 0; j < len(files); j++ {
		lastIKey := base.MakeInternalKey(lastKey, base.SeqNum(j), InternalKeyKindSet)
		require.NoError(b, writers[j][1].Add(lastIKey, nil, false /* forceObsolete */))
	}
	for _, levelWriters := range writers {
		for j, w := range levelWriters {
			if err := w.Close(); err != nil {
				b.Fatal(err)
			}
			meta, err := w.Metadata()
			require.NoError(b, err)
			if forceTwoLevelIndex && j == 0 && meta.Properties.IndexType != 2 {
				b.Fatalf("did not produce two level index")
			}
		}
	}

	opts := sstable.ReaderOptions{Comparer: DefaultComparer}
	opts.CacheOpts = sstableinternal.CacheOptions{CacheHandle: ch}

	if writeBloomFilters {
		opts.Filters = make(map[string]FilterPolicy)
		opts.Filters[filterPolicy.Name()] = filterPolicy
	}

	readers = make([][]*sstable.Reader, levelCount)
	fileCount := 0
	for i := range files {
		for j := range files[i] {
			f, err := mem.Open(fmt.Sprintf("bench%d_%d", i, j))
			if err != nil {
				b.Fatal(err)
			}
			readable, err := sstable.NewSimpleReadable(f)
			if err != nil {
				b.Fatal(err)
			}
			opts.CacheOpts.FileNum = base.DiskFileNum(fileCount)
			fileCount++
			r, err := sstable.NewReader(context.Background(), readable, opts)
			if err != nil {
				b.Fatal(err)
			}
			readers[i] = append(readers[i], r)
		}
	}
	levelSlices = make([]manifest.LevelSlice, levelCount)
	for i := range readers {
		meta := make([]*tableMetadata, len(readers[i]))
		for j := range readers[i] {
			iter, err := readers[i][j].NewIter(sstable.NoTransforms, nil /* lower */, nil /* upper */, sstable.AssertNoBlobHandles)
			require.NoError(b, err)
			smallest := iter.First()
			meta[j] = &tableMetadata{}
			// The same FileNum is being reused across different levels, which
			// is harmless for the benchmark since each level has its own iterator
			// creation func.
			meta[j].FileNum = base.FileNum(j)
			largest := iter.Last()
			meta[j].ExtendPointKeyBounds(opts.Comparer.Compare, smallest.K.Clone(), largest.K.Clone())
			meta[j].InitPhysicalBacking()
		}
		levelSlices[i] = manifest.NewLevelSliceSpecificOrder(meta)
	}
	return readers, levelSlices, keys
}

func buildMergingIter(readers [][]*sstable.Reader, levelSlices []manifest.LevelSlice) *mergingIter {
	mils := make([]mergingIterLevel, len(levelSlices))
	for i := len(readers) - 1; i >= 0; i-- {
		levelIndex := i
		level := len(readers) - 1 - i
		newIters := func(
			_ context.Context, file *manifest.TableMetadata, opts *IterOptions, iio internalIterOpts, _ iterKinds,
		) (iterSet, error) {
			iter, err := readers[levelIndex][file.FileNum].NewIter(
				sstable.NoTransforms, opts.LowerBound, opts.UpperBound, sstable.AssertNoBlobHandles)
			if err != nil {
				return iterSet{}, err
			}
			rdIter, err := readers[levelIndex][file.FileNum].NewRawRangeDelIter(context.Background(), sstable.NoFragmentTransforms, iio.readEnv)
			if err != nil {
				iter.Close()
				return iterSet{}, err
			}
			return iterSet{point: iter, rangeDeletion: rdIter}, err
		}
		l := newLevelIter(
			context.Background(), IterOptions{}, testkeys.Comparer, newIters, levelSlices[i].Iter(),
			manifest.Level(level), internalIterOpts{})
		l.initRangeDel(&mils[level])
		mils[level].iter = l
	}
	var stats base.InternalIteratorStats
	m := &mergingIter{}
	m.init(nil /* logger */, &stats, testkeys.Comparer.Compare,
		func(a []byte) int { return len(a) }, mils...)
	return m
}

// A benchmark that simulates the behavior of a mergingIter where
// monotonically increasing narrow bounds are repeatedly set and used to Seek
// and then iterate over the keys within the bounds. This resembles MVCC
// scanning by CockroachDB when doing a lookup/index join with a large number
// of left rows, that are batched and reuse the same iterator, and which can
// have good locality of access. This results in the successive bounds being
// in the same file.
func BenchmarkMergingIterSeqSeekGEWithBounds(b *testing.B) {
	const blockSize = 32 << 10

	c := NewCache(128 << 20 /* 128MB */)
	defer c.Unref()
	restartInterval := 16
	for _, levelCount := range []int{5} {
		b.Run(fmt.Sprintf("levelCount=%d", levelCount),
			func(b *testing.B) {
				ch := c.NewHandle()
				defer ch.Close()
				readers, levelSlices, keys := buildLevelsForMergingIterSeqSeek(
					b, ch, blockSize, restartInterval, levelCount, nil, 0 /* keyOffset */, false, false, false)
				m := buildMergingIter(readers, levelSlices)
				keyCount := len(keys)
				b.ResetTimer()
				for i := 0; i < b.N; i++ {
					pos := i % (keyCount - 1)
					m.SetBounds(keys[pos], keys[pos+1])
					// SeekGE will return keys[pos].
					k := m.SeekGE(keys[pos], base.SeekGEFlagsNone)
					for k != nil {
						k = m.Next()
					}
				}
				m.Close()
				for i := range readers {
					for j := range readers[i] {
						readers[i][j].Close()
					}
				}
			})
	}
}

func BenchmarkMergingIterSeqSeekPrefixGE(b *testing.B) {
	const blockSize = 32 << 10
	const restartInterval = 16
	const levelCount = 5

	c := NewCache(128 << 20 /* 128MB */)
	defer c.Unref()
	ch := c.NewHandle()
	defer ch.Close()
	readers, levelSlices, keys := buildLevelsForMergingIterSeqSeek(
		b, ch, blockSize, restartInterval, levelCount, nil, 0 /* keyOffset */, false, false, false)

	for _, skip := range []int{1, 2, 4, 8, 16} {
		for _, useNext := range []bool{false, true} {
			b.Run(fmt.Sprintf("skip=%d/use-next=%t", skip, useNext),
				func(b *testing.B) {
					m := buildMergingIter(readers, levelSlices)
					keyCount := len(keys)
					pos := 0

					m.SeekPrefixGE(keys[pos], keys[pos], base.SeekGEFlagsNone)
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
						m.SeekPrefixGE(keys[pos], keys[pos], flags)
					}
					b.StopTimer()
					m.Close()
				})
		}
	}
	for i := range readers {
		for j := range readers[i] {
			readers[i][j].Close()
		}
	}
}

// Benchmarks seeking and nexting when almost all the data is in L6.
func BenchmarkMergingIterSeekAndNextWithDominantL6AndLongKey(b *testing.B) {
	const blockSize = 32 << 10

	c := NewCache(128 << 20 /* 128MB */)
	defer c.Unref()
	restartInterval := 16
	levelCount := 5
	for _, keyPrefixLength := range []int{0, 20, 50, 100} {
		b.Run(fmt.Sprintf("key-prefix=%d", keyPrefixLength),
			func(b *testing.B) {
				ch := c.NewHandle()
				defer ch.Close()
				keyPrefix := bytes.Repeat([]byte{'a'}, keyPrefixLength)
				readers, levelSlices, keys := buildLevelsForMergingIterSeqSeek(
					b, ch, blockSize, restartInterval, levelCount, keyPrefix, 0 /* keyOffset */, false, false, false)
				m := buildMergingIter(readers, levelSlices)
				b.ResetTimer()
				for i := 0; i < b.N; i++ {
					k := m.SeekGE(keys[0], base.SeekGEFlagsNone)
					for k != nil {
						k = m.Next()
					}
				}
				m.Close()
				for i := range readers {
					for j := range readers[i] {
						readers[i][j].Close()
					}
				}
			})
	}
}
