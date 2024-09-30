// Copyright 2018 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package sstable

import (
	"bytes"
	"context"
	"encoding/binary"
	"fmt"
	"math/rand"
	"strconv"
	"strings"
	"sync"
	"testing"
	"unsafe"

	"github.com/cockroachdb/crlib/testutils/leaktest"
	"github.com/cockroachdb/datadriven"
	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/pebble/bloom"
	"github.com/cockroachdb/pebble/internal/base"
	"github.com/cockroachdb/pebble/internal/cache"
	"github.com/cockroachdb/pebble/internal/humanize"
	"github.com/cockroachdb/pebble/internal/sstableinternal"
	"github.com/cockroachdb/pebble/internal/testkeys"
	"github.com/cockroachdb/pebble/objstorage"
	"github.com/cockroachdb/pebble/objstorage/objstorageprovider"
	"github.com/cockroachdb/pebble/sstable/block"
	"github.com/cockroachdb/pebble/sstable/rowblk"
	"github.com/cockroachdb/pebble/vfs"
	"github.com/stretchr/testify/require"
)

func testWriterParallelism(t *testing.T, parallelism bool) {
	for _, format := range []TableFormat{TableFormatPebblev2, TableFormatPebblev3} {
		tdFile := "testdata/writer"
		if format == TableFormatPebblev3 {
			tdFile = "testdata/writer_v3"
		}
		t.Run(format.String(), func(t *testing.T) { runDataDriven(t, tdFile, format, parallelism) })
	}
}
func TestWriter(t *testing.T) {
	defer leaktest.AfterTest(t)()
	testWriterParallelism(t, false)
}

func testRewriterParallelism(t *testing.T, parallelism bool) {
	for _, format := range []TableFormat{TableFormatPebblev2, TableFormatPebblev3} {
		tdFile := "testdata/rewriter"
		if format == TableFormatPebblev3 {
			tdFile = "testdata/rewriter_v3"
		}
		t.Run(format.String(), func(t *testing.T) { runDataDriven(t, tdFile, format, parallelism) })
	}
}

func TestRewriter(t *testing.T) {
	defer leaktest.AfterTest(t)()
	testRewriterParallelism(t, false)
}

func TestWriterParallel(t *testing.T) {
	defer leaktest.AfterTest(t)()
	testWriterParallelism(t, true)
}

func TestRewriterParallel(t *testing.T) {
	defer leaktest.AfterTest(t)()
	testRewriterParallelism(t, true)
}

func formatWriterMetadata(td *datadriven.TestData, m *WriterMetadata) string {
	var requestedProps []string
	for _, cmdArg := range td.CmdArgs {
		switch cmdArg.Key {
		case "props":
			requestedProps = cmdArg.Vals
		}
	}

	var b bytes.Buffer
	if m.HasPointKeys {
		fmt.Fprintf(&b, "point:    [%s-%s]\n", m.SmallestPoint, m.LargestPoint)
	}
	if m.HasRangeDelKeys {
		fmt.Fprintf(&b, "rangedel: [%s-%s]\n", m.SmallestRangeDel, m.LargestRangeDel)
	}
	if m.HasRangeKeys {
		fmt.Fprintf(&b, "rangekey: [%s-%s]\n", m.SmallestRangeKey, m.LargestRangeKey)
	}
	fmt.Fprintf(&b, "seqnums:  [%d-%d]\n", m.SmallestSeqNum, m.LargestSeqNum)

	if len(requestedProps) > 0 {
		props := strings.Split(m.Properties.String(), "\n")
		for _, requestedProp := range requestedProps {
			fmt.Fprintf(&b, "props %q:\n", requestedProp)
			for _, prop := range props {
				if strings.Contains(prop, requestedProp) {
					fmt.Fprintf(&b, "  %s\n", prop)
				}
			}
		}
	}
	return b.String()
}

func runDataDriven(t *testing.T, file string, tableFormat TableFormat, parallelism bool) {
	var r *Reader
	var obj *objstorage.MemObj
	defer func() {
		if r != nil {
			require.NoError(t, r.Close())
		}
	}()

	datadriven.RunTest(t, file, func(t *testing.T, td *datadriven.TestData) string {
		switch td.Cmd {
		case "build":
			if r != nil {
				_ = r.Close()
				r = nil
			}
			var meta *WriterMetadata
			var err error
			wopts := &WriterOptions{
				Comparer:    testkeys.Comparer,
				TableFormat: tableFormat,
				Parallelism: parallelism,
			}
			meta, obj, err = runBuildMemObjCmd(td, wopts)
			if err != nil {
				return err.Error()
			}
			r, err = openReader(obj, wopts, 0)
			if err != nil {
				return err.Error()
			}
			return formatWriterMetadata(td, meta)

		case "build-raw":
			if r != nil {
				_ = r.Close()
				r = nil
			}
			var meta *WriterMetadata
			var err error
			meta, r, err = runBuildRawCmd(td, &WriterOptions{
				Comparer:    testkeys.Comparer,
				TableFormat: tableFormat,
			})
			if err != nil {
				return err.Error()
			}
			return formatWriterMetadata(td, meta)

		case "scan":
			iter, err := r.NewIter(NoTransforms, nil /* lower */, nil /* upper */)
			if err != nil {
				return err.Error()
			}
			defer iter.Close()

			var buf bytes.Buffer
			for kv := iter.First(); kv != nil; kv = iter.Next() {
				v, _, err := kv.Value(nil)
				if err != nil {
					return err.Error()
				}
				fmt.Fprintf(&buf, "%s:%s\n", kv.K, v)
			}
			return buf.String()

		case "get":
			var buf bytes.Buffer
			for _, k := range strings.Split(td.Input, "\n") {
				value, err := r.get([]byte(k))
				if err != nil {
					fmt.Fprintf(&buf, "get %s: %s\n", k, err.Error())
				} else {
					fmt.Fprintf(&buf, "%s\n", value)
				}
			}
			return buf.String()

		case "scan-range-del":
			iter, err := r.NewRawRangeDelIter(context.Background(), NoFragmentTransforms)
			if err != nil {
				return err.Error()
			}
			if iter == nil {
				return ""
			}
			defer iter.Close()

			var buf bytes.Buffer
			s, err := iter.First()
			for ; s != nil; s, err = iter.Next() {
				fmt.Fprintf(&buf, "%s\n", s)
			}
			if err != nil {
				return err.Error()
			}
			return buf.String()

		case "scan-range-key":
			iter, err := r.NewRawRangeKeyIter(context.Background(), NoFragmentTransforms)
			if err != nil {
				return err.Error()
			}
			if iter == nil {
				return ""
			}
			defer iter.Close()

			var buf bytes.Buffer
			s, err := iter.First()
			for ; s != nil; s, err = iter.Next() {
				fmt.Fprintf(&buf, "%s\n", s)
			}
			if err != nil {
				return err.Error()
			}
			return buf.String()

		case "layout":
			l, err := r.Layout()
			if err != nil {
				return err.Error()
			}
			verbose := false
			if len(td.CmdArgs) > 0 {
				if td.CmdArgs[0].Key == "verbose" {
					verbose = true
				} else {
					return "unknown arg"
				}
			}
			var buf bytes.Buffer
			l.Describe(&buf, verbose, r, nil)
			return buf.String()

		case "decode-layout":
			l, err := decodeLayout(testkeys.Comparer, obj.Data())
			if err != nil {
				return err.Error()
			}
			verbose := false
			if len(td.CmdArgs) > 0 {
				if td.CmdArgs[0].Key == "verbose" {
					verbose = true
				} else {
					return "unknown arg"
				}
			}
			var buf bytes.Buffer
			l.Describe(&buf, verbose, r, nil)
			return buf.String()

		case "rewrite":
			var meta *WriterMetadata
			var err error
			meta, r, err = runRewriteCmd(td, r, WriterOptions{
				TableFormat: tableFormat,
			})
			if err != nil {
				return err.Error()
			}
			return formatWriterMetadata(td, meta)

		default:
			return fmt.Sprintf("unknown command: %s", td.Cmd)
		}
	})
}

func TestWriterWithValueBlocks(t *testing.T) {
	defer leaktest.AfterTest(t)()
	var r *Reader
	defer func() {
		if r != nil {
			require.NoError(t, r.Close())
		}
	}()
	formatVersion := TableFormatMax
	formatMeta := func(m *WriterMetadata) string {
		return fmt.Sprintf("value-blocks: num-values %d, num-blocks: %d, size: %d",
			m.Properties.NumValuesInValueBlocks, m.Properties.NumValueBlocks,
			m.Properties.ValueBlocksSize)
	}

	parallelism := false
	if rand.Intn(2) == 0 {
		parallelism = true
	}
	t.Logf("writer parallelism %t", parallelism)
	attributeExtractor := func(
		key []byte, keyPrefixLen int, value []byte) (base.ShortAttribute, error) {
		require.NotNil(t, key)
		require.Less(t, 0, keyPrefixLen)
		attribute := base.ShortAttribute(len(value) & '\x07')
		return attribute, nil
	}

	datadriven.RunTest(t, "testdata/writer_value_blocks", func(t *testing.T, td *datadriven.TestData) string {
		switch td.Cmd {
		case "build":
			if r != nil {
				_ = r.Close()
				r = nil
			}
			var meta *WriterMetadata
			var err error
			var blockSize int
			if td.HasArg("block-size") {
				td.ScanArgs(t, "block-size", &blockSize)
			}
			var inPlaceValueBound UserKeyPrefixBound
			if td.HasArg("in-place-bound") {
				var l, u string
				td.ScanArgs(t, "in-place-bound", &l, &u)
				inPlaceValueBound.Lower = []byte(l)
				inPlaceValueBound.Upper = []byte(u)
			}
			var disableValueBlocks bool
			if td.HasArg("disable-value-blocks") {
				td.ScanArgs(t, "disable-value-blocks", &disableValueBlocks)
			}
			meta, r, err = runBuildCmd(td, &WriterOptions{
				BlockSize:                 blockSize,
				Comparer:                  testkeys.Comparer,
				TableFormat:               formatVersion,
				Parallelism:               parallelism,
				RequiredInPlaceValueBound: inPlaceValueBound,
				ShortAttributeExtractor:   attributeExtractor,
				DisableValueBlocks:        disableValueBlocks,
			}, 0)
			if err != nil {
				return err.Error()
			}
			return formatMeta(meta)

		case "layout":
			l, err := r.Layout()
			if err != nil {
				return err.Error()
			}
			var buf bytes.Buffer
			l.Describe(&buf, true, r, func(key *base.InternalKey, value []byte) {
				fmt.Fprintf(&buf, "  %s:%s\n", key.String(), string(value))
			})
			return buf.String()

		case "scan-raw":
			// Raw scan does not fetch from value blocks.
			iter, err := r.NewIter(NoTransforms, nil /* lower */, nil /* upper */)
			if err != nil {
				return err.Error()
			}
			forceIgnoreValueBlocks := func(i *singleLevelIteratorRowBlocks) {
				i.vbReader = nil
				i.data.SetGetLazyValuer(nil)
				i.data.SetHasValuePrefix(false)
			}
			switch i := iter.(type) {
			case *twoLevelIteratorRowBlocks:
				forceIgnoreValueBlocks(&i.secondLevel)
			case *singleLevelIteratorRowBlocks:
				forceIgnoreValueBlocks(i)
			}
			defer iter.Close()

			var buf bytes.Buffer
			for kv := iter.First(); kv != nil; kv = iter.Next() {
				if kv.K.Kind() == InternalKeyKindSet {
					prefix := block.ValuePrefix(kv.V.ValueOrHandle[0])
					setWithSamePrefix := prefix.SetHasSamePrefix()
					if prefix.IsValueHandle() {
						attribute := prefix.ShortAttribute()
						vh := decodeValueHandle(kv.V.ValueOrHandle[1:])
						fmt.Fprintf(&buf, "%s:value-handle len %d block %d offset %d, att %d, same-pre %t\n",
							kv.K, vh.valueLen, vh.blockNum, vh.offsetInBlock, attribute, setWithSamePrefix)
					} else {
						fmt.Fprintf(&buf, "%s:in-place %s, same-pre %t\n", kv.K, kv.V.ValueOrHandle[1:], setWithSamePrefix)
					}
				} else {
					fmt.Fprintf(&buf, "%s:%s\n", kv.K, kv.V.ValueOrHandle)
				}
			}
			return buf.String()

		case "scan":
			iter, err := r.NewIter(NoTransforms, nil /* lower */, nil /* upper */)
			if err != nil {
				return err.Error()
			}
			defer iter.Close()
			var buf bytes.Buffer
			for kv := iter.First(); kv != nil; kv = iter.Next() {
				v, _, err := kv.Value(nil)
				if err != nil {
					return err.Error()
				}
				fmt.Fprintf(&buf, "%s:%s\n", kv.K, v)
			}
			return buf.String()

		case "scan-cloned-lazy-values":
			iter, err := r.NewIter(NoTransforms, nil /* lower */, nil /* upper */)
			if err != nil {
				return err.Error()
			}
			var fetchers [100]base.LazyFetcher
			var values []base.LazyValue
			n := 0
			var b []byte
			for kv := iter.First(); kv != nil; kv = iter.Next() {
				var lvClone base.LazyValue
				lvClone, b = kv.V.Clone(b, &fetchers[n])
				if kv.V.Fetcher != nil {
					_, callerOwned, err := kv.V.Value(nil)
					require.False(t, callerOwned)
					require.NoError(t, err)
				}
				n++
				values = append(values, lvClone)
			}
			require.NoError(t, iter.Error())
			iter.Close()
			var buf bytes.Buffer
			for i := range values {
				fmt.Fprintf(&buf, "%d", i)
				v, callerOwned, err := values[i].Value(nil)
				require.NoError(t, err)
				if values[i].Fetcher != nil {
					require.True(t, callerOwned)
					fmt.Fprintf(&buf, "(lazy: len %d, attr: %d): %s\n",
						values[i].Len(), values[i].Fetcher.Attribute.ShortAttribute, string(v))
					v2, callerOwned, err := values[i].Value(nil)
					require.NoError(t, err)
					require.True(t, callerOwned)
					require.Equal(t, &v[0], &v2[0])

				} else {
					require.False(t, callerOwned)
					fmt.Fprintf(&buf, "(in-place: len %d): %s\n", values[i].Len(), string(v))
				}
			}
			return buf.String()

		default:
			return fmt.Sprintf("unknown command: %s", td.Cmd)
		}
	})
}

func testBlockBufClear(t *testing.T, b1, b2 *blockBuf) {
	require.Equal(t, b1.tmp, b2.tmp)
}

func TestBlockBufClear(t *testing.T) {
	defer leaktest.AfterTest(t)()
	b1 := &blockBuf{}
	b1.tmp[0] = 1
	b1.compressedBuf = make([]byte, 1)
	b1.clear()
	testBlockBufClear(t, b1, &blockBuf{})
}

func TestClearDataBlockBuf(t *testing.T) {
	defer leaktest.AfterTest(t)()
	d := newDataBlockBuf(1, block.ChecksumTypeCRC32c)
	d.blockBuf.compressedBuf = make([]byte, 1)
	d.dataBlock.Add(ikey("apple"), nil)
	d.dataBlock.Add(ikey("banana"), nil)

	d.clear()
	testBlockBufClear(t, &d.blockBuf, &blockBuf{})

	dataBlockBufPool.Put(d)
}

func TestClearIndexBlockBuf(t *testing.T) {
	defer leaktest.AfterTest(t)()
	i := newIndexBlockBuf(false)
	i.block.Add(ikey("apple"), nil)
	i.block.Add(ikey("banana"), nil)
	i.clear()

	require.Equal(
		t, i.size.estimate, sizeEstimate{emptySize: rowblk.EmptySize},
	)
	indexBlockBufPool.Put(i)
}

func ikey(s string) base.InternalKey {
	return base.InternalKey{UserKey: []byte(s)}
}

func TestClearWriteTask(t *testing.T) {
	defer leaktest.AfterTest(t)()
	w := writeTaskPool.Get().(*writeTask)
	ch := make(chan bool, 1)
	w.compressionDone = ch
	w.buf = &dataBlockBuf{}
	w.flushableIndexBlock = &indexBlockBuf{}
	w.currIndexBlock = &indexBlockBuf{}
	w.indexEntrySep = ikey("apple")
	w.indexInflightSize = 1
	w.finishedIndexProps = []byte{'a', 'v'}

	w.clear()

	var nilDataBlockBuf *dataBlockBuf
	var nilIndexBlockBuf *indexBlockBuf
	// Channels should be the same(no new channel should be allocated)
	require.Equal(t, w.compressionDone, ch)
	require.Equal(t, w.buf, nilDataBlockBuf)
	require.Equal(t, w.flushableIndexBlock, nilIndexBlockBuf)
	require.Equal(t, w.currIndexBlock, nilIndexBlockBuf)
	require.Equal(t, w.indexEntrySep, base.InvalidInternalKey)
	require.Equal(t, w.indexInflightSize, 0)
	require.Equal(t, w.finishedIndexProps, []byte(nil))

	writeTaskPool.Put(w)
}

func TestDoubleClose(t *testing.T) {
	defer leaktest.AfterTest(t)()
	// There is code in Cockroach land which relies on Writer.Close being
	// idempotent. We should test this in Pebble, so that we don't cause
	// Cockroach test failures.
	f := &discardFile{}
	w := NewWriter(f, WriterOptions{
		BlockSize:   1,
		TableFormat: TableFormatPebblev1,
	})
	w.Set(ikey("a").UserKey, nil)
	w.Set(ikey("b").UserKey, nil)
	err := w.Close()
	require.NoError(t, err)
	err = w.Close()
	require.Equal(t, err, errWriterClosed)
}

func TestParallelWriterErrorProp(t *testing.T) {
	defer leaktest.AfterTest(t)()
	fs := vfs.NewMem()
	f, err := fs.Create("test", vfs.WriteCategoryUnspecified)
	require.NoError(t, err)
	opts := WriterOptions{
		TableFormat: TableFormatPebblev1, BlockSize: 1, Parallelism: true,
	}

	w := NewWriter(objstorageprovider.NewFileWritable(f), opts)
	// Directly testing this, because it's difficult to get the Writer to
	// encounter an error, precisely when the writeQueue is doing block writes.
	w.rw.(*RawRowWriter).coordination.writeQueue.err = errors.New("write queue write error")
	w.Set(ikey("a").UserKey, nil)
	w.Set(ikey("b").UserKey, nil)
	err = w.Close()
	require.Equal(t, err.Error(), "write queue write error")
}

func TestSizeEstimate(t *testing.T) {
	defer leaktest.AfterTest(t)()
	var sizeEstimate sizeEstimate
	datadriven.RunTest(t, "testdata/size_estimate",
		func(t *testing.T, td *datadriven.TestData) string {
			switch td.Cmd {
			case "init":
				if len(td.CmdArgs) != 1 {
					return "init <empty size>"
				}
				emptySize, err := strconv.Atoi(td.CmdArgs[0].String())
				if err != nil {
					return "invalid empty size"
				}
				sizeEstimate.init(uint64(emptySize))
				return "success"
			case "clear":
				sizeEstimate.clear()
				return fmt.Sprintf("%d", sizeEstimate.size())
			case "size":
				return fmt.Sprintf("%d", sizeEstimate.size())
			case "add_inflight":
				if len(td.CmdArgs) != 1 {
					return "add_inflight <inflight size estimate>"
				}
				inflightSize, err := strconv.Atoi(td.CmdArgs[0].String())
				if err != nil {
					return "invalid inflight size"
				}
				sizeEstimate.addInflight(inflightSize)
				return fmt.Sprintf("%d", sizeEstimate.size())
			case "entry_written":
				if len(td.CmdArgs) != 2 {
					return "entry_written <new_total_size> <prev_inflight_size>"
				}
				newTotalSize, err := strconv.Atoi(td.CmdArgs[0].String())
				if err != nil {
					return "invalid inflight size"
				}
				inflightSize, err := strconv.Atoi(td.CmdArgs[1].String())
				if err != nil {
					return "invalid inflight size"
				}
				sizeEstimate.writtenWithTotal(uint64(newTotalSize), inflightSize)
				return fmt.Sprintf("%d", sizeEstimate.size())
			case "num_written_entries":
				return fmt.Sprintf("%d", sizeEstimate.numWrittenEntries)
			case "num_inflight_entries":
				return fmt.Sprintf("%d", sizeEstimate.numInflightEntries)
			case "num_entries":
				return fmt.Sprintf("%d", sizeEstimate.numWrittenEntries+sizeEstimate.numInflightEntries)
			default:
				return fmt.Sprintf("unknown command: %s", td.Cmd)
			}
		})
}

func TestWriterClearCache(t *testing.T) {
	defer leaktest.AfterTest(t)()
	// Verify that Writer clears the cache of blocks that it writes.
	mem := vfs.NewMem()

	cacheOpts := sstableinternal.CacheOptions{
		Cache:   cache.New(64 << 20),
		CacheID: 1,
		FileNum: 1,
	}
	defer cacheOpts.Cache.Unref()

	readerOpts := ReaderOptions{
		Comparer: testkeys.Comparer,
		internal: sstableinternal.ReaderOptions{
			CacheOpts: cacheOpts,
		},
	}

	writerOpts := WriterOptions{
		Comparer:    testkeys.Comparer,
		TableFormat: TableFormatPebblev3,
		internal: sstableinternal.WriterOptions{
			CacheOpts: cacheOpts,
		},
	}
	invalidData := func() *cache.Value {
		invalid := []byte("invalid data")
		v := cache.Alloc(len(invalid))
		copy(v.Buf(), invalid)
		return v
	}

	build := func(name string) {
		f, err := mem.Create(name, vfs.WriteCategoryUnspecified)
		require.NoError(t, err)

		w := NewWriter(objstorageprovider.NewFileWritable(f), writerOpts)
		require.NoError(t, w.Set([]byte("hello"), []byte("world")))
		require.NoError(t, w.Set([]byte("hello@42"), []byte("world@42")))
		require.NoError(t, w.Set([]byte("hello@5"), []byte("world@5")))
		require.NoError(t, w.Close())
	}

	// Build the sstable a first time so that we can determine the locations of
	// all of the blocks.
	build("test")

	f, err := mem.Open("test")
	require.NoError(t, err)

	r, err := newReader(f, readerOpts)
	require.NoError(t, err)

	layout, err := r.Layout()
	require.NoError(t, err)

	foreachBH := func(layout *Layout, f func(bh block.Handle)) {
		for _, bh := range layout.Data {
			f(bh.Handle)
		}
		for _, bh := range layout.Index {
			f(bh)
		}
		f(layout.TopIndex)
		for _, nbh := range layout.Filter {
			f(nbh.Handle)
		}
		f(layout.RangeDel)
		for _, bh := range layout.ValueBlock {
			f(bh)
		}
		if layout.ValueIndex.Length != 0 {
			f(layout.ValueIndex)
		}
		f(layout.Properties)
		f(layout.MetaIndex)
	}

	// Poison the cache for each of the blocks.
	poison := func(bh block.Handle) {
		cacheOpts.Cache.Set(cacheOpts.CacheID, cacheOpts.FileNum, bh.Offset, invalidData()).Release()
	}
	foreachBH(layout, poison)

	// Build the table a second time. This should clear the cache for the blocks
	// that are written.
	build("test")

	// Verify that the written blocks have been cleared from the cache.
	check := func(bh block.Handle) {
		h := cacheOpts.Cache.Get(cacheOpts.CacheID, cacheOpts.FileNum, bh.Offset)
		if h.Get() != nil {
			t.Fatalf("%d: expected cache to be cleared, but found %q", bh.Offset, h.Get())
		}
	}
	foreachBH(layout, check)

	require.NoError(t, r.Close())
}

func TestWriterFlushHeuristics(t *testing.T) {
	defer leaktest.AfterTest(t)()
	datadriven.RunTest(t, "testdata/flush_heuristics", func(t *testing.T, td *datadriven.TestData) string {
		switch td.Cmd {
		case "build":
			var keySize, valSize, blockSize, targetSize, sizeThreshold, sizeClassAwareThreshold int
			td.ScanArgs(t, "key-size", &keySize)
			td.ScanArgs(t, "val-size", &valSize)
			td.ScanArgs(t, "block-size", &blockSize)
			td.ScanArgs(t, "target-size", &targetSize)
			td.ScanArgs(t, "threshold", &sizeThreshold)
			td.ScanArgs(t, "size-class-threshold", &sizeClassAwareThreshold)

			var sizeClasses []int
			if td.HasArg("hints") {
				var sizeClassHints string
				td.ScanArgs(t, "hints", &sizeClassHints)
				sizeStrClasses := strings.Split(sizeClassHints, ",")
				for _, strClass := range sizeStrClasses {
					size, err := strconv.Atoi(strClass)
					require.NoError(t, err)
					sizeClasses = append(sizeClasses, size)
				}
			}

			flush := shouldFlushWithHints(
				keySize,
				valSize,
				base.DefaultBlockRestartInterval,
				blockSize,
				1, /* numEntries */
				flushDecisionOptions{
					blockSize:               targetSize,
					blockSizeThreshold:      sizeThreshold,
					sizeClassAwareThreshold: sizeClassAwareThreshold,
				},
				sizeClasses,
			)
			return strconv.FormatBool(flush)

		default:
			return fmt.Sprintf("unknown command: %s", td.Cmd)
		}
	})
}

type discardFile struct {
	wrote int64
}

var _ objstorage.Writable = (*discardFile)(nil)

func (f *discardFile) Finish() error {
	return nil
}

func (f *discardFile) Abort() {}

func (f *discardFile) Write(p []byte) error {
	f.wrote += int64(len(p))
	return nil
}

type blockPropErrSite uint

const (
	errSiteAdd blockPropErrSite = iota
	errSiteFinishBlock
	errSiteFinishIndex
	errSiteFinishTable
	errSiteNone
)

type testBlockPropCollector struct {
	errSite blockPropErrSite
	err     error
}

var _ BlockPropertyCollector = (*testBlockPropCollector)(nil)

func (c *testBlockPropCollector) Name() string { return "testBlockPropCollector" }

func (c *testBlockPropCollector) AddPointKey(_ InternalKey, _ []byte) error {
	if c.errSite == errSiteAdd {
		return c.err
	}
	return nil
}

func (c *testBlockPropCollector) AddRangeKeys(_ Span) error {
	if c.errSite == errSiteAdd {
		return c.err
	}
	return nil
}

func (c *testBlockPropCollector) FinishDataBlock(_ []byte) ([]byte, error) {
	if c.errSite == errSiteFinishBlock {
		return nil, c.err
	}
	return nil, nil
}

func (c *testBlockPropCollector) AddPrevDataBlockToIndexBlock() {}

func (c *testBlockPropCollector) FinishIndexBlock(_ []byte) ([]byte, error) {
	if c.errSite == errSiteFinishIndex {
		return nil, c.err
	}
	return nil, nil
}

func (c *testBlockPropCollector) FinishTable(_ []byte) ([]byte, error) {
	if c.errSite == errSiteFinishTable {
		return nil, c.err
	}
	return nil, nil
}

func (c *testBlockPropCollector) AddCollectedWithSuffixReplacement(
	oldProp []byte, oldSuffix, newSuffix []byte,
) error {
	return errors.Errorf("not implemented")
}

func (c *testBlockPropCollector) SupportsSuffixReplacement() bool {
	return false
}

func TestWriterBlockPropertiesErrors(t *testing.T) {
	defer leaktest.AfterTest(t)()
	blockPropErr := errors.Newf("block property collector failed")
	testCases := []blockPropErrSite{
		errSiteAdd,
		errSiteFinishBlock,
		errSiteFinishIndex,
		errSiteFinishTable,
		errSiteNone,
	}

	var (
		k1 = base.MakeInternalKey([]byte("a"), 0, base.InternalKeyKindSet)
		v1 = []byte("apples")
		k2 = base.MakeInternalKey([]byte("b"), 0, base.InternalKeyKindSet)
		v2 = []byte("bananas")
		k3 = base.MakeInternalKey([]byte("c"), 0, base.InternalKeyKindSet)
		v3 = []byte("carrots")
	)

	for _, tc := range testCases {
		t.Run("", func(t *testing.T) {
			fs := vfs.NewMem()
			f, err := fs.Create("test", vfs.WriteCategoryUnspecified)
			require.NoError(t, err)

			w := NewRawWriter(objstorageprovider.NewFileWritable(f), WriterOptions{
				BlockSize: 1,
				BlockPropertyCollectors: []func() BlockPropertyCollector{
					func() BlockPropertyCollector {
						return &testBlockPropCollector{
							errSite: tc,
							err:     blockPropErr,
						}
					},
				},
				TableFormat: TableFormatPebblev1,
			})
			defer func() {
				if w != nil {
					_ = w.Close()
				}
			}()

			err = w.AddWithForceObsolete(k1, v1, false /* forceObsolete */)
			switch tc {
			case errSiteAdd:
				require.Error(t, err)
				require.Equal(t, blockPropErr, err)
				return
			case errSiteFinishBlock:
				require.NoError(t, err)
				// Addition of a second key completes the first block.
				err = w.AddWithForceObsolete(k2, v2, false /* forceObsolete */)
				require.Error(t, err)
				require.Equal(t, blockPropErr, err)
				return
			case errSiteFinishIndex:
				require.NoError(t, err)
				// Addition of a second key completes the first block.
				err = w.AddWithForceObsolete(k2, v2, false /* forceObsolete */)
				require.NoError(t, err)
				// The index entry for the first block is added after the completion of
				// the second block, which is triggered by adding a third key.
				err = w.AddWithForceObsolete(k3, v3, false /* forceObsolete */)
				require.Error(t, err)
				require.Equal(t, blockPropErr, err)
				return
			}

			err = w.Close()
			w = nil
			if tc == errSiteFinishTable {
				require.Error(t, err)
				require.Equal(t, blockPropErr, err)
			} else {
				require.NoError(t, err)
			}
		})
	}
}

func TestWriter_TableFormatCompatibility(t *testing.T) {
	defer leaktest.AfterTest(t)()
	testCases := []struct {
		name        string
		minFormat   TableFormat
		configureFn func(opts *WriterOptions)
		writeFn     func(w *Writer) error
	}{
		{
			name:      "block properties",
			minFormat: TableFormatPebblev1,
			configureFn: func(opts *WriterOptions) {
				opts.BlockPropertyCollectors = []func() BlockPropertyCollector{
					func() BlockPropertyCollector {
						return NewBlockIntervalCollector(
							"collector", &valueCharIntervalMapper{charIdx: 0}, nil,
						)
					},
				}
			},
		},
		{
			name:      "range keys",
			minFormat: TableFormatPebblev2,
			writeFn: func(w *Writer) error {
				return w.RangeKeyDelete([]byte("a"), []byte("b"))
			},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			for tf := TableFormatLevelDB; tf <= TableFormatMax; tf++ {
				t.Run(tf.String(), func(t *testing.T) {
					fs := vfs.NewMem()
					f, err := fs.Create("sst", vfs.WriteCategoryUnspecified)
					require.NoError(t, err)

					opts := WriterOptions{TableFormat: tf}
					if tc.configureFn != nil {
						tc.configureFn(&opts)
					}

					w := NewWriter(objstorageprovider.NewFileWritable(f), opts)
					if tc.writeFn != nil {
						err = tc.writeFn(w)
						require.NoError(t, err)
					}

					err = w.Close()
					if tf < tc.minFormat {
						require.Error(t, err)
					} else {
						require.NoError(t, err)
					}
				})
			}
		})
	}
}

// Tests for races, such as https://github.com/cockroachdb/cockroach/issues/77194,
// in the Writer.
func TestWriterRace(t *testing.T) {
	defer leaktest.AfterTest(t)()
	ks := testkeys.Alpha(5)
	ks = ks.EveryN(ks.Count() / 1_000)
	keys := make([][]byte, ks.Count())
	for ki := 0; ki < len(keys); ki++ {
		keys[ki] = testkeys.Key(ks, int64(ki))
	}
	readerOpts := ReaderOptions{
		Comparer: testkeys.Comparer,
		Filters:  map[string]base.FilterPolicy{},
	}

	var wg sync.WaitGroup
	for i := 0; i < 16; i++ {
		wg.Add(1)
		go func() {
			val := make([]byte, rand.Intn(1000))
			opts := WriterOptions{
				Comparer:    testkeys.Comparer,
				BlockSize:   rand.Intn(1 << 10),
				Compression: block.NoCompression,
			}
			defer wg.Done()
			f := &objstorage.MemObj{}
			w := newRowWriter(f, opts)
			for ki := 0; ki < len(keys); ki++ {
				require.NoError(t, w.AddWithForceObsolete(
					base.MakeInternalKey(keys[ki], base.SeqNum(ki), InternalKeyKindSet), val, false /* forceObsolete */))
				require.Equal(
					t, w.dataBlockBuf.dataBlock.CurKey().UserKey, keys[ki],
				)
			}
			require.NoError(t, w.Close())
			require.Equal(t, w.meta.LargestPoint.UserKey, keys[len(keys)-1])
			r, err := NewMemReader(f.Data(), readerOpts)
			require.NoError(t, err)
			defer r.Close()
			it, err := r.NewIter(NoTransforms, nil, nil)
			require.NoError(t, err)
			defer it.Close()
			ki := 0
			for kv := it.First(); kv != nil; kv = it.Next() {
				require.Equal(t, kv.K.UserKey, keys[ki])
				vBytes, _, err := kv.Value(nil)
				require.NoError(t, err)
				require.Equal(t, vBytes, val)
				ki++
			}
		}()
	}
	wg.Wait()
}

func TestObsoleteBlockPropertyCollectorFilter(t *testing.T) {
	defer leaktest.AfterTest(t)()
	var c obsoleteKeyBlockPropertyCollector
	var f obsoleteKeyBlockPropertyFilter
	require.Equal(t, c.Name(), f.Name())
	// Data block with 1 obsolete and 1 non-obsolete point.
	c.AddPoint(false)
	c.AddPoint(true)
	finishAndCheck := func(finishFunc func([]byte) ([]byte, error), expectedIntersects bool) {
		var buf [1]byte
		prop, err := finishFunc(buf[:0:1])
		require.NoError(t, err)
		expectedLength := 1
		if expectedIntersects {
			// The common case is encoded in 0 bytes
			expectedLength = 0
		}
		require.Equal(t, expectedLength, len(prop))
		// Confirm that the collector used the slice.
		require.Equal(t, unsafe.Pointer(&buf[0]), unsafe.Pointer(&prop[:1][0]))
		intersects, err := f.Intersects(prop)
		require.NoError(t, err)
		require.Equal(t, expectedIntersects, intersects)
	}
	finishAndCheck(c.FinishDataBlock, true)
	c.AddPrevDataBlockToIndexBlock()
	// Data block with only obsolete points.
	c.AddPoint(true)
	c.AddPoint(true)
	finishAndCheck(c.FinishDataBlock, false)
	c.AddPrevDataBlockToIndexBlock()
	// Index block has one obsolete block and one non-obsolete block.
	finishAndCheck(c.FinishIndexBlock, true)

	// Data block with obsolete point.
	c.AddPoint(true)
	finishAndCheck(c.FinishDataBlock, false)
	c.AddPrevDataBlockToIndexBlock()
	// Data block with obsolete point.
	c.AddPoint(true)
	finishAndCheck(c.FinishDataBlock, false)
	c.AddPrevDataBlockToIndexBlock()
	// Index block has only obsolete blocks.
	finishAndCheck(c.FinishIndexBlock, false)
	// Table is not obsolete.
	finishAndCheck(c.FinishTable, true)

	// Reset the collector state.
	c = obsoleteKeyBlockPropertyCollector{}
	// Table with only obsolete blocks.

	// Data block with obsolete point.
	c.AddPoint(true)
	finishAndCheck(c.FinishDataBlock, false)
	c.AddPrevDataBlockToIndexBlock()
	// Data block with obsolete point.
	c.AddPoint(true)
	finishAndCheck(c.FinishDataBlock, false)
	c.AddPrevDataBlockToIndexBlock()
	// Index block has only obsolete blocks.
	finishAndCheck(c.FinishIndexBlock, false)
	// Table is obsolete.
	finishAndCheck(c.FinishTable, false)
}

func BenchmarkWriter(b *testing.B) {
	keys := make([][]byte, 1e6)
	const keyLen = 24
	keySlab := make([]byte, keyLen*len(keys))
	for i := range keys {
		key := keySlab[i*keyLen : i*keyLen+keyLen]
		binary.BigEndian.PutUint64(key[:8], 123) // 16-byte shared prefix
		binary.BigEndian.PutUint64(key[8:16], 456)
		binary.BigEndian.PutUint64(key[16:], uint64(i))
		keys[i] = key
	}
	for _, format := range []TableFormat{TableFormatPebblev2, TableFormatPebblev3} {
		b.Run(fmt.Sprintf("format=%s", format.String()), func(b *testing.B) {
			runWriterBench(b, keys, nil, format)
		})
	}
}

func BenchmarkWriterWithVersions(b *testing.B) {
	keys := make([][]byte, 1e6)
	const keyLen = 26
	keySlab := make([]byte, keyLen*len(keys))
	for i := range keys {
		key := keySlab[i*keyLen : i*keyLen+keyLen]
		binary.BigEndian.PutUint64(key[:8], 123) // 16-byte shared prefix
		binary.BigEndian.PutUint64(key[8:16], 456)
		// @ is ascii value 64. Placing any byte with value 64 in these 8 bytes
		// will confuse testkeys.Comparer, when we pass it a key after splitting
		// of the suffix, since Comparer thinks this prefix is also a key with a
		// suffix. Hence, we print as a base 10 string.
		require.Equal(b, 8, copy(key[16:], fmt.Sprintf("%8d", i/2)))
		key[24] = '@'
		// Ascii representation of single digit integer 2-(i%2).
		key[25] = byte(48 + 2 - (i % 2))
		keys[i] = key
	}
	// TableFormatPebblev3 can sometimes be ~50% slower than
	// TableFormatPebblev2, since testkeys.Compare is expensive (mainly due to
	// split) and with v3 we have to call it twice for 50% of the Set calls,
	// since they have the same prefix as the preceding key.
	for _, format := range []TableFormat{TableFormatPebblev2, TableFormatPebblev3} {
		b.Run(fmt.Sprintf("format=%s", format.String()), func(b *testing.B) {
			runWriterBench(b, keys, testkeys.Comparer, format)
		})
	}
}

func runWriterBench(b *testing.B, keys [][]byte, comparer *base.Comparer, format TableFormat) {
	for _, bs := range []int{base.DefaultBlockSize, 32 << 10} {
		b.Run(fmt.Sprintf("block=%s", humanize.Bytes.Int64(int64(bs))), func(b *testing.B) {
			for _, filter := range []bool{true, false} {
				b.Run(fmt.Sprintf("filter=%t", filter), func(b *testing.B) {
					for _, comp := range []block.Compression{block.NoCompression, block.SnappyCompression, block.ZstdCompression} {
						b.Run(fmt.Sprintf("compression=%s", comp), func(b *testing.B) {
							opts := WriterOptions{
								BlockRestartInterval: 16,
								BlockSize:            bs,
								Comparer:             comparer,
								Compression:          comp,
								TableFormat:          format,
							}
							if filter {
								opts.FilterPolicy = bloom.FilterPolicy(10)
							}
							f := &discardFile{}
							b.ResetTimer()
							for i := 0; i < b.N; i++ {
								f.wrote = 0
								w := NewWriter(f, opts)

								for j := range keys {
									if err := w.Set(keys[j], keys[j]); err != nil {
										b.Fatal(err)
									}
								}
								if err := w.Close(); err != nil {
									b.Fatal(err)
								}
								b.SetBytes(int64(f.wrote))
							}
						})
					}
				})
			}
		})
	}
}
