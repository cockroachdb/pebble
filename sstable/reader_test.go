// Copyright 2018 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package sstable

import (
	"bytes"
	"context"
	"encoding/binary"
	"fmt"
	"io"
	"math"
	"math/rand/v2"
	"os"
	"path"
	"path/filepath"
	"regexp"
	"slices"
	"strconv"
	"strings"
	"testing"
	"time"

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
	"github.com/cockroachdb/pebble/objstorage/remote"
	"github.com/cockroachdb/pebble/sstable/block"
	"github.com/cockroachdb/pebble/sstable/blockiter"
	"github.com/cockroachdb/pebble/sstable/valblk"
	"github.com/cockroachdb/pebble/sstable/virtual"
	"github.com/cockroachdb/pebble/vfs"
	"github.com/cockroachdb/pebble/vfs/errorfs"
	"github.com/stretchr/testify/require"
)

// get is a testing helper that simulates a read and helps verify bloom filters
// until they are available through iterators.
func (r *Reader) get(key []byte) (value []byte, err error) {
	if r.err != nil {
		return nil, r.err
	}

	if r.tableFilter != nil {
		dataH, err := r.readFilterBlock(context.Background(), block.NoReadEnv, noReadHandle, r.filterBH)
		if err != nil {
			return nil, err
		}
		var lookupKey []byte
		if r.Comparer.Split != nil {
			lookupKey = key[:r.Comparer.Split(key)]
		} else {
			lookupKey = key
		}
		mayContain := r.tableFilter.mayContain(dataH.BlockData(), lookupKey)
		dataH.Release()
		if !mayContain {
			return nil, base.ErrNotFound
		}
	}

	i, err := r.NewIter(NoTransforms, nil /* lower */, nil /* upper */, AssertNoBlobHandles)
	if err != nil {
		return nil, err
	}
	ikv := i.SeekGE(key, base.SeekGEFlagsNone)

	if ikv == nil || r.Comparer.Compare(key, ikv.K.UserKey) != 0 {
		err := i.Close()
		if err == nil {
			err = base.ErrNotFound
		}
		return nil, err
	}
	value, _, err = ikv.Value(nil)
	if err != nil {
		return nil, err
	}

	// The value will be "freed" when the iterator is closed, so make a copy
	// which will outlast the lifetime of the iterator.
	newValue := make([]byte, len(value))
	copy(newValue, value)
	if err := i.Close(); err != nil {
		return nil, err
	}
	return newValue, nil
}

func TestVirtualReader(t *testing.T) {
	defer leaktest.AfterTest(t)()
	t.Run("props", func(t *testing.T) {
		runVirtualReaderTest(t, "testdata/virtual_reader_props", 0 /* blockSize */, 0 /* indexBlockSize */)
	})
	t.Run("iter", func(t *testing.T) {
		for run := 0; run < 100; run++ {
			var blockSize, indexBlockSize int
			if run > 0 {
				blockSize = rand.IntN(200)
				indexBlockSize = rand.IntN(200)
			}
			t.Logf("run %d: blockSize=%d indexBlockSize=%d", run, blockSize, indexBlockSize)
			runVirtualReaderTest(t, "testdata/virtual_reader_iter", blockSize, indexBlockSize)
		}
	})
}

func runVirtualReaderTest(t *testing.T, path string, blockSize, indexBlockSize int) {
	// A faux filenum used to create fake filemetadata for testing.
	var fileNum int = 1
	nextFileNum := func() base.FileNum {
		fileNum++
		return base.FileNum(fileNum - 1)
	}

	// Set during the latest build command.
	var r *Reader
	var wMeta *WriterMetadata
	var bp block.BufferPool

	// Set during the latest virtualize command.
	var env ReadEnv
	var syntheticSuffix SyntheticSuffix

	defer func() {
		if r != nil {
			require.NoError(t, r.Close())
			bp.Release()
		}
	}()

	formatVirtualReader := func(r *Reader, showProps bool, env ReadEnv, backingSize, tableSize uint64) string {
		var b bytes.Buffer
		fmt.Fprintf(&b, "bounds:  [%s-%s]\n", env.Virtual.Lower, env.Virtual.Upper)
		if showProps {
			fmt.Fprintf(&b, "filenum: %s\n", env.Virtual.FileNum.String())
			fmt.Fprintf(&b, "props:\n")
			props, err := r.ReadPropertiesBlock(context.Background(), nil)
			require.NoError(t, err)
			p := props.GetScaledProperties(backingSize, tableSize)
			for _, line := range strings.Split(strings.TrimSpace(p.String()), "\n") {
				fmt.Fprintf(&b, "  %s\n", line)
			}
		}
		return b.String()
	}

	datadriven.RunTest(t, path, func(t *testing.T, td *datadriven.TestData) string {
		switch td.Cmd {
		case "build":
			if r != nil {
				bp.Release()
				_ = r.Close()
				r = nil
			}
			var err error
			writerOpts := &WriterOptions{
				TableFormat:    TableFormatMax,
				Comparer:       testkeys.Comparer,
				BlockSize:      blockSize,
				IndexBlockSize: indexBlockSize,
				BlockPropertyCollectors: []func() BlockPropertyCollector{
					NewTestKeysBlockPropertyCollector,
				},
			}
			wMeta, r, err = runBuildCmd(td, writerOpts, nil /* cacheHandle */)
			if err != nil {
				return err.Error()
			}
			bp.Init(5)
			return formatWriterMetadata(td, wMeta)

		case "virtualize":
			// virtualize will split the previously built physical sstable into
			// a single sstable with virtual bounds. The command assumes that
			// the bounds for the virtual sstable are valid. For the purposes of
			// this command the bounds must be valid keys. In general, and for
			// this command, range key/range del spans must also not span across
			// virtual sstable bounds.
			if wMeta == nil {
				return "build must be called at least once before virtualize"
			}

			var params virtual.VirtualReaderParams
			// Parse the virtualization bounds.
			var lowerStr, upperStr string
			td.ScanArgs(t, "lower", &lowerStr)
			td.ScanArgs(t, "upper", &upperStr)
			params.Lower = base.ParseInternalKey(lowerStr)
			params.Upper = base.ParseInternalKey(upperStr)

			showProps := td.HasArg("show-props")

			var syntheticPrefix []byte
			if td.HasArg("prefix") {
				var synthPrefixStr string
				td.ScanArgs(t, "prefix", &synthPrefixStr)
				syntheticPrefix = []byte(synthPrefixStr)
			}

			syntheticSuffix = nil
			if td.HasArg("suffix") {
				var synthSuffixStr string
				td.ScanArgs(t, "suffix", &synthSuffixStr)
				syntheticSuffix = []byte(synthSuffixStr)
			}

			params.FileNum = nextFileNum()
			env.Virtual = &params

			var err error
			transforms := IterTransforms{
				SyntheticPrefixAndSuffix: blockiter.MakeSyntheticPrefixAndSuffix(syntheticPrefix, syntheticSuffix),
			}
			tableSize, err := r.EstimateDiskUsage(params.Lower.UserKey, params.Upper.UserKey, env, transforms)
			if err != nil {
				return err.Error()
			}
			return formatVirtualReader(r, showProps, env, wMeta.Size, tableSize)

		case "compaction-iter":
			// Creates a compaction iterator from the virtual reader, and then
			// just scans the keyspace. Which is all a compaction iterator is
			// used for. This tests the First and Next calls.
			if env.Virtual == nil {
				return "virtualize must be called before creating compaction iters"
			}

			var rp valblk.ReaderProvider
			transforms := IterTransforms{
				SyntheticPrefixAndSuffix: blockiter.MakeSyntheticPrefixAndSuffix(nil, syntheticSuffix),
			}
			env.Block.BufferPool = &bp
			iter, err := r.NewCompactionIter(transforms, env, rp, AssertNoBlobHandles)
			if err != nil {
				return err.Error()
			}

			var buf bytes.Buffer
			for kv := iter.First(); kv != nil; kv = iter.Next() {
				fmt.Fprintf(&buf, "%s:%s\n", kv.K.String(), kv.InPlaceValue())
			}
			err = iter.Close()
			if err != nil {
				return err.Error()
			}
			return buf.String()

		case "constrain":
			if env.Virtual == nil {
				return "virtualize must be called before constrain"
			}
			splits := strings.Split(td.CmdArgs[0].String(), ",")
			of, ol := []byte(splits[0]), []byte(splits[1])
			inclusive, f, l := env.Virtual.ConstrainBounds(of, ol, splits[2] == "true", r.Comparer.Compare)
			var buf bytes.Buffer
			buf.Write(f)
			buf.WriteByte(',')
			buf.Write(l)
			buf.WriteByte(',')
			if inclusive {
				buf.WriteString("true")
			} else {
				buf.WriteString("false")
			}
			buf.WriteByte('\n')
			return buf.String()

		case "scan-range-del":
			if env.Virtual == nil {
				return "virtualize must be called before scan-range-del"
			}
			transforms := FragmentIterTransforms{
				SyntheticPrefixAndSuffix: blockiter.MakeSyntheticPrefixAndSuffix(nil, syntheticSuffix),
			}
			iter, err := r.NewRawRangeDelIter(context.Background(), transforms, env)
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
			if env.Virtual == nil {
				return "virtualize must be called before scan-range-key"
			}
			transforms := FragmentIterTransforms{
				SyntheticPrefixAndSuffix: blockiter.MakeSyntheticPrefixAndSuffix(nil, syntheticSuffix),
			}

			iter, err := r.NewRawRangeKeyIter(context.Background(), transforms, env)
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

		case "iter":
			if env.Virtual == nil {
				return "virtualize must be called before iter"
			}
			var lower, upper []byte
			var lowerStr, upperStr string
			td.MaybeScanArgs(t, "lower", &lowerStr)
			td.MaybeScanArgs(t, "upper", &upperStr)
			if lowerStr != "" {
				lower = []byte(lowerStr)
			}
			if upperStr != "" {
				upper = []byte(upperStr)
			}
			var stats base.InternalIteratorStats
			runIterCmdOpts := []runIterCmdOption{
				runIterCmdStats(&stats),
			}
			var filterer *BlockPropertiesFilterer
			if td.HasArg("with-masking-filter") {
				maskingFilter := NewTestKeysMaskingFilter()
				runIterCmdOpts = append(runIterCmdOpts, runIterCmdMaskingFilter(maskingFilter))
				var err error
				filterer, err = IntersectsTable(
					[]BlockPropertyFilter{maskingFilter},
					nil, wMeta.Properties.UserProperties, syntheticSuffix,
				)
				if err != nil {
					td.Fatalf(t, "error creating filterer: %v", err)
				}
				if filterer == nil {
					td.Fatalf(t, "nil filterer")
				}
			}
			env.Block.Stats = &stats
			iter, err := r.NewPointIter(context.Background(), IterOptions{
				Transforms: IterTransforms{
					SyntheticPrefixAndSuffix: blockiter.MakeSyntheticPrefixAndSuffix(nil, syntheticSuffix),
				},
				Lower:                lower,
				Upper:                upper,
				Filterer:             filterer,
				FilterBlockSizeLimit: NeverUseFilterBlock,
				Env:                  env,
				ReaderProvider:       MakeTrivialReaderProvider(r),
				BlobContext:          AssertNoBlobHandles,
			})
			if err != nil {
				return err.Error()
			}
			return runIterCmd(td, iter, true, runIterCmdOpts...)

		default:
			return fmt.Sprintf("unknown command: %s", td.Cmd)
		}
	})
}

func TestReader(t *testing.T) {
	defer leaktest.AfterTest(t)()
	writerOpts := map[string]WriterOptions{
		// No bloom filters.
		"default": {},
		"bloom10bit": {
			// The standard policy.
			FilterPolicy: bloom.FilterPolicy(10),
			FilterType:   base.TableFilter,
		},
		"bloom1bit": {
			// A policy with many false positives.
			FilterPolicy: bloom.FilterPolicy(1),
			FilterType:   base.TableFilter,
		},
		"bloom100bit": {
			// A policy unlikely to have false positives.
			FilterPolicy: bloom.FilterPolicy(100),
			FilterType:   base.TableFilter,
		},
	}

	blockSizes := map[string]int{
		"1bytes":   1,
		"5bytes":   5,
		"10bytes":  10,
		"25bytes":  25,
		"Maxbytes": math.MaxInt32,
	}

	opts := map[string]*Comparer{
		"default":      testkeys.Comparer,
		"prefixFilter": fixtureComparer,
	}

	testDirs := map[string]string{
		"default":      "testdata/reader",
		"prefixFilter": "testdata/prefixreader",
	}

	for format := TableFormatPebblev2; format <= TableFormatMax; format++ {
		for dName, blockSize := range blockSizes {
			for iName, indexBlockSize := range blockSizes {
				for lName, tableOpt := range writerOpts {
					for oName, cmp := range opts {
						tableOpt.BlockSize = blockSize
						tableOpt.Comparer = cmp
						tableOpt.IndexBlockSize = indexBlockSize
						tableOpt.TableFormat = format

						t.Run(
							fmt.Sprintf("format=%d,opts=%s,writerOpts=%s,blockSize=%s,indexSize=%s",
								format, oName, lName, dName, iName),
							func(t *testing.T) {
								runTestReader(
									t, tableOpt, testDirs[oName], nil /* Reader */, true)
							})
					}
				}
			}
		}
	}
}

func TestReaderHideObsolete(t *testing.T) {
	defer leaktest.AfterTest(t)()
	blockSizes := map[string]int{
		"1bytes":   1,
		"5bytes":   5,
		"10bytes":  10,
		"25bytes":  25,
		"Maxbytes": math.MaxInt32,
	}
	for dName, blockSize := range blockSizes {
		opts := WriterOptions{
			TableFormat:    TableFormatPebblev4,
			BlockSize:      blockSize,
			IndexBlockSize: blockSize,
			Comparer:       testkeys.Comparer,
		}
		t.Run(fmt.Sprintf("blockSize=%s", dName), func(t *testing.T) {
			runTestReader(
				t, opts, "testdata/reader_hide_obsolete",
				nil /* Reader */, true)
		})
		opts = WriterOptions{
			TableFormat:    TableFormatPebblev4,
			BlockSize:      blockSize,
			IndexBlockSize: 1 << 30, // Force a single-level index block.
			Comparer:       testkeys.Comparer,
		}
		t.Run(fmt.Sprintf("singleLevel/blockSize=%s", dName), func(t *testing.T) {
			runTestReader(
				t, opts, "testdata/reader_hide_obsolete",
				nil /* Reader */, true)
		})
	}
}

func TestHamletReader(t *testing.T) {
	defer leaktest.AfterTest(t)()
	for _, fixture := range TestFixtures {
		f, err := vfs.Default.Open(filepath.Join("testdata", fixture.Filename))
		require.NoError(t, err)

		r, err := newReader(f, ReaderOptions{})
		require.NoError(t, err)

		t.Run(
			fmt.Sprintf("sst=%s", fixture.Filename),
			func(t *testing.T) {
				runTestReader(t, WriterOptions{}, "testdata/hamletreader", r, false)
			},
		)
	}
}

func forEveryTableFormat[I any](
	t *testing.T, formatTable [NumTableFormats]I, runTest func(*testing.T, TableFormat, I),
) {
	t.Helper()
	for tf := TableFormatUnspecified + 1; tf <= TableFormatMax; tf++ {
		t.Run(tf.String(), func(t *testing.T) {
			runTest(t, tf, formatTable[tf])
		})
	}
}

func TestReaderStats(t *testing.T) {
	defer leaktest.AfterTest(t)()
	forEveryTableFormat[string](t,
		[NumTableFormats]string{
			TableFormatUnspecified: "",
			TableFormatLevelDB:     "testdata/readerstats_LevelDB",
			TableFormatRocksDBv2:   "testdata/readerstats_LevelDB",
			TableFormatPebblev1:    "testdata/readerstats_LevelDB",
			TableFormatPebblev2:    "testdata/readerstats_LevelDB",
			TableFormatPebblev3:    "testdata/readerstats_Pebblev3",
			TableFormatPebblev4:    "testdata/readerstats_Pebblev3",
		}, func(t *testing.T, format TableFormat, dir string) {
			if dir == "" {
				t.Skip()
			}
			writerOpt := WriterOptions{
				BlockSize:      32 << 10,
				IndexBlockSize: 32 << 10,
				Comparer:       testkeys.Comparer,
				TableFormat:    format,
			}
			runTestReader(t, writerOpt, dir, nil /* Reader */, false /* printValue */)
		})
}

func TestReaderWithBlockPropertyFilter(t *testing.T) {
	defer leaktest.AfterTest(t)()
	// Some of these tests examine internal iterator state, so they require
	// determinism. When the invariants tag is set, disableBoundsOpt may disable
	// the bounds optimization depending on the iterator pointer address. This
	// can add nondeterminism to the internal iterator statae. Disable this
	// nondeterminism for the duration of this test.
	ensureBoundsOptDeterminism = true
	defer func() { ensureBoundsOptDeterminism = false }()

	forEveryTableFormat[string](t,
		[NumTableFormats]string{
			TableFormatUnspecified: "", // Block properties unsupported
			TableFormatLevelDB:     "", // Block properties unsupported
			TableFormatRocksDBv2:   "", // Block properties unsupported
			TableFormatPebblev1:    "", // Block properties unsupported
			TableFormatPebblev2:    "testdata/reader_bpf/Pebblev2",
			TableFormatPebblev3:    "testdata/reader_bpf/Pebblev3",
			TableFormatPebblev4:    "testdata/reader_bpf/Pebblev3",
		}, func(t *testing.T, format TableFormat, dir string) {
			if dir == "" {
				t.Skip("Block-properties unsupported")
			}
			writerOpt := WriterOptions{
				Comparer:                testkeys.Comparer,
				TableFormat:             format,
				BlockPropertyCollectors: []func() BlockPropertyCollector{NewTestKeysBlockPropertyCollector},
			}
			runTestReader(t, writerOpt, dir, nil /* Reader */, false)
		})
}

func TestReaderAttributes(t *testing.T) {
	defer leaktest.AfterTest(t)()
	opts := WriterOptions{
		BlockSize:      math.MaxInt32,
		IndexBlockSize: math.MaxInt32,
		Comparer:       testkeys.Comparer,
	}
	// Iterate through TableFormats >= TableFormatPebblev6, which
	// introduces blob handles.
	for tf := TableFormatPebblev6; tf <= TableFormatMax; tf++ {
		opts.TableFormat = tf
		t.Run(tf.String(), func(t *testing.T) {
			runTestReader(t, opts, "testdata/reader_attributes", nil /* Reader */, false)
		})
	}
}

func TestInjectedErrors(t *testing.T) {
	defer leaktest.AfterTest(t)()
	for _, fixture := range TestFixtures {
		run := func(i int) (reterr error) {
			f, err := vfs.Default.Open(filepath.Join("testdata", fixture.Filename))
			require.NoError(t, err)

			r, err := newReader(errorfs.WrapFile(f, errorfs.ErrInjected.If(errorfs.OnIndex(int32(i)))), ReaderOptions{})
			if err != nil {
				return firstError(err, f.Close())
			}
			defer func() { reterr = firstError(reterr, r.Close()) }()

			_, err = r.EstimateDiskUsage([]byte("borrower"), []byte("lender"), NoReadEnv, NoTransforms)
			if err != nil {
				return err
			}

			iter, err := r.NewIter(NoTransforms, nil, nil, AssertNoBlobHandles)
			if err != nil {
				return err
			}
			defer func() { reterr = firstError(reterr, iter.Close()) }()
			for kv := iter.First(); kv != nil; kv = iter.Next() {
				val, _, err := kv.Value(nil)
				if err != nil {
					return err
				}
				if val == nil {
					break
				}
			}
			if err = iter.Error(); err != nil {
				return err
			}
			return nil
		}
		for i := 0; ; i++ {
			err := run(i)
			if errors.Is(err, errorfs.ErrInjected) {
				t.Logf("%q, index %d: %s", fixture.Filename, i, err)
				continue
			}
			if err != nil {
				t.Errorf("%q, index %d: non-injected error: %+v", fixture.Filename, i, err)
				break
			}
			t.Logf("%q: no error at index %d", fixture.Filename, i)
			break
		}
	}
}

func TestInvalidReader(t *testing.T) {
	defer leaktest.AfterTest(t)()
	invalid, err := NewSimpleReadable(vfs.NewMemFile([]byte("invalid sst bytes")))
	if err != nil {
		t.Fatal(err)
	}
	testCases := []struct {
		readable objstorage.Readable
		expected string
	}{
		{nil, "nil file"},
		{invalid, "invalid table"},
	}
	for _, tc := range testCases {
		r, err := NewReader(context.Background(), tc.readable, ReaderOptions{})
		if !strings.Contains(err.Error(), tc.expected) {
			t.Fatalf("expected %q, but found %q", tc.expected, err.Error())
		}
		if r != nil {
			t.Fatalf("found non-nil reader returned with non-nil error %q", err.Error())
		}
	}
}

func indexLayoutString(t *testing.T, r *Reader) string {
	indexH, err := r.readTopLevelIndexBlock(context.Background(), block.NoReadEnv, noReadHandle)
	require.NoError(t, err)
	defer indexH.Release()
	var buf strings.Builder
	twoLevelIndex := r.Attributes.Has(AttributeTwoLevelIndex)
	buf.WriteString("index entries:\n")
	iter := r.tableFormat.newIndexIter()
	require.NoError(t, iter.Init(r.Comparer, indexH.BlockData(), NoTransforms))
	defer func() {
		require.NoError(t, iter.Close())
	}()
	require.NoError(t, err)
	for valid := iter.First(); valid; valid = iter.Next() {
		bh, err := iter.BlockHandleWithProperties()
		require.NoError(t, err)
		fmt.Fprintf(&buf, " %s: size %d\n", string(iter.Separator()), bh.Length)
		if twoLevelIndex {
			b, err := r.readIndexBlock(context.Background(), block.NoReadEnv, noReadHandle, bh.Handle)
			require.NoError(t, err)
			defer b.Release()
			iter2 := r.tableFormat.newIndexIter()
			require.NoError(t, iter2.Init(r.Comparer, b.BlockData(), NoTransforms))
			defer func() {
				require.NoError(t, iter2.Close())
			}()
			require.NoError(t, err)
			for valid := iter2.First(); valid; valid = iter2.Next() {
				bh, err := iter2.BlockHandleWithProperties()
				require.NoError(t, err)
				fmt.Fprintf(&buf, "   %s: size %d\n", string(iter2.Separator()), bh.Length)
			}
		}
	}
	return buf.String()
}

func runTestReader(t *testing.T, o WriterOptions, dir string, r *Reader, printValue bool) {
	datadriven.Walk(t, dir, func(t *testing.T, path string) {
		var c *cache.Cache
		var ch *cache.Handle
		closeCache := func() {
			if ch != nil {
				ch.Close()
				ch = nil
			}
			if c != nil {
				c.Unref()
				c = nil
			}
		}
		defer func() {
			if r != nil {
				r.Close()
				r = nil
			}
			closeCache()
		}()

		datadriven.RunTest(t, path, func(t *testing.T, d *datadriven.TestData) string {
			switch d.Cmd {
			case "build":
				if r != nil {
					r.Close()
					r = nil
				}
				var cacheSize int
				var printLayout, printAttributes bool
				d.MaybeScanArgs(t, "cache-size", &cacheSize)
				d.MaybeScanArgs(t, "print-layout", &printLayout)
				d.MaybeScanArgs(t, "block-size", &o.BlockSize)
				d.MaybeScanArgs(t, "index-block-size", &o.IndexBlockSize)
				d.MaybeScanArgs(t, "print-attributes", &printAttributes)

				closeCache()
				c = cache.New(int64(cacheSize))
				ch = c.NewHandle()
				var err error
				_, r, err = runBuildCmd(d, &o, ch)
				if err != nil {
					return err.Error()
				}
				var buf bytes.Buffer
				if printLayout {
					buf.WriteString(indexLayoutString(t, r))
				}
				if printAttributes {
					buf.WriteString("attributes: ")
					buf.WriteString(r.Attributes.String())
				}
				return buf.String()

			case "iter":
				var globalSeqNum uint64
				d.MaybeScanArgs(t, "globalSeqNum", &globalSeqNum)
				var stats base.InternalIteratorStats
				transforms := IterTransforms{
					SyntheticSeqNum: SyntheticSeqNum(globalSeqNum),
				}
				var bpfs []BlockPropertyFilter
				if d.HasArg("block-property-filter") {
					var filterMin, filterMax uint64
					d.ScanArgs(t, "block-property-filter", &filterMin, &filterMax)
					bpf := NewTestKeysBlockPropertyFilter(filterMin, filterMax)
					bpfs = append(bpfs, bpf)
				}
				if d.HasArg("hide-obsolete-points") {
					d.ScanArgs(t, "hide-obsolete-points", &transforms.HideObsoletePoints)
					if transforms.HideObsoletePoints {
						var retHideObsoletePoints bool
						retHideObsoletePoints, bpfs = r.TryAddBlockPropertyFilterForHideObsoletePoints(
							base.SeqNumMax, base.SeqNumMax-1, bpfs)
						require.True(t, retHideObsoletePoints)
					}
				}
				if d.HasArg("hide-obsolete-points-without-filter") {
					var hideObsoletePoints bool
					d.ScanArgs(t, "hide-obsolete-points-without-filter", &hideObsoletePoints)
					if hideObsoletePoints {
						transforms.HideObsoletePoints = true
					}
				}
				var filterer *BlockPropertiesFilterer
				if len(bpfs) > 0 {
					filterer = newBlockPropertiesFilterer(bpfs, nil, nil)
					intersects, err :=
						filterer.intersectsUserPropsAndFinishInit(r.UserProperties)
					if err != nil {
						return err.Error()
					}
					if !intersects {
						return "table does not intersect BlockPropertyFilter"
					}
				}
				iter, err := r.NewPointIter(context.Background(), IterOptions{
					Transforms:           transforms,
					Filterer:             filterer,
					FilterBlockSizeLimit: AlwaysUseFilterBlock,
					Env:                  ReadEnv{Block: block.ReadEnv{Stats: &stats, IterStats: nil}},
					ReaderProvider:       MakeTrivialReaderProvider(r),
					BlobContext:          AssertNoBlobHandles,
				})
				if err != nil {
					return err.Error()
				}
				return runIterCmd(d, iter, printValue, runIterCmdStats(&stats), runIterCmdShowCommands)

			case "get":
				var b bytes.Buffer
				for _, k := range strings.Split(d.Input, "\n") {
					v, err := r.get([]byte(k))
					if err != nil {
						fmt.Fprintf(&b, "<err: %s>\n", err)
					} else {
						fmt.Fprintln(&b, string(v))
					}
				}
				return b.String()
			default:
				return fmt.Sprintf("unknown command: %s", d.Cmd)
			}
		})
	})
}

func TestReaderCheckComparerMerger(t *testing.T) {
	defer leaktest.AfterTest(t)()
	const testTable = "test"

	testComparer := &base.Comparer{
		Name:      "test.comparer",
		Compare:   base.DefaultComparer.Compare,
		Equal:     base.DefaultComparer.Equal,
		Separator: base.DefaultComparer.Separator,
		Successor: base.DefaultComparer.Successor,
	}
	testMerger := &base.Merger{
		Name:  "test.merger",
		Merge: base.DefaultMerger.Merge,
	}
	writerOpts := WriterOptions{
		Comparer:   testComparer,
		MergerName: "test.merger",
	}

	mem := vfs.NewMem()
	f0, err := mem.Create(testTable, vfs.WriteCategoryUnspecified)
	require.NoError(t, err)

	w := NewWriter(objstorageprovider.NewFileWritable(f0), writerOpts)
	require.NoError(t, w.Set([]byte("test"), nil))
	require.NoError(t, w.Close())

	testCases := []struct {
		comparers []*base.Comparer
		mergers   []*base.Merger
		expected  string
	}{
		{
			[]*base.Comparer{testComparer},
			[]*base.Merger{testMerger},
			"",
		},
		{
			[]*base.Comparer{testComparer, base.DefaultComparer},
			[]*base.Merger{testMerger, base.DefaultMerger},
			"",
		},
		{
			[]*base.Comparer{},
			[]*base.Merger{testMerger},
			"unknown comparer test.comparer",
		},
		{
			[]*base.Comparer{base.DefaultComparer},
			[]*base.Merger{testMerger},
			"unknown comparer test.comparer",
		},
		{
			[]*base.Comparer{testComparer},
			[]*base.Merger{},
			"unknown merger test.merger",
		},
		{
			[]*base.Comparer{testComparer},
			[]*base.Merger{base.DefaultMerger},
			"unknown merger test.merger",
		},
	}

	for _, c := range testCases {
		t.Run("", func(t *testing.T) {
			f1, err := mem.Open(testTable)
			require.NoError(t, err)

			comparers := make(Comparers)
			for _, comparer := range c.comparers {
				comparers[comparer.Name] = comparer
			}
			mergers := make(Mergers)
			for _, merger := range c.mergers {
				mergers[merger.Name] = merger
			}

			r, err := newReader(f1, ReaderOptions{
				Comparers: comparers,
				Mergers:   mergers,
			})
			if err != nil {
				if r != nil {
					t.Fatalf("found non-nil reader returned with non-nil error %q", err.Error())
				}
				if !strings.HasSuffix(err.Error(), c.expected) {
					t.Fatalf("expected %q, but found %q", c.expected, err.Error())
				}
			} else if c.expected != "" {
				t.Fatalf("expected %q, but found success", c.expected)
			}
			if r != nil {
				_ = r.Close()
			}
		})
	}
}
func checkValidPrefix(prefix, key []byte) bool {
	return prefix == nil || bytes.HasPrefix(key, prefix)
}

func TestCompactionIteratorSetupForCompaction(t *testing.T) {
	defer leaktest.AfterTest(t)()
	tmpDir := path.Join(t.TempDir())
	provider, err := objstorageprovider.Open(objstorageprovider.DefaultSettings(vfs.Default, tmpDir))
	require.NoError(t, err)
	defer provider.Close()
	blockSizes := []int{10, 100, 1000, 4096, math.MaxInt32}
	for _, blockSize := range blockSizes {
		for _, indexBlockSize := range blockSizes {
			for _, numEntries := range []uint64{0, 1, 1e5} {
				c := cache.New(128 << 20)
				ch := c.NewHandle()
				r := buildTestTableWithProvider(t, provider, numEntries, blockSize, indexBlockSize, block.DefaultCompression, nil, ch)
				var pool block.BufferPool
				pool.Init(5)
				citer, err := r.NewCompactionIter(
					NoTransforms, ReadEnv{Block: block.ReadEnv{BufferPool: &pool}},
					MakeTrivialReaderProvider(r), AssertNoBlobHandles)
				require.NoError(t, err)
				switch i := citer.(type) {
				case *singleLevelIteratorRowBlocks:
					require.True(t, objstorageprovider.TestingCheckMaxReadahead(i.dataRH))
					// Each key has one version, so no value block, regardless of
					// sstable version.
					require.Nil(t, i.vbRH)
				case *twoLevelIteratorRowBlocks:
					require.True(t, objstorageprovider.TestingCheckMaxReadahead(i.secondLevel.dataRH))
					// Each key has one version, so no value block, regardless of
					// sstable version.
					require.Nil(t, i.secondLevel.vbRH)
				default:
					require.Failf(t, fmt.Sprintf("unknown compaction iterator type: %T", citer), "")
				}
				require.NoError(t, citer.Close())
				require.NoError(t, r.Close())
				ch.Close()
				c.Unref()
				pool.Release()
			}
		}
	}
}

func TestReadaheadSetupForV3TablesWithMultipleVersions(t *testing.T) {
	defer leaktest.AfterTest(t)()
	tmpDir := path.Join(t.TempDir())
	provider, err := objstorageprovider.Open(objstorageprovider.DefaultSettings(vfs.Default, tmpDir))
	require.NoError(t, err)
	defer provider.Close()
	f0, _, err := provider.Create(context.Background(), base.FileTypeTable, base.DiskFileNum(0), objstorage.CreateOptions{})
	require.NoError(t, err)

	w := NewWriter(f0, WriterOptions{
		TableFormat: TableFormatPebblev3,
		Comparer:    testkeys.Comparer,
	})
	keys := testkeys.Alpha(1)
	keyBuf := make([]byte, 1+testkeys.MaxSuffixLen)
	// Write a few keys with multiple timestamps (MVCC versions).
	for i := int64(0); i < 2; i++ {
		for j := int64(2); j >= 1; j-- {
			n := testkeys.WriteKeyAt(keyBuf[:], keys, i, j)
			key := keyBuf[:n]
			require.NoError(t, w.Set(key, key))
		}
	}
	require.NoError(t, w.Close())
	f1, err := provider.OpenForReading(context.Background(), base.FileTypeTable, base.DiskFileNum(0), objstorage.OpenOptions{})
	require.NoError(t, err)
	r, err := NewReader(context.Background(), f1, ReaderOptions{Comparer: testkeys.Comparer})
	require.NoError(t, err)
	defer r.Close()
	{
		var pool block.BufferPool
		pool.Init(5)
		defer pool.Release()
		citer, err := r.NewCompactionIter(
			NoTransforms, ReadEnv{Block: block.ReadEnv{BufferPool: &pool}},
			MakeTrivialReaderProvider(r), AssertNoBlobHandles)
		require.NoError(t, err)
		defer citer.Close()
		i := citer.(*singleLevelIteratorRowBlocks)
		require.True(t, objstorageprovider.TestingCheckMaxReadahead(i.dataRH))
		require.True(t, objstorageprovider.TestingCheckMaxReadahead(i.vbRH))
	}
	{
		iter, err := r.NewIter(NoTransforms, nil, nil, AssertNoBlobHandles)
		require.NoError(t, err)
		defer iter.Close()
		i := iter.(*singleLevelIteratorRowBlocks)
		require.False(t, objstorageprovider.TestingCheckMaxReadahead(i.dataRH))
		require.False(t, objstorageprovider.TestingCheckMaxReadahead(i.vbRH))
	}
}

type readCallType int

const (
	SeekGE readCallType = iota
	SeekPrefixGE
	Next
	Prev
	SeekLT
	First
	Last
)

func (rct readCallType) String() string {
	switch rct {
	case SeekGE:
		return "SeekGE"
	case SeekPrefixGE:
		return "SeekPrefixGE"
	case Next:
		return "Next"
	case Prev:
		return "Prev"
	case SeekLT:
		return "SeekLT"
	case First:
		return "First"
	case Last:
		return "Last"
	default:
		panic("unavailable read call type")
	}
}

func (rct readCallType) Opposite() readCallType {
	switch rct {
	case SeekGE, SeekPrefixGE:
		return SeekLT
	case Next:
		return Prev
	case Prev:
		return Next
	case SeekLT:
		return SeekGE
	case First:
		return Last
	case Last:
		return First
	default:
		panic("unavailable read call type")
	}
}

// readCall represents an Iterator call. For seek calls, the seekKey must be
// set. For Next and Prev, the repeatCount instructs the iterator to repeat the
// call.
type readCall struct {
	callType    readCallType
	seekKey     []byte
	repeatCount int
}

func (rc readCall) String() string {
	if rc.seekKey != nil {
		return fmt.Sprintf("%s(%s)", rc.callType, string(rc.seekKey))
	}
	return rc.callType.String()
}

// readerWorkload creates a random sequence of Iterator calls. If the iterator
// becomes invalid, the user can call handleInvalid to move the iterator so
// random part in the keyspace. The readerWorkload assumes the underlying file
// contains keys throughout the keyspace.
//
// TODO(msbutler): eventually merge this randomized reader helper with the utility
// in sstable/random_test.go. This will require teaching `buildRandomSSTable()` to
// build a "treatment" sst with randomized timestamp suffixes and a "control" sst
// with fixed timestamp suffixes.
type readerWorkload struct {
	ks                  testkeys.Keyspace
	t                   *testing.T
	calls               []readCall
	maxTs               int64
	seekKeyAfterInvalid []byte
	rng                 *rand.Rand
	prefix              []byte
}

// setCallAfterInvalid chooses the kind the call to return the iterator to a
// valid state. If seekAfterInvalid is empty after this call, then Next or Prev
// will be used, else SeekLT/SeekPrev will be used.
func (rw *readerWorkload) setCallAfterInvalid() {
	if rw.rng.IntN(3) == 0 {
		rw.seekKeyAfterInvalid = rw.seekKeyAfterInvalid[:0]
		rw.t.Logf("Handle Invalid: Next/Prev")
		return
	}
	idx := rw.rng.Int64N(rw.ks.Count())
	ts := rw.rng.Int64N(rw.maxTs)
	key := testkeys.KeyAt(rw.ks, idx, ts)
	rw.seekKeyAfterInvalid = append(rw.seekKeyAfterInvalid[:0], rw.prefix...)
	rw.seekKeyAfterInvalid = append(rw.seekKeyAfterInvalid, key...)

	if rw.rng.Int32N(10) == 0 {
		// Occasioinally seek without a prefix, even if there is one, which may not
		// land the iterator back in valid key space. This further checks that both
		// iterators behave the same in in invalid key space.
		rw.seekKeyAfterInvalid = append(rw.seekKeyAfterInvalid[:0], key...)
	}

	rw.t.Logf("Handle Invalid: Seek with %s", rw.seekKeyAfterInvalid)

}

func (rw *readerWorkload) handleInvalid(callType readCallType, iter Iterator) *base.InternalKV {
	switch callType {
	case SeekGE, Next, Last:
		if len(rw.seekKeyAfterInvalid) == 0 {
			return iter.Prev()
		}
		return iter.SeekLT(rw.seekKeyAfterInvalid, base.SeekLTFlagsNone)
	case SeekPrefixGE:
		if len(rw.seekKeyAfterInvalid) == 0 {
			return iter.First()
		}
		return iter.SeekLT(rw.seekKeyAfterInvalid, base.SeekLTFlagsNone)
	case SeekLT, Prev, First:
		if len(rw.seekKeyAfterInvalid) == 0 {
			return iter.Next()
		}
		return iter.SeekGE(rw.seekKeyAfterInvalid, base.SeekGEFlagsNone)
	default:
		rw.t.Fatalf("unkown call")
		return nil
	}
}

func (rw *readerWorkload) read(call readCall, iter Iterator) *base.InternalKV {
	switch call.callType {
	case SeekGE:
		return iter.SeekGE(call.seekKey, base.SeekGEFlagsNone)
	case SeekPrefixGE:
		cmp := testkeys.Comparer
		prefix := call.seekKey[:cmp.Split(call.seekKey)]
		kv := iter.SeekPrefixGE(prefix, call.seekKey, base.SeekGEFlagsNone)
		// If there is no key with this prefix to return, SeekPrefixGE might return
		// nil or it might return a larger key. Make it always return nil so we can
		// cross-check results.
		if kv != nil && !cmp.Equal(prefix, kv.K.UserKey[:cmp.Split(kv.K.UserKey)]) {
			return nil
		}
		return kv
	case Next:
		return rw.repeatRead(call, iter)
	case SeekLT:
		return iter.SeekLT(call.seekKey, base.SeekLTFlagsNone)
	case Prev:
		return rw.repeatRead(call, iter)
	case First:
		return iter.First()
	case Last:
		return iter.Last()
	default:
		rw.t.Fatalf("unkown call")
		return nil
	}
}

func (rw *readerWorkload) repeatRead(call readCall, iter Iterator) *base.InternalKV {
	var repeatCall func() *base.InternalKV

	switch call.callType {
	case Next:
		repeatCall = iter.Next
	case Prev:
		repeatCall = iter.Prev
	default:
		rw.t.Fatalf("unknown repeat read call")
	}
	for i := 0; i < call.repeatCount; i++ {
		kv := repeatCall()
		if kv == nil {
			return kv
		}
	}
	return repeatCall()
}

func createReadWorkload(
	t *testing.T, rng *rand.Rand, callCount int, ks testkeys.Keyspace, maxTS int64, prefix []byte,
) readerWorkload {
	calls := make([]readCall, 0, callCount)

	for i := 0; i < callCount; i++ {
		var seekKey []byte

		callType := readCallType(rng.IntN(int(Last + 1)))
		repeatCount := 0
		if callType == First || callType == Last {
			// Sqrt the likelihood of calling First and Last as they're not very interesting.
			callType = readCallType(rng.IntN(int(Last + 1)))
		}
		if callType == SeekLT || callType == SeekGE || callType == SeekPrefixGE {
			idx := rng.Int64N(int64(ks.MaxLen()))
			ts := rng.Int64N(maxTS) + 1
			key := testkeys.KeyAt(ks, idx, ts)
			if prefix != nil && rng.IntN(10) != 0 {
				// If there's a prefix, prepend it most of the time.
				seekKey = append(seekKey, prefix...)
			}
			seekKey = append(seekKey, key...)
		}
		if callType == Next || callType == Prev {
			repeatCount = rng.IntN(100)
		}
		calls = append(calls, readCall{callType: callType, seekKey: seekKey, repeatCount: repeatCount})
	}
	return readerWorkload{
		calls:               calls,
		t:                   t,
		ks:                  ks,
		maxTs:               maxTS,
		rng:                 rng,
		prefix:              prefix,
		seekKeyAfterInvalid: append([]byte{}, prefix...),
	}
}

// TestRandomizedPrefixSuffixRewriter runs a random sequence of iterator calls
// on an sst with keys with a fixed timestamp and asserts that all calls return
// the same sequence of keys as another iterator initialized with a suffix
// replacement rule to that fixed timestamp which reads an sst with the same
// keys and randomized suffixes. The iterator with the suffix replacement rule
// may also be initialized with a prefix synthesis rule while the control file
// will contain all keys with that prefix. In other words, this is a randomized
// version of TestBlockSyntheticSuffix and TestBlockSyntheticPrefix.
func TestRandomizedPrefixSuffixRewriter(t *testing.T) {
	defer leaktest.AfterTest(t)()
	ks := testkeys.Alpha(3)

	callCount := 500
	maxTs := int64(10)
	suffix := int64(12)
	syntheticSuffix := []byte("@" + strconv.Itoa(int(suffix)))
	var syntheticPrefix []byte

	blockSizeCandidates := []int{32, 64, 128, 256}
	restartIntervalCandidates := []int{1, 4, 8, 16}
	filterPolicyCandidates := []FilterPolicy{nil, bloom.FilterPolicy(1), bloom.FilterPolicy(10)}

	seed := uint64(time.Now().UnixNano())
	rng := rand.New(rand.NewPCG(0, seed))
	mem := vfs.NewMem()

	blockSize := blockSizeCandidates[rng.IntN(len(blockSizeCandidates))]
	restartInterval := restartIntervalCandidates[rng.IntN(len(restartIntervalCandidates))]
	filterPolicy := filterPolicyCandidates[rng.IntN(len(filterPolicyCandidates))]
	if rng.IntN(10) < 9 {
		randKey := testkeys.Key(ks, rng.Int64N(ks.Count()))
		// Choose from 3 prefix candidates: "_" sorts before all keys, randKey sorts
		// somewhere between all keys, and "~" sorts after all keys
		prefixCandidates := []string{"~", string(randKey) + "_", "_"}
		syntheticPrefix = []byte(prefixCandidates[rng.IntN(len(prefixCandidates))])
	}
	t.Logf("Configured Block Size %d, Restart Interval %d, Seed %d, Prefix %s, Filter policy %v", blockSize, restartInterval, seed, string(syntheticPrefix), filterPolicy)

	createIter := func(fileName string, syntheticSuffix SyntheticSuffix, syntheticPrefix SyntheticPrefix) (Iterator, func()) {
		f, err := mem.Open(fileName)
		require.NoError(t, err)
		opts := ReaderOptions{Comparer: testkeys.Comparer}
		if filterPolicy != nil {
			opts.Filters = map[string]FilterPolicy{filterPolicy.Name(): filterPolicy}
		}
		eReader, err := newReader(f, opts)
		require.NoError(t, err)
		iter, err := eReader.newPointIter(context.Background(), IterOptions{
			Transforms: blockiter.Transforms{
				SyntheticPrefixAndSuffix: blockiter.MakeSyntheticPrefixAndSuffix(syntheticPrefix, syntheticSuffix),
			},
			FilterBlockSizeLimit: AlwaysUseFilterBlock,
			ReaderProvider:       MakeTrivialReaderProvider(eReader),
			BlobContext:          AssertNoBlobHandles,
			Env: ReadEnv{
				Virtual: &virtual.VirtualReaderParams{
					Lower: base.MakeInternalKey([]byte("_"), base.SeqNumMax, base.InternalKeyKindSet),
					Upper: base.MakeRangeDeleteSentinelKey([]byte("~~~~~~~~~~~~~~~~"))},
			},
		})

		require.NoError(t, err)
		return iter, func() {
			require.NoError(t, iter.Close())
			require.NoError(t, eReader.Close())
		}
	}

	for _, twoLevelIndex := range []bool{false, true} {

		testCaseName := "single-level"
		if twoLevelIndex == true {
			testCaseName = "two-level"
		}
		t.Run(testCaseName, func(t *testing.T) {
			indexBlockSize := 10 * 1024 * 1024
			if twoLevelIndex {
				indexBlockSize = 1
			}

			createFile := func(randSuffix bool, prefix []byte) string {
				// initialize a new rng so that every createFile
				// call generates the same sequence of random numbers.
				localRng := rand.New(rand.NewPCG(0, seed))
				name := "randTS"
				if !randSuffix {
					name = "fixedTS"
				}
				name = name + testCaseName
				f, err := mem.Create(name, vfs.WriteCategoryUnspecified)
				require.NoError(t, err)

				w := NewWriter(objstorageprovider.NewFileWritable(f), WriterOptions{
					BlockRestartInterval: restartInterval,
					BlockSize:            blockSize,
					IndexBlockSize:       indexBlockSize,
					Comparer:             testkeys.Comparer,
					FilterPolicy:         filterPolicy,
				})

				keyIdx := int64(0)
				maxIdx := ks.Count()
				for keyIdx < maxIdx {
					// We need to call rng here even if we don't actually use the ts
					// because the sequence of rng calls must be identical for the treatment
					// and control sst.
					ts := localRng.Int64N(maxTs)
					if !randSuffix {
						ts = suffix
					}
					var keyToWrite []byte
					key := testkeys.KeyAt(ks, keyIdx, ts)
					if rng.IntN(10) == 0 && randSuffix {
						// Occasionally include keys without a suffix.
						key = testkeys.Key(ks, keyIdx)
					}
					if prefix != nil {
						keyToWrite = append(slices.Clip(prefix), key...)
					} else {
						keyToWrite = key
					}
					require.NoError(t, w.Set(keyToWrite, []byte(fmt.Sprint(keyIdx))))
					skipIdx := localRng.Int64N(5) + 1
					keyIdx += skipIdx
				}
				require.NoError(t, w.Close())
				return name
			}

			eFileName := createFile(false, syntheticPrefix)
			eIter, eCleanup := createIter(eFileName, nil, nil)
			defer eCleanup()

			fileName := createFile(true, nil)
			iter, cleanup := createIter(fileName, syntheticSuffix, syntheticPrefix)
			defer cleanup()

			w := createReadWorkload(t, rng, callCount, ks, maxTs+2, syntheticPrefix)
			workloadChecker := checker{
				t: t,
				alsoCheck: func() {
					require.Nil(t, eIter.Error())
					require.Nil(t, iter.Error())
				},
			}
			for _, call := range w.calls {
				t.Logf("%s", call)
				workloadChecker.check(w.read(call, eIter))(w.read(call, iter))
				if workloadChecker.notValid {
					w.setCallAfterInvalid()
					workloadChecker.check(w.handleInvalid(call.callType, eIter))(w.handleInvalid(call.callType, iter))
					if workloadChecker.notValid {
						// Try correcting using the opposite call if the iterator is still
						// invalid. This may occur if the Seek key exhausted the iterator.
						workloadChecker.check(w.handleInvalid(call.callType.Opposite(), eIter))(w.handleInvalid(call.callType.Opposite(), iter))
					}
					require.Equal(t, false, workloadChecker.notValid)
				}
			}
		})
	}
}

// checker is a test helper that verifies that two different iterators running
// the same sequence of operations return the same result. To use correctly, pass
// the iter call directly as an arg to check(), i.e.:
//
// c.check(expect.SeekGE([]byte("apricot@2"), base.SeekGEFlagsNone))(got.SeekGE([]byte("apricot@2"), base.SeekGEFlagsNone))
// c.check(expect.Next())(got.Next())
//
// NB: the signature to check is not simply `check(eKey,eVal,gKey,gVal)` because
// `check(expect.Next(),got.Next())` does not compile.
type checker struct {
	t         *testing.T
	notValid  bool
	alsoCheck func()
}

func (c *checker) check(eKV *base.InternalKV) func(*base.InternalKV) {
	return func(gKV *base.InternalKV) {
		c.t.Helper()
		if eKV != nil {
			require.NotNil(c.t, gKV, "expected %q", eKV.K.UserKey)
			c.t.Logf("expected %q, got %q", eKV.K.UserKey, gKV.K.UserKey)
			require.Equal(c.t, eKV, gKV)
			c.notValid = false
		} else {
			c.t.Logf("expected nil, got %v", gKV)
			require.Nil(c.t, gKV)
			c.notValid = true
		}
		c.alsoCheck()
	}
}

func TestReaderChecksumErrors(t *testing.T) {
	defer leaktest.AfterTest(t)()
	for _, checksumType := range []block.ChecksumType{block.ChecksumTypeCRC32c, block.ChecksumTypeXXHash64} {
		t.Run(fmt.Sprintf("checksum-type=%d", checksumType), func(t *testing.T) {
			for _, twoLevelIndex := range []bool{false, true} {
				t.Run(fmt.Sprintf("two-level-index=%t", twoLevelIndex), func(t *testing.T) {
					for _, corruptionType := range []string{"first-byte", "random-bit"} {
						t.Run(fmt.Sprintf("corruption-type=%s", corruptionType), func(t *testing.T) {
							mem := vfs.NewMem()

							{
								// Create an sstable with 3 data blocks.
								f, err := mem.Create("test", vfs.WriteCategoryUnspecified)
								require.NoError(t, err)

								const blockSize = 32
								indexBlockSize := 4096
								if twoLevelIndex {
									indexBlockSize = 1
								}

								w := NewWriter(objstorageprovider.NewFileWritable(f), WriterOptions{
									BlockSize:      blockSize,
									IndexBlockSize: indexBlockSize,
									Checksum:       checksumType,
								})
								require.NoError(t, w.Set(bytes.Repeat([]byte("a"), blockSize), nil))
								require.NoError(t, w.Set(bytes.Repeat([]byte("b"), blockSize), nil))
								require.NoError(t, w.Set(bytes.Repeat([]byte("c"), blockSize), nil))
								require.NoError(t, w.Close())
							}

							// Load the layout so that we know the location of the data blocks.
							var layout *Layout
							{
								f, err := mem.Open("test")
								require.NoError(t, err)

								r, err := newReader(f, ReaderOptions{})
								require.NoError(t, err)
								layout, err = r.Layout()
								require.NoError(t, err)
								require.EqualValues(t, len(layout.Data), 3)
								require.NoError(t, r.Close())
							}

							for _, bh := range layout.Data {

								// Read the sstable and corrupt the first byte or a random bit in
								// the target data block.
								orig, err := mem.Open("test")
								require.NoError(t, err)
								data, err := io.ReadAll(orig)
								require.NoError(t, err)
								require.NoError(t, orig.Close())

								if corruptionType == "first-byte" {
									data[bh.Offset] ^= 0xff
								} else {
									// Corrupt a random bit in the block.
									r := rand.New(rand.NewPCG(uint64(time.Now().UnixNano()), 0))
									randOffset := r.Uint64N(bh.Length) + bh.Offset
									randBit := uint(r.IntN(8))
									data[randOffset] ^= (1 << randBit)
								}

								corrupted, err := mem.Create("corrupted", vfs.WriteCategoryUnspecified)
								require.NoError(t, err)
								_, err = corrupted.Write(data)
								require.NoError(t, err)
								require.NoError(t, corrupted.Close())

								// Verify that we encounter a checksum mismatch error while iterating
								// over the sstable.
								corrupted, err = mem.Open("corrupted")
								require.NoError(t, err)

								r, err := newReader(corrupted, ReaderOptions{})

								if corruptionType == "first-byte" {
									require.NoError(t, err)
									iter, err := r.NewIter(NoTransforms, nil, nil, AssertNoBlobHandles)
									require.NoError(t, err)
									for kv := iter.First(); kv != nil; kv = iter.Next() {
									}
									require.Regexp(t, `checksum mismatch`, iter.Error())
									require.Regexp(t, `checksum mismatch`, iter.Close())

									iter, err = r.NewIter(NoTransforms, nil, nil, AssertNoBlobHandles)
									require.NoError(t, err)
									for kv := iter.Last(); kv != nil; kv = iter.Prev() {
									}
									require.Regexp(t, `checksum mismatch`, iter.Error())
									require.Regexp(t, `checksum mismatch`, iter.Close())

									require.NoError(t, r.Close())
								} else {
									// Check that the error message has the bit flip message if there was an error.
									checkBitFlipErr := func(err error) bool {
										if err != nil {
											details := errors.GetAllSafeDetails(err)
											re := regexp.MustCompile(`bit flip found.+byte index \d+\. got: 0x[0-9A-Fa-f]{1,2}\. want: 0x[0-9A-Fa-f]{1,2}\.`)
											for _, d := range details {
												for _, s := range d.SafeDetails {
													if re.MatchString(s) {
														return true
													}
												}
											}
											require.Fail(t, "expected at least one detail to match bit flip pattern", err)
										}
										return false
									}
									if checkBitFlipErr(err) {
										break
									}
									iter, err := r.NewIter(NoTransforms, nil, nil, AssertNoBlobHandles)
									if checkBitFlipErr(err) {
										break
									}
									for kv := iter.First(); kv != nil; kv = iter.Next() {
									}
									if checkBitFlipErr(iter.Error()) && checkBitFlipErr(iter.Close()) {
									}
									iter, err = r.NewIter(NoTransforms, nil, nil, AssertNoBlobHandles)
									if checkBitFlipErr(err) {
										break
									}
									for kv := iter.Last(); kv != nil; kv = iter.Prev() {
									}
									if checkBitFlipErr(iter.Error()) && checkBitFlipErr(iter.Close()) {
									}
									require.NoError(t, r.Close())
								}
							}
						})
					}
				})
			}
		})
	}
}

func TestValidateBlockChecksums(t *testing.T) {
	defer leaktest.AfterTest(t)()
	seed := uint64(time.Now().UnixNano())
	rng := rand.New(rand.NewPCG(0, seed))
	t.Logf("using seed = %d", seed)

	var allFiles []string
	for _, fixture := range TestFixtures {
		allFiles = append(allFiles, fixture.Filename)
	}

	type corruptionLocation int
	const (
		corruptionLocationData corruptionLocation = iota
		corruptionLocationIndex
		corruptionLocationTopIndex
		corruptionLocationFilter
		corruptionLocationRangeDel
		corruptionLocationProperties
		corruptionLocationMetaIndex
	)

	testCases := []struct {
		name                string
		files               []string
		corruptionLocations []corruptionLocation
	}{
		{
			name:                "no corruption",
			corruptionLocations: []corruptionLocation{},
		},
		{
			name: "data block corruption",
			corruptionLocations: []corruptionLocation{
				corruptionLocationData,
			},
		},
		{
			name: "index block corruption",
			corruptionLocations: []corruptionLocation{
				corruptionLocationIndex,
			},
		},
		{
			name: "top index block corruption",
			files: []string{
				"h-no-compression-two-level-index-sst/000003.sst",
			},
			corruptionLocations: []corruptionLocation{
				corruptionLocationTopIndex,
			},
		},
		{
			name: "filter block corruption",
			files: []string{
				"h-table-bloom-no-compression-prefix-extractor-no-whole-key-filter-sst/000013.sst",
				"h-table-bloom-no-compression-sst/000011.sst",
				"h-table-bloom-sst/000010.sst",
			},
			corruptionLocations: []corruptionLocation{
				corruptionLocationFilter,
			},
		},
		{
			name: "range deletion block corruption",
			corruptionLocations: []corruptionLocation{
				corruptionLocationRangeDel,
			},
		},
		{
			name: "properties block corruption",
			corruptionLocations: []corruptionLocation{
				corruptionLocationProperties,
			},
		},
		{
			name: "metaindex block corruption",
			corruptionLocations: []corruptionLocation{
				corruptionLocationMetaIndex,
			},
		},
		{
			name: "multiple blocks corrupted",
			corruptionLocations: []corruptionLocation{
				corruptionLocationData,
				corruptionLocationIndex,
				corruptionLocationRangeDel,
				corruptionLocationProperties,
				corruptionLocationMetaIndex,
			},
		},
	}

	testFn := func(t *testing.T, file string, corruptionLocations []corruptionLocation) {
		// Create a copy of the SSTable that we can freely corrupt.
		f, err := os.Open(filepath.Join("testdata", file))
		require.NoError(t, err)

		pathCopy := path.Join(t.TempDir(), path.Base(file))
		fCopy, err := vfs.Default.OpenReadWrite(pathCopy, vfs.WriteCategoryUnspecified)
		require.NoError(t, err)
		defer fCopy.Close()

		_, err = io.Copy(fCopy, f)
		require.NoError(t, err)
		err = fCopy.Sync()
		require.NoError(t, err)
		require.NoError(t, f.Close())

		filter := bloom.FilterPolicy(10)
		r, err := newReader(fCopy, ReaderOptions{
			Filters: map[string]FilterPolicy{
				filter.Name(): filter,
			},
		})
		require.NoError(t, err)
		defer func() { require.NoError(t, r.Close()) }()

		// Prior to corruption, validation is successful.
		require.NoError(t, r.ValidateBlockChecksums())

		// If we are not testing for corruption, we can stop here.
		if len(corruptionLocations) == 0 {
			return
		}

		// Perform bit flips in various corruption locations.
		layout, err := r.Layout()
		require.NoError(t, err)
		for _, location := range corruptionLocations {
			var bh block.Handle
			switch location {
			case corruptionLocationData:
				bh = layout.Data[rng.IntN(len(layout.Data))].Handle
			case corruptionLocationIndex:
				bh = layout.Index[rng.IntN(len(layout.Index))]
			case corruptionLocationTopIndex:
				bh = layout.TopIndex
			case corruptionLocationFilter:
				bh = layout.Filter[0].Handle
			case corruptionLocationRangeDel:
				bh = layout.RangeDel
			case corruptionLocationProperties:
				bh = layout.Properties
			case corruptionLocationMetaIndex:
				bh = layout.MetaIndex
			default:
				t.Fatalf("unknown location")
			}

			// Corrupt a random byte within the selected block.
			pos := int64(bh.Offset) + rng.Int64N(int64(bh.Length))
			t.Logf("altering file=%s @ offset = %d", file, pos)

			b := make([]byte, 1)
			n, err := fCopy.ReadAt(b, pos)
			require.NoError(t, err)
			require.Equal(t, 1, n)
			t.Logf("data (before) = %08b", b)

			b[0] ^= 0xff
			t.Logf("data (after) = %08b", b)

			_, err = fCopy.WriteAt(b, pos)
			require.NoError(t, err)
		}

		// Write back to the file.
		err = fCopy.Sync()
		require.NoError(t, err)

		// Confirm that checksum validation fails.
		err = r.ValidateBlockChecksums()
		require.Error(t, err)
		require.Regexp(t, `checksum mismatch`, err.Error())
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			// By default, test across all files, unless overridden.
			files := tc.files
			if files == nil {
				files = allFiles
			}
			for _, file := range files {
				t.Run(file, func(t *testing.T) {
					testFn(t, file, tc.corruptionLocations)
				})
			}
		})
	}
}

func TestReader_TableFormat(t *testing.T) {
	defer leaktest.AfterTest(t)()
	test := func(t *testing.T, want TableFormat) {
		fs := vfs.NewMem()
		f, err := fs.Create("test", vfs.WriteCategoryUnspecified)
		require.NoError(t, err)

		opts := WriterOptions{
			Comparer:    testkeys.Comparer,
			KeySchema:   &testkeysSchema,
			TableFormat: want,
		}
		w := NewRawWriter(objstorageprovider.NewFileWritable(f), opts)
		err = w.Close()
		require.NoError(t, err)

		f, err = fs.Open("test")
		require.NoError(t, err)
		r, err := newReader(f, ReaderOptions{
			Comparer:   opts.Comparer,
			KeySchemas: MakeKeySchemas(opts.KeySchema),
		})
		require.NoError(t, err)
		defer r.Close()

		got, err := r.TableFormat()
		require.NoError(t, err)
		require.Equal(t, want, got)
	}

	for tf := TableFormatLevelDB; tf <= TableFormatMax; tf++ {
		t.Run(tf.String(), func(t *testing.T) {
			test(t, tf)
		})
	}
}

func buildTestTableWithProvider(
	t *testing.T,
	provider objstorage.Provider,
	numEntries uint64,
	blockSize, indexBlockSize int,
	compression *block.CompressionProfile,
	prefix []byte,
	ch *cache.Handle,
) *Reader {
	f0, _, err := provider.Create(context.Background(), base.FileTypeTable, base.DiskFileNum(0), objstorage.CreateOptions{})
	require.NoError(t, err)

	w := NewRawWriter(f0, WriterOptions{
		BlockSize:      blockSize,
		IndexBlockSize: indexBlockSize,
		Compression:    compression,
		FilterPolicy:   nil,
	})

	var ikey InternalKey
	for i := uint64(0); i < numEntries; i++ {
		key := make([]byte, len(prefix), uint64(len(prefix))+8+i%3)
		copy(key, prefix)
		value := make([]byte, i%100)
		key = binary.BigEndian.AppendUint64(key, i)
		ikey.UserKey = key
		require.NoError(t, w.Add(ikey, value, false /* forceObsolete */))
	}

	require.NoError(t, w.Close())

	// Re-open that Filename for reading.
	f1, err := provider.OpenForReading(context.Background(), base.FileTypeTable, base.DiskFileNum(0), objstorage.OpenOptions{})
	require.NoError(t, err)

	r, err := NewReader(context.Background(), f1, ReaderOptions{
		ReaderOptions: block.ReaderOptions{
			CacheOpts: sstableinternal.CacheOptions{
				CacheHandle: ch,
			},
		},
	})
	require.NoError(t, err)
	return r
}

func buildBenchmarkTable(
	b *testing.B, options WriterOptions, confirmTwoLevelIndex bool, offset int, ch *cache.Handle,
) (*Reader, [][]byte) {
	mem := vfs.NewMem()
	f0, err := mem.Create("bench", vfs.WriteCategoryUnspecified)
	if err != nil {
		b.Fatal(err)
	}

	w := NewRawWriter(objstorageprovider.NewFileWritable(f0), options)

	var keys [][]byte
	var ikey InternalKey
	for i := uint64(0); i < 1e6; i++ {
		key := make([]byte, 8)
		binary.BigEndian.PutUint64(key, i+uint64(offset))
		keys = append(keys, key)
		ikey.UserKey = key
		require.NoError(b, w.Add(ikey, nil, false /* forceObsolete */))
	}

	if err := w.Close(); err != nil {
		b.Fatal(err)
	}

	// Re-open that Filename for reading.
	f1, err := mem.Open("bench")
	if err != nil {
		b.Fatal(err)
	}
	r, err := newReader(f1, ReaderOptions{
		ReaderOptions: block.ReaderOptions{
			CacheOpts: sstableinternal.CacheOptions{
				CacheHandle: ch,
			},
		},
	})
	if err != nil {
		b.Fatal(err)
	}
	if confirmTwoLevelIndex && !r.Attributes.Has(AttributeTwoLevelIndex) {
		b.Fatalf("should have constructed two level index")
	}
	return r, keys
}

var basicBenchmarks = []struct {
	name    string
	options WriterOptions
}{
	{
		name: "restart=16,compression=Snappy",
		options: WriterOptions{
			BlockSize:            32 << 10,
			BlockRestartInterval: 16,
			FilterPolicy:         nil,
			Compression:          block.SnappyCompression,
			TableFormat:          TableFormatPebblev2,
		},
	},
	{
		name: "restart=16,compression=ZSTD",
		options: WriterOptions{
			BlockSize:            32 << 10,
			BlockRestartInterval: 16,
			FilterPolicy:         nil,
			Compression:          block.ZstdCompression,
			TableFormat:          TableFormatPebblev2,
		},
	},
}

func BenchmarkTableIterSeekGE(b *testing.B) {
	for _, bm := range basicBenchmarks {
		b.Run(bm.name,
			func(b *testing.B) {
				c := cache.New(128 << 20)
				ch := c.NewHandle()
				r, keys := buildBenchmarkTable(b, bm.options, false, 0, ch)
				it, err := r.NewIter(NoTransforms, nil /* lower */, nil /* upper */, AssertNoBlobHandles)
				require.NoError(b, err)
				rng := rand.New(rand.NewPCG(0, uint64(time.Now().UnixNano())))

				b.ResetTimer()
				for i := 0; i < b.N; i++ {
					it.SeekGE(keys[rng.IntN(len(keys))], base.SeekGEFlagsNone)
				}

				b.StopTimer()
				it.Close()
				r.Close()
				ch.Close()
				c.Unref()
			})
	}
}

func BenchmarkTableIterSeekLT(b *testing.B) {
	for _, bm := range basicBenchmarks {
		b.Run(bm.name,
			func(b *testing.B) {
				c := cache.New(128 << 20)
				ch := c.NewHandle()
				r, keys := buildBenchmarkTable(b, bm.options, false, 0, ch)
				it, err := r.NewIter(NoTransforms, nil /* lower */, nil /* upper */, AssertNoBlobHandles)
				require.NoError(b, err)
				rng := rand.New(rand.NewPCG(0, uint64(time.Now().UnixNano())))

				b.ResetTimer()
				for i := 0; i < b.N; i++ {
					it.SeekLT(keys[rng.IntN(len(keys))], base.SeekLTFlagsNone)
				}

				b.StopTimer()
				it.Close()
				r.Close()
				ch.Close()
				c.Unref()
			})
	}
}

func BenchmarkTableIterNext(b *testing.B) {
	for _, bm := range basicBenchmarks {
		b.Run(bm.name,
			func(b *testing.B) {
				c := cache.New(128 << 20)
				ch := c.NewHandle()
				r, _ := buildBenchmarkTable(b, bm.options, false, 0, ch)
				it, err := r.NewIter(NoTransforms, nil /* lower */, nil /* upper */, AssertNoBlobHandles)
				require.NoError(b, err)

				b.ResetTimer()
				var sum int64
				var kv *base.InternalKV
				for i := 0; i < b.N; i++ {
					if kv == nil {
						kv = it.First()
					}
					sum += int64(binary.BigEndian.Uint64(kv.K.UserKey))
					kv = it.Next()
				}
				if testing.Verbose() {
					fmt.Fprint(io.Discard, sum)
				}

				b.StopTimer()
				it.Close()
				r.Close()
				ch.Close()
				c.Unref()
			})
	}
}

func BenchmarkTableIterPrev(b *testing.B) {
	for _, bm := range basicBenchmarks {
		b.Run(bm.name,
			func(b *testing.B) {
				c := cache.New(128 << 20)
				ch := c.NewHandle()
				r, _ := buildBenchmarkTable(b, bm.options, false, 0, ch)
				it, err := r.NewIter(NoTransforms, nil /* lower */, nil /* upper */, AssertNoBlobHandles)
				require.NoError(b, err)

				b.ResetTimer()
				var sum int64
				var kv *base.InternalKV
				for i := 0; i < b.N; i++ {
					if kv == nil {
						kv = it.Last()
					}
					sum += int64(binary.BigEndian.Uint64(kv.K.UserKey))
					kv = it.Prev()
				}
				if testing.Verbose() {
					fmt.Fprint(io.Discard, sum)
				}

				b.StopTimer()
				it.Close()
				r.Close()
				ch.Close()
				c.Unref()
			})
	}
}

func BenchmarkLayout(b *testing.B) {
	c := cache.New(128 << 20)
	ch := c.NewHandle()
	r, _ := buildBenchmarkTable(b, WriterOptions{}, false, 0, ch)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		r.Layout()
	}
	b.StopTimer()
	r.Close()
	ch.Close()
	c.Unref()
}

func BenchmarkSeqSeekGEExhausted(b *testing.B) {
	// Snappy with no bloom filter.
	options := basicBenchmarks[0].options

	for _, twoLevelIndex := range []bool{false, true} {
		switch twoLevelIndex {
		case false:
			options.IndexBlockSize = 0
		case true:
			options.IndexBlockSize = 512
		}
		const offsetCount = 5000
		c := cache.New(128 << 20)
		ch := c.NewHandle()
		reader, keys := buildBenchmarkTable(b, options, twoLevelIndex, offsetCount, ch)
		var preKeys [][]byte
		for i := 0; i < offsetCount; i++ {
			key := make([]byte, 8)
			binary.BigEndian.PutUint64(key, uint64(i))
			preKeys = append(preKeys, key)
		}
		var postKeys [][]byte
		for i := 0; i < offsetCount; i++ {
			key := make([]byte, 8)
			binary.BigEndian.PutUint64(key, uint64(i+offsetCount+len(keys)))
			postKeys = append(postKeys, key)
		}
		for _, exhaustedBounds := range []bool{false, true} {
			for _, prefixSeek := range []bool{false, true} {
				exhausted := "file"
				if exhaustedBounds {
					exhausted = "bounds"
				}
				seekKind := "ge"
				if prefixSeek {
					seekKind = "prefix-ge"
				}
				b.Run(fmt.Sprintf(
					"two-level=%t/exhausted=%s/seek=%s", twoLevelIndex, exhausted, seekKind),
					func(b *testing.B) {
						var upper []byte
						var seekKeys [][]byte
						if exhaustedBounds {
							seekKeys = preKeys
							upper = keys[0]
						} else {
							seekKeys = postKeys
						}
						it, err := reader.NewIter(NoTransforms, nil /* lower */, upper, AssertNoBlobHandles)
						require.NoError(b, err)
						b.ResetTimer()
						pos := 0
						var seekGEFlags base.SeekGEFlags
						for i := 0; i < b.N; i++ {
							seekKey := seekKeys[0]
							var kv *base.InternalKV
							if prefixSeek {
								kv = it.SeekPrefixGE(seekKey, seekKey, seekGEFlags)
							} else {
								kv = it.SeekGE(seekKey, seekGEFlags)
							}
							if kv != nil {
								b.Fatal("found a key")
							}
							if it.Error() != nil {
								b.Fatalf("%s", it.Error().Error())
							}
							pos++
							if pos == len(seekKeys) {
								pos = 0
								seekGEFlags = seekGEFlags.DisableTrySeekUsingNext()
							} else {
								seekGEFlags = seekGEFlags.EnableTrySeekUsingNext()
							}
						}
						b.StopTimer()
						it.Close()
					})
			}
		}
		reader.Close()
		ch.Close()
		c.Unref()
	}
}

func BenchmarkIteratorScanManyVersions(b *testing.B) {
	options := WriterOptions{
		BlockSize:            32 << 10,
		BlockRestartInterval: 16,
		FilterPolicy:         nil,
		Compression:          block.SnappyCompression,
		Comparer:             testkeys.Comparer,
	}
	// 10,000 key prefixes, each with 100 versions.
	const keyCount = 10000
	const sharedPrefixLen = 32
	const unsharedPrefixLen = 8
	const versionCount = 100

	// Take the very large keyspace consisting of alphabetic characters of
	// lengths up to unsharedPrefixLen and reduce it down to keyCount keys by
	// picking every 1 key every keyCount keys.
	keys := testkeys.Alpha(unsharedPrefixLen)
	keys = keys.EveryN(keys.Count() / keyCount)
	if keys.Count() < keyCount {
		b.Fatalf("expected %d keys, found %d", keyCount, keys.Count())
	}
	keyBuf := make([]byte, sharedPrefixLen+unsharedPrefixLen+testkeys.MaxSuffixLen)
	for i := 0; i < sharedPrefixLen; i++ {
		keyBuf[i] = 'A' + byte(i)
	}
	// v2 sstable is 115,178,070 bytes. v3 sstable is 107,181,105 bytes with
	// 99,049,269 bytes in value blocks.
	setupBench := func(b *testing.B, tableFormat TableFormat, ch *cache.Handle) *Reader {
		mem := vfs.NewMem()
		f0, err := mem.Create("bench", vfs.WriteCategoryUnspecified)
		require.NoError(b, err)
		options.TableFormat = tableFormat
		w := NewWriter(objstorageprovider.NewFileWritable(f0), options)
		val := make([]byte, 100)
		rng := rand.New(rand.NewPCG(0, 100))
		for i := int64(0); i < keys.Count(); i++ {
			for v := 0; v < versionCount; v++ {
				n := testkeys.WriteKeyAt(keyBuf[sharedPrefixLen:], keys, i, int64(versionCount-v+1))
				key := keyBuf[:n+sharedPrefixLen]
				for j := range val {
					val[j] = byte(rng.Uint32())
				}
				require.NoError(b, w.Set(key, val))
			}
		}
		require.NoError(b, w.Close())
		// Re-open the Filename for reading.
		f0, err = mem.Open("bench")
		require.NoError(b, err)
		r, err := newReader(f0, ReaderOptions{
			Comparer: testkeys.Comparer,
			ReaderOptions: block.ReaderOptions{
				CacheOpts: sstableinternal.CacheOptions{
					CacheHandle: ch,
				},
			},
		})
		require.NoError(b, err)
		return r
	}
	for _, format := range []TableFormat{TableFormatPebblev2, TableFormatPebblev3, TableFormatPebblev4} {
		b.Run(fmt.Sprintf("format=%s", format.String()), func(b *testing.B) {
			// 150MiB results in a high cache hit rate for both formats. 20MiB
			// results in a high cache hit rate for the data blocks in
			// TableFormatPebblev3.
			for _, cacheSize := range []int64{20 << 20, 150 << 20} {
				b.Run(fmt.Sprintf("cache-size=%s", humanize.Bytes.Int64(cacheSize)),
					func(b *testing.B) {
						c := cache.New(cacheSize)
						ch := c.NewHandle()
						r := setupBench(b, format, ch)
						defer func() {
							require.NoError(b, r.Close())
							ch.Close()
							c.Unref()
						}()
						b.Run("NewIter", func(b *testing.B) {
							for i := 0; i < b.N; i++ {
								iter, err := r.NewIter(NoTransforms, nil, nil, AssertNoBlobHandles)
								require.NoError(b, err)
								require.NoError(b, iter.Close())
							}
						})
						for _, readValue := range []bool{false, true} {
							b.Run(fmt.Sprintf("read-value=%t", readValue), func(b *testing.B) {
								iter, err := r.NewIter(NoTransforms, nil, nil, AssertNoBlobHandles)
								require.NoError(b, err)
								var kv *base.InternalKV
								var valBuf [100]byte
								b.ResetTimer()
								for i := 0; i < b.N; i++ {
									if kv == nil {
										kv = iter.First()
										if kv == nil {
											b.Fatalf("kv is nil")
										}
									}
									kv = iter.Next()
									if kv != nil && readValue {
										_, callerOwned, err := kv.Value(valBuf[:])
										if err != nil {
											b.Fatal(err)
										} else if callerOwned {
											b.Fatalf("unexpected callerOwned: %t", callerOwned)
										}
									}
								}
							})
						}
					})
			}
		})
	}
}

func BenchmarkIteratorScanNextPrefix(b *testing.B) {
	options := WriterOptions{
		BlockSize:            32 << 10,
		BlockRestartInterval: 16,
		FilterPolicy:         nil,
		Compression:          block.SnappyCompression,
		TableFormat:          TableFormatPebblev3,
		Comparer:             testkeys.Comparer,
	}
	const keyCount = 10000
	const sharedPrefixLen = 32
	const unsharedPrefixLen = 8
	val := make([]byte, 100)
	rng := rand.New(rand.NewPCG(0, 100))
	for j := range val {
		val[j] = byte(rng.Uint32())
	}

	// Take the very large keyspace consisting of alphabetic characters of
	// lengths up to unsharedPrefixLen and reduce it down to keyCount keys by
	// picking every 1 key every keyCount keys.
	keys := testkeys.Alpha(unsharedPrefixLen)
	keys = keys.EveryN(keys.Count() / keyCount)
	if keys.Count() < keyCount {
		b.Fatalf("expected %d keys, found %d", keyCount, keys.Count())
	}
	keyBuf := make([]byte, sharedPrefixLen+unsharedPrefixLen+testkeys.MaxSuffixLen)
	for i := 0; i < sharedPrefixLen; i++ {
		keyBuf[i] = 'A' + byte(i)
	}
	setupBench := func(b *testing.B, ch *cache.Handle, versCount int) (r *Reader, succKeys [][]byte) {
		mem := vfs.NewMem()
		f0, err := mem.Create("bench", vfs.WriteCategoryUnspecified)
		require.NoError(b, err)
		w := NewWriter(objstorageprovider.NewFileWritable(f0), options)
		for i := int64(0); i < keys.Count(); i++ {
			for v := 0; v < versCount; v++ {
				n := testkeys.WriteKeyAt(keyBuf[sharedPrefixLen:], keys, i, int64(versCount-v+1))
				key := keyBuf[:n+sharedPrefixLen]
				require.NoError(b, w.Set(key, val))
				if v == 0 {
					prefixLen := testkeys.Comparer.Split(key)
					prefixKey := key[:prefixLen]
					succKey := testkeys.Comparer.ImmediateSuccessor(nil, prefixKey)
					succKeys = append(succKeys, succKey)
				}
			}
		}
		require.NoError(b, w.Close())
		// Re-open the Filename for reading.
		f0, err = mem.Open("bench")
		require.NoError(b, err)
		r, err = newReader(f0, ReaderOptions{
			Comparer: testkeys.Comparer,
			ReaderOptions: block.ReaderOptions{
				CacheOpts: sstableinternal.CacheOptions{
					CacheHandle: ch,
				},
			},
		})
		require.NoError(b, err)
		return r, succKeys
	}
	// Analysis of some sample results with TableFormatPebblev2:
	// versions=1/method=seek-ge-10         	22107622	        53.57 ns/op
	// versions=1/method=next-prefix-10     	36292837	        33.07 ns/op
	// versions=2/method=seek-ge-10         	14429138	        82.92 ns/op
	// versions=2/method=next-prefix-10     	19676055	        60.78 ns/op
	// versions=10/method=seek-ge-10        	 1453726	       825.2 ns/op
	// versions=10/method=next-prefix-10    	 2450498	       489.6 ns/op
	// versions=100/method=seek-ge-10       	  965143	      1257 ns/op
	// versions=100/method=next-prefix-10   	 1000000	      1054 ns/op
	//
	// With 1 version, both SeekGE and NextPrefix will be able to complete after
	// doing a single call to blockIter.Next. However, SeekGE has to do two key
	// comparisons unlike the one key comparison in NextPrefix. This is because
	// SeekGE also compares *before* calling Next since it is possible that the
	// preceding SeekGE is already at the right place.
	//
	// With 2 versions, both will do two calls to blockIter.Next. The difference
	// in the cost is the same as in the 1 version case.
	//
	// With 10 versions, it is still likely that the desired key is in the same
	// data block. NextPrefix will seek only the blockIter. And in the rare case
	// that the key is in the next data block, it will step the index block (not
	// seek). In comparison, SeekGE will seek the index block too.
	//
	// With 100 versions we more often cross from one data block to the next, so
	// the difference in cost declines.
	//
	// Some sample results with TableFormatPebblev3:

	// versions=1/method=seek-ge-10         	18702609	        53.90 ns/op
	// versions=1/method=next-prefix-10     	77440167	        15.41 ns/op
	// versions=2/method=seek-ge-10         	13554286	        87.91 ns/op
	// versions=2/method=next-prefix-10     	62148526	        19.25 ns/op
	// versions=10/method=seek-ge-10        	 1316676	       910.5 ns/op
	// versions=10/method=next-prefix-10    	18829448	        62.61 ns/op
	// versions=100/method=seek-ge-10       	 1166139	      1025 ns/op
	// versions=100/method=next-prefix-10   	 4443386	       265.3 ns/op
	//
	// NextPrefix is much cheaper than in TableFormatPebblev2 with larger number
	// of versions. It is also cheaper with 1 and 2 versions since
	// setHasSamePrefix=false eliminates a key comparison.
	for _, versionCount := range []int{1, 2, 10, 100} {
		b.Run(fmt.Sprintf("versions=%d", versionCount), func(b *testing.B) {
			// NB: This 200MiB cache is sufficient for even the largest file: 10,000
			// keys * 100 versions = 1M keys, where each key-value pair is ~140 bytes
			// = 140MB. So we are not measuring the caching benefit of
			// TableFormatPebblev3 storing older values in value blocks.
			c := cache.New(200 << 20)
			ch := c.NewHandle()
			r, succKeys := setupBench(b, ch, versionCount)
			defer func() {
				require.NoError(b, r.Close())
				ch.Close()
				c.Unref()
			}()
			for _, method := range []string{"seek-ge", "next-prefix"} {
				b.Run(fmt.Sprintf("method=%s", method), func(b *testing.B) {
					for _, readValue := range []bool{false, true} {
						b.Run(fmt.Sprintf("read-value=%t", readValue), func(b *testing.B) {
							iter, err := r.NewIter(NoTransforms, nil, nil, AssertNoBlobHandles)
							require.NoError(b, err)
							var nextFunc func(index int) *base.InternalKV
							switch method {
							case "seek-ge":
								nextFunc = func(index int) *base.InternalKV {
									var flags base.SeekGEFlags
									return iter.SeekGE(succKeys[index], flags.EnableTrySeekUsingNext())
								}
							case "next-prefix":
								nextFunc = func(index int) *base.InternalKV {
									return iter.NextPrefix(succKeys[index])
								}
							default:
								b.Fatalf("unknown method %s", method)
							}
							n := keys.Count()
							j := n
							var kv *base.InternalKV
							var valBuf [100]byte
							b.ResetTimer()
							for i := 0; i < b.N; i++ {
								if kv == nil {
									if j != n {
										b.Fatalf("unexpected %d != %d", j, n)
									}
									kv = iter.First()
									j = 0
								} else {
									kv = nextFunc(int(j - 1))
									if kv != nil && readValue {
										_, callerOwned, err := kv.Value(valBuf[:])
										if err != nil {
											b.Fatal(err)
										} else if callerOwned {
											b.Fatalf("unexpected callerOwned: %t", callerOwned)
										}
									}

								}
								if kv != nil {
									j++
								}
							}
						})
					}
				})
			}
		})
	}
}

func BenchmarkIteratorScanObsolete(b *testing.B) {
	options := WriterOptions{
		BlockSize:            32 << 10,
		BlockRestartInterval: 16,
		FilterPolicy:         nil,
		Compression:          block.SnappyCompression,
		Comparer:             testkeys.Comparer,
	}
	const keyCount = 1 << 20
	const keyLen = 10

	// Take the very large keyspace consisting of alphabetic characters of
	// lengths up to unsharedPrefixLen and reduce it down to keyCount keys by
	// picking every 1 key every keyCount keys.
	keys := testkeys.Alpha(keyLen)
	keys = keys.EveryN(keys.Count() / keyCount)
	if keys.Count() < keyCount {
		b.Fatalf("expected %d keys, found %d", keyCount, keys.Count())
	}
	expectedKeyCount := keys.Count()
	keyBuf := make([]byte, keyLen)
	setupBench := func(b *testing.B, tableFormat TableFormat, ch *cache.Handle) *Reader {
		mem := vfs.NewMem()
		f0, err := mem.Create("bench", vfs.WriteCategoryUnspecified)
		require.NoError(b, err)
		options.TableFormat = tableFormat
		w := NewRawWriter(objstorageprovider.NewFileWritable(f0), options)
		val := make([]byte, 100)
		rng := rand.New(rand.NewPCG(0, 100))
		for i := int64(0); i < keys.Count(); i++ {
			n := testkeys.WriteKey(keyBuf, keys, i)
			key := keyBuf[:n]
			for j := range val {
				val[j] = byte(rng.Uint32())
			}
			forceObsolete := true
			if i == 0 {
				forceObsolete = false
			}
			require.NoError(b, w.Add(
				base.MakeInternalKey(key, 0, InternalKeyKindSet), val, forceObsolete))
		}
		require.NoError(b, w.Close())
		// Re-open the Filename for reading.
		f0, err = mem.Open("bench")
		require.NoError(b, err)
		r, err := newReader(f0, ReaderOptions{
			Comparer: testkeys.Comparer,
			ReaderOptions: block.ReaderOptions{
				CacheOpts: sstableinternal.CacheOptions{
					CacheHandle: ch,
				},
			},
		})
		require.NoError(b, err)
		return r
	}
	for _, format := range []TableFormat{TableFormatPebblev3, TableFormatPebblev4} {
		b.Run(fmt.Sprintf("format=%s", format.String()), func(b *testing.B) {
			// 150MiB results in a high cache hit rate for both formats.
			for _, cacheSize := range []int64{1, 150 << 20} {
				b.Run(fmt.Sprintf("cache-size=%s", humanize.Bytes.Int64(cacheSize)),
					func(b *testing.B) {
						c := cache.New(cacheSize)
						ch := c.NewHandle()
						r := setupBench(b, format, ch)
						defer func() {
							require.NoError(b, r.Close())
							ch.Close()
							c.Unref()
						}()
						for _, hideObsoletePoints := range []bool{false, true} {
							b.Run(fmt.Sprintf("hide-obsolete=%t", hideObsoletePoints), func(b *testing.B) {
								var filterer *BlockPropertiesFilterer
								if format == TableFormatPebblev4 && hideObsoletePoints {
									filterer = newBlockPropertiesFilterer(
										[]BlockPropertyFilter{obsoleteKeyBlockPropertyFilter{}}, nil, nil)
									intersects, err :=
										filterer.intersectsUserPropsAndFinishInit(r.UserProperties)
									if err != nil {
										b.Fatalf("%s", err.Error())
									}
									if !intersects {
										b.Fatalf("sstable does not intersect")
									}
								}
								iter, err := r.NewPointIter(context.Background(), IterOptions{
									Transforms:     IterTransforms{HideObsoletePoints: hideObsoletePoints},
									Filterer:       filterer,
									ReaderProvider: MakeTrivialReaderProvider(r),
									BlobContext:    AssertNoBlobHandles,
									Env:            NoReadEnv,
								})
								require.NoError(b, err)
								b.ResetTimer()
								for i := 0; i < b.N; i++ {
									count := int64(0)
									kv := iter.First()
									for kv != nil {
										count++
										kv = iter.Next()
									}
									if format == TableFormatPebblev4 && hideObsoletePoints {
										if count != 1 {
											b.Fatalf("found %d points", count)
										}
									} else {
										if count != expectedKeyCount {
											b.Fatalf("found %d points", count)
										}
									}
								}
							})
						}
					})
			}
		})
	}
}

func newReader(r ReadableFile, o ReaderOptions) (*Reader, error) {
	readable, err := NewSimpleReadable(r)
	if err != nil {
		return nil, err
	}
	reader, err := NewReader(context.Background(), readable, o)
	if err != nil {
		return nil, errors.CombineErrors(err, readable.Close())
	}
	return reader, nil
}

// TestReaderReportsCorruption tests that the reader reports corruption when
// an external file goes missing after obtaining a remote.ObjectReader for it.
func TestReaderReportsCorruption(t *testing.T) {
	defer leaktest.AfterTest(t)()

	ctx := context.Background()
	remoteStorage := remote.NewInMem()
	settings := objstorageprovider.DefaultSettings(vfs.NewMem(), "")
	settings.Remote.StorageFactory = remote.MakeSimpleFactory(map[remote.Locator]remote.Storage{
		"locator": remoteStorage,
	})
	settings.Remote.CreateOnSharedLocator = "locator"
	settings.Remote.CreateOnShared = remote.CreateOnSharedAll
	provider, err := objstorageprovider.Open(settings)
	require.NoError(t, err)
	defer provider.Close()
	require.NoError(t, provider.SetCreatorID(1))

	writable, objMeta, err := provider.Create(ctx, base.FileTypeTable, 1, objstorage.CreateOptions{
		PreferSharedStorage: true,
	})
	require.NoError(t, err)
	require.NotNil(t, objMeta.Remote.Storage)

	// Create an sst file with multiple data blocks.
	w := NewWriter(writable, WriterOptions{BlockSize: 1})
	for i := range 100 {
		require.NoError(t, w.Set([]byte(fmt.Sprintf("%04d", i)), []byte(fmt.Sprintf("value%04d", i))))
	}
	require.NoError(t, w.Close())

	readable, err := provider.OpenForReading(ctx, base.FileTypeTable, 1, objstorage.OpenOptions{})
	require.NoError(t, err)
	r, err := NewReader(context.Background(), readable, ReaderOptions{})
	require.NoError(t, err)
	defer r.Close()

	var lastReportedCorruption error
	env := ReadEnv{Block: block.ReadEnv{
		ReportCorruptionFn: func(info base.ObjectInfo, err error) error {
			t.Logf("ReportCorruptionFn: %v", err)
			lastReportedCorruption = err
			return errors.Wrap(err, "error passed through ReportCorruptionFn")
		},
	}}
	iter, err := r.NewPointIter(context.Background(), IterOptions{
		Transforms: NoTransforms,
		Env:        env,
	})
	require.NoError(t, err)
	defer iter.Close()
	kv := iter.First()
	require.NotNil(t, kv)

	// Delete all objects from the store and expect to get a corruption error.
	objects, err := remoteStorage.List("", "")
	require.NoError(t, err)
	for _, o := range objects {
		require.NoError(t, remoteStorage.Delete(o))
	}
	for ; kv != nil; kv = iter.Next() {
	}
	iterErr := iter.Error()
	require.ErrorContains(t, iterErr, "error passed through ReportCorruptionFn: in-mem remote storage object does not exist")
	require.True(t, base.IsCorruptionError(iterErr))
	require.ErrorContains(t, lastReportedCorruption, "in-mem remote storage object does not exist")
	require.True(t, base.IsCorruptionError(lastReportedCorruption))
}
