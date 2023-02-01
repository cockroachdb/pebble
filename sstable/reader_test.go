// Copyright 2018 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package sstable

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"io"
	"math"
	"os"
	"path"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/cockroachdb/datadriven"
	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/pebble/bloom"
	"github.com/cockroachdb/pebble/internal/base"
	"github.com/cockroachdb/pebble/internal/cache"
	"github.com/cockroachdb/pebble/internal/errorfs"
	"github.com/cockroachdb/pebble/internal/humanize"
	"github.com/cockroachdb/pebble/internal/testkeys"
	"github.com/cockroachdb/pebble/objstorage"
	"github.com/cockroachdb/pebble/vfs"
	"github.com/stretchr/testify/require"
	"golang.org/x/exp/rand"
)

// get is a testing helper that simulates a read and helps verify bloom filters
// until they are available through iterators.
func (r *Reader) get(key []byte) (value []byte, err error) {
	if r.err != nil {
		return nil, r.err
	}

	if r.tableFilter != nil {
		dataH, err := r.readFilter(nil /* stats */)
		if err != nil {
			return nil, err
		}
		var lookupKey []byte
		if r.Split != nil {
			lookupKey = key[:r.Split(key)]
		} else {
			lookupKey = key
		}
		mayContain := r.tableFilter.mayContain(dataH.Get(), lookupKey)
		dataH.Release()
		if !mayContain {
			return nil, base.ErrNotFound
		}
	}

	i, err := r.NewIter(nil /* lower */, nil /* upper */)
	if err != nil {
		return nil, err
	}
	var v base.LazyValue
	ikey, v := i.SeekGE(key, base.SeekGEFlagsNone)
	value, _, err = v.Value(nil)
	if err != nil {
		return nil, err
	}

	if ikey == nil || r.Compare(key, ikey.UserKey) != 0 {
		err := i.Close()
		if err == nil {
			err = base.ErrNotFound
		}
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

// iterAdapter adapts the new Iterator API which returns the key and value from
// positioning methods (Seek*, First, Last, Next, Prev) to the old API which
// returned a boolean corresponding to Valid. Only used by test code.
type iterAdapter struct {
	Iterator
	key *InternalKey
	val []byte
}

func newIterAdapter(iter Iterator) *iterAdapter {
	return &iterAdapter{
		Iterator: iter,
	}
}

func (i *iterAdapter) update(key *InternalKey, val base.LazyValue) bool {
	i.key = key
	if v, _, err := val.Value(nil); err != nil {
		i.key = nil
		i.val = nil
	} else {
		i.val = v
	}
	return i.key != nil
}

func (i *iterAdapter) String() string {
	return "iter-adapter"
}

func (i *iterAdapter) SeekGE(key []byte, flags base.SeekGEFlags) bool {
	return i.update(i.Iterator.SeekGE(key, flags))
}

func (i *iterAdapter) SeekPrefixGE(prefix, key []byte, flags base.SeekGEFlags) bool {
	return i.update(i.Iterator.SeekPrefixGE(prefix, key, flags))
}

func (i *iterAdapter) SeekLT(key []byte, flags base.SeekLTFlags) bool {
	return i.update(i.Iterator.SeekLT(key, flags))
}

func (i *iterAdapter) First() bool {
	return i.update(i.Iterator.First())
}

func (i *iterAdapter) Last() bool {
	return i.update(i.Iterator.Last())
}

func (i *iterAdapter) Next() bool {
	return i.update(i.Iterator.Next())
}

func (i *iterAdapter) NextPrefix(succKey []byte) bool {
	return i.update(i.Iterator.NextPrefix(succKey))
}

func (i *iterAdapter) NextIgnoreResult() {
	i.Iterator.Next()
	i.update(nil, base.LazyValue{})
}

func (i *iterAdapter) Prev() bool {
	return i.update(i.Iterator.Prev())
}

func (i *iterAdapter) Key() *InternalKey {
	return i.key
}

func (i *iterAdapter) Value() []byte {
	return i.val
}

func (i *iterAdapter) Valid() bool {
	return i.key != nil
}

func (i *iterAdapter) SetBounds(lower, upper []byte) {
	i.Iterator.SetBounds(lower, upper)
	i.key = nil
}

func TestReader(t *testing.T) {
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

	for _, format := range []TableFormat{TableFormatPebblev2, TableFormatPebblev3} {
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
									t, tableOpt, testDirs[oName], nil /* Reader */, 0, false, true)
							})
					}
				}
			}
		}
	}
}

func TestHamletReader(t *testing.T) {
	prebuiltSSTs := []string{
		"testdata/h.ldb",
		"testdata/h.sst",
		"testdata/h.no-compression.sst",
		"testdata/h.no-compression.two_level_index.sst",
		"testdata/h.block-bloom.no-compression.sst",
		"testdata/h.table-bloom.no-compression.prefix_extractor.no_whole_key_filter.sst",
		"testdata/h.table-bloom.no-compression.sst",
	}

	for _, prebuiltSST := range prebuiltSSTs {
		f, err := os.Open(filepath.FromSlash(prebuiltSST))
		require.NoError(t, err)

		r, err := newReader(f, ReaderOptions{})
		require.NoError(t, err)

		t.Run(
			fmt.Sprintf("sst=%s", prebuiltSST),
			func(t *testing.T) {
				runTestReader(t, WriterOptions{}, "testdata/hamletreader", r, 0, false, false)
			},
		)
	}
}

func TestReaderStats(t *testing.T) {
	tableOpt := WriterOptions{
		BlockSize:      30,
		IndexBlockSize: 30,
	}
	runTestReader(t, tableOpt, "testdata/readerstats", nil, 10000, false, false)
}

func TestReaderStatsV3(t *testing.T) {
	writerOpt := WriterOptions{
		BlockSize:      32 << 10,
		IndexBlockSize: 32 << 10,
		Comparer:       testkeys.Comparer,
		TableFormat:    TableFormatPebblev3,
	}
	tdFile := "testdata/readerstats_v3"
	runTestReader(t, writerOpt, tdFile, nil /* Reader */, 0, true, false)
}

func TestReaderWithBlockPropertyFilter(t *testing.T) {
	for _, format := range []TableFormat{TableFormatPebblev2, TableFormatPebblev3} {
		writerOpt := WriterOptions{
			BlockSize:               1,
			IndexBlockSize:          40,
			Comparer:                testkeys.Comparer,
			TableFormat:             format,
			BlockPropertyCollectors: []func() BlockPropertyCollector{NewTestKeysBlockPropertyCollector},
		}
		tdFile := "testdata/reader_bpf"
		if format == TableFormatPebblev3 {
			tdFile = "testdata/reader_bpf_v3"
		}
		runTestReader(t, writerOpt, tdFile, nil /* Reader */, 0, true, false)
	}
}

func TestInjectedErrors(t *testing.T) {
	prebuiltSSTs := []string{
		"testdata/h.ldb",
		"testdata/h.sst",
		"testdata/h.no-compression.sst",
		"testdata/h.no-compression.two_level_index.sst",
		"testdata/h.block-bloom.no-compression.sst",
		"testdata/h.table-bloom.no-compression.prefix_extractor.no_whole_key_filter.sst",
		"testdata/h.table-bloom.no-compression.sst",
	}

	for _, prebuiltSST := range prebuiltSSTs {
		run := func(i int) (reterr error) {
			f, err := vfs.Default.Open(filepath.FromSlash(prebuiltSST))
			require.NoError(t, err)

			r, err := newReader(errorfs.WrapFile(f, errorfs.OnIndex(int32(i))), ReaderOptions{})
			if err != nil {
				return firstError(err, f.Close())
			}
			defer func() { reterr = firstError(reterr, r.Close()) }()

			_, err = r.EstimateDiskUsage([]byte("borrower"), []byte("lender"))
			if err != nil {
				return err
			}

			iter, err := r.NewIter(nil, nil)
			if err != nil {
				return err
			}
			defer func() { reterr = firstError(reterr, iter.Close()) }()
			for k, v := iter.First(); k != nil; k, v = iter.Next() {
				val, _, err := v.Value(nil)
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
				t.Logf("%q, index %d: %s", prebuiltSST, i, err)
				continue
			}
			if err != nil {
				t.Errorf("%q, index %d: non-injected error: %+v", prebuiltSST, i, err)
				break
			}
			t.Logf("%q: no error at index %d", prebuiltSST, i)
			break
		}
	}
}

func TestInvalidReader(t *testing.T) {
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
		r, err := NewReader(tc.readable, ReaderOptions{})
		if !strings.Contains(err.Error(), tc.expected) {
			t.Fatalf("expected %q, but found %q", tc.expected, err.Error())
		}
		if r != nil {
			t.Fatalf("found non-nil reader returned with non-nil error %q", err.Error())
		}
	}
}

func indexLayoutString(t *testing.T, r *Reader) string {
	indexH, err := r.readIndex(nil /* stats */)
	require.NoError(t, err)
	defer indexH.Release()
	var buf strings.Builder
	twoLevelIndex := r.Properties.IndexType == twoLevelIndex
	buf.WriteString("index entries:\n")
	iter, err := newBlockIter(r.Compare, indexH.Get())
	defer func() {
		require.NoError(t, iter.Close())
	}()
	require.NoError(t, err)
	for key, value := iter.First(); key != nil; key, value = iter.Next() {
		bh, err := decodeBlockHandleWithProperties(value.InPlaceValue())
		require.NoError(t, err)
		fmt.Fprintf(&buf, " %s: size %d\n", string(key.UserKey), bh.Length)
		if twoLevelIndex {
			b, err := r.readBlock(bh.BlockHandle, nil, nil, nil)
			require.NoError(t, err)
			defer b.Release()
			iter2, err := newBlockIter(r.Compare, b.Get())
			defer func() {
				require.NoError(t, iter2.Close())
			}()
			require.NoError(t, err)
			for key, value := iter2.First(); key != nil; key, value = iter2.Next() {
				bh, err := decodeBlockHandleWithProperties(value.InPlaceValue())
				require.NoError(t, err)
				fmt.Fprintf(&buf, "   %s: size %d\n", string(key.UserKey), bh.Length)
			}
		}
	}
	return buf.String()
}

func runTestReader(
	t *testing.T,
	o WriterOptions,
	dir string,
	r *Reader,
	cacheSize int,
	printLayout bool,
	printValue bool,
) {
	datadriven.Walk(t, dir, func(t *testing.T, path string) {
		defer func() {
			if r != nil {
				r.Close()
				r = nil
			}
		}()

		datadriven.RunTest(t, path, func(t *testing.T, d *datadriven.TestData) string {
			switch d.Cmd {
			case "build":
				if r != nil {
					r.Close()
					r = nil
				}
				var err error
				_, r, err = runBuildCmd(d, &o, cacheSize)
				if err != nil {
					return err.Error()
				}
				if printLayout {
					return indexLayoutString(t, r)
				}
				return ""

			case "iter":
				seqNum, err := scanGlobalSeqNum(d)
				if err != nil {
					return err.Error()
				}
				var stats base.InternalIteratorStats
				r.Properties.GlobalSeqNum = seqNum
				var filterer *BlockPropertiesFilterer
				if d.HasArg("block-property-filter") {
					var filterMin, filterMax uint64
					d.ScanArgs(t, "block-property-filter", &filterMin, &filterMax)
					bpf := NewTestKeysBlockPropertyFilter(filterMin, filterMax)
					filterer = NewBlockPropertiesFilterer([]BlockPropertyFilter{bpf}, nil)
					intersects, err :=
						filterer.IntersectsUserPropsAndFinishInit(r.Properties.UserProperties)
					if err != nil {
						return err.Error()
					}
					if !intersects {
						return "table does not intersect BlockPropertyFilter"
					}
				}
				iter, err := r.NewIterWithBlockPropertyFilters(
					nil, /* lower */
					nil, /* upper */
					filterer,
					true, /* use filter block */
					&stats,
					TrivialReaderProvider{Reader: r},
				)
				if err != nil {
					return err.Error()
				}
				return runIterCmd(d, iter, printValue, runIterCmdStats(&stats))

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
	f0, err := mem.Create(testTable)
	require.NoError(t, err)

	w := NewWriter(f0, writerOpts)
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

			r, err := newReader(f1, ReaderOptions{}, comparers, mergers)
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

func testBytesIteratedWithCompression(
	t *testing.T,
	compression Compression,
	allowedSizeDeviationPercent uint64,
	blockSizes []int,
	maxNumEntries []uint64,
) {
	for i, blockSize := range blockSizes {
		for _, indexBlockSize := range blockSizes {
			for _, numEntries := range []uint64{0, 1, maxNumEntries[i]} {
				r := buildTestTable(t, numEntries, blockSize, indexBlockSize, compression)
				var bytesIterated, prevIterated uint64
				citer, err := r.NewCompactionIter(&bytesIterated, TrivialReaderProvider{Reader: r})
				require.NoError(t, err)

				for key, _ := citer.First(); key != nil; key, _ = citer.Next() {
					if bytesIterated < prevIterated {
						t.Fatalf("bytesIterated moved backward: %d < %d", bytesIterated, prevIterated)
					}
					prevIterated = bytesIterated
				}

				expected := r.Properties.DataSize
				allowedSizeDeviation := expected * allowedSizeDeviationPercent / 100
				// There is some inaccuracy due to compression estimation.
				if bytesIterated < expected-allowedSizeDeviation || bytesIterated > expected+allowedSizeDeviation {
					t.Fatalf("bytesIterated: got %d, want %d", bytesIterated, expected)
				}

				require.NoError(t, citer.Close())
				require.NoError(t, r.Close())
			}
		}
	}
}

func TestBytesIterated(t *testing.T) {
	blockSizes := []int{10, 100, 1000, 4096, math.MaxInt32}
	t.Run("Compressed", func(t *testing.T) {
		testBytesIteratedWithCompression(t, SnappyCompression, 1, blockSizes, []uint64{1e5, 1e5, 1e5, 1e5, 1e5})
	})
	t.Run("Uncompressed", func(t *testing.T) {
		testBytesIteratedWithCompression(t, NoCompression, 0, blockSizes, []uint64{1e5, 1e5, 1e5, 1e5, 1e5})
	})
	t.Run("Zstd", func(t *testing.T) {
		// compression with zstd is extremely slow with small block size (esp the nocgo version).
		// use less numEntries to make the test run at reasonable speed (under 10 seconds).
		maxNumEntries := []uint64{1e2, 1e2, 1e3, 4e3, 1e5}
		if useStandardZstdLib {
			maxNumEntries = []uint64{1e3, 1e3, 1e4, 4e4, 1e5}
		}
		testBytesIteratedWithCompression(t, ZstdCompression, 1, blockSizes, maxNumEntries)
	})
}

func TestCompactionIteratorSetupForCompaction(t *testing.T) {
	tmpDir := path.Join(t.TempDir())
	provider := objstorage.New(objstorage.DefaultSettings(vfs.Default, tmpDir))
	blockSizes := []int{10, 100, 1000, 4096, math.MaxInt32}
	for _, blockSize := range blockSizes {
		for _, indexBlockSize := range blockSizes {
			for _, numEntries := range []uint64{0, 1, 1e5} {
				r := buildTestTableWithProvider(t, provider, numEntries, blockSize, indexBlockSize, DefaultCompression)
				var bytesIterated uint64
				citer, err := r.NewCompactionIter(&bytesIterated, TrivialReaderProvider{Reader: r})
				require.NoError(t, err)
				switch i := citer.(type) {
				case *compactionIterator:
					require.True(t, objstorage.TestingCheckMaxReadahead(i.dataRH))
				case *twoLevelCompactionIterator:
					require.True(t, objstorage.TestingCheckMaxReadahead(i.dataRH))
				default:
					require.Failf(t, fmt.Sprintf("unknown compaction iterator type: %T", citer), "")
				}
				require.NoError(t, citer.Close())
				require.NoError(t, r.Close())
			}
		}
	}
}

func TestReaderChecksumErrors(t *testing.T) {
	for _, checksumType := range []ChecksumType{ChecksumTypeCRC32c, ChecksumTypeXXHash64} {
		t.Run(fmt.Sprintf("checksum-type=%d", checksumType), func(t *testing.T) {
			for _, twoLevelIndex := range []bool{false, true} {
				t.Run(fmt.Sprintf("two-level-index=%t", twoLevelIndex), func(t *testing.T) {
					mem := vfs.NewMem()

					{
						// Create an sstable with 3 data blocks.
						f, err := mem.Create("test")
						require.NoError(t, err)

						const blockSize = 32
						indexBlockSize := 4096
						if twoLevelIndex {
							indexBlockSize = 1
						}

						w := NewWriter(f, WriterOptions{
							BlockSize:      blockSize,
							IndexBlockSize: indexBlockSize,
							Checksum:       checksumType,
						})
						require.NoError(t, w.Set(bytes.Repeat([]byte("a"), blockSize), nil))
						require.NoError(t, w.Set(bytes.Repeat([]byte("b"), blockSize), nil))
						require.NoError(t, w.Set(bytes.Repeat([]byte("c"), blockSize), nil))
						require.NoError(t, w.Close())
					}

					// Load the layout so that we no the location of the data blocks.
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
						// Read the sstable and corrupt the first byte in the target data
						// block.
						orig, err := mem.Open("test")
						require.NoError(t, err)
						data, err := io.ReadAll(orig)
						require.NoError(t, err)
						require.NoError(t, orig.Close())

						// Corrupt the first byte in the block.
						data[bh.Offset] ^= 0xff

						corrupted, err := mem.Create("corrupted")
						require.NoError(t, err)
						_, err = corrupted.Write(data)
						require.NoError(t, err)
						require.NoError(t, corrupted.Close())

						// Verify that we encounter a checksum mismatch error while iterating
						// over the sstable.
						corrupted, err = mem.Open("corrupted")
						require.NoError(t, err)

						r, err := newReader(corrupted, ReaderOptions{})
						require.NoError(t, err)

						iter, err := r.NewIter(nil, nil)
						require.NoError(t, err)
						for k, _ := iter.First(); k != nil; k, _ = iter.Next() {
						}
						require.Regexp(t, `checksum mismatch`, iter.Error())
						require.Regexp(t, `checksum mismatch`, iter.Close())

						iter, err = r.NewIter(nil, nil)
						require.NoError(t, err)
						for k, _ := iter.Last(); k != nil; k, _ = iter.Prev() {
						}
						require.Regexp(t, `checksum mismatch`, iter.Error())
						require.Regexp(t, `checksum mismatch`, iter.Close())

						require.NoError(t, r.Close())
					}
				})
			}
		})
	}
}

func TestValidateBlockChecksums(t *testing.T) {
	seed := uint64(time.Now().UnixNano())
	rng := rand.New(rand.NewSource(seed))
	t.Logf("using seed = %d", seed)

	allFiles := []string{
		"testdata/h.no-compression.sst",
		"testdata/h.no-compression.two_level_index.sst",
		"testdata/h.sst",
		"testdata/h.table-bloom.no-compression.prefix_extractor.no_whole_key_filter.sst",
		"testdata/h.table-bloom.no-compression.sst",
		"testdata/h.table-bloom.sst",
		"testdata/h.zstd-compression.sst",
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
				"testdata/h.no-compression.two_level_index.sst",
			},
			corruptionLocations: []corruptionLocation{
				corruptionLocationTopIndex,
			},
		},
		{
			name: "filter block corruption",
			files: []string{
				"testdata/h.table-bloom.no-compression.prefix_extractor.no_whole_key_filter.sst",
				"testdata/h.table-bloom.no-compression.sst",
				"testdata/h.table-bloom.sst",
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
		f, err := os.Open(filepath.FromSlash(file))
		require.NoError(t, err)

		pathCopy := path.Join(t.TempDir(), path.Base(file))
		fCopy, err := os.OpenFile(pathCopy, os.O_CREATE|os.O_RDWR, 0600)
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
			var bh BlockHandle
			switch location {
			case corruptionLocationData:
				bh = layout.Data[rng.Intn(len(layout.Data))].BlockHandle
			case corruptionLocationIndex:
				bh = layout.Index[rng.Intn(len(layout.Index))]
			case corruptionLocationTopIndex:
				bh = layout.TopIndex
			case corruptionLocationFilter:
				bh = layout.Filter
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
			pos := int64(bh.Offset) + rng.Int63n(int64(bh.Length))
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
		// By default, test across all files, unless overridden.
		files := tc.files
		if files == nil {
			files = allFiles
		}
		for _, file := range files {
			t.Run(tc.name+" "+path.Base(file), func(t *testing.T) {
				testFn(t, file, tc.corruptionLocations)
			})
		}
	}
}

func TestReader_TableFormat(t *testing.T) {
	test := func(t *testing.T, want TableFormat) {
		fs := vfs.NewMem()
		f, err := fs.Create("test")
		require.NoError(t, err)

		opts := WriterOptions{TableFormat: want}
		w := NewWriter(f, opts)
		err = w.Close()
		require.NoError(t, err)

		f, err = fs.Open("test")
		require.NoError(t, err)
		r, err := newReader(f, ReaderOptions{})
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

func buildTestTable(
	t *testing.T, numEntries uint64, blockSize, indexBlockSize int, compression Compression,
) *Reader {
	provider := objstorage.New(objstorage.DefaultSettings(vfs.NewMem(), "" /* dirName */))
	return buildTestTableWithProvider(t, provider, numEntries, blockSize, indexBlockSize, compression)
}

func buildTestTableWithProvider(
	t *testing.T,
	provider *objstorage.Provider,
	numEntries uint64,
	blockSize, indexBlockSize int,
	compression Compression,
) *Reader {
	f0, err := provider.Create(base.FileTypeTable, 0 /* fileNum */)
	require.NoError(t, err)

	w := NewWriter(f0, WriterOptions{
		BlockSize:      blockSize,
		IndexBlockSize: indexBlockSize,
		Compression:    compression,
		FilterPolicy:   nil,
	})

	var ikey InternalKey
	for i := uint64(0); i < numEntries; i++ {
		key := make([]byte, 8+i%3)
		value := make([]byte, i%100)
		binary.BigEndian.PutUint64(key, i)
		ikey.UserKey = key
		w.Add(ikey, value)
	}

	require.NoError(t, w.Close())

	// Re-open that filename for reading.
	f1, err := provider.OpenForReading(base.FileTypeTable, 0 /* fileNum */)
	require.NoError(t, err)

	c := cache.New(128 << 20)
	defer c.Unref()
	r, err := NewReader(f1, ReaderOptions{
		Cache: c,
	})
	require.NoError(t, err)
	return r
}

func buildBenchmarkTable(
	b *testing.B, options WriterOptions, confirmTwoLevelIndex bool, offset int,
) (*Reader, [][]byte) {
	mem := vfs.NewMem()
	f0, err := mem.Create("bench")
	if err != nil {
		b.Fatal(err)
	}

	w := NewWriter(f0, options)

	var keys [][]byte
	var ikey InternalKey
	for i := uint64(0); i < 1e6; i++ {
		key := make([]byte, 8)
		binary.BigEndian.PutUint64(key, i+uint64(offset))
		keys = append(keys, key)
		ikey.UserKey = key
		w.Add(ikey, nil)
	}

	if err := w.Close(); err != nil {
		b.Fatal(err)
	}

	// Re-open that filename for reading.
	f1, err := mem.Open("bench")
	if err != nil {
		b.Fatal(err)
	}
	c := cache.New(128 << 20)
	defer c.Unref()
	r, err := newReader(f1, ReaderOptions{
		Cache: c,
	})
	if err != nil {
		b.Fatal(err)
	}
	if confirmTwoLevelIndex && r.Properties.IndexPartitions == 0 {
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
			Compression:          SnappyCompression,
			TableFormat:          TableFormatPebblev2,
		},
	},
	{
		name: "restart=16,compression=ZSTD",
		options: WriterOptions{
			BlockSize:            32 << 10,
			BlockRestartInterval: 16,
			FilterPolicy:         nil,
			Compression:          ZstdCompression,
			TableFormat:          TableFormatPebblev2,
		},
	},
}

func BenchmarkTableIterSeekGE(b *testing.B) {
	for _, bm := range basicBenchmarks {
		b.Run(bm.name,
			func(b *testing.B) {
				r, keys := buildBenchmarkTable(b, bm.options, false, 0)
				it, err := r.NewIter(nil /* lower */, nil /* upper */)
				require.NoError(b, err)
				rng := rand.New(rand.NewSource(uint64(time.Now().UnixNano())))

				b.ResetTimer()
				for i := 0; i < b.N; i++ {
					it.SeekGE(keys[rng.Intn(len(keys))], base.SeekGEFlagsNone)
				}

				b.StopTimer()
				it.Close()
				r.Close()
			})
	}
}

func BenchmarkTableIterSeekLT(b *testing.B) {
	for _, bm := range basicBenchmarks {
		b.Run(bm.name,
			func(b *testing.B) {
				r, keys := buildBenchmarkTable(b, bm.options, false, 0)
				it, err := r.NewIter(nil /* lower */, nil /* upper */)
				require.NoError(b, err)
				rng := rand.New(rand.NewSource(uint64(time.Now().UnixNano())))

				b.ResetTimer()
				for i := 0; i < b.N; i++ {
					it.SeekLT(keys[rng.Intn(len(keys))], base.SeekLTFlagsNone)
				}

				b.StopTimer()
				it.Close()
				r.Close()
			})
	}
}

func BenchmarkTableIterNext(b *testing.B) {
	for _, bm := range basicBenchmarks {
		b.Run(bm.name,
			func(b *testing.B) {
				r, _ := buildBenchmarkTable(b, bm.options, false, 0)
				it, err := r.NewIter(nil /* lower */, nil /* upper */)
				require.NoError(b, err)

				b.ResetTimer()
				var sum int64
				var key *InternalKey
				for i := 0; i < b.N; i++ {
					if key == nil {
						key, _ = it.First()
					}
					sum += int64(binary.BigEndian.Uint64(key.UserKey))
					key, _ = it.Next()
				}
				if testing.Verbose() {
					fmt.Fprint(io.Discard, sum)
				}

				b.StopTimer()
				it.Close()
				r.Close()
			})
	}
}

func BenchmarkTableIterPrev(b *testing.B) {
	for _, bm := range basicBenchmarks {
		b.Run(bm.name,
			func(b *testing.B) {
				r, _ := buildBenchmarkTable(b, bm.options, false, 0)
				it, err := r.NewIter(nil /* lower */, nil /* upper */)
				require.NoError(b, err)

				b.ResetTimer()
				var sum int64
				var key *InternalKey
				for i := 0; i < b.N; i++ {
					if key == nil {
						key, _ = it.Last()
					}
					sum += int64(binary.BigEndian.Uint64(key.UserKey))
					key, _ = it.Prev()
				}
				if testing.Verbose() {
					fmt.Fprint(io.Discard, sum)
				}

				b.StopTimer()
				it.Close()
				r.Close()
			})
	}
}

func BenchmarkLayout(b *testing.B) {
	r, _ := buildBenchmarkTable(b, WriterOptions{}, false, 0)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		r.Layout()
	}
	b.StopTimer()
	r.Close()
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
		reader, keys := buildBenchmarkTable(b, options, twoLevelIndex, offsetCount)
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
						it, err := reader.NewIter(nil /* lower */, upper)
						require.NoError(b, err)
						b.ResetTimer()
						pos := 0
						var seekGEFlags SeekGEFlags
						for i := 0; i < b.N; i++ {
							seekKey := seekKeys[0]
							var k *InternalKey
							if prefixSeek {
								k, _ = it.SeekPrefixGE(seekKey, seekKey, seekGEFlags)
							} else {
								k, _ = it.SeekGE(seekKey, seekGEFlags)
							}
							if k != nil {
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
	}
}

func BenchmarkIteratorScanManyVersions(b *testing.B) {
	options := WriterOptions{
		BlockSize:            32 << 10,
		BlockRestartInterval: 16,
		FilterPolicy:         nil,
		Compression:          SnappyCompression,
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
	setupBench := func(b *testing.B, tableFormat TableFormat, cacheSize int64) *Reader {
		mem := vfs.NewMem()
		f0, err := mem.Create("bench")
		require.NoError(b, err)
		options.TableFormat = tableFormat
		w := NewWriter(f0, options)
		val := make([]byte, 100)
		rng := rand.New(rand.NewSource(100))
		for i := 0; i < keys.Count(); i++ {
			for v := 0; v < versionCount; v++ {
				n := testkeys.WriteKeyAt(keyBuf[sharedPrefixLen:], keys, i, versionCount-v+1)
				key := keyBuf[:n+sharedPrefixLen]
				rng.Read(val)
				require.NoError(b, w.Set(key, val))
			}
		}
		require.NoError(b, w.Close())
		c := cache.New(cacheSize)
		defer c.Unref()
		// Re-open the filename for reading.
		f0, err = mem.Open("bench")
		require.NoError(b, err)
		r, err := newReader(f0, ReaderOptions{
			Cache:    c,
			Comparer: testkeys.Comparer,
		})
		require.NoError(b, err)
		return r
	}
	for _, format := range []TableFormat{TableFormatPebblev2, TableFormatPebblev3} {
		b.Run(fmt.Sprintf("format=%s", format.String()), func(b *testing.B) {
			// 150MiB results in a high cache hit rate for both formats. 20MiB
			// results in a high cache hit rate for the data blocks in
			// TableFormatPebblev3.
			for _, cacheSize := range []int64{20 << 20, 150 << 20} {
				b.Run(fmt.Sprintf("cache-size=%s", humanize.IEC.Int64(cacheSize)),
					func(b *testing.B) {
						r := setupBench(b, format, cacheSize)
						defer func() {
							require.NoError(b, r.Close())
						}()
						for _, readValue := range []bool{false, true} {
							b.Run(fmt.Sprintf("read-value=%t", readValue), func(b *testing.B) {
								iter, err := r.NewIter(nil, nil)
								require.NoError(b, err)
								var k *InternalKey
								var v base.LazyValue
								var valBuf [100]byte
								b.ResetTimer()
								for i := 0; i < b.N; i++ {
									if k == nil {
										k, _ = iter.First()
										if k == nil {
											b.Fatalf("k is nil")
										}
									}
									k, v = iter.Next()
									if k != nil && readValue {
										_, callerOwned, err := v.Value(valBuf[:])
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
		Compression:          SnappyCompression,
		TableFormat:          TableFormatPebblev3,
		Comparer:             testkeys.Comparer,
	}
	const keyCount = 10000
	const sharedPrefixLen = 32
	const unsharedPrefixLen = 8
	val := make([]byte, 100)
	rand.New(rand.NewSource(100)).Read(val)

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
	setupBench := func(b *testing.B, versCount int) (r *Reader, succKeys [][]byte) {
		mem := vfs.NewMem()
		f0, err := mem.Create("bench")
		require.NoError(b, err)
		w := NewWriter(f0, options)
		for i := 0; i < keys.Count(); i++ {
			for v := 0; v < versCount; v++ {
				n := testkeys.WriteKeyAt(keyBuf[sharedPrefixLen:], keys, i, versCount-v+1)
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
		// NB: This 200MiB cache is sufficient for even the largest file: 10,000
		// keys * 100 versions = 1M keys, where each key-value pair is ~140 bytes
		// = 140MB. So we are not measuring the caching benefit of
		// TableFormatPebblev3 storing older values in value blocks.
		c := cache.New(200 << 20)
		defer c.Unref()
		// Re-open the filename for reading.
		f0, err = mem.Open("bench")
		require.NoError(b, err)
		r, err = newReader(f0, ReaderOptions{
			Cache:    c,
			Comparer: testkeys.Comparer,
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
			r, succKeys := setupBench(b, versionCount)
			defer func() {
				require.NoError(b, r.Close())
			}()
			for _, method := range []string{"seek-ge", "next-prefix"} {
				b.Run(fmt.Sprintf("method=%s", method), func(b *testing.B) {
					for _, readValue := range []bool{false, true} {
						b.Run(fmt.Sprintf("read-value=%t", readValue), func(b *testing.B) {
							iter, err := r.NewIter(nil, nil)
							require.NoError(b, err)
							var nextFunc func(index int) (*InternalKey, base.LazyValue)
							switch method {
							case "seek-ge":
								nextFunc = func(index int) (*InternalKey, base.LazyValue) {
									var flags base.SeekGEFlags
									return iter.SeekGE(succKeys[index], flags.EnableTrySeekUsingNext())
								}
							case "next-prefix":
								nextFunc = func(index int) (*InternalKey, base.LazyValue) {
									return iter.NextPrefix(succKeys[index])
								}
							default:
								b.Fatalf("unknown method %s", method)
							}
							n := keys.Count()
							j := n
							var k *InternalKey
							var v base.LazyValue
							var valBuf [100]byte
							b.ResetTimer()
							for i := 0; i < b.N; i++ {
								if k == nil {
									if j != n {
										b.Fatalf("unexpected %d != %d", j, n)
									}
									k, _ = iter.First()
									j = 0
								} else {
									k, v = nextFunc(j - 1)
									if k != nil && readValue {
										_, callerOwned, err := v.Value(valBuf[:])
										if err != nil {
											b.Fatal(err)
										} else if callerOwned {
											b.Fatalf("unexpected callerOwned: %t", callerOwned)
										}
									}

								}
								if k != nil {
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

func newReader(r ReadableFile, o ReaderOptions, extraOpts ...ReaderOption) (*Reader, error) {
	readable, err := NewSimpleReadable(r)
	if err != nil {
		return nil, err
	}
	return NewReader(readable, o, extraOpts...)
}
