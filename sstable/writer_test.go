// Copyright 2018 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package sstable

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"strconv"
	"strings"
	"testing"

	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/pebble/bloom"
	"github.com/cockroachdb/pebble/internal/base"
	"github.com/cockroachdb/pebble/internal/cache"
	"github.com/cockroachdb/pebble/internal/datadriven"
	"github.com/cockroachdb/pebble/internal/humanize"
	"github.com/cockroachdb/pebble/vfs"
	"github.com/stretchr/testify/require"
)

func TestWriter(t *testing.T) {
	runDataDriven(t, "testdata/writer")
}

func TestRewriter(t *testing.T) {
	runDataDriven(t, "testdata/rewriter")
}

func runDataDriven(t *testing.T, file string) {
	var r *Reader
	defer func() {
		if r != nil {
			require.NoError(t, r.Close())
		}
	}()

	format := func(m *WriterMetadata) string {
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
		return b.String()
	}

	datadriven.RunTest(t, file, func(td *datadriven.TestData) string {
		switch td.Cmd {
		case "build":
			if r != nil {
				_ = r.Close()
				r = nil
			}
			var meta *WriterMetadata
			var err error
			meta, r, err = runBuildCmd(td, &WriterOptions{
				TableFormat: TableFormatMax,
			}, 0)
			if err != nil {
				return err.Error()
			}
			return format(meta)

		case "build-raw":
			if r != nil {
				_ = r.Close()
				r = nil
			}
			var meta *WriterMetadata
			var err error
			meta, r, err = runBuildRawCmd(td, &WriterOptions{
				TableFormat: TableFormatMax,
			})
			if err != nil {
				return err.Error()
			}
			return format(meta)

		case "scan":
			origIter, err := r.NewIter(nil /* lower */, nil /* upper */)
			if err != nil {
				return err.Error()
			}
			iter := newIterAdapter(origIter)
			defer iter.Close()

			var buf bytes.Buffer
			for valid := iter.First(); valid; valid = iter.Next() {
				fmt.Fprintf(&buf, "%s:%s\n", iter.Key(), iter.Value())
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
			iter, err := r.NewRawRangeDelIter()
			if err != nil {
				return err.Error()
			}
			if iter == nil {
				return ""
			}
			defer iter.Close()

			var buf bytes.Buffer
			for key, val := iter.First(); key != nil; key, val = iter.Next() {
				fmt.Fprintf(&buf, "%s:%s\n", key, val)
			}
			return buf.String()

		case "scan-range-key":
			iter, err := r.NewRawRangeKeyIter()
			if err != nil {
				return err.Error()
			}
			if iter == nil {
				return ""
			}
			defer iter.Close()

			var buf bytes.Buffer
			for key, val := iter.First(); key != nil; key, val = iter.Next() {
				fmt.Fprintf(&buf, "%s:%s\n", key, val)
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

		case "rewrite":
			var meta *WriterMetadata
			var err error
			meta, r, err = runRewriteCmd(td, r, WriterOptions{})
			if err != nil {
				return err.Error()
			}
			if err != nil {
				return err.Error()
			}
			return format(meta)

		default:
			return fmt.Sprintf("unknown command: %s", td.Cmd)
		}
	})
}

func TestSizeEstimate(t *testing.T) {
	var sizeEstimate sizeEstimate
	datadriven.RunTest(t, "testdata/size_estimate",
		func(td *datadriven.TestData) string {
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
				if len(td.CmdArgs) != 3 {
					return "entry_written <new_size> <prev_inflight_size> <entry_size>"
				}
				newSize, err := strconv.Atoi(td.CmdArgs[0].String())
				if err != nil {
					return "invalid inflight size"
				}
				inflightSize, err := strconv.Atoi(td.CmdArgs[1].String())
				if err != nil {
					return "invalid inflight size"
				}
				entrySize, err := strconv.Atoi(td.CmdArgs[2].String())
				if err != nil {
					return "invalid inflight size"
				}
				sizeEstimate.written(uint64(newSize), inflightSize, entrySize)
				return fmt.Sprintf("%d", sizeEstimate.size())
			case "num_entries":
				return fmt.Sprintf("%d", sizeEstimate.numEntries)
			default:
				return fmt.Sprintf("unknown command: %s", td.Cmd)
			}
		})
}

func TestWriterClearCache(t *testing.T) {
	// Verify that Writer clears the cache of blocks that it writes.
	mem := vfs.NewMem()
	opts := ReaderOptions{Cache: cache.New(64 << 20)}
	defer opts.Cache.Unref()

	writerOpts := WriterOptions{Cache: opts.Cache}
	cacheOpts := &cacheOpts{cacheID: 1, fileNum: 1}
	invalidData := func() *cache.Value {
		invalid := []byte("invalid data")
		v := opts.Cache.Alloc(len(invalid))
		copy(v.Buf(), invalid)
		return v
	}

	build := func(name string) {
		f, err := mem.Create(name)
		require.NoError(t, err)

		w := NewWriter(f, writerOpts, cacheOpts)
		require.NoError(t, w.Set([]byte("hello"), []byte("world")))
		require.NoError(t, w.Close())
	}

	// Build the sstable a first time so that we can determine the locations of
	// all of the blocks.
	build("test")

	f, err := mem.Open("test")
	require.NoError(t, err)

	r, err := NewReader(f, opts)
	require.NoError(t, err)

	layout, err := r.Layout()
	require.NoError(t, err)

	foreachBH := func(layout *Layout, f func(bh BlockHandle)) {
		for _, bh := range layout.Data {
			f(bh.BlockHandle)
		}
		for _, bh := range layout.Index {
			f(bh)
		}
		f(layout.TopIndex)
		f(layout.Filter)
		f(layout.RangeDel)
		f(layout.Properties)
		f(layout.MetaIndex)
	}

	// Poison the cache for each of the blocks.
	poison := func(bh BlockHandle) {
		opts.Cache.Set(cacheOpts.cacheID, cacheOpts.fileNum, bh.Offset, invalidData()).Release()
	}
	foreachBH(layout, poison)

	// Build the table a second time. This should clear the cache for the blocks
	// that are written.
	build("test")

	// Verify that the written blocks have been cleared from the cache.
	check := func(bh BlockHandle) {
		h := opts.Cache.Get(cacheOpts.cacheID, cacheOpts.fileNum, bh.Offset)
		if h.Get() != nil {
			t.Fatalf("%d: expected cache to be cleared, but found %q", bh.Offset, h.Get())
		}
	}
	foreachBH(layout, check)

	require.NoError(t, r.Close())
}

type discardFile struct{ wrote int64 }

func (f discardFile) Close() error {
	return nil
}

func (f *discardFile) Write(p []byte) (int, error) {
	f.wrote += int64(len(p))
	return len(p), nil
}

func (f discardFile) Sync() error {
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

func (c *testBlockPropCollector) Name() string { return "testBlockPropCollector" }

func (c *testBlockPropCollector) Add(_ InternalKey, _ []byte) error {
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

func TestWriterBlockPropertiesErrors(t *testing.T) {
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
			f, err := fs.Create("test")
			require.NoError(t, err)

			w := NewWriter(f, WriterOptions{
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

			err = w.Add(k1, v1)
			switch tc {
			case errSiteAdd:
				require.Error(t, err)
				require.Equal(t, blockPropErr, err)
				return
			case errSiteFinishBlock:
				require.NoError(t, err)
				// Addition of a second key completes the first block.
				err = w.Add(k2, v2)
				require.Error(t, err)
				require.Equal(t, blockPropErr, err)
				return
			case errSiteFinishIndex:
				require.NoError(t, err)
				// Addition of a second key completes the first block.
				err = w.Add(k2, v2)
				require.NoError(t, err)
				// The index entry for the first block is added after the completion of
				// the second block, which is triggered by adding a third key.
				err = w.Add(k3, v3)
				require.Error(t, err)
				require.Equal(t, blockPropErr, err)
				return
			}

			err = w.Close()
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
							"collector", &valueCharBlockIntervalCollector{charIdx: 0}, nil,
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
					f, err := fs.Create("sst")
					require.NoError(t, err)

					opts := WriterOptions{TableFormat: tf}
					if tc.configureFn != nil {
						tc.configureFn(&opts)
					}

					w := NewWriter(f, opts)
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

	b.ResetTimer()

	for _, bs := range []int{base.DefaultBlockSize, 32 << 10} {
		b.Run(fmt.Sprintf("block=%s", humanize.IEC.Int64(int64(bs))), func(b *testing.B) {
			for _, filter := range []bool{true, false} {
				b.Run(fmt.Sprintf("filter=%t", filter), func(b *testing.B) {
					for _, comp := range []Compression{NoCompression, SnappyCompression, ZstdCompression} {
						b.Run(fmt.Sprintf("compression=%s", comp), func(b *testing.B) {
							opts := WriterOptions{
								BlockRestartInterval: 16,
								BlockSize:            bs,
								Compression:          comp,
							}
							if filter {
								opts.FilterPolicy = bloom.FilterPolicy(10)
							}
							f := &discardFile{}
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

var test4bSuffixComparer = &base.Comparer{
	Compare:   base.DefaultComparer.Compare,
	Equal:     base.DefaultComparer.Equal,
	Separator: base.DefaultComparer.Separator,
	Successor: base.DefaultComparer.Successor,
	Split: func(key []byte) int {
		if len(key) > 4 {
			return len(key) - 4
		}
		return len(key)
	},
	Name: "comparer-split-4b-suffix",
}
