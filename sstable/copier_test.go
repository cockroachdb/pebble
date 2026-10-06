// Copyright 2024 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package sstable

import (
	"context"
	"fmt"
	"math/rand/v2"
	"strings"
	"testing"

	"github.com/cockroachdb/datadriven"
	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/pebble/internal/base"
	"github.com/cockroachdb/pebble/internal/cache"
	"github.com/cockroachdb/pebble/internal/sstableinternal"
	"github.com/cockroachdb/pebble/internal/testkeys"
	"github.com/cockroachdb/pebble/objstorage"
	"github.com/cockroachdb/pebble/objstorage/objstorageprovider"
	"github.com/cockroachdb/pebble/sstable/block"
	"github.com/cockroachdb/pebble/sstable/blockiter"
	"github.com/cockroachdb/pebble/sstable/colblk"
	"github.com/cockroachdb/pebble/sstable/tablefilters/bloom"
	"github.com/cockroachdb/pebble/vfs"
	"github.com/stretchr/testify/require"
)

func TestCopySpan(t *testing.T) {
	fs := vfs.NewMem()
	blockCache := cache.New(2 << 20 /* 1 MB */)
	defer blockCache.Unref()
	cacheHandle := blockCache.NewHandle()
	defer cacheHandle.Close()
	fileNameToNum := make(map[string]base.FileNum)
	nextFileNum := base.FileNum(1)
	keySchema := colblk.DefaultKeySchema(testkeys.Comparer, 16)

	getReader := func(d *datadriven.TestData) (*Reader, error) {
		f, err := fs.Open(d.CmdArgs[0].Key)
		if err != nil {
			return nil, err
		}
		readable, err := objstorage.NewSimpleReadable(f)
		if err != nil {
			return nil, err
		}

		rOpts := ReaderOptions{
			FilterDecoders: []base.TableFilterDecoder{bloom.Decoder},
			ReaderOptions: block.ReaderOptions{
				CacheOpts: sstableinternal.CacheOptions{
					CacheHandle: cacheHandle,
					FileNum:     base.DiskFileNum(fileNameToNum[d.CmdArgs[0].Key]),
				},
			},
			Comparer:   testkeys.Comparer,
			KeySchemas: KeySchemas{keySchema.Name: &keySchema},
		}
		r, err := NewReader(context.TODO(), readable, rOpts)
		if err != nil {
			return nil, errors.CombineErrors(err, readable.Close())
		}
		return r, nil
	}

	datadriven.RunTest(t, "testdata/copy_span", func(t *testing.T, d *datadriven.TestData) string {
		switch d.Cmd {
		case "build":
			// Build an sstable from the specified keys
			f, err := fs.Create(d.CmdArgs[0].Key, vfs.WriteCategoryUnspecified)
			if err != nil {
				return err.Error()
			}
			fileNameToNum[d.CmdArgs[0].Key] = nextFileNum
			nextFileNum++
			tableFormat := TableFormatMax

			writerOpts := WriterOptions{
				BlockSize:   1,
				TableFormat: tableFormat,
				Comparer:    testkeys.Comparer,
				KeySchema:   &keySchema,
			}
			if err := ParseWriterOptions(&writerOpts, d.CmdArgs[1:]...); err != nil {
				t.Fatal(err)
			}
			w := NewWriter(objstorageprovider.NewFileWritable(f), writerOpts)
			if err := ParseTestSST(w.rw, d.Input, nil /* bv */); err != nil {
				t.Fatal(err)
			}
			if err := w.Close(); err != nil {
				return err.Error()
			}

			return ""

		case "iter":
			// Iterate over the specified sstable
			var start, end []byte
			for _, arg := range d.CmdArgs[1:] {
				switch arg.Key {
				case "start":
					start = []byte(arg.FirstVal(t))
				case "end":
					end = []byte(arg.FirstVal(t))
				}
			}

			r, err := getReader(d)
			if err != nil {
				return err.Error()
			}
			defer r.Close()
			iter, err := r.NewIter(blockiter.NoTransforms, start, end, AssertNoBlobHandles)
			if err != nil {
				return err.Error()
			}
			defer iter.Close()
			var result strings.Builder
			for kv := iter.First(); kv != nil; kv = iter.Next() {
				fmt.Fprintf(&result, "%s: %s\n", kv.K, kv.InPlaceValue())
			}
			return result.String()

		case "copy-span":
			// Copy a span from one sstable to another
			if len(d.CmdArgs) < 4 {
				t.Fatalf("expected input sstable, output sstable, start and end keys")
			}

			inputFile := d.CmdArgs[0].Key
			outputFile := d.CmdArgs[1].Key
			start := base.ParseInternalKey(d.CmdArgs[2].String())
			end := base.ParseInternalKey(d.CmdArgs[3].String())
			wOpts := WriterOptions{
				Comparer:  testkeys.Comparer,
				KeySchema: &keySchema,
			}
			if err := ParseWriterOptions(&wOpts, d.CmdArgs[4:]...); err != nil {
				t.Fatal(err)
			}

			output, err := fs.Create(outputFile, vfs.WriteCategoryUnspecified)
			if err != nil {
				return err.Error()
			}
			writable := objstorageprovider.NewFileWritable(output)
			fileNameToNum[outputFile] = nextFileNum
			nextFileNum++

			r, err := getReader(d)
			if err != nil {
				return err.Error()
			}
			defer r.Close()

			// CopySpan closes readable but not reader. We need to open a new readable for it.
			f2, err := fs.Open(inputFile)
			if err != nil {
				t.Fatalf("failed to open sstable: %v", err)
			}
			readable2, err := objstorage.NewSimpleReadable(f2)
			if err != nil {
				return err.Error()
			}
			size, err := CopySpan(context.TODO(), readable2, r, 0 /* level */, writable, wOpts, start, end)
			if err != nil {
				return err.Error()
			}
			return fmt.Sprintf("copied %d bytes", size)

		case "describe":
			r, err := getReader(d)
			if err != nil {
				return err.Error()
			}
			defer r.Close()
			l, err := r.Layout()
			if err != nil {
				return err.Error()
			}
			return l.Describe(false /* verbose */, r, nil)

		case "props":
			r, err := getReader(d)
			if err != nil {
				return err.Error()
			}
			defer r.Close()
			props, err := r.ReadPropertiesBlock(context.TODO(), nil)
			if err != nil {
				return err.Error()
			}
			return props.String()

		default:
			t.Fatalf("unknown command: %s", d.Cmd)
			return ""
		}
	})
}

// TestCopySpanCompressionStats verifies that the compression stats of a table
// produced by CopySpan account for all its blocks, regardless of whether the
// data blocks were copied verbatim (block cache misses) or recompressed (block
// cache hits).
func TestCopySpanCompressionStats(t *testing.T) {
	ctx := context.Background()
	keySchema := colblk.DefaultKeySchema(testkeys.Comparer, 16)
	key := func(i int) []byte { return fmt.Appendf(nil, "key%05d", i) }
	const numKeys = 1000

	for _, profile := range []*block.CompressionProfile{
		block.SnappyCompression, block.MinLZCompression, block.ZstdCompression,
	} {
		t.Run(profile.Name, func(t *testing.T) {
			// Use the same compression for the source and the copy, so that
			// recompressing a block reproduces the original block.
			wOpts := WriterOptions{
				Comparer:    testkeys.Comparer,
				KeySchema:   &keySchema,
				TableFormat: TableFormatMax,
				BlockSize:   512,
				Compression: profile,
			}
			src := &objstorage.MemObj{}
			w := NewWriter(src, wOpts)
			rng := rand.New(rand.NewPCG(0, 1))
			value := make([]byte, 100)
			for i := range numKeys {
				// Alternate between runs of compressible and incompressible values,
				// so that some blocks are stored uncompressed.
				for j := range value {
					if (i/50)%2 == 0 {
						value[j] = byte(rng.Uint32())
					} else {
						value[j] = byte(j % 4)
					}
				}
				require.NoError(t, w.Set(key(i), value))
			}
			require.NoError(t, w.Close())

			// copySpan populates a fresh block cache by reading the keys in
			// [warmStart, warmEnd), then copies a span of the source table and
			// returns the compression stats of the result.
			copySpan := func(warmStart, warmEnd int) string {
				c := cache.New(16 << 20)
				defer c.Unref()
				h := c.NewHandle()
				defer h.Close()
				rOpts := ReaderOptions{
					ReaderOptions: block.ReaderOptions{
						CacheOpts: sstableinternal.CacheOptions{CacheHandle: h, FileNum: 1},
					},
					Comparer:   testkeys.Comparer,
					KeySchemas: KeySchemas{keySchema.Name: &keySchema},
				}
				r, err := NewReader(ctx, src, rOpts)
				require.NoError(t, err)
				defer func() { require.NoError(t, r.Close()) }()
				if warmStart < warmEnd {
					iter, err := r.NewIter(blockiter.NoTransforms, key(warmStart), key(warmEnd), AssertNoBlobHandles)
					require.NoError(t, err)
					for kv := iter.First(); kv != nil; kv = iter.Next() {
					}
					require.NoError(t, iter.Close())
				}
				out := &objstorage.MemObj{}
				_, err = CopySpan(ctx, src, r, 0 /* level */, out, wOpts,
					base.MakeInternalKey(key(200), base.SeqNumMax, base.InternalKeyKindSet),
					base.MakeInternalKey(key(800), 0, base.InternalKeyKindSet))
				require.NoError(t, err)

				outR, err := NewMemReader(out.Data(), rOpts)
				require.NoError(t, err)
				defer func() { require.NoError(t, outR.Close()) }()
				props, err := outR.ReadPropertiesBlock(ctx, nil /* buffer pool */)
				require.NoError(t, err)
				return props.CompressionStats
			}

			warm := copySpan(0, numKeys)
			// The data blocks dominate the stats; make sure the expected settings
			// are present.
			// Uncompressed blocks can be accounted under a setting with a level
			// (e.g. None3).
			require.Contains(t, warm, "None")
			require.Contains(t, warm, profile.DataBlocks.Setting.String()+":")
			require.Equal(t, warm, copySpan(0, 0), "cold cache")
			require.Equal(t, warm, copySpan(400, 600), "partially warm cache")
		})
	}
}
