// Copyright 2025 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package blob

import (
	"bytes"
	"context"
	"fmt"
	"math/rand/v2"
	"testing"

	"github.com/cockroachdb/crlib/crstrings"
	"github.com/cockroachdb/crlib/testutils/leaktest"
	"github.com/cockroachdb/datadriven"
	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/pebble/internal/base"
	"github.com/cockroachdb/pebble/internal/cache"
	"github.com/cockroachdb/pebble/internal/sstableinternal"
	"github.com/cockroachdb/pebble/internal/testutils"
	"github.com/cockroachdb/pebble/objstorage"
	"github.com/cockroachdb/pebble/sstable/block"
	"github.com/cockroachdb/pebble/sstable/valblk"
	"github.com/stretchr/testify/require"
)

type mockReaderProvider struct {
	w       *bytes.Buffer
	readers map[base.DiskFileNum]*FileReader
}

func (rp *mockReaderProvider) GetValueReader(
	ctx context.Context, fileNum base.DiskFileNum,
) (r ValueReader, closeFunc func(), err error) {
	if rp.w != nil {
		fmt.Fprintf(rp.w, "# GetValueReader(%s)\n", fileNum)
	}
	vr, ok := rp.readers[fileNum]
	if !ok {
		return nil, nil, errors.Newf("no reader for file %s", fileNum)
	}
	return vr, func() {}, nil
}

func TestValueFetcher(t *testing.T) {
	ctx := context.Background()
	var buf bytes.Buffer
	rp := &mockReaderProvider{
		w:       &buf,
		readers: map[base.DiskFileNum]*FileReader{},
	}
	fetchers := map[string]*ValueFetcher{}
	defer func() {
		for _, f := range fetchers {
			require.NoError(t, f.Close())
		}
		for _, r := range rp.readers {
			require.NoError(t, r.Close())
		}
	}()

	var handleBuf [valblk.HandleMaxLen]byte
	datadriven.RunTest(t, "testdata/value_fetcher", func(t *testing.T, td *datadriven.TestData) string {
		buf.Reset()
		switch td.Cmd {
		case "define":
			var fileNum uint64
			td.ScanArgs(t, "filenum", &fileNum)
			opts := scanFileWriterOptions(t, td)
			obj := &objstorage.MemObj{}
			w := NewFileWriter(base.DiskFileNum(fileNum), obj, opts)
			for _, l := range crstrings.Lines(td.Input) {
				h := w.AddValue([]byte(l))
				fmt.Fprintln(&buf, h)
			}
			stats, err := w.Close()
			if err != nil {
				t.Fatal(err)
			}
			printFileWriterStats(&buf, stats)

			r, err := NewFileReader(ctx, obj, FileReaderOptions{
				ReaderOptions: block.ReaderOptions{
					CacheOpts: sstableinternal.CacheOptions{
						FileNum: base.DiskFileNum(fileNum),
					},
				},
			})
			if err != nil {
				t.Fatal(err)
			}
			rp.readers[base.DiskFileNum(fileNum)] = r
			return buf.String()
		case "new-fetcher":
			var name string
			td.ScanArgs(t, "name", &name)
			fetchers[name] = &ValueFetcher{}
			fetchers[name].Init(rp, block.ReadEnv{})
			return ""
		case "fetch":
			var (
				name                            string
				blobFileNum                     uint64
				valLen, blockNum, offsetInBlock uint32
			)
			td.ScanArgs(t, "name", &name)
			td.ScanArgs(t, "filenum", &blobFileNum)
			td.ScanArgs(t, "valLen", &valLen)
			td.ScanArgs(t, "blknum", &blockNum)
			td.ScanArgs(t, "off", &offsetInBlock)
			fetcher := fetchers[name]
			if fetcher == nil {
				t.Fatalf("fetcher %s not found", name)
			}
			handle := encodeRemainingHandle(handleBuf[:], blockNum, offsetInBlock)

			val, _, err := fetcher.Fetch(ctx, handle, base.DiskFileNum(blobFileNum), valLen, nil)
			if err != nil {
				t.Fatal(err)
			}
			writeValueFetcherState(&buf, fetcher)
			fmt.Fprintf(&buf, "%s\n", val)
			return buf.String()
		default:
			panic(fmt.Sprintf("unknown command: %s", td.Cmd))
		}
	})
}

func encodeRemainingHandle(dst []byte, blockNum uint32, offsetInBlock uint32) []byte {
	n := valblk.EncodeHandle(dst, valblk.Handle{
		ValueLen:      0,
		BlockNum:      blockNum,
		OffsetInBlock: offsetInBlock,
	})
	return dst[1:n]
}

func writeValueFetcherState(w *bytes.Buffer, f *ValueFetcher) {
	fmt.Fprintf(w, "ValueFetcher{\n")
	for _, cr := range f.readers {
		if cr.r == nil {
			fmt.Fprintln(w, "  empty")
			continue
		}
		fmt.Fprintf(w, "  %s (blk%d)\n", cr.fileNum, cr.currentBlockNum)
	}
	fmt.Fprintf(w, "}\n")
}

func BenchmarkValueFetcherRetrieve(b *testing.B) {
	defer leaktest.AfterTest(b)()
	b.Run("uncached", func(b *testing.B) {
		b.Run("2MiB", func(b *testing.B) {
			benchmarkValueFetcherRetrieve(b, 2<<20, nil)
		})
		b.Run("16MiB", func(b *testing.B) {
			benchmarkValueFetcherRetrieve(b, 16<<20, nil)
		})
		b.Run("64MiB", func(b *testing.B) {
			benchmarkValueFetcherRetrieve(b, 64<<20, nil)
		})
	})
	// The cache variants use a cache double the size of the files' uncompressed
	// value size to ensure all blocks fit in the cache. Each run uses a fresh
	// cache to ensure there's no interference between variants.
	b.Run("cache", func(b *testing.B) {
		b.Run("2MiB", func(b *testing.B) {
			c := cache.NewWithShards(4<<20, 1)
			defer c.Unref()
			ch := c.NewHandle()
			defer ch.Close()
			benchmarkValueFetcherRetrieve(b, 2<<20, ch)
		})
		b.Run("16MiB", func(b *testing.B) {
			c := cache.NewWithShards(32<<20, 1)
			defer c.Unref()
			ch := c.NewHandle()
			defer ch.Close()
			benchmarkValueFetcherRetrieve(b, 16<<20, ch)
		})
		b.Run("64MiB", func(b *testing.B) {
			c := cache.NewWithShards(128<<20, 1)
			defer c.Unref()
			ch := c.NewHandle()
			defer ch.Close()
			benchmarkValueFetcherRetrieve(b, 64<<20, ch)
		})
	})
}

func benchmarkValueFetcherRetrieve(b *testing.B, valueSize int, ch *cache.Handle) {
	ctx := context.Background()
	opts := FileWriterOptions{}
	obj := &objstorage.MemObj{}
	w := NewFileWriter(000001, obj, opts)
	rng := rand.New(rand.NewPCG(0, 0))
	var handles []Handle
	for v := valueSize; v > 0; {
		n := testutils.RandIntInRange(rng, 1, 4096)
		h := w.AddValue(testutils.RandBytes(rng, n))
		handles = append(handles, h)
		v -= n
	}
	stats, err := w.Close()
	if err != nil {
		b.Fatal(err)
	}
	b.Logf("%d blocks, %d values", stats.BlockCount, stats.ValueCount)

	r, err := NewFileReader(ctx, obj, FileReaderOptions{
		ReaderOptions: block.ReaderOptions{
			CacheOpts: sstableinternal.CacheOptions{
				CacheHandle: ch,
				FileNum:     base.DiskFileNum(1),
			},
		},
	})
	require.NoError(b, err)
	rp := &mockReaderProvider{readers: map[base.DiskFileNum]*FileReader{000001: r}}

	b.Run("sequential", func(b *testing.B) {
		var fetcher ValueFetcher
		fetcher.Init(rp, block.ReadEnv{})
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			h := handles[i%len(handles)]
			_, err := fetcher.retrieve(ctx, h)
			if err != nil {
				b.Fatal(err)
			}
		}
	})
	b.Run("random", func(b *testing.B) {
		indices := make([]int, b.N)
		for i := 0; i < b.N; i++ {
			indices[i] = testutils.RandIntInRange(rng, 0, len(handles))
		}
		var fetcher ValueFetcher
		fetcher.Init(rp, block.ReadEnv{})
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			h := handles[indices[i]]
			_, err := fetcher.retrieve(ctx, h)
			if err != nil {
				b.Fatal(err)
			}
		}
	})
}
