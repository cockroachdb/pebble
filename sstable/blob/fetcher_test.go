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
	"time"

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
	"github.com/stretchr/testify/require"
)

// identityFileMapping is an implementation of base.BlobFileMapping that casts a
// BlobFileID to a DiskFileNum.
type identityFileMapping struct{}

// Assert that (identityFileMapping) implements base.BlobFileMapping.
var _ base.BlobFileMapping = identityFileMapping{}

func (identityFileMapping) Lookup(blobFileID base.BlobFileID) (base.ObjectInfo, bool) {
	return base.ObjectInfoLiteral{
		FileType:    base.FileTypeBlob,
		DiskFileNum: base.DiskFileNum(blobFileID),
		// TODO(jackson): Add bounds for blob files.
		Bounds: base.UserKeyBounds{},
	}, true
}

type mockReaderProvider struct {
	w       *bytes.Buffer
	readers map[base.DiskFileNum]*FileReader
	ch      *cache.Handle
	c       *cache.Cache
}

func (rp *mockReaderProvider) GetValueReader(
	ctx context.Context, obj base.ObjectInfo, _ block.InitFileReadStats,
) (r ValueReader, closeFunc func(), err error) {
	_, fileNum := obj.FileInfo()
	if rp.w != nil {
		fmt.Fprintf(rp.w, "# GetValueReader(%s)\n", fileNum)
	}
	vr, ok := rp.readers[fileNum]
	if !ok {
		return nil, nil, errors.Newf("no reader for file %s", fileNum)
	}
	return vr, func() {}, nil
}

func (rp *mockReaderProvider) Close() {
	if rp.ch != nil {
		rp.ch.Close()
	}
	if rp.c != nil {
		rp.c.Unref()
	}
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

	var handleBuf [MaxInlineHandleLength]byte
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
				fmt.Fprintf(&buf, "%-25s: %q\n", h, l)
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
			maxCachedReaders := 5
			td.MaybeScanArgs(t, "maxCachedReaders", &maxCachedReaders)
			fetchers[name] = &ValueFetcher{}
			fetchers[name].Init(identityFileMapping{}, rp, block.ReadEnv{}, maxCachedReaders)
			return ""
		case "fetch":
			var (
				name            string
				blobFileNum     uint64
				valLen, valueID uint32
				blockID         uint32
			)
			td.ScanArgs(t, "name", &name)
			td.ScanArgs(t, "filenum", &blobFileNum)
			td.ScanArgs(t, "valLen", &valLen)
			td.ScanArgs(t, "blockID", &blockID)
			td.ScanArgs(t, "valueID", &valueID)
			fetcher := fetchers[name]
			if fetcher == nil {
				t.Fatalf("fetcher %s not found", name)
			}

			n := HandleSuffix{
				BlockID: BlockID(blockID),
				ValueID: BlockValueID(valueID),
			}.Encode(handleBuf[:])
			encodedHandleSuffix := handleBuf[:n]

			val, _, err := fetcher.FetchHandle(ctx, encodedHandleSuffix, base.BlobFileID(blobFileNum), valLen, nil)
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

func writeValueFetcherState(w *bytes.Buffer, f *ValueFetcher) {
	fmt.Fprintf(w, "ValueFetcher{\n")
	for _, cr := range f.readers {
		if cr.r == nil {
			fmt.Fprintln(w, "  empty")
			continue
		}
		fmt.Fprintf(w, "  %s (blk%d)\n", cr.diskFileNum, cr.currentValueBlock.physicalIndex)
	}
	fmt.Fprintf(w, "}\n")
}

func TestValueFetcherRetrieveRandomized(t *testing.T) {
	defer leaktest.AfterTest(t)()
	seed := uint64(time.Now().UnixNano())
	t.Logf("seed: %d", seed)

	ctx := context.Background()
	opts := FileWriterOptions{}
	obj := &objstorage.MemObj{}
	w := NewFileWriter(1, obj, opts)
	rng := rand.New(rand.NewPCG(seed, seed))
	var handles []Handle
	var values [][]byte
	for v := 4 << 20; v > 0; {
		n := testutils.RandIntInRange(rng, 1, 4096)
		val := testutils.RandBytes(rng, n)
		h := w.AddValue(val)
		handles = append(handles, h)
		values = append(values, val)
		v -= n
	}
	_, err := w.Close()
	require.NoError(t, err)

	r, err := NewFileReader(ctx, obj, FileReaderOptions{
		ReaderOptions: block.ReaderOptions{
			CacheOpts: sstableinternal.CacheOptions{
				FileNum: base.DiskFileNum(1),
			},
		},
	})
	require.NoError(t, err)
	rp := &mockReaderProvider{readers: map[base.DiskFileNum]*FileReader{1: r}}

	t.Run("sequential", func(t *testing.T) {
		var fetcher ValueFetcher
		fetcher.Init(identityFileMapping{}, rp, block.ReadEnv{}, 5)
		defer fetcher.Close()
		for i := 0; i < len(handles); i++ {
			val, err := fetcher.retrieve(ctx, handles[i])
			require.NoError(t, err)
			require.Equal(t, values[i], val)
		}
	})
	t.Run("random", func(t *testing.T) {
		var fetcher ValueFetcher
		fetcher.Init(identityFileMapping{}, rp, block.ReadEnv{}, 5)
		defer fetcher.Close()
		for _, i := range rng.Perm(len(handles)) {
			val, err := fetcher.retrieve(ctx, handles[i])
			require.NoError(t, err)
			require.Equal(t, values[i], val)
		}
	})
}

func BenchmarkValueFetcherRetrieve(b *testing.B) {
	defer leaktest.AfterTest(b)()
	b.Run("uncached", func(b *testing.B) {
		b.Run("2MiB", func(b *testing.B) {
			benchmarkValueFetcherRetrieve(b, 2<<20, 0)
		})
		b.Run("16MiB", func(b *testing.B) {
			benchmarkValueFetcherRetrieve(b, 16<<20, 0)
		})
		b.Run("64MiB", func(b *testing.B) {
			benchmarkValueFetcherRetrieve(b, 64<<20, 0)
		})
	})
	// The cached variants use a cache double the size of the files'
	// uncompressed value size to ensure all blocks fit in the cache. Each run
	// uses a fresh cache to ensure there's no interference between variants.
	b.Run("cached", func(b *testing.B) {
		b.Run("2MiB", func(b *testing.B) {
			benchmarkValueFetcherRetrieve(b, 2<<20, 4<<20)
		})
		b.Run("16MiB", func(b *testing.B) {
			benchmarkValueFetcherRetrieve(b, 16<<20, 32<<20)
		})
		b.Run("64MiB", func(b *testing.B) {
			benchmarkValueFetcherRetrieve(b, 64<<20, 128<<20)
		})
	})
}

func benchmarkValueFetcherRetrieve(b *testing.B, valueSize int, cacheSize int64) {
	ctx := context.Background()
	opts := FileWriterOptions{}
	obj := &objstorage.MemObj{}
	w := NewFileWriter(1, obj, opts)
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

	b.Run("sequential", func(b *testing.B) {
		rp := makeMockReaderProvider(b, obj, cacheSize, handles)
		defer rp.Close()
		var fetcher ValueFetcher
		fetcher.Init(identityFileMapping{}, rp, block.ReadEnv{}, 5)
		defer fetcher.Close()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			h := handles[i%len(handles)]
			_, err := fetcher.retrieve(ctx, h)
			if err != nil {
				b.Fatal(err)
			}
		}
		b.StopTimer()
	})
	b.Run("random", func(b *testing.B) {
		rp := makeMockReaderProvider(b, obj, cacheSize, handles)
		defer rp.Close()
		indices := make([]int, b.N)
		for i := 0; i < b.N; i++ {
			indices[i] = testutils.RandIntInRange(rng, 0, len(handles))
		}
		var fetcher ValueFetcher
		fetcher.Init(identityFileMapping{}, rp, block.ReadEnv{}, 5)
		defer fetcher.Close()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			h := handles[indices[i]]
			_, err := fetcher.retrieve(ctx, h)
			if err != nil {
				b.Fatal(err)
			}
		}
		b.StopTimer()
	})
}

func makeMockReaderProvider(
	tb testing.TB, obj objstorage.Readable, cacheSize int64, handles []Handle,
) *mockReaderProvider {
	ctx := context.Background()
	rp := &mockReaderProvider{readers: map[base.DiskFileNum]*FileReader{}}
	if cacheSize > 0 {
		rp.c = cache.NewWithShards(cacheSize, 1)
		rp.ch = rp.c.NewHandle()
	}
	r, err := NewFileReader(ctx, obj, FileReaderOptions{
		ReaderOptions: block.ReaderOptions{
			CacheOpts: sstableinternal.CacheOptions{
				CacheHandle: rp.ch,
				FileNum:     base.DiskFileNum(1),
			},
		},
	})
	require.NoError(tb, err)
	rp.readers[1] = r

	// If configured with a cache, populate the cache by retrieving all the
	// blocks.
	if cacheSize > 0 {
		var fetcher ValueFetcher
		fetcher.Init(identityFileMapping{}, rp, block.ReadEnv{}, 5)
		defer fetcher.Close()
		for i, h := range handles {
			if i > 0 && handles[i-1].BlockID == h.BlockID {
				// Already retrieved this block.
				continue
			}
			_, err := fetcher.retrieve(ctx, h)
			require.NoError(tb, err)
		}
	}
	return rp
}
