// Copyright 2025 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package blob

import (
	"bytes"
	"context"
	"fmt"
	"testing"

	"github.com/cockroachdb/crlib/crstrings"
	"github.com/cockroachdb/datadriven"
	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/pebble/internal/base"
	"github.com/cockroachdb/pebble/internal/sstableinternal"
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
	ctx context.Context, it any, fileNum base.DiskFileNum,
) (r ValueReader, closeFunc func(any), err error) {
	fmt.Fprintf(rp.w, "# GetValueReader(%s)\n", fileNum)

	vr, ok := rp.readers[fileNum]
	if !ok {
		return nil, nil, errors.Newf("no reader for file %s", fileNum)
	}
	return vr, func(any) {}, nil
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
