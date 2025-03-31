// Copyright 2024 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package blob

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"testing"

	"github.com/cockroachdb/crlib/crstrings"
	"github.com/cockroachdb/crlib/testutils/leaktest"
	"github.com/cockroachdb/datadriven"
	"github.com/cockroachdb/pebble/objstorage"
	"github.com/cockroachdb/pebble/sstable/block"
	"github.com/stretchr/testify/require"
)

func TestBlobWriter(t *testing.T) {
	defer leaktest.AfterTest(t)()
	var buf bytes.Buffer
	var obj *objstorage.MemObj
	datadriven.RunTest(t, "testdata/writer", func(t *testing.T, td *datadriven.TestData) string {
		buf.Reset()
		switch td.Cmd {
		case "build":
			opts := scanFileWriterOptions(t, td)
			obj = &objstorage.MemObj{}
			w := NewFileWriter(000001, obj, opts)
			for _, l := range crstrings.Lines(td.Input) {
				h := w.AddValue([]byte(l))
				fmt.Fprintln(&buf, h)
			}
			stats, err := w.Close()
			if err != nil {
				t.Fatal(err)
			}
			printFileWriterStats(&buf, stats)
			return buf.String()
		case "open":
			r, err := NewFileReader(context.Background(), obj, FileReaderOptions{})
			if err != nil {
				t.Fatal(err)
			}
			defer r.Close()
			fmt.Fprintf(&buf, "TableFormat: %s\n", r.footer.format)
			fmt.Fprintf(&buf, "ChecksumType: %s\n", r.footer.checksum)
			fmt.Fprintf(&buf, "IndexHandle: %s\n", r.footer.indexHandle.String())
			return buf.String()
		default:
			panic(fmt.Sprintf("unknown command: %s", td.Cmd))
		}
	})
}

func scanFileWriterOptions(t *testing.T, td *datadriven.TestData) FileWriterOptions {
	var (
		targetBlockSize    int = 128
		blockSizeThreshold int = 90
		compression            = block.NoCompression
	)
	td.MaybeScanArgs(t, "target-block-size", &targetBlockSize)
	td.MaybeScanArgs(t, "block-size-threshold", &blockSizeThreshold)
	if cmdArg, ok := td.Arg("compression"); ok {
		compression = block.CompressionFromString(cmdArg.SingleVal(t))
	}
	return FileWriterOptions{
		Compression:   compression,
		ChecksumType:  block.ChecksumTypeCRC32c,
		FlushGovernor: block.MakeFlushGovernor(targetBlockSize, blockSizeThreshold, 0, nil),
	}
}

func printFileWriterStats(w io.Writer, stats FileWriterStats) {
	fmt.Fprintf(w, "Stats:\n")
	fmt.Fprintf(w, "  BlockCount: %d\n", stats.BlockCount)
	fmt.Fprintf(w, "  ValueCount: %d\n", stats.ValueCount)
	fmt.Fprintf(w, "  BlockLenLongest: %d\n", stats.BlockLenLongest)
	fmt.Fprintf(w, "  UncompressedValueBytes: %d\n", stats.UncompressedValueBytes)
	fmt.Fprintf(w, "  FileLen: %d\n", stats.FileLen)
}

func TestHandleRoundtrip(t *testing.T) {
	handles := []InlineHandle{
		{
			InlineHandlePreface: InlineHandlePreface{
				ReferenceID: 0,
				ValueLen:    29357353,
			},
			HandleSuffix: HandleSuffix{
				BlockNum:      194,
				OffsetInBlock: 32911,
			},
		},
		{
			InlineHandlePreface: InlineHandlePreface{
				ReferenceID: 129,
				ValueLen:    205,
			},
			HandleSuffix: HandleSuffix{
				BlockNum:      2,
				OffsetInBlock: 20,
			},
		},
	}

	for _, h := range handles {
		var buf [MaxInlineHandleLength]byte
		n := h.Encode(buf[:])
		preface, rem := DecodeInlineHandlePreface(buf[:n])
		suffix := DecodeHandleSuffix(rem)
		require.Equal(t, h.InlineHandlePreface, preface)
		require.Equal(t, h.HandleSuffix, suffix)
	}
}
