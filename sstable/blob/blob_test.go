// Copyright 2024 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package blob

import (
	"bytes"
	"context"
	"fmt"
	"testing"

	"github.com/cockroachdb/crlib/crstrings"
	"github.com/cockroachdb/crlib/testutils/leaktest"
	"github.com/cockroachdb/datadriven"
	"github.com/cockroachdb/pebble/objstorage"
	"github.com/cockroachdb/pebble/sstable/block"
)

func TestBlobWriter(t *testing.T) {
	defer leaktest.AfterTest(t)()
	var buf bytes.Buffer
	var obj *objstorage.MemObj
	datadriven.RunTest(t, "testdata/writer", func(t *testing.T, td *datadriven.TestData) string {
		buf.Reset()
		switch td.Cmd {
		case "build":
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

			obj = &objstorage.MemObj{}
			w := NewFileWriter(000001, obj, FileWriterOptions{
				Compression:   compression,
				ChecksumType:  block.ChecksumTypeCRC32c,
				FlushGovernor: block.MakeFlushGovernor(targetBlockSize, blockSizeThreshold, 0, nil),
			})
			for _, l := range crstrings.Lines(td.Input) {
				h := w.AddValue([]byte(l))
				fmt.Fprintln(&buf, h)
			}
			stats, err := w.Close()
			if err != nil {
				t.Fatal(err)
			}
			fmt.Fprintf(&buf, "Stats:\n")
			fmt.Fprintf(&buf, "  BlockCount: %d\n", stats.BlockCount)
			fmt.Fprintf(&buf, "  ValueCount: %d\n", stats.ValueCount)
			fmt.Fprintf(&buf, "  BlockLenLongest: %d\n", stats.BlockLenLongest)
			fmt.Fprintf(&buf, "  UncompressedValueBytes: %d\n", stats.UncompressedValueBytes)
			fmt.Fprintf(&buf, "  FileLen: %d\n", stats.FileLen)
			return buf.String()
		case "open":
			r, err := NewFileReader(context.Background(), obj)
			if err != nil {
				t.Fatal(err)
			}
			fmt.Fprintf(&buf, "TableFormat: %s\n", r.footer.format)
			fmt.Fprintf(&buf, "ChecksumType: %s\n", r.footer.checksum)
			fmt.Fprintf(&buf, "IndexHandle: %s\n", r.footer.indexHandle.String())
			return buf.String()
		default:
			panic(fmt.Sprintf("unknown command: %s", td.Cmd))
		}
	})
}
