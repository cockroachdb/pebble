// Copyright 2024 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package sstable

import (
	"bytes"
	"context"
	"fmt"
	"testing"

	"github.com/cockroachdb/datadriven"
	"github.com/cockroachdb/pebble/internal/aligned"
	"github.com/cockroachdb/pebble/internal/binfmt"
	"github.com/cockroachdb/pebble/internal/testkeys"
	"github.com/cockroachdb/pebble/objstorage"
	"github.com/cockroachdb/pebble/sstable/block"
	"github.com/cockroachdb/pebble/sstable/colblk"
	"github.com/stretchr/testify/require"
)

func TestColumnarWriter(t *testing.T) {
	var meta *WriterMetadata
	var obj *objstorage.MemObj
	var r *Reader
	defer func() {
		if r != nil {
			require.NoError(t, r.Close())
			r = nil
		}
	}()
	keySchema := colblk.DefaultKeySchema(testkeys.Comparer, 16)
	var buf bytes.Buffer
	datadriven.Walk(t, "testdata/columnar_writer", func(t *testing.T, path string) {
		datadriven.RunTest(t, path, func(t *testing.T, td *datadriven.TestData) string {
			buf.Reset()
			switch td.Cmd {
			case "build":
				var writerOpts WriterOptions
				writerOpts.Comparer = testkeys.Comparer
				writerOpts.Compression = block.NoCompression
				writerOpts.TableFormat = TableFormatPebblev5
				writerOpts.KeySchema = keySchema
				writerOpts.BlockPropertyCollectors = []func() BlockPropertyCollector{NewTestKeysBlockPropertyCollector}
				if err := optsFromArgs(td, &writerOpts); err != nil {
					require.NoError(t, err)
				}
				var err error
				meta, obj, err = runBuildMemObjCmd(td, &writerOpts)
				if err != nil {
					return fmt.Sprintf("error: %s", err)
				}
				return formatWriterMetadata(td, meta)
			case "open":
				if r != nil {
					require.NoError(t, r.Close())
					r = nil
				}
				var err error
				r, err = NewReader(context.Background(), obj, ReaderOptions{
					Comparer:  testkeys.Comparer,
					KeySchema: keySchema,
				})
				require.NoError(t, err)
				return "ok"
			case "layout":
				l, err := r.Layout()
				if err != nil {
					return err.Error()
				}
				l.Describe(&buf, true /* verbose */, r, nil)
				return buf.String()
			case "props":
				return r.Properties.String()
			case "describe-binary":
				f := binfmt.New(obj.Data())
				if err := describeSSTableBinary(f, keySchema); err != nil {
					return err.Error()
				}
				return f.String()
			default:
				panic(fmt.Sprintf("unrecognized command %q", td.Cmd))
			}
		})
	})
}

func describeSSTableBinary(f *binfmt.Formatter, schema colblk.KeySchema) error {
	layout, err := decodeLayout(testkeys.Comparer, f.Data())
	if err != nil {
		return err
	}
	blockHandles := layout.orderedBlocks()
	for i, bh := range blockHandles {
		if f.Offset() < int(bh.Offset) {
			f.HexBytesln(int(bh.Offset)-f.Offset(), "???")
		}
		if bh.Name == "footer" {
			// Skip the footer; we'll format it down below.
			continue
		}
		f.CommentLine("block %d %s (%04d-%04d)", i, bh.Name, bh.Offset, bh.Offset+bh.Length)

		if layout.Format >= TableFormatPebblev5 {
			// We can only describe uncompressed data.
			if block.CompressionIndicator(f.Data()[bh.Offset+bh.Length]) == block.NoCompressionIndicator {
				switch bh.Name {
				case "top-index", "index":
					var d colblk.IndexBlockDecoder
					// NB: The byte slice used to Init must be aligned (like an
					// allocated block would be in practice).
					d.Init(aligned.Copy(f.Data()[bh.Offset : bh.Offset+bh.Length]))
					d.Describe(f)
					f.HexBytesln(block.TrailerLen, "%s block trailer", bh.Name)
					continue
				case "data":
					var d colblk.DataBlockDecoder
					// NB: The byte slice used to Init must be aligned (like an
					// allocated block would be in practice).
					d.Init(schema, aligned.Copy(f.Data()[bh.Offset:bh.Offset+bh.Length]))
					d.Describe(f)
					f.HexBytesln(block.TrailerLen, "%s block trailer", bh.Name)
					continue
				case "range-del", "range-key":
					var d colblk.KeyspanDecoder
					d.Init(aligned.Copy(f.Data()[bh.Offset : bh.Offset+bh.Length]))
					d.Describe(f)
					f.HexBytesln(block.TrailerLen, "%s block trailer", bh.Name)
					continue
				case "properties":
					f.HexTextln(int(bh.Length))
					f.HexBytesln(block.TrailerLen, "%s block trailer", bh.Name)
					continue
				}
				// Otherwise fall through.
			}
		}
		// Fall back to just formatting the entire block as an opaque hex bytes.
		f.HexBytesln(int(bh.Length), bh.Name)
		f.HexBytesln(block.TrailerLen, "%s block trailer", bh.Name)
	}
	if f.Remaining() > rocksDBFooterLen {
		f.HexBytesln(f.Remaining()-rocksDBFooterLen, "???")
	}
	describeFooter(f)
	return nil
}
