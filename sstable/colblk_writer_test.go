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
				writerOpts.KeySchema = &keySchema
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
					Comparer:   testkeys.Comparer,
					KeySchemas: KeySchemas{keySchema.Name: &keySchema},
				})
				require.NoError(t, err)
				return "ok"
			case "layout":
				l, err := r.Layout()
				if err != nil {
					return err.Error()
				}
				return l.Describe(true /* verbose */, r, nil /* fmtKV */)
			case "props":
				props, err := r.ReadPropertiesBlock(context.Background(), nil)
				if err != nil {
					return err.Error()
				}
				return props.String()
			default:
				panic(fmt.Sprintf("unrecognized command %q", td.Cmd))
			}
		})
	})
}
