// Copyright 2024 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package sstable

import (
	"context"
	"fmt"
	"strconv"
	"strings"
	"testing"

	"github.com/cockroachdb/datadriven"
	"github.com/cockroachdb/pebble/internal/base"
	"github.com/cockroachdb/pebble/internal/cache"
	"github.com/cockroachdb/pebble/internal/sstableinternal"
	"github.com/cockroachdb/pebble/internal/testkeys"
	"github.com/cockroachdb/pebble/objstorage/objstorageprovider"
	"github.com/cockroachdb/pebble/sstable/block"
	"github.com/cockroachdb/pebble/sstable/colblk"
	"github.com/cockroachdb/pebble/vfs"
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
			blockSize := 1
			var indexBlockSize int
			for i := range d.CmdArgs[1:] {
				switch d.CmdArgs[i+1].Key {
				case "format":
					switch d.CmdArgs[i+1].Vals[0] {
					case "pebblev4":
						tableFormat = TableFormatPebblev4
					case "pebblev5":
						tableFormat = TableFormatPebblev5
					case "pebblev6":
						tableFormat = TableFormatPebblev6
					}
				case "block_size":
					var err error
					blockSize, err = strconv.Atoi(d.CmdArgs[i+1].FirstVal(t))
					if err != nil {
						return err.Error()
					}
				case "index_block_size":
					var err error
					indexBlockSize, err = strconv.Atoi(d.CmdArgs[i+1].FirstVal(t))
					if err != nil {
						return err.Error()
					}
				}
			}
			w := NewWriter(objstorageprovider.NewFileWritable(f), WriterOptions{
				BlockSize:      blockSize,
				IndexBlockSize: indexBlockSize,
				TableFormat:    tableFormat,
				Comparer:       testkeys.Comparer,
				KeySchema:      &keySchema,
			})
			for _, key := range strings.Split(d.Input, "\n") {
				j := strings.Index(key, ":")
				ikey := base.ParseInternalKey(key[:j])
				value := []byte(key[j+1:])
				if err := w.Set(ikey.UserKey, value); err != nil {
					return err.Error()
				}
			}
			if err := w.Close(); err != nil {
				return err.Error()
			}

			return ""

		case "iter":
			// Iterate over the specified sstable
			f, err := fs.Open(d.CmdArgs[0].Key)
			if err != nil {
				return err.Error()
			}
			readable, err := NewSimpleReadable(f)
			if err != nil {
				return err.Error()
			}
			var start, end []byte
			for _, arg := range d.CmdArgs[1:] {
				switch arg.Key {
				case "start":
					start = []byte(arg.FirstVal(t))
				case "end":
					end = []byte(arg.FirstVal(t))
				}
			}
			rOpts := ReaderOptions{
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
			defer r.Close()
			if err != nil {
				return err.Error()
			}
			iter, err := r.NewIter(block.NoTransforms, start, end, AssertNoBlobHandles)
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
			if len(d.CmdArgs) != 4 {
				t.Fatalf("expected input sstable, output sstable, start and end keys")
			}

			inputFile := d.CmdArgs[0].Key
			outputFile := d.CmdArgs[1].Key
			start := base.ParseInternalKey(d.CmdArgs[2].String())
			end := base.ParseInternalKey(d.CmdArgs[3].String())
			output, err := fs.Create(outputFile, vfs.WriteCategoryUnspecified)
			if err != nil {
				return err.Error()
			}
			writable := objstorageprovider.NewFileWritable(output)
			fileNameToNum[outputFile] = nextFileNum
			nextFileNum++

			f, err := fs.Open(inputFile)
			if err != nil {
				t.Fatalf("failed to open sstable: %v", err)
			}
			readable, err := NewSimpleReadable(f)
			if err != nil {
				return err.Error()
			}
			rOpts := ReaderOptions{
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
				return err.Error()
			}
			defer r.Close()
			wOpts := WriterOptions{
				Comparer:  testkeys.Comparer,
				KeySchema: &keySchema,
			}
			// CopySpan closes readable but not reader. We need to open a new readable for it.
			f2, err := fs.Open(inputFile)
			if err != nil {
				t.Fatalf("failed to open sstable: %v", err)
			}
			readable2, err := NewSimpleReadable(f2)
			if err != nil {
				return err.Error()
			}
			size, err := CopySpan(context.TODO(), readable2, r, rOpts, writable, wOpts, start, end)
			if err != nil {
				return err.Error()
			}
			return fmt.Sprintf("copied %d bytes", size)

		case "describe":
			f, err := fs.Open(d.CmdArgs[0].Key)
			if err != nil {
				return err.Error()
			}
			readable, err := NewSimpleReadable(f)
			if err != nil {
				return err.Error()
			}
			r, err := NewReader(context.TODO(), readable, ReaderOptions{
				Comparer:   testkeys.Comparer,
				KeySchemas: KeySchemas{keySchema.Name: &keySchema},
			})
			if err != nil {
				return err.Error()
			}
			defer r.Close()
			l, err := r.Layout()
			if err != nil {
				return err.Error()
			}
			return l.Describe(true, r, nil)

		default:
			t.Fatalf("unknown command: %s", d.Cmd)
			return ""
		}
	})
}
