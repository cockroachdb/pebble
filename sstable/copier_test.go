// Copyright 2024 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package sstable

import (
	"context"
	"fmt"
	"strings"
	"testing"

	"github.com/cockroachdb/datadriven"
	"github.com/cockroachdb/pebble/internal/base"
	"github.com/cockroachdb/pebble/internal/cache"
	"github.com/cockroachdb/pebble/internal/keyspan"
	"github.com/cockroachdb/pebble/internal/sstableinternal"
	"github.com/cockroachdb/pebble/internal/testkeys"
	"github.com/cockroachdb/pebble/objstorage"
	"github.com/cockroachdb/pebble/objstorage/objstorageprovider"
	"github.com/cockroachdb/pebble/sstable/block"
	"github.com/cockroachdb/pebble/sstable/colblk"
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

// TestCopySpanMaxUserKeySize verifies that a table produced by CopySpan carries
// over the MaxUserKeySize property of the source table (which is an upper
// bound for the copied keys), and that the output lacks the property if the
// source lacks it.
func TestCopySpanMaxUserKeySize(t *testing.T) {
	blockCache := cache.New(1 << 20 /* 1 MB */)
	defer blockCache.Unref()
	cacheHandle := blockCache.NewHandle()
	defer cacheHandle.Close()
	nextFileNum := base.DiskFileNum(1)

	longKey := strings.Repeat("b", 20) + "@1"
	readerOpts := ReaderOptions{
		Comparer:   testkeys.Comparer,
		KeySchemas: KeySchemas{testkeysSchema.Name: &testkeysSchema},
	}
	testCases := []struct {
		name string
		// rangeDel adds a range deletion to the source table, which causes
		// CopySpan to copy the entire file.
		rangeDel bool
		// sourceLacksProp simulates a source table written by a Pebble version
		// that did not record the property.
		sourceLacksProp bool
	}{
		{name: "copy-blocks"},
		{name: "copy-blocks-source-lacks-prop", sourceLacksProp: true},
		{name: "copy-whole-file", rangeDel: true},
		{name: "copy-whole-file-source-lacks-prop", rangeDel: true, sourceLacksProp: true},
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			for tf := TableFormatPebblev1; tf <= TableFormatMax; tf++ {
				t.Run(tf.String(), func(t *testing.T) {
					obj := &objstorage.MemObj{}
					w := NewRawWriter(obj, WriterOptions{
						BlockSize:      1,
						IndexBlockSize: 1,
						TableFormat:    tf,
						Comparer:       testkeys.Comparer,
						KeySchema:      &testkeysSchema,
					})
					for _, k := range []string{"a@1", longKey, "c@1", "d@1", "e@1"} {
						require.NoError(t, w.Add(
							base.MakeInternalKey([]byte(k), 1, base.InternalKeyKindSet), []byte("val"),
							false /* forceObsolete */))
					}
					if tc.rangeDel {
						require.NoError(t, w.EncodeSpan(keyspan.Span{
							Start: []byte("x"),
							End:   []byte("y"),
							Keys:  []keyspan.Key{{Trailer: base.MakeTrailer(2, base.InternalKeyKindRangeDelete)}},
						}))
					}
					if tc.sourceLacksProp {
						clearMaxUserKeySize(t, w)
					}
					require.NoError(t, w.Close())

					rOpts := readerOpts
					rOpts.CacheOpts = sstableinternal.CacheOptions{
						CacheHandle: cacheHandle,
						FileNum:     nextFileNum,
					}
					nextFileNum++
					r, err := NewMemReader(obj.Data(), rOpts)
					require.NoError(t, err)
					defer func() { require.NoError(t, r.Close()) }()
					srcProp, srcPresent := readMaxUserKeySizeProp(t, r)
					require.Equal(t, !tc.sourceLacksProp, srcPresent)
					if srcPresent {
						require.Equal(t, uint64(len(longKey)), srcProp)
					}

					output := &objstorage.MemObj{}
					_, err = CopySpan(context.Background(), newMemReader(obj.Data()), r, rOpts,
						output, WriterOptions{Comparer: testkeys.Comparer, KeySchema: &testkeysSchema},
						base.MakeInternalKey([]byte("c@1"), base.SeqNumMax, base.InternalKeyKindMax),
						base.MakeInternalKey([]byte("d@1"), 0, base.InternalKeyKindSet))
					require.NoError(t, err)

					outReader, err := NewMemReader(output.Data(), readerOpts)
					require.NoError(t, err)
					defer func() { require.NoError(t, outReader.Close()) }()
					if !tc.rangeDel {
						// The output does not contain the longest key, so the
						// property is a strict upper bound.
						require.Less(t, maxUserKeySizeFromContents(t, outReader), uint64(len(longKey)))
					} else {
						require.Equal(t, obj.Data(), output.Data())
					}
					got, present := readMaxUserKeySizeProp(t, outReader)
					require.Equal(t, srcPresent, present)
					require.Equal(t, srcProp, got)
				})
			}
		})
	}
}
