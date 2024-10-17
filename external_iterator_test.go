// Copyright 2022 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package pebble

import (
	"bytes"
	"fmt"
	"testing"

	"github.com/cockroachdb/datadriven"
	"github.com/cockroachdb/pebble/internal/testkeys"
	"github.com/cockroachdb/pebble/objstorage/objstorageprovider"
	"github.com/cockroachdb/pebble/sstable"
	"github.com/cockroachdb/pebble/vfs"
	"github.com/stretchr/testify/require"
)

func TestExternalIterator(t *testing.T) {
	mem := vfs.NewMem()
	o := &Options{
		FS:                 mem,
		FormatMajorVersion: internalFormatNewest,
		Comparer:           testkeys.Comparer,
	}
	o.Experimental.EnableColumnarBlocks = func() bool { return true }
	o.testingRandomized(t)
	o.EnsureDefaults()
	d, err := Open("", o)
	require.NoError(t, err)
	defer func() { require.NoError(t, d.Close()) }()

	datadriven.RunTest(t, "testdata/external_iterator", func(t *testing.T, td *datadriven.TestData) string {
		switch td.Cmd {
		case "reset":
			mem = vfs.NewMem()
			return ""
		case "build":
			if err := runBuildCmd(td, d, mem); err != nil {
				return err.Error()
			}
			return ""
		case "iter":
			opts := IterOptions{KeyTypes: IterKeyTypePointsAndRanges}
			var files [][]sstable.ReadableFile
			for _, arg := range td.CmdArgs {
				switch arg.Key {
				case "mask-suffix":
					opts.RangeKeyMasking.Suffix = []byte(arg.Vals[0])
				case "lower":
					opts.LowerBound = []byte(arg.Vals[0])
				case "upper":
					opts.UpperBound = []byte(arg.Vals[0])
				case "files":
					for _, v := range arg.Vals {
						f, err := mem.Open(v)
						require.NoError(t, err)
						files = append(files, []sstable.ReadableFile{f})
					}
				}
			}
			it, err := NewExternalIter(o, &opts, files)
			require.NoError(t, err)
			return runIterCmd(td, it, true /* close iter */)
		default:
			return fmt.Sprintf("unknown command: %s", td.Cmd)
		}
	})
}

func BenchmarkExternalIter_NonOverlapping_Scan(b *testing.B) {
	ks := testkeys.Alpha(6)
	opts := (&Options{Comparer: testkeys.Comparer}).EnsureDefaults()
	iterOpts := &IterOptions{
		KeyTypes: IterKeyTypePointsAndRanges,
	}
	writeOpts := opts.MakeWriterOptions(6, sstable.TableFormatPebblev2)

	for _, keyCount := range []int{100, 10_000, 100_000} {
		b.Run(fmt.Sprintf("keys=%d", keyCount), func(b *testing.B) {
			for _, fileCount := range []int{1, 10, 100} {
				b.Run(fmt.Sprintf("files=%d", fileCount), func(b *testing.B) {
					var fs vfs.FS = vfs.NewMem()
					filenames := make([]string, fileCount)
					var keys [][]byte
					for i := 0; i < fileCount; i++ {
						filename := fmt.Sprintf("%03d.sst", i)
						wf, err := fs.Create(filename, vfs.WriteCategoryUnspecified)
						require.NoError(b, err)
						w := sstable.NewWriter(objstorageprovider.NewFileWritable(wf), writeOpts)
						for j := 0; j < keyCount/fileCount; j++ {
							key := testkeys.Key(ks, int64(len(keys)))
							keys = append(keys, key)
							require.NoError(b, w.Set(key, key))
						}
						require.NoError(b, w.Close())
						filenames[i] = filename
					}

					b.ResetTimer()
					for i := 0; i < b.N; i++ {
						func() {
							files := make([][]sstable.ReadableFile, fileCount)
							for i := 0; i < fileCount; i++ {
								f, err := fs.Open(filenames[i])
								require.NoError(b, err)
								files[i] = []sstable.ReadableFile{f}
							}

							it, err := NewExternalIter(opts, iterOpts, files)
							require.NoError(b, err)
							defer it.Close()

							k := 0
							for valid := it.First(); valid; valid = it.NextPrefix() {
								if !bytes.Equal(it.Key(), keys[k]) {
									b.Fatalf("expected key %q, found %q", keys[k+1], it.Key())
								}
								k++
							}
							if k != len(keys) {
								b.Fatalf("k=%d, expected %d\n", k, len(keys))
							}
						}()
					}
				})
			}
		})
	}
}
