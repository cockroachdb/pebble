// Copyright 2022 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package pebble

import (
	"bytes"
	"fmt"
	"math/rand/v2"
	"slices"
	"testing"

	"github.com/cockroachdb/datadriven"
	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/pebble/internal/blobtest"
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
	var bv blobtest.Values

	getOptsAndFiles := func(td *datadriven.TestData) (opts IterOptions, files [][]sstable.ReadableFile) {
		opts = IterOptions{KeyTypes: IterKeyTypePointsAndRanges}
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
		return opts, files
	}

	datadriven.RunTest(t, "testdata/external_iterator", func(t *testing.T, td *datadriven.TestData) string {
		switch td.Cmd {
		case "reset":
			mem = vfs.NewMem()
			return ""
		case "build":
			if err := runBuildCmd(td, d, mem, withBlobValues(&bv)); err != nil {
				return err.Error()
			}
			return ""
		case "iter-init-error":
			opts, files := getOptsAndFiles(td)
			testExternalIteratorInitError(t, o, &opts, files)
			return ""
		case "iter":
			opts, files := getOptsAndFiles(td)
			it, err := NewExternalIter(o, &opts, files)
			if err != nil {
				return fmt.Sprintf("error: %s", err.Error())
			}
			return runIterCmd(td, it, true /* close iter */)
		default:
			return fmt.Sprintf("unknown command: %s", td.Cmd)
		}
	})
}

// testExternalIteratorInitError tests error handling paths inside
// NewExternalIter by injecting errors when reading files.
//
// See github.com/cockroachdb/cockroach/issues/141606 where an error during
// initialization caused NewExternalIter to panic.
func testExternalIteratorInitError(
	t *testing.T, o *Options, iterOpts *IterOptions, files [][]sstable.ReadableFile,
) {
	files = slices.Clone(files)
	for i := range files {
		files[i] = slices.Clone(files[i])
		for j := range files[i] {
			files[i][j] = &flakyFile{ReadableFile: files[i][j]}
		}
	}

	for iter := 0; iter < 100; iter++ {
		it, err := NewExternalIter(o, iterOpts, files)
		if err != nil {
			require.Contains(t, err.Error(), "flaky file")
		} else {
			it.Close()
		}
	}
}

type flakyFile struct {
	sstable.ReadableFile
}

func (ff *flakyFile) ReadAt(p []byte, off int64) (n int, err error) {
	if rand.IntN(10) == 0 {
		return 0, errors.New("flaky file")
	}
	return ff.ReadableFile.ReadAt(p, off)
}

func (ff *flakyFile) Close() error { return nil }

func BenchmarkExternalIter_NonOverlapping_Scan(b *testing.B) {
	ks := testkeys.Alpha(6)
	opts := &Options{Comparer: testkeys.Comparer}
	opts.EnsureDefaults()
	iterOpts := &IterOptions{
		KeyTypes: IterKeyTypePointsAndRanges,
	}
	writeOpts := opts.MakeWriterOptions(6, sstable.TableFormatPebblev2)
	var valBuf [512]byte

	for _, keyCount := range []int{100, 10_000, 100_000} {
		b.Run(fmt.Sprintf("keys=%d", keyCount), func(b *testing.B) {
			for _, fileCount := range []int{1, 10, 100} {
				b.Run(fmt.Sprintf("files=%d", fileCount), func(b *testing.B) {
					prng := rand.New(rand.NewPCG(0, 0))

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
							for i := range valBuf {
								valBuf[i] = byte(prng.Uint32())
							}
							require.NoError(b, err)
							require.NoError(b, w.Set(key, valBuf[:]))
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
