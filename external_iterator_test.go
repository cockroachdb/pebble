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

	"github.com/cockroachdb/crlib/testutils/leaktest"
	"github.com/cockroachdb/datadriven"
	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/pebble/internal/base"
	"github.com/cockroachdb/pebble/internal/blobtest"
	"github.com/cockroachdb/pebble/internal/testkeys"
	"github.com/cockroachdb/pebble/objstorage"
	"github.com/cockroachdb/pebble/objstorage/objstorageprovider"
	"github.com/cockroachdb/pebble/sstable"
	"github.com/cockroachdb/pebble/vfs"
	"github.com/stretchr/testify/require"
)

func TestExternalIterator(t *testing.T) {
	defer leaktest.AfterTest(t)()
	mem := vfs.NewMem()
	o := &Options{
		FS:                 mem,
		FormatMajorVersion: internalFormatNewest,
		Comparer:           testkeys.Comparer,
	}
	o.randomizeForTesting(t)
	o.EnsureDefaults()
	d, err := Open("", o)
	require.NoError(t, err)
	defer func() { require.NoError(t, d.Close()) }()
	var bv blobtest.Values

	getOptsAndFiles := func(td *datadriven.TestData) (opts IterOptions, files [][]objstorage.ReadableFile) {
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
					files = append(files, []objstorage.ReadableFile{f})
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
	t *testing.T, o *Options, iterOpts *IterOptions, files [][]objstorage.ReadableFile,
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
	objstorage.ReadableFile
}

func (ff *flakyFile) ReadAt(p []byte, off int64) (n int, err error) {
	if rand.IntN(10) == 0 {
		return 0, errors.New("flaky file")
	}
	return ff.ReadableFile.ReadAt(p, off)
}

func (ff *flakyFile) Close() error { return nil }

// TestExternalIteratorSyntheticSeqNums verifies that each file opened by
// NewExternalIter receives the same non-zero SyntheticSeqNum for its point
// iterator and its range-deletion iterator (descending from the total file
// count down to 1). Regression test for #5617, where decrementing seqNum
// before constructing the range-deletion iterator gave each file's range
// deletions seqNum-1 and left the last file with NoSyntheticSeqNum (0).
func TestExternalIteratorSyntheticSeqNums(t *testing.T) {
	defer leaktest.AfterTest(t)()

	for _, tableFormat := range []sstable.TableFormat{
		sstable.TableFormatPebblev4,
		sstable.TableFormatMax,
	} {
		t.Run(tableFormat.String(), func(t *testing.T) {
			mem := vfs.NewMem()
			opts := &Options{
				FS:       mem,
				Comparer: testkeys.Comparer,
			}
			opts.EnsureDefaults()
			writeOpts := opts.MakeWriterOptions(6, tableFormat)

			buildSST := func(name string, fn func(w *sstable.Writer)) {
				wf, err := mem.Create(name, vfs.WriteCategoryUnspecified)
				require.NoError(t, err)
				w := sstable.NewWriter(objstorageprovider.NewFileWritable(wf), writeOpts)
				fn(w)
				require.NoError(t, w.Close())
			}

			openSST := func(name string) objstorage.ReadableFile {
				f, err := mem.Open(name)
				require.NoError(t, err)
				return f
			}

			buildSST("f0.sst", func(w *sstable.Writer) {
				require.NoError(t, w.Set([]byte("a"), []byte("v0")))
				require.NoError(t, w.DeleteRange([]byte("a"), []byte("c")))
				require.NoError(t, w.RangeKeySet([]byte("a"), []byte("c"), []byte("@1"), []byte("rk0")))
			})
			buildSST("f1.sst", func(w *sstable.Writer) {
				require.NoError(t, w.Set([]byte("d"), []byte("v1")))
				require.NoError(t, w.DeleteRange([]byte("d"), []byte("f")))
				require.NoError(t, w.RangeKeySet([]byte("d"), []byte("f"), []byte("@1"), []byte("rk1")))
			})
			buildSST("f2.sst", func(w *sstable.Writer) {
				require.NoError(t, w.Set([]byte("b"), []byte("v2")))
				require.NoError(t, w.Set([]byte("e"), []byte("v2")))
				require.NoError(t, w.DeleteRange([]byte("a"), []byte("z")))
				require.NoError(t, w.RangeKeySet([]byte("a"), []byte("z"), []byte("@1"), []byte("rk2")))
			})

			checkSeqNums := func(t *testing.T, layers [][]string) {
				t.Helper()
				var files [][]objstorage.ReadableFile
				total := 0
				for _, layer := range layers {
					row := make([]objstorage.ReadableFile, len(layer))
					for j, name := range layer {
						row[j] = openSST(name)
					}
					files = append(files, row)
					total += len(layer)
				}

				it, err := NewExternalIter(opts, &IterOptions{
					KeyTypes: IterKeyTypePointsAndRanges,
				}, files)
				require.NoError(t, err)
				defer func() { require.NoError(t, it.Close()) }()

				m, ok := it.pointIter.(*mergingIter)
				require.True(t, ok, "expected external pointIter to be *mergingIter")
				require.Len(t, m.levels, total)

				for i := range m.levels {
					wantSeq := base.SeqNum(total - i)
					kv := m.levels[i].iter.First()
					require.NotNil(t, kv, "level %d point iter unexpectedly empty", i)
					require.Equal(t, wantSeq, kv.SeqNum(), "level %d point iter synthetic seqnum", i)

					require.NotNil(t, m.levels[i].rangeDelIter, "level %d rangeDelIter unexpectedly nil", i)
					span, err := m.levels[i].rangeDelIter.First()
					require.NoError(t, err)
					require.NotNil(t, span, "level %d rangeDelIter unexpectedly empty", i)
					require.Len(t, span.Keys, 1)
					require.Equal(t, wantSeq, span.Keys[0].SeqNum(), "level %d rangeDelIter synthetic seqnum", i)
				}
			}

			t.Run("single-file", func(t *testing.T) {
				checkSeqNums(t, [][]string{{"f0.sst"}})
			})

			t.Run("multi-layer", func(t *testing.T) {
				checkSeqNums(t, [][]string{{"f0.sst", "f1.sst"}, {"f2.sst"}})

				// Also verify public point+range iteration semantics across layers:
				// f0 and f1 shadow f2's point keys (b, e) via range deletions while
				// preserving their own point keys (a, d).
				it, err := NewExternalIter(opts, &IterOptions{
					KeyTypes: IterKeyTypePointsAndRanges,
				}, [][]objstorage.ReadableFile{
					{openSST("f0.sst"), openSST("f1.sst")},
					{openSST("f2.sst")},
				})
				require.NoError(t, err)
				defer func() { require.NoError(t, it.Close()) }()

				var gotPoints []string
				var gotRanges []string
				for valid := it.First(); valid; valid = it.Next() {
					hasPoint, hasRange := it.HasPointAndRange()
					if hasPoint {
						gotPoints = append(gotPoints, fmt.Sprintf("%s=%s", it.Key(), it.Value()))
					}
					if hasRange && it.RangeKeyChanged() {
						start, end := it.RangeBounds()
						rks := it.RangeKeys()
						require.Len(t, rks, 1)
						gotRanges = append(gotRanges, fmt.Sprintf("[%s,%s)=%s", start, end, rks[0].Value))
					}
				}
				require.NoError(t, it.Error())
				require.Equal(t, []string{"a=v0", "d=v1"}, gotPoints)
				require.Equal(t, []string{"[a,c)=rk0", "[c,d)=rk2", "[d,f)=rk1", "[f,z)=rk2"}, gotRanges)
			})
		})
	}
}

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
							key := testkeys.Key(ks, uint64(len(keys)))
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
							files := make([][]objstorage.ReadableFile, fileCount)
							for i := 0; i < fileCount; i++ {
								f, err := fs.Open(filenames[i])
								require.NoError(b, err)
								files[i] = []objstorage.ReadableFile{f}
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
