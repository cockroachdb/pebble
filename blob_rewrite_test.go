// Copyright 2025 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package pebble

import (
	"bytes"
	"context"
	"fmt"
	"math/rand/v2"
	"slices"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/cockroachdb/crlib/crstrings"
	"github.com/cockroachdb/datadriven"
	"github.com/cockroachdb/pebble/internal/base"
	"github.com/cockroachdb/pebble/internal/blobtest"
	"github.com/cockroachdb/pebble/internal/bytealloc"
	"github.com/cockroachdb/pebble/internal/cache"
	"github.com/cockroachdb/pebble/internal/manifest"
	"github.com/cockroachdb/pebble/internal/testkeys"
	"github.com/cockroachdb/pebble/internal/testutils"
	"github.com/cockroachdb/pebble/objstorage"
	"github.com/cockroachdb/pebble/objstorage/objstorageprovider"
	"github.com/cockroachdb/pebble/sstable"
	"github.com/cockroachdb/pebble/sstable/blob"
	"github.com/cockroachdb/pebble/sstable/block"
	"github.com/cockroachdb/pebble/valsep"
	"github.com/cockroachdb/pebble/vfs"
	"github.com/stretchr/testify/require"
)

func TestBlobRewrite(t *testing.T) {
	var (
		bv  blobtest.Values
		vs  valsep.ValueSeparation
		tw  sstable.RawWriter
		fn  base.DiskFileNum
		buf bytes.Buffer
		// Set up a logging FS to capture the filesystem operations throughout
		// the test. When testing a value separation policy that writes new blob
		// files, this demonstrates the creation of new blob files and that
		// they're created lazily, the first time a value is actually separated.
		fs = vfs.WithLogging(vfs.NewMem(), func(format string, args ...interface{}) {
			fmt.Fprint(&buf, "# ")
			fmt.Fprintf(&buf, format, args...)
			fmt.Fprintln(&buf)
		})
	)
	ctx := context.Background()
	objStore, err := objstorageprovider.Open(objstorageprovider.Settings{
		FS: fs,
	})
	require.NoError(t, err)

	initRawWriter := func() {
		if tw != nil {
			require.NoError(t, tw.Close())
		}
		fn++
		w, _, err := objStore.Create(ctx, base.FileTypeTable, fn, objstorage.CreateOptions{})
		require.NoError(t, err)
		tw = sstable.NewRawWriter(w, sstable.WriterOptions{
			TableFormat: sstable.TableFormatMax,
		})
		tw = &loggingRawWriter{w: &buf, RawWriter: tw}
	}

	datadriven.RunTest(t, "testdata/blob_rewrite",
		func(t *testing.T, d *datadriven.TestData) string {
			buf.Reset()
			switch d.Cmd {
			case "init":
				if tw != nil {
					require.NoError(t, tw.Close())
				}
				bv = blobtest.Values{}
				switch x := d.CmdArgs[0].String(); x {
				case "preserve-blob-references":
					pbr := &preserveBlobReferences{}
					lines := crstrings.Lines(d.Input)
					pbr.inputBlobPhysicalFiles = make(map[base.BlobFileID]*manifest.PhysicalBlobFile, len(lines))
					for _, line := range lines {
						bfm, err := manifest.ParseBlobFileMetadataDebug(line)
						require.NoError(t, err)
						fn = max(fn, bfm.Physical.FileNum)
						pbr.inputBlobPhysicalFiles[bfm.FileID] = bfm.Physical
					}
					vs = pbr
				case "write-new-blob-files":
					newSep := &writeNewBlobFiles{
						comparer: testkeys.Comparer,
						newBlobObject: func() (objstorage.Writable, objstorage.ObjectMetadata, error) {
							fn++
							return objStore.Create(ctx, base.FileTypeBlob, fn, objstorage.CreateOptions{})
						},
					}
					d.MaybeScanArgs(t, "minimum-size", &newSep.minimumSize)
					vs = newSep
				default:
					t.Fatalf("unknown value separation policy: %s", x)
				}
				return buf.String()
			case "add":
				if tw == nil {
					initRawWriter()
				}
				var ikv base.InternalKV
				for _, line := range crstrings.Lines(d.Input) {
					parts := strings.SplitN(line, ":", 2)
					ik := base.ParseInternalKey(parts[0])
					ikv.K = ik
					if strings.HasPrefix(parts[1], "blob") {
						ikv.V, err = bv.ParseInternalValue(parts[1])
						require.NoError(t, err)
					} else {
						ikv.V = base.MakeInPlaceValue([]byte(parts[1]))
					}
					require.NoError(t, vs.Add(tw, &ikv, false /* forceObsolete */, false /* isLikeyMVCCGarbage */))
				}
				return buf.String()
			case "close-output":
				if tw != nil {
					require.NoError(t, tw.Close())
					tw = nil
				}

				meta, err := vs.FinishOutput()
				require.NoError(t, err)
				if len(meta.NewBlobFiles) == 0 {
					fmt.Fprintln(&buf, "no blob file created")
				} else {
					for _, bf := range meta.NewBlobFiles {
						fmt.Fprintf(&buf, "Blob file created: %s\n", bf.FileMetadata)
						fmt.Fprintln(&buf, bf.FileStats)
					}
				}
				if len(meta.BlobReferences) == 0 {
					fmt.Fprintln(&buf, "blobrefs:[]")
				} else {
					fmt.Fprintln(&buf, "blobrefs:[")
					for i, ref := range meta.BlobReferences {
						fmt.Fprintf(&buf, " %d: %s %d", i, ref.FileID, ref.ValueSize)
						if ref.ValueSize != ref.BackingValueSize && ref.BackingValueSize > 0 {
							fmt.Fprintf(&buf, "/%d", ref.BackingValueSize)
						}
						fmt.Fprintf(&buf, "\n")
					}
					fmt.Fprintln(&buf, "]")
				}
				return buf.String()
			case "rewrite-blob":
				// rewrite-blob <target-blob-file> <sstable1> <sstable2> ...
				if len(d.CmdArgs) < 2 {
					t.Fatalf("rewrite-blob requires at least a target blob file and one SSTable")
				}
				targetBlobFileStr := d.CmdArgs[0].String()
				targetBlobFileNum, err := strconv.ParseUint(targetBlobFileStr, 10, 64)
				if err != nil {
					t.Fatalf("invalid blob file number %s: %v", targetBlobFileStr, err)
				}
				var sstableFileNums []base.DiskFileNum
				for i := 1; i < len(d.CmdArgs); i++ {
					sstFileNumStr := d.CmdArgs[i].String()
					sstFileNum, err := strconv.ParseUint(sstFileNumStr, 10, 64)
					if err != nil {
						t.Fatalf("invalid SSTable file number %s: %v", sstFileNumStr, err)
					}
					sstableFileNums = append(sstableFileNums, base.DiskFileNum(sstFileNum))
				}

				fileCache := NewFileCache(1, 100)
				mockFC := fileCache.newHandle(
					nil,
					objStore,
					&base.LoggerWithNoopTracer{Logger: base.DefaultLogger},
					sstable.ReaderOptions{},
					func(base.ObjectInfo, error) error { return nil },
				)
				var sstables []*manifest.TableMetadata
				for _, sstFileNum := range sstableFileNums {
					sst := &manifest.TableMetadata{
						TableBacking: &manifest.TableBacking{
							DiskFileNum: sstFileNum,
						},
						BlobReferences: []manifest.BlobReference{{FileID: base.BlobFileID(targetBlobFileNum)}},
					}
					sstables = append(sstables, sst)
				}

				inputBlob := manifest.BlobFileMetadata{
					FileID: base.BlobFileID(targetBlobFileNum),
					Physical: &manifest.PhysicalBlobFile{
						FileNum:      base.DiskFileNum(targetBlobFileNum),
						CreationTime: uint64(time.Now().Unix()),
					},
				}
				fn++
				outputWritable, _, err := objStore.Create(ctx, base.FileTypeBlob, fn, objstorage.CreateOptions{})
				require.NoError(t, err)

				rewriter := newBlobFileRewriter(mockFC, block.ReadEnv{}, fn, outputWritable,
					blob.FileWriterOptions{}, sstables, inputBlob)
				stats, err := rewriter.Rewrite(context.Background())
				if err != nil {
					fmt.Fprintf(&buf, "rewrite error: %v\n", err)
				} else {
					fmt.Fprintf(&buf, "Successfully rewrote blob file %s to %s\n",
						targetBlobFileStr, fn.String())
					fmt.Fprintf(&buf, "Input SSTables: %v\n", sstableFileNums)
					fmt.Fprintf(&buf, "SSTables with blob references: %d\n", len(sstables))
					fmt.Fprintln(&buf, stats)
				}

				return buf.String()
			default:
				t.Fatalf("unknown command: %s", d.Cmd)
			}
			panic("unreachable")
		})
}

// TestBlobRewriteRandomized tests blob file rewriting by constructing a blob
// file and n sstables that each reference one value in the blob file.
//
// It then runs a blob rewrite repeatedly, passing in a random subset of the
// sstables as extant references. Each blob rewrite may rewrite the original
// blob file, or one of the previous iteration's rewritten blob files.
func TestBlobRewriteRandomized(t *testing.T) {
	const numKVs = 1000
	const blobFileID = 100000
	const numRewrites = 10

	seed := time.Now().UnixNano()
	t.Logf("seed: %d", seed)
	rng := rand.New(rand.NewPCG(0, uint64(seed)))

	// Generate keys and values.
	keys, values := func() (keys, values [][]byte) {
		keys = make([][]byte, numKVs)
		values = make([][]byte, numKVs)
		var alloc bytealloc.A
		for i := 0; i < numKVs; i++ {
			var key, value []byte
			key, alloc = alloc.Copy([]byte(fmt.Sprintf("%06d", i)))
			value, alloc = alloc.Copy(append([]byte(fmt.Sprintf("%d", i)),
				bytes.Repeat([]byte{'v'}, rng.IntN(100))...))
			keys[i] = key
			values[i] = value
		}
		return keys, values
	}()

	ctx := context.Background()
	objStore, err := objstorageprovider.Open(objstorageprovider.Settings{
		FS: vfs.NewMem(),
	})
	require.NoError(t, err)

	// Write the source blob file.
	var originalBlobFile manifest.BlobFileMetadata
	handles := make([]blob.Handle, numKVs)
	{
		w, _, err := objStore.Create(ctx, base.FileTypeBlob, base.DiskFileNum(blobFileID), objstorage.CreateOptions{})
		require.NoError(t, err)
		blobWriter := blob.NewFileWriter(base.DiskFileNum(blobFileID), w, blob.FileWriterOptions{})
		for i := range numKVs {
			if rng.IntN(20) == 0 {
				blobWriter.FlushForTesting()
			}
			handles[i] = blobWriter.AddValue(values[i], false /* isLikelyMVCCGarbage */)
		}
		stats, err := blobWriter.Close()
		require.NoError(t, err)
		require.Equal(t, numKVs, int(stats.ValueCount))
		originalBlobFile = manifest.BlobFileMetadata{
			FileID: base.BlobFileID(blobFileID),
			Physical: &manifest.PhysicalBlobFile{
				FileNum:      base.DiskFileNum(blobFileID),
				CreationTime: uint64(time.Now().Unix()),
				ValueSize:    stats.UncompressedValueBytes,
				Size:         stats.FileLen,
			},
		}
	}

	// Write numKVs SSTables, each with 1 kv pair, all referencing the same blob
	// file.
	originalTables := make([]*manifest.TableMetadata, numKVs)
	originalValueIndices := make([]int, numKVs)
	for i := range numKVs {
		w, _, err := objStore.Create(ctx, base.FileTypeTable, base.DiskFileNum(i), objstorage.CreateOptions{})
		require.NoError(t, err)
		tw := sstable.NewRawWriter(w, sstable.WriterOptions{
			TableFormat: sstable.TableFormatMax,
		})
		require.NoError(t, tw.AddWithBlobHandle(
			base.MakeInternalKey(keys[i], base.SeqNum(i), base.InternalKeyKindSet),
			blob.InlineHandle{
				InlineHandlePreface: blob.InlineHandlePreface{
					ReferenceID: base.BlobReferenceID(0),
					ValueLen:    uint32(len(values[i])),
				},
				HandleSuffix: blob.HandleSuffix{
					BlockID: handles[i].BlockID,
					ValueID: handles[i].ValueID,
				},
			},
			base.ShortAttribute(0),
			false, /* forceObsolete */
		))
		require.NoError(t, tw.Close())
		originalValueIndices[i] = i
		valueSize := uint64(len(values[i]))
		originalTables[i] = &manifest.TableMetadata{
			TableNum: base.TableNum(i),
			TableBacking: &manifest.TableBacking{
				DiskFileNum: base.DiskFileNum(i),
			},
			BlobReferences: []manifest.BlobReference{
				{FileID: base.BlobFileID(blobFileID), ValueSize: valueSize, BackingValueSize: valueSize},
			},
		}
	}

	fc := NewFileCache(1, 100)
	defer fc.Unref()
	c := cache.New(100)
	defer c.Unref()
	ch := c.NewHandle()
	defer ch.Close()
	fch := fc.newHandle(ch, objStore, base.NoopLoggerAndTracer{}, sstable.ReaderOptions{}, nil)
	defer fch.Close()
	var bufferPool block.BufferPool
	bufferPool.Init(4, block.ForBlobFileRewrite)
	defer bufferPool.Release()
	readEnv := block.ReadEnv{BufferPool: &bufferPool}

	type sourceFile struct {
		metadata          manifest.BlobFileMetadata
		valueIndices      []int
		referencingTables []*manifest.TableMetadata
	}
	files := []sourceFile{{
		metadata:          originalBlobFile,
		valueIndices:      originalValueIndices,
		referencingTables: originalTables,
	}}

	var verboseBuffer bytes.Buffer
	verboseBuffer.Grow(16 << 10 /* 16KiB */)
	defer func() {
		if t.Failed() {
			t.Logf("verbose output:\n%s", verboseBuffer.String())
		}
	}()

	for i := range numRewrites {
		fileIdx := rng.IntN(len(files))
		fileToRewrite := files[fileIdx]

		// Rewrite the blob file.
		newBlobFileNum := base.DiskFileNum(blobFileID + i + 1)

		// Pick a random subset of the referencing tables to use as remaining
		// extant references.
		n := 1
		if len(fileToRewrite.valueIndices) > 1 {
			n = testutils.RandIntInRange(rng, 1, len(fileToRewrite.valueIndices))
		}
		fmt.Fprintf(&verboseBuffer, "rewriting file %s, preserving %d values\n", fileToRewrite.metadata.Physical.FileNum, n)

		// Produce the inputs for the rewrite.
		newFile := sourceFile{
			metadata: manifest.BlobFileMetadata{
				FileID: base.BlobFileID(blobFileID),
				Physical: &manifest.PhysicalBlobFile{
					FileNum: newBlobFileNum,
				},
			},
			valueIndices:      make([]int, n),
			referencingTables: make([]*manifest.TableMetadata, n),
		}
		for k, j := range rng.Perm(len(fileToRewrite.valueIndices))[:n] {
			valueIndex := fileToRewrite.valueIndices[j]
			newFile.valueIndices[k] = valueIndex
			newFile.referencingTables[k] = originalTables[valueIndex]
		}
		slices.Sort(newFile.valueIndices)
		for _, idx := range newFile.valueIndices {
			fmt.Fprintf(&verboseBuffer, "newFile.valueIndices: %d: %q; handle: %s\n", idx, values[idx], handles[idx])
		}

		// Rewrite the blob file.
		w, _, err := objStore.Create(ctx, base.FileTypeBlob, newBlobFileNum, objstorage.CreateOptions{})
		require.NoError(t, err)
		opts := blob.FileWriterOptions{
			FlushGovernor: block.MakeFlushGovernor(128<<rng.IntN(6), 90, 100, nil),
		}
		rewriter := newBlobFileRewriter(fch, readEnv, newBlobFileNum,
			w, opts, newFile.referencingTables, fileToRewrite.metadata)
		stats, err := rewriter.Rewrite(ctx)
		require.NoError(t, err)
		require.LessOrEqual(t, n, int(stats.ValueCount))
		newFile.metadata.Physical.ValueSize = stats.UncompressedValueBytes
		newFile.metadata.Physical.Size = stats.FileLen

		// Verify that the rewritten blob file contains the correct values, and
		// that they may still be accessed using the original handles.
		var valueFetcher blob.ValueFetcher
		valueFetcher.Init(constantFileMapping(newBlobFileNum), fch, readEnv,
			blob.SuggestedCachedReaders(1))
		func() {
			defer func() { _ = valueFetcher.Close() }()
			for _, valueIndex := range newFile.valueIndices {
				handle := handles[valueIndex]
				val, _, err := valueFetcher.Fetch(ctx, blobFileID, handle.BlockID, handle.ValueID)
				require.NoError(t, err)
				require.Equal(t, values[valueIndex], val)
			}
		}()

		// Add the new blob file to the list of blob files so that a future
		// rewrite can use it as the source file. This ensures we test rewrites
		// of rewritten blob files.
		files = append(files, newFile)
	}
}

// constantFileMapping implements base.BlobFileMapping and always maps to itself.
type constantFileMapping base.DiskFileNum

// Assert that (*inputFileMapping) implements base.BlobFileMapping.
var _ base.BlobFileMapping = constantFileMapping(0)

func (m constantFileMapping) Lookup(fileID base.BlobFileID) (base.ObjectInfo, bool) {
	return base.ObjectInfoLiteral{
		FileType:    base.FileTypeBlob,
		DiskFileNum: base.DiskFileNum(m),
		Bounds:      base.UserKeyBounds{},
	}, true
}
