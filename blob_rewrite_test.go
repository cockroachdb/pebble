// Copyright 2025 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package pebble

import (
	"bytes"
	"context"
	"fmt"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/cockroachdb/crlib/crstrings"
	"github.com/cockroachdb/datadriven"
	"github.com/cockroachdb/pebble/internal/base"
	"github.com/cockroachdb/pebble/internal/blobtest"
	"github.com/cockroachdb/pebble/internal/compact"
	"github.com/cockroachdb/pebble/internal/manifest"
	"github.com/cockroachdb/pebble/internal/testkeys"
	"github.com/cockroachdb/pebble/objstorage"
	"github.com/cockroachdb/pebble/objstorage/objstorageprovider"
	"github.com/cockroachdb/pebble/sstable"
	"github.com/cockroachdb/pebble/sstable/blob"
	"github.com/cockroachdb/pebble/sstable/block"
	"github.com/cockroachdb/pebble/vfs"
	"github.com/stretchr/testify/require"
)

func TestBlobRewrite(t *testing.T) {
	var (
		bv  blobtest.Values
		vs  compact.ValueSeparation
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
					require.NoError(t, vs.Add(tw, &ikv, false /* forceObsolete */))
				}
				return buf.String()
			case "close-output":
				if tw != nil {
					require.NoError(t, tw.Close())
					tw = nil
				}

				meta, err := vs.FinishOutput()
				require.NoError(t, err)
				if meta.BlobFileObject.DiskFileNum == 0 {
					fmt.Fprintln(&buf, "no blob file created")
				} else {
					fmt.Fprintf(&buf, "Blob file created: %s\n", meta.BlobFileMetadata)
					fmt.Fprintln(&buf, meta.BlobFileStats)
				}
				if len(meta.BlobReferences) == 0 {
					fmt.Fprintln(&buf, "blobrefs:[]")
				} else {
					fmt.Fprintln(&buf, "blobrefs:[")
					for i, ref := range meta.BlobReferences {
						fmt.Fprintf(&buf, " %d: %s %d\n", i, ref.FileID, ref.ValueSize)
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

				mockFC := &fileCacheHandle{
					objProvider: objStore,
				}
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
					Physical: &manifest.PhysicalBlobFile{
						FileNum:      base.DiskFileNum(targetBlobFileNum),
						CreationTime: uint64(time.Now().Unix()),
					},
				}
				rewriter := newBlobFileRewriter(mockFC, sstables, inputBlob)

				fn++
				outputWritable, _, err := objStore.Create(ctx, base.FileTypeBlob, fn, objstorage.CreateOptions{})
				require.NoError(t, err)

				blobWriter := blob.NewFileWriter(fn, outputWritable, blob.FileWriterOptions{})
				rewriter.writer = blobWriter
				rewriter.env = block.ReadEnv{}
				err = rewriter.Rewrite()
				if err != nil {
					fmt.Fprintf(&buf, "rewrite error: %v\n", err)
				} else {
					fmt.Fprintf(&buf, "Successfully rewrote blob file %s to %s\n",
						targetBlobFileStr, fn.String())
					fmt.Fprintf(&buf, "Input SSTables: %v\n", sstableFileNums)
					fmt.Fprintf(&buf, "SSTables with blob references: %d\n", len(sstables))
				}

				return buf.String()
			default:
				t.Fatalf("unknown command: %s", d.Cmd)
			}
			panic("unreachable")
		})
}
