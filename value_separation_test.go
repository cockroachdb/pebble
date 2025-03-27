// Copyright 2025 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package pebble

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"strings"
	"testing"

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
	"github.com/cockroachdb/pebble/vfs"
	"github.com/stretchr/testify/require"
)

func TestValueSeparationPolicy(t *testing.T) {
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
			TableFormat: sstable.TableFormatPebblev6,
		})
		tw = &loggingRawWriter{w: &buf, RawWriter: tw}
	}

	datadriven.RunTest(t, "testdata/value_separation_policy",
		func(t *testing.T, d *datadriven.TestData) string {
			buf.Reset()
			switch d.Cmd {
			case "init":
				if tw != nil {
					require.NoError(t, tw.Close())
				}
				bv = blobtest.Values{}
				switch x := d.CmdArgs[0].String(); x {
				case "never-separate-values":
					vs = compact.NeverSeparateValues{}
				case "preserve-blob-references":
					pbr := &preserveBlobReferences{}
					lines := crstrings.Lines(d.Input)
					pbr.inputBlobMetadatas = make([]*manifest.BlobFileMetadata, 0, len(lines))
					for _, line := range lines {
						bfm, err := manifest.ParseBlobFileMetadataDebug(line)
						require.NoError(t, err)
						fn = max(fn, bfm.FileNum)
						pbr.inputBlobMetadatas = append(pbr.inputBlobMetadatas, bfm)
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
					if cmdArg, ok := d.Arg("required-in-place"); ok {
						cmdArg.ExpectNumVals(t, 2)
						newSep.requiredInPlaceValueBound = UserKeyPrefixBound{
							Lower: []byte(cmdArg.Vals[0]),
							Upper: []byte(cmdArg.Vals[1]),
						}
					}
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
			case "estimated-sizes":
				fmt.Fprintf(&buf, "file: %d, references: %d\n", vs.EstimatedFileSize(), vs.EstimatedReferenceSize())
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
						fmt.Fprintf(&buf, " %d: %s %d\n", i, ref.FileNum, ref.ValueSize)
					}
					fmt.Fprintln(&buf, "]")
				}
				return buf.String()
			default:
				t.Fatalf("unknown command: %s", d.Cmd)
			}
			panic("unreachable")
		})
}

// loggingRawWriter wraps a sstable.RawWriter and logs calls to Add and
// AddWithBlobHandle to provide observability into the separation of values into
// blob files.
type loggingRawWriter struct {
	w io.Writer
	sstable.RawWriter
}

func (w *loggingRawWriter) Add(key InternalKey, value []byte, forceObsolete bool) error {
	fmt.Fprintf(w.w, "RawWriter.Add(%q, %q, %t)\n", key, value, forceObsolete)
	return w.RawWriter.Add(key, value, forceObsolete)
}

func (w *loggingRawWriter) AddWithBlobHandle(
	key InternalKey, h blob.InlineHandle, attr base.ShortAttribute, forceObsolete bool,
) error {
	fmt.Fprintf(w.w, "RawWriter.AddWithBlobHandle(%q, %q, %x, %t)\n", key, h, attr, forceObsolete)
	return w.RawWriter.AddWithBlobHandle(key, h, attr, forceObsolete)
}
