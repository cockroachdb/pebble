// Copyright 2025 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package valsep

import (
	"bytes"
	"context"
	"fmt"
	"strings"
	"testing"

	"github.com/cockroachdb/crlib/crstrings"
	"github.com/cockroachdb/datadriven"
	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/pebble/internal/base"
	"github.com/cockroachdb/pebble/internal/blobtest"
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
		vs  ValueSeparation
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
		tw = &sstable.LoggingRawWriter{LogWriter: &buf, RawWriter: tw}
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
					vs = NeverSeparateValues{}
				case "preserve-blob-references":
					lines := crstrings.Lines(d.Input)
					inputBlobPhysicalFiles := make(map[base.BlobFileID]*manifest.PhysicalBlobFile, len(lines))
					for _, line := range lines {
						bfm, err := manifest.ParseBlobFileMetadataDebug(line)
						require.NoError(t, err)
						fn = max(fn, bfm.Physical.FileNum)
						inputBlobPhysicalFiles[bfm.FileID] = bfm.Physical
					}
					pbr := NewPreserveAllHotBlobReferences(
						inputBlobPhysicalFiles, /* blob file set */
						manifest.BlobReferenceDepth(0),
						0, /* minimum size */
						0, /* minimum MVCC garbage size */
					)
					vs = pbr
				case "write-new-blob-files":
					var minimumSize int
					var shortAttrExtractor base.ShortAttributeExtractor
					d.MaybeScanArgs(t, "minimum-size", &minimumSize)
					if arg, ok := d.Arg("short-attr-extractor"); ok {
						switch arg.SingleVal(t) {
						case "error":
							shortAttrExtractor = errShortAttrExtractor
						default:
							t.Fatalf("unknown short attribute extractor: %s", arg.String())
						}
					}
					newSep := NewWriteNewBlobFiles(
						testkeys.Comparer,
						func() (objstorage.Writable, objstorage.ObjectMetadata, error) {
							fn++
							return objStore.Create(ctx, base.FileTypeBlob, fn, objstorage.CreateOptions{})
						},
						blob.FileWriterOptions{},
						minimumSize,
						0, /* minimum MVCC garbage size */
						WriteNewBlobFilesOptions{
							ShortAttrExtractor: shortAttrExtractor,
							InvalidValueCallback: func(userKey []byte, value []byte, err error) {
								fmt.Fprintf(&buf, "# invalid value for key %q, value: %q: %s\n", userKey, value, err)
							},
						},
					)
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
				for line := range crstrings.LinesSeq(d.Input) {
					parts := strings.SplitN(line, ":", 2)
					ik := base.ParseInternalKey(parts[0])
					ikv.K = ik
					if strings.HasPrefix(parts[1], "blob") {
						ikv.V, err = bv.ParseInternalValue(parts[1])
						require.NoError(t, err)
					} else {
						ikv.V = base.MakeInPlaceValue([]byte(parts[1]))
					}
					require.NoError(t, vs.Add(tw, &ikv, false /* forceObsolete */, false /* isLikelyMVCCGarbage */))
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
			default:
				t.Fatalf("unknown command: %s", d.Cmd)
			}
			panic("unreachable")
		})
}

func errShortAttrExtractor(
	key []byte, keyPrefixLen int, value []byte,
) (base.ShortAttribute, error) {
	return 0, errors.New("short attribute extractor error")
}

// Assert that errShortAttrExtractor implements the ShortAttributeExtractor
var _ base.ShortAttributeExtractor = errShortAttrExtractor
