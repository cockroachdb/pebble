// Copyright 2025 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package pebble

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"slices"
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

// defineDBValueSeparator is a compact.ValueSeparation implementation used by
// datadriven tests when defining a database state. It is a wrapper around
// preserveBlobReferences that also parses string representations of blob
// references from values.
type defineDBValueSeparator struct {
	bv    blobtest.Values
	metas map[base.DiskFileNum]*manifest.BlobFileMetadata
	pbr   *preserveBlobReferences
	kv    base.InternalKV
}

// Assert that *defineDBValueSeparator implements the compact.ValueSeparation interface.
var _ compact.ValueSeparation = (*defineDBValueSeparator)(nil)

// EstimatedFileSize returns an estimate of the disk space consumed by the current
// blob file if it were closed now.
func (vs *defineDBValueSeparator) EstimatedFileSize() uint64 {
	return vs.pbr.EstimatedFileSize()
}

// EstimatedReferenceSize returns an estimate of the disk space consumed by the
// current output sstable's blob references so far.
func (vs *defineDBValueSeparator) EstimatedReferenceSize() uint64 {
	return vs.pbr.EstimatedReferenceSize()
}

// Add adds the provided key-value pair to the sstable, possibly separating the
// value into a blob file.
func (vs *defineDBValueSeparator) Add(
	tw sstable.RawWriter, kv *base.InternalKV, forceObsolete bool,
) error {
	// In datadriven tests, all defined values are in-place initially. See
	// runDBDefineCmdReuseFS.
	v := kv.V.InPlaceValue()
	// If the value doesn't begin with "blob", don't separate it.
	if !bytes.HasPrefix(v, []byte("blob")) {
		return tw.Add(kv.K, v, forceObsolete)
	}

	// This looks like a blob reference. Parse it.
	iv, err := vs.bv.ParseInternalValue(string(v))
	if err != nil {
		return err
	}
	lv := iv.LazyValue()
	// If we haven't seen this blob file before, fabricate a metadata for it.
	fileNum := lv.Fetcher.BlobFileNum
	meta, ok := vs.metas[fileNum]
	if !ok {
		meta = &manifest.BlobFileMetadata{
			FileNum:      fileNum,
			CreationTime: uint64(time.Now().Unix()),
		}
		vs.metas[fileNum] = meta
	}
	meta.Size += uint64(lv.Fetcher.Attribute.ValueLen)
	meta.ValueSize += uint64(lv.Fetcher.Attribute.ValueLen)

	// If it's not already in pbr.inputBlobMetadatas, add it.
	if !slices.Contains(vs.pbr.inputBlobMetadatas, meta) {
		vs.pbr.inputBlobMetadatas = append(vs.pbr.inputBlobMetadatas, meta)
	}
	// Return a KV that uses the original key but our constructed blob reference.
	vs.kv.K = kv.K
	vs.kv.V = iv
	return vs.pbr.Add(tw, &vs.kv, forceObsolete)
}

// FinishOutput implements compact.ValueSeparation.
func (d *defineDBValueSeparator) FinishOutput() (compact.ValueSeparationMetadata, error) {
	m, err := d.pbr.FinishOutput()
	if err != nil {
		return compact.ValueSeparationMetadata{}, err
	}
	// TODO(jackson): Support setting a specific depth from the datadriven test.
	m.BlobReferenceDepth = manifest.BlobReferenceDepth(len(m.BlobReferences))
	return m, nil
}
