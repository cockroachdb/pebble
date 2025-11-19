// Copyright 2025 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package valsep

import (
	"bytes"
	"context"
	"fmt"
	"strconv"
	"strings"
	"testing"

	"github.com/cockroachdb/crlib/testutils/leaktest"
	"github.com/cockroachdb/datadriven"
	"github.com/cockroachdb/pebble/internal/base"
	"github.com/cockroachdb/pebble/internal/testkeys"
	"github.com/cockroachdb/pebble/objstorage"
	"github.com/cockroachdb/pebble/objstorage/objstorageprovider"
	"github.com/cockroachdb/pebble/sstable"
	"github.com/cockroachdb/pebble/vfs"
	"github.com/stretchr/testify/require"
)

func TestSSTBlobWriter(t *testing.T) {
	defer leaktest.AfterTest(t)()
	runDataDriven(t, "testdata/sst_blob_writer")
}

// The span policy string is in the form "(<option1>=<val>,<option2>=<val>...)"
func parseSpanPolicy(t *testing.T, spanPolicyStr string) base.SpanPolicy {
	spanPolicyStr = strings.TrimPrefix(spanPolicyStr, "(")
	spanPolicyStr = strings.TrimSuffix(spanPolicyStr, ")")
	var policy base.ValueStoragePolicyAdjustment
	var err error
	for part := range strings.SplitSeq(spanPolicyStr, ",") {
		fieldParts := strings.Split(part, "=")
		switch fieldParts[0] {
		case "no-value-separation":
			policy.DisableBlobSeparation = true
		case "value-separation-min-size":
			policy.OverrideBlobSeparationMinimumSize, err = strconv.Atoi(fieldParts[1])
			if err != nil {
				t.Fatalf("parsing value-separation-min-size: %v", err)
			}
		case "disable-value-separation-by-suffix":
			policy.DisableSeparationBySuffix = true
		default:
			t.Fatalf("unrecognized span policy option: %s", fieldParts[0])
		}
	}

	return base.SpanPolicy{
		ValueStoragePolicy: policy,
	}
}

func parseBuildSSTBlobWriterOptions(t *testing.T, td *datadriven.TestData) SSTBlobWriterOptions {
	opts := SSTBlobWriterOptions{}
	td.MaybeScanArgs(t, "value-separation-min-size", &opts.ValueSeparationMinSize)

	var spanPolicyStr string
	td.MaybeScanArgs(t, "span-policy", &spanPolicyStr)
	if spanPolicyStr != "" {
		opts.SpanPolicy = parseSpanPolicy(t, spanPolicyStr)
	}
	return opts
}

func runDataDriven(t *testing.T, file string) {
	datadriven.RunTest(t, file, func(t *testing.T, td *datadriven.TestData) string {
		ctx := context.Background()
		switch td.Cmd {
		case "build":
			var buf bytes.Buffer
			fs := vfs.WithLogging(vfs.NewMem(), func(format string, args ...any) {
				fmt.Fprint(&buf, "# ")
				fmt.Fprintf(&buf, format, args...)
				fmt.Fprintln(&buf)
			})
			objSettings := objstorageprovider.DefaultSettings(fs, "")
			objStore, err := objstorageprovider.Open(objSettings)
			require.NoError(t, err)
			blobFileCount := 0
			opts := parseBuildSSTBlobWriterOptions(t, td)
			opts.SSTWriterOpts.Comparer = testkeys.Comparer
			opts.SSTWriterOpts.TableFormat = sstable.TableFormatPebblev7
			opts.NewBlobFileFn = func() (objstorage.Writable, error) {
				fnum := blobFileCount
				w, _, err := objStore.Create(ctx, base.FileTypeBlob, base.DiskFileNum(fnum), objstorage.CreateOptions{})
				if err != nil {
					return nil, err
				}
				blobFileCount++
				return w, err
			}
			sstHandle, _, err := objStore.Create(ctx, base.FileTypeTable, 0, objstorage.CreateOptions{})
			require.NoError(t, err)
			writer := NewSSTBlobWriter(sstHandle, opts)
			defer func() {
				if !writer.closed {
					_ = writer.Close()
				}
			}()
			kvs, err := sstable.ParseTestKVsAndSpans(td.Input, nil)
			if err != nil {
				return fmt.Sprintf("error parsing input: %v", err)
			}
			if err := HandleTestKVs(writer, kvs); err != nil {
				return fmt.Sprintf("error handling test KVs: %v", err)
			}
			if err := writer.Close(); err != nil {
				return fmt.Sprintf("error closing writer: %v", err)
			}

			tableMeta, err := writer.Metadata()
			if err != nil {
				return fmt.Sprintf("error getting metadata: %v", err)
			}

			blobMetas, err := writer.BlobWriterMetas()
			if err != nil {
				return fmt.Sprintf("error getting blob metas: %v", err)
			}

			var outputBuf bytes.Buffer
			// Print some sst properties.
			fmt.Fprintf(&outputBuf, "size:%d\n", tableMeta.Size)
			outputBuf.WriteString("blobfiles:")
			require.Equal(t, blobFileCount, len(blobMetas))
			if len(blobMetas) > 0 {
				outputBuf.WriteString("\n")
				for i, bm := range blobMetas {
					fmt.Fprintf(&outputBuf, "%d: %s\n", i+1, bm.String())
				}
			} else {
				outputBuf.WriteString(" none\n")
			}
			return outputBuf.String()
		default:
			return fmt.Sprintf("unrecognized command %s", td.Cmd)
		}
	})
}
