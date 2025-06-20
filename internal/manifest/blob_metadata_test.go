// Copyright 2025 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package manifest

import (
	"bytes"
	"fmt"
	"testing"

	"github.com/cockroachdb/datadriven"
	"github.com/cockroachdb/pebble/internal/base"
	"github.com/stretchr/testify/require"
)

func TestPhysicalBlobFile_ParseRoundTrip(t *testing.T) {
	testCases := []struct {
		name         string
		input        string
		output       string
		creationTime uint64
	}{
		{
			name:  "verbatim",
			input: "000001 size:[903530 (882KB)] vals:[39531 (39KB)]",
		},
		{
			name:         "whitespace is insignificant",
			input:        "000001   size  : [ 903530 (882KB )] vals: [ 39531 ( 39KB ) ] creationTime:   1718851200",
			output:       "000001 size:[903530 (882KB)] vals:[39531 (39KB)]",
			creationTime: 1718851200,
		},
		{
			name:   "humanized sizes are optional",
			input:  "000001 size:[903530] vals:[39531]",
			output: "000001 size:[903530 (882KB)] vals:[39531 (39KB)]",
		},
		{
			name:         "creation time is optional",
			input:        "000001 size:[903530 (882KB)] vals:[39531 (39KB)] creationTime:1718851200",
			output:       "000001 size:[903530 (882KB)] vals:[39531 (39KB)]",
			creationTime: 1718851200,
		},
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			m, err := ParsePhysicalBlobFileDebug(tc.input)
			require.NoError(t, err)
			got := m.String()
			want := tc.input
			if tc.output != "" {
				want = tc.output
			}
			require.Equal(t, want, got)
			require.Equal(t, tc.creationTime, m.CreationTime)
		})
	}
}

func TestBlobFileMetadata_ParseRoundTrip(t *testing.T) {
	testCases := []struct {
		name   string
		input  string
		output string
	}{
		{
			name:  "verbatim",
			input: "B000002 physical:{000001 size:[903530 (882KB)] vals:[39531 (39KB)]}",
		},
		{
			name:   "whitespace is insignificant",
			input:  "B000002          physical : {000001   size  : [ 903530 (882KB )] vals: [ 39531 ( 39KB ) ] creationTime:   1718851200  }",
			output: "B000002 physical:{000001 size:[903530 (882KB)] vals:[39531 (39KB)]}",
		},
		{
			name:   "humanized sizes are optional",
			input:  "B000002 physical:{000001 size:[903530] vals:[39531]}",
			output: "B000002 physical:{000001 size:[903530 (882KB)] vals:[39531 (39KB)]}",
		},
		{
			name:   "creation time is optional",
			input:  "B000002 physical:{000001 size:[903530 (882KB)] vals:[39531 (39KB)] creationTime:1718851200}",
			output: "B000002 physical:{000001 size:[903530 (882KB)] vals:[39531 (39KB)]}",
		},
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			m, err := ParseBlobFileMetadataDebug(tc.input)
			require.NoError(t, err)
			got := m.String()
			want := tc.input
			if tc.output != "" {
				want = tc.output
			}
			require.Equal(t, want, got)
		})
	}
}

func TestCurrentBlobFileSet(t *testing.T) {
	var (
		buf        bytes.Buffer
		set        CurrentBlobFileSet
		tableMetas = make(map[base.FileNum]*TableMetadata)
	)
	parseAndFillVersionEdit := func(s string) *VersionEdit {
		ve, err := ParseVersionEditDebug(s)
		require.NoError(t, err)
		for i, m := range ve.NewTables {
			if existingMeta, ok := tableMetas[m.Meta.TableNum]; ok {
				// Ensure pointer equality of the *TableMetadata.
				// ParseVersionEditDebug will return a new *TableMetadata every
				// time it decodes it.
				ve.NewTables[i].Meta = existingMeta
			} else {
				tableMetas[m.Meta.TableNum] = m.Meta
			}
		}
		for dte := range ve.DeletedTables {
			ve.DeletedTables[dte] = tableMetas[dte.FileNum]
		}
		return ve
	}

	datadriven.RunTest(t, "testdata/current_blob_file_set", func(t *testing.T, d *datadriven.TestData) string {
		buf.Reset()
		switch d.Cmd {
		case "init":
			ve := parseAndFillVersionEdit(d.Input)
			bve := &BulkVersionEdit{}
			if err := bve.Accumulate(ve); err != nil {
				return fmt.Sprintf("error accumulating version edit: %s", err)
			}
			set.Init(bve)
			return set.Stats().String()
		case "applyAndUpdateVersionEdit":
			ve := parseAndFillVersionEdit(d.Input)
			if err := set.ApplyAndUpdateVersionEdit(ve); err != nil {
				return fmt.Sprintf("error applying and updating version edit: %s", err)
			}
			fmt.Fprintf(&buf, "modified version edit:\n%s", ve.DebugString(base.DefaultFormatter))
			fmt.Fprintf(&buf, "current blob file set:\n%s", set.Stats().String())
			return buf.String()
		case "metadatas":
			for _, m := range set.Metadatas() {
				fmt.Fprintf(&buf, "%s\n", m)
			}
			return buf.String()
		case "stats":
			return set.Stats().String()
		default:
			t.Fatalf("unknown command: %s", d.Cmd)
		}
		return ""
	})
}

func TestBlobFileSet_Lookup(t *testing.T) {
	const numBlobFiles = 10000
	set, files := makeTestBlobFiles(numBlobFiles)
	for i := 0; i < numBlobFiles; i++ {
		fn, ok := set.Lookup(base.BlobFileID(i))
		require.True(t, ok)
		require.Equal(t, files[i].FileNum, fn)
	}
}

func makeTestBlobFiles(numBlobFiles int) (BlobFileSet, []PhysicalBlobFile) {
	files := make([]PhysicalBlobFile, numBlobFiles)
	for i := 0; i < numBlobFiles; i++ {
		fileNum := base.DiskFileNum(i)
		if i%2 == 0 {
			fileNum = base.DiskFileNum(2*numBlobFiles + i)
		}
		files[i] = PhysicalBlobFile{
			FileNum:      fileNum,
			Size:         uint64(i),
			ValueSize:    uint64(i),
			CreationTime: uint64(i),
		}
	}
	set := MakeBlobFileSet(nil)
	for i := 0; i < numBlobFiles; i++ {
		set.insert(BlobFileMetadata{
			FileID:   base.BlobFileID(i % numBlobFiles),
			Physical: &files[i],
		})
	}
	return set, files
}

func BenchmarkBlobFileSet_Lookup(b *testing.B) {
	const numBlobFiles = 10000
	set, _ := makeTestBlobFiles(numBlobFiles)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _ = set.Lookup(base.BlobFileID(i % numBlobFiles))
	}
}
