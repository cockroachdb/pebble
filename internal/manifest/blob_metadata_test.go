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

func TestBlobFileMetadata_ParseRoundTrip(t *testing.T) {
	testCases := []struct {
		name   string
		input  string
		output string
	}{
		{
			name:  "verbatim",
			input: "000001 size:[903530 (882KB)] vals:[39531 (39KB)]",
		},
		{
			name:   "whitespace is insignificant",
			input:  "000001   size  : [ 903530 (882KB )] vals: [ 39531 ( 39KB ) ]",
			output: "000001 size:[903530 (882KB)] vals:[39531 (39KB)]",
		},
		{
			name:   "humanized sizes are optional",
			input:  "000001 size:[903530] vals:[39531]",
			output: "000001 size:[903530 (882KB)] vals:[39531 (39KB)]",
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
