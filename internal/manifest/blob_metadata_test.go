// Copyright 2025 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package manifest

import (
	"testing"

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
