// Copyright 2024 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package block

import (
	"fmt"
	"testing"

	"github.com/cockroachdb/pebble/internal/base"
	"github.com/stretchr/testify/require"
)

func TestValuePrefix(t *testing.T) {
	testCases := []struct {
		isValueBlockHandle bool
		isBlobHandle       bool
		setHasSamePrefix   bool
		attr               base.ShortAttribute
	}{
		{
			isValueBlockHandle: false,
			isBlobHandle:       false,
			setHasSamePrefix:   false,
		},
		{
			isValueBlockHandle: false,
			isBlobHandle:       false,
			setHasSamePrefix:   true,
		},
		{
			isValueBlockHandle: true,
			isBlobHandle:       false,
			setHasSamePrefix:   false,
			attr:               5,
		},
		{
			isValueBlockHandle: true,
			isBlobHandle:       false,
			setHasSamePrefix:   true,
			attr:               2,
		},
		{
			isValueBlockHandle: false,
			isBlobHandle:       true,
			setHasSamePrefix:   false,
		},
		{
			isValueBlockHandle: false,
			isBlobHandle:       true,
			setHasSamePrefix:   true,
			attr:               2,
		},
	}
	for _, tc := range testCases {
		t.Run(fmt.Sprintf("%+v", tc), func(t *testing.T) {
			var prefix ValuePrefix
			if tc.isValueBlockHandle {
				prefix = ValueBlockHandlePrefix(tc.setHasSamePrefix, tc.attr)
			} else if tc.isBlobHandle {
				prefix = BlobValueHandlePrefix(tc.setHasSamePrefix, tc.attr)
			} else {
				prefix = InPlaceValuePrefix(tc.setHasSamePrefix)
			}
			require.Equal(t, tc.isValueBlockHandle, prefix.IsValueBlockHandle())
			require.Equal(t, tc.isBlobHandle, prefix.IsBlobValueHandle())
			require.Equal(t, tc.setHasSamePrefix, prefix.SetHasSamePrefix())
			if tc.isValueBlockHandle {
				require.Equal(t, tc.attr, prefix.ShortAttribute())
			}
		})
	}
}
