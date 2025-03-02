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
		isHandle         bool
		setHasSamePrefix bool
		attr             base.ShortAttribute
	}{
		{
			isHandle:         false,
			setHasSamePrefix: false,
		},
		{
			isHandle:         false,
			setHasSamePrefix: true,
		},
		{
			isHandle:         true,
			setHasSamePrefix: false,
			attr:             5,
		},
		{
			isHandle:         true,
			setHasSamePrefix: true,
			attr:             2,
		},
	}
	for _, tc := range testCases {
		t.Run(fmt.Sprintf("%+v", tc), func(t *testing.T) {
			var prefix ValuePrefix
			if tc.isHandle {
				prefix = ValueBlockHandlePrefix(tc.setHasSamePrefix, tc.attr)
			} else {
				prefix = InPlaceValuePrefix(tc.setHasSamePrefix)
			}
			require.Equal(t, tc.isHandle, prefix.IsValueBlockHandle())
			require.Equal(t, tc.setHasSamePrefix, prefix.SetHasSamePrefix())
			if tc.isHandle {
				require.Equal(t, tc.attr, prefix.ShortAttribute())
			}
		})
	}
}
