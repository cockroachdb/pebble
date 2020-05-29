// Copyright 2020 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package manifest

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestMakeLevel(t *testing.T) {
	testCases := []struct {
		level    int
		sublevel int
		expected string
	}{
		{0, 0, "L0.0"},
		{0, 1, "L0.1"},
		{0, 2, "L0.2"},
		{0, 1000, "L0.1000"},
		{1, -1, "L1"},
		{2, -1, "L2"},
		{3, -1, "L3"},
		{4, -1, "L4"},
		{5, -1, "L5"},
		{6, -1, "L6"},
		{7, -1, "invalid level: 7"},
		{-1, -1, "invalid level: -1"},
		{1, 0, "invalid L1 sublevel: 0"},
		{0, -2, "invalid L0 sublevel: -2"},
	}

	for _, c := range testCases {
		t.Run("", func(t *testing.T) {
			s := func() (result string) {
				defer func() {
					if r := recover(); r != nil {
						result = fmt.Sprint(r)
					}
				}()
				return MakeLevel(c.level, c.sublevel).String()
			}()
			require.EqualValues(t, c.expected, s)
		})
	}
}
