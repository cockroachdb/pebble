// Copyright 2020 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package manifest

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestLevel(t *testing.T) {
	testCases := []struct {
		level    Level
		expected string
	}{
		{Level(0), "L0"},
		{Level(1), "L1"},
		{Level(2), "L2"},
		{Level(3), "L3"},
		{Level(4), "L4"},
		{Level(5), "L5"},
		{Level(6), "L6"},

		{L0Sublevel(0), "L0.0"},
		{L0Sublevel(1), "L0.1"},
		{L0Sublevel(2), "L0.2"},

		{FlushableIngestLevel(), "flushable-ingest"},
	}

	for _, c := range testCases {
		t.Run(c.expected, func(t *testing.T) {
			s := c.level.String()
			require.EqualValues(t, c.expected, s)
		})
	}
}
