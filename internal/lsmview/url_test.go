// Copyright 2024 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package lsmview

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestGenerateURL(t *testing.T) {
	d := Data{
		Levels: []Level{
			{
				Name: "L0",
				Tables: []Table{
					{
						Label:       "1",
						Size:        1024,
						SmallestKey: 1, // b
						LargestKey:  2, // c
						Details:     []string{"foo", "bar"},
					},
					{
						Label:       "2",
						Size:        4096,
						SmallestKey: 3, // d
						LargestKey:  5, // f
						Details:     []string{"details about table 2"},
					},
				},
			},
			{
				Name: "L1",
				Tables: []Table{
					{
						Label:       "3",
						Size:        2000,
						SmallestKey: 1, // b
						LargestKey:  4, // e
						Details:     []string{"details about table 3"},
					},
					{
						Label:       "4",
						Size:        3000,
						SmallestKey: 6, // g
						LargestKey:  7, // g
						Details:     []string{"details about table 5"},
					},
				},
			},
		},
		Keys: []string{
			"a", // 0
			"b", // 1
			"c", // 2
			"d", // 3
			"e", // 4
			"f", // 5
			"g", // 6
			"k", // 7
		},
	}
	url, err := GenerateURL(d)
	require.NoError(t, err)
	expected := `
https://raduberinde.github.io/lsmview/decode.html#eJyE0NFqgzAUxvH7PYX8r89FjLZjeYa9wZByXE9LMZ1Q3aAbvvuYkxRF8O6EhN-X7_wQ7ctiR3ibxsOHXo3Aq0PotY42XWptkUCO0F2-jZA7XwrdVWO0rj80difkQtTbOZ29cLReL2MCp7ZFqPVGNciD9Iks3ct-SRYLcjcjpzHTuv3ss_G_maca_gNmhfL1QkVK9865rULlZnoxL1cmvljh9wv-eZPfjeUqobH7-Ej_VorwjnBEMIQTwhmhoRqefgMAAP__SwKZFA==
`
	require.Equal(t, strings.TrimSpace(expected), url.String())
}
