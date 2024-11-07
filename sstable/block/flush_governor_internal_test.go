// Copyright 2024 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package block

import "testing"

func TestFindClosestClass(t *testing.T) {
	vals := []int{10, 20, 30, 50}
	testCases := [][2]int{
		{0, 10},
		{9, 10},
		{10, 10},
		{11, 10},
		{14, 10},
		{16, 20},
		{18, 20},
		{20, 20},
		{24, 20},
		{26, 30},
		{39, 30},
		{41, 50},
		{60, 50},
		{1000, 50},
	}
	for _, tc := range testCases {
		res := vals[findClosestClass(vals, tc[0])]
		if res != tc[1] {
			t.Errorf("expected %d, but got %d", tc[1], res)
		}
	}
}
