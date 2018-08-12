// Copyright 2011 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package db

import (
	"testing"
)

func TestDefAppendSeparator(t *testing.T) {
	testCases := []struct {
		a, b, want string
	}{
		// Examples from the doc comments.
		{"black", "blue", "blb"},
		{"green", "", "green"},
		// Non-empty b values. The C++ Level-DB code calls these separators.
		{"", "2", ""},
		{"1", "2", "1"},
		{"1", "29", "2"},
		{"13", "19", "14"},
		{"13", "99", "2"},
		{"135", "19", "14"},
		{"1357", "19", "14"},
		{"1357", "2", "14"},
		{"13\xff", "14", "13\xff"},
		{"13\xff", "19", "14"},
		{"1\xff\xff", "19", "1\xff\xff"},
		{"1\xff\xff", "2", "1\xff\xff"},
		{"1\xff\xff", "9", "2"},
		// Empty b values. The C++ Level-DB code calls these successors.
		{"", "", ""},
		{"1", "", "1"},
		{"11", "", "11"},
		{"11\xff", "", "11\xff"},
		{"1\xff", "", "1\xff"},
		{"1\xff\xff", "", "1\xff\xff"},
		{"\xff", "", "\xff"},
		{"\xff\xff", "", "\xff\xff"},
		{"\xff\xff\xff", "", "\xff\xff\xff"},
	}
	for _, tc := range testCases {
		t.Run("", func(t *testing.T) {
			got := string(DefaultComparer.Separator(nil, []byte(tc.a), []byte(tc.b)))
			if got != tc.want {
				t.Errorf("a, b = %q, %q: got %q, want %q", tc.a, tc.b, got, tc.want)
			}
		})
	}
}
