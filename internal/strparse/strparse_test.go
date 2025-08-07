// Copyright 2024 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package strparse

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestParserOffsets(t *testing.T) {
	tests := []struct {
		sep   string
		input string
		want  []token
	}{
		{sep: "|", input: "a   |  b   |c",
			want: []token{
				{tok: "a", offset: 0},
				{tok: "|", offset: 4},
				{tok: "b", offset: 7},
				{tok: "|", offset: 11},
				{tok: "c", offset: 12},
			},
		},
		{sep: "|", input: "a|b|c",
			want: []token{
				{tok: "a", offset: 0},
				{tok: "|", offset: 1},
				{tok: "b", offset: 2},
				{tok: "|", offset: 3},
				{tok: "c", offset: 4},
			},
		},
		{sep: "()", input: "a    (   (  b )            ) c       ",
			want: []token{
				{tok: "a", offset: 0},
				{tok: "(", offset: 5},
				{tok: "(", offset: 9},
				{tok: "b", offset: 12},
				{tok: ")", offset: 14},
				{tok: ")", offset: 27},
				{tok: "c", offset: 29},
			},
		},
	}
	for _, test := range tests {
		p := MakeParser(test.sep, test.input)
		require.Equal(t, test.want, p.tokens)
	}
}
