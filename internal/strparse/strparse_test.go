// Copyright 2024 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package strparse

import (
	"strings"
	"testing"

	"github.com/cockroachdb/pebble/internal/base"
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

func TestHashSeqNum(t *testing.T) {
	t.Run("valid", func(t *testing.T) {
		p := MakeParser("[]-", "#42")
		require.Equal(t, base.SeqNum(42), p.HashSeqNum())
	})

	// Each invalid case should produce an Errf-style panic that includes the
	// original input, rather than panicking on out-of-bounds inside ParseSeqNum.
	invalidCases := []struct {
		name      string
		separator string
		input     string
		wantSub   string
	}{
		{name: "missing prefix", separator: "[]-", input: "123", wantSub: `expected sequence number with "#" prefix`},
		{name: "empty number", separator: "[]-", input: "#", wantSub: `expected sequence number after "#" prefix`},
		{name: "wrong token", separator: "[]-", input: "]", wantSub: `expected sequence number with "#" prefix`},
	}
	for _, tc := range invalidCases {
		t.Run(tc.name, func(t *testing.T) {
			p := MakeParser(tc.separator, tc.input)
			defer func() {
				r := recover()
				require.NotNil(t, r, "expected panic")
				err, ok := r.(error)
				require.True(t, ok, "expected error panic, got %T", r)
				msg := err.Error()
				require.Contains(t, msg, tc.wantSub)
				require.Contains(t, msg, tc.input, "error should reference original input")
			}()
			_ = p.HashSeqNum()
		})
	}
}

func TestSeqNumRangeMalformed(t *testing.T) {
	// Malformed range "[#1-]" should produce an Errf-style panic, not an
	// out-of-bounds panic from ParseSeqNum("").
	p := MakeParser("[]-", "[#1-]")
	defer func() {
		r := recover()
		require.NotNil(t, r, "expected panic")
		err, ok := r.(error)
		require.True(t, ok, "expected error panic, got %T", r)
		require.True(t, strings.Contains(err.Error(), "error parsing"),
			"expected Errf-formatted error, got %q", err.Error())
	}()
	_ = p.SeqNumRange()
}
