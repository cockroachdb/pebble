// Copyright 2025 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package ascii

import (
	"fmt"
	"strings"
	"testing"

	"github.com/cockroachdb/datadriven"
	"github.com/cockroachdb/pebble/internal/strparse"
	"github.com/stretchr/testify/require"
)

func TestASCIIBoardDatadriven(t *testing.T) {
	var board Board
	datadriven.RunTest(t, "testdata/ascii_board", func(t *testing.T, td *datadriven.TestData) string {
		switch td.Cmd {
		case "make":
			var w, h int
			td.ScanArgs(t, "w", &w)
			td.ScanArgs(t, "h", &h)
			board = Make(w, h)
			return board.String()
		case "write":
			for _, line := range strings.Split(td.Input, "\n") {
				p := strparse.MakeParser(" ", line)
				r := p.Int()
				c := p.Int()
				board.At(r, c).WriteString(p.Remaining())
			}
			return board.String()
		default:
			return fmt.Sprintf("unknown command: %s", td.Cmd)
		}
	})
}

func TestASCIIBoard(t *testing.T) {
	board := Make(10, 2)
	board.At(0, 0).Printf("Hello\nworld!")
	require.Equal(t, `Hello
world!`, board.String())

	board.Reset(10)
	cur := board.At(1, 5).SetCarriageReturnPosition().Printf("a\nb\nc\nd\ne\nf\ng\nh\ni\nj\n")
	require.Equal(t, 11, cur.Row())
	require.Equal(t, 5, cur.Column())
	require.Equal(t, `
     a
     b
     c
     d
     e
     f
     g
     h
     i
     j`, board.String())
}
