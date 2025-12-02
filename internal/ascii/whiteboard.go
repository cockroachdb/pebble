// Copyright 2025 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package ascii

import (
	"bytes"
	"fmt"
	"slices"
	"strings"
)

// Board is a simple ASCII-based board for rendering ASCII text diagrams.
type Board struct {
	buf   []rune
	width int
}

// Make returns a new Board with the given initial width and height.
func Make(width, height int) Board {
	buf := make([]rune, 0, width*height)
	return Board{buf: buf, width: width}
}

// At returns a position at the given coordinates.
func (b *Board) At(r, c int) Cursor {
	if r >= b.lines() {
		b.growBuf((r - b.lines() + 1) * b.width)
	}
	return Cursor{b: b, r: r, c: c}
}

func (b *Board) growBuf(n int) {
	b.buf = slices.Grow(b.buf, n)
	for range n {
		b.buf = append(b.buf, ' ')
	}
}

// NewLine appends a new line to the board and returns a position at the
// beginning of the line.
func (b *Board) NewLine() Cursor {
	return b.At(b.lines(), 0)
}

// String returns the Board as a string.
func (b *Board) String() string {
	return b.Render("")
}

// Render returns the Board as a string, with every line prefixed by
// indent.
func (b *Board) Render(indent string) string {
	var buf bytes.Buffer
	for r := 0; r < b.lines(); r++ {
		if r > 0 {
			buf.WriteByte('\n')
		}
		buf.WriteString(indent)
		buf.WriteString(strings.TrimRight(string(b.row(r)), " "))
	}
	return buf.String()
}

// Reset resets the board to the given width and clears the contents.
func (b *Board) Reset(w int) {
	b.buf = b.buf[:0]
	b.width = w
}

func (b *Board) write(r, c int, s string) {
	if n := len([]rune(s)); c+n > b.width {
		b.growWidth(c + n)
	}
	row := b.row(r)
	i := 0
	for _, ch := range s {
		row[c+i] = ch
		i++
	}
}

func (b *Board) repeat(r, c int, n int, ch rune) {
	if c+n > b.width {
		b.growWidth(c + n)
	}
	row := b.row(r)
	for i := 0; i < n; i++ {
		row[c+i] = ch
	}
}

func (b *Board) growWidth(w int) {
	buf := make([]rune, w*b.lines())
	for i := range buf {
		buf[i] = ' '
	}
	for i := range b.lines() {
		copy(buf[i*w:(i+1)*w], b.buf[i*b.width:(i+1)*b.width])
	}
	b.buf = buf
	b.width = w
}

func (b *Board) lines() int {
	return len(b.buf) / b.width
}

func (b *Board) row(r int) []rune {
	if sz := (r + 1) * b.width; sz > len(b.buf) {
		b.growBuf(sz - len(b.buf))
	}
	return b.buf[r*b.width : (r+1)*b.width]
}

// Cursor is a position on a Board.
type Cursor struct {
	b    *Board
	r, c int
	// carriageReturnCol is the column to which newlines will return.  It is set
	// by SetCarriageReturnPosition. It's used when writing text in a column,
	// and newlines should return to the same column position.
	carriageReturnCol int
}

// Offset returns a new cursor with the given offset from the current cursor.
func (c Cursor) Offset(dr, dc int) Cursor {
	c.r += dr
	c.c += dc
	return c
}

// Down returns a new cursor with the given row offset from the current cursor.
func (c Cursor) Down(numRows int) Cursor {
	c.r += numRows
	return c
}

// Right returns a new cursor with the given column offset from the current cursor.
func (c Cursor) Right(numCols int) Cursor {
	c.c += numCols
	return c
}

// SetCarriageReturnPosition returns a copy of the cursor, but with a carriage
// return position set so that newlines written to the resulting Cursor will
// return to the current column.
func (c Cursor) SetCarriageReturnPosition() Cursor {
	c.carriageReturnCol = c.c
	return c
}

// Row returns the row of the current position.
func (c Cursor) Row() int {
	return c.r
}

// Column returns the column of the current position.
func (c Cursor) Column() int {
	return c.c
}

// SetRow returns a copy of the cursor, but with the row set to the given value.
func (c Cursor) SetRow(row int) Cursor {
	c.r = row
	return c
}

// SetColumn returns a copy of the cursor, but with the column set to the given
// value.
func (c Cursor) SetColumn(col int) Cursor {
	c.c = col
	return c
}

// Printf writes the formatted string to cursor, returning a cursor where the
// written text ends.
func (c Cursor) Printf(format string, args ...interface{}) Cursor {
	return c.WriteString(fmt.Sprintf(format, args...))
}

// WriteString writes the provided string starting at the cursor, returning a
// cursor where the written text ends. Newlines in the string break to the next
// row, with the column reset to the cursor's carriage return column.
func (c Cursor) WriteString(s string) Cursor {
	for len(s) > 0 {
		i := strings.IndexByte(s, '\n')
		if i >= 0 {
			c.b.write(c.r, c.c, s[:i])
			c = c.NewlineReturn()
			s = s[i+1:]
		} else {
			c.b.write(c.r, c.c, s)
			c.c += len([]rune(s))
			break
		}
	}
	return c
}

// Repeat writes the given character n times starting at the cursor, returning a
// cursor where the written bytes end.
func (c Cursor) Repeat(n int, ch rune) Cursor {
	c.b.repeat(c.r, c.c, n, ch)
	return c.Right(n)
}

// NewlineReturn returns a cursor at the next line, with the column set to the
// cursor's carriage return column.
func (c Cursor) NewlineReturn() Cursor {
	c.r += 1
	c.c = c.carriageReturnCol
	return c
}
