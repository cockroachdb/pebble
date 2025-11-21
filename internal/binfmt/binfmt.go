// Copyright 2024 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

// Package binfmt exposes utilities for formatting binary data with descriptive
// comments.
package binfmt

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"math"
	"strconv"
	"strings"
	"unsafe"

	"github.com/cockroachdb/crlib/crstrings"
	"github.com/cockroachdb/pebble/internal/treeprinter"
)

// New constructs a new binary formatter.
func New(data []byte) *Formatter {
	offsetWidth := strconv.Itoa(max(int(math.Log10(float64(len(data)-1)))+1, 1))
	return &Formatter{
		data:            data,
		lineWidth:       40,
		offsetFormatStr: "%0" + offsetWidth + "d-%0" + offsetWidth + "d: ",
	}
}

// Formatter is a utility for formatting binary data with descriptive comments.
type Formatter struct {
	buf       bytes.Buffer
	lines     [][2]string // (binary data, comment) tuples
	data      []byte
	off       int
	anchorOff int

	// config
	lineWidth       int
	linePrefix      string
	offsetFormatStr string
}

// SetLinePrefix sets a prefix for each line of formatted output.
func (f *Formatter) SetLinePrefix(prefix string) {
	f.linePrefix = prefix
}

// SetAnchorOffset sets the reference point for relative offset calculations to
// the current offset. Future calls to RelativeOffset() will return an offset
// relative to the current offset.
func (f *Formatter) SetAnchorOffset() {
	f.anchorOff = f.off
}

// RelativeOffset retrieves the current offset relative to the offset at the
// last time SetAnchorOffset was called. If SetAnchorOffset was never called,
// RelativeOffset is equivalent to Offset.
func (f *Formatter) RelativeOffset() int {
	return f.off - f.anchorOff
}

// RelativeData returns the subslice of the original data slice beginning at the
// offset at which SetAnchorOffset was last called. If SetAnchorOffset was never
// called, RelativeData is equivalent to Data.
func (f *Formatter) RelativeData() []byte {
	return f.data[f.anchorOff:]
}

// LineWidth sets the Formatter's maximum line width for binary data.
func (f *Formatter) LineWidth(width int) *Formatter {
	f.lineWidth = width
	return f
}

// More returns true if there is more data in the byte slice that can be formatted.
func (f *Formatter) More() bool {
	return f.off < len(f.data)
}

// Remaining returns the number of unformatted bytes remaining in the byte slice.
func (f *Formatter) Remaining() int {
	return len(f.data) - f.off
}

// Offset returns the current offset within the original data slice.
func (f *Formatter) Offset() int {
	return f.off
}

// PeekUint reads a little-endian unsigned integer of the specified width at the
// current offset.
func (f *Formatter) PeekUint(w int) uint64 {
	switch w {
	case 1:
		return uint64(f.data[f.off])
	case 2:
		return uint64(binary.LittleEndian.Uint16(f.data[f.off:]))
	case 4:
		return uint64(binary.LittleEndian.Uint32(f.data[f.off:]))
	case 8:
		return binary.LittleEndian.Uint64(f.data[f.off:])
	default:
		panic("unsupported width")
	}
}

// Byte formats a single byte in binary format, displaying each bit as a zero or
// one.
func (f *Formatter) Byte(format string, args ...interface{}) int {
	f.printOffsets(1)
	f.printf("b %08b", f.data[f.off])
	f.off++
	f.newline(f.buf.String(), fmt.Sprintf(format, args...))
	return 1
}

// HexBytesln formats the next n bytes in hexadecimal format, appending the
// formatted comment string to each line and ending on a newline.
func (f *Formatter) HexBytesln(n int, format string, args ...interface{}) int {
	commentLine := strings.TrimSpace(fmt.Sprintf(format, args...))
	printLine := func() {
		bytesInLine := min(f.lineWidth/2, n)
		if f.buf.Len() == 0 {
			f.printOffsets(bytesInLine)
		}
		f.printf("x %0"+strconv.Itoa(bytesInLine*2)+"x", f.data[f.off:f.off+bytesInLine])
		f.newline(f.buf.String(), commentLine)
		f.off += bytesInLine
		n -= bytesInLine
	}
	printLine()
	commentLine = "(continued...)"
	for n > 0 {
		printLine()
	}
	return n
}

// HexTextln formats the next n bytes in hexadecimal format, appending a comment
// to each line showing the ASCII equivalent characters for each byte for bytes
// that are human-readable.
func (f *Formatter) HexTextln(n int) int {
	printLine := func() {
		bytesInLine := min(f.lineWidth/2, n)
		if f.buf.Len() == 0 {
			f.printOffsets(bytesInLine)
		}
		f.printf("x %0"+strconv.Itoa(bytesInLine*2)+"x", f.data[f.off:f.off+bytesInLine])
		commentLine := asciiChars(f.data[f.off : f.off+bytesInLine])
		f.newline(f.buf.String(), commentLine)
		f.off += bytesInLine
		n -= bytesInLine
	}
	printLine()
	for n > 0 {
		printLine()
	}
	return n
}

// Uvarint decodes the bytes at the current offset as a uvarint, formatting them
// in hexadecimal and prefixing the comment with the encoded decimal value.
func (f *Formatter) Uvarint(format string, args ...interface{}) {
	comment := fmt.Sprintf(format, args...)
	v, n := binary.Uvarint(f.data[f.off:])
	f.HexBytesln(n, "uvarint(%d): %s", v, comment)
}

// Line prepares a single line of formatted output that will consume n bytes,
// but formatting those n bytes in multiple ways. The line will be prefixed with
// the offsets for the line's entire data.
func (f *Formatter) Line(n int) Line {
	f.printOffsets(n)
	return Line{f: f, n: n, i: 0}
}

// String returns the current formatted output.
func (f *Formatter) String() string {
	f.buf.Reset()
	// Identify the max width of the binary data so that we can add padding to
	// align comments on the right.
	binaryLineWidth := 0
	for _, lineData := range f.lines {
		binaryLineWidth = max(binaryLineWidth, len(lineData[0]))
	}
	for _, lineData := range f.lines {
		fmt.Fprint(&f.buf, f.linePrefix)
		fmt.Fprint(&f.buf, lineData[0])
		if len(lineData[1]) > 0 {
			if len(lineData[0]) == 0 {
				// There's no binary data on this line, just a comment. Print
				// the comment left-aligned.
				fmt.Fprint(&f.buf, "# ")
			} else {
				// Align the comment to the right of the binary data.
				fmt.Fprint(&f.buf, strings.Repeat(" ", binaryLineWidth-len(lineData[0])))
				fmt.Fprint(&f.buf, " # ")
			}
			fmt.Fprint(&f.buf, lineData[1])
		}
		fmt.Fprintln(&f.buf)
	}
	return f.buf.String()
}

// ToTreePrinter formats the current output and creates a treeprinter child node
// for each line. The current output is reset; the position within the binary
// buffer is not.
func (f *Formatter) ToTreePrinter(tp treeprinter.Node) {
	for l := range crstrings.LinesSeq(f.String()) {
		tp.Child(l)
	}
	f.buf.Reset()
	f.lines = f.lines[:0]
}

// Pointer returns a pointer into the original data slice at the specified
// offset.
func (f *Formatter) Pointer(off int) unsafe.Pointer {
	return unsafe.Pointer(&f.data[f.off+off])
}

// Data returns the original data slice. Offset may be used to retrieve the
// current offset within the slice.
func (f *Formatter) Data() []byte {
	return f.data
}

func (f *Formatter) newline(binaryData, comment string) {
	f.lines = append(f.lines, [2]string{binaryData, comment})
	f.buf.Reset()
}

func (f *Formatter) printOffsets(n int) {
	f.printf(f.offsetFormatStr, f.off, f.off+n)
}

func (f *Formatter) printf(format string, args ...interface{}) {
	fmt.Fprintf(&f.buf, format, args...)
}

// Line is a pending line of formatted binary output.
type Line struct {
	f *Formatter
	n int
	i int
}

// Append appends the provided string to the current line.
func (l Line) Append(s string) Line {
	fmt.Fprint(&l.f.buf, s)
	return l
}

// Binary formats the next n bytes in binary format, displaying each bit as
// a zero or one.
func (l Line) Binary(n int) Line {
	if n+l.i > l.n {
		panic("binary data exceeds consumed line length")
	}
	for i := 0; i < n; i++ {
		l.f.printf("%08b", l.f.data[l.f.off+l.i])
		l.i++
	}
	return l
}

// HexBytes formats the next n bytes in hexadecimal format.
func (l Line) HexBytes(n int) Line {
	if n+l.i > l.n {
		panic("binary data exceeds consumed line length")
	}
	l.f.printf("%0"+strconv.Itoa(n*2)+"x", l.f.data[l.f.off+l.i:l.f.off+l.i+n])
	l.i += n
	return l
}

// Done finishes the line, appending the provided comment if any.
func (l Line) Done(format string, args ...interface{}) int {
	if l.n != l.i {
		panic("unconsumed data in line")
	}
	l.f.newline(l.f.buf.String(), fmt.Sprintf(format, args...))
	l.f.off += l.n
	return l.n
}

func asciiChars(b []byte) string {
	s := make([]byte, len(b))
	for i := range b {
		if b[i] >= 32 && b[i] <= 126 {
			s[i] = b[i]
		} else {
			s[i] = '.'
		}
	}
	return string(s)
}
